package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// CreateWorkerToken stores a pool token's hash (ADR-025). The audit row goes to the
// platform-admin's tenant.
func (s *Store) CreateWorkerToken(ctx context.Context, t domain.WorkerToken, auditTenant domain.TenantID, audit Audit) (domain.WorkerToken, error) {
	tenant, ok := canonicalUUID(string(auditTenant))
	if !ok {
		return domain.WorkerToken{}, domain.ErrNotFound
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO worker_tokens (id, pool, name, prefix, secret_hash, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING created_at`,
			t.ID, t.Pool, t.Name, t.Prefix, t.SecretHash, nullTime(t.ExpiresAt)).Scan(&t.CreatedAt); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "worker_token.create", t.ID, map[string]any{"pool": t.Pool, "name": t.Name})
	})
	return t, err
}

const workerTokenColumns = `id, pool, name, prefix, secret_hash, created_at, expires_at, revoked_at, last_used_at`

func scanWorkerToken(row pgx.Row) (domain.WorkerToken, error) {
	var (
		t                         domain.WorkerToken
		id                        pgtype.UUID
		expires, revoked, lastUse pgtype.Timestamptz
	)
	err := row.Scan(&id, &t.Pool, &t.Name, &t.Prefix, &t.SecretHash, &t.CreatedAt, &expires, &revoked, &lastUse)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WorkerToken{}, domain.ErrNotFound
	}
	t.ID, t.ExpiresAt, t.RevokedAt, t.LastUsedAt = uuidString(id), expires.Time, revoked.Time, lastUse.Time
	return t, err
}

// ListWorkerTokens returns a pool's tokens, newest first.
func (s *Store) ListWorkerTokens(ctx context.Context, pool string) ([]domain.WorkerToken, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+workerTokenColumns+` FROM worker_tokens WHERE pool = $1
		ORDER BY created_at DESC LIMIT 1000`, pool)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.WorkerToken, error) { return scanWorkerToken(r) })
}

// FindWorkerToken returns the token with prefix, whether or not it is still usable.
func (s *Store) FindWorkerToken(ctx context.Context, prefix string) (domain.WorkerToken, error) {
	return scanWorkerToken(s.pool.QueryRow(ctx, `SELECT `+workerTokenColumns+` FROM worker_tokens WHERE prefix = $1`, prefix))
}

// RevokeWorkerToken revokes a pool's token; revoking twice is a no-op.
func (s *Store) RevokeWorkerToken(ctx context.Context, pool, id string, auditTenant domain.TenantID, audit Audit) error {
	tenant, ok1 := canonicalUUID(string(auditTenant))
	tokenID, ok2 := canonicalUUID(id)
	if !ok1 || !ok2 {
		return domain.ErrNotFound
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE worker_tokens SET revoked_at = COALESCE(revoked_at, now())
			WHERE id = $1 AND pool = $2`, tokenID, pool)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrNotFound
		}
		return writeAudit(ctx, tx, tenant, audit, "worker_token.revoke", tokenID, map[string]any{"pool": pool})
	})
}

// TouchWorkerToken records that a token was just used.
func (s *Store) TouchWorkerToken(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE worker_tokens SET last_used_at = now() WHERE id = $1`, id)
	return err
}

const apiKeyColumns = `id, tenant_id, name, prefix, secret_hash, role, created_at, expires_at, revoked_at, last_used_at`

func scanAPIKey(row pgx.Row) (domain.APIKey, error) {
	var (
		k                         domain.APIKey
		id, tenant                pgtype.UUID
		role                      string
		expires, revoked, lastUse pgtype.Timestamptz
	)
	err := row.Scan(&id, &tenant, &k.Name, &k.Prefix, &k.SecretHash, &role, &k.CreatedAt, &expires, &revoked, &lastUse)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.APIKey{}, domain.ErrNotFound
	}
	k.ID, k.TenantID, k.Role = uuidString(id), domain.TenantID(uuidString(tenant)), domain.Role(role)
	k.ExpiresAt, k.RevokedAt, k.LastUsedAt = expires.Time, revoked.Time, lastUse.Time
	return k, err
}

// ListAPIKeys returns a tenant's keys, newest first.
func (s *Store) ListAPIKeys(ctx context.Context, tenantID domain.TenantID) ([]domain.APIKey, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return nil, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT `+apiKeyColumns+` FROM api_keys WHERE tenant_id = $1
		ORDER BY created_at DESC LIMIT 1000`, tenant)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.APIKey, error) { return scanAPIKey(r) })
}

// GetAPIKey returns a tenant's key; another tenant's key is not found.
func (s *Store) GetAPIKey(ctx context.Context, tenantID domain.TenantID, id string) (domain.APIKey, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	keyID, ok2 := canonicalUUID(id)
	if !ok1 || !ok2 {
		return domain.APIKey{}, domain.ErrNotFound
	}
	return scanAPIKey(s.pool.QueryRow(ctx, `SELECT `+apiKeyColumns+` FROM api_keys WHERE id = $1 AND tenant_id = $2`, keyID, tenant))
}

// RotateAPIKey stores next and makes the active key it replaces expire by graceEnds, unless it
// expires sooner. A revoked or unknown key is not found.
func (s *Store) RotateAPIKey(ctx context.Context, tenantID domain.TenantID, oldID string, next domain.APIKey, graceEnds time.Time, audit Audit) (domain.APIKey, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	keyID, ok2 := canonicalUUID(oldID)
	if !ok1 || !ok2 {
		return domain.APIKey{}, domain.ErrNotFound
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE api_keys SET expires_at = LEAST(COALESCE(expires_at, $3), $3)
			WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL`, keyID, tenant, graceEnds)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrNotFound
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO api_keys (id, tenant_id, name, prefix, secret_hash, role, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING created_at`,
			next.ID, tenant, next.Name, next.Prefix, next.SecretHash, string(next.Role), nullTime(next.ExpiresAt),
		).Scan(&next.CreatedAt); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "api_key.rotate", keyID,
			map[string]any{"replacement": next.ID, "grace_ends": graceEnds})
	})
	return next, err
}

// TouchAPIKey records that a key was just used.
func (s *Store) TouchAPIKey(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id = $1`, id)
	return err
}
