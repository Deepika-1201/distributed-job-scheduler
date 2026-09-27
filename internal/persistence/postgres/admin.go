package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// Audit identifies who performed a control-plane action. The store writes the audit_log
// row in the same transaction as the action (invariant I5).
type Audit struct {
	Actor     string
	RequestID string
}

func writeAudit(ctx context.Context, tx pgx.Tx, tenant string, a Audit, action, target string, details map[string]any) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO audit_log (tenant_id, actor, action, target, request_id, details)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		tenant, a.Actor, action, target, nullText(a.RequestID), details)
	return err
}

func newID() string { return uuid.Must(uuid.NewV7()).String() }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// CreateTenant registers a tenant; names are unique.
func (s *Store) CreateTenant(ctx context.Context, name string) (domain.TenantID, error) {
	id := newID()
	_, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2)`, id, name)
	if isUniqueViolation(err) {
		return "", domain.ErrAlreadyExists
	}
	return domain.TenantID(id), err
}

// CreateAPIKey stores a key's hash. audit is nil only when bootstrapping a tenant.
func (s *Store) CreateAPIKey(ctx context.Context, k domain.APIKey, audit *Audit) (domain.APIKey, error) {
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO api_keys (id, tenant_id, name, prefix, secret_hash, role, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING created_at`,
			k.ID, string(k.TenantID), k.Name, k.Prefix, k.SecretHash, string(k.Role), nullTime(k.ExpiresAt),
		).Scan(&k.CreatedAt); err != nil {
			return err
		}
		if audit == nil {
			return nil
		}
		return writeAudit(ctx, tx, string(k.TenantID), *audit, "api_key.create", k.ID,
			map[string]any{"name": k.Name, "role": k.Role})
	})
	return k, err
}

// FindAPIKey returns the key with prefix, whether or not it is still usable.
func (s *Store) FindAPIKey(ctx context.Context, prefix string) (domain.APIKey, error) {
	var (
		k                  domain.APIKey
		id, tenant         pgtype.UUID
		role               string
		expires, revokedAt pgtype.Timestamptz
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, name, prefix, secret_hash, role, created_at, expires_at, revoked_at
		FROM api_keys WHERE prefix = $1`, prefix,
	).Scan(&id, &tenant, &k.Name, &k.Prefix, &k.SecretHash, &role, &k.CreatedAt, &expires, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.APIKey{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.APIKey{}, err
	}
	k.ID, k.TenantID, k.Role = uuidString(id), domain.TenantID(uuidString(tenant)), domain.Role(role)
	k.ExpiresAt, k.RevokedAt = expires.Time, revokedAt.Time
	return k, nil
}

// RevokeAPIKey revokes a tenant's key; revoking twice is a no-op.
func (s *Store) RevokeAPIKey(ctx context.Context, tenantID domain.TenantID, id string, audit Audit) error {
	tenant, ok1 := canonicalUUID(string(tenantID))
	keyID, ok2 := canonicalUUID(id)
	if !ok1 || !ok2 {
		return domain.ErrNotFound
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1 AND tenant_id = $2`, keyID, tenant)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrNotFound
		}
		return writeAudit(ctx, tx, tenant, audit, "api_key.revoke", keyID, nil)
	})
}

const jobTypeColumns = `tenant_id, name, version, pool, default_priority, attempt_timeout_ms, retry_policy,
	at_most_once, enabled, paused, created_at, updated_at`

func scanJobType(row pgx.Row) (domain.JobType, error) {
	var (
		jt        domain.JobType
		tenant    pgtype.UUID
		priority  int16
		timeoutMS int64
		policy    retryPolicyJSON
	)
	err := row.Scan(&tenant, &jt.Name, &jt.Version, &jt.Pool, &priority, &timeoutMS, &policy,
		&jt.AtMostOnce, &jt.Enabled, &jt.Paused, &jt.CreatedAt, &jt.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.JobType{}, domain.ErrNotFound
	}
	jt.TenantID = domain.TenantID(uuidString(tenant))
	jt.DefaultPriority = domain.Priority(priority)
	jt.AttemptTimeout = time.Duration(timeoutMS) * time.Millisecond
	jt.RetryPolicy = policy.policy()
	return jt, err
}

func (s *Store) CreateJobType(ctx context.Context, jt domain.JobType, audit Audit) (domain.JobType, error) {
	var created domain.JobType
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		created, err = scanJobType(tx.QueryRow(ctx, `
			INSERT INTO job_types (tenant_id, name, version, pool, default_priority, attempt_timeout_ms,
			    retry_policy, at_most_once, enabled)
			VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
			RETURNING `+jobTypeColumns,
			string(jt.TenantID), jt.Name, jt.Pool, int16(jt.DefaultPriority), jt.AttemptTimeout.Milliseconds(),
			policyToJSON(jt.RetryPolicy), jt.AtMostOnce, jt.Enabled))
		if isUniqueViolation(err) {
			return domain.ErrAlreadyExists
		}
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, string(jt.TenantID), audit, "job_type.create", jt.Name, nil)
	})
	return created, err
}

func (s *Store) GetJobType(ctx context.Context, tenantID domain.TenantID, name string) (domain.JobType, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return domain.JobType{}, domain.ErrNotFound
	}
	return scanJobType(s.pool.QueryRow(ctx,
		`SELECT `+jobTypeColumns+` FROM job_types WHERE tenant_id = $1 AND name = $2`, tenant, name))
}

func (s *Store) ListJobTypes(ctx context.Context, tenantID domain.TenantID) ([]domain.JobType, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return nil, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT `+jobTypeColumns+` FROM job_types WHERE tenant_id = $1 ORDER BY name`, tenant)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.JobType, error) { return scanJobType(r) })
}

// UpdateJobType replaces a job type's mutable fields.
func (s *Store) UpdateJobType(ctx context.Context, jt domain.JobType, audit Audit) (domain.JobType, error) {
	var updated domain.JobType
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		updated, err = scanJobType(tx.QueryRow(ctx, `
			UPDATE job_types SET pool = $3, default_priority = $4, attempt_timeout_ms = $5, retry_policy = $6,
			    at_most_once = $7, enabled = $8, updated_at = now()
			WHERE tenant_id = $1 AND name = $2
			RETURNING `+jobTypeColumns,
			string(jt.TenantID), jt.Name, jt.Pool, int16(jt.DefaultPriority), jt.AttemptTimeout.Milliseconds(),
			policyToJSON(jt.RetryPolicy), jt.AtMostOnce, jt.Enabled))
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, string(jt.TenantID), audit, "job_type.update", jt.Name, nil)
	})
	return updated, err
}
