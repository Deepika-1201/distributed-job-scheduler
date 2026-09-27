package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

const tenantColumns = `id, name, created_at, rate_limit, max_pending, max_running, max_schedules,
	min_schedule_interval_ms, max_payload_bytes`

func scanTenant(row pgx.Row) (domain.Tenant, error) {
	var (
		t                                    domain.Tenant
		id                                   pgtype.UUID
		rate                                 pgtype.Float8
		pending, running, schedules, payload pgtype.Int4
		intervalMS                           pgtype.Int8
	)
	err := row.Scan(&id, &t.Name, &t.CreatedAt, &rate, &pending, &running, &schedules, &intervalMS, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Tenant{}, domain.ErrNotFound
	}
	t.ID = domain.TenantID(uuidString(id))
	intPtr := func(v pgtype.Int4) *int {
		if !v.Valid {
			return nil
		}
		n := int(v.Int32)
		return &n
	}
	q := domain.Quotas{MaxPending: intPtr(pending), MaxRunning: intPtr(running), MaxSchedules: intPtr(schedules), MaxPayloadBytes: intPtr(payload)}
	if rate.Valid {
		q.RateLimit = &rate.Float64
	}
	if intervalMS.Valid {
		d := time.Duration(intervalMS.Int64) * time.Millisecond
		q.MinScheduleInterval = &d
	}
	t.Quotas = q
	return t, err
}

// GetTenant returns a tenant with its configured quotas.
func (s *Store) GetTenant(ctx context.Context, tenantID domain.TenantID) (domain.Tenant, error) {
	id, ok := canonicalUUID(string(tenantID))
	if !ok {
		return domain.Tenant{}, domain.ErrNotFound
	}
	return scanTenant(s.pool.QueryRow(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id))
}

// ListTenants returns up to 1,000 tenants by name.
func (s *Store) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+tenantColumns+` FROM tenants ORDER BY name LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Tenant, error) { return scanTenant(r) })
}

// SetQuotas replaces a tenant's quotas; the audit row goes to the platform-admin's tenant.
func (s *Store) SetQuotas(ctx context.Context, tenantID domain.TenantID, q domain.Quotas, auditTenant domain.TenantID, audit Audit) (domain.Tenant, error) {
	id, ok1 := canonicalUUID(string(tenantID))
	actor, ok2 := canonicalUUID(string(auditTenant))
	if !ok1 || !ok2 {
		return domain.Tenant{}, domain.ErrNotFound
	}
	var interval pgtype.Int8
	if q.MinScheduleInterval != nil {
		interval = pgtype.Int8{Int64: q.MinScheduleInterval.Milliseconds(), Valid: true}
	}
	var t domain.Tenant
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		if t, err = scanTenant(tx.QueryRow(ctx, `
			UPDATE tenants SET rate_limit = $2, max_pending = $3, max_running = $4, max_schedules = $5,
			    min_schedule_interval_ms = $6, max_payload_bytes = $7
			WHERE id = $1 RETURNING `+tenantColumns,
			id, q.RateLimit, q.MaxPending, q.MaxRunning, q.MaxSchedules, interval, q.MaxPayloadBytes)); err != nil {
			return err
		}
		return writeAudit(ctx, tx, actor, audit, "tenant.quotas", id, map[string]any{"quotas": q})
	})
	return t, err
}

// CountPending counts the tenant's unfinished jobs, stopping at limit so the cost is bounded.
func (s *Store) CountPending(ctx context.Context, tenantID domain.TenantID, limit int) (int, error) {
	id, ok := canonicalUUID(string(tenantID))
	if !ok {
		return 0, domain.ErrNotFound
	}
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM jobs WHERE tenant_id = $1 LIMIT $2) AS pending`, id, limit).Scan(&n)
	return n, err
}

// BacklogAges returns, for each unpaused pool with READY jobs, how long its oldest has been due.
func (s *Store) BacklogAges(ctx context.Context) (map[string]time.Duration, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.pool, extract(epoch FROM now() - min(o.run_at))
		FROM (SELECT DISTINCT pool FROM job_types) AS p
		CROSS JOIN unnest('{1,2,3,4}'::smallint[]) AS pr
		CROSS JOIN LATERAL (SELECT run_at FROM jobs
		    WHERE state = 'READY' AND pool = p.pool AND priority = pr ORDER BY run_at LIMIT 1) AS o
		WHERE NOT EXISTS (SELECT 1 FROM pools WHERE name = p.pool AND paused)
		GROUP BY p.pool`)
	if err != nil {
		return nil, err
	}
	ages := map[string]time.Duration{}
	var (
		pool    string
		seconds float64
	)
	_, err = pgx.ForEachRow(rows, []any{&pool, &seconds}, func() error {
		ages[pool] = time.Duration(seconds * float64(time.Second))
		return nil
	})
	return ages, err
}

// Saturated reports whether every database connection is in use and callers have waited
// for one since the last call; it drives load shedding (ADR-018).
func (s *Store) Saturated() bool {
	st := s.pool.Stat()
	waited := st.EmptyAcquireCount()
	s.satMu.Lock()
	defer s.satMu.Unlock()
	grew := waited > s.lastEmptyAcquire
	s.lastEmptyAcquire = waited
	return grew && st.AcquiredConns() >= st.MaxConns()
}

// lockTenantForScheduleQuota serializes schedule creation per tenant and enforces max_schedules.
func lockTenantForScheduleQuota(ctx context.Context, tx pgx.Tx, tenant string) error {
	var limit pgtype.Int4
	if err := tx.QueryRow(ctx, `SELECT max_schedules FROM tenants WHERE id = $1 FOR UPDATE`, tenant).Scan(&limit); err != nil {
		return err
	}
	if !limit.Valid {
		return nil
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM schedules WHERE tenant_id = $1 AND state IN ('ACTIVE', 'PAUSED')`,
		tenant).Scan(&n); err != nil {
		return err
	}
	if n >= int(limit.Int32) {
		return domain.ErrQuotaExceeded
	}
	return nil
}
