package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// SetJobTypePaused holds or releases dispatch of a tenant's job type (ADR-017).
func (s *Store) SetJobTypePaused(ctx context.Context, tenantID domain.TenantID, name string, paused bool, audit Audit) (domain.JobType, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return domain.JobType{}, domain.ErrNotFound
	}
	var jt domain.JobType
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		if jt, err = scanJobType(tx.QueryRow(ctx, `UPDATE job_types SET paused = $3, updated_at = now()
			WHERE tenant_id = $1 AND name = $2 RETURNING `+jobTypeColumns, tenant, name, paused)); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, map[bool]string{true: "job_type.pause", false: "job_type.resume"}[paused], name, nil)
	})
	return jt, err
}

// PoolInfo is a pool's state for operators.
type PoolInfo struct {
	Name          string
	Paused        bool
	Owner         *Lease // nil while no node holds the pool's lease
	Workers       int
	Slots         int
	ReadyJobs     int
	RunningJobs   int
	BacklogTarget *time.Duration // nil means the platform default
	BacklogAge    *time.Duration // from the owner's latest sample; nil when there is none within BacklogFreshness
}

// ListPools returns every pool known from job types, live sessions or pool rows.
func (s *Store) ListPools(ctx context.Context) ([]PoolInfo, error) {
	rows, err := s.pool.Query(ctx, `
		WITH names AS (
		    SELECT pool AS name FROM job_types
		    UNION SELECT pool FROM worker_sessions WHERE state = 'ACTIVE'
		    UNION SELECT name FROM pools)
		SELECT n.name, COALESCE(p.paused, false), l.holder, l.address, l.epoch, l.expires_at,
		    (SELECT count(*) FROM worker_sessions ws WHERE ws.pool = n.name AND ws.state = 'ACTIVE'),
		    (SELECT COALESCE(sum(slots), 0) FROM worker_sessions ws WHERE ws.pool = n.name AND ws.state = 'ACTIVE'),
		    (SELECT count(*) FROM jobs j WHERE j.pool = n.name AND j.state = 'READY'),
		    (SELECT count(*) FROM jobs j WHERE j.pool = n.name AND j.state = 'RUNNING'),
		    p.backlog_target_ms,
		    CASE WHEN p.sampled_at > now() - make_interval(secs => $1)
		        THEN extract(epoch FROM now() - COALESCE(p.oldest_due_at, now()))::float8 END
		FROM names n
		LEFT JOIN pools p ON p.name = n.name
		LEFT JOIN leases l ON l.name = 'pool:' || n.name AND l.expires_at > now()
		ORDER BY n.name`, BacklogFreshness.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (PoolInfo, error) {
		var (
			p       PoolInfo
			holder  pgtype.Text
			address pgtype.Text
			epoch   pgtype.Int8
			expires pgtype.Timestamptz
			target  pgtype.Int8
			age     pgtype.Float8
		)
		err := r.Scan(&p.Name, &p.Paused, &holder, &address, &epoch, &expires, &p.Workers, &p.Slots, &p.ReadyJobs, &p.RunningJobs,
			&target, &age)
		if holder.Valid {
			p.Owner = &Lease{Name: PoolLeaseName(p.Name), Holder: holder.String, Address: address.String, Epoch: epoch.Int64, ExpiresAt: expires.Time}
		}
		p.BacklogTarget = msDuration(target)
		if age.Valid {
			d := time.Duration(age.Float64 * float64(time.Second))
			p.BacklogAge = &d
		}
		return p, err
	})
}

// SetPoolPaused holds or releases dispatch of a whole pool; the audit row goes to the
// platform-admin's tenant.
func (s *Store) SetPoolPaused(ctx context.Context, pool string, paused bool, auditTenant domain.TenantID, audit Audit) error {
	tenant, ok := canonicalUUID(string(auditTenant))
	if !ok {
		return domain.ErrNotFound
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO pools (name, paused) VALUES ($1, $2)
			ON CONFLICT (name) DO UPDATE SET paused = excluded.paused, updated_at = now()`, pool, paused); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, map[bool]string{true: "pool.pause", false: "pool.resume"}[paused], pool, nil)
	})
}

// SessionInfo is an active worker session with the number of attempts it holds.
type SessionInfo struct {
	domain.WorkerSession
	Running int
}

// ListSessions returns active sessions, optionally of one pool, oldest first.
func (s *Store) ListSessions(ctx context.Context, pool string) ([]SessionInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+sessionColumns+`,
		    (SELECT count(*) FROM jobs j WHERE j.current_session_id = worker_sessions.id AND j.state = 'RUNNING')
		FROM worker_sessions WHERE state = 'ACTIVE' AND ($1::text IS NULL OR pool = $1)
		ORDER BY pool, created_at, id LIMIT 1000`, nullText(pool))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (SessionInfo, error) {
		var info SessionInfo
		ws, err := scanSession(r, &info.Running)
		info.WorkerSession = ws
		return info, err
	})
}

// DrainSession tells an active session's worker to finish its work and deregister.
func (s *Store) DrainSession(ctx context.Context, id domain.SessionID, auditTenant domain.TenantID, audit Audit) error {
	sid, ok1 := canonicalUUID(string(id))
	tenant, ok2 := canonicalUUID(string(auditTenant))
	if !ok1 || !ok2 {
		return domain.ErrNotFound
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE worker_sessions SET draining = true WHERE id = $1 AND state = 'ACTIVE'`, sid)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrNotFound
		}
		return writeAudit(ctx, tx, tenant, audit, "worker.drain", sid, nil)
	})
}

// DeregisterSession closes a session on an operator's request; its held attempts are lost.
func (s *Store) DeregisterSession(ctx context.Context, id domain.SessionID, auditTenant domain.TenantID, audit Audit) error {
	tenant, ok := canonicalUUID(string(auditTenant))
	if !ok {
		return domain.ErrNotFound
	}
	if err := s.CloseSession(ctx, id, ""); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		return writeAudit(ctx, tx, tenant, audit, "worker.deregister", string(id), nil)
	})
}

// TriggerSchedule creates one job for the schedule now, outside its cadence (ADR-017). The
// job is due at once; the promoter applies the overlap policy.
func (s *Store) TriggerSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit) (domain.Job, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	schedID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Job{}, domain.ErrNotFound
	}
	var job domain.Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var now time.Time
		sc, err := scanSchedule(tx.QueryRow(ctx, `SELECT `+scheduleColumns+`, now() FROM schedules
			WHERE id = $1 AND tenant_id = $2 AND state <> 'DELETED' FOR UPDATE`, schedID, tenant), &now)
		if err != nil {
			return err
		}
		jt, err := scanJobType(tx.QueryRow(ctx, `SELECT `+jobTypeColumns+` FROM job_types WHERE tenant_id = $1 AND name = $2`,
			tenant, sc.JobType))
		if err != nil {
			return err
		}
		if !jt.Enabled {
			return fmt.Errorf("%w: job type %s is disabled", domain.ErrConflict, jt.Name)
		}
		for attempt := range 3 {
			fire := now.Add(time.Duration(attempt) * time.Microsecond) // a scheduled fire may hold this instant
			job, err = insertFire(ctx, tx, sc, jt, fire, audit.Actor)
			if !errors.Is(err, pgx.ErrNoRows) {
				break
			}
		}
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "schedule.trigger", schedID, map[string]any{"job_id": job.ID})
	})
	return job, err
}

// insertFire records one fire in the ledger and creates its job, due at the fire time.
func insertFire(ctx context.Context, tx pgx.Tx, sc domain.Schedule, jt domain.JobType, fire time.Time, createdBy string) (domain.Job, error) {
	priority := sc.Priority
	if priority == 0 {
		priority = jt.DefaultPriority
	}
	labels := sc.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return scanJob(tx.QueryRow(ctx, insertFiresSQL+` RETURNING `+activeColumns, string(sc.ID), []time.Time{fire},
		uuidArray([]string{newID()}), string(sc.TenantID), sc.JobType, jt.Version, jt.Pool, int16(priority), sc.Payload,
		labels, createdBy, jt.AttemptTimeout.Milliseconds(), policyToJSON(jt.RetryPolicy), jt.AtMostOnce, []time.Time{fire}))
}
