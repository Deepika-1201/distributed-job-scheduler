package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

const sessionColumns = `id, pool, worker_id, job_types, slots, labels, runtime_version, state, created_at,
	heartbeat_at, lease_expires_at, draining`

func scanSession(row pgx.Row, extra ...any) (domain.WorkerSession, error) {
	var (
		s     domain.WorkerSession
		id    pgtype.UUID
		state string
	)
	err := row.Scan(append([]any{&id, &s.Pool, &s.WorkerID, &s.JobTypes, &s.Slots, &s.Labels, &s.RuntimeVersion, &state,
		&s.CreatedAt, &s.HeartbeatAt, &s.LeaseExpiresAt, &s.Draining}, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WorkerSession{}, domain.ErrNotFound
	}
	s.ID, s.State = domain.SessionID(uuidString(id)), domain.SessionState(state)
	return s, err
}

// CreateSession registers a worker session whose lease lasts ttl.
func (s *Store) CreateSession(ctx context.Context, ws domain.WorkerSession, ttl time.Duration) (domain.WorkerSession, error) {
	jobTypes, labels := ws.JobTypes, ws.Labels
	if jobTypes == nil {
		jobTypes = []string{}
	}
	if labels == nil {
		labels = map[string]string{}
	}
	return scanSession(s.pool.QueryRow(ctx, `
		INSERT INTO worker_sessions (id, pool, worker_id, job_types, slots, labels, runtime_version, state, lease_expires_at, renewed_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', now() + make_interval(secs => $8), NULLIF($9, ''))
		RETURNING `+sessionColumns,
		newID(), ws.Pool, ws.WorkerID, jobTypes, ws.Slots, labels, ws.RuntimeVersion, ttl.Seconds(), ws.RenewedBy))
}

// GetActiveSession returns a session that is still active; others are reported as not found.
func (s *Store) GetActiveSession(ctx context.Context, id domain.SessionID) (domain.WorkerSession, error) {
	sid, ok := canonicalUUID(string(id))
	if !ok {
		return domain.WorkerSession{}, domain.ErrNotFound
	}
	return scanSession(s.pool.QueryRow(ctx,
		`SELECT `+sessionColumns+` FROM worker_sessions WHERE id = $1 AND state = 'ACTIVE'`, sid))
}

// AttemptRef identifies an attempt of a job.
type AttemptRef struct {
	JobID     domain.JobID
	AttemptID domain.AttemptID
}

// HeartbeatResult tells a worker which of its attempts to cancel and which to abandon.
type HeartbeatResult struct {
	Cancel []domain.AttemptID // the job was cancelled
	Stale  []domain.AttemptID // no longer the job's current attempt
	Drain  bool               // an operator asked the worker to drain
}

// unreportedGrace is how long an attempt may be missing from its session's heartbeats before
// it is released as undelivered (LLD §12.3): three heartbeat intervals.
const unreportedGrace = 15 * time.Second

// Heartbeat renews an active session's lease and reconciles the attempts the worker reports
// holding with those the database assigns to it. A non-empty pool limits it to that pool's
// sessions (ADR-025); node is the engine renewing it (ADR-029).
func (s *Store) Heartbeat(ctx context.Context, id domain.SessionID, pool, node string, held []AttemptRef, ttl time.Duration) (HeartbeatResult, error) {
	sid, ok := canonicalUUID(string(id))
	if !ok {
		return HeartbeatResult{}, domain.ErrNotFound
	}
	jobIDs, attemptIDs := refArrays(held)
	var res HeartbeatResult
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE worker_sessions SET heartbeat_at = now(), lease_expires_at = now() + make_interval(secs => $2),
			    renewed_by = NULLIF($4, '')
			WHERE id = $1 AND state = 'ACTIVE' AND ($3 = '' OR pool = $3)
			RETURNING draining`, sid, ttl.Seconds(), pool, node).Scan(&res.Drain)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		if res.Stale, err = collectAttemptIDs(tx.Query(ctx, `
			SELECT r.attempt_id FROM unnest($1::uuid[], $2::uuid[]) AS r (job_id, attempt_id)
			WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = r.job_id AND j.state = 'RUNNING'
			    AND j.current_attempt_id = r.attempt_id AND j.current_session_id = $3)`, jobIDs, attemptIDs, sid)); err != nil {
			return err
		}
		if res.Cancel, err = collectAttemptIDs(tx.Query(ctx, `
			SELECT current_attempt_id FROM jobs
			WHERE current_session_id = $1 AND state = 'RUNNING' AND cancel_requested_at IS NOT NULL`, sid)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, releaseSQL+`
			WHERE current_session_id = $1 AND state = 'RUNNING' AND attempt_started_at < now() - make_interval(secs => $3)
			  AND NOT (current_attempt_id = ANY ($2::uuid[]))`, sid, attemptIDs, unreportedGrace.Seconds())
		return err
	})
	return res, err
}

// CloseSession ends a drained session. Attempts it still holds are recorded as lost, since
// they may have run. A non-empty pool limits it to that pool's sessions (ADR-025).
func (s *Store) CloseSession(ctx context.Context, id domain.SessionID, pool string) error {
	sid, ok := canonicalUUID(string(id))
	if !ok {
		return domain.ErrNotFound
	}
	tag, err := s.pool.Exec(ctx, `UPDATE worker_sessions SET state = 'CLOSED', closed_at = now()
		WHERE id = $1 AND state = 'ACTIVE' AND ($2 = '' OR pool = $2)`, sid, pool)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	_, err = s.endAttempts(ctx, domain.AttemptEnd{State: domain.AttemptLost}, domain.ActorDispatcher,
		"worker deregistered while holding the attempt",
		`SELECT id, current_attempt_id, attempt_count FROM jobs WHERE current_session_id = $1 AND state = 'RUNNING'`, sid)
	return err
}

// ReadyPriorities reports which priority classes of a pool have claimable jobs.
func (s *Store) ReadyPriorities(ctx context.Context, pool string) (map[domain.Priority]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p FROM unnest('{1,2,3,4}'::smallint[]) AS p
		WHERE NOT EXISTS (SELECT 1 FROM pools WHERE name = $1 AND paused)
		  AND EXISTS (SELECT 1 FROM jobs WHERE state = 'READY' AND pool = $1 AND priority = p
		    AND (start_deadline IS NULL OR start_deadline > now() OR attempt_count > 0)
		    AND `+notHeldSQL+`)`, pool)
	if err != nil {
		return nil, err
	}
	classes, err := pgx.CollectRows(rows, pgx.RowTo[int16])
	has := make(map[domain.Priority]bool, len(classes))
	for _, c := range classes {
		has[domain.Priority(c)] = true
	}
	return has, err
}

// TenantCaps returns the running-job caps of tenants that have one.
func (s *Store) TenantCaps(ctx context.Context) (map[domain.TenantID]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, max_running FROM tenants WHERE max_running IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	caps := map[domain.TenantID]int{}
	var (
		id  string
		max int
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &max}, func() error {
		caps[domain.TenantID(id)] = max
		return nil
	})
	return caps, err
}

// RunningCounts counts the running jobs of the given tenants in one pool; caps apply per pool.
func (s *Store) RunningCounts(ctx context.Context, pool string, tenants []domain.TenantID) (map[domain.TenantID]int, error) {
	counts := make(map[domain.TenantID]int, len(tenants))
	if len(tenants) == 0 {
		return counts, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id::text, count(*) FROM jobs
		WHERE state = 'RUNNING' AND pool = $2 AND tenant_id = ANY ($1::uuid[]) GROUP BY tenant_id`, uuidArray(tenants), pool)
	if err != nil {
		return nil, err
	}
	var (
		id string
		n  int
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &n}, func() error {
		counts[domain.TenantID(id)] = n
		return nil
	})
	return counts, err
}

// releaseSQL returns claimed jobs to READY (T23) and refunds the attempt to the retry
// budget; attempt_count is kept so fencing tokens stay monotonic. Callers append a WHERE clause.
const releaseSQL = `
UPDATE jobs SET state = 'READY', budget_attempts = budget_attempts - 1,
    budget_started_at = CASE WHEN budget_attempts = 1 THEN NULL ELSE budget_started_at END,
    current_attempt_id = NULL, current_session_id = NULL, attempt_started_at = NULL, attempt_deadline = NULL,
    ready_at = now(), updated_at = now()`

// ReleaseAttempts returns jobs whose assignment was never delivered to READY, if the given
// attempts are still current (LLD §12.3).
func (s *Store) ReleaseAttempts(ctx context.Context, refs []AttemptRef) (int, error) {
	if err := domain.ValidateJobTransition(domain.StateRunning, domain.StateReady, domain.ActorDispatcher); err != nil {
		return 0, err
	}
	if len(refs) == 0 {
		return 0, nil
	}
	jobIDs, attemptIDs := refArrays(refs)
	tag, err := s.pool.Exec(ctx, releaseSQL+`
		WHERE state = 'RUNNING' AND (id, current_attempt_id) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))`,
		jobIDs, attemptIDs)
	return int(tag.RowsAffected()), err
}

func refArrays(refs []AttemptRef) ([]pgtype.UUID, []pgtype.UUID) {
	jobs, attempts := make([]pgtype.UUID, 0, len(refs)), make([]pgtype.UUID, 0, len(refs))
	for _, r := range refs {
		j, err1 := uuid.Parse(string(r.JobID))
		a, err2 := uuid.Parse(string(r.AttemptID))
		if err1 == nil && err2 == nil {
			jobs = append(jobs, pgtype.UUID{Bytes: j, Valid: true})
			attempts = append(attempts, pgtype.UUID{Bytes: a, Valid: true})
		}
	}
	return jobs, attempts
}

func uuidArray[T ~string](ids []T) []pgtype.UUID {
	out := make([]pgtype.UUID, 0, len(ids))
	for _, id := range ids {
		if u, err := uuid.Parse(string(id)); err == nil {
			out = append(out, pgtype.UUID{Bytes: u, Valid: true})
		}
	}
	return out
}

func collectAttemptIDs(rows pgx.Rows, err error) ([]domain.AttemptID, error) {
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[pgtype.UUID])
	out := make([]domain.AttemptID, len(ids))
	for i, id := range ids {
		out[i] = domain.AttemptID(uuidString(id))
	}
	return out, err
}
