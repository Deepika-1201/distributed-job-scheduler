package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// updateActive locks one of the tenant's active jobs and runs the UPDATE chosen by plan,
// recording the action in audit_log. plan returns "" for a no-op. A finished job is
// returned with an ErrInvalidTransition so callers can report its state.
func (s *Store) updateActive(ctx context.Context, tenantID domain.TenantID, id domain.JobID, audit Audit,
	action string, plan func(domain.Job) (string, error)) (domain.Job, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	jobID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Job{}, domain.ErrNotFound
	}
	var res domain.Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		job, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+activeColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2 FOR UPDATE`, jobID, tenant))
		if errors.Is(err, pgx.ErrNoRows) {
			return finishedConflict(ctx, tx, tenant, jobID, &res)
		}
		if err != nil {
			return err
		}
		res = job
		sql, err := plan(job)
		if err != nil || sql == "" {
			return err
		}
		if res, err = scanJob(tx.QueryRow(ctx, sql+activeColumns, jobID)); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, action, jobID, map[string]any{"from": job.State, "to": res.State})
	})
	return res, err
}

// finishedConflict reports a finished job as an invalid transition, or ErrNotFound.
func finishedConflict(ctx context.Context, tx pgx.Tx, tenant, jobID string, res *domain.Job) error {
	job, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+historyColumns+` FROM job_history WHERE id = $1 AND tenant_id = $2`, jobID, tenant))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return err
	}
	*res = job
	return fmt.Errorf("%w: job is %s", domain.ErrInvalidTransition, job.State)
}

// PauseJob holds a job that hasn't started (T11, T12).
func (s *Store) PauseJob(ctx context.Context, tenant domain.TenantID, id domain.JobID, audit Audit) (domain.Job, error) {
	return s.updateActive(ctx, tenant, id, audit, "job.pause", func(j domain.Job) (string, error) {
		if err := domain.ValidateJobTransition(j.State, domain.StatePaused, domain.ActorAPI); err != nil {
			return "", err
		}
		return `UPDATE jobs SET state = 'PAUSED', ready_at = NULL, updated_at = now() WHERE id = $1 RETURNING `, nil
	})
}

// ResumeJob releases a paused job; the promoter makes it READY once due (T13).
func (s *Store) ResumeJob(ctx context.Context, tenant domain.TenantID, id domain.JobID, audit Audit) (domain.Job, error) {
	return s.updateActive(ctx, tenant, id, audit, "job.resume", func(j domain.Job) (string, error) {
		if err := domain.ValidateJobTransition(j.State, domain.StateScheduled, domain.ActorAPI); err != nil {
			return "", err
		}
		return `UPDATE jobs SET state = 'SCHEDULED', updated_at = now() WHERE id = $1 RETURNING `, nil
	})
}

// RunJobNow makes a delayed or paused job due immediately; a READY job is left unchanged.
func (s *Store) RunJobNow(ctx context.Context, tenant domain.TenantID, id domain.JobID, audit Audit) (domain.Job, error) {
	return s.updateActive(ctx, tenant, id, audit, "job.run_now", func(j domain.Job) (string, error) {
		switch j.State {
		case domain.StateReady:
			return "", nil
		case domain.StateScheduled:
			return `UPDATE jobs SET run_at = now(), updated_at = now() WHERE id = $1 RETURNING `, nil
		case domain.StatePaused:
			return `UPDATE jobs SET state = 'SCHEDULED', run_at = now(), updated_at = now() WHERE id = $1 RETURNING `, nil
		}
		return "", fmt.Errorf("%w: cannot run a %s job now", domain.ErrInvalidTransition, j.State)
	})
}

var retryFromHistorySQL = func() string {
	overrides := map[string]string{
		"state":               "'READY'",
		"run_at":              "now()",
		"ready_at":            "now()",
		"budget_attempts":     "0",
		"budget_lost":         "0",
		"budget_started_at":   "NULL::timestamptz",
		"cancel_requested_at": "NULL::timestamptz",
		"updated_at":          "now()",
	}
	values := make([]string, len(jobColumns))
	for i, c := range jobColumns {
		values[i] = c
		if o, ok := overrides[c]; ok {
			values[i] = o
		}
	}
	return `WITH moved AS (DELETE FROM job_history WHERE id = $1 AND tenant_id = $2 AND state = $3 RETURNING *)
INSERT INTO jobs (` + strings.Join(jobColumns, ", ") + `)
SELECT ` + strings.Join(values, ", ") + ` FROM moved
RETURNING ` + activeColumns
}()

// RetryJob re-queues a FAILED or DEAD_LETTERED job with a fresh retry budget (T21, T22).
// Its attempt count is kept so fencing tokens keep increasing.
func (s *Store) RetryJob(ctx context.Context, tenantID domain.TenantID, id domain.JobID, audit Audit) (domain.Job, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	jobID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Job{}, domain.ErrNotFound
	}
	var res domain.Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		job, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+historyColumns+` FROM job_history WHERE id = $1 AND tenant_id = $2 FOR UPDATE`, jobID, tenant))
		if errors.Is(err, pgx.ErrNoRows) {
			active, err := scanJob(tx.QueryRow(ctx,
				`SELECT `+activeColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2`, jobID, tenant))
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrNotFound
			}
			if err != nil {
				return err
			}
			res = active
			return fmt.Errorf("%w: job is %s", domain.ErrInvalidTransition, active.State)
		}
		if err != nil {
			return err
		}
		res = job
		if err := domain.ValidateJobTransition(job.State, domain.StateReady, domain.ActorAPI); err != nil {
			return err
		}
		requeued, err := scanJob(tx.QueryRow(ctx, retryFromHistorySQL, jobID, tenant, string(job.State)))
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: an active job already uses dedupe key %q", domain.ErrConflict, job.DedupeKey)
		}
		if err != nil {
			return err
		}
		res = requeued
		return writeAudit(ctx, tx, tenant, audit, "job.retry", jobID, map[string]any{"from": job.State})
	})
	return res, err
}

// JobFilter selects jobs to list; zero fields don't filter.
type JobFilter struct {
	State      domain.JobState
	Type       string
	ScheduleID domain.ScheduleID
	LabelKey   string
	LabelValue string
	Limit      int
	After      *JobCursor
}

// JobCursor marks the last job of a page in (created_at, id) descending order.
type JobCursor struct {
	CreatedAt time.Time
	ID        domain.JobID
}

const listWhere = ` WHERE tenant_id = $1
  AND ($2::text IS NULL OR state = $2)
  AND ($3::text IS NULL OR job_type = $3)
  AND ($4::jsonb IS NULL OR labels @> $4)
  AND ($5::timestamptz IS NULL OR (created_at, id) < ($5, $6::uuid))
  AND ($8::uuid IS NULL OR schedule_id = $8)
ORDER BY created_at DESC, id DESC
LIMIT $7`

// ListJobs returns a page of the tenant's jobs, newest first, merging active and finished
// jobs (LLD §9.7), and the cursor for the next page, if any.
func (s *Store) ListJobs(ctx context.Context, tenantID domain.TenantID, f JobFilter) ([]domain.Job, *JobCursor, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return nil, nil, domain.ErrNotFound
	}
	var label any
	if f.LabelKey != "" {
		label = map[string]string{f.LabelKey: f.LabelValue}
	}
	var afterTime pgtype.Timestamptz
	var afterID *string
	if f.After != nil {
		id, ok := canonicalUUID(string(f.After.ID))
		if !ok {
			return nil, nil, fmt.Errorf("invalid cursor id %q", f.After.ID)
		}
		afterTime, afterID = pgtype.Timestamptz{Time: f.After.CreatedAt, Valid: true}, &id
	}
	var schedule *string
	if f.ScheduleID != "" {
		id, ok := canonicalUUID(string(f.ScheduleID))
		if !ok {
			return nil, nil, nil
		}
		schedule = &id
	}
	args := []any{tenant, nullText(string(f.State)), nullText(f.Type), label, afterTime, afterID, f.Limit + 1, schedule}

	var jobs []domain.Job
	for _, src := range []struct {
		query    string
		terminal bool
	}{
		{`SELECT ` + activeColumns + ` FROM jobs` + listWhere, false},
		{`SELECT ` + historyColumns + ` FROM job_history` + listWhere, true},
	} {
		if f.State != "" && f.State.Terminal() != src.terminal {
			continue
		}
		rows, err := s.pool.Query(ctx, src.query, args...)
		if err != nil {
			return nil, nil, err
		}
		page, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Job, error) { return scanJob(r) })
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, page...)
	}
	slices.SortFunc(jobs, func(a, b domain.Job) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(string(b.ID), string(a.ID))
	})
	if len(jobs) <= f.Limit {
		return jobs, nil, nil
	}
	last := jobs[f.Limit-1]
	return jobs[:f.Limit], &JobCursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// ListAttempts returns a job's attempts in order, including the running one.
func (s *Store) ListAttempts(ctx context.Context, tenantID domain.TenantID, id domain.JobID) ([]domain.Attempt, error) {
	job, err := s.GetJob(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, number, session_id, state, retryable, error, started_at, deadline, finished_at, actor
		FROM attempts WHERE job_id = $1 AND tenant_id = $2 ORDER BY number`, string(job.ID), string(job.TenantID))
	if err != nil {
		return nil, err
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Attempt, error) {
		var (
			a                 domain.Attempt
			attemptID, sessID pgtype.UUID
			state, actor      string
			errText           pgtype.Text
		)
		err := r.Scan(&attemptID, &a.Number, &sessID, &state, &a.Retryable, &errText, &a.StartedAt, &a.Deadline, &a.FinishedAt, &actor)
		a.ID, a.SessionID = domain.AttemptID(uuidString(attemptID)), domain.SessionID(uuidString(sessID))
		a.JobID, a.TenantID = job.ID, job.TenantID
		a.State, a.Actor, a.Error = domain.AttemptState(state), domain.Actor(actor), errText.String
		return a, err
	})
	if err != nil {
		return nil, err
	}
	if c := job.Current; c != nil {
		attempts = append(attempts, domain.Attempt{
			ID: c.ID, JobID: job.ID, TenantID: job.TenantID, Number: c.Number, SessionID: c.SessionID,
			State: domain.AttemptRunning, StartedAt: c.StartedAt, Deadline: c.Deadline,
		})
	}
	return attempts, nil
}
