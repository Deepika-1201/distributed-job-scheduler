package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// NewJob is a fully resolved submission: job-type defaults and overrides are already applied.
type NewJob struct {
	TenantID       domain.TenantID
	Type           string
	TypeVersion    int
	Pool           string
	Priority       domain.Priority
	Payload        []byte
	Labels         map[string]string
	DedupeKey      string
	CorrelationID  string
	CreatedBy      string
	RequestID      string
	RunAt          time.Time // zero means now
	StartDeadline  time.Time
	Deadline       time.Time
	AttemptTimeout time.Duration
	RetryPolicy    domain.RetryPolicy
	AtMostOnce     bool
}

// Idempotency identifies a client request so that retries of it are replayed (LLD §8.4).
type Idempotency struct {
	Key         string
	RequestHash []byte
}

type SubmitOutcome string

const (
	Created      SubmitOutcome = "created"
	Replayed     SubmitOutcome = "replayed"
	Deduplicated SubmitOutcome = "deduplicated"
)

type SubmitResult struct {
	Job     domain.Job
	Outcome SubmitOutcome
}

const opCreateJob = "job.create"

var insertJobSQL = `
INSERT INTO jobs (id, tenant_id, job_type, job_type_version, pool, state, priority, payload, labels,
    dedupe_key, correlation_id, created_by, request_id, run_at, start_deadline, deadline,
    attempt_timeout_ms, retry_policy, at_most_once, created_at, ready_at, updated_at)
SELECT $1::uuid, $2::uuid, $3::text, $4::integer, $5::text,
    CASE WHEN r.run_at <= now() THEN 'READY' ELSE 'SCHEDULED' END,
    $6::smallint, $7::json, $8::jsonb, $9::text, $10::text, $11::text, $12::text, r.run_at,
    $14::timestamptz, $15::timestamptz, $16::bigint, $17::jsonb, $18::boolean,
    now(), CASE WHEN r.run_at <= now() THEN now() END, now()
FROM (SELECT COALESCE($13::timestamptz, now()) AS run_at) AS r
ON CONFLICT (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
RETURNING ` + activeColumns

// SubmitJob creates a job, or returns the job an earlier request with the same idempotency
// key created, or the active job with the same dedupe key.
func (s *Store) SubmitJob(ctx context.Context, nj NewJob, idem *Idempotency) (SubmitResult, error) {
	tenant, ok := canonicalUUID(string(nj.TenantID))
	if !ok {
		return SubmitResult{}, fmt.Errorf("invalid tenant id %q", nj.TenantID)
	}
	newID, err := uuid.NewV7()
	if err != nil {
		return SubmitResult{}, err
	}

	var res SubmitResult
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if idem != nil {
			earlier, err := s.claimIdempotencyKey(ctx, tx, tenant, idem, newID.String())
			if err != nil || earlier != nil {
				res = SubmitResult{Job: deref(earlier), Outcome: Replayed}
				return err
			}
		}
		job, outcome, err := insertJob(ctx, tx, tenant, newID.String(), nj)
		if err != nil {
			return err
		}
		if idem != nil && outcome == Deduplicated {
			if _, err := tx.Exec(ctx,
				`UPDATE idempotency_keys SET resource_id = $4 WHERE tenant_id = $1 AND operation = $2 AND key = $3`,
				tenant, opCreateJob, idem.Key, string(job.ID)); err != nil {
				return err
			}
		}
		res = SubmitResult{Job: job, Outcome: outcome}
		return nil
	})
	if idem != nil && isLockTimeout(err) {
		return SubmitResult{}, domain.ErrIdempotencyInProgress
	}
	if err != nil {
		return SubmitResult{}, err
	}
	return res, nil
}

// claimIdempotencyKey records the key for this request, or returns the job created by an
// earlier request with the same key and request hash.
func (s *Store) claimIdempotencyKey(ctx context.Context, tx pgx.Tx, tenant string, idem *Idempotency, newID string) (*domain.Job, error) {
	ttl := s.idempotencyTTL.Seconds()
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, operation, key, request_hash, resource_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
		ON CONFLICT (tenant_id, operation, key) DO NOTHING`,
		tenant, opCreateJob, idem.Key, idem.RequestHash, newID, ttl)
	if err != nil || tag.RowsAffected() == 1 {
		return nil, err
	}

	var (
		hash     []byte
		resource string
		expired  bool
	)
	if err := tx.QueryRow(ctx, `
		SELECT request_hash, resource_id::text, expires_at <= now() FROM idempotency_keys
		WHERE tenant_id = $1 AND operation = $2 AND key = $3 FOR UPDATE`,
		tenant, opCreateJob, idem.Key).Scan(&hash, &resource, &expired); err != nil {
		return nil, err
	}
	switch {
	case expired:
		_, err := tx.Exec(ctx, `
			UPDATE idempotency_keys
			SET request_hash = $4, resource_id = $5, created_at = now(), expires_at = now() + make_interval(secs => $6)
			WHERE tenant_id = $1 AND operation = $2 AND key = $3`,
			tenant, opCreateJob, idem.Key, idem.RequestHash, newID, ttl)
		return nil, err
	case !bytes.Equal(hash, idem.RequestHash):
		return nil, domain.ErrIdempotencyKeyReused
	}
	job, err := getJobByID(ctx, tx, resource)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func insertJob(ctx context.Context, tx pgx.Tx, tenant, id string, nj NewJob) (domain.Job, SubmitOutcome, error) {
	labels := nj.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	args := []any{
		id, tenant, nj.Type, nj.TypeVersion, nj.Pool, int16(nj.Priority), nj.Payload, labels,
		nullText(nj.DedupeKey), nullText(nj.CorrelationID), nj.CreatedBy, nullText(nj.RequestID),
		nullTime(nj.RunAt), nullTime(nj.StartDeadline), nullTime(nj.Deadline),
		nj.AttemptTimeout.Milliseconds(), policyToJSON(nj.RetryPolicy), nj.AtMostOnce,
	}
	for range 3 {
		job, err := scanJob(tx.QueryRow(ctx, insertJobSQL, args...))
		if err == nil {
			return job, Created, domain.ValidateJobTransition(domain.StateNew, job.State, domain.ActorAPI)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, "", err
		}
		existing, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+activeColumns+` FROM jobs WHERE tenant_id = $1 AND dedupe_key = $2`, tenant, nj.DedupeKey))
		if err == nil {
			return existing, Deduplicated, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, "", err
		}
		// The conflicting job finished between the insert and the lookup; try again.
	}
	return domain.Job{}, "", fmt.Errorf("dedupe key %q kept conflicting with jobs that were finishing", nj.DedupeKey)
}

// GetJob returns a job, active or finished. Another tenant's job is reported as not found.
func (s *Store) GetJob(ctx context.Context, tenantID domain.TenantID, id domain.JobID) (domain.Job, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	jobID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Job{}, domain.ErrNotFound
	}
	job, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT `+activeColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2
		UNION ALL
		SELECT `+historyColumns+` FROM job_history WHERE id = $1 AND tenant_id = $2
		LIMIT 1`, jobID, tenant))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	return job, err
}

// getJobByID reads a job regardless of tenant, for internal paths that already hold its ID.
func getJobByID(ctx context.Context, tx pgx.Tx, id string) (domain.Job, error) {
	job, err := scanJob(tx.QueryRow(ctx, `
		SELECT `+activeColumns+` FROM jobs WHERE id = $1
		UNION ALL
		SELECT `+historyColumns+` FROM job_history WHERE id = $1
		LIMIT 1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	return job, err
}

// ClaimRequest asks for up to Limit ready jobs from one pool and priority class.
type ClaimRequest struct {
	Pool        string
	Priority    domain.Priority
	Limit       int
	SessionID   domain.SessionID
	SkipTenants []domain.TenantID // tenants at their concurrency cap
}

const claimSQL = `
WITH picked AS (
    SELECT id FROM jobs
    WHERE state = 'READY' AND pool = $1 AND priority = $2
      AND NOT (tenant_id = ANY ($3::uuid[]))
      AND (start_deadline IS NULL OR start_deadline > now() OR attempt_count > 0)
    ORDER BY run_at, id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs AS j SET
    state = 'RUNNING',
    attempt_count = j.attempt_count + 1,
    budget_attempts = j.budget_attempts + 1,
    budget_started_at = COALESCE(j.budget_started_at, now()),
    current_attempt_id = gen_random_uuid(),
    current_session_id = $5,
    attempt_started_at = now(),
    attempt_deadline = now() + j.attempt_timeout_ms * interval '1 millisecond',
    updated_at = now()
FROM picked
WHERE j.id = picked.id
RETURNING `

// ClaimReady moves up to Limit READY jobs to RUNNING, each under a new attempt. Rows locked
// by concurrent claimers or cancellations are skipped, so no job is claimed twice.
func (s *Store) ClaimReady(ctx context.Context, req ClaimRequest) ([]domain.Job, error) {
	if err := domain.ValidateJobTransition(domain.StateReady, domain.StateRunning, domain.ActorDispatcher); err != nil {
		return nil, err
	}
	session, ok := canonicalUUID(string(req.SessionID))
	if !ok {
		return nil, fmt.Errorf("invalid session id %q", req.SessionID)
	}
	skip := make([]pgtype.UUID, 0, len(req.SkipTenants))
	for _, t := range req.SkipTenants {
		if id, err := uuid.Parse(string(t)); err == nil {
			skip = append(skip, pgtype.UUID{Bytes: id, Valid: true})
		}
	}
	rows, err := s.pool.Query(ctx, claimSQL+activeColumnsJ, req.Pool, int16(req.Priority), skip, req.Limit, session)
	if err != nil {
		return nil, err
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Job, error) { return scanJob(r) })
	if err != nil {
		return nil, err
	}
	slices.SortFunc(jobs, func(a, b domain.Job) int {
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return strings.Compare(string(a.ID), string(b.ID))
	})
	return jobs, nil
}

// Completion reports how an attempt ended. Number is the attempt's fencing token.
type Completion struct {
	JobID     domain.JobID
	AttemptID domain.AttemptID
	Number    int
	End       domain.AttemptEnd
	Error     string
	Result    []byte // JSON; stored only on success
	Actor     domain.Actor
}

type CompletionResult struct {
	Job      domain.Job
	Decision domain.Decision // zero for a replay
	Replayed bool
}

const retrySQL = `
UPDATE jobs SET state = 'RETRY_PENDING', run_at = $2, budget_lost = $3, last_error = COALESCE($4, last_error),
    current_attempt_id = NULL, current_session_id = NULL, attempt_started_at = NULL, attempt_deadline = NULL,
    ready_at = NULL, updated_at = now()
WHERE id = $1 AND state = 'RUNNING'
RETURNING `

// CompleteAttempt records an attempt's outcome and applies the retry decision, but only if
// the attempt still holds the job's lease (LLD §8.3).
func (s *Store) CompleteAttempt(ctx context.Context, c Completion) (CompletionResult, error) {
	if err := domain.ValidateAttemptTransition(domain.AttemptRunning, c.End.State, c.Actor); err != nil {
		return CompletionResult{}, err
	}
	jobID, ok := canonicalUUID(string(c.JobID))
	if !ok {
		return CompletionResult{}, domain.ErrNotFound
	}
	attemptID, ok := canonicalUUID(string(c.AttemptID))
	if !ok {
		return CompletionResult{}, domain.ErrStaleAttempt
	}

	var res CompletionResult
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var now time.Time
		job, err := scanJob(tx.QueryRow(ctx, `SELECT `+activeColumns+`, now() FROM jobs WHERE id = $1 FOR UPDATE`, jobID), &now)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err != nil || !holdsLease(job, c.Number, attemptID) {
			res, err = resolveUnmatchedCompletion(ctx, tx, jobID, attemptID, c)
			return err
		}

		budget := job.Budget
		if c.End.State == domain.AttemptLost {
			budget.Lost++
		}
		decision, err := domain.DecideAfterAttempt(domain.DecisionInput{
			End:             c.End,
			Policy:          job.RetryPolicy,
			Budget:          budget,
			Deadline:        job.Deadline,
			CancelRequested: !job.CancelRequestedAt.IsZero(),
			AtMostOnce:      job.AtMostOnce,
			Now:             now,
		}, s.rnd)
		if err != nil {
			return err
		}
		if err := domain.ValidateJobTransition(domain.StateRunning, decision.Next, c.Actor); err != nil {
			return err
		}
		if err := insertAttempt(ctx, tx, job, c); err != nil {
			return err
		}

		var updated domain.Job
		if decision.Next == domain.StateRetryPending {
			updated, err = scanJob(tx.QueryRow(ctx, retrySQL+activeColumns, jobID, decision.RunAt, budget.Lost, nullText(c.Error)))
		} else {
			t := terminal{State: decision.Next, Reason: decision.Reason, Error: c.Error, BudgetLost: budget.Lost}
			if decision.Next == domain.StateSucceeded {
				t.Result = c.Result
			}
			updated, err = moveToHistory(ctx, tx, jobID, domain.StateRunning, t)
		}
		res = CompletionResult{Job: updated, Decision: decision}
		return err
	})
	return res, err
}

func holdsLease(j domain.Job, number int, attemptID string) bool {
	return j.State == domain.StateRunning && j.Current != nil &&
		j.Current.Number == number && string(j.Current.ID) == attemptID
}

// resolveUnmatchedCompletion handles a completion whose attempt no longer holds the lease: a
// repeat of an already recorded outcome is a replay; anything else is stale.
func resolveUnmatchedCompletion(ctx context.Context, tx pgx.Tx, jobID, attemptID string, c Completion) (CompletionResult, error) {
	var recorded string
	err := tx.QueryRow(ctx, `SELECT state FROM attempts WHERE job_id = $1 AND number = $2 AND id = $3`,
		jobID, c.Number, attemptID).Scan(&recorded)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return CompletionResult{}, err
	}
	job, jobErr := getJobByID(ctx, tx, jobID)
	switch {
	case jobErr != nil:
		return CompletionResult{}, jobErr
	case err == nil && domain.AttemptState(recorded) == c.End.State:
		return CompletionResult{Job: job, Replayed: true}, nil
	default:
		return CompletionResult{}, domain.ErrStaleAttempt
	}
}

func insertAttempt(ctx context.Context, tx pgx.Tx, job domain.Job, c Completion) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO attempts (id, job_id, tenant_id, number, session_id, state, retryable, error,
		    started_at, deadline, finished_at, actor)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), $11)`,
		string(job.Current.ID), string(job.ID), string(job.TenantID), job.Current.Number,
		string(job.Current.SessionID), string(c.End.State), c.End.Retryable, nullText(c.Error),
		job.Current.StartedAt, job.Current.Deadline, string(c.Actor))
	return err
}

// terminal describes the history record written when a job reaches a terminal state.
type terminal struct {
	State      domain.JobState
	Reason     domain.Reason
	Result     []byte
	Error      string // replaces last_error when non-empty
	BudgetLost int
}

var moveToHistorySQL = moveSQL("id = $1 AND state = $2", map[string]string{
	"state":       "$3::text",
	"budget_lost": "$4::integer",
	"last_error":  "COALESCE($5::text, last_error)",
}, "$6::text, $7::json", historyColumns)

// moveSQL builds a statement that deletes the active jobs matching where and inserts them
// into job_history with the overridden columns; tail supplies the reason and result.
func moveSQL(where string, overrides map[string]string, tail, returning string) string {
	values := make([]string, len(jobColumns))
	for i, c := range jobColumns {
		values[i] = c
		if o, ok := overrides[c]; ok {
			values[i] = o
		} else if c == "updated_at" {
			values[i] = "now()"
		}
	}
	return `WITH moved AS (DELETE FROM jobs WHERE ` + where + ` RETURNING *)
INSERT INTO job_history (` + strings.Join(jobColumns, ", ") + `, finished_at, reason, result)
SELECT ` + strings.Join(values, ", ") + `, now(), ` + tail + ` FROM moved
RETURNING ` + returning
}

// moveToHistory deletes the job from jobs if it is still in state from, and records it in
// job_history with the terminal outcome, in one statement. A fixed-delay schedule's next
// fire is set in the same transaction.
func moveToHistory(ctx context.Context, tx pgx.Tx, jobID string, from domain.JobState, t terminal) (domain.Job, error) {
	job, err := scanJob(tx.QueryRow(ctx, moveToHistorySQL, jobID, string(from), string(t.State), t.BudgetLost,
		nullText(t.Error), nullText(string(t.Reason)), t.Result))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, fmt.Errorf("job %s left state %s concurrently", jobID, from)
	}
	if err != nil || job.ScheduleID == "" {
		return job, err
	}
	id, err := uuid.Parse(string(job.ScheduleID))
	if err != nil {
		return job, err
	}
	return job, advanceFixedDelay(ctx, tx, []pgtype.UUID{{Bytes: id, Valid: true}})
}

// RequestCancel cancels a job that hasn't started, or flags a running job so its worker is
// told to stop; the job then ends CANCELLED whatever the attempt's outcome. Cancelling a job
// that is already cancelled returns it unchanged.
func (s *Store) RequestCancel(ctx context.Context, tenantID domain.TenantID, id domain.JobID, audit Audit) (domain.Job, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	jobID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Job{}, domain.ErrNotFound
	}
	var res domain.Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		job, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+activeColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2 FOR UPDATE`, jobID, tenant))
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			res, err = cancelFinished(ctx, tx, tenant, jobID)
			return err
		case err != nil:
			return err
		case job.State == domain.StateRunning:
			res = job
			if !job.CancelRequestedAt.IsZero() {
				return nil
			}
			if res, err = scanJob(tx.QueryRow(ctx,
				`UPDATE jobs SET cancel_requested_at = now(), updated_at = now() WHERE id = $1 RETURNING `+activeColumns, jobID)); err != nil {
				return err
			}
			return writeAudit(ctx, tx, tenant, audit, "job.cancel", jobID, map[string]any{"from": job.State, "cancel_requested": true})
		}
		if err := domain.ValidateJobTransition(job.State, domain.StateCancelled, domain.ActorAPI); err != nil {
			return err
		}
		if res, err = moveToHistory(ctx, tx, jobID, job.State,
			terminal{State: domain.StateCancelled, Reason: domain.ReasonCancelled, BudgetLost: job.Budget.Lost}); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "job.cancel", jobID, map[string]any{"from": job.State, "to": res.State})
	})
	return res, err
}

func cancelFinished(ctx context.Context, tx pgx.Tx, tenant, jobID string) (domain.Job, error) {
	job, err := scanJob(tx.QueryRow(ctx,
		`SELECT `+historyColumns+` FROM job_history WHERE id = $1 AND tenant_id = $2`, jobID, tenant))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.Job{}, domain.ErrNotFound
	case err != nil:
		return domain.Job{}, err
	case job.State == domain.StateCancelled:
		return job, nil
	}
	return job, domain.ValidateJobTransition(job.State, domain.StateCancelled, domain.ActorAPI)
}

// EnsurePartitions creates daily job_history and attempts partitions for days UTC days
// starting at from. Existing partitions are left untouched.
func (s *Store) EnsurePartitions(ctx context.Context, from time.Time, days int) error {
	start := from.UTC().Truncate(24 * time.Hour)
	for i := range days {
		lo := start.AddDate(0, 0, i)
		hi := lo.AddDate(0, 0, 1)
		for _, parent := range []string{"job_history", "attempts"} {
			name := pgx.Identifier{parent + "_p" + lo.Format("20060102")}.Sanitize()
			sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				name, parent, lo.Format(time.RFC3339), hi.Format(time.RFC3339))
			if _, err := s.pool.Exec(ctx, sql); err != nil {
				return fmt.Errorf("create partition %s: %w", name, err)
			}
		}
	}
	return nil
}

func deref(j *domain.Job) domain.Job {
	if j == nil {
		return domain.Job{}
	}
	return *j
}
