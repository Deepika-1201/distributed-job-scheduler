// Package postgres contains PostgreSQL adapters: the connection pool, migrations and the
// job store (LLD §8).
package postgres

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/domain"
)

// Store persists jobs and executes their state transitions with database-enforced guards.
type Store struct {
	pool           *pgxpool.Pool
	rnd            func() float64
	idempotencyTTL time.Duration

	satMu            sync.Mutex
	lastEmptyAcquire int64
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, rnd: rand.Float64, idempotencyTTL: 24 * time.Hour}
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, fn)
}

// isLockTimeout reports whether err is PostgreSQL's lock_not_available (lock_timeout expired).
func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// canonicalUUID normalizes an externally supplied ID; ok is false if it isn't a UUID.
func canonicalUUID(s string) (string, bool) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

// jobColumns are shared by jobs and job_history, in scanJob order.
var jobColumns = []string{
	"id", "tenant_id", "job_type", "job_type_version", "pool", "schedule_id", "fire_time", "state",
	"priority", "payload", "labels", "dedupe_key", "correlation_id", "created_by", "request_id",
	"run_at", "start_deadline", "deadline", "attempt_timeout_ms", "retry_policy", "at_most_once",
	"attempt_count", "budget_attempts", "budget_lost", "budget_started_at", "cancel_requested_at",
	"last_error", "created_at", "ready_at", "updated_at",
}

// activeColumns and historyColumns project both tables onto one column list so scanJob can
// read either; activeColumnsJ qualifies the list for statements that alias jobs as j.
var (
	activeColumns  = activeSelect("")
	activeColumnsJ = activeSelect("j.")
	historyColumns = strings.Join(jobColumns, ", ") +
		", NULL::uuid, NULL::uuid, NULL::timestamptz, NULL::timestamptz, finished_at, reason, result"
)

func activeSelect(prefix string) string {
	cols := make([]string, 0, len(jobColumns)+4)
	for _, c := range append(jobColumns, "current_attempt_id", "current_session_id", "attempt_started_at", "attempt_deadline") {
		cols = append(cols, prefix+c)
	}
	return strings.Join(cols, ", ") + ", NULL::timestamptz, NULL::text, NULL::json"
}

type retryPolicyJSON struct {
	Strategy           domain.BackoffStrategy `json:"strategy"`
	InitialDelayMS     int64                  `json:"initial_delay_ms"`
	Multiplier         float64                `json:"multiplier"`
	MaxDelayMS         int64                  `json:"max_delay_ms"`
	Jitter             domain.Jitter          `json:"jitter"`
	MaxAttempts        int                    `json:"max_attempts"`
	MaxRetryDurationMS int64                  `json:"max_retry_duration_ms"`
	MaxLostAttempts    int                    `json:"max_lost_attempts"`
}

func policyToJSON(p domain.RetryPolicy) retryPolicyJSON {
	return retryPolicyJSON{
		Strategy:           p.Strategy,
		InitialDelayMS:     p.InitialDelay.Milliseconds(),
		Multiplier:         p.Multiplier,
		MaxDelayMS:         p.MaxDelay.Milliseconds(),
		Jitter:             p.Jitter,
		MaxAttempts:        p.MaxAttempts,
		MaxRetryDurationMS: p.MaxRetryDuration.Milliseconds(),
		MaxLostAttempts:    p.MaxLostAttempts,
	}
}

func (p retryPolicyJSON) policy() domain.RetryPolicy {
	return domain.RetryPolicy{
		Strategy:         p.Strategy,
		InitialDelay:     time.Duration(p.InitialDelayMS) * time.Millisecond,
		Multiplier:       p.Multiplier,
		MaxDelay:         time.Duration(p.MaxDelayMS) * time.Millisecond,
		Jitter:           p.Jitter,
		MaxAttempts:      p.MaxAttempts,
		MaxRetryDuration: time.Duration(p.MaxRetryDurationMS) * time.Millisecond,
		MaxLostAttempts:  p.MaxLostAttempts,
	}
}

// scanJob reads a row produced by activeColumns or historyColumns, followed by extra.
func scanJob(row pgx.Row, extra ...any) (domain.Job, error) {
	var (
		j                                         domain.Job
		id, tenantID, scheduleID                  pgtype.UUID
		attemptID, sessionID                      pgtype.UUID
		fireTime, startDeadline, deadline         pgtype.Timestamptz
		budgetStarted, cancelRequested, readyAt   pgtype.Timestamptz
		attemptStarted, attemptDeadline, finished pgtype.Timestamptz
		dedupeKey, correlationID, requestID       pgtype.Text
		lastError, reason                         pgtype.Text
		state                                     string
		priority                                  int16
		timeoutMS                                 int64
		policy                                    retryPolicyJSON
	)
	dest := []any{
		&id, &tenantID, &j.Type, &j.TypeVersion, &j.Pool, &scheduleID, &fireTime, &state,
		&priority, &j.Payload, &j.Labels, &dedupeKey, &correlationID, &j.CreatedBy, &requestID,
		&j.RunAt, &startDeadline, &deadline, &timeoutMS, &policy, &j.AtMostOnce,
		&j.AttemptCount, &j.Budget.Attempts, &j.Budget.Lost, &budgetStarted, &cancelRequested,
		&lastError, &j.CreatedAt, &readyAt, &j.UpdatedAt,
		&attemptID, &sessionID, &attemptStarted, &attemptDeadline, &finished, &reason, &j.Result,
	}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return domain.Job{}, err
	}

	j.ID = domain.JobID(uuidString(id))
	j.TenantID = domain.TenantID(uuidString(tenantID))
	j.ScheduleID = domain.ScheduleID(uuidString(scheduleID))
	j.FireTime = fireTime.Time
	j.State = domain.JobState(state)
	j.Priority = domain.Priority(priority)
	j.DedupeKey = dedupeKey.String
	j.CorrelationID = correlationID.String
	j.RequestID = requestID.String
	j.StartDeadline = startDeadline.Time
	j.Deadline = deadline.Time
	j.AttemptTimeout = time.Duration(timeoutMS) * time.Millisecond
	j.RetryPolicy = policy.policy()
	j.Budget.StartedAt = budgetStarted.Time
	j.CancelRequestedAt = cancelRequested.Time
	j.LastError = lastError.String
	j.ReadyAt = readyAt.Time
	j.FinishedAt = finished.Time
	j.Reason = domain.Reason(reason.String)
	if attemptID.Valid {
		j.Current = &domain.RunningAttempt{
			ID:        domain.AttemptID(uuidString(attemptID)),
			Number:    j.AttemptCount,
			SessionID: domain.SessionID(uuidString(sessionID)),
			StartedAt: attemptStarted.Time,
			Deadline:  attemptDeadline.Time,
		}
	}
	return j, nil
}

func uuidString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	return uuid.UUID(u.Bytes).String()
}

func nullText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

func nullTime(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()} }
