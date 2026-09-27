package postgres

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
)

// committed holds, per open transaction, the metric updates to apply once it commits, so
// work that is rolled back is never counted.
var committed sync.Map // pgx.Tx → *[]func()

// onCommit runs fn after tx, opened by inTx, commits.
func onCommit(tx pgx.Tx, fn func()) {
	v, _ := committed.LoadOrStore(tx, new([]func()))
	fns := v.(*[]func())
	*fns = append(*fns, fn)
}

// inTx runs fn in a transaction, timed as db_transaction_duration_seconds and traced when
// ctx already carries a span (LLD §17).
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return s.inTxWith(ctx, pgx.TxOptions{}, fn)
}

// inSnapshot runs fn in a read-only transaction that sees a single snapshot of the database.
func (s *Store) inSnapshot(ctx context.Context, fn func(pgx.Tx) error) error {
	return s.inTxWith(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (s *Store) inTxWith(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) (err error) {
	op := txOperation()
	start := time.Now()
	if trace.SpanContextFromContext(ctx).IsValid() {
		var span trace.Span
		ctx, span = observability.Tracer().Start(ctx, "db.tx "+op, trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("db.system.name", "postgresql")))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}
	var opened pgx.Tx
	err = pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		opened = tx
		return fn(tx)
	})
	if opened != nil {
		if v, ok := committed.LoadAndDelete(opened); ok && err == nil {
			for _, f := range *v.(*[]func()) {
				f()
			}
		}
	}
	observability.DBTransaction.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op)))
	return err
}

// txOperation names a transaction after the exported Store method that opened it, which
// keeps the label set bounded and meaningful.
func txOperation() string {
	pcs := make([]uintptr, 8)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)]) // skip Callers, txOperation, inTxWith
	for {
		f, more := frames.Next()
		if _, method, ok := strings.Cut(f.Function, ".(*Store)."); ok {
			method, _, _ = strings.Cut(method, ".") // closures are Method.funcN
			if method != "" && method[0] >= 'A' && method[0] <= 'Z' {
				return method
			}
		}
		if !more {
			return "other"
		}
	}
}

// countCompleted counts a job that reached a terminal state, once tx commits.
func countCompleted(tx pgx.Tx, jobType string, state domain.JobState) {
	onCommit(tx, func() {
		observability.JobsCompleted.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("type", jobType), attribute.String("state", string(state))))
	})
}

type movedRows struct {
	rows      int
	schedules []pgtype.UUID
}

// movedReturning is what a batch move to history returns: collectMoved reads it.
const movedReturning = "schedule_id, job_type, state"

// collectMoved runs a batch move to history, counting each moved job once tx commits and
// collecting the schedules whose jobs finished.
func collectMoved(ctx context.Context, tx pgx.Tx, sql string, args ...any) (movedRows, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return movedRows{}, err
	}
	var (
		m              movedRows
		schedule       pgtype.UUID
		jobType, state string
	)
	_, err = pgx.ForEachRow(rows, []any{&schedule, &jobType, &state}, func() error {
		m.rows++
		if schedule.Valid {
			m.schedules = append(m.schedules, schedule)
		}
		countCompleted(tx, jobType, domain.JobState(state))
		return nil
	})
	return m, err
}

// promotedReturning is what a promotion to READY returns: readLags reads it.
const promotedReturning = " RETURNING pool, extract(epoch FROM ready_at - run_at)::float8"

type lag struct {
	pool    string
	seconds float64
}

// readLags reads each promoted job's pool and scheduling lag (ready_at − run_at).
func readLags(rows pgx.Rows, err error) ([]lag, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (lag, error) {
		var l lag
		return l, r.Scan(&l.pool, &l.seconds)
	})
}

func recordLags(lags []lag) {
	for _, l := range lags {
		observability.SchedulingLag.Record(context.Background(), l.seconds,
			metric.WithAttributes(attribute.String("pool", l.pool)))
	}
}

// recordAttempt counts an ended attempt, its run time, and the retry or dead letter it led to.
func recordAttempt(job domain.Job, outcome domain.AttemptState, d domain.Decision, now time.Time) {
	ctx := context.Background()
	attrs := metric.WithAttributes(attribute.String("type", job.Type), attribute.String("outcome", string(outcome)))
	observability.Attempts.Add(ctx, 1, attrs)
	observability.ExecutionDuration.Record(ctx, max(0, now.Sub(job.Current.StartedAt).Seconds()), attrs)
	reason := metric.WithAttributes(attribute.String("type", job.Type), attribute.String("reason", string(d.Reason)))
	switch d.Next {
	case domain.StateRetryPending:
		observability.JobsRetried.Add(ctx, 1, reason)
	case domain.StateDeadLettered:
		observability.JobsDeadLettered.Add(ctx, 1, reason)
	}
}

// ClockOffset estimates the database clock minus this node's clock from one round trip.
func (s *Store) ClockOffset(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	var db time.Time
	if err := s.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&db); err != nil {
		return 0, err
	}
	return db.Sub(start.Add(time.Since(start) / 2)), nil
}
