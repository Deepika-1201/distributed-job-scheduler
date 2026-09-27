package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
)

var scheduleCols = []string{
	"id", "tenant_id", "name", "job_type", "payload", "labels", "priority", "trigger_kind", "cron_expr", "time_zone",
	"interval_ms", "start_at", "end_at", "max_runs", "jitter_ms", "misfire_policy", "overlap_policy", "state",
	"next_fire_at", "last_fire_at", "fire_count", "created_by", "created_at", "updated_at",
}

var (
	scheduleColumns  = strings.Join(scheduleCols, ", ")
	scheduleColumnsS = "s." + strings.Join(scheduleCols, ", s.")
)

func scanSchedule(row pgx.Row, extra ...any) (domain.Schedule, error) {
	var (
		sc                         domain.Schedule
		id, tenant                 pgtype.UUID
		priority                   pgtype.Int2
		kind, misfire, overlap     string
		state                      string
		cronExpr, zone             pgtype.Text
		intervalMS                 pgtype.Int8
		startAt, endAt, next, last pgtype.Timestamptz
		maxRuns                    pgtype.Int4
		jitterMS                   int64
	)
	dest := append([]any{&id, &tenant, &sc.Name, &sc.JobType, &sc.Payload, &sc.Labels, &priority, &kind, &cronExpr,
		&zone, &intervalMS, &startAt, &endAt, &maxRuns, &jitterMS, &misfire, &overlap, &state, &next, &last,
		&sc.FireCount, &sc.CreatedBy, &sc.CreatedAt, &sc.UpdatedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Schedule{}, domain.ErrNotFound
		}
		return domain.Schedule{}, err
	}
	sc.ID, sc.TenantID = domain.ScheduleID(uuidString(id)), domain.TenantID(uuidString(tenant))
	sc.Priority = domain.Priority(priority.Int16)
	sc.Trigger = domain.Trigger{Kind: domain.TriggerKind(kind), Cron: cronExpr.String, TimeZone: zone.String,
		Interval: time.Duration(intervalMS.Int64) * time.Millisecond}
	sc.StartAt, sc.EndAt, sc.NextFireAt, sc.LastFireAt = startAt.Time, endAt.Time, next.Time, last.Time
	sc.MaxRuns, sc.Jitter = int(maxRuns.Int32), time.Duration(jitterMS)*time.Millisecond
	sc.Misfire, sc.Overlap, sc.State = domain.MisfirePolicy(misfire), domain.OverlapPolicy(overlap), domain.ScheduleState(state)
	return sc, nil
}

// definitionArgs are the values of the mutable columns, in scheduleCols order from "payload"
// through "state", then next_fire_at, fire_count and name.
func definitionArgs(sc domain.Schedule) []any {
	var cronExpr, zone pgtype.Text
	var interval pgtype.Int8
	if sc.Trigger.Kind == domain.TriggerCron {
		tz := sc.Trigger.TimeZone
		if tz == "" {
			tz = "UTC"
		}
		cronExpr, zone = pgtype.Text{String: sc.Trigger.Cron, Valid: true}, pgtype.Text{String: tz, Valid: true}
	} else {
		interval = pgtype.Int8{Int64: sc.Trigger.Interval.Milliseconds(), Valid: true}
	}
	labels := sc.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return []any{
		sc.Payload, labels, pgtype.Int2{Int16: int16(sc.Priority), Valid: sc.Priority != 0}, string(sc.Trigger.Kind),
		cronExpr, zone, interval, nullTime(sc.StartAt), nullTime(sc.EndAt),
		pgtype.Int4{Int32: int32(sc.MaxRuns), Valid: sc.MaxRuns > 0}, sc.Jitter.Milliseconds(),
		string(sc.Misfire), string(sc.Overlap), string(sc.State), nullTime(sc.NextFireAt), sc.FireCount, sc.Name,
	}
}

// CreateSchedule stores a validated schedule with its first fire time as the cursor.
func (s *Store) CreateSchedule(ctx context.Context, sc domain.Schedule, audit Audit) (domain.Schedule, error) {
	tenant, ok := canonicalUUID(string(sc.TenantID))
	if !ok {
		return domain.Schedule{}, domain.ErrNotFound
	}
	var created domain.Schedule
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := lockTenantForScheduleQuota(ctx, tx, tenant); err != nil {
			return err
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
			return err
		}
		sc.ID, sc.CreatedAt, sc.State, sc.FireCount = domain.ScheduleID(newID()), now, domain.ScheduleActive, 0
		next, err := sc.FirstFire(now.Add(-time.Nanosecond))
		if err != nil {
			return err
		}
		sc.NextFireAt = next
		args := append([]any{string(sc.ID), tenant, sc.JobType, sc.CreatedBy, now}, definitionArgs(sc)...)
		created, err = scanSchedule(tx.QueryRow(ctx, `
			INSERT INTO schedules (id, tenant_id, job_type, created_by, created_at, updated_at, payload, labels, priority,
			    trigger_kind, cron_expr, time_zone, interval_ms, start_at, end_at, max_runs, jitter_ms, misfire_policy,
			    overlap_policy, state, next_fire_at, fire_count, name)
			VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
			RETURNING `+scheduleColumns, args...))
		if isUniqueViolation(err) {
			return domain.ErrAlreadyExists
		}
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "schedule.create", string(created.ID), map[string]any{"name": sc.Name})
	})
	return created, err
}

// GetSchedule returns a schedule that hasn't been deleted.
func (s *Store) GetSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID) (domain.Schedule, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	schedID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Schedule{}, domain.ErrNotFound
	}
	return scanSchedule(s.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE id = $1 AND tenant_id = $2 AND state <> 'DELETED'`, schedID, tenant))
}

// ScheduleCursor marks the last schedule of a page in (created_at, id) descending order.
type ScheduleCursor struct {
	CreatedAt time.Time
	ID        domain.ScheduleID
}

// ListSchedules returns a page of the tenant's schedules, newest first.
func (s *Store) ListSchedules(ctx context.Context, tenantID domain.TenantID, limit int, after *ScheduleCursor) ([]domain.Schedule, *ScheduleCursor, error) {
	tenant, ok := canonicalUUID(string(tenantID))
	if !ok {
		return nil, nil, domain.ErrNotFound
	}
	var afterTime pgtype.Timestamptz
	var afterID *string
	if after != nil {
		id, ok := canonicalUUID(string(after.ID))
		if !ok {
			return nil, nil, fmt.Errorf("invalid cursor id %q", after.ID)
		}
		afterTime, afterID = pgtype.Timestamptz{Time: after.CreatedAt, Valid: true}, &id
	}
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE tenant_id = $1 AND state <> 'DELETED' AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC LIMIT $4`, tenant, afterTime, afterID, limit+1)
	if err != nil {
		return nil, nil, err
	}
	page, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Schedule, error) { return scanSchedule(r) })
	if err != nil || len(page) <= limit {
		return page, nil, err
	}
	last := page[limit-1]
	return page[:limit], &ScheduleCursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// changeSchedule locks a schedule, lets change modify it, then saves it and records an audit
// row. change receives the database time.
func (s *Store) changeSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit, action string,
	change func(tx pgx.Tx, sc *domain.Schedule, now time.Time) (map[string]any, error)) (domain.Schedule, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	schedID, ok2 := canonicalUUID(string(id))
	if !ok1 || !ok2 {
		return domain.Schedule{}, domain.ErrNotFound
	}
	var res domain.Schedule
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var now time.Time
		sc, err := scanSchedule(tx.QueryRow(ctx, `SELECT `+scheduleColumns+`, now() FROM schedules
			WHERE id = $1 AND tenant_id = $2 AND state <> 'DELETED' FOR UPDATE`, schedID, tenant), &now)
		if err != nil {
			return err
		}
		res = sc
		details, err := change(tx, &sc, now)
		if err != nil {
			return err
		}
		args := append([]any{schedID}, definitionArgs(sc)...)
		res, err = scanSchedule(tx.QueryRow(ctx, `
			UPDATE schedules SET payload = $2, labels = $3, priority = $4, trigger_kind = $5, cron_expr = $6,
			    time_zone = $7, interval_ms = $8, start_at = $9, end_at = $10, max_runs = $11, jitter_ms = $12,
			    misfire_policy = $13, overlap_policy = $14, state = $15, next_fire_at = $16, fire_count = $17,
			    name = $18, updated_at = now()
			WHERE id = $1
			RETURNING `+scheduleColumns, args...))
		if isUniqueViolation(err) {
			return domain.ErrAlreadyExists
		}
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, action, schedID, details)
	})
	return res, err
}

// withdraw deletes a schedule's provisional jobs, those materialized but not yet due, with
// their ledger rows, and rewinds the cursor to the earliest of them (LLD §10.5).
func withdraw(ctx context.Context, tx pgx.Tx, sc *domain.Schedule) (int, error) {
	var (
		n        int
		earliest pgtype.Timestamptz
	)
	err := tx.QueryRow(ctx, `
		WITH w AS (DELETE FROM jobs WHERE schedule_id = $1 AND state = 'SCHEDULED' AND run_at > now() RETURNING fire_time),
		     f AS (DELETE FROM schedule_fires WHERE schedule_id = $1 AND fire_time IN (SELECT fire_time FROM w))
		SELECT count(*), min(fire_time) FROM w`, string(sc.ID)).Scan(&n, &earliest)
	if err != nil || n == 0 {
		return n, err
	}
	sc.FireCount -= n
	if sc.NextFireAt.IsZero() || earliest.Time.Before(sc.NextFireAt) {
		sc.NextFireAt = earliest.Time
	}
	return n, nil
}

func hasActiveRun(ctx context.Context, tx pgx.Tx, id domain.ScheduleID) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE schedule_id = $1)`, string(id)).Scan(&active)
	return active, err
}

// UpdateSchedule applies edit to the definition, withdraws provisional jobs and recomputes the
// cursor from the later of now and the last kept fire. The job type cannot change.
func (s *Store) UpdateSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit,
	edit func(*domain.Schedule) error) (domain.Schedule, error) {
	return s.changeSchedule(ctx, tenantID, id, audit, "schedule.update", func(tx pgx.Tx, sc *domain.Schedule, now time.Time) (map[string]any, error) {
		oldKind := sc.Trigger.Kind
		n, err := withdraw(ctx, tx, sc)
		if err != nil {
			return nil, err
		}
		if err := edit(sc); err != nil {
			return nil, err
		}
		if sc.State == domain.ScheduleCompleted {
			sc.State = domain.ScheduleActive
		}
		if sc.Trigger.Kind == domain.TriggerFixedDelay {
			active, err := hasActiveRun(ctx, tx, sc.ID)
			switch {
			case err != nil:
				return nil, err
			case active:
				sc.NextFireAt = time.Time{}
			case oldKind != domain.TriggerFixedDelay || sc.NextFireAt.IsZero():
				if sc.NextFireAt, err = sc.FirstFire(now); err != nil {
					return nil, err
				}
			}
			return map[string]any{"withdrawn": n}, nil
		}
		from := now
		var lastKept pgtype.Timestamptz
		if err := tx.QueryRow(ctx, `SELECT max(fire_time) FROM schedule_fires WHERE schedule_id = $1`,
			string(sc.ID)).Scan(&lastKept); err != nil {
			return nil, err
		}
		if lastKept.Valid && lastKept.Time.After(from) {
			from = lastKept.Time
		}
		if sc.NextFireAt, err = sc.FirstFire(from); err != nil {
			return nil, err
		}
		return map[string]any{"withdrawn": n}, nil
	})
}

// PauseSchedule stops materialization and withdraws provisional jobs.
func (s *Store) PauseSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit) (domain.Schedule, error) {
	return s.changeSchedule(ctx, tenantID, id, audit, "schedule.pause", func(tx pgx.Tx, sc *domain.Schedule, _ time.Time) (map[string]any, error) {
		if sc.State != domain.ScheduleActive {
			return nil, fmt.Errorf("%w: schedule is %s", domain.ErrInvalidTransition, sc.State)
		}
		n, err := withdraw(ctx, tx, sc)
		sc.State = domain.SchedulePaused
		return map[string]any{"withdrawn": n}, err
	})
}

// ResumeSchedule reactivates a paused schedule; a stale cursor is then handled as a misfire.
func (s *Store) ResumeSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit) (domain.Schedule, error) {
	return s.changeSchedule(ctx, tenantID, id, audit, "schedule.resume", func(tx pgx.Tx, sc *domain.Schedule, now time.Time) (map[string]any, error) {
		if sc.State != domain.SchedulePaused {
			return nil, fmt.Errorf("%w: schedule is %s", domain.ErrInvalidTransition, sc.State)
		}
		sc.State = domain.ScheduleActive
		if sc.Trigger.Kind == domain.TriggerFixedDelay && sc.NextFireAt.IsZero() {
			active, err := hasActiveRun(ctx, tx, sc.ID)
			if err != nil || active {
				return nil, err
			}
			sc.NextFireAt = now
		}
		return nil, nil
	})
}

// DeleteSchedule soft-deletes a schedule and withdraws provisional jobs; jobs that already
// ran stay in history.
func (s *Store) DeleteSchedule(ctx context.Context, tenantID domain.TenantID, id domain.ScheduleID, audit Audit) error {
	_, err := s.changeSchedule(ctx, tenantID, id, audit, "schedule.delete", func(tx pgx.Tx, sc *domain.Schedule, _ time.Time) (map[string]any, error) {
		n, err := withdraw(ctx, tx, sc)
		sc.State, sc.NextFireAt = domain.ScheduleDeleted, time.Time{}
		return map[string]any{"withdrawn": n}, err
	})
	return err
}

// MaterializeDue creates the jobs for the due fires of up to limit schedules whose job type is
// enabled (LLD §10.3), and returns how many schedules it processed.
func (s *Store) MaterializeDue(ctx context.Context, lim domain.PlanLimits, limit int) (int, error) {
	if err := domain.ValidateJobTransition(domain.StateNew, domain.StateScheduled, domain.ActorMaterializer); err != nil {
		return 0, err
	}
	var n int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+scheduleColumnsS+`, now(), t.version, t.pool, t.default_priority, t.attempt_timeout_ms,
			    t.retry_policy, t.at_most_once
			FROM schedules s JOIN job_types t ON t.tenant_id = s.tenant_id AND t.name = s.job_type
			WHERE s.state = 'ACTIVE' AND s.next_fire_at <= now() + make_interval(secs => $1) AND t.enabled
			ORDER BY s.next_fire_at
			LIMIT $2
			FOR UPDATE OF s SKIP LOCKED`, lim.Lookahead.Seconds(), limit)
		if err != nil {
			return err
		}
		type due struct {
			sc  domain.Schedule
			jt  domain.JobType
			now time.Time
		}
		batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
			var (
				d         due
				priority  int16
				timeoutMS int64
				policy    retryPolicyJSON
			)
			sc, err := scanSchedule(r, &d.now, &d.jt.Version, &d.jt.Pool, &priority, &timeoutMS, &policy, &d.jt.AtMostOnce)
			d.sc = sc
			d.jt.DefaultPriority, d.jt.AttemptTimeout = domain.Priority(priority), time.Duration(timeoutMS)*time.Millisecond
			d.jt.RetryPolicy = policy.policy()
			return d, err
		})
		if err != nil {
			return err
		}
		for _, d := range batch {
			if err := materialize(ctx, tx, d.sc, d.jt, d.now, lim); err != nil {
				return fmt.Errorf("materialize schedule %s: %w", d.sc.ID, err)
			}
		}
		n = len(batch)
		return nil
	})
	return n, err
}

const insertFiresSQL = `
WITH f AS (
    INSERT INTO schedule_fires (schedule_id, fire_time, job_id)
    SELECT $1::uuid, t.fire_time, t.job_id FROM unnest($2::timestamptz[], $3::uuid[]) AS t (fire_time, job_id)
    ON CONFLICT DO NOTHING
    RETURNING fire_time, job_id)
INSERT INTO jobs (id, tenant_id, job_type, job_type_version, pool, schedule_id, fire_time, state, priority, payload,
    labels, created_by, run_at, attempt_timeout_ms, retry_policy, at_most_once, created_at, updated_at)
SELECT f.job_id, $4::uuid, $5::text, $6::integer, $7::text, $1::uuid, f.fire_time, 'SCHEDULED', $8::smallint, $9::json,
    $10::jsonb, $11::text, r.run_at, $12::bigint, $13::jsonb, $14::boolean, now(), now()
FROM f JOIN unnest($2::timestamptz[], $15::timestamptz[]) AS r (fire_time, run_at) ON r.fire_time = f.fire_time`

// materialize plans one schedule's fires and inserts a job for each fire the ledger hadn't
// recorded yet, then advances the cursor.
func materialize(ctx context.Context, tx pgx.Tx, sc domain.Schedule, jt domain.JobType, now time.Time, lim domain.PlanLimits) error {
	plan, err := domain.PlanFires(sc, now, lim)
	if err != nil {
		return err
	}
	var inserted int64
	var last pgtype.Timestamptz
	if len(plan.Fires) > 0 {
		ids := make([]pgtype.UUID, len(plan.Fires))
		runAts := make([]time.Time, len(plan.Fires))
		for i, fire := range plan.Fires {
			ids[i] = pgtype.UUID{Bytes: uuid.Must(uuid.NewV7()), Valid: true}
			runAts[i] = fire.Add(domain.JitterOffset(sc.ID, fire, sc.Jitter))
		}
		priority := sc.Priority
		if priority == 0 {
			priority = jt.DefaultPriority
		}
		labels := sc.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		tag, err := tx.Exec(ctx, insertFiresSQL, string(sc.ID), plan.Fires, ids, string(sc.TenantID), sc.JobType,
			jt.Version, jt.Pool, int16(priority), sc.Payload, labels, "schedule:"+string(sc.ID),
			jt.AttemptTimeout.Milliseconds(), policyToJSON(jt.RetryPolicy), jt.AtMostOnce, runAts)
		if err != nil {
			return err
		}
		inserted = tag.RowsAffected()
		if inserted > 0 {
			onCommit(tx, func() {
				observability.JobsScheduled.Add(context.Background(), inserted, metric.WithAttributes(
					attribute.String("tenant", string(sc.TenantID)), attribute.String("type", sc.JobType),
					attribute.String("source", "schedule")))
			})
		}
		last = pgtype.Timestamptz{Time: plan.Fires[len(plan.Fires)-1], Valid: true}
	}
	_, err = tx.Exec(ctx, `
		UPDATE schedules SET next_fire_at = $2, fire_count = fire_count + $3, last_fire_at = GREATEST(last_fire_at, $4),
		    state = CASE WHEN $5 THEN 'COMPLETED' ELSE state END, updated_at = now()
		WHERE id = $1`, string(sc.ID), nullTime(plan.Next), inserted, last, plan.Completed)
	return err
}

// advanceFixedDelay starts the delay of fixed-delay schedules whose run just finished, in the
// transaction that finished it (LLD §10.2).
func advanceFixedDelay(ctx context.Context, tx pgx.Tx, scheduleIDs []pgtype.UUID) error {
	if len(scheduleIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE schedules SET next_fire_at = now() + interval_ms * interval '1 millisecond', updated_at = now()
		WHERE id = ANY ($1::uuid[]) AND trigger_kind = 'fixed_delay' AND state IN ('ACTIVE', 'PAUSED')
		  AND next_fire_at IS NULL`, scheduleIDs)
	return err
}
