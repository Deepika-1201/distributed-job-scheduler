package postgres

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// bufferRecheck is how long a buffered schedule job waits before its overlap is checked again.
const bufferRecheck = 5 * time.Second

var (
	expireSQL = moveSQL(`id IN (
    SELECT id FROM jobs
    WHERE state IN ('SCHEDULED', 'READY') AND attempt_count = 0 AND start_deadline <= now()
    ORDER BY start_deadline
    LIMIT $1
    FOR UPDATE SKIP LOCKED)`, map[string]string{"state": "'EXPIRED'"}, "'"+string(domain.ReasonStartDeadline)+"', NULL::json", movedReturning)

	skipSQL = moveSQL(`id = ANY ($1::uuid[]) AND state = 'SCHEDULED'`,
		map[string]string{"state": "'SKIPPED'"}, "$2::text, NULL::json", movedReturning)

	supersedeSQL = moveSQL(`schedule_id = $1 AND fire_time < $2 AND state IN ('READY', 'RETRY_PENDING')`,
		map[string]string{"state": "'CANCELLED'"}, "'"+string(domain.ReasonSuperseded)+"', NULL::json", movedReturning)
)

// ExpireOverdue ends never-started jobs whose start deadline has passed (T18, T19).
func (s *Store) ExpireOverdue(ctx context.Context, limit int) (int, error) {
	for _, from := range []domain.JobState{domain.StateScheduled, domain.StateReady} {
		if err := domain.ValidateJobTransition(from, domain.StateExpired, domain.ActorPromoter); err != nil {
			return 0, err
		}
	}
	var n int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		ids, err := collectMoved(ctx, tx, expireSQL, limit)
		n = ids.rows
		if err != nil {
			return err
		}
		return advanceFixedDelay(ctx, tx, ids.schedules)
	})
	return n, err
}

// PromoteDue makes due jobs READY: retries (T4), and delayed jobs that don't belong to a
// schedule (T3). Schedule jobs go through PromoteScheduled.
func (s *Store) PromoteDue(ctx context.Context, limit int) (int, error) {
	for _, from := range []domain.JobState{domain.StateScheduled, domain.StateRetryPending} {
		if err := domain.ValidateJobTransition(from, domain.StateReady, domain.ActorPromoter); err != nil {
			return 0, err
		}
	}
	lags, err := readLags(s.pool.Query(ctx, `
		UPDATE jobs SET state = 'READY', ready_at = now(), updated_at = now()
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE state IN ('SCHEDULED', 'RETRY_PENDING') AND run_at <= now()
		      AND (schedule_id IS NULL OR state = 'RETRY_PENDING')
		    ORDER BY run_at
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED)`+promotedReturning, limit))
	if err != nil {
		return 0, err
	}
	recordLags(lags)
	return len(lags), nil
}

// PromoteStats counts the outcomes of one PromoteScheduled batch.
type PromoteStats struct {
	Promoted, Skipped, Buffered, Superseded int
}

func (p PromoteStats) Total() int { return p.Promoted + p.Skipped + p.Buffered + p.Superseded }

type dueScheduleJob struct {
	id             pgtype.UUID
	schedule       pgtype.UUID
	fire           time.Time
	policy         domain.OverlapPolicy
	active, queued int // for a schedule's earliest due run: earlier runs that are active, and that wait
}

// promoteHeadsSQL locks up to $1 heads, each schedule's earliest due run, and counts the runs
// before each head (ADR-033). Laterals keep every lookup on an index, whatever plan the
// statement cache settles on.
const promoteHeadsSQL = `
WITH heads AS MATERIALIZED (
    SELECT id, schedule_id, fire_time FROM jobs j
    WHERE state = 'SCHEDULED' AND run_at <= now() AND schedule_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM jobs p WHERE p.schedule_id = j.schedule_id AND p.fire_time < j.fire_time
          AND p.state = 'SCHEDULED' AND p.run_at <= now())
    ORDER BY run_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED)
SELECT h.id, h.schedule_id, h.fire_time, s.overlap_policy, e.active, e.queued
FROM heads h
CROSS JOIN LATERAL (SELECT overlap_policy FROM schedules WHERE id = h.schedule_id) s
CROSS JOIN LATERAL (
    SELECT count(*) FILTER (WHERE p.state IN ('READY', 'RUNNING', 'RETRY_PENDING')) AS active,
        count(*) FILTER (WHERE p.state = 'SCHEDULED') AS queued
    FROM jobs p WHERE p.schedule_id = h.schedule_id AND p.fire_time < h.fire_time) e`

// promoteLaterSQL locks the heads' later due runs, in fire order per head, stopping after $3.
// It runs only when there are heads: a generic plan of one statement doing both scanned the
// whole schedule index on every idle poll.
const promoteLaterSQL = `
SELECT l.id, l.schedule_id, l.fire_time
FROM unnest($1::uuid[], $2::timestamptz[]) AS h (schedule_id, fire_time)
CROSS JOIN LATERAL (
    SELECT j.id, j.schedule_id, j.fire_time FROM jobs j
    WHERE j.schedule_id = h.schedule_id AND j.fire_time > h.fire_time
      AND j.state = 'SCHEDULED' AND j.run_at <= now()
    ORDER BY j.fire_time
    FOR UPDATE) l
LIMIT $3`

// PromoteScheduled applies each schedule's overlap policy to its due jobs (LLD §10.4).
//
// A batch takes schedules by their earliest due run, the head, locked with SKIP LOCKED, and
// then the schedule's later due runs. Holding the head gives one batch the schedule's
// decisions, so concurrent promoters can't split them (ADR-033). Heads never wait behind
// another run, so every batch makes progress. Each part is capped at limit jobs.
func (s *Store) PromoteScheduled(ctx context.Context, limit int) (PromoteStats, error) {
	var stats PromoteStats
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, promoteHeadsSQL, limit)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (dueScheduleJob, error) {
			var d dueScheduleJob
			var policy string
			err := r.Scan(&d.id, &d.schedule, &d.fire, &policy, &d.active, &d.queued)
			d.policy = domain.OverlapPolicy(policy)
			return d, err
		})
		if err != nil || len(batch) == 0 {
			return err
		}
		later, err := lockLaterRuns(ctx, tx, batch, limit)
		if err != nil {
			return err
		}
		d := decideOverlaps(append(batch, later...))
		stats = PromoteStats{Promoted: len(d.promote), Skipped: len(d.skip), Buffered: len(d.buffer), Superseded: len(d.supersede)}
		return applyOverlaps(ctx, tx, d)
	})
	return stats, err
}

// lockLaterRuns locks up to limit later due runs of the heads' schedules, under their policy.
func lockLaterRuns(ctx context.Context, tx pgx.Tx, heads []dueScheduleJob, limit int) ([]dueScheduleJob, error) {
	schedules := make([]pgtype.UUID, len(heads))
	fires := make([]time.Time, len(heads))
	policies := make(map[pgtype.UUID]domain.OverlapPolicy, len(heads))
	for i, h := range heads {
		schedules[i], fires[i], policies[h.schedule] = h.schedule, h.fire, h.policy
	}
	rows, err := tx.Query(ctx, promoteLaterSQL, schedules, fires, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (dueScheduleJob, error) {
		var d dueScheduleJob
		err := r.Scan(&d.id, &d.schedule, &d.fire)
		d.policy = policies[d.schedule]
		return d, err
	})
}

type overlapDecisions struct {
	promote, skip, buffer, supersede []pgtype.UUID
	cancelBefore                     map[pgtype.UUID]time.Time // schedule → fire time whose earlier runs are cancelled
}

// decideOverlaps applies the overlap policies, treating jobs promoted or buffered earlier in
// the batch like runs that were already active or waiting. Each schedule's first job in the
// batch is its earliest due run, which carries the counts of the runs before it.
func decideOverlaps(batch []dueScheduleJob) overlapDecisions {
	slices.SortFunc(batch, func(a, b dueScheduleJob) int {
		if c := strings.Compare(uuidString(a.schedule), uuidString(b.schedule)); c != 0 {
			return c
		}
		return a.fire.Compare(b.fire)
	})
	d := overlapDecisions{cancelBefore: map[pgtype.UUID]time.Time{}}
	for start := 0; start < len(batch); {
		end := start
		for end < len(batch) && batch[end].schedule == batch[start].schedule {
			end++
		}
		group := batch[start:end]
		start = end
		// Counts for later jobs include the earlier jobs of this batch, which were SCHEDULED.
		active, waiting := group[0].active > 0, group[0].queued > 0
		for i, j := range group {
			switch j.policy {
			case domain.OverlapAllow:
				d.promote = append(d.promote, j.id)
			case domain.OverlapBufferOne:
				switch {
				case waiting:
					d.skip = append(d.skip, j.id)
				case active:
					d.buffer, waiting = append(d.buffer, j.id), true
				default:
					d.promote, active = append(d.promote, j.id), true
				}
			case domain.OverlapCancelPrevious:
				if i < len(group)-1 {
					d.supersede = append(d.supersede, j.id)
					continue
				}
				if active {
					d.cancelBefore[j.schedule] = j.fire
				}
				d.promote = append(d.promote, j.id)
			default: // skip
				if active {
					d.skip = append(d.skip, j.id)
				} else {
					d.promote, active = append(d.promote, j.id), true
				}
			}
		}
	}
	return d
}

func applyOverlaps(ctx context.Context, tx pgx.Tx, d overlapDecisions) error {
	for _, edge := range []struct{ from, to domain.JobState }{
		{domain.StateScheduled, domain.StateReady}, {domain.StateScheduled, domain.StateSkipped},
		{domain.StateReady, domain.StateCancelled}, {domain.StateRetryPending, domain.StateCancelled},
	} {
		if err := domain.ValidateJobTransition(edge.from, edge.to, domain.ActorPromoter); err != nil {
			return err
		}
	}
	var finished []pgtype.UUID
	for schedule, fire := range d.cancelBefore {
		ids, err := collectMoved(ctx, tx, supersedeSQL, schedule, fire)
		if err != nil {
			return err
		}
		finished = append(finished, ids.schedules...)
		if _, err := tx.Exec(ctx, `
			UPDATE jobs SET cancel_requested_at = now(), updated_at = now()
			WHERE schedule_id = $1 AND fire_time < $2 AND state = 'RUNNING' AND cancel_requested_at IS NULL`,
			schedule, fire); err != nil {
			return err
		}
	}
	for _, skip := range []struct {
		ids    []pgtype.UUID
		reason domain.Reason
	}{{d.skip, domain.ReasonOverlap}, {d.supersede, domain.ReasonSuperseded}} {
		if len(skip.ids) == 0 {
			continue
		}
		ids, err := collectMoved(ctx, tx, skipSQL, skip.ids, string(skip.reason))
		if err != nil {
			return err
		}
		finished = append(finished, ids.schedules...)
	}
	if len(d.promote) > 0 {
		lags, err := readLags(tx.Query(ctx, `UPDATE jobs SET state = 'READY', ready_at = now(), updated_at = now()
			WHERE id = ANY ($1::uuid[])`+promotedReturning, d.promote))
		if err != nil {
			return err
		}
		onCommit(tx, func() { recordLags(lags) })
	}
	if len(d.buffer) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET run_at = now() + make_interval(secs => $2), updated_at = now()
			WHERE id = ANY ($1::uuid[])`, d.buffer, bufferRecheck.Seconds()); err != nil {
			return err
		}
	}
	return advanceFixedDelay(ctx, tx, finished)
}
