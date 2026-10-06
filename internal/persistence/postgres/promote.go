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
	id       pgtype.UUID
	schedule pgtype.UUID
	fire     time.Time
	policy   domain.OverlapPolicy
	// The schedule's runs that fire earlier: active ones, waiting ones, and waiting ones now due.
	active, queued, due int
}

// earlierRunsSQL counts the runs of r's schedule that fire before r.
const earlierRunsSQL = `
    SELECT count(*) FILTER (WHERE p.state IN ('READY', 'RUNNING', 'RETRY_PENDING')) AS active,
        count(*) FILTER (WHERE p.state = 'SCHEDULED') AS queued,
        count(*) FILTER (WHERE p.state = 'SCHEDULED' AND p.run_at <= now()) AS due
    FROM jobs p WHERE p.schedule_id = r.schedule_id AND p.fire_time < r.fire_time`

// promoteBatchSQL locks up to $1 due schedule jobs in run_at order.
const promoteBatchSQL = `
WITH batch AS MATERIALIZED (
    SELECT id, schedule_id, fire_time FROM jobs
    WHERE state = 'SCHEDULED' AND run_at <= now() AND schedule_id IS NOT NULL
    ORDER BY run_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED)
SELECT r.id, r.schedule_id, r.fire_time, s.overlap_policy, e.active, e.queued, e.due, 0
FROM batch r JOIN schedules s ON s.id = r.schedule_id
CROSS JOIN LATERAL (` + earlierRunsSQL + `) e`

// promoteGapsSQL locks the other due runs of schedules ($1) whose batch jobs have earlier due
// runs outside the batch ($2), and counts each schedule's due runs in the same snapshot. At
// most $3 rows: a schedule cut off by the limit counts as not fully held.
const promoteGapsSQL = `
SELECT r.id, g.schedule_id, r.fire_time, s.overlap_policy, e.active, e.queued, e.due, t.total
FROM unnest($1::uuid[]) AS g (schedule_id)
CROSS JOIN LATERAL (SELECT overlap_policy FROM schedules WHERE id = g.schedule_id) s
CROSS JOIN LATERAL (SELECT count(*) AS total FROM jobs
    WHERE schedule_id = g.schedule_id AND state = 'SCHEDULED' AND run_at <= now()) t
LEFT JOIN LATERAL (
    SELECT j.id, j.schedule_id, j.fire_time FROM jobs j
    WHERE j.schedule_id = g.schedule_id AND j.state = 'SCHEDULED' AND j.run_at <= now()
      AND NOT (j.id = ANY ($2::uuid[]))
    ORDER BY j.fire_time
    FOR UPDATE SKIP LOCKED) r ON true
LEFT JOIN LATERAL (` + earlierRunsSQL + `) e ON r.id IS NOT NULL
LIMIT $3`

// PromoteScheduled applies each schedule's overlap policy to its due jobs (LLD §10.4).
//
// A schedule's jobs are decided in fire order, by whoever holds all of its due runs (ADR-033).
// The batch takes due jobs in run_at order. Where a job has an earlier due run outside the
// batch, because jitter sorted it later or another promoter holds it, the schedule's other due
// runs are locked too. A schedule whose due runs aren't all held is left for a later batch.
func (s *Store) PromoteScheduled(ctx context.Context, limit int) (PromoteStats, error) {
	var stats PromoteStats
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		batch, err := collectDue(tx.Query(ctx, promoteBatchSQL, limit))
		if err != nil || len(batch) == 0 {
			return err
		}
		if gaps := gapSchedules(batch); len(gaps) > 0 {
			ids := make([]pgtype.UUID, len(batch))
			for i, j := range batch {
				ids[i] = j.id
			}
			more, err := collectDue(tx.Query(ctx, promoteGapsSQL, gaps, ids, limit))
			if err != nil {
				return err
			}
			batch = completeGaps(batch, gaps, more)
		}
		d := decideOverlaps(batch)
		stats = PromoteStats{Promoted: len(d.promote), Skipped: len(d.skip), Buffered: len(d.buffer), Superseded: len(d.supersede)}
		return applyOverlaps(ctx, tx, d)
	})
	return stats, err
}

// dueRow is a row of promoteBatchSQL or promoteGapsSQL; total is a gap schedule's due runs.
type dueRow struct {
	dueScheduleJob
	total int
}

func collectDue(rows pgx.Rows, err error) ([]dueRow, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (dueRow, error) {
		var d dueRow
		var policy string
		var fire *time.Time
		var active, queued, due *int
		err := r.Scan(&d.id, &d.schedule, &fire, &policy, &active, &queued, &due, &d.total)
		d.policy = domain.OverlapPolicy(policy)
		if fire != nil && active != nil {
			d.fire, d.active, d.queued, d.due = *fire, *active, *queued, *due
		}
		return d, err
	})
}

// gapSchedules returns the schedules with a batch job whose earlier due runs aren't all in the
// batch. Batch jobs share one snapshot, so their counts agree.
func gapSchedules(batch []dueRow) []pgtype.UUID {
	before := map[pgtype.UUID]int{}
	sorted := slices.Clone(batch)
	slices.SortFunc(sorted, func(a, b dueRow) int { return a.fire.Compare(b.fire) })
	var gaps []pgtype.UUID
	for _, j := range sorted {
		if j.due > before[j.schedule] && !slices.Contains(gaps, j.schedule) {
			gaps = append(gaps, j.schedule)
		}
		before[j.schedule]++
	}
	return gaps
}

// completeGaps adds the gap schedules' other due runs to the batch when this transaction holds
// all of them, and otherwise drops the schedule from the batch: its runs stay SCHEDULED, are
// unlocked at commit, and are decided once their holder has finished.
func completeGaps(batch []dueRow, gaps []pgtype.UUID, more []dueRow) []dueRow {
	total := map[pgtype.UUID]int{}
	held := map[pgtype.UUID]int{}
	for _, j := range batch {
		held[j.schedule]++
	}
	var fetched []dueRow
	for _, m := range more {
		total[m.schedule] = m.total
		if m.id.Valid {
			held[m.schedule]++
			fetched = append(fetched, m)
		}
	}
	incomplete := map[pgtype.UUID]bool{}
	for _, s := range gaps {
		if t, seen := total[s]; !seen || held[s] != t {
			incomplete[s] = true
		}
	}
	return slices.DeleteFunc(append(batch, fetched...), func(j dueRow) bool { return incomplete[j.schedule] })
}

type overlapDecisions struct {
	promote, skip, buffer, supersede []pgtype.UUID
	cancelBefore                     map[pgtype.UUID]time.Time // schedule → fire time whose earlier runs are cancelled
}

// decideOverlaps applies the overlap policies, treating jobs promoted or buffered earlier in
// the batch like runs that were already active or waiting. Each schedule's first job in the
// batch is its earliest due run, which carries the counts of the runs before it.
func decideOverlaps(rows []dueRow) overlapDecisions {
	batch := make([]dueScheduleJob, len(rows))
	for i, r := range rows {
		batch[i] = r.dueScheduleJob
	}
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
