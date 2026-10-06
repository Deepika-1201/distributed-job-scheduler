package postgres

import (
	"errors"
	"sync"
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

var planLimits = domain.DefaultPlanLimits

// createSchedule stores a schedule firing every minute from now, with overlap skip.
func (f *fixture) createSchedule(mods ...func(*domain.Schedule)) domain.Schedule {
	f.t.Helper()
	sc := domain.Schedule{
		TenantID:  f.tenant,
		Name:      "every-minute-" + newID(),
		JobType:   "email.send",
		Payload:   []byte(`{"digest": true}`),
		Trigger:   domain.Trigger{Kind: domain.TriggerFixedRate, Interval: time.Minute},
		Misfire:   domain.MisfireFireOnce,
		Overlap:   domain.OverlapSkip,
		CreatedBy: "test",
	}
	for _, mod := range mods {
		mod(&sc)
	}
	created, err := f.store.CreateSchedule(ctx, sc, testAudit)
	if err != nil {
		f.t.Fatalf("CreateSchedule: %v", err)
	}
	return created
}

func (f *fixture) materialize() int {
	f.t.Helper()
	n, err := f.store.MaterializeDue(ctx, planLimits, 100)
	if err != nil {
		f.t.Fatalf("MaterializeDue: %v", err)
	}
	return n
}

func (f *fixture) promoteScheduled() PromoteStats {
	f.t.Helper()
	stats, err := f.store.PromoteScheduled(ctx, 500)
	if err != nil {
		f.t.Fatalf("PromoteScheduled: %v", err)
	}
	return stats
}

func (f *fixture) schedule(id domain.ScheduleID) domain.Schedule {
	f.t.Helper()
	sc, err := f.store.GetSchedule(ctx, f.tenant, id)
	if err != nil {
		f.t.Fatalf("GetSchedule: %v", err)
	}
	return sc
}

// fireState returns the state of the schedule's job for the nth fire (0-based), active or finished.
func (f *fixture) fireState(sc domain.Schedule, n int) domain.JobState {
	f.t.Helper()
	var state string
	fire := sc.CreatedAt.Add(time.Duration(n) * time.Minute)
	if err := f.pool.QueryRow(ctx, `
		SELECT state FROM jobs WHERE schedule_id = $1 AND fire_time = $2
		UNION ALL SELECT state FROM job_history WHERE schedule_id = $1 AND fire_time = $2`,
		string(sc.ID), fire).Scan(&state); err != nil {
		f.t.Fatalf("fire %d: %v", n, err)
	}
	return domain.JobState(state)
}

// makeDue moves the nth fire's job run_at into the past.
func (f *fixture) makeDue(sc domain.Schedule, n int) {
	f.exec(`UPDATE jobs SET run_at = now() - interval '1 second' WHERE schedule_id = $1 AND fire_time = $2`,
		string(sc.ID), sc.CreatedAt.Add(time.Duration(n)*time.Minute))
}

func TestMaterializeIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule()
	if n := f.materialize(); n != 1 {
		t.Fatalf("materialized %d schedules, want 1", n)
	}
	const byFire = `SELECT count(*) FROM jobs WHERE schedule_id = $1 AND state = 'SCHEDULED' AND created_by = 'schedule:' || $1`
	if n := f.count(byFire, string(sc.ID)); n != 3 {
		t.Fatalf("%d jobs within the 2-minute lookahead, want 3", n)
	}
	got := f.schedule(sc.ID)
	if got.FireCount != 3 || !got.NextFireAt.Equal(sc.CreatedAt.Add(3*time.Minute)) || !got.LastFireAt.Equal(sc.CreatedAt.Add(2*time.Minute)) {
		t.Errorf("after materializing: fire count %d, next %v, last %v", got.FireCount, got.NextFireAt.Sub(sc.CreatedAt), got.LastFireAt.Sub(sc.CreatedAt))
	}
	if n := f.materialize(); n != 0 {
		t.Errorf("materialized %d schedules again, want 0", n)
	}

	// Replaying from an old cursor, e.g. after a lost update, must not duplicate fires.
	f.exec(`UPDATE schedules SET next_fire_at = created_at WHERE id = $1`, string(sc.ID))
	f.materialize()
	if n := f.count(byFire, string(sc.ID)); n != 3 {
		t.Errorf("%d jobs after replay, want 3", n)
	}
	if got := f.schedule(sc.ID); got.FireCount != 3 {
		t.Errorf("fire count %d after replay, want 3", got.FireCount)
	}
}

func TestConcurrentMaterializersNeverFireTwice(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const schedules = 20
	for range schedules {
		f.createSchedule()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				n, err := f.store.MaterializeDue(ctx, planLimits, 3)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id IS NOT NULL`); n != schedules*3 {
		t.Errorf("%d jobs, want %d", n, schedules*3)
	}
	if n := f.count(`SELECT count(*) FROM (SELECT DISTINCT schedule_id, fire_time FROM jobs) d`); n != schedules*3 {
		t.Errorf("%d distinct fires, want %d", n, schedules*3)
	}
}

func TestMaterializeAppliesMisfirePolicyAndLimits(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	skip := f.createSchedule(func(s *domain.Schedule) { s.Misfire = domain.MisfireSkip })
	once := f.createSchedule()
	limited := f.createSchedule(func(s *domain.Schedule) { s.MaxRuns = 2 })
	// Pretend the system was down for ten minutes: anchors and cursors move into the past.
	f.exec(`UPDATE schedules SET created_at = created_at - interval '10 minutes', next_fire_at = next_fire_at - interval '10 minutes'`)
	f.materialize()
	for _, tc := range []struct {
		sc   domain.Schedule
		want int
	}{{skip, 2}, {once, 3}, {limited, 2}} {
		if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id = $1`, string(tc.sc.ID)); n != tc.want {
			t.Errorf("%s: %d jobs, want %d", tc.sc.Name, n, tc.want)
		}
	}
	if got := f.schedule(limited.ID); got.State != domain.ScheduleCompleted || !got.NextFireAt.IsZero() {
		t.Errorf("max_runs schedule is %s with cursor %v, want COMPLETED", got.State, got.NextFireAt)
	}
}

func TestDisabledJobTypeHoldsItsSchedules(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule()
	f.exec(`UPDATE job_types SET enabled = false`)
	if n := f.materialize(); n != 0 {
		t.Errorf("materialized %d schedules of a disabled type", n)
	}
	f.exec(`UPDATE job_types SET enabled = true`)
	f.materialize()
	if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id = $1`, string(sc.ID)); n != 3 {
		t.Errorf("%d jobs after re-enabling, want 3", n)
	}
}

func TestPromoteDueAndExpireOverdue(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	delayed := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = now.Add(time.Hour) }))
	overdue := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt, nj.StartDeadline = now.Add(time.Hour), now.Add(2*time.Hour) }))
	waiting := f.submit(f.newJob(func(nj *NewJob) { nj.StartDeadline = now.Add(time.Hour) }))
	retried := f.submit(f.newJob(func(nj *NewJob) { nj.Priority = domain.PriorityLow }))
	f.exec(`UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ANY ($1::uuid[])`, []string{string(delayed.ID), string(overdue.ID)})
	f.exec(`UPDATE jobs SET start_deadline = now() - interval '1 second' WHERE id = ANY ($1::uuid[])`, []string{string(overdue.ID), string(waiting.ID)})

	// A job that already started once is not expired by its start deadline.
	f.exec(`UPDATE jobs SET attempt_count = 1, start_deadline = now() - interval '1 second' WHERE id = $1`, string(retried.ID))

	if n, err := f.store.ExpireOverdue(ctx, 100); err != nil || n != 2 {
		t.Fatalf("ExpireOverdue = %d, %v; want 2", n, err)
	}
	if n, err := f.store.PromoteDue(ctx, 100); err != nil || n != 1 {
		t.Fatalf("PromoteDue = %d, %v; want 1", n, err)
	}
	for id, want := range map[domain.JobID]domain.JobState{
		delayed.ID: domain.StateReady, overdue.ID: domain.StateExpired, waiting.ID: domain.StateExpired, retried.ID: domain.StateReady,
	} {
		got, err := f.store.GetJob(ctx, f.tenant, id)
		if err != nil || got.State != want {
			t.Errorf("job %s: %s (%v), want %s", id, got.State, err, want)
		}
		if want == domain.StateExpired && got.Reason != domain.ReasonStartDeadline {
			t.Errorf("expired job reason %q", got.Reason)
		}
	}
	claimed, err := f.store.ClaimReady(ctx, ClaimRequest{Lease: f.poolLease(), Pool: "default", Priority: domain.PriorityLow, Limit: 1, SessionID: "0191f000-0000-7000-8000-000000000001"})
	if err != nil || len(claimed) != 1 {
		t.Errorf("retried job past its start deadline was not claimable: %v, %d", err, len(claimed))
	}
}

func TestOverlapPolicies(t *testing.T) {
	t.Parallel()
	running := func(t *testing.T, policy domain.OverlapPolicy) (*fixture, domain.Schedule, domain.Job) {
		f := newFixture(t)
		sc := f.createSchedule(func(s *domain.Schedule) { s.Overlap = policy })
		f.materialize()
		if st := f.promoteScheduled(); st.Promoted != 1 {
			t.Fatalf("first fire: %+v, want one promotion", st)
		}
		return f, sc, f.claimOne()
	}

	t.Run("skip", func(t *testing.T) {
		t.Parallel()
		f, sc, _ := running(t, domain.OverlapSkip)
		f.makeDue(sc, 1)
		if st := f.promoteScheduled(); st.Skipped != 1 || f.fireState(sc, 1) != domain.StateSkipped {
			t.Errorf("stats %+v, fire 1 %s; want SKIPPED", st, f.fireState(sc, 1))
		}
	})
	t.Run("allow", func(t *testing.T) {
		t.Parallel()
		f, sc, _ := running(t, domain.OverlapAllow)
		f.makeDue(sc, 1)
		if f.promoteScheduled(); f.fireState(sc, 1) != domain.StateReady {
			t.Errorf("fire 1 %s, want READY", f.fireState(sc, 1))
		}
	})
	t.Run("buffer one", func(t *testing.T) {
		t.Parallel()
		f, sc, first := running(t, domain.OverlapBufferOne)
		f.makeDue(sc, 1)
		if st := f.promoteScheduled(); st.Buffered != 1 || f.fireState(sc, 1) != domain.StateScheduled {
			t.Fatalf("stats %+v, fire 1 %s; want buffered", st, f.fireState(sc, 1))
		}
		f.makeDue(sc, 1)
		f.makeDue(sc, 2)
		if st := f.promoteScheduled(); st.Buffered != 1 || st.Skipped != 1 || f.fireState(sc, 2) != domain.StateSkipped {
			t.Fatalf("stats %+v; want fire 1 buffered again and fire 2 skipped", st)
		}
		f.complete(completion(first, succeeded))
		f.makeDue(sc, 1)
		if f.promoteScheduled(); f.fireState(sc, 1) != domain.StateReady {
			t.Errorf("fire 1 %s after the first run finished, want READY", f.fireState(sc, 1))
		}
	})
	t.Run("cancel previous", func(t *testing.T) {
		t.Parallel()
		f, sc, first := running(t, domain.OverlapCancelPrevious)
		f.makeDue(sc, 1)
		f.promoteScheduled()
		got, err := f.store.GetJob(ctx, f.tenant, first.ID)
		if err != nil || got.CancelRequestedAt.IsZero() || f.fireState(sc, 1) != domain.StateReady {
			t.Fatalf("running job cancel requested %v (%v), fire 1 %s", got.CancelRequestedAt, err, f.fireState(sc, 1))
		}
		// Fire 1 is READY and not started; fire 2 supersedes it.
		f.makeDue(sc, 2)
		f.promoteScheduled()
		if f.fireState(sc, 1) != domain.StateCancelled || f.fireState(sc, 2) != domain.StateReady {
			t.Errorf("fire 1 %s, fire 2 %s; want CANCELLED, READY", f.fireState(sc, 1), f.fireState(sc, 2))
		}
	})
	t.Run("skip within one batch", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		sc := f.createSchedule()
		f.materialize()
		f.makeDue(sc, 1)
		if st := f.promoteScheduled(); st.Promoted != 1 || st.Skipped != 1 {
			t.Errorf("stats %+v; want fire 0 promoted and fire 1 skipped", st)
		}
	})
}

// Two promoters can split a schedule's due runs between their batches. The later run must
// wait for the earlier one's decision, or skip would start both (ADR-033).
func TestPromoterDefersRunsBehindAnEarlierDueRun(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	skip := f.createSchedule()
	allow := f.createSchedule(func(s *domain.Schedule) { s.Overlap = domain.OverlapAllow })
	f.materialize()
	f.makeDue(skip, 1)
	f.makeDue(allow, 1)

	// Another promoter's batch holds both schedules' first runs.
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, sc := range []domain.Schedule{skip, allow} {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM jobs WHERE schedule_id = $1 AND fire_time = $2 FOR UPDATE`, string(sc.ID), sc.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if st := f.promoteScheduled(); st.Promoted != 1 || st.Deferred != 1 || f.fireState(skip, 1) != domain.StateScheduled ||
		f.fireState(allow, 1) != domain.StateReady {
		t.Fatalf("stats %+v; want skip's fire 1 deferred and allow's promoted", st)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if st := f.promoteScheduled(); st.Promoted != 2 || st.Skipped != 1 || f.fireState(skip, 1) != domain.StateSkipped {
		t.Errorf("stats %+v, skip's fire 1 %s; want both first runs promoted and fire 1 skipped", st, f.fireState(skip, 1))
	}
}

func TestPauseWithdrawsAndResumeRematerializes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule()
	f.materialize()
	paused, err := f.store.PauseSchedule(ctx, f.tenant, sc.ID, testAudit)
	if err != nil {
		t.Fatal(err)
	}
	// Fire 0 is already due and stays; fires 1 and 2 are withdrawn.
	if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id = $1`, string(sc.ID)); n != 1 {
		t.Errorf("%d jobs after pause, want 1", n)
	}
	if paused.State != domain.SchedulePaused || paused.FireCount != 1 || !paused.NextFireAt.Equal(sc.CreatedAt.Add(time.Minute)) {
		t.Errorf("paused schedule: %s, fire count %d, next %v", paused.State, paused.FireCount, paused.NextFireAt.Sub(sc.CreatedAt))
	}
	if f.materialize() != 0 {
		t.Error("a paused schedule was materialized")
	}
	if _, err := f.store.PauseSchedule(ctx, f.tenant, sc.ID, testAudit); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("pausing twice: %v", err)
	}
	if _, err := f.store.ResumeSchedule(ctx, f.tenant, sc.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	f.materialize()
	if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id = $1`, string(sc.ID)); n != 3 {
		t.Errorf("%d jobs after resume, want 3", n)
	}
	if n := f.count(`SELECT count(*) FROM audit_log WHERE action LIKE 'schedule.%'`); n != 3 {
		t.Errorf("%d schedule audit rows, want 3 (create, pause, resume)", n)
	}
}

func TestUpdateRecomputesCursor(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule(func(s *domain.Schedule) {
		s.Trigger = domain.Trigger{Kind: domain.TriggerCron, Cron: "0 0 1 1 *", TimeZone: "Europe/Paris"}
	})
	updated, err := f.store.UpdateSchedule(ctx, f.tenant, sc.ID, testAudit, func(s *domain.Schedule) error {
		s.Trigger = domain.Trigger{Kind: domain.TriggerFixedRate, Interval: time.Hour}
		s.MaxRuns = 5
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := sc.CreatedAt.Add(time.Hour); !updated.NextFireAt.Equal(want) || updated.MaxRuns != 5 {
		t.Errorf("cursor %v, max runs %d; want created+1h, 5", updated.NextFireAt.Sub(sc.CreatedAt), updated.MaxRuns)
	}
	if _, err := f.store.UpdateSchedule(ctx, f.tenant, sc.ID, testAudit, func(s *domain.Schedule) error {
		s.Name = "taken"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	other := f.createSchedule()
	if _, err := f.store.UpdateSchedule(ctx, f.tenant, other.ID, testAudit, func(s *domain.Schedule) error {
		s.Name = "taken"
		return nil
	}); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Errorf("renaming onto a taken name: %v", err)
	}
}

func TestFixedDelayChainsOnCompletion(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule(func(s *domain.Schedule) { s.Trigger.Kind = domain.TriggerFixedDelay })
	f.materialize()
	if got := f.schedule(sc.ID); !got.NextFireAt.IsZero() || got.FireCount != 1 {
		t.Fatalf("after the first fire: cursor %v, fire count %d; want waiting", got.NextFireAt, got.FireCount)
	}
	if f.materialize() != 0 {
		t.Fatal("a waiting fixed-delay schedule was materialized")
	}
	f.promoteScheduled()
	job := f.claimOne()
	f.complete(completion(job, succeeded))
	got := f.schedule(sc.ID)
	var now time.Time
	if err := f.pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	if d := got.NextFireAt.Sub(now); d < 50*time.Second || d > time.Minute {
		t.Errorf("next fire in %v after completion, want about 1m", d)
	}
}

func TestDeleteScheduleFreesItsName(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule(func(s *domain.Schedule) { s.Name = "nightly" })
	f.materialize()
	if err := f.store.DeleteSchedule(ctx, f.tenant, sc.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetSchedule(ctx, f.tenant, sc.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetSchedule after delete: %v", err)
	}
	if n := f.count(`SELECT count(*) FROM jobs WHERE schedule_id = $1 AND run_at > now()`, string(sc.ID)); n != 0 {
		t.Errorf("%d future jobs remain after delete", n)
	}
	f.createSchedule(func(s *domain.Schedule) { s.Name = "nightly" })
	if _, err := f.store.CreateSchedule(ctx, domain.Schedule{TenantID: f.tenant, Name: "nightly", JobType: "email.send",
		Payload: []byte(`{}`), Trigger: domain.Trigger{Kind: domain.TriggerFixedRate, Interval: time.Minute},
		Misfire: domain.MisfireSkip, Overlap: domain.OverlapSkip, CreatedBy: "test"}, testAudit); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Errorf("duplicate name: %v", err)
	}
}
