package postgres

import (
	"errors"
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

func TestPoolAndJobTypeHoldsBlockClaims(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	ws := f.session("default")
	ready := func() bool {
		has, err := f.store.ReadyPriorities(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}
		return has[domain.PriorityNormal]
	}

	if err := f.store.SetPoolPaused(ctx, "default", true, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if ready() || len(f.claimFor(ws, 10, nil)) != 0 {
		t.Fatal("a paused pool reported or handed out work")
	}
	if err := f.store.SetPoolPaused(ctx, "default", false, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if jt, err := f.store.SetJobTypePaused(ctx, f.tenant, "email.send", true, testAudit); err != nil || !jt.Paused {
		t.Fatalf("pause job type = %+v, %v", jt, err)
	}
	if ready() || len(f.claimFor(ws, 10, nil)) != 0 {
		t.Fatal("a paused job type's job was reported or claimed")
	}
	if _, err := f.store.SetJobTypePaused(ctx, f.tenant, "email.send", false, testAudit); err != nil {
		t.Fatal(err)
	}
	if !ready() || len(f.claimFor(ws, 10, nil)) != 1 {
		t.Error("the job was not claimable after both holds were lifted")
	}
	if _, err := f.store.SetJobTypePaused(ctx, f.tenant, "missing", true, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("pausing an unknown job type = %v", err)
	}
	if n := f.count(`SELECT count(*) FROM audit_log WHERE action IN ('pool.pause', 'pool.resume', 'job_type.pause', 'job_type.resume')`); n != 4 {
		t.Errorf("%d hold audit rows, want 4", n)
	}
}

func TestDrainReachesTheWorker(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	ws := f.session("default")
	f.claimFor(ws, 1, nil)
	if err := f.store.DrainSession(ctx, ws.ID, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	res, err := f.store.Heartbeat(ctx, ws.ID, "", "", nil, 30*time.Second)
	if err != nil || !res.Drain {
		t.Fatalf("heartbeat after drain = %+v, %v; want drain", res, err)
	}
	sessions, err := f.store.ListSessions(ctx, "default")
	if err != nil || len(sessions) != 1 || !sessions[0].Draining || sessions[0].Running != 1 {
		t.Fatalf("sessions = %+v, %v", sessions, err)
	}
	if err := f.store.DeregisterSession(ctx, ws.ID, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DrainSession(ctx, ws.ID, f.tenant, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("draining a closed session = %v, want ErrNotFound", err)
	}
	if n := f.count(`SELECT count(*) FROM jobs WHERE state = 'RETRY_PENDING'`); n != 1 {
		t.Errorf("%d jobs retried after deregistration, want 1", n)
	}
}

func TestTriggerScheduleRunsOnceNow(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sc := f.createSchedule(func(s *domain.Schedule) {
		s.Trigger = domain.Trigger{Kind: domain.TriggerCron, Cron: "0 0 1 1 *", TimeZone: "UTC"}
	})
	job, err := f.store.TriggerSchedule(ctx, f.tenant, sc.ID, testAudit)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != domain.StateScheduled || job.ScheduleID != sc.ID || job.CreatedBy != testAudit.Actor || time.Since(job.RunAt) < 0 {
		t.Errorf("triggered job = %+v", job)
	}
	if got := f.schedule(sc.ID); got.FireCount != 0 || !got.NextFireAt.Equal(sc.NextFireAt) {
		t.Errorf("trigger changed the schedule: fire count %d, cursor %v", got.FireCount, got.NextFireAt)
	}
	if st := f.promoteScheduled(); st.Promoted != 1 {
		t.Errorf("promoting the triggered job: %+v", st)
	}
	f.exec(`UPDATE job_types SET enabled = false`)
	if _, err := f.store.TriggerSchedule(ctx, f.tenant, sc.ID, testAudit); !errors.Is(err, domain.ErrConflict) {
		t.Errorf("triggering a disabled type's schedule = %v, want ErrConflict", err)
	}
}

func TestListPoolsReportsState(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	f.session("default")
	f.poolLease()
	if err := f.store.SetPoolPaused(ctx, "reports", true, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	pools, err := f.store.ListPools(ctx)
	if err != nil || len(pools) != 2 {
		t.Fatalf("pools = %+v, %v; want default and reports", pools, err)
	}
	def, reports := pools[0], pools[1]
	if def.Name != "default" || def.Paused || def.Owner == nil || def.Owner.Holder != "test-node" ||
		def.Workers != 1 || def.Slots != 4 || def.ReadyJobs != 1 {
		t.Errorf("default pool = %+v, owner %+v", def, def.Owner)
	}
	if reports.Name != "reports" || !reports.Paused || reports.Owner != nil || reports.Workers != 0 {
		t.Errorf("reports pool = %+v", reports)
	}
}
