package postgres

import (
	"slices"
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

func (f *fixture) backdate(id domain.JobID, age time.Duration) {
	f.t.Helper()
	f.exec(`UPDATE jobs SET run_at = now() - make_interval(secs => $2) WHERE id = $1`, string(id), age.Seconds())
}

func TestPoolGaugesSeparateHeldWork(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	beta := f.addTenant("beta")
	f.exec(`INSERT INTO job_types (tenant_id, name, pool, attempt_timeout_ms, retry_policy, paused)
		VALUES ($1, 'report.build', 'default', 60000, '{}', true)`, string(f.tenant))

	f.backdate(f.submit(f.newJob()).ID, 90*time.Second)
	f.submit(f.newJob())
	f.submit(f.newJob(func(nj *NewJob) { nj.Priority = domain.PriorityHigh }))
	for range 2 { // older, but beta may start only one more job
		f.backdate(f.submit(f.newJob(func(nj *NewJob) { nj.TenantID = beta })).ID, 10*time.Minute)
	}
	f.backdate(f.submit(f.newJob(func(nj *NewJob) { nj.Type = "report.build" })).ID, 20*time.Minute)

	g, err := f.store.SamplePoolGauges(ctx, "default", map[domain.TenantID]int{beta: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !mapsEqual(g.Ready, map[domain.Priority]int{domain.PriorityNormal: 3, domain.PriorityHigh: 1}) {
		t.Errorf("Ready = %v, want acme's two NORMAL and one HIGH, plus the one job beta's cap allows", g.Ready)
	}
	if !mapsEqual(g.Held, map[string]int{HeldTenantCap: 1, HeldJobTypePaused: 1}) {
		t.Errorf("Held = %v", g.Held)
	}
	if g.OldestAge < 90*time.Second || g.OldestAge > 2*time.Minute || g.OldestDue.IsZero() {
		t.Errorf("oldest dispatchable job due %v ago (%v), want about 90s: a cap-limited tenant doesn't set the age", g.OldestAge, g.OldestDue)
	}
	if g.BacklogTarget != nil {
		t.Errorf("BacklogTarget = %v, want the platform default", *g.BacklogTarget)
	}
	if g, err = f.store.SamplePoolGauges(ctx, "default", map[domain.TenantID]int{beta: 5}); err != nil ||
		g.Held[HeldTenantCap] != 0 || g.Ready[domain.PriorityNormal] != 4 || g.OldestAge < 10*time.Minute {
		t.Errorf("beta within its cap: Ready %v, Held %v, oldest %v (%v); want its jobs dispatchable and setting the age",
			g.Ready, g.Held, g.OldestAge, err)
	}

	target := 10 * time.Minute
	if err := f.store.SetPoolSettings(ctx, "default", &target, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetPoolPaused(ctx, "default", true, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	g, err = f.store.SamplePoolGauges(ctx, "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Ready) != 0 || !g.OldestDue.IsZero() || !mapsEqual(g.Held, map[string]int{HeldPoolPaused: 6}) {
		t.Errorf("paused pool: Ready %v, oldest %v, Held %v; want everything held by the pool pause", g.Ready, g.OldestDue, g.Held)
	}
	if g.BacklogTarget == nil || *g.BacklogTarget != target {
		t.Errorf("BacklogTarget = %v, want %v", g.BacklogTarget, target)
	}
}

func TestOwnerRecordsBacklogForAdmission(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	job := f.submit(f.newJob())
	f.backdate(job.ID, 90*time.Second)
	lease := f.poolLease()
	g, err := f.store.SamplePoolGauges(ctx, "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPoolBacklog(ctx, lease, "default", g.OldestDue); err != nil {
		t.Fatal(err)
	}
	about90s := func(d time.Duration) bool { return d >= 90*time.Second && d < 2*time.Minute }
	backlogs, err := f.store.PoolBacklogs(ctx)
	if b, ok := backlogs["default"]; err != nil || !ok || !about90s(b.Age) || b.Target != nil {
		t.Fatalf("PoolBacklogs = %v, %v; want default at about 90s with the default target", backlogs, err)
	}

	deposed := Lease{Name: lease.Name, Holder: "old-owner", Epoch: lease.Epoch - 1}
	if err := f.store.RecordPoolBacklog(ctx, deposed, "default", time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPoolBacklog(ctx, lease, "other", time.Time{}); err == nil {
		t.Error("recorded a pool under another pool's lease")
	}
	target := 10 * time.Minute
	if err := f.store.SetPoolSettings(ctx, "default", &target, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if b := mustBacklogs(t, f)["default"]; !about90s(b.Age) || b.Target == nil || *b.Target != target {
		t.Errorf("after a deposed owner's write and a target change: %+v", b)
	}
	pools, err := f.store.ListPools(ctx)
	if err != nil || len(pools) != 1 || pools[0].BacklogAge == nil || !about90s(*pools[0].BacklogAge) ||
		pools[0].BacklogTarget == nil || *pools[0].BacklogTarget != target {
		t.Errorf("ListPools = %+v, %v", pools, err)
	}

	f.exec(`UPDATE pools SET sampled_at = now() - interval '2 minutes'`)
	if b, ok := mustBacklogs(t, f)["default"]; ok {
		t.Errorf("a stale sample was used: %+v", b)
	}
	if pools, _ := f.store.ListPools(ctx); pools[0].BacklogAge != nil {
		t.Errorf("ListPools reports a stale backlog age %v", *pools[0].BacklogAge)
	}
	if n := f.count(`SELECT count(*) FROM audit_log WHERE action = 'pool.settings' AND target = 'default'
		AND details->>'backlog_target' = '10m0s'`); n != 1 {
		t.Errorf("pool.settings audit rows = %d, want 1", n)
	}
	if err := f.store.SetPoolSettings(ctx, "default", nil, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	if pools, _ := f.store.ListPools(ctx); pools[0].BacklogTarget != nil {
		t.Errorf("target after reset = %v, want the default", *pools[0].BacklogTarget)
	}
}

func mustBacklogs(t *testing.T, f *fixture) map[string]PoolBacklog {
	t.Helper()
	b, err := f.store.PoolBacklogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPoolsWithWorkOrWorkersNeedAnOwner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	check := func(wantPools []string, wantUnowned int) {
		t.Helper()
		pools, err := f.store.WantedPools(ctx)
		if err != nil || !slices.Equal(pools, wantPools) {
			t.Errorf("WantedPools = %v, %v; want %v", pools, err, wantPools)
		}
		if n, err := f.store.UnownedPools(ctx); err != nil || n != wantUnowned {
			t.Errorf("UnownedPools = %d, %v; want %d", n, err, wantUnowned)
		}
	}
	check(nil, 0)
	job := f.submit(f.newJob()) // READY with no workers anywhere
	check([]string{"default"}, 1)
	f.poolLease()
	check([]string{"default"}, 0)
	f.session("other")
	check([]string{"default", "other"}, 1)
	if _, err := f.store.RequestCancel(ctx, f.tenant, job.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	check([]string{"other"}, 1)
}

func mapsEqual[K comparable](a, b map[K]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
