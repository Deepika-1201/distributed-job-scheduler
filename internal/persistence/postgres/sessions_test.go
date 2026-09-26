package postgres

import (
	"errors"
	"slices"
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

func (f *fixture) session(pool string, jobTypes ...string) domain.WorkerSession {
	f.t.Helper()
	ws, err := f.store.CreateSession(ctx, domain.WorkerSession{Pool: pool, WorkerID: "test-worker", JobTypes: jobTypes,
		Slots: 4, Labels: map[string]string{"zone": "a"}}, 30*time.Second)
	if err != nil {
		f.t.Fatalf("CreateSession: %v", err)
	}
	return ws
}

func (f *fixture) claimFor(ws domain.WorkerSession, limit int, mod func(*ClaimRequest)) []domain.Job {
	f.t.Helper()
	req := ClaimRequest{Lease: f.poolLease(), Pool: "default", Priority: domain.PriorityNormal, Limit: limit,
		SessionID: ws.ID, JobTypes: ws.JobTypes}
	if mod != nil {
		mod(&req)
	}
	got, err := f.store.ClaimReady(ctx, req)
	if err != nil {
		f.t.Fatalf("ClaimReady: %v", err)
	}
	return got
}

func TestSessionLifecycle(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ws := f.session("default", "email.send")
	if ws.State != domain.SessionActive || !slices.Equal(ws.JobTypes, []string{"email.send"}) || ws.Labels["zone"] != "a" {
		t.Fatalf("session = %+v", ws)
	}
	if pools, err := f.store.ActivePools(ctx); err != nil || !slices.Equal(pools, []string{"default"}) {
		t.Errorf("ActivePools = %v, %v", pools, err)
	}
	if _, err := f.store.Heartbeat(ctx, ws.ID, nil, 30*time.Second); err != nil {
		t.Errorf("Heartbeat: %v", err)
	}
	if err := f.store.CloseSession(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Heartbeat(ctx, ws.ID, nil, 30*time.Second); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("heartbeat on a closed session = %v, want ErrNotFound", err)
	}
	if _, err := f.store.GetActiveSession(ctx, ws.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetActiveSession after close = %v", err)
	}
	if pools, _ := f.store.ActivePools(ctx); len(pools) != 0 {
		t.Errorf("pools after close = %v", pools)
	}
}

func TestClaimFiltersByJobType(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.exec(`INSERT INTO job_types (tenant_id, name, pool, attempt_timeout_ms, retry_policy)
		VALUES ($1, 'reports.build', 'default', 60000, '{}')`, string(f.tenant))
	email := f.submit(f.newJob())
	report := f.submit(f.newJob(func(nj *NewJob) { nj.Type = "reports.build" }))

	got := f.claimFor(f.session("default", "reports.build"), 10, nil)
	if len(got) != 1 || got[0].ID != report.ID {
		t.Fatalf("typed claim = %v, want only %s", ids(got), report.ID)
	}
	if got := f.claimFor(f.session("default"), 10, nil); len(got) != 1 || got[0].ID != email.ID {
		t.Errorf("untyped claim = %v, want %s", ids(got), email.ID)
	}
}

func TestClaimRespectsTenantAllowances(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	capped := f.addTenant("capped")
	for range 5 {
		f.submit(f.newJob(func(nj *NewJob) { nj.TenantID = capped }))
	}
	for range 3 {
		f.submit(f.newJob())
	}
	got := f.claimFor(f.session("default"), 10, func(r *ClaimRequest) {
		r.Allowances = map[domain.TenantID]int{capped: 2}
	})
	per := map[domain.TenantID]int{}
	for _, j := range got {
		per[j.TenantID]++
	}
	if per[capped] != 2 || per[f.tenant] != 3 {
		t.Errorf("claimed %v, want 2 for the capped tenant and 3 for the other", per)
	}
	if n := f.count(`SELECT count(*) FROM jobs WHERE state = 'READY'`); n != 3 {
		t.Errorf("%d jobs left READY, want 3 (locked candidates must be released)", n)
	}
}

func TestReleaseRefundsTheAttempt(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	ws := f.session("default")
	job := f.claimFor(ws, 1, nil)[0]
	ref := AttemptRef{JobID: job.ID, AttemptID: job.Current.ID}
	if n, err := f.store.ReleaseAttempts(ctx, []AttemptRef{ref}); err != nil || n != 1 {
		t.Fatalf("ReleaseAttempts = %d, %v", n, err)
	}
	got, _ := f.store.GetJob(ctx, f.tenant, job.ID)
	if got.State != domain.StateReady || got.Budget.Attempts != 0 || !got.Budget.StartedAt.IsZero() || got.AttemptCount != 1 {
		t.Errorf("released job: %s, budget %+v, attempt count %d", got.State, got.Budget, got.AttemptCount)
	}
	if n, _ := f.store.ReleaseAttempts(ctx, []AttemptRef{ref}); n != 0 {
		t.Error("released a stale attempt")
	}
	again := f.claimFor(ws, 1, nil)[0]
	if again.Current.Number != 2 {
		t.Errorf("next attempt number %d, want 2: fencing tokens must keep increasing", again.Current.Number)
	}
}

func TestHeartbeatReconcilesAttempts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for range 3 {
		f.submit(f.newJob())
	}
	ws := f.session("default")
	jobs := f.claimFor(ws, 3, nil)
	reported, cancelled, forgotten := jobs[0], jobs[1], jobs[2]
	if _, err := f.store.RequestCancel(ctx, f.tenant, cancelled.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	stale := AttemptRef{JobID: reported.ID, AttemptID: "0191f000-0000-7000-8000-00000000dead"}
	// The forgotten attempt started long enough ago to count as undelivered.
	f.exec(`UPDATE jobs SET attempt_started_at = now() - interval '1 minute' WHERE id = $1`, string(forgotten.ID))

	res, err := f.store.Heartbeat(ctx, ws.ID, []AttemptRef{
		{JobID: reported.ID, AttemptID: reported.Current.ID}, {JobID: cancelled.ID, AttemptID: cancelled.Current.ID}, stale,
	}, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Cancel, []domain.AttemptID{cancelled.Current.ID}) || !slices.Equal(res.Stale, []domain.AttemptID{stale.AttemptID}) {
		t.Errorf("heartbeat = %+v", res)
	}
	if got, _ := f.store.GetJob(ctx, f.tenant, forgotten.ID); got.State != domain.StateReady {
		t.Errorf("unreported attempt's job is %s, want READY", got.State)
	}
	if got, _ := f.store.GetJob(ctx, f.tenant, reported.ID); got.State != domain.StateRunning {
		t.Errorf("reported attempt's job is %s, want RUNNING", got.State)
	}
}

func TestCloseSessionLosesHeldAttempts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	ws := f.session("default")
	job := f.claimFor(ws, 1, nil)[0]
	if err := f.store.CloseSession(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := f.store.GetJob(ctx, f.tenant, job.ID)
	if got.State != domain.StateRetryPending || got.Budget.Lost != 1 {
		t.Errorf("job after close: %s, lost %d; want RETRY_PENDING after a lost attempt", got.State, got.Budget.Lost)
	}
}
