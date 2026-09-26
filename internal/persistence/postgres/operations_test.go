package postgres

import (
	"testing"

	"jobscheduler/internal/domain"
)

func (f *fixture) operation(kind domain.OperationKind, filter domain.OperationFilter) domain.Operation {
	f.t.Helper()
	op, err := f.store.CreateOperation(ctx, domain.Operation{TenantID: f.tenant, Kind: kind, Filter: filter, CreatedBy: "test"}, testAudit)
	if err != nil {
		f.t.Fatalf("CreateOperation: %v", err)
	}
	return op
}

// drain processes batches until no operation is open and returns how many batches ran.
func (f *fixture) drain(batch int) int {
	f.t.Helper()
	n := 0
	for {
		found, err := f.store.ProcessOperation(ctx, batch)
		if err != nil {
			f.t.Fatalf("ProcessOperation: %v", err)
		}
		if !found {
			return n
		}
		if n++; n > 100 {
			f.t.Fatal("operation never finished")
		}
	}
}

func TestBulkCancelRespectsFilterAndScope(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	team := func(v string) func(*NewJob) {
		return func(nj *NewJob) { nj.Labels = map[string]string{"team": v} }
	}
	var targets []domain.JobID
	for range 5 {
		targets = append(targets, f.submit(f.newJob(team("a"))).ID)
	}
	other := f.submit(f.newJob(team("b")))
	running := f.submit(f.newJob(team("a")))
	f.claimOne() // the oldest READY job is now RUNNING and gets a cancel request instead
	op := f.operation(domain.OperationCancel, domain.OperationFilter{LabelKey: "team", LabelValue: "a"})
	late := f.submit(f.newJob(team("a")))

	if batches := f.drain(2); batches < 3 {
		t.Errorf("ran %d batches of 2 for 6 jobs, want at least 3", batches)
	}
	got, err := f.store.GetOperation(ctx, f.tenant, op.ID)
	if err != nil || got.State != domain.OperationSucceeded || got.Succeeded != 6 || got.Skipped != 0 || got.FinishedAt.IsZero() {
		t.Fatalf("operation = %+v, %v; want SUCCEEDED with 6 succeeded", got, err)
	}
	for _, id := range append(targets, running.ID) {
		if j, _ := f.store.GetJob(ctx, f.tenant, id); j.State != domain.StateCancelled && j.CancelRequestedAt.IsZero() {
			t.Errorf("job %s is %s without a cancel request", id, j.State)
		}
	}
	for _, id := range []domain.JobID{other.ID, late.ID} {
		if j, _ := f.store.GetJob(ctx, f.tenant, id); j.State != domain.StateReady {
			t.Errorf("job %s outside the operation is %s, want READY", id, j.State)
		}
	}
	if n := f.count(`SELECT count(*) FROM audit_log WHERE actor = $1`, "operation:"+op.ID); n != 6 {
		t.Errorf("%d audit rows by the operation, want 6", n)
	}
	if _, err := f.store.GetOperation(ctx, f.addTenant("globex"), op.ID); err == nil {
		t.Error("another tenant can read the operation")
	}
}

func TestBulkRedriveDeadLetteredJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var dead []domain.JobID
	for range 3 {
		f.submit(f.newJob(func(nj *NewJob) { nj.RetryPolicy.MaxAttempts = 1 }))
	}
	for range 3 {
		j := f.claimOne()
		f.complete(completion(j, retryable))
		dead = append(dead, j.ID)
	}
	f.operation(domain.OperationRedrive, domain.OperationFilter{State: domain.StateDeadLettered})
	f.drain(100)
	for _, id := range dead {
		if j, _ := f.store.GetJob(ctx, f.tenant, id); j.State != domain.StateReady || j.Budget.Attempts != 0 {
			t.Errorf("re-driven job %s: %s, budget %+v", id, j.State, j.Budget)
		}
	}
	// Running it again finds nothing left to re-drive.
	op := f.operation(domain.OperationRedrive, domain.OperationFilter{State: domain.StateDeadLettered})
	f.drain(100)
	if got, _ := f.store.GetOperation(ctx, f.tenant, op.ID); got.State != domain.OperationSucceeded || got.Succeeded != 0 {
		t.Errorf("second re-drive = %+v", got)
	}
}
