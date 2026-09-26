package postgres

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

func (f *fixture) claim(n int) []domain.Job {
	f.t.Helper()
	got, err := f.store.ClaimReady(ctx, ClaimRequest{
		Lease: f.poolLease(), Pool: "default", Priority: domain.PriorityNormal, Limit: n, SessionID: domain.SessionID(uuid.NewString()),
	})
	if err != nil {
		f.t.Fatalf("ClaimReady: %v", err)
	}
	return got
}

func (f *fixture) claimOne() domain.Job {
	f.t.Helper()
	got := f.claim(1)
	if len(got) != 1 {
		f.t.Fatalf("claimed %d jobs, want 1", len(got))
	}
	return got[0]
}

func TestClaimReadyFiltersAndOrders(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	older := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = now.Add(-2 * time.Minute) }))
	newer := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = now.Add(-time.Minute) }))
	f.submit(f.newJob(func(nj *NewJob) { nj.Priority = domain.PriorityHigh }))
	f.submit(f.newJob(func(nj *NewJob) { nj.Pool = "reports" }))
	f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = now.Add(time.Hour) }))
	f.submit(f.newJob(func(nj *NewJob) { nj.StartDeadline = now.Add(-time.Second) }))
	other := f.addTenant("globex")
	f.submit(f.newJob(func(nj *NewJob) { nj.TenantID = other }))

	session := domain.SessionID(uuid.NewString())
	got, err := f.store.ClaimReady(ctx, ClaimRequest{
		Lease: f.poolLease(), Pool: "default", Priority: domain.PriorityNormal, Limit: 10, SessionID: session,
		SkipTenants: []domain.TenantID{other},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != older.ID || got[1].ID != newer.ID {
		t.Fatalf("claimed %v, want [%s %s] in run_at order", ids(got), older.ID, newer.ID)
	}
	for _, j := range got {
		c := j.Current
		switch {
		case j.State != domain.StateRunning || c == nil:
			t.Errorf("%s: state %s, current %+v", j.ID, j.State, c)
		case j.AttemptCount != 1 || c.Number != 1 || j.Budget.Attempts != 1:
			t.Errorf("%s: attempt %d/%d, budget %d, want 1", j.ID, j.AttemptCount, c.Number, j.Budget.Attempts)
		case c.SessionID != session || c.ID == "":
			t.Errorf("%s: session %s, attempt id %q", j.ID, c.SessionID, c.ID)
		case c.Deadline.Sub(c.StartedAt) != time.Minute || !j.Budget.StartedAt.Equal(c.StartedAt):
			t.Errorf("%s: started %v, deadline %v, budget start %v", j.ID, c.StartedAt, c.Deadline, j.Budget.StartedAt)
		}
	}
	if again := f.claim(10); len(again) != 1 || again[0].TenantID != other {
		t.Errorf("second claim = %v, want only the other tenant's job", ids(again))
	}
}

func TestClaimReadyRespectsLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for range 5 {
		f.submit(f.newJob())
	}
	if got := f.claim(3); len(got) != 3 {
		t.Errorf("claimed %d, want 3", len(got))
	}
	if got := f.claim(3); len(got) != 2 {
		t.Errorf("claimed %d, want the remaining 2", len(got))
	}
}

func TestConcurrentClaimersNeverShareAJob(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const total = 200
	for range total {
		f.submit(f.newJob())
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = map[domain.JobID]int{}
		lease   = f.poolLease()
	)
	for range 8 {
		wg.Go(func() {
			session := domain.SessionID(uuid.NewString())
			for {
				got, err := f.store.ClaimReady(ctx, ClaimRequest{
					Lease: lease, Pool: "default", Priority: domain.PriorityNormal, Limit: 7, SessionID: session,
				})
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, j := range got {
					claimed[j.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimed) != total {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("job %s claimed %d times", id, n)
		}
	}
	if n := f.count(`SELECT count(*) FROM jobs WHERE state = 'RUNNING' AND attempt_count = 1`); n != total {
		t.Errorf("%d jobs running with exactly one attempt, want %d", n, total)
	}
}

func ids(jobs []domain.Job) []domain.JobID {
	out := make([]domain.JobID, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
