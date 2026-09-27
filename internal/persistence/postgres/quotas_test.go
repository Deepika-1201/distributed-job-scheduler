package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

func TestRunningCountsArePerPool(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.exec(`INSERT INTO job_types (tenant_id, name, pool, attempt_timeout_ms, retry_policy)
		VALUES ($1, 'reports.build', 'reports', 60000, '{}')`, string(f.tenant))
	f.submit(f.newJob())
	f.submit(f.newJob(func(nj *NewJob) { nj.Type, nj.Pool = "reports.build", "reports" }))
	reports, ok, err := f.store.AcquireLease(ctx, PoolLeaseName("reports"), "test-node", "", time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease: %v, %v", ok, err)
	}
	f.claim(1)
	if _, err := f.store.ClaimReady(ctx, ClaimRequest{Lease: reports, Pool: "reports", Priority: domain.PriorityNormal, Limit: 1,
		SessionID: domain.SessionID(uuid.NewString())}); err != nil {
		t.Fatal(err)
	}
	for _, pool := range []string{"default", "reports"} {
		counts, err := f.store.RunningCounts(ctx, pool, []domain.TenantID{f.tenant})
		if err != nil || counts[f.tenant] != 1 {
			t.Errorf("%s: running = %v, %v; want 1 per pool", pool, counts, err)
		}
	}
}

func TestCountPendingStopsAtTheLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for range 5 {
		f.submit(f.newJob())
	}
	for limit, want := range map[int]int{3: 3, 10: 5} {
		if n, err := f.store.CountPending(ctx, f.tenant, limit); err != nil || n != want {
			t.Errorf("CountPending(limit %d) = %d, %v; want %d", limit, n, err, want)
		}
	}
}
