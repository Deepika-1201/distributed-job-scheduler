package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability/telemetrytest"
)

func TestOnCommitRunsOnlyAfterCommit(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ran := 0
	rollback := errors.New("roll back")
	for _, want := range []error{rollback, nil} {
		err := f.store.inTx(ctx, func(tx pgx.Tx) error {
			onCommit(tx, func() { ran++ })
			return want
		})
		if !errors.Is(err, want) {
			t.Fatalf("inTx = %v, want %v", err, want)
		}
	}
	if ran != 1 {
		t.Errorf("hooks ran %d times, want once: for the committed transaction only", ran)
	}
}

func TestTransitionMetrics(t *testing.T) {
	t.Parallel()
	metrics, _ := telemetrytest.Install()
	before := telemetrytest.Scrape(t, metrics) // counters are cumulative across -count runs
	f := newFixture(t)
	// A job type and pool of their own keep these series apart from parallel tests.
	f.exec(`INSERT INTO job_types (tenant_id, name, pool, attempt_timeout_ms, retry_policy)
		VALUES ($1, 'metrics.job', 'metrics', 60000, '{}')`, string(f.tenant))
	job := func(maxAttempts int, runAt time.Time) NewJob {
		return f.newJob(func(nj *NewJob) {
			nj.Type, nj.Pool, nj.RunAt = "metrics.job", "metrics", runAt
			nj.RetryPolicy.MaxAttempts = maxAttempts
		})
	}
	f.submit(job(1, time.Time{}))
	f.submit(job(3, time.Time{}))
	cancelled := f.submit(job(3, time.Time{}))
	delayed := f.submit(job(3, time.Now().Add(time.Hour)))

	if _, err := f.store.RequestCancel(ctx, f.tenant, cancelled.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := f.store.AcquireLease(ctx, PoolLeaseName("metrics"), "test-node", "", time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireLease: %v, %v", ok, err)
	}
	ws := f.session("metrics")
	claimed, err := f.store.ClaimReady(ctx, ClaimRequest{Lease: lease, Pool: "metrics",
		Priority: domain.PriorityNormal, Limit: 2, SessionID: ws.ID})
	if err != nil || len(claimed) != 2 {
		t.Fatalf("ClaimReady = %d jobs, %v", len(claimed), err)
	}
	for _, j := range claimed { // one dead-letters, the other is retried
		f.complete(completion(j, retryable))
	}
	f.exec(`UPDATE jobs SET run_at = now() - interval '50 milliseconds' WHERE id = $1`, string(delayed.ID))
	if n, err := f.store.PromoteDue(ctx, 100); err != nil || n != 1 {
		t.Fatalf("PromoteDue = %d, %v", n, err)
	}
	f.exec(`UPDATE worker_sessions SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, string(ws.ID))
	if n, err := f.store.ExpireSessions(ctx, 100); err != nil || n != 1 {
		t.Fatalf("ExpireSessions = %d, %v", n, err)
	}
	f.createSchedule()
	f.materialize()

	text := telemetrytest.Scrape(t, metrics)
	rose := func(name string, labels map[string]string) float64 {
		n, _ := telemetrytest.Value(text, name, labels)
		prev, _ := telemetrytest.Value(before, name, labels)
		return n - prev
	}
	typ := map[string]string{"type": "metrics.job"}
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"jobs_completed_total", map[string]string{"type": "metrics.job", "state": "DEAD_LETTERED"}, 1},
		{"jobs_completed_total", map[string]string{"type": "metrics.job", "state": "CANCELLED"}, 1},
		{"attempts_total", map[string]string{"type": "metrics.job", "outcome": "FAILED"}, 2},
		{"execution_duration_seconds_count", typ, 2},
		{"jobs_retried_total", typ, 1},
		{"jobs_dead_lettered_total", map[string]string{"type": "metrics.job", "reason": string(domain.ReasonAttemptsExhausted)}, 1},
		{"scheduling_lag_seconds_count", map[string]string{"pool": "metrics"}, 1},
		{"worker_sessions_expired_total", map[string]string{"pool": "metrics"}, 1},
	} {
		if got := rose(c.name, c.labels); got != c.want {
			t.Errorf("%s%v rose by %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if lag := rose("scheduling_lag_seconds_sum", map[string]string{"pool": "metrics"}); lag < 0.05 {
		t.Errorf("scheduling lag = %vs, want at least the 50 ms the job was overdue", lag)
	}
	if n, _ := telemetrytest.Value(text, "jobs_scheduled_total", map[string]string{"tenant": string(f.tenant), "source": "schedule"}); n < 1 {
		t.Errorf("jobs_scheduled_total{source=schedule} = %v, want the materialized fires", n)
	}
	if n, _ := telemetrytest.Value(text, "db_transaction_duration_seconds_count", map[string]string{"operation": "CompleteAttempt"}); n < 2 {
		t.Errorf("db_transaction_duration_seconds_count{operation=CompleteAttempt} = %v, want at least 2", n)
	}
}

func TestSamplePoolGaugesAndUnownedPools(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob(func(nj *NewJob) { nj.Priority = domain.PriorityHigh }))
	f.submit(f.newJob())
	f.submit(f.newJob())
	ws := f.session("default") // 4 slots
	if n, err := f.store.UnownedPools(ctx); err != nil || n != 1 {
		t.Errorf("UnownedPools before a lease = %d, %v; want 1", n, err)
	}
	f.claimFor(ws, 1, nil)
	if n, err := f.store.UnownedPools(ctx); err != nil || n != 0 {
		t.Errorf("UnownedPools with a lease = %d, %v; want 0", n, err)
	}

	g, err := f.store.SamplePoolGauges(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if g.Ready[domain.PriorityHigh] != 1 || g.Ready[domain.PriorityNormal] != 1 || len(g.Ready) != 2 {
		t.Errorf("Ready = %v, want one HIGH and one NORMAL", g.Ready)
	}
	if g.Running[f.tenant] != 1 || len(g.Running) != 1 {
		t.Errorf("Running = %v, want one for the tenant", g.Running)
	}
	if g.SlotsBusy != 1 || g.SlotsFree != 3 {
		t.Errorf("slots busy %d, free %d; want 1 and 3", g.SlotsBusy, g.SlotsFree)
	}
	if g.OldestReady <= 0 || g.OldestReady > time.Minute {
		t.Errorf("OldestReady = %v", g.OldestReady)
	}
	if off, err := f.store.ClockOffset(ctx); err != nil || off.Abs() > time.Second {
		t.Errorf("ClockOffset = %v, %v; the test database shares this clock", off, err)
	}
}
