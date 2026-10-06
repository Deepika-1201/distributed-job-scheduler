package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/pgtest"
	"jobscheduler/internal/scheduling"
	"jobscheduler/pkg/workersdk"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m, func(ctx context.Context, url string) error {
		pool, err := postgres.NewPool(ctx, url, 2)
		if err != nil {
			return err
		}
		defer pool.Close()
		return postgres.Migrate(ctx, pool, slog.New(slog.DiscardHandler))
	}))
}

const token = "test-worker-token"

var ctx = context.Background()

type cluster struct {
	t      *testing.T
	url    string
	store  *postgres.Store
	tenant domain.TenantID
}

// newCluster creates a database with tenant "acme", job type "email.send" in pool "default"
// with fast retries, and a promoter so retries become READY again.
func newCluster(t *testing.T) *cluster {
	t.Helper()
	url := pgtest.NewDatabase(t)
	pool, err := postgres.NewPool(ctx, url, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.NewStore(pool)
	if err := store.EnsurePartitions(ctx, time.Now(), 2); err != nil {
		t.Fatal(err)
	}
	tenant, err := store.CreateTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.DefaultRetryPolicy()
	policy.InitialDelay, policy.Jitter, policy.MaxAttempts = time.Second, domain.JitterNone, 3
	if _, err := store.CreateJobType(ctx, domain.JobType{TenantID: tenant, Name: "email.send", Pool: "default",
		DefaultPriority: domain.PriorityNormal, AttemptTimeout: time.Minute, RetryPolicy: policy, Enabled: true},
		postgres.Audit{Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, url: url, store: store, tenant: tenant}
	c.run(scheduling.NewPromoter(store, slog.New(slog.DiscardHandler)).Run)
	return c
}

func (c *cluster) run(fn func(context.Context) error) func() {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = fn(runCtx); close(done) }()
	stop := func() { cancel(); <-done }
	c.t.Cleanup(stop)
	return stop
}

// engine starts an engine node's worker server and returns its address.
func (c *cluster) engine(node string, mods ...func(*dispatch.Config)) string {
	addr, _ := c.stoppableEngine(node, mods...)
	return addr
}

// stoppableEngine starts an engine node and returns its address and a graceful stop.
func (c *cluster) stoppableEngine(node string, mods ...func(*dispatch.Config)) (string, func()) {
	return c.startEngine(c.store, node, mods...)
}

func (c *cluster) startEngine(store *postgres.Store, node string, mods ...func(*dispatch.Config)) (string, func()) {
	c.t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	cfg := dispatch.Config{NodeID: node, AdvertiseAddr: lis.Addr().String(), Token: token,
		Listener: lis, RoundInterval: 20 * time.Millisecond}
	for _, mod := range mods {
		mod(&cfg)
	}
	d, err := dispatch.New(store, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		c.t.Fatal(err)
	}
	stop := c.run(d.Run)
	return lis.Addr().String(), stop
}

func (c *cluster) worker(addr string, slots int, handlers map[string]workersdk.Handler) {
	c.run(func(ctx context.Context) error {
		err := workersdk.Run(ctx, workersdk.Config{Address: addr, Token: token, Pool: "default", Slots: slots,
			Handlers: handlers, PollWait: time.Second, DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
		if err != nil {
			c.t.Errorf("worker: %v", err)
		}
		return err
	})
}

func (c *cluster) submit(mods ...func(*postgres.NewJob)) domain.JobID {
	c.t.Helper()
	nj := postgres.NewJob{TenantID: c.tenant, Type: "email.send", TypeVersion: 1, Pool: "default", Priority: domain.PriorityNormal,
		Payload: []byte(`{"to": "a@example.com"}`), CreatedBy: "test", AttemptTimeout: time.Minute, RetryPolicy: func() domain.RetryPolicy {
			p := domain.DefaultRetryPolicy()
			p.InitialDelay, p.Jitter, p.MaxAttempts = time.Second, domain.JitterNone, 3
			return p
		}()}
	for _, mod := range mods {
		mod(&nj)
	}
	res, err := c.store.SubmitJob(ctx, nj, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	return res.Job.ID
}

// await waits until the job reaches state and returns it.
func (c *cluster) await(id domain.JobID, state domain.JobState, within time.Duration) domain.Job {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for {
		job, err := c.store.GetJob(ctx, c.tenant, id)
		if err == nil && job.State == state {
			return job
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("job %s is %s (%v) after %s, want %s", id, job.State, err, within, state)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *cluster) attempts(id domain.JobID) []domain.Attempt {
	c.t.Helper()
	attempts, err := c.store.ListAttempts(ctx, c.tenant, id)
	if err != nil {
		c.t.Fatal(err)
	}
	return attempts
}

func TestJobOutcomesEndToEnd(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	addr := c.engine("node-a")
	var flaky atomic.Int32
	c.worker(addr, 4, map[string]workersdk.Handler{
		"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
			switch job.Labels["mode"] {
			case "flaky":
				if flaky.Add(1) == 1 {
					return nil, errors.New("smtp unavailable")
				}
			case "permanent":
				return nil, workersdk.Permanent(errors.New("invalid address"))
			case "slow":
				<-ctx.Done()
				return nil, ctx.Err()
			case "panic":
				panic("handler bug")
			}
			return fmt.Appendf(nil, `{"sent": true, "attempt": %d}`, job.Attempt), nil
		},
	})
	label := func(mode string) func(*postgres.NewJob) {
		return func(nj *postgres.NewJob) { nj.Labels = map[string]string{"mode": mode} }
	}
	ok := c.submit()
	flakyJob := c.submit(label("flaky"))
	permanent := c.submit(label("permanent"))
	slow := c.submit(label("slow"), func(nj *postgres.NewJob) { nj.AttemptTimeout = 300 * time.Millisecond; nj.RetryPolicy.MaxAttempts = 1 })
	panicky := c.submit(label("panic"), func(nj *postgres.NewJob) { nj.RetryPolicy.MaxAttempts = 1 })

	if job := c.await(ok, domain.StateSucceeded, 10*time.Second); string(job.Result) != `{"sent": true, "attempt": 1}` {
		t.Errorf("result = %s", job.Result)
	}
	c.await(flakyJob, domain.StateSucceeded, 15*time.Second)
	// A release before delivery (T23) uses up an attempt number, so only the order is fixed.
	if a := c.attempts(flakyJob); len(a) != 2 || a[0].State != domain.AttemptFailed || a[0].Error != "smtp unavailable" || a[1].Number <= a[0].Number {
		t.Errorf("flaky attempts = %+v", a)
	}
	if job := c.await(permanent, domain.StateFailed, 10*time.Second); job.Reason != domain.ReasonNonRetryable {
		t.Errorf("permanent failure reason = %s", job.Reason)
	}
	if job := c.await(slow, domain.StateDeadLettered, 10*time.Second); c.attempts(slow)[0].State != domain.AttemptTimedOut {
		t.Errorf("slow job attempts = %+v, reason %s", c.attempts(slow), job.Reason)
	}
	if a := c.attempts(panicky); len(a) != 1 || a[0].State != domain.AttemptFailed {
		t.Errorf("panicking handler attempts = %+v", a)
	}
}

func TestCancellationReachesTheHandler(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	started := make(chan string, 1)
	c.worker(c.engine("node-a"), 1, map[string]workersdk.Handler{
		"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
			started <- job.ID
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	id := c.submit()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	if _, err := c.store.RequestCancel(ctx, c.tenant, id, postgres.Audit{Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	// Delivered with the next heartbeat (every 5 s).
	job := c.await(id, domain.StateCancelled, 15*time.Second)
	if a := c.attempts(id); len(a) != 1 || a[0].State != domain.AttemptCancelled || job.Reason != domain.ReasonCancelled {
		t.Errorf("attempts = %+v, reason %s", a, job.Reason)
	}
}

func TestWorkerIsRedirectedToThePoolOwner(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	owner := c.engine("node-a")
	// A worker registering through node-a makes node-a the pool owner.
	var ranOn sync.Map
	handler := func(name string) workersdk.Handler {
		return func(ctx context.Context, job workersdk.Job) ([]byte, error) {
			ranOn.Store(job.ID, name)
			return nil, nil
		}
	}
	c.worker(owner, 1, map[string]workersdk.Handler{"email.send": handler("first")})
	waitForOwner(t, c.store, "node-a")

	other := c.engine("node-b")
	c.worker(other, 2, map[string]workersdk.Handler{"email.send": handler("redirected")})
	for range 20 {
		c.submit()
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		redirected := false
		ranOn.Range(func(_, v any) bool { redirected = redirected || v == "redirected"; return !redirected })
		if redirected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the worker connected to node-b never got work: it was not redirected to the owner")
		}
		c.submit()
		time.Sleep(100 * time.Millisecond)
	}
	if l, err := c.store.GetLease(ctx, postgres.PoolLeaseName("default")); err != nil || l.Holder != "node-a" || l.Epoch != 1 {
		t.Errorf("pool lease = %+v (%v); want node-a at epoch 1 throughout", l, err)
	}
}

func waitForOwner(t *testing.T, store *postgres.Store, node string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if l, err := store.GetLease(ctx, postgres.PoolLeaseName("default")); err == nil && l.Holder == node {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never became the pool owner", node)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestSlotsAreSharedByPriorityWeight(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	for range 30 {
		c.submit(func(nj *postgres.NewJob) { nj.Priority = domain.PriorityCritical })
		c.submit(func(nj *postgres.NewJob) { nj.Priority = domain.PriorityLow })
	}
	var mu sync.Mutex
	var order []string
	release := make(chan struct{})
	c.worker(c.engine("node-a"), 9, map[string]workersdk.Handler{
		"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
			mu.Lock()
			order = append(order, job.Priority)
			mu.Unlock()
			<-release
			return nil, nil
		},
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d handlers started", n)
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(release)
	counts := map[string]int{}
	mu.Lock()
	for _, p := range order[:9] {
		counts[p]++
	}
	mu.Unlock()
	// With weights 8:1, nine slots split 8 CRITICAL to 1 LOW.
	if counts["CRITICAL"] != 8 || counts["LOW"] != 1 {
		t.Errorf("first nine assignments = %v, want 8 CRITICAL and 1 LOW", counts)
	}
}

func TestUnauthenticatedWorkerIsRejected(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	err := workersdk.Run(ctx, workersdk.Config{Address: c.engine("node-a"), Token: "wrong", Pool: "default",
		Handlers: map[string]workersdk.Handler{"email.send": func(context.Context, workersdk.Job) ([]byte, error) { return nil, nil }},
		Logger:   slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("a worker with a wrong token registered")
	}
}
