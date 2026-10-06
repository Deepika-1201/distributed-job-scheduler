package dispatch_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/pgtest"
	"jobscheduler/internal/recovery"
	"jobscheduler/pkg/workerpb"
	"jobscheduler/pkg/workersdk"
)

// Fault-injection tests for HLD §14.5's engine and database scenarios (LLD §21).

// engineVia starts an engine whose database connections go through a fault proxy, with its
// own reaper as in production. It returns the engine's address, the proxy and a stop.
func (c *cluster) engineVia(node string, reaper recovery.ReaperConfig, mods ...func(*dispatch.Config)) (string, *pgtest.Proxy, func()) {
	c.t.Helper()
	proxy, url := pgtest.NewProxy(c.t, c.url)
	pool, err := postgres.NewPool(ctx, url, 16)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(pool.Close)
	store := postgres.NewStore(pool)
	addr, stop := c.startEngine(store, node, mods...)
	c.run(recovery.NewReaper(store, reaper, slog.New(slog.DiscardHandler)).Run)
	return addr, proxy, stop
}

// held runs until released and records whether its context ended first.
type held struct {
	started, release, cancelled chan struct{}
}

func newHeld() *held {
	return &held{started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
}

func (h *held) handler(ctx context.Context, _ workersdk.Job) ([]byte, error) {
	close(h.started)
	select {
	case <-h.release:
		return []byte(`{"held": true}`), nil
	case <-ctx.Done():
		close(h.cancelled)
		return nil, ctx.Err()
	}
}

func (h *held) awaitStart(t *testing.T) {
	t.Helper()
	select {
	case <-h.started:
	case <-time.After(15 * time.Second):
		t.Fatal("the handler never started")
	}
}

// S6: the database is unreachable for every engine, refused or blackholed, for several session
// TTLs. Workers keep their handlers running, and the outcome lands once it is back (ADR-029).
func TestWorkersRideOutADatabaseOutage(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"cut", "stall"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			c := newCluster(t)
			addr, proxy, _ := c.engineVia("node-a", recovery.ReaperConfig{Interval: 100 * time.Millisecond, WarmUp: 2 * time.Second},
				func(cfg *dispatch.Config) {
					cfg.SessionTTL, cfg.HeartbeatInterval = 2*time.Second, 200*time.Millisecond
				})
			h := newHeld()
			c.worker(addr, 1, map[string]workersdk.Handler{"email.send": h.handler})
			id := c.submit()
			h.awaitStart(t)

			if fault == "cut" {
				proxy.Cut()
			} else {
				proxy.Stall()
			}
			select {
			case <-h.cancelled:
				t.Fatal("the handler was cancelled during the outage")
			case <-time.After(6 * time.Second):
			}
			proxy.Restore()
			close(h.release)
			job := c.await(id, domain.StateSucceeded, 20*time.Second)
			if a := c.attempts(id); len(a) != 1 || string(job.Result) != `{"held": true}` {
				t.Errorf("attempts = %+v, result %s; want the one attempt that rode out the outage", a, job.Result)
			}
		})
	}
}

// S12: one engine loses the database while another keeps it. The healthy node takes the pool
// over and runs new work. The cut-off node's worker rides the partition out, and the healthy
// node's reaper must not expire its session meanwhile, or its job would run twice (ADR-029).
func TestEnginePartitionedFromTheDatabase(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	fast := func(cfg *dispatch.Config) {
		cfg.SessionTTL, cfg.HeartbeatInterval, cfg.PoolLeaseTTL = 2*time.Second, 200*time.Millisecond, 2*time.Second
		cfg.OutageTolerance = 20 * time.Second
	}
	reaper := recovery.ReaperConfig{Interval: 100 * time.Millisecond, WarmUp: 2 * time.Second, OutageTolerance: 20 * time.Second}
	addrA, proxyA, _ := c.engineVia("node-a", reaper, fast)
	long := newHeld()
	c.worker(addrA, 1, map[string]workersdk.Handler{"email.send": long.handler})
	waitForOwner(t, c.store, "node-a")
	first := c.submit()
	long.awaitStart(t)

	addrB := c.engine("node-b", fast)
	c.run(recovery.NewReaper(c.store, reaper, slog.New(slog.DiscardHandler)).Run)
	c.worker(addrB, 1, map[string]workersdk.Handler{
		"email.send": func(context.Context, workersdk.Job) ([]byte, error) { return nil, nil },
	})

	proxyA.Cut()
	second := c.submit()
	c.await(second, domain.StateSucceeded, 20*time.Second)
	if l, err := c.store.GetLease(ctx, postgres.PoolLeaseName("default")); err != nil || l.Holder != "node-b" || l.Epoch < 2 {
		t.Errorf("pool lease = %+v (%v); want node-b at a later epoch", l, err)
	}
	time.Sleep(3 * time.Second) // the cut-off worker's session lease has long passed
	if job, err := c.store.GetJob(ctx, c.tenant, first); err != nil || job.State != domain.StateRunning {
		t.Fatalf("first job = %s (%v) during the partition; want it still running on the cut-off worker", job.State, err)
	}
	proxyA.Restore()
	close(long.release)
	c.await(first, domain.StateSucceeded, 20*time.Second)
	if a := c.attempts(first); len(a) != 1 {
		t.Errorf("first job attempts = %+v; want one: its session must outlive the partition", a)
	}
}

// S1 and S11: the pool owner crashes. Another node takes the pool over within the lease TTL plus
// one acquire round, sessions renewed elsewhere survive, and work keeps flowing.
func TestEngineCrashHandsPoolsOver(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	const leaseTTL = 2 * time.Second
	fast := func(cfg *dispatch.Config) {
		cfg.SessionTTL, cfg.HeartbeatInterval, cfg.PoolLeaseTTL = 3*time.Second, 300*time.Millisecond, leaseTTL
	}
	addrA, proxyA, stopA := c.engineVia("node-a", recovery.ReaperConfig{}, fast)
	registerVia(t, addrA) // node-a wants the pool first, so it becomes the owner
	waitForOwner(t, c.store, "node-a")
	addrB := c.engine("node-b", fast)

	long := newHeld()
	c.worker(addrB, 2, map[string]workersdk.Handler{"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
		if job.Labels["mode"] == "long" {
			return long.handler(ctx, job)
		}
		return nil, nil
	}})
	first := c.submit(func(nj *postgres.NewJob) { nj.Labels = map[string]string{"mode": "long"} })
	long.awaitStart(t)

	crashed := time.Now()
	proxyA.Cut() // node-a can neither renew nor release its leases
	go stopA()
	for {
		l, err := c.store.GetLease(ctx, postgres.PoolLeaseName("default"))
		if err == nil && l.Holder == "node-b" {
			break
		}
		if time.Since(crashed) > 10*time.Second {
			t.Fatalf("node-b never took the pool over: %+v (%v)", l, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The lease TTL plus one acquire round (TTL/3), and slack for a loaded machine.
	if took := time.Since(crashed); took > leaseTTL+leaseTTL/3+2*time.Second {
		t.Errorf("takeover took %s", took)
	}
	second := c.submit()
	c.await(second, domain.StateSucceeded, 15*time.Second)
	close(long.release)
	c.await(first, domain.StateSucceeded, 15*time.Second)
	if a := c.attempts(first); len(a) != 1 {
		t.Errorf("first job attempts = %+v; its worker's session should have survived the failover", a)
	}
}

// registerVia registers a throwaway session through addr, making that node want the pool.
func registerVia(t *testing.T, addr string) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	if _, err := workerpb.NewWorkerServiceClient(conn).Register(callCtx,
		&workerpb.RegisterRequest{Pool: "default", Slots: 1, WorkerId: "probe"}); err != nil {
		t.Fatal(err)
	}
}
