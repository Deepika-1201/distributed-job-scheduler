package coordination

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/pgtest"
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

const (
	testTTL    = 2 * time.Second
	testMargin = 500 * time.Millisecond
	duty       = "singleton:test"
)

func newStore(t *testing.T) *postgres.Store {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), pgtest.NewDatabase(t), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewStore(pool)
}

// dutyProbe records when its duty runs and stops.
type dutyProbe struct {
	running atomic.Bool
	stopped atomic.Int64 // UnixNano of the last stop
	started atomic.Int64 // UnixNano of the last start
}

func (p *dutyProbe) run(ctx context.Context) {
	p.started.Store(time.Now().UnixNano())
	p.running.Store(true)
	<-ctx.Done()
	p.running.Store(false)
	p.stopped.Store(time.Now().UnixNano())
}

func startManager(t *testing.T, store Store, holder string, probe *dutyProbe) (*Manager, func()) {
	t.Helper()
	m := NewManager(store, Config{Name: holder, Holder: holder, TTL: testTTL, Margin: testMargin,
		Duties: map[string]func(context.Context){duty: probe.run}}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	stop := func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("%s: Run = %v", holder, err)
		}
	}
	t.Cleanup(func() { cancel() })
	return m, stop
}

func waitFor(t *testing.T, what string, cond func() bool, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestManagerHandsOverOnShutdown(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	var pa, pb dutyProbe
	a, stopA := startManager(t, store, "node-a", &pa)
	waitFor(t, "node-a to run the duty", pa.running.Load, 5*time.Second)
	b, stopB := startManager(t, store, "node-b", &pb)
	defer stopB()

	time.Sleep(testTTL) // node-b keeps trying while node-a renews
	if pb.running.Load() {
		t.Fatal("both nodes ran the singleton duty")
	}
	la, _ := a.Lease(duty)
	stopA()
	if pa.running.Load() {
		t.Fatal("node-a's duty still ran after shutdown")
	}
	// Release makes the takeover immediate rather than waiting for expiry.
	waitFor(t, "node-b to take over", pb.running.Load, testTTL)
	if lb, ok := b.Lease(duty); !ok || lb.Epoch != la.Epoch+1 {
		t.Errorf("node-b lease %+v (held %v), want epoch %d", lb, ok, la.Epoch+1)
	}
}

// partitionedStore fails lease calls, as if the node lost its database connection.
type partitionedStore struct {
	Store
	down atomic.Bool
}

var errPartitioned = errors.New("connection refused")

func (p *partitionedStore) RenewLease(ctx context.Context, l postgres.Lease, ttl time.Duration) (postgres.Lease, error) {
	if p.down.Load() {
		return postgres.Lease{}, errPartitioned
	}
	return p.Store.RenewLease(ctx, l, ttl)
}

func (p *partitionedStore) AcquireLease(ctx context.Context, name, holder, address string, ttl time.Duration) (postgres.Lease, bool, error) {
	if p.down.Load() {
		return postgres.Lease{}, false, errPartitioned
	}
	return p.Store.AcquireLease(ctx, name, holder, address, ttl)
}

func TestManagerSelfFencesBeforeTakeover(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	flaky := &partitionedStore{Store: store}
	var pa, pb dutyProbe
	a, _ := startManager(t, flaky, "node-a", &pa)
	waitFor(t, "node-a to run the duty", pa.running.Load, 5*time.Second)
	_, stopB := startManager(t, store, "node-b", &pb)
	defer stopB()

	flaky.down.Store(true)
	waitFor(t, "node-a to stop its duty", func() bool { return !pa.running.Load() }, testTTL)
	if _, ok := a.Lease(duty); ok {
		t.Error("node-a still reports the lease after self-fencing")
	}
	waitFor(t, "node-b to take over", pb.running.Load, 2*testTTL)
	if gap := time.Duration(pb.started.Load() - pa.stopped.Load()); gap <= 0 {
		t.Errorf("node-b started %v before node-a stopped: the duty ran twice", -gap)
	}
}
