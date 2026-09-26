package postgres

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

func (f *fixture) expireLease(name string) {
	f.exec(`UPDATE leases SET expires_at = now() - interval '1 second' WHERE name = $1`, name)
}

func TestLeaseAcquireRenewAndTakeover(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const name = "singleton:test"
	a, ok, err := f.store.AcquireLease(ctx, name, "node-a", "10.0.0.1:7000", time.Minute)
	if err != nil || !ok || a.Epoch != 1 || a.Address != "10.0.0.1:7000" {
		t.Fatalf("first acquire = %+v, %v, %v", a, ok, err)
	}
	if _, ok, err := f.store.AcquireLease(ctx, name, "node-b", "", time.Minute); err != nil || ok {
		t.Fatalf("acquire of a held lease = %v, %v; want false", ok, err)
	}
	if _, err := f.store.RenewLease(ctx, a, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}

	// An expired lease nobody took over can still be renewed: the epoch is unchanged.
	f.expireLease(name)
	if a, err = f.store.RenewLease(ctx, a, time.Minute); err != nil {
		t.Fatalf("renew after expiry without takeover: %v", err)
	}

	f.expireLease(name)
	b, ok, err := f.store.AcquireLease(ctx, name, "node-b", "", time.Minute)
	if err != nil || !ok || b.Epoch != 2 || b.Holder != "node-b" {
		t.Fatalf("takeover = %+v, %v, %v; want node-b at epoch 2", b, ok, err)
	}
	if _, err := f.store.RenewLease(ctx, a, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
		t.Errorf("stale renew = %v, want ErrLeaseLost", err)
	}
	if err := f.store.ReleaseLease(ctx, a); err != nil {
		t.Fatal(err)
	}
	if cur, _ := f.store.GetLease(ctx, name); cur.Holder != "node-b" || !cur.ExpiresAt.After(time.Now()) {
		t.Errorf("a stale release changed the lease: %+v", cur)
	}

	if err := f.store.ReleaseLease(ctx, b); err != nil {
		t.Fatal(err)
	}
	if c, ok, err := f.store.AcquireLease(ctx, name, "node-c", "", time.Minute); err != nil || !ok || c.Epoch != 3 {
		t.Errorf("acquire after release = %+v, %v, %v; want epoch 3 at once", c, ok, err)
	}
}

func TestConcurrentAcquireHasOneWinner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var (
		wg      sync.WaitGroup
		winners atomic.Int32
	)
	for i := range 16 {
		wg.Go(func() {
			_, ok, err := f.store.AcquireLease(ctx, "pool:default", uuid.NewString(), "", time.Minute)
			if err != nil {
				t.Errorf("acquirer %d: %v", i, err)
			}
			if ok {
				winners.Add(1)
			}
		})
	}
	wg.Wait()
	if n := winners.Load(); n != 1 {
		t.Errorf("%d winners, want 1", n)
	}
}

func TestClaimIsFencedByThePoolLease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	job := f.submit(f.newJob())
	stale := f.poolLease()
	f.expireLease(stale.Name)
	owner, ok, err := f.store.AcquireLease(ctx, stale.Name, "node-b", "", time.Minute)
	if err != nil || !ok {
		t.Fatalf("takeover: %v, %v", ok, err)
	}

	req := ClaimRequest{Lease: stale, Pool: "default", Priority: domain.PriorityNormal, Limit: 10, SessionID: domain.SessionID(uuid.NewString())}
	if got, err := f.store.ClaimReady(ctx, req); !errors.Is(err, domain.ErrLeaseLost) || len(got) != 0 {
		t.Fatalf("stale owner claimed %d jobs, err %v; want ErrLeaseLost", len(got), err)
	}
	if got, _ := f.store.GetJob(ctx, f.tenant, job.ID); got.State != domain.StateReady {
		t.Fatalf("job is %s after a fenced claim, want READY", got.State)
	}
	req.Lease = owner
	if got, err := f.store.ClaimReady(ctx, req); err != nil || len(got) != 1 {
		t.Errorf("owner claimed %d jobs, err %v; want 1", len(got), err)
	}
	if got, err := f.store.ClaimReady(ctx, req); err != nil || len(got) != 0 {
		t.Errorf("claim from an empty queue = %d jobs, %v; want none and no error", len(got), err)
	}
	req.Pool = "reports"
	if _, err := f.store.ClaimReady(ctx, req); err == nil {
		t.Error("claimed from a pool under another pool's lease")
	}
}
