package dispatch_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/pkg/workersdk"
)

// The pool owner shuts down: it releases the pool lease, the surviving node takes over with
// a higher epoch, and the worker, redirected until then, keeps getting work (HLD S1).
func TestPoolOwnershipHandsOverWhenTheOwnerStops(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	ownerAddr, stopOwner := c.stoppableEngine("node-a")
	var done atomic.Int32
	handler := map[string]workersdk.Handler{"email.send": func(context.Context, workersdk.Job) ([]byte, error) {
		done.Add(1)
		return nil, nil
	}}
	// A first worker on node-a makes it the owner; the second worker, on node-b, is redirected.
	c.worker(ownerAddr, 1, handler)
	waitForOwner(t, c.store, "node-a")
	survivor := c.engine("node-b")
	c.worker(survivor, 2, handler)
	before, _ := c.store.GetLease(ctx, postgres.PoolLeaseName("default"))

	stopOwner()
	var ids []domain.JobID
	for range 5 {
		ids = append(ids, c.submit())
	}
	for _, id := range ids {
		c.await(id, domain.StateSucceeded, 20*time.Second)
	}
	after, err := c.store.GetLease(ctx, postgres.PoolLeaseName("default"))
	if err != nil || after.Holder != "node-b" || after.Epoch != before.Epoch+1 {
		t.Errorf("lease after handoff = %+v (%v); want node-b at epoch %d", after, err, before.Epoch+1)
	}
}
