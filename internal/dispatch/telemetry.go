package dispatch

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
)

// nodeSample holds the engine-wide gauges this node reports.
type nodeSample struct {
	unowned     int
	clockOffset time.Duration
}

// registerGauges reports the latest samples at each collection: the pools this node owns,
// and the node's own view (ADR-020). A pool's series move with its lease.
func (d *Dispatcher) registerGauges() (metric.Registration, error) {
	return observability.Meter().RegisterCallback(d.observe, observability.JobsReady, observability.OldestReadyAge,
		observability.JobsRunning, observability.WorkerSlots, observability.PoolsUnowned, observability.DBClockOffset)
}

func (d *Dispatcher) observe(_ context.Context, o metric.Observer) error {
	d.mu.Lock()
	pools := make([]*pool, 0, len(d.pools))
	for _, p := range d.pools {
		pools = append(pools, p)
	}
	d.mu.Unlock()
	for _, p := range pools {
		g := p.gauges.Load()
		if g == nil {
			continue
		}
		name := attribute.String("pool", p.name)
		for _, pr := range urgency {
			o.ObserveInt64(observability.JobsReady, int64(g.Ready[pr]),
				metric.WithAttributes(name, attribute.String("priority", pr.String())))
		}
		o.ObserveFloat64(observability.OldestReadyAge, g.OldestReady.Seconds(), metric.WithAttributes(name))
		for tenant, n := range g.Running {
			o.ObserveInt64(observability.JobsRunning, int64(n),
				metric.WithAttributes(name, attribute.String("tenant", string(tenant))))
		}
		o.ObserveInt64(observability.WorkerSlots, int64(g.SlotsBusy), metric.WithAttributes(name, attribute.String("state", "busy")))
		o.ObserveInt64(observability.WorkerSlots, int64(g.SlotsFree), metric.WithAttributes(name, attribute.String("state", "free")))
	}
	if n := d.node.Load(); n != nil {
		o.ObserveInt64(observability.PoolsUnowned, int64(n.unowned))
		o.ObserveFloat64(observability.DBClockOffset, n.clockOffset.Seconds())
	}
	return nil
}

// sampleNode refreshes the node's gauges every MetricsInterval until ctx ends.
func (d *Dispatcher) sampleNode(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		unowned, err := d.store.UnownedPools(ctx)
		var offset time.Duration
		if err == nil {
			offset, err = d.store.ClockOffset(ctx)
		}
		if err == nil {
			d.node.Store(&nodeSample{unowned: unowned, clockOffset: offset})
		} else if ctx.Err() == nil {
			d.log.Warn("sampling node gauges failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sampleGauges refreshes the pool's gauges every MetricsInterval while this node owns it.
func (p *pool) sampleGauges() {
	ticker := time.NewTicker(p.d.cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		if g, err := p.d.store.SamplePoolGauges(p.ctx, p.name); err == nil {
			p.gauges.Store(&g)
		} else {
			p.logError("sampling gauges", err)
		}
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// recordDispatch measures READY to attempt start for jobs handed to a worker; both times
// are on the database clock.
func (p *pool) recordDispatch(jobs []domain.Job) {
	for _, j := range jobs {
		observability.DispatchLatency.Record(context.Background(), max(0, j.Current.StartedAt.Sub(j.ReadyAt).Seconds()),
			metric.WithAttributes(attribute.String("pool", p.name), attribute.String("priority", j.Priority.String())))
	}
}
