package dispatch

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
)

// nodeSample holds the engine-wide gauges this node reports.
type nodeSample struct {
	unowned     int
	clockOffset time.Duration
}

// registerGauges reports the latest samples at each collection: the pools this node owns,
// and the node's own view (ADR-020). A pool's series move with its lease.
func (d *Dispatcher) registerGauges() (metric.Registration, error) {
	return observability.Meter().RegisterCallback(d.observe, observability.JobsReady, observability.JobsHeld,
		observability.OldestReadyAge, observability.BacklogTarget, observability.JobsRunning, observability.WorkerSlots,
		observability.PoolsUnowned, observability.DBClockOffset)
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
		for _, reason := range postgres.HeldReasons {
			o.ObserveInt64(observability.JobsHeld, int64(g.Held[reason]),
				metric.WithAttributes(name, attribute.String("reason", reason)))
		}
		o.ObserveFloat64(observability.OldestReadyAge, g.OldestAge.Seconds(), metric.WithAttributes(name))
		o.ObserveFloat64(observability.BacklogTarget, d.backlogTarget(g).Seconds(), metric.WithAttributes(name))
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

// backlogTarget is the pool's own target, or the platform default.
func (d *Dispatcher) backlogTarget(g *postgres.PoolGauges) time.Duration {
	if g.BacklogTarget != nil {
		return *g.BacklogTarget
	}
	return d.cfg.BacklogTarget
}

// initPoolCounters creates the pool-scoped counters at zero. A series that first appears
// at 1 hides that increment from increase(), and these events are rare.
func initPoolCounters(pools []string) {
	for _, p := range pools {
		attrs := metric.WithAttributes(attribute.String("pool", p))
		observability.SessionsExpired.Add(context.Background(), 0, attrs)
		observability.StaleCompletions.Add(context.Background(), 0, attrs)
		observability.PoolOwnerChanges.Add(context.Background(), 0, attrs)
	}
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
		if err := p.sample(); err != nil {
			p.logError("sampling gauges", err)
		}
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sample reads the pool's gauges and records its dispatchable backlog, which api nodes shed
// on (ADR-021). The backlog is recorded first, so admission never lags what metrics show.
func (p *pool) sample() error {
	caps, err := p.d.tenantCaps(p.ctx)
	if err != nil {
		return err
	}
	g, err := p.d.store.SamplePoolGauges(p.ctx, p.name, caps)
	if err != nil {
		return err
	}
	if lease, ok := p.d.leases.Lease(p.lease.Name); ok {
		err = p.d.store.RecordPoolBacklog(p.ctx, lease, p.name, g.OldestDue)
	}
	p.gauges.Store(&g)
	return err
}

// recordDispatch measures jobs handed to a worker: how long each waited in READY, and how long
// it waited once a worker was free for it (NFR-4), from the later of its ready time and the
// poll's arrival. Ready and start times are on the database clock and the poll's wait on this
// node's monotonic clock, so the two clocks are never compared.
func (p *pool) recordDispatch(jobs []domain.Job, polled time.Time) {
	free := time.Since(polled)
	for _, j := range jobs {
		attrs := metric.WithAttributes(attribute.String("pool", p.name), attribute.String("priority", j.Priority.String()))
		wait := max(0, j.Current.StartedAt.Sub(j.ReadyAt))
		observability.QueueWait.Record(context.Background(), wait.Seconds(), attrs)
		observability.DispatchLatency.Record(context.Background(), min(wait, free).Seconds(), attrs)
	}
}
