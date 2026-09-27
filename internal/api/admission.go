package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

const (
	defaultQuotaTTL   = 30 * time.Second
	defaultPendingTTL = 5 * time.Second
	defaultBacklogTTL = 2 * time.Second
	shedRetryAfter    = 30 * time.Second
)

// admission applies tenant quotas and load shedding on this node (ADR-018, ADR-021, LLD §15.3).
type admission struct {
	store                            *postgres.Store
	now                              func() time.Time
	defaultRate                      float64 // per node
	replicas                         int
	defaultMinInterval               time.Duration
	backlogTarget                    time.Duration // for pools without their own
	quotaTTL, pendingTTL, backlogTTL time.Duration

	mu         sync.Mutex
	quotas     map[domain.TenantID]cached[domain.Quotas]
	pending    map[domain.TenantID]cached[int]
	backlog    map[string]postgres.PoolBacklog
	saturated  bool
	sampledAt  time.Time
	refreshing bool
}

type cached[T any] struct {
	value   T
	expires time.Time
}

// tenantQuotas returns the tenant's configured quotas, cached for quotaTTL.
func (a *admission) tenantQuotas(ctx context.Context, tenant domain.TenantID) (domain.Quotas, error) {
	a.mu.Lock()
	c, ok := a.quotas[tenant]
	a.mu.Unlock()
	if ok && a.now().Before(c.expires) {
		return c.value, nil
	}
	t, err := a.store.GetTenant(ctx, tenant)
	if err != nil {
		return domain.Quotas{}, err
	}
	a.mu.Lock()
	a.quotas[tenant] = cached[domain.Quotas]{value: t.Quotas, expires: a.now().Add(a.quotaTTL)}
	a.mu.Unlock()
	return t.Quotas, nil
}

func (a *admission) forget(tenant domain.TenantID) {
	a.mu.Lock()
	delete(a.quotas, tenant)
	delete(a.pending, tenant)
	a.mu.Unlock()
}

// rate is the tenant's per-node share of its submission rate.
func (a *admission) rate(q domain.Quotas) float64 {
	if q.RateLimit != nil {
		return *q.RateLimit / float64(a.replicas)
	}
	return a.defaultRate
}

func (a *admission) payloadLimit(q domain.Quotas) int {
	if q.MaxPayloadBytes != nil {
		return min(*q.MaxPayloadBytes, domain.MaxPayloadBytes)
	}
	return domain.MaxPayloadBytes
}

func (a *admission) minInterval(q domain.Quotas) time.Duration {
	if q.MinScheduleInterval != nil {
		return *q.MinScheduleInterval
	}
	return a.defaultMinInterval
}

// checkPending enforces max_pending with a bounded count, cached for pendingTTL.
func (a *admission) checkPending(ctx context.Context, tenant domain.TenantID, q domain.Quotas) error {
	if q.MaxPending == nil {
		return nil
	}
	limit := *q.MaxPending
	a.mu.Lock()
	c, ok := a.pending[tenant]
	a.mu.Unlock()
	if !ok || !a.now().Before(c.expires) {
		n, err := a.store.CountPending(ctx, tenant, limit)
		if err != nil {
			return err
		}
		c = cached[int]{value: n, expires: a.now().Add(a.pendingTTL)}
		a.mu.Lock()
		a.pending[tenant] = c
		a.mu.Unlock()
	}
	if c.value >= limit {
		return &apiError{status: http.StatusTooManyRequests, code: "quota_exceeded", retryAfter: max(a.pendingTTL, time.Second),
			message: fmt.Sprintf("the tenant has reached its max_pending quota of %d unfinished jobs", limit)}
	}
	return nil
}

// shed rejects LOW submissions to a pool past its backlog target, and NORMAL ones past three
// times it; HIGH and CRITICAL pass. A pool without a fresh owner sample never sheds.
func (a *admission) shed(ctx context.Context, pool string, p domain.Priority) error {
	if p >= domain.PriorityHigh {
		return nil
	}
	b, sampled, saturated := a.sample(ctx, pool)
	limit := a.target(b)
	if p == domain.PriorityNormal {
		limit *= 3
	}
	if (sampled && b.Age > limit) || (saturated && p == domain.PriorityLow) {
		return &apiError{status: http.StatusServiceUnavailable, code: "overloaded", retryAfter: shedRetryAfter,
			message: fmt.Sprintf("the platform is shedding %s submissions for pool %s; retry later or raise the priority", p, pool)}
	}
	return nil
}

func (a *admission) target(b postgres.PoolBacklog) time.Duration {
	if b.Target != nil {
		return *b.Target
	}
	return a.backlogTarget
}

// sample returns the pool's backlog, whether its owner sampled it recently, and whether the
// database is saturated. One request refreshes a stale copy while the others use the previous one.
func (a *admission) sample(ctx context.Context, pool string) (postgres.PoolBacklog, bool, bool) {
	a.mu.Lock()
	stale := !a.now().Before(a.sampledAt.Add(a.backlogTTL))
	if stale && !a.refreshing {
		a.refreshing = true
		a.mu.Unlock()
		backlogs, err := a.store.PoolBacklogs(ctx)
		saturated := a.store.Saturated()
		a.mu.Lock()
		a.refreshing = false
		if err == nil {
			a.backlog, a.sampledAt = backlogs, a.now()
		}
		a.saturated = saturated
	}
	defer a.mu.Unlock()
	b, ok := a.backlog[pool]
	return b, ok, a.saturated
}
