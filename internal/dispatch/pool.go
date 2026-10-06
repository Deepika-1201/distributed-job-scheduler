package dispatch

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

var urgency = []domain.Priority{domain.PriorityCritical, domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}

// waiter is one Poll waiting on the pool's owner. Exactly one of deliver and abandon wins.
type waiter struct {
	session domain.WorkerSession
	max     int
	polled  time.Time // when the poll arrived: the worker has been free since
	ready   chan struct{}

	mu   sync.Mutex
	done bool
	jobs []domain.Job
}

// deliver hands claimed jobs to the poll. It returns false if the poll has already gone,
// in which case the caller must release the jobs.
func (w *waiter) deliver(jobs []domain.Job) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done, w.jobs = true, jobs
	close(w.ready)
	return true
}

// abandon withdraws the waiter and returns any jobs delivered in the meantime.
func (w *waiter) abandon() []domain.Job {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done = true
	return w.jobs
}

func (w *waiter) waiting() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.done
}

// pool dispatches one pool while this node holds its lease.
type pool struct {
	d        *Dispatcher
	name     string
	lease    postgres.Lease
	selector *domain.PrioritySelector // used only by the run goroutine

	mu    sync.Mutex
	queue []*waiter
	kick  chan struct{}

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once

	gauges atomic.Pointer[postgres.PoolGauges] // latest sample, reported while owned

	// Run goroutine only: capped tenants last seen at their cap, and when each was last seen with
	// room again, so that dispatch latency leaves out time held by the cap (ADR-022).
	atCap    map[domain.TenantID]bool
	capFreed map[domain.TenantID]time.Time
}

func newPool(d *Dispatcher, name string, l postgres.Lease, selector *domain.PrioritySelector) *pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &pool{d: d, name: name, lease: l, selector: selector, kick: make(chan struct{}, 1), ctx: ctx, cancel: cancel,
		atCap: map[domain.TenantID]bool{}, capFreed: map[domain.TenantID]time.Time{}}
}

func (p *pool) stop() { p.stopOnce.Do(p.cancel) }

// wait queues a poll and blocks until jobs are delivered, the wait elapses, the pool stops or
// the caller goes away. Jobs delivered to a caller that has gone are released.
func (p *pool) wait(ctx context.Context, sess domain.WorkerSession, limit int, wait time.Duration) []domain.Job {
	w := &waiter{session: sess, max: limit, polled: time.Now(), ready: make(chan struct{})}
	p.mu.Lock()
	if p.ctx.Err() != nil {
		p.mu.Unlock()
		return nil
	}
	p.queue = append(p.queue, w)
	p.mu.Unlock()
	select {
	case p.kick <- struct{}{}:
	default:
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-w.ready:
		return w.jobs
	case <-timer.C:
		return w.abandon()
	case <-p.ctx.Done():
		return w.abandon()
	case <-ctx.Done():
		p.release(w.abandon())
		return nil
	}
}

func (p *pool) run() {
	ticker := time.NewTicker(p.d.cfg.RoundInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.kick:
		case <-ticker.C:
		}
		p.round()
	}
}

// waiting drops finished waiters and returns the rest, oldest first.
func (p *pool) waiting() []*waiter {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queue = slices.DeleteFunc(p.queue, func(w *waiter) bool { return !w.waiting() })
	return slices.Clone(p.queue)
}

// capState tracks, within one round, which tenants are at their cap and how many more jobs
// each capped tenant may start.
type capState struct {
	allow map[domain.TenantID]int
	skip  []domain.TenantID
}

func (c *capState) claimed(jobs []domain.Job) {
	for _, j := range jobs {
		left, capped := c.allow[j.TenantID]
		if !capped {
			continue
		}
		if left <= 1 {
			delete(c.allow, j.TenantID)
			c.skip = append(c.skip, j.TenantID)
		} else {
			c.allow[j.TenantID] = left - 1
		}
	}
}

func (p *pool) caps(ctx context.Context) (*capState, error) {
	caps, err := p.d.tenantCaps(ctx)
	if err != nil || len(caps) == 0 {
		return &capState{}, err
	}
	tenants := make([]domain.TenantID, 0, len(caps))
	for t := range caps {
		tenants = append(tenants, t)
	}
	running, err := p.d.store.RunningCounts(ctx, p.name, tenants)
	if err != nil {
		return nil, err
	}
	cs := &capState{allow: map[domain.TenantID]int{}}
	now := time.Now()
	for t, limit := range caps {
		if left := limit - running[t]; left > 0 {
			cs.allow[t] = left
			if p.atCap[t] {
				delete(p.atCap, t)
				p.capFreed[t] = now
			}
		} else {
			cs.skip = append(cs.skip, t)
			p.atCap[t] = true
		}
	}
	return cs, nil
}

// round serves waiting polls in arrival order (LLD §12.3).
func (p *pool) round() {
	waiters := p.waiting()
	if len(waiters) == 0 {
		return
	}
	lease, ok := p.d.leases.Lease(p.lease.Name)
	if !ok {
		return // the lease manager stops this pool
	}
	has, err := p.d.store.ReadyPriorities(p.ctx, p.name)
	if err != nil {
		p.logError("reading ready classes", err)
		return
	}
	cs, err := p.caps(p.ctx)
	if err != nil {
		p.logError("reading tenant caps", err)
		return
	}
	sup := &supply{has: has, dry: map[string]map[domain.Priority]bool{}}
	for _, w := range waiters {
		if !sup.any("") {
			return
		}
		types := typesKey(w.session.JobTypes)
		if !sup.any(types) {
			continue
		}
		jobs, err := p.fill(lease, w, types, sup, cs)
		if len(jobs) > 0 {
			if w.deliver(jobs) {
				p.recordDispatch(jobs, w.polled)
			} else {
				p.release(jobs)
			}
		}
		if err != nil {
			if !errors.Is(err, domain.ErrLeaseLost) {
				p.logError("claiming jobs", err)
			}
			return
		}
	}
}

// supply tracks, within one round, the priority classes that may still hold claimable jobs.
// A claim that comes back short empties its class for every waiter if the waiter takes any
// job type, and otherwise for the later waiters with the same job types (LLD §12.3).
type supply struct {
	has map[domain.Priority]bool
	dry map[string]map[domain.Priority]bool // by typesKey
}

func (s *supply) open(types string, pr domain.Priority) bool { return s.has[pr] && !s.dry[types][pr] }

func (s *supply) any(types string) bool {
	return slices.ContainsFunc(urgency, func(pr domain.Priority) bool { return s.open(types, pr) })
}

func (s *supply) ranDry(types string, pr domain.Priority) {
	if types == "" {
		s.has[pr] = false
		return
	}
	if s.dry[types] == nil {
		s.dry[types] = map[domain.Priority]bool{}
	}
	s.dry[types][pr] = true
}

// typesKey identifies a waiter's set of job types; "" means any type.
func typesKey(types []string) string {
	if len(types) == 0 {
		return ""
	}
	sorted := slices.Clone(types)
	slices.Sort(sorted)
	return "\x00" + strings.Join(slices.Compact(sorted), "\x00")
}

// fill claims up to w.max jobs, allocating slots one at a time across priority classes with
// the smooth weighted round-robin selector, then filling leftovers in urgency order.
func (p *pool) fill(lease postgres.Lease, w *waiter, types string, sup *supply, cs *capState) ([]domain.Job, error) {
	var got []domain.Job
	claim := func(pr domain.Priority, n int) error {
		jobs, err := p.d.store.ClaimReady(p.ctx, postgres.ClaimRequest{Lease: lease, Pool: p.name, Priority: pr, Limit: n,
			SessionID: w.session.ID, JobTypes: w.session.JobTypes, SkipTenants: cs.skip, Allowances: cs.allow})
		if err != nil {
			return err
		}
		got = append(got, jobs...)
		cs.claimed(jobs)
		if len(jobs) < n {
			sup.ranDry(types, pr)
		}
		return nil
	}
	plan := allocate(p.selector, w.max, func(pr domain.Priority) bool { return sup.open(types, pr) })
	for _, pr := range urgency {
		if n := plan[pr]; n > 0 {
			if err := claim(pr, n); err != nil {
				return got, err
			}
		}
	}
	for _, pr := range urgency {
		if left := w.max - len(got); left > 0 && sup.open(types, pr) {
			if err := claim(pr, left); err != nil {
				return got, err
			}
		}
	}
	return got, nil
}

// allocate splits n slots across the classes with work, one slot per selector turn, so the
// weights hold per job rather than per batch.
func allocate(s *domain.PrioritySelector, n int, hasWork func(domain.Priority) bool) map[domain.Priority]int {
	plan := map[domain.Priority]int{}
	for range n {
		pr, ok := s.Next(hasWork)
		if !ok {
			break
		}
		plan[pr]++
	}
	return plan
}

// release returns jobs that were claimed but never delivered to READY (T23).
func (p *pool) release(jobs []domain.Job) {
	if len(jobs) == 0 {
		return
	}
	refs := make([]postgres.AttemptRef, len(jobs))
	for i, j := range jobs {
		refs[i] = postgres.AttemptRef{JobID: j.ID, AttemptID: j.Current.ID}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.d.store.ReleaseAttempts(ctx, refs); err != nil {
		p.logError("releasing undelivered jobs", err)
	}
}

func (p *pool) logError(what string, err error) {
	if p.ctx.Err() == nil {
		p.d.log.Error("dispatch round failed", "pool", p.name, "step", what, "error", err)
	}
}
