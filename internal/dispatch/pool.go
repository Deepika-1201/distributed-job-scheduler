package dispatch

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

var urgency = []domain.Priority{domain.PriorityCritical, domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}

// waiter is one Poll waiting on the pool's owner. Exactly one of deliver and abandon wins.
type waiter struct {
	session domain.WorkerSession
	max     int
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
}

func newPool(d *Dispatcher, name string, l postgres.Lease, selector *domain.PrioritySelector) *pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &pool{d: d, name: name, lease: l, selector: selector, kick: make(chan struct{}, 1), ctx: ctx, cancel: cancel}
}

func (p *pool) stop() { p.stopOnce.Do(p.cancel) }

// wait queues a poll and blocks until jobs are delivered, the wait elapses, the pool stops or
// the caller goes away. Jobs delivered to a caller that has gone are released.
func (p *pool) wait(ctx context.Context, sess domain.WorkerSession, limit int, wait time.Duration) []domain.Job {
	w := &waiter{session: sess, max: limit, ready: make(chan struct{})}
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
	for t, limit := range caps {
		if left := limit - running[t]; left > 0 {
			cs.allow[t] = left
		} else {
			cs.skip = append(cs.skip, t)
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
	for _, w := range waiters {
		if !slices.ContainsFunc(urgency, func(pr domain.Priority) bool { return has[pr] }) {
			return
		}
		jobs, err := p.fill(lease, w, has, cs)
		if len(jobs) > 0 && !w.deliver(jobs) {
			p.release(jobs)
		}
		if err != nil {
			if !errors.Is(err, domain.ErrLeaseLost) {
				p.logError("claiming jobs", err)
			}
			return
		}
	}
}

// fill claims up to w.max jobs, allocating slots one at a time across priority classes with
// the smooth weighted round-robin selector, then filling leftovers in urgency order.
func (p *pool) fill(lease postgres.Lease, w *waiter, has map[domain.Priority]bool, cs *capState) ([]domain.Job, error) {
	typed := len(w.session.JobTypes) > 0
	avail := make(map[domain.Priority]bool, len(has))
	for pr, ok := range has {
		avail[pr] = ok
	}
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
			avail[pr] = false
			if !typed {
				has[pr] = false // nothing left in the class for anyone
			}
		}
		return nil
	}
	plan := allocate(p.selector, w.max, func(pr domain.Priority) bool { return avail[pr] })
	for _, pr := range urgency {
		if n := plan[pr]; n > 0 {
			if err := claim(pr, n); err != nil {
				return got, err
			}
		}
	}
	for _, pr := range urgency {
		if left := w.max - len(got); left > 0 && avail[pr] {
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
