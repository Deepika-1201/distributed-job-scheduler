// Package coordination holds leases for pool ownership and singleton duties (LLD §11).
package coordination

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

// Store is the lease persistence; *postgres.Store implements it.
type Store interface {
	AcquireLease(ctx context.Context, name, holder, address string, ttl time.Duration) (postgres.Lease, bool, error)
	RenewLease(ctx context.Context, l postgres.Lease, ttl time.Duration) (postgres.Lease, error)
	ReleaseLease(ctx context.Context, l postgres.Lease) error
}

// TTL classes from HLD §15.
const (
	PoolTTL      = 10 * time.Second
	SingletonTTL = 30 * time.Second
	// Margin is how long before the database expiry a holder stops acting on a lease.
	Margin = 2 * time.Second
)

type Config struct {
	Name    string // component name for logs, e.g. "singleton-leases"
	Holder  string // unique per process start
	Address string // advertised to nodes that don't hold the lease
	TTL     time.Duration
	Margin  time.Duration
	// Wanted lists the leases to hold; nil means the names of Duties.
	Wanted func() []string
	// Duties run while their lease is held; their context ends when it is lost.
	Duties map[string]func(context.Context)
	// OnAcquired and OnLost are called without the manager's lock held.
	OnAcquired func(postgres.Lease)
	OnLost     func(name string)
}

// Manager acquires, renews and releases a set of leases. A lease counts as held only until
// renewal start + TTL − margin on this node's monotonic clock, so a node that can't reach the
// database stops acting before another node can take the lease over.
type Manager struct {
	cfg   Config
	store Store
	log   *slog.Logger
	now   func() time.Time

	mu     sync.Mutex
	held   map[string]*heldLease
	duties sync.WaitGroup
}

type heldLease struct {
	lease      postgres.Lease
	validUntil time.Time
	expiry     *time.Timer
	stopDuty   context.CancelFunc
}

func NewManager(store Store, cfg Config, log *slog.Logger) *Manager {
	if cfg.Wanted == nil {
		names := make([]string, 0, len(cfg.Duties))
		for name := range cfg.Duties {
			names = append(names, name)
		}
		cfg.Wanted = func() []string { return names }
	}
	return &Manager{cfg: cfg, store: store, log: log.With("component", cfg.Name), now: time.Now, held: map[string]*heldLease{}}
}

func (m *Manager) Name() string { return m.cfg.Name }

// Lease returns the named lease while it is held and locally valid.
func (m *Manager) Lease(name string) (postgres.Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.held[name]
	if !ok || !m.now().Before(h.validUntil) {
		return postgres.Lease{}, false
	}
	return h.lease, true
}

// Run renews and acquires leases every TTL/3 until ctx ends, then releases them and waits
// for duties to stop.
func (m *Manager) Run(ctx context.Context) error {
	interval := m.cfg.TTL / 3
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			m.releaseAll()
			m.duties.Wait()
			return nil
		case <-timer.C:
		}
		round, cancel := context.WithTimeout(ctx, interval)
		m.renew(round)
		m.acquire(round)
		cancel()
		timer.Reset(interval)
	}
}

func (m *Manager) renew(ctx context.Context) {
	m.mu.Lock()
	leases := make([]*heldLease, 0, len(m.held))
	for _, h := range m.held {
		leases = append(leases, h)
	}
	m.mu.Unlock()
	for _, h := range leases {
		start := m.now()
		renewed, err := m.store.RenewLease(ctx, h.lease, m.cfg.TTL)
		switch {
		case errors.Is(err, domain.ErrLeaseLost):
			m.log.Warn("lease lost to another holder", "lease", h.lease.Name, "epoch", h.lease.Epoch)
			m.drop(h)
		case err != nil:
			m.log.Warn("lease renewal failed; keeping it while locally valid", "lease", h.lease.Name, "error", err)
		default:
			m.mu.Lock()
			if m.held[h.lease.Name] == h {
				h.lease, h.validUntil = renewed, start.Add(m.cfg.TTL-m.cfg.Margin)
				h.expiry.Reset(h.validUntil.Sub(m.now()))
				m.mu.Unlock()
				continue
			}
			m.mu.Unlock()
			// It expired locally while the renewal was in flight; hand it over rather than hold it idle.
			m.release(renewed)
		}
	}
}

func (m *Manager) acquire(ctx context.Context) {
	for _, name := range m.cfg.Wanted() {
		m.mu.Lock()
		_, held := m.held[name]
		m.mu.Unlock()
		if held {
			continue
		}
		start := m.now()
		l, ok, err := m.store.AcquireLease(ctx, name, m.cfg.Holder, m.cfg.Address, m.cfg.TTL)
		if err != nil {
			m.log.Warn("lease acquisition failed", "lease", name, "error", err)
			continue
		}
		if !ok {
			continue
		}
		h := &heldLease{lease: l, validUntil: start.Add(m.cfg.TTL - m.cfg.Margin)}
		m.mu.Lock()
		m.held[name] = h // before the timer exists, so an immediate expiry still finds it
		h.expiry = time.AfterFunc(h.validUntil.Sub(m.now()), func() {
			m.log.Warn("lease expired before it could be renewed; stopped acting on it", "lease", name, "epoch", l.Epoch)
			m.drop(h)
		})
		if duty, ok := m.cfg.Duties[name]; ok {
			var dctx context.Context
			dctx, h.stopDuty = context.WithCancel(context.Background())
			m.duties.Go(func() { duty(dctx) })
		}
		m.mu.Unlock()
		m.log.Info("lease acquired", "lease", name, "epoch", l.Epoch)
		if m.cfg.OnAcquired != nil {
			m.cfg.OnAcquired(l)
		}
	}
}

// drop forgets a lease once, stopping its duty and notifying OnLost.
func (m *Manager) drop(h *heldLease) {
	m.mu.Lock()
	if m.held[h.lease.Name] != h {
		m.mu.Unlock()
		return
	}
	delete(m.held, h.lease.Name)
	h.expiry.Stop()
	m.mu.Unlock()
	if h.stopDuty != nil {
		h.stopDuty()
	}
	if m.cfg.OnLost != nil {
		m.cfg.OnLost(h.lease.Name)
	}
}

func (m *Manager) release(l postgres.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.ReleaseLease(ctx, l); err != nil {
		m.log.Warn("lease release failed; it will expire", "lease", l.Name, "error", err)
	}
}

// releaseAll stops acting on every lease, then releases them so other nodes take over at once.
func (m *Manager) releaseAll() {
	m.mu.Lock()
	leases := make([]*heldLease, 0, len(m.held))
	for _, h := range m.held {
		leases = append(leases, h)
	}
	m.mu.Unlock()
	for _, h := range leases {
		m.drop(h)
		m.release(h.lease)
		m.log.Info("lease released", "lease", h.lease.Name, "epoch", h.lease.Epoch)
	}
}
