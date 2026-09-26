// Package recovery runs the reaper and the maintenance duty (LLD §13).
package recovery

import (
	"context"
	"log/slog"
	"time"

	"jobscheduler/internal/persistence/postgres"
)

// Store is what the reaper needs; *postgres.Store implements it.
type Store interface {
	Ping(ctx context.Context) error
	ExpireSessions(ctx context.Context, limit int) (int, error)
	LoseOrphanedAttempts(ctx context.Context, limit int) (int, error)
	TimeOutOverdueAttempts(ctx context.Context, grace time.Duration, limit int) (int, error)
}

// ReaperConfig holds the HLD §15 values; zero fields take the defaults.
type ReaperConfig struct {
	Interval time.Duration // 5 s
	WarmUp   time.Duration // one session TTL, 30 s
	Grace    time.Duration // after the attempt deadline, 30 s
}

const reapBatch = 100

// Reaper ends the attempts of dead workers and overdue attempts on every engine node.
type Reaper struct {
	cfg          ReaperConfig
	store        Store
	log          *slog.Logger
	now          func() time.Time
	healthySince time.Time
}

func NewReaper(store Store, cfg ReaperConfig, log *slog.Logger) *Reaper {
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.WarmUp == 0 {
		cfg.WarmUp = 30 * time.Second
	}
	if cfg.Grace == 0 {
		cfg.Grace = 30 * time.Second
	}
	return &Reaper{cfg: cfg, store: store, log: log.With("component", "reaper"), now: time.Now}
}

func (r *Reaper) Name() string { return "reaper" }

func (r *Reaper) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		wait := r.cfg.Interval
		full, err := r.step(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.log.Warn("reaper step failed; warm-up restarts", "error", err)
		case full:
			wait = 0
		}
		timer.Reset(wait)
	}
}

// step runs one pass and reports whether a batch was full. Until the node has been connected
// for WarmUp, it expires nothing: live workers first need a chance to renew (HLD S6).
func (r *Reaper) step(ctx context.Context) (bool, error) {
	if err := r.store.Ping(ctx); err != nil {
		r.healthySince = time.Time{}
		return false, err
	}
	now := r.now()
	if r.healthySince.IsZero() {
		r.healthySince = now
	}
	if now.Sub(r.healthySince) < r.cfg.WarmUp {
		return false, nil
	}
	expired, err := r.store.ExpireSessions(ctx, reapBatch)
	if err != nil {
		r.healthySince = time.Time{}
		return false, err
	}
	lost, err := r.store.LoseOrphanedAttempts(ctx, reapBatch)
	if err != nil {
		r.healthySince = time.Time{}
		return false, err
	}
	timedOut, err := r.store.TimeOutOverdueAttempts(ctx, r.cfg.Grace, reapBatch)
	if err != nil {
		r.healthySince = time.Time{}
		return false, err
	}
	if expired+lost+timedOut > 0 {
		r.log.Info("reaped", "sessions_expired", expired, "attempts_lost", lost, "attempts_timed_out", timedOut)
	}
	return expired == reapBatch || lost == reapBatch || timedOut == reapBatch, nil
}

// MaintenanceStore runs one maintenance pass; *postgres.Store implements it.
type MaintenanceStore interface {
	RunMaintenance(ctx context.Context, r postgres.Retention) (postgres.MaintenanceReport, error)
}

// OperationStore processes bulk operations; *postgres.Store implements it.
type OperationStore interface {
	ProcessOperation(ctx context.Context, batch int) (bool, error)
}

// Operations processes one batch of a bulk operation per second on each engine node, which
// is the operations' rate limit (LLD §13.3).
type Operations struct {
	store OperationStore
	log   *slog.Logger
}

func NewOperations(store OperationStore, log *slog.Logger) *Operations {
	return &Operations{store: store, log: log.With("component", "operations")}
}

func (o *Operations) Name() string { return "operations" }

func (o *Operations) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if _, err := o.store.ProcessOperation(ctx, 100); err != nil && ctx.Err() == nil {
			o.log.Warn("operation batch failed", "error", err)
		}
	}
}

// Maintenance returns the singleton duty that runs maintenance at once and then every
// interval until its lease is lost (LLD §13.2).
func Maintenance(store MaintenanceStore, ret postgres.Retention, interval time.Duration, log *slog.Logger) func(context.Context) {
	log = log.With("component", "maintenance")
	return func(ctx context.Context) {
		for {
			rep, err := store.RunMaintenance(ctx, ret)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				log.Warn("maintenance incomplete; the rest runs next time", "error", err)
			}
			log.Info("maintenance done", "partitions_created", rep.PartitionsCreated,
				"partitions_dropped", rep.PartitionsDropped, "rows_deleted", rep.RowsDeleted)
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}
}
