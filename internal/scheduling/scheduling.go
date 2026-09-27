// Package scheduling runs the engine's schedule materializer and due-job promoter (LLD §10).
package scheduling

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
)

// Store is the persistence the loops drive; *postgres.Store implements it.
type Store interface {
	MaterializeDue(ctx context.Context, lim domain.PlanLimits, limit int) (int, error)
	ExpireOverdue(ctx context.Context, limit int) (int, error)
	PromoteDue(ctx context.Context, limit int) (int, error)
	PromoteScheduled(ctx context.Context, limit int) (postgres.PromoteStats, error)
}

// Batch sizes and intervals from LLD §10.3 and §10.4.
const (
	MaterializeInterval = time.Second
	PromoteInterval     = 250 * time.Millisecond
	materializeBatch    = 100
	promoteBatch        = 1000
	scheduleJobBatch    = 500
	errorBackoff        = 2 * time.Second
)

// Loop runs step every interval, immediately again while step reports a full batch, and
// after errorBackoff when it fails. Failures are logged; they are expected to be transient.
type Loop struct {
	name     string
	interval time.Duration
	step     func(context.Context) (full bool, err error)
	log      *slog.Logger
}

func (l *Loop) Name() string { return l.name }

func (l *Loop) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		wait := l.interval
		full, err := l.step(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			l.log.Error("scheduling step failed", "loop", l.name, "error", err)
			wait = errorBackoff
		case full:
			wait = 0
		}
		timer.Reset(wait)
	}
}

// NewMaterializer creates schedule jobs ahead of their fire times.
func NewMaterializer(store Store, lim domain.PlanLimits, log *slog.Logger) *Loop {
	return &Loop{name: "materializer", interval: MaterializeInterval, log: log, step: func(ctx context.Context) (bool, error) {
		start := time.Now()
		n, err := store.MaterializeDue(ctx, lim, materializeBatch)
		if n > 0 {
			observability.RecordSpan(ctx, "materialize", start, err, attribute.Int("schedules", n))
			log.Debug("materialized schedules", "schedules", n)
		}
		return n == materializeBatch, err
	}}
}

// NewPromoter expires overdue jobs, then makes due jobs READY, applying overlap policies.
func NewPromoter(store Store, log *slog.Logger) *Loop {
	return &Loop{name: "promoter", interval: PromoteInterval, log: log, step: func(ctx context.Context) (bool, error) {
		start := time.Now()
		expired, err := store.ExpireOverdue(ctx, promoteBatch)
		if err != nil {
			return false, err
		}
		promoted, err := store.PromoteDue(ctx, promoteBatch)
		if err != nil {
			return false, err
		}
		stats, err := store.PromoteScheduled(ctx, scheduleJobBatch)
		if err != nil {
			return false, err
		}
		if expired+promoted+stats.Total() > 0 {
			observability.RecordSpan(ctx, "promote", start, nil, attribute.Int("expired", expired),
				attribute.Int("promoted", promoted), attribute.Int("schedule_jobs", stats.Total()))
			log.Debug("promoted jobs", "expired", expired, "promoted", promoted, "schedule_jobs", stats)
		}
		return expired == promoteBatch || promoted == promoteBatch || stats.Total() == scheduleJobBatch, nil
	}}
}
