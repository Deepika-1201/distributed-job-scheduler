// Package app runs a process's components and coordinates graceful shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Component is a long-running part of the process. Run blocks until ctx is cancelled,
// returning nil after a clean stop, or returns an error if the component fails.
type Component interface {
	Name() string
	Run(ctx context.Context) error
}

// ErrStoppedUnexpectedly reports a component that returned before shutdown began.
var ErrStoppedUnexpectedly = errors.New("component stopped unexpectedly")

type App struct {
	Log        *slog.Logger
	Components []Component
	// ShutdownDelay keeps components running after a signal so load balancers can
	// observe failing readiness before connections are closed.
	ShutdownDelay time.Duration
	// OnShutdown runs once when shutdown begins, before the delay.
	OnShutdown func()
}

// Run starts all components and blocks until every one has stopped. Shutdown begins when
// ctx is cancelled (after ShutdownDelay) or immediately when a component fails; the first
// failure is returned.
func (a *App) Run(ctx context.Context) error {
	if len(a.Components) == 0 {
		return errors.New("no components to run")
	}

	runCtx, stopComponents := context.WithCancel(context.WithoutCancel(ctx))
	defer stopComponents()

	var (
		wg       sync.WaitGroup
		failOnce sync.Once
		failed   = make(chan struct{})
		firstErr error
	)
	for _, c := range a.Components {
		wg.Go(func() {
			err := c.Run(runCtx)
			if err == nil && runCtx.Err() == nil {
				err = ErrStoppedUnexpectedly
			}
			if err != nil {
				failOnce.Do(func() {
					firstErr = fmt.Errorf("%s: %w", c.Name(), err)
					close(failed)
				})
			}
		})
	}

	select {
	case <-ctx.Done():
		a.beginShutdown("signal", a.ShutdownDelay)
		select {
		case <-time.After(a.ShutdownDelay):
		case <-failed:
		}
	case <-failed:
		a.beginShutdown("component failure", 0)
	}

	stopComponents()
	wg.Wait()
	if firstErr != nil {
		a.Log.Error("shutdown complete after failure", "error", firstErr)
	} else {
		a.Log.Info("shutdown complete")
	}
	return firstErr
}

func (a *App) beginShutdown(reason string, delay time.Duration) {
	a.Log.Info("shutdown started", "reason", reason, "delay", delay.String())
	if a.OnShutdown != nil {
		a.OnShutdown()
	}
}
