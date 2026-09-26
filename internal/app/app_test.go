package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type fakeComponent struct {
	name    string
	stopped atomic.Bool
	run     func(ctx context.Context) error
}

func (f *fakeComponent) Name() string { return f.name }

func (f *fakeComponent) Run(ctx context.Context) error {
	defer f.stopped.Store(true)
	if f.run != nil {
		return f.run(ctx)
	}
	<-ctx.Done()
	return nil
}

func newApp(delay time.Duration, cs ...Component) (*App, *atomic.Int32) {
	var shutdowns atomic.Int32
	return &App{
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Components:    cs,
		ShutdownDelay: delay,
		OnShutdown:    func() { shutdowns.Add(1) },
	}, &shutdowns
}

func TestRunStopsAllComponentsOnSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := &fakeComponent{name: "a"}, &fakeComponent{name: "b"}
		app, shutdowns := newApp(0, a, b)
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() { done <- app.Run(ctx) }()
		synctest.Wait()
		if a.stopped.Load() || b.stopped.Load() {
			t.Fatal("components stopped before shutdown")
		}

		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
		if !a.stopped.Load() || !b.stopped.Load() {
			t.Error("not all components stopped")
		}
		if n := shutdowns.Load(); n != 1 {
			t.Errorf("OnShutdown called %d times, want 1", n)
		}
	})
}

func TestRunWaitsShutdownDelayBeforeStopping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &fakeComponent{name: "server"}
		app, shutdowns := newApp(5*time.Second, c)
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() { done <- app.Run(ctx) }()
		cancel()

		time.Sleep(4 * time.Second)
		synctest.Wait()
		if c.stopped.Load() {
			t.Fatal("component stopped before the shutdown delay elapsed")
		}
		if shutdowns.Load() != 1 {
			t.Error("OnShutdown must run at the start of the delay")
		}

		time.Sleep(2 * time.Second)
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
		if !c.stopped.Load() {
			t.Error("component still running after the delay")
		}
	})
}

func TestComponentFailureStopsOthersImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		healthy := &fakeComponent{name: "healthy"}
		broken := &fakeComponent{name: "broken", run: func(context.Context) error {
			time.Sleep(time.Second)
			return boom
		}}
		app, shutdowns := newApp(time.Hour, healthy, broken)

		err := app.Run(context.Background())
		if !errors.Is(err, boom) || err.Error() != "broken: boom" {
			t.Errorf("Run = %v, want wrapped boom", err)
		}
		if !healthy.stopped.Load() {
			t.Error("healthy component not stopped")
		}
		if shutdowns.Load() != 1 {
			t.Error("OnShutdown not called on failure")
		}
	})
}

func TestComponentReturningEarlyIsAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		quitter := &fakeComponent{name: "quitter", run: func(context.Context) error { return nil }}
		app, _ := newApp(0, quitter, &fakeComponent{name: "other"})
		if err := app.Run(context.Background()); !errors.Is(err, ErrStoppedUnexpectedly) {
			t.Errorf("Run = %v, want ErrStoppedUnexpectedly", err)
		}
	})
}

func TestFailureDuringShutdownDelayEndsDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		crashesLater := &fakeComponent{name: "c", run: func(ctx context.Context) error {
			time.Sleep(10 * time.Second)
			return boom
		}}
		app, _ := newApp(time.Hour, crashesLater)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		if err := app.Run(ctx); !errors.Is(err, boom) {
			t.Errorf("Run = %v, want boom", err)
		}
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Errorf("shutdown took %v, want 10s (failure should cut the delay short)", elapsed)
		}
	})
}

func TestRunWithoutComponents(t *testing.T) {
	app, _ := newApp(0)
	if err := app.Run(context.Background()); err == nil {
		t.Error("Run with no components succeeded, want error")
	}
}
