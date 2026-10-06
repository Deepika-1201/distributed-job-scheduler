package recovery

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type fakeStore struct {
	pingErr error
	stall   bool // calls block until their context ends, like a blackholed database
	expires int
}

func (f *fakeStore) Ping(ctx context.Context) error {
	if f.stall {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.pingErr
}
func (f *fakeStore) ExpireSessions(context.Context, int, time.Duration) (int, error) {
	f.expires++
	return 0, nil
}
func (f *fakeStore) LoseOrphanedAttempts(context.Context, int) (int, error) { return 0, nil }
func (f *fakeStore) TimeOutOverdueAttempts(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

func TestReaperWarmsUpAfterConnectivityReturns(t *testing.T) {
	store := &fakeStore{}
	r := NewReaper(store, ReaperConfig{}, slog.New(slog.DiscardHandler))
	clock := time.Unix(0, 0)
	r.now = func() time.Time { return clock }
	step := func(at time.Duration) {
		clock = time.Unix(0, 0).Add(at)
		_, _ = r.step(context.Background())
	}

	step(0)
	step(29 * time.Second)
	if store.expires != 0 {
		t.Fatalf("expired sessions %d times during warm-up", store.expires)
	}
	step(30 * time.Second)
	if store.expires != 1 {
		t.Fatalf("expired sessions %d times after warm-up, want 1", store.expires)
	}

	store.pingErr = errors.New("connection refused")
	step(35 * time.Second)
	store.pingErr = nil
	step(40 * time.Second)
	step(69 * time.Second)
	if store.expires != 1 {
		t.Fatalf("expired sessions %d times while warming up again after an outage", store.expires)
	}
	step(70 * time.Second)
	if store.expires != 2 {
		t.Errorf("expired sessions %d times, want 2 once warm again", store.expires)
	}
}

// A stalled database must fail the step and restart the warm-up, not hang until it returns and
// then expire sessions that couldn't renew meanwhile (LLD §21.2).
func TestReaperStallRestartsWarmUp(t *testing.T) {
	store := &fakeStore{}
	r := NewReaper(store, ReaperConfig{CallTimeout: 10 * time.Millisecond}, slog.New(slog.DiscardHandler))
	clock := time.Unix(0, 0)
	r.now = func() time.Time { return clock }
	step := func(at time.Duration) error {
		clock = time.Unix(0, 0).Add(at)
		_, err := r.step(context.Background())
		return err
	}
	_ = step(0)
	_ = step(30 * time.Second)
	if store.expires != 1 {
		t.Fatalf("expired sessions %d times once warm, want 1", store.expires)
	}
	store.stall = true
	if err := step(35 * time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("step during a stall = %v, want its deadline to expire", err)
	}
	store.stall = false
	_ = step(40 * time.Second)
	if store.expires != 1 {
		t.Errorf("expired sessions right after a stall: %d calls, want the warm-up to restart", store.expires)
	}
	_ = step(70 * time.Second)
	if store.expires != 2 {
		t.Errorf("expired sessions %d times, want 2 once warm again", store.expires)
	}
}
