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
	expires int
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }
func (f *fakeStore) ExpireSessions(context.Context, int) (int, error) {
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
