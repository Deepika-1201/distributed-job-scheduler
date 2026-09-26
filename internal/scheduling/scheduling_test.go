package scheduling

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoopRepeatsFullBatchesAndSurvivesErrors(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	l := &Loop{name: "test", interval: time.Hour, log: slog.New(slog.DiscardHandler), step: func(context.Context) (bool, error) {
		switch calls.Add(1) {
		case 1, 2:
			return true, nil // full batches run again immediately
		case 3:
			cancel()
			return false, errors.New("transient")
		}
		return false, nil
	}}
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not repeat full batches immediately or did not stop")
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("step ran %d times, want 3", n)
	}
}
