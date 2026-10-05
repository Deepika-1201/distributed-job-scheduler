package dispatch_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/recovery"
	"jobscheduler/pkg/workerpb"
	"jobscheduler/pkg/workersdk"
)

// A worker that takes a job and dies without deregistering: its session expires, the
// reaper records the attempt as lost, and the retry runs on a healthy worker (HLD S3).
func TestCrashedWorkersJobIsRetriedElsewhere(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	addr := c.engine("node-a", func(cfg *dispatch.Config) {
		cfg.SessionTTL, cfg.HeartbeatInterval = time.Second, 200*time.Millisecond
	})
	c.run(recovery.NewReaper(c.store, recovery.ReaperConfig{Interval: 50 * time.Millisecond, WarmUp: time.Millisecond,
		Grace: time.Minute}, slog.New(slog.DiscardHandler)).Run)
	id := c.submit()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	crashed := workerpb.NewWorkerServiceClient(conn)
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	reg, err := crashed.Register(callCtx, &workerpb.RegisterRequest{Pool: "default", Slots: 1, WorkerId: "crashed"})
	if err != nil {
		t.Fatal(err)
	}
	// Heartbeat until the job arrives: under load that can take longer than the 1 s session.
	alive := make(chan struct{})
	stopHeartbeats := sync.OnceFunc(func() { close(alive) })
	defer stopHeartbeats()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-alive:
				return
			case <-ticker.C:
				_, _ = crashed.Heartbeat(callCtx, &workerpb.HeartbeatRequest{SessionId: reg.SessionId})
			}
		}
	}()
	var got *workerpb.PollResponse
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if got, err = crashed.Poll(callCtx, &workerpb.PollRequest{SessionId: reg.SessionId, MaxJobs: 1,
			Wait: durationpb.New(time.Second)}); err != nil {
			t.Fatal(err)
		}
		if len(got.Assignments) > 0 {
			break
		}
	}
	if got == nil || len(got.Assignments) != 1 || got.Assignments[0].JobId != string(id) {
		t.Fatalf("the crashing worker never got the job: %v", got)
	}
	stopHeartbeats() // it now goes silent: no heartbeats, no completion, no deregistration

	c.worker(addr, 1, map[string]workersdk.Handler{
		"email.send": func(context.Context, workersdk.Job) ([]byte, error) { return []byte(`{"ok": true}`), nil },
	})
	c.await(id, domain.StateSucceeded, 20*time.Second)
	attempts := c.attempts(id)
	// An assignment released before delivery (T23) uses up its number without a row, so the
	// retry's number is only known to be higher.
	if len(attempts) != 2 || attempts[0].State != domain.AttemptLost || attempts[0].Actor != domain.ActorReaper ||
		attempts[1].State != domain.AttemptSucceeded || attempts[1].Number <= attempts[0].Number {
		t.Errorf("attempts = %+v, want LOST by the reaper, then a later attempt SUCCEEDED", attempts)
	}
}
