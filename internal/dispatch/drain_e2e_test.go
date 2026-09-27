package dispatch_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/pkg/workersdk"
)

// A drained worker finishes the job it is running, takes no new work, deregisters and
// returns ErrDrained (ADR-017).
func TestDrainedWorkerFinishesAndDeregisters(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	addr := c.engine("node-a")
	started, release := make(chan string, 2), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- workersdk.Run(context.Background(), workersdk.Config{Address: addr, Token: token, Pool: "default", Slots: 1,
			PollWait: time.Second, DrainTimeout: 30 * time.Second, Logger: slog.New(slog.DiscardHandler),
			Handlers: map[string]workersdk.Handler{"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
				started <- job.ID
				<-release
				return nil, nil
			}}})
	}()
	first := c.submit()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	sessions, err := c.store.ListSessions(ctx, "default")
	if err != nil || len(sessions) != 1 {
		t.Fatalf("sessions = %v, %v", sessions, err)
	}
	if err := c.store.DrainSession(ctx, sessions[0].ID, c.tenant, postgres.Audit{Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	second := c.submit()
	time.Sleep(6 * time.Second) // one heartbeat interval delivers the drain
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, workersdk.ErrDrained) {
			t.Fatalf("Run = %v, want ErrDrained", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a drained worker kept running")
	}
	c.await(first, domain.StateSucceeded, 5*time.Second)
	if job, _ := c.store.GetJob(ctx, c.tenant, second); job.State != domain.StateReady {
		t.Errorf("the job submitted after the drain is %s, want READY: a draining worker took new work", job.State)
	}
	if left, _ := c.store.ListSessions(ctx, "default"); len(left) != 0 {
		t.Errorf("%d sessions still active after the drain", len(left))
	}
}
