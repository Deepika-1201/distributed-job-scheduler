package workersdk_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"jobscheduler/pkg/workerpb"
	"jobscheduler/pkg/workersdk"
)

// lostEngine accepts one registration, then acts like an engine that has become unreachable.
// Its only poll answers once the heartbeat loop is re-registering, and reports the session gone.
type lostEngine struct {
	workerpb.UnimplementedWorkerServiceServer
	mu            sync.Mutex
	registrations int
	renewing      chan struct{}
	polled        chan struct{}
	renewOnce     sync.Once
	pollOnce      sync.Once
}

func (e *lostEngine) Register(context.Context, *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.registrations++; e.registrations == 1 {
		return &workerpb.RegisterResponse{SessionId: "s1", LeaseTtl: durationpb.New(2 * time.Second),
			HeartbeatInterval: durationpb.New(50 * time.Millisecond)}, nil
	}
	e.renewOnce.Do(func() { close(e.renewing) })
	return nil, status.Error(codes.Unavailable, "engine unreachable")
}

func (e *lostEngine) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	return nil, status.Error(codes.Unavailable, "engine unreachable")
}

func (e *lostEngine) Poll(ctx context.Context, _ *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	select {
	case <-e.renewing:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	e.pollOnce.Do(func() { close(e.polled) })
	return nil, status.Error(codes.NotFound, "session not found")
}

func (e *lostEngine) Deregister(context.Context, *workerpb.DeregisterRequest) (*workerpb.DeregisterResponse, error) {
	return nil, status.Error(codes.Unavailable, "engine unreachable")
}

// A worker whose engine is unreachable and whose session is gone must still shut down: the
// poll loop can't wait on a re-registration that only ends once shutdown completes.
func TestRunReturnsWhileReregistrationRetries(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	engine := &lostEngine{renewing: make(chan struct{}), polled: make(chan struct{})}
	srv := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(srv, engine)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- workersdk.Run(ctx, workersdk.Config{Address: lis.Addr().String(), Token: "token", Pool: "default",
			Handlers: map[string]workersdk.Handler{"email.send": func(context.Context, workersdk.Job) ([]byte, error) {
				return nil, nil
			}}, PollWait: time.Second, DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
	}()
	select {
	case <-engine.polled:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never polled once its heartbeats failed")
	}
	time.Sleep(100 * time.Millisecond) // the poll loop now wants to re-register too
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation: polling is stuck behind the heartbeat loop's re-registration")
	}
}
