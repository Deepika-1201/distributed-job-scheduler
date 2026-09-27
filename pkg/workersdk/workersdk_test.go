package workersdk_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
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

func serve(t *testing.T, impl workerpb.WorkerServiceServer) (string, *grpc.Server) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), srv
}

func run(t *testing.T, addr string, handler workersdk.Handler) (cancel func()) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workersdk.Run(ctx, workersdk.Config{Address: addr, Token: "token", Pool: "default",
			Handlers: map[string]workersdk.Handler{"email.send": handler}, PollWait: time.Second,
			DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
	}()
	return func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	}
}

var assignment = &workerpb.Assignment{JobId: "j1", AttemptId: "a1", AttemptNumber: 1, JobType: "email.send",
	Timeout: durationpb.New(time.Minute)}

// entryEngine is the worker's configured node: it registers the worker, serves its
// heartbeats and redirects its polls to the pool owner.
type entryEngine struct {
	workerpb.UnimplementedWorkerServiceServer
	owner string
}

func (e *entryEngine) Register(context.Context, *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	return &workerpb.RegisterResponse{SessionId: "s1", LeaseTtl: durationpb.New(30 * time.Second),
		HeartbeatInterval: durationpb.New(50 * time.Millisecond)}, nil
}

func (e *entryEngine) Poll(context.Context, *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	return &workerpb.PollResponse{RedirectAddress: e.owner}, nil
}

func (e *entryEngine) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	return &workerpb.HeartbeatResponse{}, nil
}

// ownerEngine hands out one job, then long-polls; it also serves heartbeats and reports.
type ownerEngine struct {
	workerpb.UnimplementedWorkerServiceServer
	assigned   atomic.Bool
	heartbeats atomic.Int32
	completed  chan string
}

func (e *ownerEngine) Poll(ctx context.Context, req *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	if e.assigned.CompareAndSwap(false, true) {
		return &workerpb.PollResponse{Assignments: []*workerpb.Assignment{assignment}}, nil
	}
	select {
	case <-ctx.Done():
	case <-time.After(req.Wait.AsDuration()):
	}
	return &workerpb.PollResponse{}, nil
}

func (e *ownerEngine) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	e.heartbeats.Add(1)
	return &workerpb.HeartbeatResponse{}, nil
}

func (e *ownerEngine) Complete(_ context.Context, req *workerpb.CompleteRequest) (*workerpb.CompleteResponse, error) {
	e.completed <- req.JobId
	return &workerpb.CompleteResponse{JobState: "SUCCEEDED"}, nil
}

// The worker's configured node goes down while the pool owner it polls keeps serving. Every
// node serves heartbeats and reports, so the owner receives them: the session stays alive and
// the outcome lands, instead of the worker taking work it can't keep or report.
func TestSessionOutlivesItsConfiguredNode(t *testing.T) {
	owner := &ownerEngine{completed: make(chan string, 1)}
	ownerAddr, _ := serve(t, owner)
	entryAddr, entry := serve(t, &entryEngine{owner: ownerAddr})
	started, release := make(chan struct{}), make(chan struct{})
	cancel := run(t, entryAddr, func(context.Context, workersdk.Job) ([]byte, error) {
		close(started)
		<-release
		return nil, nil
	})
	defer cancel()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never ran the owner's job")
	}

	entry.Stop()
	close(release)
	select {
	case id := <-owner.completed:
		if id != assignment.JobId {
			t.Errorf("owner received the outcome of %q, want %q", id, assignment.JobId)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the outcome never reached the owner once the configured node stopped")
	}
	for deadline := time.Now().Add(10 * time.Second); owner.heartbeats.Load() < 3; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("owner received %d heartbeats, want the session kept alive through it", owner.heartbeats.Load())
		}
	}
}

// fencedEngine never renews the worker's session. Its first poll answers with a job only once
// the worker has self-fenced and is registering again.
type fencedEngine struct {
	workerpb.UnimplementedWorkerServiceServer
	registrations atomic.Int32
	renewing      chan struct{}
	renewOnce     sync.Once
	answered      atomic.Bool
}

func (e *fencedEngine) Register(context.Context, *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	if e.registrations.Add(1) == 1 {
		return &workerpb.RegisterResponse{SessionId: "s1", LeaseTtl: durationpb.New(2 * time.Second),
			HeartbeatInterval: durationpb.New(50 * time.Millisecond)}, nil
	}
	e.renewOnce.Do(func() { close(e.renewing) })
	return nil, status.Error(codes.Unavailable, "engine unreachable")
}

func (e *fencedEngine) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	return nil, status.Error(codes.Unavailable, "database unavailable")
}

func (e *fencedEngine) Poll(ctx context.Context, _ *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	select {
	case <-e.renewing:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	e.answered.Store(true)
	return &workerpb.PollResponse{Assignments: []*workerpb.Assignment{assignment}}, nil
}

// A worker that has self-fenced has abandoned its session, so it must not start work that
// arrives for it: its session can't be renewed, and the job would be lost.
func TestAbandonedSessionTakesNoWork(t *testing.T) {
	engine := &fencedEngine{renewing: make(chan struct{})}
	addr, _ := serve(t, engine)
	var ran atomic.Bool
	cancel := run(t, addr, func(context.Context, workersdk.Job) ([]byte, error) {
		ran.Store(true)
		return nil, nil
	})
	defer cancel()
	for deadline := time.Now().Add(10 * time.Second); !engine.answered.Load(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the worker never polled while re-registering")
		}
	}
	time.Sleep(500 * time.Millisecond)
	if ran.Load() {
		t.Error("the worker ran a job assigned to the session it had abandoned")
	}
}
