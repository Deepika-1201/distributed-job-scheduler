package workersdk_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"jobscheduler/pkg/workerpb"
	"jobscheduler/pkg/workersdk"
)

// explained is how an engine reports that it can't reach the database (ADR-029).
func explained(node string) error {
	st, err := status.New(codes.Unavailable, "the database is unavailable").WithDetails(&errdetails.ErrorInfo{
		Reason: workerpb.ReasonDatabaseUnavailable, Domain: workerpb.ErrorDomain,
		Metadata: map[string]string{workerpb.MetadataNodeID: node}})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

// outageEngine is node-a: it hands out one job, then fails heartbeats with whatever error the
// test sets, or serves them while none is set.
type outageEngine struct {
	workerpb.UnimplementedWorkerServiceServer
	ttl, tolerance time.Duration
	heartbeatErr   atomic.Pointer[error]
	registrations  atomic.Int32
	polls          atomic.Int32
	duplicates     int32 // how many polls repeat the assignment
	completed      chan string
}

func (e *outageEngine) Register(context.Context, *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	e.registrations.Add(1)
	return &workerpb.RegisterResponse{SessionId: "s1", LeaseTtl: durationpb.New(e.ttl),
		HeartbeatInterval: durationpb.New(50 * time.Millisecond), NodeId: "node-a",
		OutageTolerance: durationpb.New(e.tolerance)}, nil
}

func (e *outageEngine) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	if err := e.heartbeatErr.Load(); err != nil {
		return nil, *err
	}
	return &workerpb.HeartbeatResponse{NodeId: "node-a"}, nil
}

func (e *outageEngine) Poll(ctx context.Context, req *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	if e.polls.Add(1) <= 1+e.duplicates {
		return &workerpb.PollResponse{Assignments: []*workerpb.Assignment{assignment}}, nil
	}
	select {
	case <-ctx.Done():
	case <-time.After(req.Wait.AsDuration()):
	}
	return &workerpb.PollResponse{}, nil
}

func (e *outageEngine) Complete(_ context.Context, req *workerpb.CompleteRequest) (*workerpb.CompleteResponse, error) {
	if err := e.heartbeatErr.Load(); err != nil {
		return nil, *err
	}
	e.completed <- req.JobId
	return &workerpb.CompleteResponse{JobState: "SUCCEEDED"}, nil
}

func (e *outageEngine) Deregister(context.Context, *workerpb.DeregisterRequest) (*workerpb.DeregisterResponse, error) {
	return &workerpb.DeregisterResponse{}, nil
}

func (e *outageEngine) fail(err error) { e.heartbeatErr.Store(&err) }
func (e *outageEngine) heal()          { e.heartbeatErr.Store(nil) }

// blockingHandler runs until released, and reports when it starts and when its context ends.
type blockingHandler struct {
	started, release, cancelled chan struct{}
	runs                        atomic.Int32
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
}

func (h *blockingHandler) run(ctx context.Context, _ workersdk.Job) ([]byte, error) {
	if h.runs.Add(1) == 1 {
		close(h.started)
	}
	select {
	case <-h.release:
		return nil, nil
	case <-ctx.Done():
		close(h.cancelled)
		return nil, ctx.Err()
	}
}

func (h *blockingHandler) awaitStart(t *testing.T) {
	t.Helper()
	select {
	case <-h.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never started")
	}
}

// A worker whose engine reports the database unavailable keeps its handlers running past the
// session TTL, and reports once the database is back (HLD S6).
func TestWorkerRidesOutExplainedFailures(t *testing.T) {
	engine := &outageEngine{ttl: time.Second, tolerance: 10 * time.Second, completed: make(chan string, 1)}
	addr, _ := serve(t, engine)
	h := newBlockingHandler()
	defer run(t, addr, h.run)()
	h.awaitStart(t)

	engine.fail(explained("node-a"))
	select {
	case <-h.cancelled:
		t.Fatal("the handler was cancelled during an explained outage")
	case <-time.After(3 * engine.ttl):
	}
	engine.heal()
	close(h.release)
	select {
	case id := <-engine.completed:
		if id != assignment.JobId {
			t.Errorf("completed %q", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the outcome was never reported after the outage")
	}
	if n := engine.registrations.Load(); n != 1 {
		t.Errorf("registered %d times, want 1: the session should have been kept", n)
	}
}

// Riding out ends at the tolerance the engine announced.
func TestRidingOutStopsAtTheTolerance(t *testing.T) {
	engine := &outageEngine{ttl: 600 * time.Millisecond, tolerance: 1500 * time.Millisecond, completed: make(chan string, 1)}
	addr, _ := serve(t, engine)
	h := newBlockingHandler()
	defer run(t, addr, h.run)()
	h.awaitStart(t)

	start := time.Now()
	engine.fail(explained("node-a"))
	select {
	case <-h.cancelled:
		if took := time.Since(start); took < engine.ttl+200*time.Millisecond {
			t.Errorf("self-fenced after %s, before the tolerance could apply", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the worker kept running past the outage tolerance")
	}
}

// Failures that aren't explained by the node that renewed the session, including explanations
// from another node, still self-fence at TTL − margin (HLD S4).
func TestUnreachableEngineSelfFences(t *testing.T) {
	for name, err := range map[string]error{
		"unexplained":       status.Error(codes.Unavailable, "connection refused"),
		"another node":      explained("node-b"),
		"deadline exceeded": status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			engine := &outageEngine{ttl: time.Second, tolerance: 30 * time.Second, completed: make(chan string, 1)}
			addr, _ := serve(t, engine)
			h := newBlockingHandler()
			defer run(t, addr, h.run)()
			h.awaitStart(t)

			engine.fail(err)
			select {
			case <-h.cancelled:
			case <-time.After(3 * engine.ttl):
				t.Fatal("the worker kept running after its session could have expired")
			}
		})
	}
}

// An assignment for an attempt the worker is already running is ignored (HLD S9).
func TestDuplicateAssignmentRunsOnce(t *testing.T) {
	engine := &outageEngine{ttl: 30 * time.Second, tolerance: time.Minute, duplicates: 2, completed: make(chan string, 1)}
	addr, _ := serve(t, engine)
	h := newBlockingHandler()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workersdk.Run(ctx, workersdk.Config{Address: addr, Token: "token", Pool: "default", Slots: 4,
			Handlers: map[string]workersdk.Handler{"email.send": h.run}, PollWait: time.Second,
			DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
	}()
	defer func() {
		cancel()
		<-done
	}()
	h.awaitStart(t)
	for deadline := time.Now().Add(10 * time.Second); engine.polls.Load() < 4; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the worker stopped polling")
		}
	}
	close(h.release)
	select {
	case <-engine.completed:
	case <-time.After(10 * time.Second):
		t.Fatal("the outcome was never reported")
	}
	if n := h.runs.Load(); n != 1 {
		t.Errorf("the handler ran %d times for one attempt", n)
	}
}
