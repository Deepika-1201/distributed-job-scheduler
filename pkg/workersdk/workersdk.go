// Package workersdk runs job handlers as a worker of the job platform (LLD §12.4).
//
// A worker registers with one pool, long-polls for as many jobs as it has free slots, runs
// each job's handler and reports the outcome. Delivery is at least once: a handler may run
// again for the same job after a crash or timeout, so handlers should be idempotent, keyed
// on Job.ID. Job.Attempt is a fencing token that increases with every attempt.
package workersdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"jobscheduler/pkg/workerpb"
)

// Job is one attempt of a job, as seen by its handler.
type Job struct {
	ID          string // idempotency key: the same for every attempt of the job
	AttemptID   string
	Attempt     int64 // fencing token: increases with every attempt
	Type        string
	TypeVersion int
	TenantID    string
	Priority    string
	Payload     []byte // JSON
	Labels      map[string]string
	ScheduleID  string
	FireTime    time.Time
	Deadline    time.Time // local deadline, enforced by the worker
}

// Handler runs one attempt and returns an optional JSON result. It must return promptly once
// ctx is done: on timeout, cancellation or shutdown.
type Handler func(ctx context.Context, job Job) (result []byte, err error)

// Permanent marks an error as not worth retrying.
func Permanent(err error) error { return &outcomeError{err: err, permanent: true} }

// RetryAfter marks an error as retryable and suggests when to try again.
func RetryAfter(err error, d time.Duration) error { return &outcomeError{err: err, retryAfter: d} }

type outcomeError struct {
	err        error
	permanent  bool
	retryAfter time.Duration
}

func (e *outcomeError) Error() string { return e.err.Error() }
func (e *outcomeError) Unwrap() error { return e.err }

type Config struct {
	Address  string // any engine node, e.g. "engine:7070"
	Token    string // the worker token
	Pool     string
	Slots    int                // concurrent attempts, default 1
	Handlers map[string]Handler // by job type; these are the worker's capabilities
	Labels   map[string]string
	WorkerID string        // default hostname and PID
	PollWait time.Duration // default 20 s
	// DrainTimeout bounds how long shutdown waits for running handlers, default 30 s.
	DrainTimeout time.Duration
	Logger       *slog.Logger
	// DialOptions replace the default insecure transport credentials, e.g. to use TLS.
	DialOptions []grpc.DialOption
}

var (
	errCancelRequested = errors.New("job cancelled")
	errStale           = errors.New("attempt no longer current")
	errShutdown        = errors.New("worker shutting down")
)

// ErrDrained is returned by Run after an operator drained the worker (ADR-017).
var ErrDrained = errors.New("workersdk: worker drained by an operator")

const (
	completeTimeout = 10 * time.Minute
	reportGrace     = 10 * time.Second
	fenceMargin     = 5 * time.Second
	minBackoff      = 500 * time.Millisecond
	maxBackoff      = 10 * time.Second
)

// Run registers the worker and processes jobs until ctx is cancelled or an operator drains
// it. Then it stops polling, waits up to DrainTimeout for running handlers, reports the rest
// as retryable failures and deregisters. After a drain it returns ErrDrained.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Address == "" || cfg.Token == "" || cfg.Pool == "" || len(cfg.Handlers) == 0 {
		return errors.New("workersdk: address, token, pool and at least one handler are required")
	}
	if cfg.Slots <= 0 {
		cfg.Slots = 1
	}
	if cfg.PollWait <= 0 {
		cfg.PollWait = 20 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s/%d", host, os.Getpid())
	}
	w := &worker{cfg: cfg, log: cfg.Logger.With("pool", cfg.Pool), running: map[string]*attempt{},
		reporting: map[string]*attempt{}, slotFreed: make(chan struct{}, 1)}
	conn, err := w.dial(cfg.Address)
	if err != nil {
		return err
	}
	defer conn.Close()
	w.client = workerpb.NewWorkerServiceClient(conn)
	defer w.closePollConn()
	var stopReports context.CancelFunc
	w.reports, stopReports = context.WithCancel(context.Background())
	defer stopReports()

	// work outlives ctx: it ends only after draining.
	work, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	if err := w.register(ctx); err != nil {
		return err
	}
	var loops sync.WaitGroup
	loops.Go(func() { w.heartbeats(work) })
	pollCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()
	w.stopPolling = stopPolling
	w.poll(pollCtx)

	w.drain()
	stopWork()
	loops.Wait()
	w.deregister()
	if w.drained.Load() {
		return ErrDrained
	}
	return nil
}

type worker struct {
	cfg         Config
	log         *slog.Logger
	client      workerpb.WorkerServiceClient
	reports     context.Context // ends when Run returns; bounds outcome retries
	stopPolling context.CancelFunc
	drained     atomic.Bool

	pollMu     sync.Mutex
	pollConn   *grpc.ClientConn // set while polling a redirect target
	pollClient workerpb.WorkerServiceClient

	mu         sync.Mutex
	renewMu    sync.Mutex // serializes re-registration
	session    string
	generation int
	ttl        time.Duration
	interval   time.Duration
	lastBeat   time.Time
	running    map[string]*attempt
	reporting  map[string]*attempt // finished, outcome not yet acknowledged
	handlers   sync.WaitGroup
	slotFreed  chan struct{}
}

type attempt struct {
	job    Job
	cancel context.CancelCauseFunc
}

func (w *worker) dial(addr string) (*grpc.ClientConn, error) {
	opts := append([]grpc.DialOption{grpc.WithUnaryInterceptor(w.withToken)}, w.cfg.DialOptions...)
	if len(w.cfg.DialOptions) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.NewClient(addr, opts...)
}

func (w *worker) withToken(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+w.cfg.Token), method, req, reply, cc, opts...)
}

func (w *worker) register(ctx context.Context) error {
	types := make([]string, 0, len(w.cfg.Handlers))
	for t := range w.cfg.Handlers {
		types = append(types, t)
	}
	for backoff := minBackoff; ; backoff = min(2*backoff, maxBackoff) {
		resp, err := w.client.Register(ctx, &workerpb.RegisterRequest{Pool: w.cfg.Pool, JobTypes: types,
			Slots: int32(w.cfg.Slots), Labels: w.cfg.Labels, WorkerId: w.cfg.WorkerID, RuntimeVersion: "go-sdk/1"})
		if err == nil {
			w.mu.Lock()
			w.session, w.ttl, w.interval = resp.SessionId, resp.LeaseTtl.AsDuration(), resp.HeartbeatInterval.AsDuration()
			w.lastBeat = time.Now()
			w.generation++
			w.mu.Unlock()
			w.log.Info("registered", "session_id", resp.SessionId, "slots", w.cfg.Slots)
			return nil
		}
		if code := status.Code(err); code == codes.InvalidArgument || code == codes.Unauthenticated {
			return fmt.Errorf("workersdk: register: %w", err)
		}
		w.log.Warn("register failed; retrying", "error", err)
		if !sleep(ctx, jitter(backoff)) {
			return ctx.Err()
		}
	}
}

// renewSession replaces a session the engine no longer knows, unless another goroutine has
// already done so. Attempts of the old session are abandoned: the engine retries them.
func (w *worker) renewSession(ctx context.Context, generation int) {
	w.renewMu.Lock()
	defer w.renewMu.Unlock()
	w.mu.Lock()
	if w.generation != generation {
		w.mu.Unlock()
		return
	}
	for id, a := range w.running {
		a.cancel(errStale)
		delete(w.running, id)
	}
	clear(w.reporting)
	w.mu.Unlock()
	w.log.Warn("session lost; abandoning running attempts and registering again")
	_ = w.register(ctx)
}

func (w *worker) sessionInfo() (string, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.session, w.generation
}

// requestDrain stops polling; Run then drains running work and deregisters.
func (w *worker) requestDrain() {
	if w.drained.CompareAndSwap(false, true) {
		w.log.Info("drain requested by the engine")
		w.stopPolling()
	}
}

func (w *worker) free() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cfg.Slots - len(w.running)
}

func (w *worker) poll(ctx context.Context) {
	backoff := minBackoff
	for ctx.Err() == nil {
		free := w.free()
		if free <= 0 {
			select {
			case <-ctx.Done():
				return
			case <-w.slotFreed:
			}
			continue
		}
		session, generation := w.sessionInfo()
		client, redirected := w.pollTarget()
		callCtx, cancel := context.WithTimeout(ctx, w.cfg.PollWait+10*time.Second)
		resp, err := client.Poll(callCtx, &workerpb.PollRequest{SessionId: session, MaxJobs: int32(free), Wait: durationpb.New(w.cfg.PollWait)})
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case status.Code(err) == codes.NotFound:
			w.renewSession(ctx, generation)
		case err != nil:
			w.log.Warn("poll failed", "error", err, "redirected", redirected)
			if redirected {
				w.setPollTarget("") // the owner may have gone; ask the configured address again
			}
			if !sleep(ctx, jitter(backoff)) {
				return
			}
			backoff = min(2*backoff, maxBackoff)
			continue
		case resp.Drain:
			w.requestDrain()
			return
		case resp.RedirectAddress != "":
			if redirected && !sleep(ctx, jitter(backoff)) { // redirected again: views of the owner disagree
				return
			}
			w.setPollTarget(resp.RedirectAddress)
			backoff = min(2*backoff, maxBackoff)
			continue
		case resp.RetryAfter != nil:
			sleep(ctx, resp.RetryAfter.AsDuration())
		}
		backoff = minBackoff
		if resp != nil {
			for _, a := range resp.Assignments {
				w.start(a, generation)
			}
		}
	}
}

func (w *worker) pollTarget() (workerpb.WorkerServiceClient, bool) {
	w.pollMu.Lock()
	defer w.pollMu.Unlock()
	if w.pollClient != nil {
		return w.pollClient, true
	}
	return w.client, false
}

// setPollTarget polls addr from now on; "" returns to the configured address.
func (w *worker) setPollTarget(addr string) {
	w.closePollConn()
	if addr == "" || addr == w.cfg.Address {
		return
	}
	conn, err := w.dial(addr)
	if err != nil {
		w.log.Warn("cannot dial the pool owner", "address", addr, "error", err)
		return
	}
	w.pollMu.Lock()
	w.pollConn, w.pollClient = conn, workerpb.NewWorkerServiceClient(conn)
	w.pollMu.Unlock()
	w.log.Info("polling the pool owner", "address", addr)
}

func (w *worker) closePollConn() {
	w.pollMu.Lock()
	defer w.pollMu.Unlock()
	if w.pollConn != nil {
		_ = w.pollConn.Close()
	}
	w.pollConn, w.pollClient = nil, nil
}

func (w *worker) start(a *workerpb.Assignment, generation int) {
	job := Job{ID: a.JobId, AttemptID: a.AttemptId, Attempt: a.AttemptNumber, Type: a.JobType,
		TypeVersion: int(a.JobTypeVersion), TenantID: a.TenantId, Priority: a.Priority, Payload: a.Payload,
		Labels: a.Labels, ScheduleID: a.ScheduleId, Deadline: time.Now().Add(a.Timeout.AsDuration())}
	if a.FireTime != nil {
		job.FireTime = a.FireTime.AsTime()
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	ctx, cancelTimeout := context.WithDeadline(ctx, job.Deadline)
	att := &attempt{job: job, cancel: cancel}
	w.mu.Lock()
	if w.generation != generation {
		w.mu.Unlock()
		cancelTimeout()
		cancel(errStale)
		return // assigned to a session this worker has abandoned; the engine retries it
	}
	w.running[job.AttemptID] = att
	w.mu.Unlock()
	w.handlers.Go(func() {
		defer cancelTimeout()
		req := w.execute(ctx, job)
		w.finish(att, req != nil)
		if req != nil {
			w.complete(req, att)
			w.mu.Lock()
			delete(w.reporting, job.AttemptID)
			w.mu.Unlock()
		}
	})
}

// execute runs the handler and maps its result to a report; nil means report nothing.
func (w *worker) execute(ctx context.Context, job Job) *workerpb.CompleteRequest {
	req := &workerpb.CompleteRequest{JobId: job.ID, AttemptId: job.AttemptID, AttemptNumber: job.Attempt}
	handler, ok := w.cfg.Handlers[job.Type]
	if !ok {
		req.Outcome, req.Error = workerpb.Outcome_OUTCOME_FAILED, "no handler for job type "+job.Type
		return req
	}
	result, err := safeCall(ctx, handler, job)
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errStale):
		return nil
	case err == nil:
		req.Outcome, req.Result = workerpb.Outcome_OUTCOME_SUCCEEDED, result
	case errors.Is(cause, errCancelRequested):
		req.Outcome, req.Error = workerpb.Outcome_OUTCOME_CANCELLED, err.Error()
	case errors.Is(cause, context.DeadlineExceeded):
		req.Outcome, req.Error = workerpb.Outcome_OUTCOME_TIMED_OUT, "attempt timed out: "+err.Error()
	case errors.Is(cause, errShutdown):
		req.Outcome, req.Retryable, req.Error = workerpb.Outcome_OUTCOME_FAILED, true, errShutdown.Error()
	default:
		req.Outcome, req.Retryable, req.Error = workerpb.Outcome_OUTCOME_FAILED, true, err.Error()
		var oe *outcomeError
		if errors.As(err, &oe) {
			req.Retryable = !oe.permanent
			if oe.retryAfter > 0 {
				req.RetryAfter = durationpb.New(oe.retryAfter)
			}
		}
	}
	return req
}

func safeCall(ctx context.Context, h Handler, job Job) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}

// finish frees the attempt's slot. An attempt with an outcome to report keeps appearing in
// heartbeats until the report is acknowledged, so the engine doesn't release it meanwhile.
func (w *worker) finish(att *attempt, reporting bool) {
	w.mu.Lock()
	if _, ok := w.running[att.job.AttemptID]; ok && reporting {
		w.reporting[att.job.AttemptID] = att
	}
	delete(w.running, att.job.AttemptID)
	w.mu.Unlock()
	select {
	case w.slotFreed <- struct{}{}:
	default:
	}
}

// complete reports an outcome, retrying until the engine accepts or rejects it.
func (w *worker) complete(req *workerpb.CompleteRequest, att *attempt) {
	req.SessionId, _ = w.sessionInfo()
	deadline := time.Now().Add(completeTimeout)
	for backoff := 200 * time.Millisecond; time.Now().Before(deadline); backoff = min(2*backoff, 5*time.Second) {
		ctx, cancel := context.WithTimeout(w.reports, 10*time.Second)
		_, err := w.client.Complete(ctx, req)
		cancel()
		switch status.Code(err) {
		case codes.OK:
			return
		case codes.FailedPrecondition, codes.NotFound, codes.InvalidArgument:
			w.log.Warn("outcome rejected", "job_id", att.job.ID, "attempt", att.job.Attempt, "error", err)
			return
		}
		w.log.Warn("reporting outcome failed; retrying", "job_id", att.job.ID, "error", err)
		if !sleep(w.reports, jitter(backoff)) {
			return
		}
	}
	w.log.Error("gave up reporting an outcome; the engine will retry the job", "job_id", att.job.ID)
}

// heartbeats renews the session, delivers cancels and self-fences when the engine is
// unreachable for longer than the session lease allows.
func (w *worker) heartbeats(ctx context.Context) {
	for {
		w.mu.Lock()
		interval := w.interval
		w.mu.Unlock()
		if !sleep(ctx, interval) {
			return
		}
		session, generation := w.sessionInfo()
		w.mu.Lock()
		running := make([]*workerpb.RunningAttempt, 0, len(w.running)+len(w.reporting))
		for _, set := range []map[string]*attempt{w.running, w.reporting} {
			for _, a := range set {
				running = append(running, &workerpb.RunningAttempt{JobId: a.job.ID, AttemptId: a.job.AttemptID, AttemptNumber: a.job.Attempt})
			}
		}
		w.mu.Unlock()
		callCtx, cancel := context.WithTimeout(ctx, interval)
		resp, err := w.client.Heartbeat(callCtx, &workerpb.HeartbeatRequest{SessionId: session, Running: running})
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case status.Code(err) == codes.NotFound:
			w.renewSession(ctx, generation)
		case err != nil:
			w.mu.Lock()
			silent, ttl := time.Since(w.lastBeat), w.ttl
			w.mu.Unlock()
			w.log.Warn("heartbeat failed", "error", err, "since_last_success", silent.Round(time.Second))
			if silent > ttl-fenceMargin {
				w.renewSession(ctx, generation) // self-fence: the engine may already have retried our jobs
			}
		default:
			if resp.Drain {
				w.requestDrain()
			}
			w.mu.Lock()
			w.lastBeat = time.Now()
			for _, id := range resp.Cancel {
				if a, ok := w.running[id]; ok {
					a.cancel(errCancelRequested)
				}
			}
			for _, id := range resp.Stale {
				if a, ok := w.running[id]; ok {
					a.cancel(errStale)
					delete(w.running, id)
				}
				delete(w.reporting, id)
			}
			w.mu.Unlock()
		}
	}
}

// drain waits for running handlers, cancelling those still running after DrainTimeout. It
// then waits at most reportGrace for their outcomes to be reported.
func (w *worker) drain() {
	done := make(chan struct{})
	go func() { w.handlers.Wait(); close(done) }()
	select {
	case <-done:
		return
	case <-time.After(w.cfg.DrainTimeout):
	}
	w.mu.Lock()
	for _, a := range w.running {
		a.cancel(errShutdown)
	}
	w.mu.Unlock()
	select {
	case <-done:
	case <-time.After(reportGrace):
		w.log.Warn("shutting down with unreported outcomes; the engine will retry those jobs")
	}
}

func (w *worker) deregister() {
	session, _ := w.sessionInfo()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := w.client.Deregister(ctx, &workerpb.DeregisterRequest{SessionId: session}); err != nil {
		w.log.Warn("deregister failed; the session will expire", "error", err)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func jitter(d time.Duration) time.Duration { return time.Duration(rand.Int64N(int64(d)) + 1) }
