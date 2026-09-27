// Package dispatch serves the worker protocol and assigns ready jobs to polling workers
// (LLD §12). Each pool is dispatched by the engine node holding its lease.
package dispatch

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"jobscheduler/internal/coordination"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/pkg/workerpb"
)

type Config struct {
	NodeID            string // lease holder identity, unique per process start
	AdvertiseAddr     string // how workers reach this node, for redirects
	Token             string // bearer token workers present
	Listener          net.Listener
	SessionTTL        time.Duration // default 30 s
	HeartbeatInterval time.Duration // default 5 s
	RoundInterval     time.Duration // default 100 ms, while workers wait
	MaxPollWait       time.Duration // default 30 s
	MetricsInterval   time.Duration // how often gauges are sampled, default 10 s
	BacklogTarget     time.Duration // pools without their own target, default 5 min (ADR-021)
	Weights           domain.PriorityWeights
}

func (c *Config) setDefaults() {
	if c.SessionTTL == 0 {
		c.SessionTTL = 30 * time.Second
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.RoundInterval == 0 {
		c.RoundInterval = 100 * time.Millisecond
	}
	if c.MaxPollWait == 0 {
		c.MaxPollWait = 30 * time.Second
	}
	if c.MetricsInterval == 0 {
		c.MetricsInterval = 10 * time.Second
	}
	if c.BacklogTarget == 0 {
		c.BacklogTarget = 5 * time.Minute
	}
	if c.Weights == nil {
		c.Weights = domain.DefaultPriorityWeights()
	}
}

const (
	maxResultBytes  = 64 << 10
	maxErrorBytes   = 4 << 10
	noOwnerRetry    = 500 * time.Millisecond
	capsRefresh     = 10 * time.Second
	wantedRefresh   = 3 * time.Second
	shutdownTimeout = 5 * time.Second
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,99}$`)

// Dispatcher is the engine's worker-protocol server and the dispatcher of the pools whose
// leases this node holds.
type Dispatcher struct {
	workerpb.UnimplementedWorkerServiceServer
	cfg    Config
	store  *postgres.Store
	log    *slog.Logger
	leases *coordination.Manager
	server *grpc.Server

	mu     sync.Mutex
	pools  map[string]*pool     // pools this node dispatches
	wanted map[string]time.Time // pool → when a worker last needed it

	capsMu      sync.Mutex
	caps        map[domain.TenantID]int
	capsFetched time.Time

	node atomic.Pointer[nodeSample]
}

func New(store *postgres.Store, cfg Config, log *slog.Logger) (*Dispatcher, error) {
	cfg.setDefaults()
	if err := cfg.Weights.Validate(); err != nil {
		return nil, err
	}
	if cfg.Token == "" || cfg.NodeID == "" || cfg.Listener == nil {
		return nil, errors.New("dispatch: token, node ID and listener are required")
	}
	d := &Dispatcher{cfg: cfg, store: store, log: log.With("component", "dispatcher"),
		pools: map[string]*pool{}, wanted: map[string]time.Time{}}
	d.leases = coordination.NewManager(store, coordination.Config{
		Name: "pool-leases", Holder: cfg.NodeID, Address: cfg.AdvertiseAddr,
		TTL: coordination.PoolTTL, Margin: coordination.Margin,
		Wanted: d.wantedPools, OnAcquired: d.startPool, OnLost: d.stopPool,
	}, log)
	d.server = grpc.NewServer(grpc.StatsHandler(observability.GRPCServerHandler()), grpc.UnaryInterceptor(d.authenticate))
	workerpb.RegisterWorkerServiceServer(d.server, d)
	return d, nil
}

func (d *Dispatcher) Name() string { return "dispatcher" }

// Run serves workers until ctx ends. On shutdown it hands its pools to other nodes first, so
// waiting polls return at once, then finishes in-flight calls.
func (d *Dispatcher) Run(ctx context.Context) error {
	gauges, err := d.registerGauges()
	if err != nil {
		return err
	}
	defer func() { _ = gauges.Unregister() }()
	leaseCtx, stopLeases := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { _ = d.leases.Run(leaseCtx) })
	wg.Go(func() { d.refreshWanted(ctx) })
	wg.Go(func() { d.sampleNode(ctx) })
	served := make(chan error, 1)
	go func() { served <- d.server.Serve(d.cfg.Listener) }()

	select {
	case <-ctx.Done():
	case err = <-served:
	}
	stopLeases()
	wg.Wait()
	stopped := make(chan struct{})
	go func() { d.server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		d.server.Stop()
	}
	return err
}

func (d *Dispatcher) authenticate(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	got := md.Get("authorization")
	if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), []byte("Bearer "+d.cfg.Token)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "a valid worker token is required")
	}
	return handler(ctx, req)
}

func (d *Dispatcher) Register(ctx context.Context, req *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	switch {
	case !namePattern.MatchString(req.Pool):
		return nil, status.Errorf(codes.InvalidArgument, "pool must match %s", namePattern)
	case req.Slots < 1 || req.Slots > 10000:
		return nil, status.Error(codes.InvalidArgument, "slots must be between 1 and 10000")
	case len(req.JobTypes) > 1000 || len(req.Labels) > 16:
		return nil, status.Error(codes.InvalidArgument, "at most 1000 job types and 16 labels")
	}
	for _, jt := range req.JobTypes {
		if !namePattern.MatchString(jt) {
			return nil, status.Errorf(codes.InvalidArgument, "job type %q must match %s", jt, namePattern)
		}
	}
	ws, err := d.store.CreateSession(ctx, domain.WorkerSession{Pool: req.Pool, WorkerID: truncate(req.WorkerId, 200),
		JobTypes: req.JobTypes, Slots: int(req.Slots), Labels: req.Labels, RuntimeVersion: truncate(req.RuntimeVersion, 100)},
		d.cfg.SessionTTL)
	if err != nil {
		return nil, d.toStatus(err)
	}
	d.want(req.Pool)
	d.log.Info("worker registered", "session_id", ws.ID, "pool", ws.Pool, "worker_id", ws.WorkerID, "slots", ws.Slots)
	return &workerpb.RegisterResponse{SessionId: string(ws.ID), LeaseTtl: durationpb.New(d.cfg.SessionTTL),
		HeartbeatInterval: durationpb.New(d.cfg.HeartbeatInterval)}, nil
}

func (d *Dispatcher) Poll(ctx context.Context, req *workerpb.PollRequest) (*workerpb.PollResponse, error) {
	sess, err := d.store.GetActiveSession(ctx, domain.SessionID(req.SessionId))
	if err != nil {
		return nil, d.toStatus(err)
	}
	if sess.Draining {
		// Hold the call so a worker that hasn't seen the flag yet doesn't spin.
		timer := time.NewTimer(max(0, min(req.Wait.AsDuration(), d.cfg.MaxPollWait)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		return &workerpb.PollResponse{Drain: true}, nil
	}
	d.mu.Lock()
	p := d.pools[sess.Pool]
	d.mu.Unlock()
	if p == nil {
		return d.redirect(ctx, sess.Pool)
	}
	limit := max(1, min(int(req.MaxJobs), sess.Slots))
	wait := max(0, min(req.Wait.AsDuration(), d.cfg.MaxPollWait))
	resp := &workerpb.PollResponse{}
	for _, j := range p.wait(ctx, sess, limit, wait) {
		resp.Assignments = append(resp.Assignments, toAssignment(j))
	}
	return resp, nil
}

// redirect points a worker at the pool's owner, or asks it to retry while the pool has none.
func (d *Dispatcher) redirect(ctx context.Context, pool string) (*workerpb.PollResponse, error) {
	d.want(pool)
	l, err := d.store.GetLease(ctx, postgres.PoolLeaseName(pool))
	if err == nil && l.Holder != d.cfg.NodeID && l.Address != "" && l.ExpiresAt.After(time.Now()) {
		return &workerpb.PollResponse{RedirectAddress: l.Address}, nil
	}
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, d.toStatus(err)
	}
	return &workerpb.PollResponse{RetryAfter: durationpb.New(noOwnerRetry)}, nil
}

func (d *Dispatcher) Heartbeat(ctx context.Context, req *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	held := make([]postgres.AttemptRef, len(req.Running))
	for i, r := range req.Running {
		held[i] = postgres.AttemptRef{JobID: domain.JobID(r.JobId), AttemptID: domain.AttemptID(r.AttemptId)}
	}
	res, err := d.store.Heartbeat(ctx, domain.SessionID(req.SessionId), held, d.cfg.SessionTTL)
	if err != nil {
		return nil, d.toStatus(err)
	}
	resp := &workerpb.HeartbeatResponse{Drain: res.Drain}
	for _, id := range res.Cancel {
		resp.Cancel = append(resp.Cancel, string(id))
	}
	for _, id := range res.Stale {
		resp.Stale = append(resp.Stale, string(id))
	}
	return resp, nil
}

func (d *Dispatcher) Complete(ctx context.Context, req *workerpb.CompleteRequest) (*workerpb.CompleteResponse, error) {
	var end domain.AttemptEnd
	switch req.Outcome {
	case workerpb.Outcome_OUTCOME_SUCCEEDED:
		end.State = domain.AttemptSucceeded
	case workerpb.Outcome_OUTCOME_FAILED:
		end = domain.AttemptEnd{State: domain.AttemptFailed, Retryable: req.Retryable, RetryAfter: req.RetryAfter.AsDuration()}
	case workerpb.Outcome_OUTCOME_TIMED_OUT:
		end.State = domain.AttemptTimedOut
	case workerpb.Outcome_OUTCOME_CANCELLED:
		end.State = domain.AttemptCancelled
	default:
		return nil, status.Error(codes.InvalidArgument, "outcome is required")
	}
	var result []byte
	if len(req.Result) > 0 && end.State == domain.AttemptSucceeded {
		if len(req.Result) > maxResultBytes || !json.Valid(req.Result) {
			return nil, status.Errorf(codes.InvalidArgument, "result must be JSON of at most %d bytes", maxResultBytes)
		}
		result = req.Result
	}
	res, err := d.store.CompleteAttempt(ctx, postgres.Completion{
		JobID: domain.JobID(req.JobId), AttemptID: domain.AttemptID(req.AttemptId), Number: int(req.AttemptNumber),
		End: end, Error: truncate(req.Error, maxErrorBytes), Result: result, Actor: domain.ActorDispatcher,
	})
	if err != nil {
		if errors.Is(err, domain.ErrStaleAttempt) {
			observability.StaleCompletions.Add(ctx, 1, metric.WithAttributes(attribute.String("pool", res.Job.Pool)))
		}
		return nil, d.toStatus(err)
	}
	return &workerpb.CompleteResponse{JobState: string(res.Job.State), Replayed: res.Replayed}, nil
}

func (d *Dispatcher) Deregister(ctx context.Context, req *workerpb.DeregisterRequest) (*workerpb.DeregisterResponse, error) {
	if err := d.store.CloseSession(ctx, domain.SessionID(req.SessionId)); err != nil {
		return nil, d.toStatus(err)
	}
	d.log.Info("worker deregistered", "session_id", req.SessionId)
	return &workerpb.DeregisterResponse{}, nil
}

func (d *Dispatcher) toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, domain.ErrStaleAttempt), errors.Is(err, domain.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	d.log.Error("worker call failed", "error", err)
	return status.Error(codes.Internal, "internal error")
}

func toAssignment(j domain.Job) *workerpb.Assignment {
	a := &workerpb.Assignment{
		JobId: string(j.ID), AttemptId: string(j.Current.ID), AttemptNumber: int64(j.Current.Number),
		JobType: j.Type, JobTypeVersion: int32(j.TypeVersion), Payload: j.Payload, Labels: j.Labels,
		TenantId: string(j.TenantID), Priority: j.Priority.String(), Deadline: timestamppb.New(j.Current.Deadline),
		Timeout: durationpb.New(j.AttemptTimeout), ScheduleId: string(j.ScheduleID), TraceParent: j.TraceParent,
	}
	if !j.FireTime.IsZero() {
		a.FireTime = timestamppb.New(j.FireTime)
	}
	return a
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}

// want records that a worker needs the pool dispatched, and wakes the lease manager.
func (d *Dispatcher) want(pool string) {
	d.mu.Lock()
	_, known := d.wanted[pool]
	d.wanted[pool] = time.Now()
	d.mu.Unlock()
	if !known {
		d.leases.Poke()
	}
}

func (d *Dispatcher) wantedPools() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := make([]string, 0, len(d.wanted))
	for p := range d.wanted {
		names = append(names, postgres.PoolLeaseName(p))
	}
	return names
}

// refreshWanted tracks the pools with live sessions or READY jobs on any node, so every node
// competes for them and a surviving node takes over when an owner dies.
func (d *Dispatcher) refreshWanted(ctx context.Context) {
	ticker := time.NewTicker(wantedRefresh)
	defer ticker.Stop()
	for {
		if pools, err := d.store.WantedPools(ctx); err == nil {
			initPoolCounters(pools)
			now := time.Now()
			d.mu.Lock()
			for _, p := range pools {
				d.wanted[p] = now
			}
			for p, seen := range d.wanted {
				if now.Sub(seen) > 2*d.cfg.SessionTTL {
					delete(d.wanted, p)
				}
			}
			d.mu.Unlock()
		} else if ctx.Err() == nil {
			d.log.Warn("listing active pools failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) startPool(l postgres.Lease) {
	name := strings.TrimPrefix(l.Name, "pool:")
	selector, err := domain.NewPrioritySelector(d.cfg.Weights)
	if err != nil {
		d.log.Error("invalid priority weights", "error", err)
		return
	}
	p := newPool(d, name, l, selector)
	d.mu.Lock()
	old := d.pools[name]
	d.pools[name] = p
	d.mu.Unlock()
	if old != nil {
		old.stop()
	}
	go p.run()
	go p.sampleGauges()
	observability.PoolOwnerChanges.Add(context.Background(), 1, metric.WithAttributes(attribute.String("pool", name)))
	d.log.Info("dispatching pool", "pool", name, "epoch", l.Epoch)
}

func (d *Dispatcher) stopPool(leaseName string) {
	name := strings.TrimPrefix(leaseName, "pool:")
	d.mu.Lock()
	p := d.pools[name]
	delete(d.pools, name)
	d.mu.Unlock()
	if p != nil {
		p.stop()
		d.log.Info("stopped dispatching pool", "pool", name)
	}
}

// tenantCaps returns running-job caps, refreshed at most every capsRefresh.
func (d *Dispatcher) tenantCaps(ctx context.Context) (map[domain.TenantID]int, error) {
	d.capsMu.Lock()
	defer d.capsMu.Unlock()
	if d.caps != nil && time.Since(d.capsFetched) < capsRefresh {
		return d.caps, nil
	}
	caps, err := d.store.TenantCaps(ctx)
	if err != nil {
		return nil, err
	}
	d.caps, d.capsFetched = caps, time.Now()
	return caps, nil
}
