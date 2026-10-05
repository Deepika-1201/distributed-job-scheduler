// Package api serves the public REST API (LLD §9).
package api

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
)

const maxBodyBytes = 1 << 20

type Config struct {
	// TenantRateLimit is each tenant's default request rate on this node, per second.
	TenantRateLimit float64
	// Replicas is the number of api nodes; a tenant's own rate quota is split across them.
	Replicas int
	// MinScheduleInterval is the shortest schedule interval accepted; zero means 1 minute.
	MinScheduleInterval time.Duration
	// BacklogTarget is the backlog target of pools without their own: LOW submissions are shed
	// past it and NORMAL past three times it (ADR-021); zero means 5 minutes.
	BacklogTarget time.Duration
	// Addr is the listen address, whose port labels the HTTP metrics.
	Addr string
}

type Server struct {
	store     *postgres.Store
	log       *slog.Logger
	auth      *authenticator
	limiter   *rateLimiter
	admission *admission
	schemas   *schemaCache
	now       func() time.Time
	mux       *http.ServeMux
	routes    []string
	addr      string
}

func New(store *postgres.Store, log *slog.Logger, cfg Config) *Server {
	s := &Server{store: store, log: log, now: time.Now, mux: http.NewServeMux(), addr: cfg.Addr}
	s.auth = newAuthenticator(store, s.now)
	s.limiter = newRateLimiter(s.now)
	s.schemas = &schemaCache{}
	s.admission = &admission{store: store, now: s.now, defaultRate: cfg.TenantRateLimit, replicas: max(cfg.Replicas, 1),
		defaultMinInterval: cmp.Or(cfg.MinScheduleInterval, time.Minute), backlogTarget: cmp.Or(cfg.BacklogTarget, 5*time.Minute),
		quotaTTL: defaultQuotaTTL, pendingTTL: defaultPendingTTL, backlogTTL: defaultBacklogTTL,
		quotas: map[domain.TenantID]cached[domain.Quotas]{}, pending: map[domain.TenantID]cached[int]{}}

	s.handle("POST /v1/job-types", domain.RoleAdmin, s.createJobType)
	s.handle("GET /v1/job-types", domain.RoleViewer, s.listJobTypes)
	s.handle("GET /v1/job-types/{name}", domain.RoleViewer, s.getJobType)
	s.handle("PATCH /v1/job-types/{name}", domain.RoleAdmin, s.patchJobType)
	s.handle("POST /v1/job-types/{name}/pause", domain.RoleOperator, s.jobTypeAction(true))
	s.handle("POST /v1/job-types/{name}/resume", domain.RoleOperator, s.jobTypeAction(false))

	s.handle("POST /v1/jobs", domain.RoleSubmitter, s.submitJob)
	s.handle("GET /v1/jobs", domain.RoleViewer, s.listJobs)
	s.handle("GET /v1/jobs/{id}", domain.RoleViewer, s.getJob)
	s.handle("GET /v1/jobs/{id}/attempts", domain.RoleViewer, s.listAttempts)
	s.handle("POST /v1/jobs/{id}/cancel", domain.RoleSubmitter, s.jobAction(store.RequestCancel))
	s.handle("POST /v1/jobs/{id}/pause", domain.RoleOperator, s.jobAction(store.PauseJob))
	s.handle("POST /v1/jobs/{id}/resume", domain.RoleOperator, s.jobAction(store.ResumeJob))
	s.handle("POST /v1/jobs/{id}/run", domain.RoleOperator, s.jobAction(store.RunJobNow))
	s.handle("POST /v1/jobs/{id}/retry", domain.RoleOperator, s.jobAction(store.RetryJob))

	s.handle("POST /v1/schedules", domain.RoleOperator, s.createSchedule)
	s.handle("GET /v1/schedules", domain.RoleViewer, s.listSchedules)
	s.handle("GET /v1/schedules/{id}", domain.RoleViewer, s.getSchedule)
	s.handle("PATCH /v1/schedules/{id}", domain.RoleOperator, s.patchSchedule)
	s.handle("DELETE /v1/schedules/{id}", domain.RoleOperator, s.deleteSchedule)
	s.handle("POST /v1/schedules/{id}/pause", domain.RoleOperator, s.scheduleAction(store.PauseSchedule))
	s.handle("POST /v1/schedules/{id}/resume", domain.RoleOperator, s.scheduleAction(store.ResumeSchedule))
	s.handle("POST /v1/schedules/{id}/trigger", domain.RoleOperator, s.triggerSchedule)

	s.handle("GET /v1/workers", domain.RolePlatformAdmin, s.listWorkers)
	s.handle("POST /v1/workers/{id}/drain", domain.RolePlatformAdmin, s.drainWorker)
	s.handle("DELETE /v1/workers/{id}", domain.RolePlatformAdmin, s.deregisterWorker)
	s.handle("GET /v1/pools", domain.RolePlatformAdmin, s.listPools)
	s.handle("POST /v1/pools/{name}/pause", domain.RolePlatformAdmin, s.poolAction(true))
	s.handle("POST /v1/pools/{name}/resume", domain.RolePlatformAdmin, s.poolAction(false))
	s.handle("PUT /v1/pools/{name}/settings", domain.RolePlatformAdmin, s.putPoolSettings)

	s.handle("GET /v1/quotas", domain.RoleViewer, s.getOwnQuotas)
	s.handle("GET /v1/tenants", domain.RolePlatformAdmin, s.listTenants)
	s.handle("GET /v1/tenants/{id}/quotas", domain.RolePlatformAdmin, s.getTenantQuotas)
	s.handle("PUT /v1/tenants/{id}/quotas", domain.RolePlatformAdmin, s.putTenantQuotas)

	s.handle("POST /v1/operations", domain.RoleOperator, s.createOperation)
	s.handle("GET /v1/operations/{id}", domain.RoleViewer, s.getOperation)

	s.handle("POST /v1/api-keys", domain.RoleAdmin, s.createAPIKey)
	s.handle("GET /v1/api-keys", domain.RoleAdmin, s.listAPIKeys)
	s.handle("DELETE /v1/api-keys/{id}", domain.RoleAdmin, s.revokeAPIKey)
	s.handle("POST /v1/api-keys/{id}/rotate", domain.RoleAdmin, s.rotateAPIKey)

	s.handle("POST /v1/pools/{name}/worker-tokens", domain.RolePlatformAdmin, s.createWorkerToken)
	s.handle("GET /v1/pools/{name}/worker-tokens", domain.RolePlatformAdmin, s.listWorkerTokens)
	s.handle("DELETE /v1/pools/{name}/worker-tokens/{id}", domain.RolePlatformAdmin, s.revokeWorkerToken)
	return s
}

// Routes lists the registered "METHOD /path" patterns, for the OpenAPI contract test.
func (s *Server) Routes() []string { return slices.Clone(s.routes) }

type handlerFunc func(w http.ResponseWriter, r *http.Request, p principal) error

// handle registers an endpoint behind authentication, authorization and rate limiting.
func (s *Server) handle(pattern string, minRole domain.Role, h handlerFunc) {
	s.routes = append(s.routes, pattern)
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		p, err := s.auth.authenticate(r.Context(), r.Header.Get("Authorization"))
		if err == nil && !p.Role.Includes(minRole) {
			err = errPermission("this operation requires the %s role", minRole)
		}
		if err == nil {
			var q domain.Quotas
			if q, err = s.admission.tenantQuotas(r.Context(), p.Tenant); err == nil {
				if ok, wait := s.limiter.allow(p.Tenant, s.admission.rate(q)); !ok {
					err = &apiError{status: http.StatusTooManyRequests, code: "rate_limited",
						message: "tenant rate limit exceeded", retryAfter: wait}
				}
			}
		}
		if err == nil {
			*tenantFrom(r) = string(p.Tenant)
			err = h(w, r, p)
		}
		if err != nil {
			ae := toAPIError(err)
			if ae == errInternal {
				s.log.Error("request failed", "request_id", requestID(r), "error", err)
			}
			if pattern == submitRoute && p.Tenant != "" {
				countRejection(r, p.Tenant, ae)
			}
			writeError(w, r, ae)
		}
	})
}

const submitRoute = "POST /v1/jobs"

// countRejection counts a submission refused by admission control: rate, quota, size or load
// shedding (HLD §17.3).
func countRejection(r *http.Request, tenant domain.TenantID, ae *apiError) {
	switch ae.status {
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests, http.StatusServiceUnavailable:
		observability.JobsRejected.Add(r.Context(), 1, metric.WithAttributes(
			attribute.String("tenant", string(tenant)), attribute.String("reason", ae.code)))
	}
}

// Handler returns the API with its request-scoped middleware, measured and traced (ADR-020).
func (s *Server) Handler() http.Handler {
	notFound := func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, newError(http.StatusNotFound, "not_found", "no such endpoint"))
	}
	return observability.HTTPHandler(s.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := s.mux.Handler(r)
		if pattern == "" {
			notFound(w, r)
			return
		}
		observability.SetRoute(r, pattern)
		s.mux.ServeHTTP(w, r)
	})), "api", s.addr)
}

type ctxKey int

const (
	requestIDKey ctxKey = iota
	tenantKey
)

var safeRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func requestID(r *http.Request) string {
	id, _ := r.Context().Value(requestIDKey).(string)
	return id
}

// tenantFrom exposes a slot the handler fills after authentication, so the access log
// written by the outer middleware can include the tenant.
func tenantFrom(r *http.Request) *string {
	if t, ok := r.Context().Value(tenantKey).(*string); ok {
		return t
	}
	return new(string)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.now()
		id := r.Header.Get("X-Request-Id")
		if !safeRequestID.MatchString(id) {
			b := make([]byte, 12)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		tenant := new(string)
		ctx := context.WithValue(context.WithValue(r.Context(), requestIDKey, id), tenantKey, tenant)
		r = r.WithContext(ctx)
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		w.Header().Set("X-Request-Id", id)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic serving request", "request_id", id, "panic", v, "stack", string(debug.Stack()))
				writeError(rec, r, errInternal)
			}
			s.log.Info("request", append([]any{"method", r.Method, "route", r.Pattern, "path", r.URL.Path,
				"status", rec.status, "duration_ms", s.now().Sub(start).Milliseconds(), "request_id", id,
				"tenant_id", *tenant}, observability.TraceAttrs(ctx)...)...)
		}()
		next.ServeHTTP(rec, r)
	})
}

// readJSON reads the whole body (kept for idempotency hashing) and decodes it strictly.
func readJSON(r *http.Request, dst any) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return nil, newError(http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds %d bytes", maxBodyBytes)
	}
	if err != nil {
		return nil, newError(http.StatusBadRequest, "invalid_json", "could not read request body")
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return nil, newError(http.StatusBadRequest, "invalid_json", "%s", err.Error())
	}
	if dec.More() {
		return nil, newError(http.StatusBadRequest, "invalid_json", "unexpected data after the JSON body")
	}
	return body, nil
}

func (s *Server) audit(r *http.Request, p principal) postgres.Audit {
	return postgres.Audit{Actor: "api_key:" + p.KeyID, RequestID: requestID(r)}
}
