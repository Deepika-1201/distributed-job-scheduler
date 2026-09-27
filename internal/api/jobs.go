package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

type retryPolicyBody struct {
	Strategy         *string  `json:"strategy,omitempty"`
	InitialDelay     *string  `json:"initial_delay,omitempty"`
	Multiplier       *float64 `json:"multiplier,omitempty"`
	MaxDelay         *string  `json:"max_delay,omitempty"`
	Jitter           *string  `json:"jitter,omitempty"`
	MaxAttempts      *int     `json:"max_attempts,omitempty"`
	MaxRetryDuration *string  `json:"max_retry_duration,omitempty"`
	MaxLostAttempts  *int     `json:"max_lost_attempts,omitempty"`
}

// apply overlays the fields present in b onto base, recording unparsable durations in fe.
func (b *retryPolicyBody) apply(base domain.RetryPolicy, fe fieldErrors, field string) domain.RetryPolicy {
	if b == nil {
		return base
	}
	p := base
	dur := func(name string, v *string, dst *time.Duration) {
		if v == nil {
			return
		}
		d, err := time.ParseDuration(*v)
		if err != nil {
			fe.add(field+"."+name, "must be a duration such as 30s")
			return
		}
		*dst = d
	}
	if b.Strategy != nil {
		p.Strategy = domain.BackoffStrategy(*b.Strategy)
	}
	if b.Jitter != nil {
		p.Jitter = domain.Jitter(*b.Jitter)
	}
	if b.Multiplier != nil {
		p.Multiplier = *b.Multiplier
	}
	if b.MaxAttempts != nil {
		p.MaxAttempts = *b.MaxAttempts
	}
	if b.MaxLostAttempts != nil {
		p.MaxLostAttempts = *b.MaxLostAttempts
	}
	dur("initial_delay", b.InitialDelay, &p.InitialDelay)
	dur("max_delay", b.MaxDelay, &p.MaxDelay)
	dur("max_retry_duration", b.MaxRetryDuration, &p.MaxRetryDuration)
	if err := p.Validate(domain.DefaultRetryLimits()); err != nil {
		fe.add(field, "%s", strings.ReplaceAll(err.Error(), "\n", "; "))
	}
	return p
}

func policyBody(p domain.RetryPolicy) *retryPolicyBody {
	str := func(s string) *string { return &s }
	return &retryPolicyBody{
		Strategy: str(string(p.Strategy)), InitialDelay: str(p.InitialDelay.String()), Multiplier: &p.Multiplier,
		MaxDelay: str(p.MaxDelay.String()), Jitter: str(string(p.Jitter)), MaxAttempts: &p.MaxAttempts,
		MaxRetryDuration: str(p.MaxRetryDuration.String()), MaxLostAttempts: &p.MaxLostAttempts,
	}
}

type attemptResponse struct {
	ID         string              `json:"id"`
	Number     int                 `json:"number"`
	State      domain.AttemptState `json:"state"`
	StartedAt  time.Time           `json:"started_at"`
	Deadline   time.Time           `json:"deadline"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
	Error      string              `json:"error,omitempty"`
	Retryable  bool                `json:"retryable,omitempty"`
}

type jobResponse struct {
	ID                string            `json:"id"`
	Type              string            `json:"type"`
	TypeVersion       int               `json:"type_version"`
	Pool              string            `json:"pool"`
	ScheduleID        string            `json:"schedule_id,omitempty"`
	FireTime          *time.Time        `json:"fire_time,omitempty"`
	State             domain.JobState   `json:"state"`
	Priority          domain.Priority   `json:"priority"`
	Payload           json.RawMessage   `json:"payload"`
	Labels            map[string]string `json:"labels"`
	DedupeKey         string            `json:"dedupe_key,omitempty"`
	CorrelationID     string            `json:"correlation_id,omitempty"`
	CreatedBy         string            `json:"created_by"`
	RunAt             time.Time         `json:"run_at"`
	StartDeadline     *time.Time        `json:"start_deadline,omitempty"`
	Deadline          *time.Time        `json:"deadline,omitempty"`
	AttemptTimeout    string            `json:"attempt_timeout"`
	RetryPolicy       *retryPolicyBody  `json:"retry_policy"`
	AttemptCount      int               `json:"attempt_count"`
	CurrentAttempt    *attemptResponse  `json:"current_attempt,omitempty"`
	CancelRequestedAt *time.Time        `json:"cancel_requested_at,omitempty"`
	LastError         string            `json:"last_error,omitempty"`
	Result            json.RawMessage   `json:"result,omitempty"`
	Reason            domain.Reason     `json:"reason,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	ReadyAt           *time.Time        `json:"ready_at,omitempty"`
	UpdatedAt         time.Time         `json:"updated_at"`
	FinishedAt        *time.Time        `json:"finished_at,omitempty"`
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func toJobResponse(j domain.Job) jobResponse {
	resp := jobResponse{
		ID: string(j.ID), Type: j.Type, TypeVersion: j.TypeVersion, Pool: j.Pool, ScheduleID: string(j.ScheduleID),
		FireTime: optTime(j.FireTime), State: j.State, Priority: j.Priority, Payload: j.Payload, Labels: j.Labels,
		DedupeKey: j.DedupeKey, CorrelationID: j.CorrelationID, CreatedBy: j.CreatedBy, RunAt: j.RunAt,
		StartDeadline: optTime(j.StartDeadline), Deadline: optTime(j.Deadline), AttemptTimeout: j.AttemptTimeout.String(),
		RetryPolicy: policyBody(j.RetryPolicy), AttemptCount: j.AttemptCount, CancelRequestedAt: optTime(j.CancelRequestedAt),
		LastError: j.LastError, Result: j.Result, Reason: j.Reason, CreatedAt: j.CreatedAt, ReadyAt: optTime(j.ReadyAt),
		UpdatedAt: j.UpdatedAt, FinishedAt: optTime(j.FinishedAt),
	}
	if c := j.Current; c != nil {
		resp.CurrentAttempt = &attemptResponse{ID: string(c.ID), Number: c.Number, State: domain.AttemptRunning,
			StartedAt: c.StartedAt, Deadline: c.Deadline}
	}
	return resp
}

type submitJobRequest struct {
	Type           string            `json:"type"`
	Payload        json.RawMessage   `json:"payload"`
	RunAt          *time.Time        `json:"run_at"`
	Delay          string            `json:"delay"`
	Priority       string            `json:"priority"`
	Labels         map[string]string `json:"labels"`
	DedupeKey      string            `json:"dedupe_key"`
	CorrelationID  string            `json:"correlation_id"`
	StartDeadline  *time.Time        `json:"start_deadline"`
	Deadline       *time.Time        `json:"deadline"`
	AttemptTimeout string            `json:"attempt_timeout"`
	RetryPolicy    *retryPolicyBody  `json:"retry_policy"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request, p principal) error {
	var req submitJobRequest
	body, err := readJSON(r, &req)
	if err != nil {
		return err
	}
	nj, err := s.resolveSubmission(r, p, req)
	if err != nil {
		return err
	}
	var idem *postgres.Idempotency
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if len(key) > 255 {
			return fieldErrors{"Idempotency-Key": "must be at most 255 characters"}.err()
		}
		sum := sha256.Sum256(body)
		idem = &postgres.Idempotency{Key: key, RequestHash: sum[:]}
	}
	res, err := s.store.SubmitJob(r.Context(), nj, idem)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if res.Outcome == postgres.Created {
		status = http.StatusCreated
	}
	w.Header().Set("X-Submission-Outcome", string(res.Outcome))
	writeJSON(w, status, toJobResponse(res.Job))
	return nil
}

// resolveSubmission validates a request and fills in the job type's defaults (LLD §9.4).
func (s *Server) resolveSubmission(r *http.Request, p principal, req submitJobRequest) (postgres.NewJob, error) {
	fe := fieldErrors{}
	quotas, err := s.admission.tenantQuotas(r.Context(), p.Tenant)
	if err != nil {
		return postgres.NewJob{}, err
	}
	payload := []byte(req.Payload)
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if limit := s.admission.payloadLimit(quotas); len(payload) > limit {
		return postgres.NewJob{}, payloadTooLarge(limit)
	}
	validateLabels(req.Labels, fe)
	if len(req.DedupeKey) > 255 {
		fe.add("dedupe_key", "must be at most 255 characters")
	}
	if len(req.CorrelationID) > 128 {
		fe.add("correlation_id", "must be at most 128 characters")
	}

	var runAt time.Time
	switch {
	case req.RunAt != nil && req.Delay != "":
		fe.add("delay", "cannot be combined with run_at")
	case req.RunAt != nil:
		runAt = *req.RunAt
	case req.Delay != "":
		if d, err := time.ParseDuration(req.Delay); err != nil || d < 0 {
			fe.add("delay", "must be a non-negative duration such as 10m")
		} else {
			runAt = s.now().Add(d)
		}
	}

	if req.Type == "" {
		fe.add("type", "required")
		return postgres.NewJob{}, fe.err()
	}
	jt, err := s.store.GetJobType(r.Context(), p.Tenant, req.Type)
	switch {
	case err == nil && !jt.Enabled, errors.Is(err, domain.ErrNotFound):
		fe.add("type", "unknown or disabled job type")
		return postgres.NewJob{}, fe.err()
	case err != nil:
		return postgres.NewJob{}, err
	}

	priority := jt.DefaultPriority
	if req.Priority != "" {
		if priority, err = domain.ParsePriority(req.Priority); err != nil {
			fe.add("priority", "must be CRITICAL, HIGH, NORMAL or LOW")
		}
	}
	timeout := jt.AttemptTimeout
	if req.AttemptTimeout != "" {
		if timeout, err = time.ParseDuration(req.AttemptTimeout); err != nil || timeout < time.Second || timeout > 24*time.Hour {
			fe.add("attempt_timeout", "must be a duration between 1s and 24h")
		}
	}
	policy := req.RetryPolicy.apply(jt.RetryPolicy, fe, "retry_policy")
	if err := fe.err(); err != nil {
		return postgres.NewJob{}, err
	}
	if priority == domain.PriorityCritical && !p.Role.Includes(domain.RoleOperator) {
		return postgres.NewJob{}, errPermission("CRITICAL priority requires the operator role")
	}
	if err := s.admission.shed(r.Context(), jt.Pool, priority); err != nil {
		return postgres.NewJob{}, err
	}
	if err := s.admission.checkPending(r.Context(), p.Tenant, quotas); err != nil {
		return postgres.NewJob{}, err
	}

	nj := postgres.NewJob{
		TenantID: p.Tenant, Type: jt.Name, TypeVersion: jt.Version, Pool: jt.Pool, Priority: priority,
		Payload: payload, Labels: req.Labels, DedupeKey: req.DedupeKey, CorrelationID: req.CorrelationID,
		CreatedBy: "api_key:" + p.KeyID, RequestID: requestID(r), RunAt: runAt,
		AttemptTimeout: timeout, RetryPolicy: policy, AtMostOnce: jt.AtMostOnce,
	}
	if req.StartDeadline != nil {
		nj.StartDeadline = *req.StartDeadline
	}
	if req.Deadline != nil {
		nj.Deadline = *req.Deadline
	}
	return nj, nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, p principal) error {
	job, err := s.store.GetJob(r.Context(), p.Tenant, domain.JobID(r.PathValue("id")))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
	return nil
}

type listResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type cursorBody struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

func payloadTooLarge(limit int) error {
	return newError(http.StatusRequestEntityTooLarge, "payload_too_large", "payload exceeds the tenant's limit of %d bytes", limit)
}

func validateLabels(labels map[string]string, fe fieldErrors) {
	if len(labels) > 16 {
		fe.add("labels", "at most 16 labels")
	}
	for k, v := range labels {
		if k == "" || len(k) > 64 || len(v) > 256 {
			fe.add("labels", "keys must be 1-64 characters and values at most 256")
		}
	}
}

// parsePage reads the limit (default 50) and opaque cursor query parameters.
func parsePage(q url.Values, fe fieldErrors) (int, *cursorBody) {
	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err != nil || n < 1 || n > 200 {
			fe.add("limit", "must be between 1 and 200")
		} else {
			limit = n
		}
	}
	c := q.Get("cursor")
	if c == "" {
		return limit, nil
	}
	var cb cursorBody
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err == nil {
		err = json.Unmarshal(raw, &cb)
	}
	if err != nil || cb.ID == "" {
		fe.add("cursor", "invalid cursor")
	}
	return limit, &cb
}

func encodeCursor(createdAt time.Time, id string) string {
	raw, _ := json.Marshal(cursorBody{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, p principal) error {
	q := r.URL.Query()
	fe := fieldErrors{}
	f := postgres.JobFilter{State: domain.JobState(q.Get("state")), Type: q.Get("type"), ScheduleID: domain.ScheduleID(q.Get("schedule_id"))}
	if f.State != "" && !f.State.Valid() {
		fe.add("state", "unknown state")
	}
	if f.ScheduleID != "" {
		if _, err := uuid.Parse(string(f.ScheduleID)); err != nil {
			fe.add("schedule_id", "must be a UUID")
		}
	}
	if label := q.Get("label"); label != "" {
		k, v, ok := strings.Cut(label, ":")
		if !ok || k == "" {
			fe.add("label", "must be key:value")
		}
		f.LabelKey, f.LabelValue = k, v
	}
	var after *cursorBody
	if f.Limit, after = parsePage(q, fe); after != nil {
		f.After = &postgres.JobCursor{CreatedAt: after.CreatedAt, ID: domain.JobID(after.ID)}
	}
	if err := fe.err(); err != nil {
		return err
	}

	jobs, next, err := s.store.ListJobs(r.Context(), p.Tenant, f)
	if err != nil {
		return err
	}
	resp := listResponse[jobResponse]{Items: make([]jobResponse, 0, len(jobs))}
	for _, j := range jobs {
		resp.Items = append(resp.Items, toJobResponse(j))
	}
	if next != nil {
		resp.NextCursor = encodeCursor(next.CreatedAt, string(next.ID))
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) listAttempts(w http.ResponseWriter, r *http.Request, p principal) error {
	attempts, err := s.store.ListAttempts(r.Context(), p.Tenant, domain.JobID(r.PathValue("id")))
	if err != nil {
		return err
	}
	resp := listResponse[attemptResponse]{Items: make([]attemptResponse, 0, len(attempts))}
	for _, a := range attempts {
		resp.Items = append(resp.Items, attemptResponse{
			ID: string(a.ID), Number: a.Number, State: a.State, StartedAt: a.StartedAt, Deadline: a.Deadline,
			FinishedAt: optTime(a.FinishedAt), Error: a.Error, Retryable: a.Retryable,
		})
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

type jobActionFunc func(ctx context.Context, tenant domain.TenantID, id domain.JobID, audit postgres.Audit) (domain.Job, error)

// jobAction adapts a lifecycle operation (LLD §9.5) to an endpoint.
func (s *Server) jobAction(action jobActionFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request, p principal) error {
		job, err := action(r.Context(), p.Tenant, domain.JobID(r.PathValue("id")), s.audit(r, p))
		if err != nil {
			return conflictWithState(err, job.State)
		}
		writeJSON(w, http.StatusOK, toJobResponse(job))
		return nil
	}
}
