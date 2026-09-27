package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

// optional tells an absent field from an explicit null, so PATCH can clear a value.
type optional[T any] struct {
	Set   bool
	Value *T // nil for an explicit null
}

func (o *optional[T]) UnmarshalJSON(b []byte) error {
	o.Set = true
	if string(b) == "null" {
		o.Value = nil
		return nil
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	o.Value = &v
	return nil
}

type triggerBody struct {
	Kind     domain.TriggerKind `json:"kind"`
	Cron     string             `json:"cron,omitempty"`
	TimeZone string             `json:"time_zone,omitempty"`
	Interval string             `json:"interval,omitempty"`
}

func (b triggerBody) parse(fe fieldErrors) domain.Trigger {
	t := domain.Trigger{Kind: b.Kind}
	switch b.Kind {
	case domain.TriggerCron:
		if b.Cron == "" {
			fe.add("trigger.cron", "required for cron triggers")
		}
		if b.Interval != "" {
			fe.add("trigger.interval", "not allowed for cron triggers")
		}
		t.Cron, t.TimeZone = b.Cron, b.TimeZone
		if t.TimeZone == "" {
			t.TimeZone = "UTC"
		}
	case domain.TriggerFixedRate, domain.TriggerFixedDelay:
		if b.Cron != "" || b.TimeZone != "" {
			fe.add("trigger.cron", "only allowed for cron triggers")
		}
		d, err := time.ParseDuration(b.Interval)
		if err != nil || d < time.Millisecond {
			fe.add("trigger.interval", "must be a positive duration such as 10m")
		}
		t.Interval = d.Truncate(time.Millisecond)
	default:
		fe.add("trigger.kind", "must be cron, fixed_rate or fixed_delay")
	}
	return t
}

type scheduleRequest struct {
	Name          *string               `json:"name"`
	JobType       *string               `json:"job_type"`
	Payload       json.RawMessage       `json:"payload"`
	Labels        map[string]string     `json:"labels"`
	Priority      optional[string]      `json:"priority"`
	Trigger       *triggerBody          `json:"trigger"`
	StartAt       optional[time.Time]   `json:"start_at"`
	EndAt         optional[time.Time]   `json:"end_at"`
	MaxRuns       optional[int]         `json:"max_runs"`
	Jitter        *string               `json:"jitter"`
	MisfirePolicy *domain.MisfirePolicy `json:"misfire_policy"`
	OverlapPolicy *domain.OverlapPolicy `json:"overlap_policy"`
}

// apply copies the fields present in the request onto sc and validates them (LLD §10.2).
func (req scheduleRequest) apply(sc *domain.Schedule, fe fieldErrors, now time.Time, minInterval time.Duration) {
	if req.Name != nil {
		if !namePattern.MatchString(*req.Name) {
			fe.add("name", "must match %s", namePattern)
		}
		sc.Name = *req.Name
	}
	if req.Payload != nil {
		sc.Payload = req.Payload
	}
	if req.Labels != nil {
		validateLabels(req.Labels, fe)
		sc.Labels = req.Labels
	}
	if req.Priority.Set {
		sc.Priority = 0
		if req.Priority.Value != nil {
			p, err := domain.ParsePriority(*req.Priority.Value)
			if err != nil {
				fe.add("priority", "must be CRITICAL, HIGH, NORMAL or LOW")
			}
			sc.Priority = p
		}
	}
	if req.StartAt.Set {
		sc.StartAt = deref(req.StartAt.Value)
	}
	if req.EndAt.Set {
		sc.EndAt = deref(req.EndAt.Value)
		if !sc.EndAt.IsZero() && !sc.EndAt.After(now) {
			fe.add("end_at", "must be in the future")
		}
	}
	if req.MaxRuns.Set {
		sc.MaxRuns = deref(req.MaxRuns.Value)
		if req.MaxRuns.Value != nil && sc.MaxRuns < 1 {
			fe.add("max_runs", "must be at least 1")
		}
	}
	if req.Jitter != nil {
		d, err := time.ParseDuration(*req.Jitter)
		if err != nil || d < 0 || d > time.Hour {
			fe.add("jitter", "must be a duration between 0s and 1h")
		}
		sc.Jitter = d.Truncate(time.Millisecond)
	}
	if req.MisfirePolicy != nil {
		if !req.MisfirePolicy.Valid() {
			fe.add("misfire_policy", "must be fire_once, skip or fire_all")
		}
		sc.Misfire = *req.MisfirePolicy
	}
	if req.OverlapPolicy != nil {
		if !req.OverlapPolicy.Valid() {
			fe.add("overlap_policy", "must be skip, buffer_one, allow or cancel_previous")
		}
		sc.Overlap = *req.OverlapPolicy
	}
	if req.Trigger != nil {
		before := len(fe)
		if sc.Trigger = req.Trigger.parse(fe); len(fe) == before {
			validateTrigger(sc.Trigger, fe, now, minInterval)
		}
	}
	if sc.Trigger.Kind == domain.TriggerFixedRate && sc.Jitter > sc.Trigger.Interval {
		fe.add("jitter", "must not exceed the interval")
	}
	if !sc.StartAt.IsZero() && !sc.EndAt.IsZero() && !sc.EndAt.After(sc.StartAt) {
		fe.add("end_at", "must be after start_at")
	}
}

// validateTrigger rejects triggers that cannot be evaluated or fire more often than allowed.
func validateTrigger(t domain.Trigger, fe fieldErrors, now time.Time, minInterval time.Duration) {
	e, err := t.Compile(now)
	switch {
	case err != nil:
		fe.add("trigger", "%s", err.Error())
	case t.Kind == domain.TriggerCron:
		if domain.MinGap(e, now, 100) < minInterval {
			fe.add("trigger.cron", "fires more often than the minimum interval of %s", minInterval)
		}
	case t.Interval < minInterval:
		fe.add("trigger.interval", "must be at least %s", minInterval)
	}
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

type scheduleResponse struct {
	ID            string               `json:"id"`
	Name          string               `json:"name"`
	JobType       string               `json:"job_type"`
	Payload       json.RawMessage      `json:"payload"`
	Labels        map[string]string    `json:"labels"`
	Priority      *domain.Priority     `json:"priority"`
	Trigger       triggerBody          `json:"trigger"`
	StartAt       *time.Time           `json:"start_at"`
	EndAt         *time.Time           `json:"end_at"`
	MaxRuns       *int                 `json:"max_runs"`
	Jitter        string               `json:"jitter"`
	MisfirePolicy domain.MisfirePolicy `json:"misfire_policy"`
	OverlapPolicy domain.OverlapPolicy `json:"overlap_policy"`
	State         domain.ScheduleState `json:"state"`
	NextFireAt    *time.Time           `json:"next_fire_at"`
	LastFireAt    *time.Time           `json:"last_fire_at"`
	FireCount     int                  `json:"fire_count"`
	Upcoming      []time.Time          `json:"upcoming,omitempty"`
	CreatedBy     string               `json:"created_by"`
	CreatedAt     time.Time            `json:"created_at"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

func toScheduleResponse(sc domain.Schedule, withUpcoming bool) scheduleResponse {
	resp := scheduleResponse{
		ID: string(sc.ID), Name: sc.Name, JobType: sc.JobType, Payload: sc.Payload, Labels: sc.Labels,
		Trigger: triggerBody{Kind: sc.Trigger.Kind}, StartAt: optTime(sc.StartAt), EndAt: optTime(sc.EndAt),
		Jitter: sc.Jitter.String(), MisfirePolicy: sc.Misfire, OverlapPolicy: sc.Overlap, State: sc.State,
		NextFireAt: optTime(sc.NextFireAt), LastFireAt: optTime(sc.LastFireAt), FireCount: sc.FireCount,
		CreatedBy: sc.CreatedBy, CreatedAt: sc.CreatedAt, UpdatedAt: sc.UpdatedAt,
	}
	if sc.Trigger.Kind == domain.TriggerCron {
		resp.Trigger.Cron, resp.Trigger.TimeZone = sc.Trigger.Cron, sc.Trigger.TimeZone
	} else {
		resp.Trigger.Interval = sc.Trigger.Interval.String()
	}
	if sc.Priority != 0 {
		resp.Priority = &sc.Priority
	}
	if sc.MaxRuns > 0 {
		resp.MaxRuns = &sc.MaxRuns
	}
	if withUpcoming {
		resp.Upcoming = sc.Upcoming(5)
	}
	return resp
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request, p principal) error {
	var req scheduleRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	fe := fieldErrors{}
	if req.Name == nil {
		fe.add("name", "required")
	}
	if req.JobType == nil || *req.JobType == "" {
		fe.add("job_type", "required")
	}
	if req.Trigger == nil {
		fe.add("trigger", "required")
	}
	if err := fe.err(); err != nil {
		return err
	}
	quotas, err := s.admission.tenantQuotas(r.Context(), p.Tenant)
	if err != nil {
		return err
	}
	if limit := s.admission.payloadLimit(quotas); len(req.Payload) > limit {
		return payloadTooLarge(limit)
	}
	jt, err := s.store.GetJobType(r.Context(), p.Tenant, *req.JobType)
	if errors.Is(err, domain.ErrNotFound) {
		return fieldErrors{"job_type": "unknown job type"}.err()
	}
	if err != nil {
		return err
	}
	sc := domain.Schedule{
		TenantID: p.Tenant, JobType: jt.Name, Payload: []byte("{}"), Misfire: domain.MisfireFireOnce,
		Overlap: domain.OverlapSkip, CreatedBy: "api_key:" + p.KeyID,
	}
	req.apply(&sc, fe, s.now(), s.admission.minInterval(quotas))
	if err := fe.err(); err != nil {
		return err
	}
	if err := s.validatePayload(jt, sc.Payload); err != nil {
		return err
	}
	created, err := s.store.CreateSchedule(r.Context(), sc, s.audit(r, p))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, toScheduleResponse(created, true))
	return nil
}

func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request, p principal) error {
	sc, err := s.store.GetSchedule(r.Context(), p.Tenant, domain.ScheduleID(r.PathValue("id")))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toScheduleResponse(sc, true))
	return nil
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request, p principal) error {
	fe := fieldErrors{}
	limit, after := parsePage(r.URL.Query(), fe)
	if err := fe.err(); err != nil {
		return err
	}
	var cursor *postgres.ScheduleCursor
	if after != nil {
		cursor = &postgres.ScheduleCursor{CreatedAt: after.CreatedAt, ID: domain.ScheduleID(after.ID)}
	}
	page, next, err := s.store.ListSchedules(r.Context(), p.Tenant, limit, cursor)
	if err != nil {
		return err
	}
	resp := listResponse[scheduleResponse]{Items: make([]scheduleResponse, 0, len(page))}
	for _, sc := range page {
		resp.Items = append(resp.Items, toScheduleResponse(sc, false))
	}
	if next != nil {
		resp.NextCursor = encodeCursor(next.CreatedAt, string(next.ID))
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) patchSchedule(w http.ResponseWriter, r *http.Request, p principal) error {
	var req scheduleRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	if req.JobType != nil {
		return fieldErrors{"job_type": "cannot be changed; create a new schedule"}.err()
	}
	quotas, err := s.admission.tenantQuotas(r.Context(), p.Tenant)
	if err != nil {
		return err
	}
	if limit := s.admission.payloadLimit(quotas); len(req.Payload) > limit {
		return payloadTooLarge(limit)
	}
	now, minInterval := s.now(), s.admission.minInterval(quotas)
	sc, err := s.store.UpdateSchedule(r.Context(), p.Tenant, domain.ScheduleID(r.PathValue("id")), s.audit(r, p),
		func(sc *domain.Schedule) error {
			fe := fieldErrors{}
			req.apply(sc, fe, now, minInterval)
			if err := fe.err(); err != nil || req.Payload == nil {
				return err
			}
			jt, err := s.store.GetJobType(r.Context(), p.Tenant, sc.JobType)
			if err != nil {
				return err
			}
			return s.validatePayload(jt, sc.Payload)
		})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toScheduleResponse(sc, true))
	return nil
}

func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request, p principal) error {
	if err := s.store.DeleteSchedule(r.Context(), p.Tenant, domain.ScheduleID(r.PathValue("id")), s.audit(r, p)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

type scheduleActionFunc func(ctx context.Context, tenant domain.TenantID, id domain.ScheduleID, audit postgres.Audit) (domain.Schedule, error)

// scheduleAction adapts pause and resume (LLD §10.5) to an endpoint.
func (s *Server) scheduleAction(action scheduleActionFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request, p principal) error {
		sc, err := action(r.Context(), p.Tenant, domain.ScheduleID(r.PathValue("id")), s.audit(r, p))
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, toScheduleResponse(sc, true))
		return nil
	}
}
