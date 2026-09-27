package api

import (
	"net/http"
	"time"

	"jobscheduler/internal/domain"
)

// quotasBody is the wire form of domain.Quotas; null means the platform default.
type quotasBody struct {
	RateLimit           *float64 `json:"rate_limit"`
	MaxPending          *int     `json:"max_pending"`
	MaxRunning          *int     `json:"max_running"`
	MaxSchedules        *int     `json:"max_schedules"`
	MinScheduleInterval *string  `json:"min_schedule_interval"`
	MaxPayloadBytes     *int     `json:"max_payload_bytes"`
}

func toQuotasBody(q domain.Quotas) quotasBody {
	b := quotasBody{RateLimit: q.RateLimit, MaxPending: q.MaxPending, MaxRunning: q.MaxRunning,
		MaxSchedules: q.MaxSchedules, MaxPayloadBytes: q.MaxPayloadBytes}
	if q.MinScheduleInterval != nil {
		s := q.MinScheduleInterval.String()
		b.MinScheduleInterval = &s
	}
	return b
}

// quotas validates the body against the limits of LLD §15.2.
func (b quotasBody) quotas() (domain.Quotas, error) {
	fe := fieldErrors{}
	q := domain.Quotas{RateLimit: b.RateLimit, MaxPending: b.MaxPending, MaxRunning: b.MaxRunning,
		MaxSchedules: b.MaxSchedules, MaxPayloadBytes: b.MaxPayloadBytes}
	if r := b.RateLimit; r != nil && (*r <= 0 || *r > 1e6) {
		fe.add("rate_limit", "must be greater than 0 and at most 1000000")
	}
	if n := b.MaxPending; n != nil && (*n < 1 || *n > 1_000_000) {
		fe.add("max_pending", "must be between 1 and 1000000")
	}
	for field, n := range map[string]*int{"max_running": b.MaxRunning, "max_schedules": b.MaxSchedules} {
		if n != nil && *n < 1 {
			fe.add(field, "must be at least 1")
		}
	}
	if n := b.MaxPayloadBytes; n != nil && (*n < 1 || *n > domain.MaxPayloadBytes) {
		fe.add("max_payload_bytes", "must be between 1 and %d", domain.MaxPayloadBytes)
	}
	if s := b.MinScheduleInterval; s != nil {
		d, err := time.ParseDuration(*s)
		if err != nil || d < time.Second || d > 24*time.Hour {
			fe.add("min_schedule_interval", "must be a duration between 1s and 24h")
		}
		d = d.Truncate(time.Millisecond)
		q.MinScheduleInterval = &d
	}
	return q, fe.err()
}

// getOwnQuotas returns the caller's effective quotas, with platform defaults filled in.
func (s *Server) getOwnQuotas(w http.ResponseWriter, r *http.Request, p principal) error {
	q, err := s.admission.tenantQuotas(r.Context(), p.Tenant)
	if err != nil {
		return err
	}
	rate := s.admission.rate(q) * float64(s.admission.replicas)
	payload, interval := s.admission.payloadLimit(q), s.admission.minInterval(q)
	q.RateLimit, q.MaxPayloadBytes, q.MinScheduleInterval = &rate, &payload, &interval
	writeJSON(w, http.StatusOK, toQuotasBody(q))
	return nil
}

type tenantResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request, _ principal) error {
	tenants, err := s.store.ListTenants(r.Context())
	if err != nil {
		return err
	}
	resp := listResponse[tenantResponse]{Items: make([]tenantResponse, 0, len(tenants))}
	for _, t := range tenants {
		resp.Items = append(resp.Items, tenantResponse{ID: string(t.ID), Name: t.Name, CreatedAt: t.CreatedAt})
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) getTenantQuotas(w http.ResponseWriter, r *http.Request, _ principal) error {
	t, err := s.store.GetTenant(r.Context(), domain.TenantID(r.PathValue("id")))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toQuotasBody(t.Quotas))
	return nil
}

func (s *Server) putTenantQuotas(w http.ResponseWriter, r *http.Request, p principal) error {
	var body quotasBody
	if _, err := readJSON(r, &body); err != nil {
		return err
	}
	q, err := body.quotas()
	if err != nil {
		return err
	}
	id := domain.TenantID(r.PathValue("id"))
	t, err := s.store.SetQuotas(r.Context(), id, q, p.Tenant, s.audit(r, p))
	if err != nil {
		return err
	}
	s.admission.forget(id)
	writeJSON(w, http.StatusOK, toQuotasBody(t.Quotas))
	return nil
}
