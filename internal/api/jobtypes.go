package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"jobscheduler/internal/domain"
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,99}$`)

type jobTypeRequest struct {
	Name            string                    `json:"name"`
	Pool            *string                   `json:"pool"`
	DefaultPriority *string                   `json:"default_priority"`
	AttemptTimeout  *string                   `json:"attempt_timeout"`
	RetryPolicy     *retryPolicyBody          `json:"retry_policy"`
	AtMostOnce      *bool                     `json:"at_most_once"`
	Enabled         *bool                     `json:"enabled"`
	PayloadSchema   optional[json.RawMessage] `json:"payload_schema"`
}

type jobTypeResponse struct {
	Name            string           `json:"name"`
	Version         int              `json:"version"`
	Pool            string           `json:"pool"`
	DefaultPriority domain.Priority  `json:"default_priority"`
	AttemptTimeout  string           `json:"attempt_timeout"`
	RetryPolicy     *retryPolicyBody `json:"retry_policy"`
	AtMostOnce      bool             `json:"at_most_once"`
	PayloadSchema   json.RawMessage  `json:"payload_schema,omitempty"`
	Enabled         bool             `json:"enabled"`
	Paused          bool             `json:"paused"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

func toJobTypeResponse(jt domain.JobType) jobTypeResponse {
	return jobTypeResponse{
		Name: jt.Name, Version: jt.Version, Pool: jt.Pool, DefaultPriority: jt.DefaultPriority,
		AttemptTimeout: jt.AttemptTimeout.String(), RetryPolicy: policyBody(jt.RetryPolicy),
		AtMostOnce: jt.AtMostOnce, PayloadSchema: jt.PayloadSchema, Enabled: jt.Enabled, Paused: jt.Paused, CreatedAt: jt.CreatedAt, UpdatedAt: jt.UpdatedAt,
	}
}

// applyTo overlays the fields present in the request onto jt and validates the result.
func (req jobTypeRequest) applyTo(jt domain.JobType) (domain.JobType, error) {
	fe := fieldErrors{}
	if req.PayloadSchema.Set {
		jt.PayloadSchema = nil
		if v := req.PayloadSchema.Value; v != nil && string(*v) != "null" {
			if _, err := compileSchema(*v); err != nil {
				fe.add("payload_schema", "%s", err.Error())
			}
			jt.PayloadSchema = *v
		}
	}
	if req.Pool != nil {
		jt.Pool = *req.Pool
	}
	if !namePattern.MatchString(jt.Pool) {
		fe.add("pool", "must match %s", namePattern)
	}
	if req.DefaultPriority != nil {
		p, err := domain.ParsePriority(*req.DefaultPriority)
		if err != nil {
			fe.add("default_priority", "must be CRITICAL, HIGH, NORMAL or LOW")
		}
		jt.DefaultPriority = p
	}
	if req.AttemptTimeout != nil {
		d, err := time.ParseDuration(*req.AttemptTimeout)
		if err != nil || d < time.Second || d > 24*time.Hour {
			fe.add("attempt_timeout", "must be a duration between 1s and 24h")
		}
		jt.AttemptTimeout = d
	}
	jt.RetryPolicy = req.RetryPolicy.apply(jt.RetryPolicy, fe, "retry_policy")
	if req.AtMostOnce != nil {
		jt.AtMostOnce = *req.AtMostOnce
	}
	if req.Enabled != nil {
		jt.Enabled = *req.Enabled
	}
	return jt, fe.err()
}

func (s *Server) createJobType(w http.ResponseWriter, r *http.Request, p principal) error {
	var req jobTypeRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	if !namePattern.MatchString(req.Name) {
		return fieldErrors{"name": "must match " + namePattern.String()}.err()
	}
	jt, err := req.applyTo(domain.JobType{
		TenantID: p.Tenant, Name: req.Name, Pool: "default", DefaultPriority: domain.PriorityNormal,
		AttemptTimeout: 5 * time.Minute, RetryPolicy: domain.DefaultRetryPolicy(), Enabled: true,
	})
	if err != nil {
		return err
	}
	created, err := s.store.CreateJobType(r.Context(), jt, s.audit(r, p))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, toJobTypeResponse(created))
	return nil
}

func (s *Server) getJobType(w http.ResponseWriter, r *http.Request, p principal) error {
	jt, err := s.store.GetJobType(r.Context(), p.Tenant, r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toJobTypeResponse(jt))
	return nil
}

func (s *Server) listJobTypes(w http.ResponseWriter, r *http.Request, p principal) error {
	types, err := s.store.ListJobTypes(r.Context(), p.Tenant)
	if err != nil {
		return err
	}
	resp := listResponse[jobTypeResponse]{Items: make([]jobTypeResponse, 0, len(types))}
	for _, jt := range types {
		resp.Items = append(resp.Items, toJobTypeResponse(jt))
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) patchJobType(w http.ResponseWriter, r *http.Request, p principal) error {
	var req jobTypeRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	if req.Name != "" && req.Name != r.PathValue("name") {
		return fieldErrors{"name": "cannot be changed"}.err()
	}
	current, err := s.store.GetJobType(r.Context(), p.Tenant, r.PathValue("name"))
	if err != nil {
		return err
	}
	jt, err := req.applyTo(current)
	if err != nil {
		return err
	}
	updated, err := s.store.UpdateJobType(r.Context(), jt, s.audit(r, p))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toJobTypeResponse(updated))
	return nil
}

type apiKeyRequest struct {
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type apiKeyResponse struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Role      domain.Role `json:"role"`
	Key       string      `json:"key"`
	CreatedAt time.Time   `json:"created_at"`
	ExpiresAt *time.Time  `json:"expires_at,omitempty"`
}

func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request, p principal) error {
	var req apiKeyRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	fe := fieldErrors{}
	if req.Name == "" || len(req.Name) > 100 {
		fe.add("name", "must be 1-100 characters")
	}
	role, err := domain.ParseRole(req.Role)
	if err != nil {
		fe.add("role", "must be viewer, submitter, operator, admin or platform-admin")
	}
	var expires time.Time
	if req.ExpiresAt != nil {
		if expires = *req.ExpiresAt; !expires.After(s.now()) {
			fe.add("expires_at", "must be in the future")
		}
	}
	if err := fe.err(); err != nil {
		return err
	}
	if !p.Role.Includes(role) {
		return errPermission("a key cannot grant more than its own role (%s)", p.Role)
	}
	plaintext, key := NewAPIKey(p.Tenant, req.Name, role, expires)
	audit := s.audit(r, p)
	created, err := s.store.CreateAPIKey(r.Context(), key, &audit)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, apiKeyResponse{
		ID: created.ID, Name: created.Name, Role: created.Role, Key: plaintext,
		CreatedAt: created.CreatedAt, ExpiresAt: optTime(created.ExpiresAt),
	})
	return nil
}

func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request, p principal) error {
	if err := s.store.RevokeAPIKey(r.Context(), p.Tenant, r.PathValue("id"), s.audit(r, p)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
