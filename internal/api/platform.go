package api

import (
	"net/http"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

type workerResponse struct {
	ID             string            `json:"id"`
	Pool           string            `json:"pool"`
	WorkerID       string            `json:"worker_id"`
	JobTypes       []string          `json:"job_types"`
	Slots          int               `json:"slots"`
	Running        int               `json:"running"`
	Labels         map[string]string `json:"labels"`
	RuntimeVersion string            `json:"runtime_version"`
	Draining       bool              `json:"draining"`
	CreatedAt      time.Time         `json:"created_at"`
	HeartbeatAt    time.Time         `json:"heartbeat_at"`
	LeaseExpiresAt time.Time         `json:"lease_expires_at"`
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request, _ principal) error {
	pool := r.URL.Query().Get("pool")
	if pool != "" && !namePattern.MatchString(pool) {
		return fieldErrors{"pool": "must match " + namePattern.String()}.err()
	}
	sessions, err := s.store.ListSessions(r.Context(), pool)
	if err != nil {
		return err
	}
	resp := listResponse[workerResponse]{Items: make([]workerResponse, 0, len(sessions))}
	for _, ws := range sessions {
		resp.Items = append(resp.Items, workerResponse{
			ID: string(ws.ID), Pool: ws.Pool, WorkerID: ws.WorkerID, JobTypes: ws.JobTypes, Slots: ws.Slots,
			Running: ws.Running, Labels: ws.Labels, RuntimeVersion: ws.RuntimeVersion, Draining: ws.Draining,
			CreatedAt: ws.CreatedAt, HeartbeatAt: ws.HeartbeatAt, LeaseExpiresAt: ws.LeaseExpiresAt,
		})
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) drainWorker(w http.ResponseWriter, r *http.Request, p principal) error {
	if err := s.store.DrainSession(r.Context(), domain.SessionID(r.PathValue("id")), p.Tenant, s.audit(r, p)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *Server) deregisterWorker(w http.ResponseWriter, r *http.Request, p principal) error {
	if err := s.store.DeregisterSession(r.Context(), domain.SessionID(r.PathValue("id")), p.Tenant, s.audit(r, p)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

type poolOwner struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Epoch     int64     `json:"epoch"`
	ExpiresAt time.Time `json:"lease_expires_at"`
}

type poolResponse struct {
	Name          string     `json:"name"`
	Paused        bool       `json:"paused"`
	Owner         *poolOwner `json:"owner"`
	Workers       int        `json:"workers"`
	Slots         int        `json:"slots"`
	ReadyJobs     int        `json:"ready_jobs"`
	RunningJobs   int        `json:"running_jobs"`
	BacklogTarget string     `json:"backlog_target"`
	BacklogAge    *string    `json:"backlog_age"`
}

func (s *Server) toPoolResponse(p postgres.PoolInfo) poolResponse {
	resp := poolResponse{Name: p.Name, Paused: p.Paused, Workers: p.Workers, Slots: p.Slots, ReadyJobs: p.ReadyJobs,
		RunningJobs: p.RunningJobs, BacklogTarget: s.admission.target(postgres.PoolBacklog{Target: p.BacklogTarget}).String()}
	if p.Owner != nil {
		resp.Owner = &poolOwner{NodeID: p.Owner.Holder, Address: p.Owner.Address, Epoch: p.Owner.Epoch, ExpiresAt: p.Owner.ExpiresAt}
	}
	if p.BacklogAge != nil {
		age := p.BacklogAge.Truncate(time.Second).String()
		resp.BacklogAge = &age
	}
	return resp
}

func (s *Server) listPools(w http.ResponseWriter, r *http.Request, _ principal) error {
	pools, err := s.store.ListPools(r.Context())
	if err != nil {
		return err
	}
	resp := listResponse[poolResponse]{Items: make([]poolResponse, 0, len(pools))}
	for _, p := range pools {
		resp.Items = append(resp.Items, s.toPoolResponse(p))
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

// writePool responds with the pool's current state.
func (s *Server) writePool(w http.ResponseWriter, r *http.Request, name string) error {
	pools, err := s.store.ListPools(r.Context())
	if err != nil {
		return err
	}
	for _, info := range pools {
		if info.Name == name {
			writeJSON(w, http.StatusOK, s.toPoolResponse(info))
			return nil
		}
	}
	return domain.ErrNotFound
}

// poolAction pauses or resumes dispatch for a pool (ADR-017).
func (s *Server) poolAction(paused bool) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request, p principal) error {
		name := r.PathValue("name")
		if !namePattern.MatchString(name) {
			return fieldErrors{"name": "must match " + namePattern.String()}.err()
		}
		if err := s.store.SetPoolPaused(r.Context(), name, paused, p.Tenant, s.audit(r, p)); err != nil {
			return err
		}
		return s.writePool(w, r, name)
	}
}

// poolSettingsBody is the wire form of a pool's settings; null means the platform default.
type poolSettingsBody struct {
	BacklogTarget *string `json:"backlog_target"`
}

// putPoolSettings replaces a pool's settings (ADR-021).
func (s *Server) putPoolSettings(w http.ResponseWriter, r *http.Request, p principal) error {
	name := r.PathValue("name")
	if !namePattern.MatchString(name) {
		return fieldErrors{"name": "must match " + namePattern.String()}.err()
	}
	var body poolSettingsBody
	if _, err := readJSON(r, &body); err != nil {
		return err
	}
	var target *time.Duration
	if b := body.BacklogTarget; b != nil {
		d, err := time.ParseDuration(*b)
		if err != nil || d < 10*time.Second || d > 24*time.Hour {
			return fieldErrors{"backlog_target": "must be a duration between 10s and 24h"}.err()
		}
		d = d.Truncate(time.Millisecond)
		target = &d
	}
	if err := s.store.SetPoolSettings(r.Context(), name, target, p.Tenant, s.audit(r, p)); err != nil {
		return err
	}
	return s.writePool(w, r, name)
}

// jobTypeAction pauses or resumes dispatch of one of the tenant's job types.
func (s *Server) jobTypeAction(paused bool) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request, p principal) error {
		jt, err := s.store.SetJobTypePaused(r.Context(), p.Tenant, r.PathValue("name"), paused, s.audit(r, p))
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, toJobTypeResponse(jt))
		return nil
	}
}

func (s *Server) triggerSchedule(w http.ResponseWriter, r *http.Request, p principal) error {
	job, err := s.store.TriggerSchedule(r.Context(), p.Tenant, domain.ScheduleID(r.PathValue("id")), s.audit(r, p))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, toJobResponse(job))
	return nil
}
