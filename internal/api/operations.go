package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

type operationFilterBody struct {
	State      domain.JobState `json:"state,omitempty"`
	Type       string          `json:"type,omitempty"`
	Label      string          `json:"label,omitempty"` // key:value
	ScheduleID string          `json:"schedule_id,omitempty"`
}

type operationRequest struct {
	Kind   domain.OperationKind `json:"kind"`
	Filter operationFilterBody  `json:"filter"`
}

type operationResponse struct {
	ID         string                `json:"id"`
	Kind       domain.OperationKind  `json:"kind"`
	Filter     operationFilterBody   `json:"filter"`
	State      domain.OperationState `json:"state"`
	Succeeded  int                   `json:"succeeded"`
	Skipped    int                   `json:"skipped"`
	Failed     int                   `json:"failed"`
	LastError  string                `json:"last_error,omitempty"`
	CreatedBy  string                `json:"created_by"`
	CreatedAt  time.Time             `json:"created_at"`
	UpdatedAt  time.Time             `json:"updated_at"`
	FinishedAt *time.Time            `json:"finished_at,omitempty"`
}

func toOperationResponse(op domain.Operation) operationResponse {
	f := operationFilterBody{State: op.Filter.State, Type: op.Filter.Type, ScheduleID: string(op.Filter.ScheduleID)}
	if op.Filter.LabelKey != "" {
		f.Label = op.Filter.LabelKey + ":" + op.Filter.LabelValue
	}
	return operationResponse{ID: op.ID, Kind: op.Kind, Filter: f, State: op.State, Succeeded: op.Succeeded,
		Skipped: op.Skipped, Failed: op.Failed, LastError: op.LastError, CreatedBy: op.CreatedBy,
		CreatedAt: op.CreatedAt, UpdatedAt: op.UpdatedAt, FinishedAt: optTime(op.FinishedAt)}
}

// createOperation starts a bulk cancel or re-drive (LLD §13.3).
func (s *Server) createOperation(w http.ResponseWriter, r *http.Request, p principal) error {
	var req operationRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	fe := fieldErrors{}
	filter := domain.OperationFilter{State: req.Filter.State, Type: req.Filter.Type, ScheduleID: domain.ScheduleID(req.Filter.ScheduleID)}
	switch req.Kind {
	case domain.OperationCancel:
		if filter.State != "" && (!filter.State.Valid() || filter.State.Terminal()) {
			fe.add("filter.state", "must be an active state for cancel")
		}
	case domain.OperationRedrive:
		if filter.State != domain.StateFailed && filter.State != domain.StateDeadLettered {
			fe.add("filter.state", "must be FAILED or DEAD_LETTERED for redrive")
		}
	default:
		fe.add("kind", "must be cancel or redrive")
	}
	if req.Filter.Label != "" {
		k, v, ok := strings.Cut(req.Filter.Label, ":")
		if !ok || k == "" {
			fe.add("filter.label", "must be key:value")
		}
		filter.LabelKey, filter.LabelValue = k, v
	}
	if filter.ScheduleID != "" {
		if _, err := uuid.Parse(string(filter.ScheduleID)); err != nil {
			fe.add("filter.schedule_id", "must be a UUID")
		}
	}
	if err := fe.err(); err != nil {
		return err
	}
	op, err := s.store.CreateOperation(r.Context(), domain.Operation{TenantID: p.Tenant, Kind: req.Kind, Filter: filter,
		CreatedBy: "api_key:" + p.KeyID, RequestID: requestID(r)}, s.audit(r, p))
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/v1/operations/"+op.ID)
	writeJSON(w, http.StatusAccepted, toOperationResponse(op))
	return nil
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request, p principal) error {
	op, err := s.store.GetOperation(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toOperationResponse(op))
	return nil
}
