package domain

import "time"

type OperationKind string

const (
	OperationCancel  OperationKind = "cancel"
	OperationRedrive OperationKind = "redrive"
)

type OperationState string

const (
	OperationPending   OperationState = "PENDING"
	OperationRunning   OperationState = "RUNNING"
	OperationSucceeded OperationState = "SUCCEEDED"
	OperationFailed    OperationState = "FAILED"
)

// OperationFilter selects the jobs a bulk operation acts on; zero fields don't filter.
type OperationFilter struct {
	State      JobState   `json:"state,omitempty"`
	Type       string     `json:"type,omitempty"`
	ScheduleID ScheduleID `json:"schedule_id,omitempty"`
	LabelKey   string     `json:"label_key,omitempty"`
	LabelValue string     `json:"label_value,omitempty"`
}

// Operation is a bulk cancel or re-drive, processed in batches by the engine (LLD §13.3).
type Operation struct {
	ID         string
	TenantID   TenantID
	Kind       OperationKind
	Filter     OperationFilter
	State      OperationState
	Succeeded  int
	Skipped    int
	Failed     int
	LastError  string
	CreatedBy  string
	RequestID  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	FinishedAt time.Time
}
