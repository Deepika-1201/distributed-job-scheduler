package domain

import "time"

type (
	TenantID   string
	JobID      string
	ScheduleID string
	AttemptID  string
	SessionID  string
)

// Job is one unit of work (LLD §3, §8.2).
type Job struct {
	ID            JobID
	TenantID      TenantID
	Type          string
	TypeVersion   int
	Pool          string
	ScheduleID    ScheduleID // empty unless created by a schedule
	FireTime      time.Time  // zero unless created by a schedule
	State         JobState
	Priority      Priority
	Payload       []byte // JSON, exactly as submitted
	Labels        map[string]string
	DedupeKey     string
	CorrelationID string
	CreatedBy     string
	RequestID     string

	RunAt          time.Time
	StartDeadline  time.Time // zero means none
	Deadline       time.Time // zero means none
	AttemptTimeout time.Duration
	RetryPolicy    RetryPolicy
	AtMostOnce     bool

	// AttemptCount is the latest attempt's number, which is also its fencing token.
	AttemptCount      int
	Budget            RetryBudget
	Current           *RunningAttempt // non-nil only while RUNNING
	CancelRequestedAt time.Time

	LastError  string
	Result     []byte // JSON; set only on success
	Reason     Reason // set only once terminal
	CreatedAt  time.Time
	ReadyAt    time.Time
	UpdatedAt  time.Time
	FinishedAt time.Time // set only once terminal
}

// RunningAttempt is the attempt currently holding a job's lease.
type RunningAttempt struct {
	ID        AttemptID
	Number    int
	SessionID SessionID
	StartedAt time.Time
	Deadline  time.Time
}

// Attempt is a finished attempt; attempts are recorded once, when they end.
type Attempt struct {
	ID         AttemptID
	JobID      JobID
	TenantID   TenantID
	Number     int
	SessionID  SessionID
	WorkerID   string // the session's worker; empty once the session is purged
	State      AttemptState
	Retryable  bool
	Error      string
	StartedAt  time.Time
	Deadline   time.Time
	FinishedAt time.Time
	Actor      Actor
}
