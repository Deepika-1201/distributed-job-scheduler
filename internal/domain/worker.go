package domain

import "time"

type SessionState string

const (
	SessionActive  SessionState = "ACTIVE"
	SessionClosed  SessionState = "CLOSED"
	SessionExpired SessionState = "EXPIRED"
)

// WorkerSession is a registered worker process serving one pool (LLD §12.2).
type WorkerSession struct {
	ID             SessionID
	Pool           string
	WorkerID       string
	JobTypes       []string // empty means every type in the pool
	Slots          int
	Labels         map[string]string
	RuntimeVersion string
	State          SessionState
	Draining       bool   // gets no new assignments; the worker is told to drain
	RenewedBy      string // the engine node that last renewed the lease (ADR-029)
	CreatedAt      time.Time
	HeartbeatAt    time.Time
	LeaseExpiresAt time.Time
}
