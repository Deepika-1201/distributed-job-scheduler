package domain

import "time"

// MaxPayloadBytes is the platform's payload limit; tenant quotas can only lower it.
const MaxPayloadBytes = 64 << 10

// Quotas are a tenant's admission and execution limits (ADR-018). A nil field means the
// platform default: unlimited, except for the rate, interval and payload size.
type Quotas struct {
	RateLimit           *float64 // submissions per second across all api replicas
	MaxPending          *int     // jobs not yet finished
	MaxRunning          *int     // running jobs in each pool
	MaxSchedules        *int     // active and paused schedules
	MinScheduleInterval *time.Duration
	MaxPayloadBytes     *int
}

// Tenant is an isolated customer of the platform.
type Tenant struct {
	ID        TenantID
	Name      string
	CreatedAt time.Time
	Quotas    Quotas
}
