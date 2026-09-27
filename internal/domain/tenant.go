package domain

import (
	"fmt"
	"slices"
	"time"
)

// JobType is a registered kind of job and the defaults its submissions inherit.
type JobType struct {
	TenantID        TenantID
	Name            string
	Version         int
	Pool            string
	DefaultPriority Priority
	AttemptTimeout  time.Duration
	RetryPolicy     RetryPolicy
	AtMostOnce      bool
	Enabled         bool // false rejects submissions and holds schedules
	Paused          bool // true holds dispatch; submissions and schedules continue
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Role is an API key's permission level; each role includes the ones before it.
type Role string

const (
	RoleViewer    Role = "viewer"
	RoleSubmitter Role = "submitter"
	RoleOperator  Role = "operator"
	RoleAdmin     Role = "admin"
	// RolePlatformAdmin manages shared platform resources: pools, workers and quotas (ADR-017).
	RolePlatformAdmin Role = "platform-admin"
)

var roleOrder = []Role{RoleViewer, RoleSubmitter, RoleOperator, RoleAdmin, RolePlatformAdmin}

func ParseRole(s string) (Role, error) {
	if r := Role(s); slices.Contains(roleOrder, r) {
		return r, nil
	}
	return "", fmt.Errorf("unknown role %q (want viewer, submitter, operator, admin or platform-admin)", s)
}

// Includes reports whether r grants at least the permissions of min.
func (r Role) Includes(min Role) bool {
	i, j := slices.Index(roleOrder, r), slices.Index(roleOrder, min)
	return i >= 0 && j >= 0 && i >= j
}

// APIKey is a stored credential; the secret itself is never stored, only its hash.
type APIKey struct {
	ID         string
	TenantID   TenantID
	Name       string
	Prefix     string
	SecretHash []byte
	Role       Role
	CreatedAt  time.Time
	ExpiresAt  time.Time // zero means never
	RevokedAt  time.Time // zero means active
}

// Usable reports whether the key may authenticate at now.
func (k APIKey) Usable(now time.Time) bool {
	return k.RevokedAt.IsZero() && (k.ExpiresAt.IsZero() || now.Before(k.ExpiresAt))
}
