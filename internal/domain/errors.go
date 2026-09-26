package domain

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	// ErrConflict reports a uniqueness clash other than a duplicate name, such as a taken dedupe key.
	ErrConflict = errors.New("conflict")
	// ErrStaleAttempt rejects a report from an attempt that no longer holds the job's lease.
	ErrStaleAttempt          = errors.New("stale attempt")
	ErrIdempotencyKeyReused  = errors.New("idempotency key reused with a different request")
	ErrIdempotencyInProgress = errors.New("a request with this idempotency key is still in progress")
)
