package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

// apiError is an error with its HTTP representation (LLD §9.2).
type apiError struct {
	status     int
	code       string
	message    string
	details    map[string]any
	retryAfter time.Duration
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func newError(status int, code, format string, args ...any) *apiError {
	return &apiError{status: status, code: code, message: fmt.Sprintf(format, args...)}
}

var (
	errUnauthenticated = newError(http.StatusUnauthorized, "unauthenticated", "a valid API key is required")
	errInternal        = newError(http.StatusInternalServerError, "internal", "internal error")
	// errUnavailable answers while the database can't be reached; requests may be retried (HLD S6).
	errUnavailable = &apiError{status: http.StatusServiceUnavailable, code: "unavailable",
		message: "the service is temporarily unavailable; retry later", retryAfter: 5 * time.Second}
)

func errPermission(format string, args ...any) *apiError {
	return newError(http.StatusForbidden, "permission_denied", format, args...)
}

// toAPIError maps domain and store errors to responses; anything unrecognized is internal.
func toAPIError(err error) *apiError {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, domain.ErrNotFound):
		return newError(http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrConflict):
		return newError(http.StatusConflict, "conflict", "%s", err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return newError(http.StatusConflict, "conflict", "resource already exists")
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		return newError(http.StatusUnprocessableEntity, "idempotency_key_reused", "idempotency key was used with a different request body")
	case errors.Is(err, domain.ErrIdempotencyInProgress):
		return newError(http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is still in progress")
	case errors.Is(err, domain.ErrQuotaExceeded):
		return newError(http.StatusTooManyRequests, "quota_exceeded", "the tenant's quota does not allow this request")
	case postgres.IsUnavailable(err):
		return errUnavailable
	}
	return errInternal
}

// conflictWithState reports an invalid transition along with the job's current state.
func conflictWithState(err error, state domain.JobState) error {
	if !errors.Is(err, domain.ErrInvalidTransition) || state == "" {
		return err
	}
	ae := toAPIError(err)
	ae.details = map[string]any{"state": state}
	return ae
}

// fieldErrors collects validation failures keyed by request field.
type fieldErrors map[string]string

func (f fieldErrors) add(field, format string, args ...any) {
	if _, dup := f[field]; !dup {
		f[field] = fmt.Sprintf(format, args...)
	}
}

func (f fieldErrors) err() error {
	if len(f) == 0 {
		return nil
	}
	return &apiError{status: http.StatusUnprocessableEntity, code: "invalid_argument",
		message: "request validation failed", details: map[string]any{"fields": map[string]string(f)}}
}

type errorBody struct {
	Error struct {
		Code      string         `json:"code"`
		Message   string         `json:"message"`
		RequestID string         `json:"request_id"`
		Details   map[string]any `json:"details,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, r *http.Request, e *apiError) {
	var body errorBody
	body.Error.Code, body.Error.Message, body.Error.RequestID, body.Error.Details = e.code, e.message, requestID(r), e.details
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int((e.retryAfter+time.Second-1)/time.Second)))
	}
	writeJSON(w, e.status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
