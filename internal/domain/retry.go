package domain

import (
	"errors"
	"fmt"
	"time"
)

// BackoffStrategy selects how retry delays grow.
type BackoffStrategy string

const (
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

// Jitter selects how retry delays are randomized.
type Jitter string

const (
	JitterNone Jitter = "none"
	JitterFull Jitter = "full"
)

// RetryPolicy controls how a job is retried after failed attempts (ADR-008, LLD §6).
type RetryPolicy struct {
	Strategy     BackoffStrategy
	InitialDelay time.Duration
	Multiplier   float64 // exponential only
	MaxDelay     time.Duration
	Jitter       Jitter
	// MaxAttempts counts attempts per budget, including the first.
	MaxAttempts      int
	MaxRetryDuration time.Duration
	// MaxLostAttempts is how many lost attempts per budget mark the job as poison.
	MaxLostAttempts int
}

// DefaultRetryPolicy returns the platform default: exponential backoff with full jitter
// from 10 s doubling to a 1 h cap, 10 attempts, within 24 h, poison after 3 lost attempts.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Strategy:         BackoffExponential,
		InitialDelay:     10 * time.Second,
		Multiplier:       2,
		MaxDelay:         time.Hour,
		Jitter:           JitterFull,
		MaxAttempts:      10,
		MaxRetryDuration: 24 * time.Hour,
		MaxLostAttempts:  3,
	}
}

// RetryLimits bound the policies that job types and submissions may request.
type RetryLimits struct {
	MaxAttempts      int
	MinDelay         time.Duration
	MaxDelay         time.Duration
	MaxRetryDuration time.Duration
	MaxMultiplier    float64
}

func DefaultRetryLimits() RetryLimits {
	return RetryLimits{
		MaxAttempts:      50,
		MinDelay:         time.Second,
		MaxDelay:         24 * time.Hour,
		MaxRetryDuration: 7 * 24 * time.Hour,
		MaxMultiplier:    10,
	}
}

// Validate reports every way p violates l.
func (p RetryPolicy) Validate(l RetryLimits) error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	switch p.Strategy {
	case BackoffFixed:
	case BackoffExponential:
		if p.Multiplier < 1 || p.Multiplier > l.MaxMultiplier {
			fail("multiplier must be between 1 and %g, got %g", l.MaxMultiplier, p.Multiplier)
		}
	default:
		fail("unknown backoff strategy %q", p.Strategy)
	}
	if p.Jitter != JitterNone && p.Jitter != JitterFull {
		fail("unknown jitter %q", p.Jitter)
	}
	if p.InitialDelay < l.MinDelay {
		fail("initial delay must be at least %v, got %v", l.MinDelay, p.InitialDelay)
	}
	if p.MaxDelay < p.InitialDelay || p.MaxDelay > l.MaxDelay {
		fail("max delay must be between the initial delay and %v, got %v", l.MaxDelay, p.MaxDelay)
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > l.MaxAttempts {
		fail("max attempts must be between 1 and %d, got %d", l.MaxAttempts, p.MaxAttempts)
	}
	if p.MaxRetryDuration <= 0 || p.MaxRetryDuration > l.MaxRetryDuration {
		fail("max retry duration must be positive and at most %v, got %v", l.MaxRetryDuration, p.MaxRetryDuration)
	}
	if p.MaxLostAttempts < 1 || p.MaxLostAttempts > p.MaxAttempts {
		fail("max lost attempts must be between 1 and max attempts, got %d", p.MaxLostAttempts)
	}
	return errors.Join(errs...)
}

// Delay returns the backoff before the next attempt after attempt n of the current
// budget (1-based) failed. rnd must return values uniformly distributed in [0, 1).
func (p RetryPolicy) Delay(n int, rnd func() float64) time.Duration {
	d := p.InitialDelay
	if p.Strategy == BackoffExponential {
		for i := 1; i < n && d < p.MaxDelay; i++ {
			d = time.Duration(float64(d) * p.Multiplier)
		}
	}
	d = min(d, p.MaxDelay)
	if p.Jitter == JitterFull {
		d = time.Duration(rnd() * float64(d))
	}
	return d
}

// AttemptEnd describes how an attempt ended.
type AttemptEnd struct {
	State AttemptState
	// Retryable reports whether the handler marked an AttemptFailed failure as retryable.
	Retryable bool
	// RetryAfter is an optional handler hint for retryable failures, capped at MaxDelay.
	RetryAfter time.Duration
}

// RetryBudget counts attempts since the job was created or last manually retried.
type RetryBudget struct {
	Attempts  int       // including the attempt that just ended
	Lost      int       // lost attempts, including the one that just ended
	StartedAt time.Time // start of the budget's first attempt
}

// DecisionInput is everything DecideAfterAttempt needs. Now must come from the
// database clock.
type DecisionInput struct {
	End             AttemptEnd
	Policy          RetryPolicy
	Budget          RetryBudget
	Deadline        time.Time // optional explicit overall deadline; zero means none
	CancelRequested bool
	AtMostOnce      bool
	Now             time.Time
}

// Reason explains why a job left RUNNING; values are persisted and exposed by the API.
type Reason string

const (
	ReasonCancelled         Reason = "CANCELLED"
	ReasonNonRetryable      Reason = "NON_RETRYABLE"
	ReasonAtMostOnce        Reason = "AT_MOST_ONCE"
	ReasonOutcomeUnknown    Reason = "OUTCOME_UNKNOWN"
	ReasonPoison            Reason = "POISON"
	ReasonAttemptsExhausted Reason = "ATTEMPTS_EXHAUSTED"
	ReasonDeadlineExceeded  Reason = "DEADLINE_EXCEEDED"
	ReasonRetryableError    Reason = "RETRYABLE_ERROR"
	ReasonTimedOut          Reason = "TIMED_OUT"
	ReasonLost              Reason = "LOST"
	ReasonStartDeadline     Reason = "START_DEADLINE_EXCEEDED"
	ReasonOverlap           Reason = "OVERLAP"    // skipped by the schedule's overlap policy
	ReasonSuperseded        Reason = "SUPERSEDED" // cancelled by a newer run (cancel_previous)
)

var retryReasons = map[AttemptState]Reason{
	AttemptFailed:   ReasonRetryableError,
	AttemptTimedOut: ReasonTimedOut,
	AttemptLost:     ReasonLost,
}

// Decision is a job's next state after an attempt ends.
type Decision struct {
	Next   JobState
	RunAt  time.Time // when the next attempt is due; set only for StateRetryPending
	Reason Reason    // empty on success
}

// DecideAfterAttempt applies the retry rules of LLD §6 in order; the first match wins.
// rnd supplies jitter and must return values uniformly distributed in [0, 1).
func DecideAfterAttempt(in DecisionInput, rnd func() float64) (Decision, error) {
	end := in.End
	if !end.State.Terminal() {
		return Decision{}, fmt.Errorf("attempt state %q is not terminal", end.State)
	}

	switch {
	case end.State == AttemptSucceeded:
		return Decision{Next: StateSucceeded}, nil
	case in.CancelRequested || end.State == AttemptCancelled:
		return Decision{Next: StateCancelled, Reason: ReasonCancelled}, nil
	case end.State == AttemptFailed && !end.Retryable:
		return Decision{Next: StateFailed, Reason: ReasonNonRetryable}, nil
	case in.AtMostOnce && end.State == AttemptFailed:
		return Decision{Next: StateFailed, Reason: ReasonAtMostOnce}, nil
	case in.AtMostOnce:
		return Decision{Next: StateFailed, Reason: ReasonOutcomeUnknown}, nil
	case end.State == AttemptLost && in.Budget.Lost >= in.Policy.MaxLostAttempts:
		return Decision{Next: StateDeadLettered, Reason: ReasonPoison}, nil
	case in.Budget.Attempts >= in.Policy.MaxAttempts:
		return Decision{Next: StateDeadLettered, Reason: ReasonAttemptsExhausted}, nil
	}

	var delay time.Duration
	if end.State == AttemptFailed && end.RetryAfter > 0 {
		delay = min(end.RetryAfter, in.Policy.MaxDelay)
	} else {
		delay = in.Policy.Delay(in.Budget.Attempts, rnd)
	}
	runAt := in.Now.Add(delay)
	if deadline := in.effectiveDeadline(); !deadline.IsZero() && runAt.After(deadline) {
		return Decision{Next: StateDeadLettered, Reason: ReasonDeadlineExceeded}, nil
	}
	return Decision{Next: StateRetryPending, RunAt: runAt, Reason: retryReasons[end.State]}, nil
}

// effectiveDeadline is the earlier of the budget's retry window and the explicit deadline.
func (in DecisionInput) effectiveDeadline() time.Time {
	var d time.Time
	if !in.Budget.StartedAt.IsZero() && in.Policy.MaxRetryDuration > 0 {
		d = in.Budget.StartedAt.Add(in.Policy.MaxRetryDuration)
	}
	if !in.Deadline.IsZero() && (d.IsZero() || in.Deadline.Before(d)) {
		d = in.Deadline
	}
	return d
}
