package domain

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func fixedRand(v float64) func() float64 { return func() float64 { return v } }

func noJitter() RetryPolicy {
	p := DefaultRetryPolicy()
	p.Jitter = JitterNone
	return p
}

func TestDefaultPolicyIsValid(t *testing.T) {
	if err := DefaultRetryPolicy().Validate(DefaultRetryLimits()); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyValidationReportsEveryViolation(t *testing.T) {
	p := RetryPolicy{
		Strategy:         BackoffExponential,
		InitialDelay:     100 * time.Millisecond,
		Multiplier:       0.5,
		MaxDelay:         48 * time.Hour,
		Jitter:           "gaussian",
		MaxAttempts:      0,
		MaxRetryDuration: 0,
		MaxLostAttempts:  5,
	}
	err := p.Validate(DefaultRetryLimits())
	if err == nil {
		t.Fatal("invalid policy accepted")
	}
	for _, want := range []string{"multiplier", "jitter", "initial delay", "max delay", "max attempts", "max retry duration", "max lost attempts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
	if err := (RetryPolicy{Strategy: "linear"}).Validate(DefaultRetryLimits()); err == nil || !strings.Contains(err.Error(), "unknown backoff strategy") {
		t.Errorf("unknown strategy: %v", err)
	}
}

func TestFixedPolicyIgnoresMultiplier(t *testing.T) {
	p := noJitter()
	p.Strategy, p.Multiplier = BackoffFixed, 0
	if err := p.Validate(DefaultRetryLimits()); err != nil {
		t.Fatalf("fixed policy rejected: %v", err)
	}
	for n := 1; n <= 5; n++ {
		if d := p.Delay(n, nil); d != 10*time.Second {
			t.Errorf("Delay(%d) = %v, want 10s", n, d)
		}
	}
}

func TestExponentialDelaySequence(t *testing.T) {
	p := noJitter()
	want := []time.Duration{10, 20, 40, 80, 160, 320, 640, 1280, 2560, 3600, 3600}
	for i, w := range want {
		if d := p.Delay(i+1, nil); d != w*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", i+1, d, w*time.Second)
		}
	}
	if d := p.Delay(1000, nil); d != time.Hour {
		t.Errorf("Delay(1000) = %v, want the 1h cap without overflow", d)
	}
}

func TestFullJitterScalesDelay(t *testing.T) {
	p := DefaultRetryPolicy()
	if d := p.Delay(3, fixedRand(0)); d != 0 {
		t.Errorf("rnd=0: %v, want 0", d)
	}
	if d := p.Delay(3, fixedRand(0.5)); d != 20*time.Second {
		t.Errorf("rnd=0.5: %v, want 20s", d)
	}
	if d := p.Delay(3, fixedRand(0.999999)); d >= 40*time.Second {
		t.Errorf("rnd→1: %v, want < 40s", d)
	}
}

func TestDecideAfterAttempt(t *testing.T) {
	base := func() DecisionInput {
		return DecisionInput{
			Policy: noJitter(),
			Budget: RetryBudget{Attempts: 1, StartedAt: t0},
			Now:    t0,
		}
	}
	tests := []struct {
		name  string
		setup func(*DecisionInput)
		want  Decision
	}{
		{"success", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptSucceeded}
		}, Decision{Next: StateSucceeded}},
		{"success wins over a late cancel request", func(in *DecisionInput) {
			in.End, in.CancelRequested = AttemptEnd{State: AttemptSucceeded}, true
		}, Decision{Next: StateSucceeded}},
		{"cancel requested beats retry", func(in *DecisionInput) {
			in.End, in.CancelRequested = AttemptEnd{State: AttemptFailed, Retryable: true}, true
		}, Decision{Next: StateCancelled, Reason: ReasonCancelled}},
		{"attempt cancelled", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptCancelled}
		}, Decision{Next: StateCancelled, Reason: ReasonCancelled}},
		{"non-retryable failure", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptFailed}
		}, Decision{Next: StateFailed, Reason: ReasonNonRetryable}},
		{"at-most-once never retries a known failure", func(in *DecisionInput) {
			in.End, in.AtMostOnce = AttemptEnd{State: AttemptFailed, Retryable: true}, true
		}, Decision{Next: StateFailed, Reason: ReasonAtMostOnce}},
		{"at-most-once lost attempt has unknown outcome", func(in *DecisionInput) {
			in.End, in.AtMostOnce = AttemptEnd{State: AttemptLost}, true
		}, Decision{Next: StateFailed, Reason: ReasonOutcomeUnknown}},
		{"at-most-once timeout has unknown outcome", func(in *DecisionInput) {
			in.End, in.AtMostOnce = AttemptEnd{State: AttemptTimedOut}, true
		}, Decision{Next: StateFailed, Reason: ReasonOutcomeUnknown}},
		{"third lost attempt is poison", func(in *DecisionInput) {
			in.End, in.Budget.Attempts, in.Budget.Lost = AttemptEnd{State: AttemptLost}, 3, 3
		}, Decision{Next: StateDeadLettered, Reason: ReasonPoison}},
		{"lost below poison threshold retries", func(in *DecisionInput) {
			in.End, in.Budget.Attempts, in.Budget.Lost = AttemptEnd{State: AttemptLost}, 2, 2
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(20 * time.Second), Reason: ReasonLost}},
		{"attempts exhausted", func(in *DecisionInput) {
			in.End, in.Budget.Attempts = AttemptEnd{State: AttemptFailed, Retryable: true}, 10
		}, Decision{Next: StateDeadLettered, Reason: ReasonAttemptsExhausted}},
		{"last allowed retry", func(in *DecisionInput) {
			in.End, in.Budget.Attempts = AttemptEnd{State: AttemptFailed, Retryable: true}, 9
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(2560 * time.Second), Reason: ReasonRetryableError}},
		{"timeout retries", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptTimedOut}
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(10 * time.Second), Reason: ReasonTimedOut}},
		{"retry-after hint is honoured", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptFailed, Retryable: true, RetryAfter: 90 * time.Second}
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(90 * time.Second), Reason: ReasonRetryableError}},
		{"retry-after hint is capped", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptFailed, Retryable: true, RetryAfter: 3 * time.Hour}
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(time.Hour), Reason: ReasonRetryableError}},
		{"retry window exceeded", func(in *DecisionInput) {
			in.End = AttemptEnd{State: AttemptFailed, Retryable: true}
			in.Budget.StartedAt = t0.Add(-24*time.Hour + 5*time.Second)
		}, Decision{Next: StateDeadLettered, Reason: ReasonDeadlineExceeded}},
		{"explicit deadline earlier than retry window", func(in *DecisionInput) {
			in.End, in.Deadline = AttemptEnd{State: AttemptFailed, Retryable: true}, t0.Add(5*time.Second)
		}, Decision{Next: StateDeadLettered, Reason: ReasonDeadlineExceeded}},
		{"retry landing exactly on the deadline is allowed", func(in *DecisionInput) {
			in.End, in.Deadline = AttemptEnd{State: AttemptFailed, Retryable: true}, t0.Add(10*time.Second)
		}, Decision{Next: StateRetryPending, RunAt: t0.Add(10 * time.Second), Reason: ReasonRetryableError}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := base()
			tt.setup(&in)
			got, err := DecideAfterAttempt(in, fixedRand(0.5))
			if err != nil {
				t.Fatalf("DecideAfterAttempt: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %+v\nwant %+v", got, tt.want)
			}
			if err := ValidateJobTransition(StateRunning, got.Next, ActorDispatcher); err != nil {
				t.Errorf("decision is not a legal transition: %v", err)
			}
		})
	}
}

func TestDecideAppliesJitter(t *testing.T) {
	in := DecisionInput{
		End:    AttemptEnd{State: AttemptFailed, Retryable: true},
		Policy: DefaultRetryPolicy(),
		Budget: RetryBudget{Attempts: 2, StartedAt: t0},
		Now:    t0,
	}
	got, err := DecideAfterAttempt(in, fixedRand(0.25))
	if err != nil {
		t.Fatal(err)
	}
	if want := t0.Add(5 * time.Second); !got.RunAt.Equal(want) {
		t.Errorf("RunAt = %v, want %v (25%% of 20s)", got.RunAt, want)
	}
}

func TestDecideRejectsRunningAttempt(t *testing.T) {
	in := DecisionInput{End: AttemptEnd{State: AttemptRunning}, Policy: DefaultRetryPolicy(), Now: t0}
	if _, err := DecideAfterAttempt(in, fixedRand(0)); err == nil {
		t.Error("decision made for a running attempt")
	}
}
