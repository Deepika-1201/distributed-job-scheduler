package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

func completion(j domain.Job, end domain.AttemptEnd) Completion {
	return Completion{JobID: j.ID, AttemptID: j.Current.ID, Number: j.Current.Number, End: end, Actor: domain.ActorDispatcher}
}

var (
	succeeded = domain.AttemptEnd{State: domain.AttemptSucceeded}
	retryable = domain.AttemptEnd{State: domain.AttemptFailed, Retryable: true}
)

func (f *fixture) complete(c Completion) CompletionResult {
	f.t.Helper()
	res, err := f.store.CompleteAttempt(ctx, c)
	if err != nil {
		f.t.Fatalf("CompleteAttempt: %v", err)
	}
	return res
}

// makeReady makes a job READY directly, without going through the promoter.
func (f *fixture) makeReady(id domain.JobID) {
	f.exec(`UPDATE jobs SET state = 'READY', run_at = now(), ready_at = now() WHERE id = $1`, string(id))
}

func TestSuccessMovesJobToHistory(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	running := f.claimOne()

	c := completion(running, succeeded)
	c.Result = []byte(`{"sent": true}`)
	res := f.complete(c)

	j := res.Job
	if j.State != domain.StateSucceeded || res.Decision.Next != domain.StateSucceeded || res.Replayed {
		t.Fatalf("result = %+v", res)
	}
	if string(j.Result) != `{"sent": true}` || j.FinishedAt.IsZero() || j.Current != nil {
		t.Errorf("history record: result %s, finished %v, current %+v", j.Result, j.FinishedAt, j.Current)
	}
	got, err := f.store.GetJob(ctx, f.tenant, running.ID)
	if err != nil || got.State != domain.StateSucceeded {
		t.Errorf("GetJob = %s, %v", got.State, err)
	}
	if f.count(`SELECT count(*) FROM jobs`) != 0 || f.count(`SELECT count(*) FROM job_history WHERE id = $1`, string(running.ID)) != 1 {
		t.Error("job was not moved from jobs to job_history")
	}
	var state, actor string
	var number int
	if err := f.pool.QueryRow(ctx, `SELECT state, number, actor FROM attempts WHERE job_id = $1`, string(running.ID)).
		Scan(&state, &number, &actor); err != nil || state != "SUCCEEDED" || number != 1 || actor != "dispatcher" {
		t.Errorf("attempt row = %s #%d by %s, %v", state, number, actor, err)
	}
}

func TestRetryableFailureSchedulesRetry(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	running := f.claimOne()

	c := completion(running, retryable)
	c.Error = "smtp 451"
	res := f.complete(c)

	j := res.Job
	if j.State != domain.StateRetryPending || res.Decision.Reason != domain.ReasonRetryableError {
		t.Fatalf("state %s, reason %s", j.State, res.Decision.Reason)
	}
	if j.Current != nil || j.LastError != "smtp 451" || j.Budget.Attempts != 1 || j.AttemptCount != 1 {
		t.Errorf("job = %+v", j)
	}
	// Full jitter with rnd = 0.5 halves the 10 s first backoff.
	if d := j.RunAt.Sub(j.UpdatedAt); d != 5*time.Second {
		t.Errorf("retry scheduled %v after the failure, want 5s", d)
	}
}

func TestExhaustedRetriesDeadLetter(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob(func(nj *NewJob) { nj.RetryPolicy.MaxAttempts, nj.RetryPolicy.MaxLostAttempts = 1, 1 }))
	res := f.complete(completion(f.claimOne(), retryable))
	if res.Job.State != domain.StateDeadLettered || res.Job.Reason != domain.ReasonAttemptsExhausted {
		t.Errorf("state %s, reason %s; want DEAD_LETTERED / ATTEMPTS_EXHAUSTED", res.Job.State, res.Job.Reason)
	}
}

func TestStaleAttemptIsFenced(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	first := f.claimOne()

	lost := completion(first, domain.AttemptEnd{State: domain.AttemptLost})
	lost.Actor = domain.ActorReaper
	if res := f.complete(lost); res.Job.State != domain.StateRetryPending || res.Job.Budget.Lost != 1 {
		t.Fatalf("after lost attempt: state %s, lost %d", res.Job.State, res.Job.Budget.Lost)
	}
	f.makeReady(first.ID)
	second := f.claimOne()
	if second.Current.Number != 2 || second.Budget.Attempts != 2 {
		t.Fatalf("second attempt number %d, budget %d", second.Current.Number, second.Budget.Attempts)
	}

	if _, err := f.store.CompleteAttempt(ctx, completion(first, succeeded)); !errors.Is(err, domain.ErrStaleAttempt) {
		t.Errorf("zombie completion: %v, want ErrStaleAttempt", err)
	}
	if res := f.complete(completion(second, succeeded)); res.Job.State != domain.StateSucceeded {
		t.Errorf("current attempt: state %s", res.Job.State)
	}
	if n := f.count(`SELECT count(*) FROM attempts WHERE job_id = $1`, string(first.ID)); n != 2 {
		t.Errorf("%d attempt rows, want 2 (lost + succeeded)", n)
	}
}

func TestDuplicateCompletionIsReplayed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	running := f.claimOne()
	c := completion(running, succeeded)
	f.complete(c)

	res, err := f.store.CompleteAttempt(ctx, c)
	if err != nil || !res.Replayed || res.Job.State != domain.StateSucceeded {
		t.Errorf("repeat = %+v, %v; want a replay", res, err)
	}
	if _, err := f.store.CompleteAttempt(ctx, completion(running, retryable)); !errors.Is(err, domain.ErrStaleAttempt) {
		t.Errorf("conflicting repeat: %v, want ErrStaleAttempt", err)
	}
	if n := f.count(`SELECT count(*) FROM attempts`); n != 1 {
		t.Errorf("%d attempt rows, want 1", n)
	}

	unknown := Completion{JobID: domain.JobID(uuid.NewString()), AttemptID: domain.AttemptID(uuid.NewString()),
		Number: 1, End: succeeded, Actor: domain.ActorDispatcher}
	if _, err := f.store.CompleteAttempt(ctx, unknown); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("unknown job: %v, want ErrNotFound", err)
	}
	reaperSuccess := c
	reaperSuccess.Actor = domain.ActorReaper
	if _, err := f.store.CompleteAttempt(ctx, reaperSuccess); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("reaper reporting success: %v, want ErrInvalidTransition", err)
	}
}
