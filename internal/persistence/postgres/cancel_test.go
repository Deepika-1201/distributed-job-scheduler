package postgres

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/pgtest"
)

func TestCancelByState(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	scheduled := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = time.Now().Add(time.Hour) }))
	got, err := f.store.RequestCancel(ctx, f.tenant, scheduled.ID, testAudit)
	if err != nil || got.State != domain.StateCancelled || got.Reason != domain.ReasonCancelled || got.FinishedAt.IsZero() {
		t.Fatalf("cancel scheduled = %s %s, %v", got.State, got.Reason, err)
	}
	if again, err := f.store.RequestCancel(ctx, f.tenant, scheduled.ID, testAudit); err != nil || again.State != domain.StateCancelled {
		t.Errorf("repeat cancel = %s, %v; want an idempotent no-op", again.State, err)
	}

	f.submit(f.newJob())
	running := f.claimOne()
	flagged, err := f.store.RequestCancel(ctx, f.tenant, running.ID, testAudit)
	if err != nil || flagged.State != domain.StateRunning || flagged.CancelRequestedAt.IsZero() {
		t.Fatalf("cancel running = %s (requested %v), %v", flagged.State, flagged.CancelRequestedAt, err)
	}
	if res := f.complete(completion(running, retryable)); res.Job.State != domain.StateCancelled {
		t.Errorf("attempt ended after cancel: job %s, want CANCELLED rather than a retry", res.Job.State)
	}

	f.submit(f.newJob())
	done := f.claimOne()
	f.complete(completion(done, succeeded))
	if _, err := f.store.RequestCancel(ctx, f.tenant, done.ID, testAudit); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("cancel succeeded job: %v, want ErrInvalidTransition", err)
	}

	if _, err := f.store.RequestCancel(ctx, f.tenant, domain.JobID(uuid.NewString()), testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("cancel unknown job: %v, want ErrNotFound", err)
	}
	other := f.addTenant("globex")
	if _, err := f.store.RequestCancel(ctx, other, scheduled.ID, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("cancel another tenant's job: %v, want ErrNotFound", err)
	}
}

// Every job must end either cancelled before any attempt, or running with the cancel flagged:
// never claimed after being cancelled, and never lost between the two.
func TestCancelRacesWithClaim(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const n = 100
	jobIDs := make([]domain.JobID, n)
	for i := range n {
		jobIDs[i] = f.submit(f.newJob()).ID
	}

	var wg sync.WaitGroup
	lease := f.poolLease()
	wg.Go(func() {
		session := domain.SessionID(uuid.NewString())
		for {
			got, err := f.store.ClaimReady(ctx, ClaimRequest{
				Lease: lease, Pool: "default", Priority: domain.PriorityNormal, Limit: 5, SessionID: session,
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(got) == 0 {
				return
			}
		}
	})
	wg.Go(func() {
		for _, id := range jobIDs {
			if _, err := f.store.RequestCancel(ctx, f.tenant, id, testAudit); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()

	for _, id := range jobIDs {
		job, err := f.store.GetJob(ctx, f.tenant, id)
		switch {
		case err != nil:
			t.Errorf("%s: %v", id, err)
		case job.State == domain.StateCancelled && job.AttemptCount == 0:
		case job.State == domain.StateRunning && !job.CancelRequestedAt.IsZero():
		default:
			t.Errorf("%s: state %s, attempts %d, cancel requested %v", id, job.State, job.AttemptCount, job.CancelRequestedAt)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	t.Parallel()
	pool, err := NewPool(ctx, pgtest.NewDatabase(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := Migrate(ctx, pool, slog.New(slog.DiscardHandler)); err != nil {
		t.Errorf("re-running migrations on a migrated database: %v", err)
	}
}

func TestEnsurePartitionsRoutesRowsByDay(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	today := time.Now().UTC()
	if err := f.store.EnsurePartitions(ctx, today, 3); err != nil {
		t.Fatalf("EnsurePartitions over existing days: %v", err)
	}
	for _, parent := range []string{"job_history", "attempts"} {
		if n := f.count(`SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass`, parent); n != 4 {
			t.Errorf("%s has %d partitions, want 3 daily + default", parent, n)
		}
	}

	f.submit(f.newJob())
	running := f.claimOne()
	f.complete(completion(running, succeeded))
	var partition string
	if err := f.pool.QueryRow(ctx, `SELECT tableoid::regclass::text FROM job_history WHERE id = $1`, string(running.ID)).
		Scan(&partition); err != nil || partition != "job_history_p"+today.Format("20060102") {
		t.Errorf("finished job stored in %q, %v; want today's partition", partition, err)
	}
}
