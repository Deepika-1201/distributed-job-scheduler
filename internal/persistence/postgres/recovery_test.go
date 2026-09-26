package postgres

import (
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

func TestExpiredSessionsLoseTheirAttempts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	retried := f.submit(f.newJob())
	poison := f.submit(f.newJob(func(nj *NewJob) { nj.RetryPolicy.MaxLostAttempts = 1 }))
	alive := f.submit(f.newJob())
	dead, live := f.session("default"), f.session("default")
	f.claimFor(dead, 2, nil)
	f.claimFor(live, 1, nil)
	f.exec(`UPDATE worker_sessions SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, string(dead.ID))

	if n, err := f.store.ExpireSessions(ctx, 100); err != nil || n != 1 {
		t.Fatalf("ExpireSessions = %d, %v; want 1", n, err)
	}
	if n, err := f.store.LoseOrphanedAttempts(ctx, 100); err != nil || n != 2 {
		t.Fatalf("LoseOrphanedAttempts = %d, %v; want 2", n, err)
	}
	if n, _ := f.store.LoseOrphanedAttempts(ctx, 100); n != 0 {
		t.Errorf("second sweep ended %d attempts, want 0", n)
	}
	for id, want := range map[domain.JobID]domain.JobState{
		retried.ID: domain.StateRetryPending, poison.ID: domain.StateDeadLettered, alive.ID: domain.StateRunning,
	} {
		if got, _ := f.store.GetJob(ctx, f.tenant, id); got.State != want {
			t.Errorf("job %s is %s, want %s", id, got.State, want)
		}
	}
	if got, _ := f.store.GetJob(ctx, f.tenant, poison.ID); got.Reason != domain.ReasonPoison {
		t.Errorf("poison job reason = %s", got.Reason)
	}
	attempts, _ := f.store.ListAttempts(ctx, f.tenant, retried.ID)
	if len(attempts) != 1 || attempts[0].State != domain.AttemptLost || attempts[0].Actor != domain.ActorReaper {
		t.Errorf("attempts = %+v", attempts)
	}
}

func TestOverdueAttemptsTimeOutAfterGrace(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.submit(f.newJob())
	job := f.claimFor(f.session("default"), 1, nil)[0]
	f.exec(`UPDATE jobs SET attempt_deadline = now() - interval '10 seconds' WHERE id = $1`, string(job.ID))
	if n, err := f.store.TimeOutOverdueAttempts(ctx, 30*time.Second, 100); err != nil || n != 0 {
		t.Fatalf("within grace: timed out %d (%v), want 0", n, err)
	}
	f.exec(`UPDATE jobs SET attempt_deadline = now() - interval '31 seconds' WHERE id = $1`, string(job.ID))
	if n, err := f.store.TimeOutOverdueAttempts(ctx, 30*time.Second, 100); err != nil || n != 1 {
		t.Fatalf("after grace: timed out %d (%v), want 1", n, err)
	}
	got, _ := f.store.GetJob(ctx, f.tenant, job.ID)
	attempts, _ := f.store.ListAttempts(ctx, f.tenant, job.ID)
	if got.State != domain.StateRetryPending || len(attempts) != 1 || attempts[0].State != domain.AttemptTimedOut {
		t.Errorf("job %s, attempts %+v", got.State, attempts)
	}
}

func TestMaintenanceCreatesPartitionsAndEnforcesRetention(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now().UTC()
	exists := func(name string) bool {
		var ok bool
		if err := f.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	day := func(offset int) string { return now.AddDate(0, 0, offset).Format("20060102") }
	if err := f.store.EnsurePartitions(ctx, now.AddDate(0, 0, -40), 1); err != nil {
		t.Fatal(err)
	}

	// Rows past retention: a history row in the default partition, an expired idempotency
	// key, an old ledger row and a long-closed session.
	old := f.submit(f.newJob())
	if _, err := f.store.RequestCancel(ctx, f.tenant, old.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE job_history SET finished_at = now() - interval '50 days' WHERE id = $1`, string(old.ID))
	if _, err := f.store.SubmitJob(ctx, f.newJob(), &Idempotency{Key: "k", RequestHash: hash("x")}); err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE idempotency_keys SET expires_at = now() - interval '1 second'`)
	f.exec(`INSERT INTO schedule_fires (schedule_id, fire_time, job_id)
		VALUES (gen_random_uuid(), now() - interval '40 days', gen_random_uuid())`)
	ws := f.session("default")
	f.exec(`UPDATE worker_sessions SET state = 'CLOSED', closed_at = now() - interval '2 days' WHERE id = $1`, string(ws.ID))

	rep, err := f.store.RunMaintenance(ctx, Retention{History: 30 * 24 * time.Hour, Sessions: 24 * time.Hour})
	if err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	for _, name := range []string{"job_history_p" + day(7), "attempts_p" + day(7), "job_history_p" + day(-1)} {
		if !exists(name) {
			t.Errorf("partition %s was not created", name)
		}
	}
	if exists("job_history_p"+day(-40)) || exists("attempts_p"+day(-40)) || rep.PartitionsDropped != 2 {
		t.Errorf("expired partitions not dropped (dropped %d)", rep.PartitionsDropped)
	}
	for table, want := range map[string]int64{"job_history_default": 1, "idempotency_keys": 1, "schedule_fires": 1, "worker_sessions": 1} {
		if rep.RowsDeleted[table] != want {
			t.Errorf("deleted %d rows from %s, want %d", rep.RowsDeleted[table], table, want)
		}
	}
	if n := f.count(`SELECT count(*) FROM jobs`); n != 1 {
		t.Errorf("maintenance touched active jobs: %d left, want 1", n)
	}
	if again, err := f.store.RunMaintenance(ctx, Retention{History: 30 * 24 * time.Hour, Sessions: 24 * time.Hour}); err != nil || again.PartitionsCreated != 0 {
		t.Errorf("second run created %d partitions (%v), want 0", again.PartitionsCreated, err)
	}
}
