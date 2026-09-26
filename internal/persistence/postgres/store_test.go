package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m, func(ctx context.Context, url string) error {
		pool, err := NewPool(ctx, url, 2)
		if err != nil {
			return err
		}
		defer pool.Close()
		return Migrate(ctx, pool, slog.New(slog.DiscardHandler))
	}))
}

var ctx = context.Background()

var testAudit = Audit{Actor: "test", RequestID: "req-test"}

type fixture struct {
	t      *testing.T
	store  *Store
	pool   *pgxpool.Pool
	tenant domain.TenantID
}

// newFixture returns a store on a fresh database with one tenant and job type "email.send".
func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool, err := NewPool(ctx, pgtest.NewDatabase(t), 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := NewStore(pool)
	store.rnd = func() float64 { return 0.5 }
	if err := store.EnsurePartitions(ctx, time.Now(), 2); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, store: store, pool: pool}
	f.tenant = f.addTenant("acme")
	return f
}

func (f *fixture) addTenant(name string) domain.TenantID {
	f.t.Helper()
	id := uuid.Must(uuid.NewV7()).String()
	f.exec(`INSERT INTO tenants (id, name) VALUES ($1, $2)`, id, name)
	f.exec(`INSERT INTO job_types (tenant_id, name, pool, attempt_timeout_ms, retry_policy)
		VALUES ($1, 'email.send', 'default', 60000, '{}')`, id)
	return domain.TenantID(id)
}

func (f *fixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(ctx, sql, args...); err != nil {
		f.t.Fatalf("exec %q: %v", sql, err)
	}
}

func (f *fixture) count(sql string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		f.t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (f *fixture) newJob(mods ...func(*NewJob)) NewJob {
	nj := NewJob{
		TenantID:       f.tenant,
		Type:           "email.send",
		TypeVersion:    1,
		Pool:           "default",
		Priority:       domain.PriorityNormal,
		Payload:        []byte(`{"to": "a@example.com"}`),
		CreatedBy:      "test",
		AttemptTimeout: time.Minute,
		RetryPolicy:    domain.DefaultRetryPolicy(),
	}
	for _, mod := range mods {
		mod(&nj)
	}
	return nj
}

func (f *fixture) submit(nj NewJob) domain.Job {
	f.t.Helper()
	res, err := f.store.SubmitJob(ctx, nj, nil)
	if err != nil {
		f.t.Fatalf("SubmitJob: %v", err)
	}
	return res.Job
}

func hash(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

func closeTo(a, b time.Time) bool {
	d := a.Sub(b)
	return d > -time.Millisecond && d < time.Millisecond
}

func TestSubmitImmediateAndDelayedJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()

	ready := f.submit(f.newJob(func(nj *NewJob) {
		nj.Labels = map[string]string{"customer": "42"}
		nj.Payload = []byte(`{"b": 2,  "a": 1}`)
		nj.CorrelationID = "corr-1"
	}))
	if ready.State != domain.StateReady || ready.ReadyAt.IsZero() || ready.RunAt.After(time.Now()) {
		t.Errorf("immediate job: state %s, ready_at %v, run_at %v", ready.State, ready.ReadyAt, ready.RunAt)
	}
	if string(ready.Payload) != `{"b": 2,  "a": 1}` {
		t.Errorf("payload = %s, want the submitted text unchanged", ready.Payload)
	}
	if ready.Labels["customer"] != "42" || ready.CorrelationID != "corr-1" || ready.CreatedBy != "test" {
		t.Errorf("job = %+v", ready)
	}
	if ready.RetryPolicy != domain.DefaultRetryPolicy() || ready.AttemptTimeout != time.Minute {
		t.Errorf("policy = %+v, timeout = %v", ready.RetryPolicy, ready.AttemptTimeout)
	}
	if ready.Current != nil || ready.AttemptCount != 0 {
		t.Errorf("new job has attempt state: %+v", ready.Current)
	}

	runAt := now.Add(time.Hour)
	delayed := f.submit(f.newJob(func(nj *NewJob) { nj.RunAt = runAt }))
	if delayed.State != domain.StateScheduled || !delayed.ReadyAt.IsZero() || !closeTo(delayed.RunAt, runAt) {
		t.Errorf("delayed job: state %s, ready_at %v, run_at %v", delayed.State, delayed.ReadyAt, delayed.RunAt)
	}
}

func TestGetJobIsTenantScoped(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	job := f.submit(f.newJob())

	got, err := f.store.GetJob(ctx, f.tenant, job.ID)
	if err != nil || got.ID != job.ID || got.State != domain.StateReady {
		t.Fatalf("GetJob = %+v, %v", got, err)
	}
	other := f.addTenant("globex")
	for _, tc := range []struct {
		tenant domain.TenantID
		id     domain.JobID
	}{
		{other, job.ID},
		{f.tenant, "not-a-uuid"},
		{f.tenant, domain.JobID(uuid.NewString())},
	} {
		if _, err := f.store.GetJob(ctx, tc.tenant, tc.id); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("GetJob(%s, %s) = %v, want ErrNotFound", tc.tenant, tc.id, err)
		}
	}
}

func TestSubmitIdempotency(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	key := &Idempotency{Key: "k1", RequestHash: hash("a")}

	first, err := f.store.SubmitJob(ctx, f.newJob(), key)
	if err != nil || first.Outcome != Created {
		t.Fatalf("first = %v, %v", first.Outcome, err)
	}
	again, err := f.store.SubmitJob(ctx, f.newJob(), key)
	if err != nil || again.Outcome != Replayed || again.Job.ID != first.Job.ID {
		t.Fatalf("retry = %v %s, %v; want replay of %s", again.Outcome, again.Job.ID, err, first.Job.ID)
	}
	if _, err := f.store.SubmitJob(ctx, f.newJob(), &Idempotency{Key: "k1", RequestHash: hash("b")}); !errors.Is(err, domain.ErrIdempotencyKeyReused) {
		t.Errorf("different request, same key: %v, want ErrIdempotencyKeyReused", err)
	}

	f.exec(`UPDATE idempotency_keys SET expires_at = now() - interval '1 second'`)
	fresh, err := f.store.SubmitJob(ctx, f.newJob(), &Idempotency{Key: "k1", RequestHash: hash("b")})
	if err != nil || fresh.Outcome != Created || fresh.Job.ID == first.Job.ID {
		t.Errorf("expired key: %v %s, %v; want a new job", fresh.Outcome, fresh.Job.ID, err)
	}

	other := f.addTenant("globex")
	res, err := f.store.SubmitJob(ctx, f.newJob(func(nj *NewJob) { nj.TenantID = other }), key)
	if err != nil || res.Outcome != Created {
		t.Errorf("same key, other tenant: %v, %v; want created", res.Outcome, err)
	}
}

func TestConcurrentSubmissionsWithSameKeyCreateOneJob(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const n = 20
	var (
		wg      sync.WaitGroup
		results [n]SubmitResult
		errs    [n]error
	)
	for i := range n {
		wg.Go(func() {
			results[i], errs[i] = f.store.SubmitJob(ctx, f.newJob(), &Idempotency{Key: "same", RequestHash: hash("x")})
		})
	}
	wg.Wait()

	created, ids := 0, map[domain.JobID]bool{}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("submission %d: %v", i, errs[i])
		}
		if results[i].Outcome == Created {
			created++
		}
		ids[results[i].Job.ID] = true
	}
	if created != 1 || len(ids) != 1 {
		t.Errorf("created %d jobs with %d distinct ids, want exactly 1", created, len(ids))
	}
	if n := f.count(`SELECT count(*) FROM jobs`); n != 1 {
		t.Errorf("jobs table has %d rows, want 1", n)
	}
}

func TestDedupeKeyIsUniqueAmongActiveJobs(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	withKey := func(nj *NewJob) { nj.DedupeKey = "invoice-7" }

	first := f.submit(f.newJob(withKey))
	res, err := f.store.SubmitJob(ctx, f.newJob(withKey), nil)
	if err != nil || res.Outcome != Deduplicated || res.Job.ID != first.ID {
		t.Fatalf("duplicate = %v %s, %v; want existing %s", res.Outcome, res.Job.ID, err, first.ID)
	}

	if _, err := f.store.RequestCancel(ctx, f.tenant, first.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	second, err := f.store.SubmitJob(ctx, f.newJob(withKey), nil)
	if err != nil || second.Outcome != Created || second.Job.ID == first.ID {
		t.Fatalf("after the first job finished: %v %s, %v; want a new job", second.Outcome, second.Job.ID, err)
	}

	key := &Idempotency{Key: "k2", RequestHash: hash("dup")}
	deduped, err := f.store.SubmitJob(ctx, f.newJob(withKey), key)
	if err != nil || deduped.Outcome != Deduplicated || deduped.Job.ID != second.Job.ID {
		t.Fatalf("dedupe with idempotency key = %v %s, %v", deduped.Outcome, deduped.Job.ID, err)
	}
	replay, err := f.store.SubmitJob(ctx, f.newJob(withKey), key)
	if err != nil || replay.Outcome != Replayed || replay.Job.ID != second.Job.ID {
		t.Errorf("replay = %v %s, %v; want the deduplicated job %s", replay.Outcome, replay.Job.ID, err, second.Job.ID)
	}
}

func TestDatabaseEnforcesRunningInvariant(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	job := f.submit(f.newJob())
	_, err := f.pool.Exec(ctx, `UPDATE jobs SET state = 'RUNNING' WHERE id = $1`, string(job.ID))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Errorf("RUNNING without an attempt: err = %v, want a check violation", err)
	}
}
