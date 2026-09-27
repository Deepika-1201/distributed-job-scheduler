package postgres

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// Reasons a READY job is held back from dispatch (ADR-021).
const (
	HeldPoolPaused    = "pool_paused"
	HeldJobTypePaused = "job_type_paused"
	HeldTenantCap     = "tenant_cap"
)

var HeldReasons = []string{HeldPoolPaused, HeldJobTypePaused, HeldTenantCap}

// BacklogFreshness is how long an owner's backlog sample stays valid.
const BacklogFreshness = time.Minute

// wantedPoolsSQL selects the pools that need an owner: those with live worker sessions or
// READY jobs, probed once per known pool.
const wantedPoolsSQL = `
	SELECT pool FROM worker_sessions WHERE state = 'ACTIVE' AND lease_expires_at > now()
	UNION
	SELECT k.pool FROM (SELECT pool FROM job_types UNION SELECT name FROM pools) AS k (pool)
	WHERE EXISTS (SELECT 1 FROM jobs WHERE state = 'READY' AND pool = k.pool)`

// WantedPools lists the pools that need an owner (ADR-021).
func (s *Store) WantedPools(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, wantedPoolsSQL+` ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// UnownedPools counts wanted pools without an unexpired lease.
func (s *Store) UnownedPools(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM (`+wantedPoolsSQL+`) AS w (pool)
		WHERE NOT EXISTS (SELECT 1 FROM leases l WHERE l.name = 'pool:' || w.pool AND l.expires_at > now())`).Scan(&n)
	return n, err
}

// PoolGauges is one pool's queue and capacity, sampled by its owner.
type PoolGauges struct {
	Ready         map[domain.Priority]int // dispatchable READY jobs
	Held          map[string]int          // READY jobs held back, by reason
	OldestDue     time.Time               // run_at of the oldest dispatchable job; zero when none
	OldestAge     time.Duration           // how long that job had been due when sampled
	Running       map[domain.TenantID]int
	SlotsBusy     int
	SlotsFree     int
	BacklogTarget *time.Duration // nil means the platform default
}

// SamplePoolGauges reads, from one snapshot, the pool's READY jobs split into dispatchable and
// held work, and its running jobs, worker slots and backlog target. caps are the running-job
// caps of the tenants that have one.
func (s *Store) SamplePoolGauges(ctx context.Context, pool string, caps map[domain.TenantID]int) (PoolGauges, error) {
	var g PoolGauges
	err := s.inSnapshot(ctx, func(tx pgx.Tx) error {
		var err error
		g, err = samplePoolGauges(ctx, tx, pool, caps)
		return err
	})
	return g, err
}

type readyGroup struct {
	priority domain.Priority
	n        int
	due      time.Time
	age      time.Duration
}

func samplePoolGauges(ctx context.Context, tx pgx.Tx, pool string, caps map[domain.TenantID]int) (PoolGauges, error) {
	g := PoolGauges{Ready: map[domain.Priority]int{}, Held: map[string]int{}, Running: map[domain.TenantID]int{}}
	var (
		priority int16
		tenant   string
		held     string
		n        int
		due      time.Time
		age      float64
	)
	unpaused := map[domain.TenantID][]readyGroup{}
	rows, err := tx.Query(ctx, `
		SELECT priority, tenant_id::text,
		    CASE WHEN EXISTS (SELECT 1 FROM pools WHERE name = $1 AND paused) THEN '`+HeldPoolPaused+`'
		         WHEN NOT `+notHeldSQL+` THEN '`+HeldJobTypePaused+`'
		         ELSE '' END,
		    count(*), min(run_at), extract(epoch FROM now() - min(run_at))::float8
		FROM jobs
		WHERE pool = $1 AND state = 'READY' AND (start_deadline IS NULL OR start_deadline > now() OR attempt_count > 0)
		GROUP BY 1, 2, 3`, pool)
	if err != nil {
		return PoolGauges{}, err
	}
	if _, err := pgx.ForEachRow(rows, []any{&priority, &tenant, &held, &n, &due, &age}, func() error {
		if held != "" {
			g.Held[held] += n
		} else {
			t := domain.TenantID(tenant)
			unpaused[t] = append(unpaused[t], readyGroup{domain.Priority(priority), n, due, time.Duration(age * float64(time.Second))})
		}
		return nil
	}); err != nil {
		return PoolGauges{}, err
	}

	rows, err = tx.Query(ctx, `
		SELECT tenant_id::text, count(*) FROM jobs WHERE pool = $1 AND state = 'RUNNING' GROUP BY tenant_id`, pool)
	if err != nil {
		return PoolGauges{}, err
	}
	running := 0
	if _, err := pgx.ForEachRow(rows, []any{&tenant, &n}, func() error {
		g.Running[domain.TenantID(tenant)] = n
		running += n
		return nil
	}); err != nil {
		return PoolGauges{}, err
	}
	for t, groups := range unpaused {
		g.classify(groups, caps, t)
	}

	var (
		slots  int
		target pgtype.Int8
	)
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT COALESCE(sum(slots), 0) FROM worker_sessions WHERE pool = $1 AND state = 'ACTIVE'),
		    (SELECT backlog_target_ms FROM pools WHERE name = $1)`, pool).Scan(&slots, &target); err != nil {
		return PoolGauges{}, err
	}
	g.SlotsBusy = min(running, slots)
	g.SlotsFree = slots - g.SlotsBusy
	g.BacklogTarget = msDuration(target)
	return g, nil
}

// classify splits a tenant's unpaused READY jobs into dispatchable and held work. A tenant
// with more waiting than its running cap lets it start now is held back by the cap: only that
// allowance is dispatchable, counted against its most urgent work, and its jobs don't set the
// backlog age, because their wait comes from the cap rather than the pool.
func (g *PoolGauges) classify(groups []readyGroup, caps map[domain.TenantID]int, t domain.TenantID) {
	total := 0
	for _, gr := range groups {
		total += gr.n
	}
	limit, capped := caps[t]
	if allowance := max(0, limit-g.Running[t]); capped && allowance < total {
		g.Held[HeldTenantCap] += total - allowance
		slices.SortFunc(groups, func(a, b readyGroup) int { return cmp.Compare(b.priority, a.priority) })
		for _, gr := range groups {
			take := min(gr.n, allowance)
			g.Ready[gr.priority] += take
			allowance -= take
		}
		return
	}
	for _, gr := range groups {
		g.Ready[gr.priority] += gr.n
		if g.OldestDue.IsZero() || gr.due.Before(g.OldestDue) {
			g.OldestDue, g.OldestAge = gr.due, gr.age
		}
	}
}

// RecordPoolBacklog stores the owner's latest sample on the pool row, for api nodes to shed
// on. Only the current lease holder's write lands.
func (s *Store) RecordPoolBacklog(ctx context.Context, l Lease, pool string, oldestDue time.Time) error {
	if l.Name != PoolLeaseName(pool) {
		return fmt.Errorf("recording pool %q requires lease %q, not %q", pool, PoolLeaseName(pool), l.Name)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO pools (name, oldest_due_at, sampled_at)
		SELECT $1, $2, now() WHERE EXISTS (`+fenceSQL("$3", "$4", "$5")+`)
		ON CONFLICT (name) DO UPDATE SET oldest_due_at = excluded.oldest_due_at, sampled_at = excluded.sampled_at`,
		pool, nullTime(oldestDue), l.Name, l.Holder, l.Epoch)
	return err
}

// PoolBacklog is a pool's dispatchable backlog as last sampled by its owner.
type PoolBacklog struct {
	Age    time.Duration
	Target *time.Duration // nil means the platform default
}

// PoolBacklogs returns the backlog of every pool sampled within BacklogFreshness.
func (s *Store) PoolBacklogs(ctx context.Context) (map[string]PoolBacklog, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, extract(epoch FROM now() - COALESCE(oldest_due_at, now()))::float8, backlog_target_ms
		FROM pools WHERE sampled_at > now() - make_interval(secs => $1)`, BacklogFreshness.Seconds())
	if err != nil {
		return nil, err
	}
	backlogs := map[string]PoolBacklog{}
	var (
		name    string
		seconds float64
		target  pgtype.Int8
	)
	_, err = pgx.ForEachRow(rows, []any{&name, &seconds, &target}, func() error {
		backlogs[name] = PoolBacklog{Age: time.Duration(seconds * float64(time.Second)), Target: msDuration(target)}
		return nil
	})
	return backlogs, err
}

// SetPoolSettings replaces a pool's settings; a nil target means the platform default. The
// audit row goes to the platform-admin's tenant.
func (s *Store) SetPoolSettings(ctx context.Context, pool string, backlogTarget *time.Duration, auditTenant domain.TenantID, audit Audit) error {
	tenant, ok := canonicalUUID(string(auditTenant))
	if !ok {
		return domain.ErrNotFound
	}
	var (
		ms     pgtype.Int8
		detail any
	)
	if backlogTarget != nil {
		ms, detail = pgtype.Int8{Int64: backlogTarget.Milliseconds(), Valid: true}, backlogTarget.String()
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO pools (name, backlog_target_ms) VALUES ($1, $2)
			ON CONFLICT (name) DO UPDATE SET backlog_target_ms = excluded.backlog_target_ms, updated_at = now()`, pool, ms); err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "pool.settings", pool, map[string]any{"backlog_target": detail})
	})
}

func msDuration(v pgtype.Int8) *time.Duration {
	if !v.Valid {
		return nil
	}
	d := time.Duration(v.Int64) * time.Millisecond
	return &d
}
