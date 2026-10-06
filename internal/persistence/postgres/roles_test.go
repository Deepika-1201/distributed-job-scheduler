package postgres

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"jobscheduler/internal/domain"
)

// A login role holding only jobscheduler_runtime runs submission, dispatch, completion, audit
// writes and maintenance, but cannot change the schema or the audit log (ADR-027).
func TestRuntimeRoleRunsThePlatformWithoutOwnerRights(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	// Roles belong to the server, which other tests share: a unique name, dropped afterwards.
	role := "js_runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	f.exec(fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'runtime' IN ROLE jobscheduler_runtime`, role))
	t.Cleanup(func() { f.exec(`DROP ROLE ` + role) })

	u, err := url.Parse(f.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "runtime")
	rt, err := NewPool(ctx, u.String(), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	var current string
	if err := rt.QueryRow(ctx, `SELECT current_user`).Scan(&current); err != nil || current != role {
		t.Fatalf("connected as %q (%v), want %s", current, err, role)
	}

	r := *f
	r.store, r.pool, r.lease = NewStore(rt), rt, nil
	r.store.rnd = f.store.rnd
	r.submit(r.newJob())
	r.complete(completion(r.claimOne(), succeeded)) // writes job_history and attempts
	sum := sha256.Sum256([]byte("key"))
	if _, err := r.store.CreateAPIKey(ctx, domain.APIKey{ID: uuid.NewString(), TenantID: f.tenant, Name: "ci",
		Prefix: "rt" + uuid.NewString()[:6], SecretHash: sum[:], Role: domain.RoleViewer}, &testAudit); err != nil {
		t.Fatalf("CreateAPIKey (audited): %v", err)
	}

	now := time.Now().UTC()
	if err := r.store.EnsurePartitions(ctx, now.AddDate(0, 0, -40), 1); err != nil {
		t.Fatalf("create an expired partition: %v", err)
	}
	rep, err := r.store.RunMaintenance(ctx, Retention{History: 30 * 24 * time.Hour, Sessions: 24 * time.Hour})
	if err != nil || rep.PartitionsDropped != 2 || rep.PartitionsCreated == 0 {
		t.Errorf("maintenance as the runtime role: %+v, %v", rep, err)
	}

	for sql, code := range map[string]string{
		`CREATE TABLE intruder (id int)`:                            "42501",
		`DROP TABLE jobs`:                                           "42501",
		`TRUNCATE jobs`:                                             "42501",
		`ALTER TABLE jobs ADD COLUMN x int`:                         "42501",
		`UPDATE audit_log SET actor = 'someone else'`:               "42501",
		`DELETE FROM audit_log`:                                     "42501",
		`DELETE FROM goose_db_version`:                              "42501",
		`SELECT jobscheduler_create_partition('jobs', now()::date)`: "22023",
		`SELECT jobscheduler_drop_partition('jobs', now()::date)`:   "22023",
	} {
		_, err := rt.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Errorf("%s: %v, want SQLSTATE %s", sql, err, code)
		}
	}
	if n := f.count(`SELECT count(*) FROM audit_log`); n == 0 {
		t.Error("audit_log is empty after an audited action")
	}
}

// migrate's runtime login role: a member of jobscheduler_runtime that logs in with its password,
// which reaches the database only as a SCRAM verifier (LLD §22.5).
func TestEnsureLoginRole(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	role := "js_app_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() { f.exec(`DROP ROLE IF EXISTS ` + role) })
	connect := func(password string) error {
		u, err := url.Parse(f.pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(role, password)
		pool, err := NewPool(ctx, u.String(), 1)
		if err != nil {
			return err
		}
		defer pool.Close()
		var member bool
		if err := pool.QueryRow(ctx, `SELECT pg_has_role(current_user, 'jobscheduler_runtime', 'member')`).Scan(&member); err != nil {
			return err
		}
		if !member {
			return errors.New("not a member of jobscheduler_runtime")
		}
		return nil
	}

	const first, second = "first-password-0123456789", "second-password-0123456789"
	if err := EnsureLoginRole(ctx, f.pool, role, first); err != nil {
		t.Fatal(err)
	}
	if err := connect(first); err != nil {
		t.Fatalf("logging in as the new role: %v", err)
	}
	if err := EnsureLoginRole(ctx, f.pool, role, second); err != nil {
		t.Fatalf("running again: %v", err)
	}
	if err := connect(second); err != nil {
		t.Errorf("logging in with the new password: %v", err)
	}
	if err := connect(first); err == nil {
		t.Error("the old password still works")
	}
	var stored string
	if err := f.pool.QueryRow(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, role).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stored, "SCRAM-SHA-256$4096:") || strings.Contains(stored, second) {
		t.Errorf("stored password %q, want a SCRAM verifier", stored)
	}
}
