package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/pgtest"
)

func TestIsUnavailable(t *testing.T) {
	for err, want := range map[error]bool{
		&pgconn.PgError{Code: "57P01"}:               true, // admin shutdown
		&pgconn.PgError{Code: "57P03"}:               true, // starting up
		&pgconn.PgError{Code: "08006"}:               true, // connection failure
		&pgconn.PgError{Code: "25006"}:               true, // read-only during failover
		&pgconn.PgError{Code: "53300"}:               true, // too many connections
		fmt.Errorf("query: %w", io.ErrUnexpectedEOF): true,
		&pgconn.PgError{Code: "23505"}:               false, // unique violation
		&pgconn.PgError{Code: "55P03"}:               false, // lock timeout
		&pgconn.PgError{Code: "40001"}:               false, // serialization failure
		context.DeadlineExceeded:                     false,
		fmt.Errorf("query: %w", context.Canceled):    false,
		domain.ErrNotFound:                           false,
	} {
		if got := IsUnavailable(err); got != want {
			t.Errorf("IsUnavailable(%v) = %v, want %v", err, got, want)
		}
	}
}

// The fault proxy's cut and stall look to the store like a stopped server and a blackholed
// network (LLD §21.1).
func TestProxyFaults(t *testing.T) {
	t.Parallel()
	proxy, url := pgtest.NewProxy(t, pgtest.NewDatabase(t))
	pool, err := NewPool(ctx, url, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ping := func(timeout time.Duration) error {
		c, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return pool.Ping(c)
	}
	recovered := func() {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ping(time.Second) != nil; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the database stayed unreachable after Restore")
			}
		}
	}
	recovered()

	proxy.Cut()
	if err := ping(5 * time.Second); !IsUnavailable(err) {
		t.Errorf("ping through a cut proxy = %v, want an unavailable error", err)
	}
	proxy.Restore()
	recovered()

	proxy.Stall()
	if err := ping(300 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ping through a stalled proxy = %v, want the deadline to expire", err)
	}
	proxy.Restore()
	recovered()
}

// A session whose lease passed expires only with evidence that its worker, not its engine,
// failed to renew; otherwise it waits for the outage tolerance (ADR-029).
func TestExpiryNeedsEvidence(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for _, node := range []string{"node-live", "node-quiet", "node-stopped"} {
		if _, ok, err := f.store.BeatNode(ctx, node, NodeBeat{}, time.Second); err != nil || !ok {
			t.Fatalf("BeatNode(%s) = %v, %v", node, ok, err)
		}
	}
	sessions := map[string]domain.SessionID{}
	for _, node := range []string{"", "node-live", "node-quiet", "node-stopped", "node-unknown"} {
		ws, err := f.store.CreateSession(ctx, domain.WorkerSession{Pool: "default", WorkerID: "w-" + node, Slots: 1, RenewedBy: node}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		sessions[node] = ws.ID
	}
	f.exec(`UPDATE worker_sessions SET lease_expires_at = now() - interval '10 seconds'`)
	f.exec(`UPDATE engine_nodes SET beat_at = now() - interval '1 minute' WHERE node_id = 'node-quiet'`)
	if _, ok, err := f.store.BeatNode(ctx, "node-live", NodeBeat{}, time.Second); err != nil || !ok {
		t.Fatalf("BeatNode(node-live) = %v, %v", ok, err)
	}
	if err := f.store.StopNode(ctx, "node-stopped"); err != nil {
		t.Fatal(err)
	}
	state := func(node string) string {
		var s string
		if err := f.pool.QueryRow(ctx, `SELECT state FROM worker_sessions WHERE id = $1`, string(sessions[node])).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	if n, err := f.store.ExpireSessions(ctx, 100, time.Hour); err != nil || n != 3 {
		t.Errorf("ExpireSessions = %d, %v; want 3", n, err)
	}
	for node, want := range map[string]string{"": "EXPIRED", "node-live": "EXPIRED", "node-stopped": "EXPIRED",
		"node-quiet": "ACTIVE", "node-unknown": "ACTIVE"} {
		if got := state(node); got != want {
			t.Errorf("session renewed by %q is %s, want %s", node, got, want)
		}
	}
	if n, err := f.store.ExpireSessions(ctx, 100, 5*time.Second); err != nil || n != 2 {
		t.Errorf("past the tolerance, ExpireSessions = %d, %v; want 2", n, err)
	}
}

// A liveness write delayed past its timeout must not land as fresh evidence (ADR-029).
func TestBeatNodeRejectsLateWrites(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	first, ok, err := f.store.BeatNode(ctx, "node", NodeBeat{}, time.Second)
	if err != nil || !ok {
		t.Fatalf("first beat = %v, %v", ok, err)
	}
	// As if this beat had been sent an hour before it ran.
	late := NodeBeat{At: first.At.Add(-time.Hour), Sent: first.Sent}
	if _, ok, err := f.store.BeatNode(ctx, "node", late, time.Second); err != nil || ok {
		t.Errorf("late beat applied = %v, %v; want it ignored", ok, err)
	}
	next, ok, err := f.store.BeatNode(ctx, "node", first, time.Second)
	if err != nil || !ok || next.At.Before(first.At) {
		t.Errorf("next beat = %+v, %v, %v", next, ok, err)
	}
	if err := f.store.StopNode(ctx, "node"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.BeatNode(ctx, "node", next, time.Second); err != nil || ok {
		t.Errorf("beat after a stop applied = %v, %v", ok, err)
	}
}
