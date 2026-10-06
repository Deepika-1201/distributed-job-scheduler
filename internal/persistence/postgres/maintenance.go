package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Retention configures what maintenance deletes (LLD §13.2).
type Retention struct {
	History  time.Duration // job_history, attempts and schedule_fires
	Sessions time.Duration // closed and expired worker sessions
}

// MaintenanceReport counts what one maintenance run did.
type MaintenanceReport struct {
	PartitionsCreated int
	PartitionsDropped int
	RowsDeleted       map[string]int64
}

const (
	purgeBatch      = 10_000
	maxPurgeBatches = 100
	partitionsAhead = 7
)

var partitionName = regexp.MustCompile(`^(job_history|attempts)_p(\d{8})$`)

// RunMaintenance creates upcoming partitions and deletes data past retention. It keeps going
// after a failed task and returns the errors joined.
func (s *Store) RunMaintenance(ctx context.Context, r Retention) (MaintenanceReport, error) {
	rep := MaintenanceReport{RowsDeleted: map[string]int64{}}
	var errs []error
	now, err := s.dbNow(ctx)
	if err != nil {
		return rep, err
	}
	created, err := s.ensurePartitions(ctx, now.Add(-24*time.Hour), partitionsAhead+2)
	rep.PartitionsCreated = created
	errs = append(errs, err)

	cutoff := now.Add(-r.History)
	dropped, err := s.dropPartitionsBefore(ctx, cutoff)
	rep.PartitionsDropped = dropped
	errs = append(errs, err)

	for _, p := range []struct {
		table, where string
		args         []any
	}{
		{"job_history_default", "finished_at < $1", []any{cutoff}},
		{"attempts_default", "finished_at < $1", []any{cutoff}},
		{"idempotency_keys", "expires_at < now()", nil},
		{"schedule_fires", "fire_time < $1", []any{cutoff}},
		{"worker_sessions", "state <> 'ACTIVE' AND closed_at < $1", []any{now.Add(-r.Sessions)}},
		{"engine_nodes", "beat_at < $1", []any{now.Add(-r.Sessions)}},
	} {
		n, err := s.purge(ctx, p.table, p.where, p.args...)
		rep.RowsDeleted[p.table] = n
		errs = append(errs, err)
	}
	return rep, errors.Join(errs...)
}

func (s *Store) dbNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&now)
	return now, err
}

// purge deletes matching rows in batches so no statement holds many row locks for long.
func (s *Store) purge(ctx context.Context, table, where string, args ...any) (int64, error) {
	var total int64
	sql := fmt.Sprintf(`DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE %[2]s LIMIT %[3]d)`,
		pgx.Identifier{table}.Sanitize(), where, purgeBatch)
	for range maxPurgeBatches {
		tag, err := s.pool.Exec(ctx, sql, args...)
		if err != nil {
			return total, fmt.Errorf("purge %s: %w", table, err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < purgeBatch {
			break
		}
	}
	return total, nil
}

// dropPartitionsBefore drops daily history partitions whose whole day ended before cutoff.
func (s *Store) dropPartitionsBefore(ctx context.Context, cutoff time.Time) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname IN ('job_history', 'attempts') ORDER BY c.relname`)
	if err != nil {
		return 0, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	dropped := 0
	var errs []error
	for _, name := range names {
		m := partitionName.FindStringSubmatch(name)
		if m == nil {
			continue // the default partitions
		}
		day, err := time.Parse("20060102", m[2])
		if err != nil || day.AddDate(0, 0, 1).After(cutoff) {
			continue
		}
		// jobscheduler_drop_partition waits at most 1 s for the parent's lock, so history inserts
		// don't queue behind it (ADR-027).
		var done bool
		err = s.pool.QueryRow(ctx, `SELECT jobscheduler_drop_partition($1, $2)`, m[1], day).Scan(&done)
		var pgErr *pgconn.PgError
		switch {
		case err == nil:
			if done {
				dropped++
			}
		case errors.As(err, &pgErr) && pgErr.Code == "55P03":
			errs = append(errs, fmt.Errorf("drop %s: lock not available; retried next run", name))
		default:
			errs = append(errs, fmt.Errorf("drop %s: %w", name, err))
		}
	}
	return dropped, errors.Join(errs...)
}
