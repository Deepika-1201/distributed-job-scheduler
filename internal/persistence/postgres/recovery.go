package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// ExpireSessions marks up to limit active sessions whose lease has passed as EXPIRED (LLD §13.1).
func (s *Store) ExpireSessions(ctx context.Context, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE worker_sessions SET state = 'EXPIRED', closed_at = now()
		WHERE id IN (SELECT id FROM worker_sessions WHERE state = 'ACTIVE' AND lease_expires_at < now()
		    ORDER BY lease_expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit)
	return int(tag.RowsAffected()), err
}

// LoseOrphanedAttempts ends up to limit running attempts whose session is no longer active
// as LOST, applying the retry rules.
func (s *Store) LoseOrphanedAttempts(ctx context.Context, limit int) (int, error) {
	return s.endAttempts(ctx, domain.AttemptEnd{State: domain.AttemptLost}, domain.ActorReaper, "worker session expired", `
		SELECT j.id, j.current_attempt_id, j.attempt_count FROM jobs j
		JOIN worker_sessions ws ON ws.id = j.current_session_id
		WHERE j.state = 'RUNNING' AND ws.state <> 'ACTIVE'
		LIMIT $1`, limit)
}

// TimeOutOverdueAttempts ends up to limit attempts still running grace after their deadline
// as TIMED_OUT. The worker enforces deadlines first; this is the backstop.
func (s *Store) TimeOutOverdueAttempts(ctx context.Context, grace time.Duration, limit int) (int, error) {
	return s.endAttempts(ctx, domain.AttemptEnd{State: domain.AttemptTimedOut}, domain.ActorReaper, "attempt deadline passed without a report", `
		SELECT id, current_attempt_id, attempt_count FROM jobs
		WHERE state = 'RUNNING' AND attempt_deadline < now() - make_interval(secs => $2)
		ORDER BY attempt_deadline LIMIT $1`, limit, grace.Seconds())
}

// endAttempts completes the attempts the query selects on the worker's behalf. Completion is
// fenced by attempt ID and number, so a concurrent report or reaper wins at most once.
func (s *Store) endAttempts(ctx context.Context, end domain.AttemptEnd, actor domain.Actor, reason, query string, args ...any) (int, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	type held struct {
		job, attempt pgtype.UUID
		number       int
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (held, error) {
		var h held
		return h, r.Scan(&h.job, &h.attempt, &h.number)
	})
	if err != nil {
		return 0, err
	}
	ended := 0
	for _, h := range attempts {
		res, err := s.CompleteAttempt(ctx, Completion{JobID: domain.JobID(uuidString(h.job)),
			AttemptID: domain.AttemptID(uuidString(h.attempt)), Number: h.number, End: end, Error: reason, Actor: actor})
		switch {
		case err == nil && !res.Replayed:
			ended++
		case err == nil, errors.Is(err, domain.ErrStaleAttempt):
		default:
			return ended, err
		}
	}
	return ended, nil
}
