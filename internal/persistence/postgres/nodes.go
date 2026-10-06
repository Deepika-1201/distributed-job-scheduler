package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// NodeBeat is a liveness write the database accepted: when it ran, on the database clock, and
// when it was sent, on the engine's monotonic clock.
type NodeBeat struct {
	At   time.Time
	Sent time.Time
}

// BeatNode records that node has the database (ADR-029). After a previous beat, the write
// applies only if it runs within timeout of being sent, measured from that beat, so a write
// delayed by a healing network can't pass for fresh evidence. It reports false when it didn't apply.
func (s *Store) BeatNode(ctx context.Context, node string, prev NodeBeat, timeout time.Duration) (NodeBeat, bool, error) {
	sent := time.Now()
	var latest *time.Time
	if !prev.At.IsZero() {
		bound := prev.At.Add(sent.Sub(prev.Sent) + timeout)
		latest = &bound
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var at time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO engine_nodes (node_id, beat_at) VALUES ($1, statement_timestamp())
		ON CONFLICT (node_id) DO UPDATE SET beat_at = excluded.beat_at
		WHERE engine_nodes.stopped_at IS NULL AND ($2::timestamptz IS NULL OR statement_timestamp() <= $2)
		RETURNING beat_at`, node, latest).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return prev, false, nil
	}
	if err != nil {
		return prev, false, err
	}
	return NodeBeat{At: at, Sent: sent}, true, nil
}

// StopNode records a graceful stop, so the node's sessions expire on their lease (ADR-029).
func (s *Store) StopNode(ctx context.Context, node string) error {
	_, err := s.pool.Exec(ctx, `UPDATE engine_nodes SET stopped_at = now() WHERE node_id = $1`, node)
	return err
}
