package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a connection pool without connecting, so the process can start while
// the database is unavailable; readiness checks report connectivity.
func NewPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns
	params := cfg.ConnConfig.RuntimeParams
	params["application_name"] = "jobscheduler"
	// Bound lock waits and runaway statements so no request can wedge a row or a connection (LLD §8.2).
	params["lock_timeout"] = "5s"
	params["statement_timeout"] = "30s"
	params["idle_in_transaction_session_timeout"] = "60s"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	return pool, nil
}
