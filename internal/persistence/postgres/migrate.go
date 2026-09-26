package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate applies pending migrations. A table-based lock (not a session advisory lock)
// serializes concurrent runs, so it also works through connection poolers.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	files, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresTableLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, files,
		goose.WithLocker(locker), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	results, err := provider.Up(ctx)
	for _, r := range results {
		log.Info("migration applied", "migration", r.Source.Version, "file", r.Source.Path, "duration", r.Duration.String())
	}
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
