// Command jobscheduler runs the job platform: "serve" (default) runs the configured server
// roles, "migrate" applies database migrations and exits.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobscheduler/internal/app"
	"jobscheduler/internal/config"
	"jobscheduler/internal/health"
	"jobscheduler/internal/httpserver"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	var err error
	switch command {
	case "serve":
		err = serve()
	case "migrate":
		err = migrate()
	default:
		err = fmt.Errorf("unknown command %q (usage: jobscheduler [serve|migrate])", command)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "jobscheduler: %v\n", err)
		os.Exit(1)
	}
}

func setup() (config.Config, *slog.Logger, error) {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return config.Config{}, nil, err
	}
	log := observability.NewLogger(os.Stdout, cfg.Log.Level, cfg.Log.Format,
		slog.String("service", "jobscheduler"),
		slog.String("version", version),
		slog.Any("roles", cfg.Roles),
	)
	slog.SetDefault(log)
	return cfg, log, nil
}

func serve() error {
	cfg, log, err := setup()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.Database.URL, cfg.Database.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	checks := health.NewRegistry(log, 2*time.Second)
	checks.Register("postgres", pool.Ping)

	ops := httpserver.New("ops", cfg.OpsAddr, checks.Handler(), cfg.ShutdownTimeout, log)
	if _, err := ops.Listen(); err != nil {
		return err
	}

	a := &app.App{
		Log:           log,
		Components:    []app.Component{ops},
		ShutdownDelay: cfg.ShutdownDelay,
		OnShutdown:    checks.SetDraining,
	}
	log.Info("starting")
	return a.Run(ctx)
}

func migrate() error {
	cfg, log, err := setup()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.Database.URL, 2)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		return err
	}
	log.Info("migrations complete")
	return nil
}
