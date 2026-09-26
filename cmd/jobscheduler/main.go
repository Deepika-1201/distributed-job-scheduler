// Command jobscheduler runs the job platform: "serve" (default) runs the configured server
// roles, "migrate" applies database migrations, and "bootstrap <tenant>" creates a tenant
// with an admin API key.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata" // schedules resolve IANA zones even in images without zoneinfo

	"jobscheduler/internal/api"
	"jobscheduler/internal/app"
	"jobscheduler/internal/config"
	"jobscheduler/internal/coordination"
	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/health"
	"jobscheduler/internal/httpserver"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/recovery"
	"jobscheduler/internal/scheduling"
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
	case "bootstrap":
		if len(os.Args) != 3 {
			err = fmt.Errorf("usage: jobscheduler bootstrap <tenant-name>")
			break
		}
		err = bootstrap(os.Args[2])
	default:
		err = fmt.Errorf("unknown command %q (usage: jobscheduler [serve|migrate|bootstrap])", command)
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
	components := []app.Component{ops}
	store := postgres.NewStore(pool)
	if cfg.Roles.Has(config.RoleAPI) {
		apiServer := api.New(store, log, api.Config{
			TenantRateLimit:     cfg.API.NodeRateLimit(),
			MinScheduleInterval: cfg.API.MinScheduleInterval,
		})
		components = append(components, httpserver.New("api", cfg.HTTPAddr, apiServer.Handler(), cfg.ShutdownTimeout, log))
	}
	if cfg.Roles.Has(config.RoleEngine) {
		lis, err := net.Listen("tcp", cfg.Engine.WorkerAddr)
		if err != nil {
			return fmt.Errorf("listen for workers on %s: %w", cfg.Engine.WorkerAddr, err)
		}
		advertise := cfg.Engine.AdvertiseAddr
		if advertise == "" {
			advertise = lis.Addr().String()
		}
		dispatcher, err := dispatch.New(store, dispatch.Config{NodeID: cfg.Engine.NodeID, AdvertiseAddr: advertise,
			Token: cfg.Engine.WorkerToken, Listener: lis}, log)
		if err != nil {
			return err
		}
		log.Info("serving workers", "addr", lis.Addr().String(), "advertise", advertise, "node_id", cfg.Engine.NodeID)
		retention := postgres.Retention{History: cfg.Engine.HistoryRetention, Sessions: 24 * time.Hour}
		singletons := coordination.NewManager(store, coordination.Config{
			Name: "singleton-leases", Holder: cfg.Engine.NodeID, TTL: coordination.SingletonTTL, Margin: coordination.Margin,
			Duties: map[string]func(context.Context){
				"singleton:maintenance": recovery.Maintenance(store, retention, time.Hour, log),
			},
		}, log)
		components = append(components,
			scheduling.NewMaterializer(store, domain.DefaultPlanLimits, log),
			scheduling.NewPromoter(store, log),
			dispatcher,
			recovery.NewReaper(store, recovery.ReaperConfig{}, log),
			recovery.NewOperations(store, log),
			singletons)
	}
	for _, c := range components {
		if srv, ok := c.(*httpserver.Server); ok {
			if _, err := srv.Listen(); err != nil {
				return err
			}
		}
	}

	a := &app.App{
		Log:           log,
		Components:    components,
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

// bootstrap creates a tenant and prints its first admin API key, which is shown only once.
func bootstrap(tenantName string) error {
	cfg, _, err := setup()
	if err != nil {
		return err
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.Database.URL, 2)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.NewStore(pool)

	tenant, err := store.CreateTenant(ctx, tenantName)
	if err != nil {
		return fmt.Errorf("create tenant %q: %w", tenantName, err)
	}
	plaintext, key := api.NewAPIKey(tenant, "bootstrap admin", domain.RoleAdmin, time.Time{})
	if _, err := store.CreateAPIKey(ctx, key, nil); err != nil {
		return fmt.Errorf("create admin key: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"tenant_id": string(tenant), "api_key": plaintext})
}
