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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // schedules resolve IANA zones even in images without zoneinfo

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

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
		role := domain.RoleAdmin
		if len(os.Args) == 4 {
			role = domain.Role(os.Args[3])
		}
		if (len(os.Args) != 3 && len(os.Args) != 4) || (role != domain.RoleAdmin && role != domain.RolePlatformAdmin) {
			err = fmt.Errorf("usage: jobscheduler bootstrap <tenant-name> [admin|platform-admin]")
			break
		}
		err = bootstrap(os.Args[2], role)
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

	tel, err := setupTelemetry(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(flushCtx); err != nil {
			log.Warn("flushing telemetry failed", "error", err)
		}
	}()

	pool, err := postgres.NewPool(ctx, cfg.Database.URL, cfg.Database.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	poolGauges, err := observePool(pool, cfg.Roles)
	if err != nil {
		return err
	}
	defer func() { _ = poolGauges.Unregister() }()

	checks := health.NewRegistry(log, 2*time.Second)
	checks.Register("postgres", pool.Ping)

	opsMux := http.NewServeMux()
	opsMux.Handle("GET /metrics", tel.Metrics)
	opsMux.Handle("/", checks.Handler())
	ops := httpserver.New("ops", cfg.OpsAddr, opsMux, cfg.ShutdownTimeout, log)
	components := []app.Component{ops}
	store := postgres.NewStore(pool)
	if cfg.Roles.Has(config.RoleAPI) {
		apiServer := api.New(store, log, api.Config{
			TenantRateLimit:     cfg.API.NodeRateLimit(),
			Replicas:            cfg.API.Replicas,
			MinScheduleInterval: cfg.API.MinScheduleInterval,
			ShedLowAfter:        cfg.API.ShedLowAfter,
			ShedNormalAfter:     cfg.API.ShedNormalAfter,
			Addr:                cfg.HTTPAddr,
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

// setupTelemetry installs metrics and, when configured, trace export (ADR-020).
func setupTelemetry(ctx context.Context, cfg config.Config, log *slog.Logger) (*observability.Telemetry, error) {
	instance := cfg.Engine.NodeID
	if instance == "" {
		instance, _ = os.Hostname()
	}
	return observability.SetupTelemetry(ctx, observability.TelemetryConfig{
		ServiceVersion: version, InstanceID: instance,
		OTLPEndpoint: cfg.Telemetry.OTLPEndpoint, OTLPInsecure: cfg.Telemetry.OTLPInsecure,
		SampleRatio: cfg.Telemetry.SampleRatio, Logger: log,
	})
}

// observePool reports the connection pool's use, labeled with the process roles: an api
// node and an engine node size their pools differently.
func observePool(pool *pgxpool.Pool, roles config.Roles) (metric.Registration, error) {
	names := make([]string, len(roles))
	for i, r := range roles {
		names[i] = string(r)
	}
	role := metric.WithAttributes(attribute.String("role", strings.Join(names, ",")))
	return observability.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := pool.Stat()
		o.ObserveInt64(observability.DBPoolInUse, int64(st.AcquiredConns()), role)
		o.ObserveInt64(observability.DBPoolMax, int64(st.MaxConns()), role)
		return nil
	}, observability.DBPoolInUse, observability.DBPoolMax)
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
func bootstrap(tenantName string, role domain.Role) error {
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
	plaintext, key := api.NewAPIKey(tenant, "bootstrap "+string(role), role, time.Time{})
	if _, err := store.CreateAPIKey(ctx, key, nil); err != nil {
		return fmt.Errorf("create %s key: %w", role, err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"tenant_id": string(tenant), "api_key": plaintext})
}
