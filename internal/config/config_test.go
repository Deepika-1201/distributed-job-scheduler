package config

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const testToken = "0123456789abcdef-worker"

// env serves m as the environment, with a valid JS_WORKER_TOKEN unless m sets one.
func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		if !ok && k == "JS_WORKER_TOKEN" {
			return testToken, true
		}
		return v, ok
	}
}

func TestLoadEngineSettings(t *testing.T) {
	cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs"}))
	if err != nil {
		t.Fatal(err)
	}
	if e := cfg.Engine; e.WorkerAddr != ":7070" || e.WorkerToken != testToken || e.NodeID == "" {
		t.Errorf("Engine = %+v", e)
	}
	if again, _ := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs"})); again.Engine.NodeID == cfg.Engine.NodeID {
		t.Error("node IDs repeat across starts; a restarted node would reuse its predecessor's leases")
	}
	_, err = Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_WORKER_TOKEN": "short",
		"JS_WORKER_ADDR": ":8080"}))
	for _, want := range []string{"JS_WORKER_TOKEN: must be at least 16", "JS_WORKER_ADDR: must differ"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
	if _, err := Load(func(k string) (string, bool) {
		return map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_ROLES": "api"}[k], k != "JS_WORKER_TOKEN"
	}); err != nil {
		t.Errorf("api-only process required a worker token: %v", err)
	}
	if cfg, err := Load(func(k string) (string, bool) {
		return map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_ROLES": "engine"}[k], k != "JS_WORKER_TOKEN"
	}); err != nil || cfg.Engine.WorkerToken != "" {
		t.Errorf("engine without a cluster token = %+v, %v; want it accepted: per-pool tokens suffice (ADR-025)", cfg.Engine, err)
	}
	if _, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_TLS_CERT_FILE": "/tls/cert.pem"})); err == nil ||
		!strings.Contains(err.Error(), "JS_TLS_CERT_FILE: must be set together with JS_TLS_KEY_FILE") {
		t.Errorf("certificate without a key: %v", err)
	}
	if cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs",
		"JS_TLS_CERT_FILE": "/tls/cert.pem", "JS_TLS_KEY_FILE": "/tls/key.pem"})); err != nil || !cfg.TLS.Enabled() {
		t.Errorf("TLS = %+v, %v", cfg.TLS, err)
	}
	if strings.Contains(fmtErr(Load(env(map[string]string{"JS_WORKER_TOKEN": "short-secret"}))), "short-secret") {
		t.Error("error echoes the worker token")
	}
}

func fmtErr(_ Config, err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://localhost/jobs"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Roles.Has(RoleAPI) || !cfg.Roles.Has(RoleEngine) || len(cfg.Roles) != 2 {
		t.Errorf("Roles = %v, want [api engine]", cfg.Roles)
	}
	if cfg.OpsAddr != ":9090" || cfg.HTTPAddr != ":8080" {
		t.Errorf("addrs = %q, %q", cfg.OpsAddr, cfg.HTTPAddr)
	}
	if cfg.API.TenantRateLimit != 500 || cfg.API.Replicas != 1 || cfg.API.NodeRateLimit() != 500 {
		t.Errorf("API = %+v", cfg.API)
	}
	if cfg.Database.MaxConns != 10 {
		t.Errorf("MaxConns = %d", cfg.Database.MaxConns)
	}
	if cfg.Log.Level != slog.LevelInfo || cfg.Log.Format != "json" {
		t.Errorf("Log = %+v", cfg.Log)
	}
	if cfg.ShutdownDelay != 0 || cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("shutdown = %v / %v", cfg.ShutdownDelay, cfg.ShutdownTimeout)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"JS_DATABASE_URL":     "postgres://db/jobs",
		"JS_ROLES":            " Engine , engine,api ",
		"JS_OPS_ADDR":         "127.0.0.1:9191",
		"JS_DB_MAX_CONNS":     "25",
		"JS_LOG_LEVEL":        "DEBUG",
		"JS_LOG_FORMAT":       "TEXT",
		"JS_SHUTDOWN_DELAY":   "5s",
		"JS_SHUTDOWN_TIMEOUT": "1m",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Roles) != 2 || cfg.Roles[0] != RoleEngine || cfg.Roles[1] != RoleAPI {
		t.Errorf("Roles = %v, want [engine api]", cfg.Roles)
	}
	if cfg.OpsAddr != "127.0.0.1:9191" || cfg.Database.MaxConns != 25 {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.Log.Level != slog.LevelDebug || cfg.Log.Format != "text" {
		t.Errorf("Log = %+v", cfg.Log)
	}
	if cfg.ShutdownDelay != 5*time.Second || cfg.ShutdownTimeout != time.Minute {
		t.Errorf("shutdown = %v / %v", cfg.ShutdownDelay, cfg.ShutdownTimeout)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"JS_ROLES":          "api,scheduler",
		"JS_DB_MAX_CONNS":   "0",
		"JS_LOG_LEVEL":      "verbose",
		"JS_LOG_FORMAT":     "xml",
		"JS_SHUTDOWN_DELAY": "-1s",
	}))
	if err == nil {
		t.Fatal("Load succeeded, want error")
	}
	for _, want := range []string{
		"JS_DATABASE_URL: is required",
		`JS_ROLES: unknown role "scheduler"`,
		"JS_DB_MAX_CONNS: must be an integer between 1 and 1000",
		"JS_LOG_LEVEL",
		"JS_LOG_FORMAT",
		"JS_SHUTDOWN_DELAY",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

func TestLoadRejectsEmptyRoles(t *testing.T) {
	_, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_ROLES": " , "}))
	if err == nil || !strings.Contains(err.Error(), "at least one role is required") {
		t.Fatalf("err = %v, want missing-role error", err)
	}
}

func TestLoadAPISettings(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"JS_DATABASE_URL": "postgres://db/jobs", "JS_TENANT_RATE_LIMIT": "900", "JS_API_REPLICAS": "3",
	}))
	if err != nil || cfg.API.NodeRateLimit() != 300 {
		t.Fatalf("NodeRateLimit = %v, %v; want 300", cfg.API.NodeRateLimit(), err)
	}
	_, err = Load(env(map[string]string{
		"JS_DATABASE_URL": "postgres://db/jobs", "JS_HTTP_ADDR": ":9090", "JS_TENANT_RATE_LIMIT": "0",
	}))
	for _, want := range []string{"JS_HTTP_ADDR: must differ", "JS_TENANT_RATE_LIMIT"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestLoadTelemetrySettings(t *testing.T) {
	cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs"}))
	if err != nil || cfg.Telemetry != (Telemetry{SampleRatio: 1}) {
		t.Fatalf("default telemetry = %+v, %v; want export off and every root sampled", cfg.Telemetry, err)
	}
	cfg, err = Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs",
		"JS_OTLP_ENDPOINT": "collector:4317", "JS_OTLP_INSECURE": "true", "JS_TRACE_SAMPLE_RATIO": "0.1"}))
	if err != nil || cfg.Telemetry != (Telemetry{OTLPEndpoint: "collector:4317", OTLPInsecure: true, SampleRatio: 0.1}) {
		t.Fatalf("telemetry = %+v, %v", cfg.Telemetry, err)
	}
	_, err = Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs",
		"JS_OTLP_ENDPOINT": "http://collector:4317", "JS_OTLP_INSECURE": "yes", "JS_TRACE_SAMPLE_RATIO": "1.5"}))
	for _, want := range []string{"JS_OTLP_ENDPOINT: must be host:port", "JS_OTLP_INSECURE", "JS_TRACE_SAMPLE_RATIO"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestLoadBacklogTarget(t *testing.T) {
	for value, want := range map[string]time.Duration{"": 5 * time.Minute, "20m": 20 * time.Minute} {
		cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_BACKLOG_TARGET": value}))
		if err != nil || cfg.BacklogTarget != want {
			t.Errorf("JS_BACKLOG_TARGET=%q: %v, %v; want %v", value, cfg.BacklogTarget, err, want)
		}
	}
	_, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://db/jobs", "JS_BACKLOG_TARGET": "5s",
		"JS_SHED_LOW_AFTER": "5m", "JS_SHED_NORMAL_AFTER": "15m"}))
	for _, want := range []string{"JS_BACKLOG_TARGET: must be between 10s and 24h",
		"JS_SHED_LOW_AFTER: was removed: set JS_BACKLOG_TARGET", "JS_SHED_NORMAL_AFTER: was removed"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestLoadNeverEchoesDatabaseURL(t *testing.T) {
	_, err := Load(env(map[string]string{
		"JS_DATABASE_URL": "postgres://user:s3cret@db/jobs",
		"JS_LOG_LEVEL":    "loud",
	}))
	if err == nil {
		t.Fatal("Load succeeded, want error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks database credentials: %v", err)
	}
}

// PEM TLS variables and the runtime login role (LLD §22.3, §22.5).
func TestLoadDeploymentSettings(t *testing.T) {
	db := func(m map[string]string) map[string]string {
		m["JS_DATABASE_URL"] = "postgres://db/jobs"
		return m
	}
	cfg, err := Load(env(db(map[string]string{"JS_TLS_CERT": "-----BEGIN CERTIFICATE-----", "JS_TLS_KEY": "pem-key-secret",
		"JS_RUNTIME_DB_ROLE": "jobscheduler_app", "JS_RUNTIME_DB_PASSWORD": "0123456789abcdef-db"})))
	if err != nil || !cfg.TLS.Enabled() || cfg.TLS.KeyPEM != "pem-key-secret" || cfg.Database.RuntimeRole != "jobscheduler_app" {
		t.Errorf("Load = %+v, %v", cfg, err)
	}
	for vars, want := range map[string]string{
		`{"JS_TLS_CERT": "x"}`: "JS_TLS_CERT: must be set together with JS_TLS_KEY",
		`{"JS_TLS_CERT": "x", "JS_TLS_KEY": "pem-key-secret", "JS_TLS_CERT_FILE": "/c", "JS_TLS_KEY_FILE": "/k"}`: "cannot be combined",
		`{"JS_RUNTIME_DB_ROLE": "app"}`: "JS_RUNTIME_DB_ROLE: must be set together with JS_RUNTIME_DB_PASSWORD",
		`{"JS_RUNTIME_DB_ROLE": "App; DROP", "JS_RUNTIME_DB_PASSWORD": "0123456789abcdef-db"}`: "JS_RUNTIME_DB_ROLE: must match",
		`{"JS_RUNTIME_DB_ROLE": "app", "JS_RUNTIME_DB_PASSWORD": "short-db-secret"}`:           "JS_RUNTIME_DB_PASSWORD: must be at least 16",
	} {
		m := map[string]string{}
		if err := json.Unmarshal([]byte(vars), &m); err != nil {
			t.Fatal(err)
		}
		_, err := Load(env(db(m)))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error %v, want %q", vars, err, want)
		}
		if err != nil && (strings.Contains(err.Error(), "pem-key-secret") || strings.Contains(err.Error(), "db-secret")) {
			t.Errorf("%s: error echoes a secret: %v", vars, err)
		}
	}
}
