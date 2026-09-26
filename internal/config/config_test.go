package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"JS_DATABASE_URL": "postgres://localhost/jobs"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Roles.Has(RoleAPI) || !cfg.Roles.Has(RoleEngine) || len(cfg.Roles) != 2 {
		t.Errorf("Roles = %v, want [api engine]", cfg.Roles)
	}
	if cfg.OpsAddr != ":9090" {
		t.Errorf("OpsAddr = %q", cfg.OpsAddr)
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
