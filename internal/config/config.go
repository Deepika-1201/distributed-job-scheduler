// Package config loads process configuration from JS_* environment variables.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Role is a process role; one binary can run several.
type Role string

const (
	RoleAPI    Role = "api"
	RoleEngine Role = "engine"
)

var knownRoles = []Role{RoleAPI, RoleEngine}

// Roles is the set of roles a process runs, in configured order without duplicates.
type Roles []Role

// Has reports whether r includes role.
func (r Roles) Has(role Role) bool { return slices.Contains(r, role) }

type Config struct {
	Roles     Roles
	HTTPAddr  string
	OpsAddr   string
	API       API
	Engine    Engine
	Database  Database
	Log       Log
	Telemetry Telemetry
	TLS       TLS
	// BacklogTarget is the backlog target of pools without their own (ADR-021).
	BacklogTarget   time.Duration
	ShutdownDelay   time.Duration
	ShutdownTimeout time.Duration
}

// Telemetry configures trace export (ADR-020); metrics are always served on the ops port.
type Telemetry struct {
	OTLPEndpoint string // host:port of an OTLP/gRPC collector; empty disables trace export
	OTLPInsecure bool   // plaintext to the collector, for local stacks
	SampleRatio  float64
}

// TLS turns on TLS for the API and worker servers when both files are set (ADR-026).
type TLS struct {
	CertFile string
	KeyFile  string
}

func (t TLS) Enabled() bool { return t.CertFile != "" }

// Engine configures the worker protocol served by engine nodes (LLD §12).
type Engine struct {
	WorkerAddr string // listen address
	// AdvertiseAddr is how workers reach this node when redirected to it.
	AdvertiseAddr string
	WorkerToken   string
	// NodeID identifies this process as a lease holder; unique per start.
	NodeID string
	// HistoryRetention is how long finished jobs and attempts are kept (NFR-9).
	HistoryRetention time.Duration
}

type API struct {
	// TenantRateLimit is each tenant's request budget per second across all api replicas.
	TenantRateLimit float64
	Replicas        int
	// MinScheduleInterval is the shortest interval a schedule may fire at (LLD §10.2).
	MinScheduleInterval time.Duration
}

// NodeRateLimit is the share of the tenant rate limit enforced by one api replica.
func (a API) NodeRateLimit() float64 { return a.TenantRateLimit / float64(a.Replicas) }

type Database struct {
	URL      string
	MaxConns int32
}

type Log struct {
	Level  slog.Level
	Format string
}

// Load reads configuration through lookup (os.LookupEnv in production) and reports
// every invalid or missing variable at once.
func Load(lookup func(string) (string, bool)) (Config, error) {
	p := &parser{lookup: lookup}
	cfg := Config{
		Roles:    p.roles("JS_ROLES", "api,engine"),
		HTTPAddr: p.str("JS_HTTP_ADDR", ":8080"),
		OpsAddr:  p.str("JS_OPS_ADDR", ":9090"),
		API: API{
			TenantRateLimit:     p.floatInRange("JS_TENANT_RATE_LIMIT", 500, 1, 1e6),
			Replicas:            p.intInRange("JS_API_REPLICAS", 1, 1, 1000),
			MinScheduleInterval: p.duration("JS_MIN_SCHEDULE_INTERVAL", time.Minute),
		},
		Database: Database{
			URL:      p.required("JS_DATABASE_URL"),
			MaxConns: int32(p.intInRange("JS_DB_MAX_CONNS", 10, 1, 1000)),
		},
		Log: Log{
			Level:  p.logLevel("JS_LOG_LEVEL", slog.LevelInfo),
			Format: p.oneOf("JS_LOG_FORMAT", "json", "json", "text"),
		},
		Telemetry: Telemetry{
			OTLPEndpoint: p.str("JS_OTLP_ENDPOINT", ""),
			OTLPInsecure: p.oneOf("JS_OTLP_INSECURE", "false", "true", "false") == "true",
			SampleRatio:  p.floatInRange("JS_TRACE_SAMPLE_RATIO", 1, 0, 1),
		},
		BacklogTarget:   p.duration("JS_BACKLOG_TARGET", 5*time.Minute),
		ShutdownDelay:   p.duration("JS_SHUTDOWN_DELAY", 0),
		ShutdownTimeout: p.duration("JS_SHUTDOWN_TIMEOUT", 30*time.Second),
		TLS:             TLS{CertFile: p.str("JS_TLS_CERT_FILE", ""), KeyFile: p.str("JS_TLS_KEY_FILE", "")},
	}
	if (cfg.TLS.CertFile == "") != (cfg.TLS.KeyFile == "") {
		p.fail("JS_TLS_CERT_FILE", "must be set together with JS_TLS_KEY_FILE")
	}
	if cfg.Roles.Has(RoleEngine) {
		cfg.Engine = Engine{
			WorkerAddr:       p.str("JS_WORKER_ADDR", ":7070"),
			AdvertiseAddr:    p.str("JS_WORKER_ADVERTISE_ADDR", ""),
			WorkerToken:      p.str("JS_WORKER_TOKEN", ""),
			NodeID:           p.str("JS_NODE_ID", defaultNodeID()),
			HistoryRetention: p.duration("JS_HISTORY_RETENTION", 30*24*time.Hour),
		}
		if cfg.Engine.HistoryRetention < 24*time.Hour {
			p.fail("JS_HISTORY_RETENTION", "must be at least 24h")
		}
		if t := cfg.Engine.WorkerToken; t != "" && len(t) < 16 {
			p.fail("JS_WORKER_TOKEN", "must be at least 16 characters")
		}
		if slices.Contains([]string{cfg.HTTPAddr, cfg.OpsAddr}, cfg.Engine.WorkerAddr) {
			p.fail("JS_WORKER_ADDR", "must differ from JS_HTTP_ADDR and JS_OPS_ADDR")
		}
	}
	if cfg.HTTPAddr == cfg.OpsAddr {
		p.fail("JS_HTTP_ADDR", "must differ from JS_OPS_ADDR")
	}
	if e := cfg.Telemetry.OTLPEndpoint; e != "" {
		if host, port, err := net.SplitHostPort(e); err != nil || host == "" || port == "" {
			p.fail("JS_OTLP_ENDPOINT", "must be host:port, got %q", e)
		}
	}
	if cfg.API.MinScheduleInterval < time.Second {
		p.fail("JS_MIN_SCHEDULE_INTERVAL", "must be at least 1s")
	}
	if cfg.BacklogTarget < 10*time.Second || cfg.BacklogTarget > 24*time.Hour {
		p.fail("JS_BACKLOG_TARGET", "must be between 10s and 24h")
	}
	for _, removed := range []string{"JS_SHED_LOW_AFTER", "JS_SHED_NORMAL_AFTER"} {
		if _, set := p.get(removed); set {
			p.fail(removed, "was removed: set JS_BACKLOG_TARGET; LOW is shed past it and NORMAL past three times it (ADR-021)")
		}
	}
	if err := errors.Join(p.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return cfg, nil
}

type parser struct {
	lookup func(string) (string, bool)
	errs   []error
}

// defaultNodeID is the hostname plus a random suffix, so a restarted process never reuses
// its predecessor's lease identity.
func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "node"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}

func (p *parser) get(key string) (string, bool) {
	v, ok := p.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (p *parser) fail(key, format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
}

func (p *parser) str(key, def string) string {
	if v, ok := p.get(key); ok {
		return v
	}
	return def
}

func (p *parser) required(key string) string {
	v, ok := p.get(key)
	if !ok {
		p.fail(key, "is required")
	}
	return v
}

func (p *parser) intInRange(key string, def, lo, hi int) int {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		p.fail(key, "must be an integer between %d and %d, got %q", lo, hi, v)
		return def
	}
	return n
}

func (p *parser) floatInRange(key string, def, lo, hi float64) float64 {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < lo || f > hi {
		p.fail(key, "must be a number between %g and %g, got %q", lo, hi, v)
		return def
	}
	return f
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		p.fail(key, "must be a non-negative duration such as 5s, got %q", v)
		return def
	}
	return d
}

func (p *parser) oneOf(key, def string, allowed ...string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	v = strings.ToLower(v)
	if !slices.Contains(allowed, v) {
		p.fail(key, "must be one of %s, got %q", strings.Join(allowed, ", "), v)
		return def
	}
	return v
}

func (p *parser) logLevel(key string, def slog.Level) slog.Level {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		p.fail(key, "must be debug, info, warn or error, got %q", v)
		return def
	}
	return l
}

func (p *parser) roles(key, def string) Roles {
	v, ok := p.get(key)
	if !ok {
		v = def
	}
	var roles Roles
	for part := range strings.SplitSeq(v, ",") {
		r := Role(strings.ToLower(strings.TrimSpace(part)))
		switch {
		case r == "":
			continue
		case !slices.Contains(knownRoles, r):
			p.fail(key, "unknown role %q (known: api, engine)", r)
		case !roles.Has(r):
			roles = append(roles, r)
		}
	}
	if len(roles) == 0 {
		p.fail(key, "at least one role is required")
	}
	return roles
}
