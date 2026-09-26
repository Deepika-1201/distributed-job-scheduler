// Package config loads process configuration from JS_* environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
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
	Roles           Roles
	OpsAddr         string
	Database        Database
	Log             Log
	ShutdownDelay   time.Duration
	ShutdownTimeout time.Duration
}

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
		Roles:   p.roles("JS_ROLES", "api,engine"),
		OpsAddr: p.str("JS_OPS_ADDR", ":9090"),
		Database: Database{
			URL:      p.required("JS_DATABASE_URL"),
			MaxConns: int32(p.intInRange("JS_DB_MAX_CONNS", 10, 1, 1000)),
		},
		Log: Log{
			Level:  p.logLevel("JS_LOG_LEVEL", slog.LevelInfo),
			Format: p.oneOf("JS_LOG_FORMAT", "json", "json", "text"),
		},
		ShutdownDelay:   p.duration("JS_SHUTDOWN_DELAY", 0),
		ShutdownTimeout: p.duration("JS_SHUTDOWN_TIMEOUT", 30*time.Second),
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
