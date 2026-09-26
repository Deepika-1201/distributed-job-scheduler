package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, h http.Handler, path string) (int, response) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body response
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: invalid JSON %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func newRegistry() *Registry {
	return NewRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)), 50*time.Millisecond)
}

func TestLivenessIgnoresChecks(t *testing.T) {
	r := newRegistry()
	r.Register("db", func(context.Context) error { return errors.New("down") })
	if code, body := get(t, r.Handler(), "/livez"); code != http.StatusOK || body.Status != "ok" {
		t.Errorf("livez = %d %+v", code, body)
	}
}

func TestReadiness(t *testing.T) {
	ok := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("connection refused") }
	hang := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

	tests := []struct {
		name       string
		checks     map[string]Check
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{"no checks", nil, http.StatusOK, "ready", nil},
		{"all passing", map[string]Check{"db": ok, "cache": ok}, http.StatusOK, "ready",
			map[string]string{"db": "ok", "cache": "ok"}},
		{"one failing", map[string]Check{"db": down, "cache": ok}, http.StatusServiceUnavailable, "not_ready",
			map[string]string{"db": "failing", "cache": "ok"}},
		{"timeout counts as failing", map[string]Check{"db": hang}, http.StatusServiceUnavailable, "not_ready",
			map[string]string{"db": "failing"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRegistry()
			for name, c := range tt.checks {
				r.Register(name, c)
			}
			code, body := get(t, r.Handler(), "/readyz")
			if code != tt.wantCode || body.Status != tt.wantStatus {
				t.Errorf("readyz = %d %q, want %d %q", code, body.Status, tt.wantCode, tt.wantStatus)
			}
			if len(body.Checks) != len(tt.wantChecks) {
				t.Errorf("checks = %v, want %v", body.Checks, tt.wantChecks)
			}
			for k, v := range tt.wantChecks {
				if body.Checks[k] != v {
					t.Errorf("check %s = %q, want %q", k, body.Checks[k], v)
				}
			}
		})
	}
}

func TestReadinessDoesNotLeakErrorDetails(t *testing.T) {
	r := newRegistry()
	r.Register("db", func(context.Context) error { return errors.New("password authentication failed for user jobs") })
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if got := rec.Body.String(); got != `{"status":"not_ready","checks":{"db":"failing"}}`+"\n" {
		t.Errorf("body = %q", got)
	}
}

func TestDrainingFailsReadinessButNotLiveness(t *testing.T) {
	r := newRegistry()
	r.Register("db", func(context.Context) error { return nil })
	r.SetDraining()

	if code, body := get(t, r.Handler(), "/readyz"); code != http.StatusServiceUnavailable || body.Status != "draining" {
		t.Errorf("readyz = %d %+v", code, body)
	}
	if code, _ := get(t, r.Handler(), "/livez"); code != http.StatusOK {
		t.Errorf("livez = %d, want 200 while draining", code)
	}
}
