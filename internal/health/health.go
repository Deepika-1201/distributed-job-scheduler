// Package health serves liveness and readiness endpoints.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Check reports whether a dependency is usable; it must return promptly once ctx is done.
type Check func(ctx context.Context) error

type Registry struct {
	log      *slog.Logger
	timeout  time.Duration
	mu       sync.RWMutex
	checks   map[string]Check
	draining atomic.Bool
}

// NewRegistry returns a registry whose readiness checks each get timeout to complete.
func NewRegistry(log *slog.Logger, timeout time.Duration) *Registry {
	return &Registry{log: log, timeout: timeout, checks: map[string]Check{}}
}

// Register adds a readiness check, replacing any existing check with the same name.
func (r *Registry) Register(name string, check Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = check
}

// SetDraining makes readiness fail permanently so load balancers stop routing traffic.
func (r *Registry) SetDraining() { r.draining.Store(true) }

// Handler serves GET /livez and GET /readyz.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, response{Status: "ok"})
	})
	mux.HandleFunc("GET /readyz", r.ready)
	return mux
}

type response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (r *Registry) ready(w http.ResponseWriter, req *http.Request) {
	if r.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, response{Status: "draining"})
		return
	}

	r.mu.RLock()
	checks := maps.Clone(r.checks)
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(req.Context(), r.timeout)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make(map[string]string, len(checks))
		healthy = true
	)
	for name, check := range checks {
		wg.Go(func() {
			err := check(ctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				healthy = false
				results[name] = "failing"
				r.log.Warn("readiness check failed", "check", name, "error", err)
				return
			}
			results[name] = "ok"
		})
	}
	wg.Wait()

	if !healthy {
		writeJSON(w, http.StatusServiceUnavailable, response{Status: "not_ready", Checks: results})
		return
	}
	writeJSON(w, http.StatusOK, response{Status: "ready", Checks: results})
}

func writeJSON(w http.ResponseWriter, status int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
