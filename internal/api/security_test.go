package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestSecurityHeaders(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
		"Referrer-Policy":         "no-referrer",
	}
	for name, r := range map[string]response{
		"success":       e.call("GET", "/v1/jobs", e.admin, "").want(http.StatusOK),
		"error":         e.call("GET", "/v1/jobs", "", "").want(http.StatusUnauthorized),
		"unknown route": e.call("GET", "/v1/nope", e.admin, "").want(http.StatusNotFound),
	} {
		for h, v := range want {
			if got := r.header.Get(h); got != v {
				t.Errorf("%s: %s = %q, want %q", name, h, got, v)
			}
		}
		if got := r.header.Get("Strict-Transport-Security"); got != "" {
			t.Errorf("%s: HSTS %q sent without TLS", name, got)
		}
	}

	rec := httptest.NewRecorder()
	New(e.store, slog.New(slog.DiscardHandler), Config{TLS: true}).Handler().
		ServeHTTP(rec, httptest.NewRequest("GET", "/v1/nope", nil))
	if got := rec.Header().Get("Strict-Transport-Security"); got != "max-age=31536000" {
		t.Errorf("with TLS, HSTS = %q", got)
	}
}

// Tenant A probes tenant B's resources through every route; a route without a case fails the
// test, so new endpoints can't skip it (LLD §20.6, ADR-028).
func TestTenantIsolationCoversEveryRoute(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	a := e.admin
	bTenant, b := e.newTenant("globex")
	e.call("POST", "/v1/job-types", b, `{"name": "b.only"}`).want(http.StatusCreated)
	bJob := e.call("POST", "/v1/jobs", b, `{"type": "b.only"}`).want(http.StatusCreated).str("id")
	bSchedule := e.call("POST", "/v1/schedules", b,
		`{"name": "b.nightly", "job_type": "b.only", "trigger": {"kind": "cron", "cron": "30 2 * * *"}}`).want(http.StatusCreated).str("id")
	bOperation := e.call("POST", "/v1/operations", b, `{"kind": "cancel", "filter": {"label": "no:match"}}`).want(http.StatusAccepted).str("id")
	bKey := e.call("POST", "/v1/api-keys", b, `{"name": "b.ci", "role": "viewer"}`).want(http.StatusCreated).str("id")
	const worker = "0191f000-0000-7000-8000-000000000001"

	notFound := func(method, path, body string) func() {
		return func() { e.call(method, path, a, body).wantError(http.StatusNotFound, "not_found") }
	}
	excludes := func(key, path string, foreign ...string) func() {
		return func() {
			raw, _ := json.Marshal(e.call("GET", path, key, "").want(http.StatusOK).body)
			for _, f := range foreign {
				if strings.Contains(string(raw), f) {
					t.Errorf("GET %s shows another tenant's %s", path, f)
				}
			}
		}
	}
	platformOnly := func(method, path, body string) func() {
		return func() { e.call(method, path, a, body).wantError(http.StatusForbidden, "permission_denied") }
	}
	cases := map[string]func(){
		"POST /v1/job-types": func() {
			e.call("POST", "/v1/job-types", a, `{"name": "a.made"}`).want(http.StatusCreated)
			e.call("GET", "/v1/job-types/a.made", b, "").wantError(http.StatusNotFound, "not_found")
		},
		"GET /v1/job-types":                excludes(a, "/v1/job-types", "b.only"),
		"GET /v1/job-types/{name}":         notFound("GET", "/v1/job-types/b.only", ""),
		"PATCH /v1/job-types/{name}":       notFound("PATCH", "/v1/job-types/b.only", `{"enabled": false}`),
		"POST /v1/job-types/{name}/pause":  notFound("POST", "/v1/job-types/b.only/pause", ""),
		"POST /v1/job-types/{name}/resume": notFound("POST", "/v1/job-types/b.only/resume", ""),

		"POST /v1/jobs": func() {
			e.call("POST", "/v1/jobs", a, `{"type": "b.only"}`).wantError(http.StatusUnprocessableEntity, "invalid_argument")
		},
		"GET /v1/jobs":                    excludes(a, "/v1/jobs", bJob),
		"GET /v1/jobs/{id}":               notFound("GET", "/v1/jobs/"+bJob, ""),
		"GET /v1/jobs/{id}/attempts":      notFound("GET", "/v1/jobs/"+bJob+"/attempts", ""),
		"POST /v1/jobs/{id}/cancel":       notFound("POST", "/v1/jobs/"+bJob+"/cancel", ""),
		"POST /v1/jobs/{id}/pause":        notFound("POST", "/v1/jobs/"+bJob+"/pause", ""),
		"POST /v1/jobs/{id}/resume":       notFound("POST", "/v1/jobs/"+bJob+"/resume", ""),
		"POST /v1/jobs/{id}/run":          notFound("POST", "/v1/jobs/"+bJob+"/run", ""),
		"POST /v1/jobs/{id}/retry":        notFound("POST", "/v1/jobs/"+bJob+"/retry", ""),
		"GET /v1/schedules":               excludes(a, "/v1/schedules", bSchedule),
		"GET /v1/schedules/{id}":          notFound("GET", "/v1/schedules/"+bSchedule, ""),
		"PATCH /v1/schedules/{id}":        notFound("PATCH", "/v1/schedules/"+bSchedule, `{"max_runs": 3}`),
		"DELETE /v1/schedules/{id}":       notFound("DELETE", "/v1/schedules/"+bSchedule, ""),
		"POST /v1/schedules/{id}/pause":   notFound("POST", "/v1/schedules/"+bSchedule+"/pause", ""),
		"POST /v1/schedules/{id}/resume":  notFound("POST", "/v1/schedules/"+bSchedule+"/resume", ""),
		"POST /v1/schedules/{id}/trigger": notFound("POST", "/v1/schedules/"+bSchedule+"/trigger", ""),
		"POST /v1/schedules": func() {
			e.call("POST", "/v1/schedules", a, `{"name": "a.nightly", "job_type": "b.only", "trigger": {"kind": "cron", "cron": "30 2 * * *"}}`).
				wantError(http.StatusUnprocessableEntity, "invalid_argument")
		},

		"GET /v1/quotas": func() { e.call("GET", "/v1/quotas", a, "").want(http.StatusOK) }, // the tenant comes from the key alone
		"POST /v1/operations": func() {
			op := e.call("POST", "/v1/operations", a, `{"kind": "cancel"}`).want(http.StatusAccepted).str("id")
			for {
				found, err := e.store.ProcessOperation(ctx, 100)
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
			}
			if s := e.call("GET", "/v1/jobs/"+bJob, b, "").want(http.StatusOK).str("state"); s != "READY" {
				t.Errorf("tenant A's cancel-everything operation left tenant B's job %s", s)
			}
			e.call("GET", "/v1/operations/"+op, b, "").wantError(http.StatusNotFound, "not_found")
		},
		"GET /v1/operations/{id}": notFound("GET", "/v1/operations/"+bOperation, ""),

		"POST /v1/api-keys": func() {
			k := e.call("POST", "/v1/api-keys", a, `{"name": "a.ci", "role": "viewer"}`).want(http.StatusCreated).str("id")
			excludes(b, "/v1/api-keys", k)()
		},
		"GET /v1/api-keys":              excludes(a, "/v1/api-keys", bKey),
		"DELETE /v1/api-keys/{id}":      notFound("DELETE", "/v1/api-keys/"+bKey, ""),
		"POST /v1/api-keys/{id}/rotate": notFound("POST", "/v1/api-keys/"+bKey+"/rotate", ""),

		"GET /v1/workers":                            platformOnly("GET", "/v1/workers", ""),
		"POST /v1/workers/{id}/drain":                platformOnly("POST", "/v1/workers/"+worker+"/drain", ""),
		"DELETE /v1/workers/{id}":                    platformOnly("DELETE", "/v1/workers/"+worker, ""),
		"GET /v1/pools":                              platformOnly("GET", "/v1/pools", ""),
		"POST /v1/pools/{name}/pause":                platformOnly("POST", "/v1/pools/default/pause", ""),
		"POST /v1/pools/{name}/resume":               platformOnly("POST", "/v1/pools/default/resume", ""),
		"PUT /v1/pools/{name}/settings":              platformOnly("PUT", "/v1/pools/default/settings", `{}`),
		"POST /v1/pools/{name}/worker-tokens":        platformOnly("POST", "/v1/pools/default/worker-tokens", `{"name": "x"}`),
		"GET /v1/pools/{name}/worker-tokens":         platformOnly("GET", "/v1/pools/default/worker-tokens", ""),
		"DELETE /v1/pools/{name}/worker-tokens/{id}": platformOnly("DELETE", "/v1/pools/default/worker-tokens/"+worker, ""),
		"GET /v1/tenants":                            platformOnly("GET", "/v1/tenants", ""),
		"GET /v1/tenants/{id}/quotas":                platformOnly("GET", "/v1/tenants/"+string(bTenant)+"/quotas", ""),
		"PUT /v1/tenants/{id}/quotas":                platformOnly("PUT", "/v1/tenants/"+string(bTenant)+"/quotas", `{}`),
	}

	routes := e.api.Routes()
	for _, route := range routes {
		check, ok := cases[route]
		if !ok {
			t.Errorf("route %s has no tenant-isolation case; add one", route)
			continue
		}
		t.Logf("checking %s", route)
		check()
	}
	for route := range cases {
		if !slices.Contains(routes, route) {
			t.Errorf("isolation case for %s, which is not a route", route)
		}
	}
}
