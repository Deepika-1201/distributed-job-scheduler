package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
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
