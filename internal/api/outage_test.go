package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/pgtest"
)

// While the database is refused or blackholed, the API answers 503 with Retry-After rather
// than 500 or a hung request, and recovers when it returns (HLD S6, LLD §21.2).
func TestDatabaseOutageAnswers503(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	proxy, url := pgtest.NewProxy(t, e.pool.Config().ConnString())
	pool, err := postgres.NewPool(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	srv := httptest.NewServer(New(postgres.NewStore(pool), slog.New(slog.DiscardHandler),
		Config{TenantRateLimit: 1000, RequestTimeout: 500 * time.Millisecond}).Handler())
	t.Cleanup(srv.Close)
	get := func() (int, string, string) {
		t.Helper()
		req, _ := http.NewRequest("GET", srv.URL+"/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer "+e.admin)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body errorBody
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body.Error.Code, resp.Header.Get("Retry-After")
	}
	recovered := func() {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if status, _, _ := get(); status == http.StatusOK {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the API did not recover after the database returned")
			}
		}
	}
	recovered()

	for name, fault := range map[string]func(){"cut": proxy.Cut, "stall": proxy.Stall} {
		fault()
		if status, code, retry := get(); status != http.StatusServiceUnavailable || code != "unavailable" || retry != "5" {
			t.Errorf("%s: answered %d %q with Retry-After %q; want 503 unavailable, Retry-After 5", name, status, code, retry)
		}
		proxy.Restore()
		recovered()
	}
}
