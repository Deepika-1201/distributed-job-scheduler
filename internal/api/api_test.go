package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/internal/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m, func(ctx context.Context, url string) error {
		pool, err := postgres.NewPool(ctx, url, 2)
		if err != nil {
			return err
		}
		defer pool.Close()
		return postgres.Migrate(ctx, pool, slog.New(slog.DiscardHandler))
	}))
}

var ctx = context.Background()

type env struct {
	t      *testing.T
	pool   *pgxpool.Pool
	store  *postgres.Store
	api    *Server
	srv    *httptest.Server
	tenant domain.TenantID
	admin  string
}

func newEnv(t *testing.T, rate float64) *env {
	t.Helper()
	pool, err := postgres.NewPool(ctx, pgtest.NewDatabase(t), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.NewStore(pool)
	if err := store.EnsurePartitions(ctx, time.Now(), 2); err != nil {
		t.Fatal(err)
	}
	apiServer := New(store, slog.New(slog.DiscardHandler), Config{TenantRateLimit: rate})
	srv := httptest.NewServer(apiServer.Handler())
	t.Cleanup(srv.Close)

	e := &env{t: t, pool: pool, store: store, api: apiServer, srv: srv}
	e.tenant, e.admin = e.newTenant("acme")
	e.call("POST", "/v1/job-types", e.admin, `{"name": "email.send"}`).want(http.StatusCreated)
	return e
}

func (e *env) newTenant(name string) (domain.TenantID, string) {
	e.t.Helper()
	tenant, err := e.store.CreateTenant(ctx, name)
	if err != nil {
		e.t.Fatal(err)
	}
	return tenant, e.key(tenant, domain.RoleAdmin)
}

func (e *env) key(tenant domain.TenantID, role domain.Role) string {
	e.t.Helper()
	plaintext, k := NewAPIKey(tenant, "test", role, time.Time{})
	if _, err := e.store.CreateAPIKey(ctx, k, nil); err != nil {
		e.t.Fatal(err)
	}
	return plaintext
}

type response struct {
	t      *testing.T
	status int
	header http.Header
	body   map[string]any
}

func (e *env) call(method, path, key, body string, headers ...string) response {
	e.t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := response{t: e.t, status: resp.StatusCode, header: resp.Header, body: map[string]any{}}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &r.body); err != nil {
			e.t.Fatalf("%s %s: invalid JSON %q", method, path, raw)
		}
	}
	return r
}

func (r response) want(status int) response {
	r.t.Helper()
	if r.status != status {
		r.t.Fatalf("status %d, want %d: %v", r.status, status, r.body)
	}
	return r
}

// get navigates a dotted path such as "error.details.fields.type".
func (r response) get(path string) any {
	var v any = r.body
	for _, part := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[part]
	}
	return v
}

func (r response) str(path string) string {
	s, _ := r.get(path).(string)
	return s
}

func (r response) wantError(status int, code string) response {
	r.t.Helper()
	r.want(status)
	if got := r.str("error.code"); got != code {
		r.t.Fatalf("error code %q, want %q: %v", got, code, r.body)
	}
	if r.str("error.request_id") == "" {
		r.t.Errorf("error without request_id: %v", r.body)
	}
	return r
}

func TestAuthentication(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.call("GET", "/v1/jobs", "", "").wantError(http.StatusUnauthorized, "unauthenticated")
	e.call("GET", "/v1/jobs", "not-a-key", "").wantError(http.StatusUnauthorized, "unauthenticated")
	e.call("GET", "/v1/jobs", "jsk_aaaaaaaa_bbbb", "").wantError(http.StatusUnauthorized, "unauthenticated")

	viewer := e.key(e.tenant, domain.RoleViewer)
	e.call("GET", "/v1/jobs", viewer, "").want(http.StatusOK)
	e.call("POST", "/v1/jobs", viewer, `{"type": "email.send"}`).wantError(http.StatusForbidden, "permission_denied")

	created := e.call("POST", "/v1/api-keys", e.admin, `{"name": "ci", "role": "submitter"}`).want(http.StatusCreated)
	e.call("DELETE", "/v1/api-keys/"+created.str("id"), e.admin, "").want(http.StatusNoContent)
	e.call("GET", "/v1/jobs", created.str("key"), "").wantError(http.StatusUnauthorized, "unauthenticated")
}

func TestRequestIDAndUnknownRoutes(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	r := e.call("GET", "/v1/nope", e.admin, "", "X-Request-Id", "trace-123").wantError(http.StatusNotFound, "not_found")
	if r.header.Get("X-Request-Id") != "trace-123" || r.str("error.request_id") != "trace-123" {
		t.Errorf("request id not propagated: header %q, body %q", r.header.Get("X-Request-Id"), r.str("error.request_id"))
	}
	if got := e.call("GET", "/v1/jobs", e.admin, "", "X-Request-Id", "bad id!").header.Get("X-Request-Id"); got == "bad id!" || got == "" {
		t.Errorf("unsafe request id %q was echoed", got)
	}
}

func TestJobTypes(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	jt := e.call("GET", "/v1/job-types/email.send", e.admin, "").want(http.StatusOK)
	if jt.str("pool") != "default" || jt.str("default_priority") != "NORMAL" || jt.str("attempt_timeout") != "5m0s" ||
		jt.str("retry_policy.initial_delay") != "10s" || jt.get("enabled") != true {
		t.Errorf("defaults not applied: %v", jt.body)
	}
	e.call("POST", "/v1/job-types", e.admin, `{"name": "email.send"}`).wantError(http.StatusConflict, "conflict")
	bad := e.call("POST", "/v1/job-types", e.admin, `{"name": "Bad Name", "attempt_timeout": "1ms"}`).
		wantError(http.StatusUnprocessableEntity, "invalid_argument")
	if bad.get("error.details.fields.name") == nil {
		t.Errorf("missing field error: %v", bad.body)
	}

	patched := e.call("PATCH", "/v1/job-types/email.send", e.admin,
		`{"attempt_timeout": "30s", "default_priority": "HIGH", "retry_policy": {"max_attempts": 3}}`).want(http.StatusOK)
	if patched.str("attempt_timeout") != "30s" || patched.str("default_priority") != "HIGH" ||
		patched.get("retry_policy.max_attempts") != float64(3) || patched.str("retry_policy.initial_delay") != "10s" {
		t.Errorf("patch = %v", patched.body)
	}
	e.call("PATCH", "/v1/job-types/email.send", e.admin, `{"retry_policy": {"max_attempts": 0}}`).
		wantError(http.StatusUnprocessableEntity, "invalid_argument")
	e.call("POST", "/v1/job-types", e.admin, `{"name": "reports.build", "pool": "batch"}`).want(http.StatusCreated)
	if items, _ := e.call("GET", "/v1/job-types", e.admin, "").want(http.StatusOK).get("items").([]any); len(items) != 2 {
		t.Errorf("listed %d job types, want 2", len(items))
	}
	e.call("GET", "/v1/job-types/missing", e.admin, "").wantError(http.StatusNotFound, "not_found")
}

func TestSubmitJob(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	submitter := e.key(e.tenant, domain.RoleSubmitter)

	r := e.call("POST", "/v1/jobs", submitter, `{"type": "email.send", "payload": {"to": "a@example.com"}, "labels": {"team": "growth"}}`).
		want(http.StatusCreated)
	if r.header.Get("X-Submission-Outcome") != "created" || r.str("state") != "READY" || r.str("priority") != "NORMAL" ||
		r.str("attempt_timeout") != "5m0s" || r.str("payload.to") != "a@example.com" || r.str("labels.team") != "growth" {
		t.Errorf("submitted job = %v", r.body)
	}
	if !strings.HasPrefix(r.str("created_by"), "api_key:") {
		t.Errorf("created_by = %q", r.str("created_by"))
	}

	delayed := e.call("POST", "/v1/jobs", submitter, `{"type": "email.send", "delay": "1h", "retry_policy": {"max_attempts": 3}}`).
		want(http.StatusCreated)
	if delayed.str("state") != "SCHEDULED" || delayed.get("retry_policy.max_attempts") != float64(3) {
		t.Errorf("delayed job = %v", delayed.body)
	}

	for body, field := range map[string]string{
		`{"type": "nope"}`: "type",
		`{"type": "email.send", "priority": "URGENT"}`:                            "priority",
		`{"type": "email.send", "delay": "soon"}`:                                 "delay",
		`{"type": "email.send", "delay": "1m", "run_at": "2030-01-01T00:00:00Z"}`: "delay",
		`{"type": "email.send", "retry_policy": {"max_attempts": 0}}`:             "retry_policy",
	} {
		v := e.call("POST", "/v1/jobs", submitter, body).wantError(http.StatusUnprocessableEntity, "invalid_argument")
		if v.get("error.details.fields."+field) == nil {
			t.Errorf("%s: missing error for %s: %v", body, field, v.body)
		}
	}
	e.call("POST", "/v1/jobs", submitter, `{"type": "email.send", "colour": "red"}`).wantError(http.StatusBadRequest, "invalid_json")
	big := `{"type": "email.send", "payload": {"blob": "` + strings.Repeat("x", 70<<10) + `"}}`
	e.call("POST", "/v1/jobs", submitter, big).wantError(http.StatusRequestEntityTooLarge, "payload_too_large")

	e.call("POST", "/v1/jobs", submitter, `{"type": "email.send", "priority": "CRITICAL"}`).wantError(http.StatusForbidden, "permission_denied")
	operator := e.key(e.tenant, domain.RoleOperator)
	e.call("POST", "/v1/jobs", operator, `{"type": "email.send", "priority": "CRITICAL"}`).want(http.StatusCreated)
}

func TestIdempotencyAndDedupe(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	body := `{"type": "email.send", "payload": {"n": 1}}`
	first := e.call("POST", "/v1/jobs", e.admin, body, "Idempotency-Key", "order-9").want(http.StatusCreated)
	replay := e.call("POST", "/v1/jobs", e.admin, body, "Idempotency-Key", "order-9").want(http.StatusOK)
	if replay.header.Get("X-Submission-Outcome") != "replayed" || replay.str("id") != first.str("id") {
		t.Errorf("replay = %s %s, want %s", replay.header.Get("X-Submission-Outcome"), replay.str("id"), first.str("id"))
	}
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "payload": {"n": 2}}`, "Idempotency-Key", "order-9").
		wantError(http.StatusUnprocessableEntity, "idempotency_key_reused")

	a := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "dedupe_key": "invoice-7"}`).want(http.StatusCreated)
	b := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "dedupe_key": "invoice-7"}`).want(http.StatusOK)
	if b.header.Get("X-Submission-Outcome") != "deduplicated" || b.str("id") != a.str("id") {
		t.Errorf("dedupe = %s %s, want %s", b.header.Get("X-Submission-Outcome"), b.str("id"), a.str("id"))
	}
}

func TestListJobsPaginatesAcrossActiveAndFinished(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	var ids []string
	for i := range 5 {
		label := map[bool]string{true: "a", false: "b"}[i%2 == 0]
		ids = append(ids, e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "labels": {"team": "`+label+`"}}`).
			want(http.StatusCreated).str("id"))
	}
	e.call("POST", "/v1/jobs/"+ids[0]+"/cancel", e.admin, "").want(http.StatusOK)
	e.call("POST", "/v1/jobs/"+ids[3]+"/cancel", e.admin, "").want(http.StatusOK)

	seen, cursor := map[string]bool{}, ""
	for page := 0; ; page++ {
		r := e.call("GET", "/v1/jobs?limit=2&cursor="+cursor, e.admin, "").want(http.StatusOK)
		for _, it := range r.get("items").([]any) {
			seen[it.(map[string]any)["id"].(string)] = true
		}
		if cursor = r.str("next_cursor"); cursor == "" {
			break
		}
		if page > 5 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Errorf("paginated over %d distinct jobs, want 5", len(seen))
	}
	count := func(query string) int {
		items, _ := e.call("GET", "/v1/jobs?"+query, e.admin, "").want(http.StatusOK).get("items").([]any)
		return len(items)
	}
	if n := count("state=CANCELLED"); n != 2 {
		t.Errorf("state=CANCELLED returned %d, want 2", n)
	}
	if n := count("label=team:a"); n != 3 {
		t.Errorf("label=team:a returned %d, want 3", n)
	}
	e.call("GET", "/v1/jobs?limit=0", e.admin, "").wantError(http.StatusUnprocessableEntity, "invalid_argument")

	_, otherAdmin := e.newTenant("globex")
	e.call("GET", "/v1/jobs/"+ids[1], otherAdmin, "").wantError(http.StatusNotFound, "not_found")
	if n := len(e.call("GET", "/v1/jobs", otherAdmin, "").get("items").([]any)); n != 0 {
		t.Errorf("other tenant sees %d jobs", n)
	}
}

func TestLifecycleOperations(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	id := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "delay": "1h"}`).want(http.StatusCreated).str("id")

	if s := e.call("POST", "/v1/jobs/"+id+"/pause", e.admin, "").want(http.StatusOK).str("state"); s != "PAUSED" {
		t.Errorf("pause -> %s", s)
	}
	conflict := e.call("POST", "/v1/jobs/"+id+"/pause", e.admin, "").wantError(http.StatusConflict, "conflict")
	if conflict.str("error.details.state") != "PAUSED" {
		t.Errorf("conflict details = %v", conflict.body)
	}
	run := e.call("POST", "/v1/jobs/"+id+"/run", e.admin, "").want(http.StatusOK)
	if runAt, _ := time.Parse(time.RFC3339Nano, run.str("run_at")); run.str("state") != "SCHEDULED" || time.Since(runAt) > time.Minute {
		t.Errorf("run now = %s at %s", run.str("state"), run.str("run_at"))
	}
	e.call("POST", "/v1/jobs/"+id+"/cancel", e.admin, "").want(http.StatusOK)
	e.call("POST", "/v1/jobs/"+id+"/retry", e.admin, "").wantError(http.StatusConflict, "conflict")

	submitter := e.key(e.tenant, domain.RoleSubmitter)
	other := e.call("POST", "/v1/jobs", submitter, `{"type": "email.send"}`).want(http.StatusCreated).str("id")
	e.call("POST", "/v1/jobs/"+other+"/pause", submitter, "").wantError(http.StatusForbidden, "permission_denied")

	// Dead-letter a job through the store, then retry it through the API.
	dead := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "priority": "LOW", "retry_policy": {"max_attempts": 1, "max_lost_attempts": 1}}`).
		want(http.StatusCreated).str("id")
	lease, _, err := e.store.AcquireLease(ctx, postgres.PoolLeaseName("default"), "test-node", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := e.store.ClaimReady(ctx, postgres.ClaimRequest{Lease: lease, Pool: "default", Priority: domain.PriorityLow, Limit: 1,
		SessionID: "0191f000-0000-7000-8000-000000000001"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, %d jobs", err, len(claimed))
	}
	c := claimed[0].Current
	if _, err := e.store.CompleteAttempt(ctx, postgres.Completion{JobID: claimed[0].ID, AttemptID: c.ID, Number: c.Number,
		End: domain.AttemptEnd{State: domain.AttemptFailed, Retryable: true}, Error: "boom", Actor: domain.ActorDispatcher}); err != nil {
		t.Fatal(err)
	}
	retried := e.call("POST", "/v1/jobs/"+dead+"/retry", e.admin, "").want(http.StatusOK)
	if retried.str("state") != "READY" || retried.get("attempt_count") != float64(1) {
		t.Errorf("retried job = %v", retried.body)
	}
	attempts, _ := e.call("GET", "/v1/jobs/"+dead+"/attempts", e.admin, "").want(http.StatusOK).get("items").([]any)
	if len(attempts) != 1 || attempts[0].(map[string]any)["state"] != "FAILED" || attempts[0].(map[string]any)["error"] != "boom" {
		t.Errorf("attempts = %v", attempts)
	}

	var audits int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE action LIKE 'job.%'`).Scan(&audits); err != nil || audits != 4 {
		t.Errorf("audit rows for job actions = %d (%v), want 4 (pause, run, cancel, retry)", audits, err)
	}
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	e.call("GET", "/v1/jobs", e.admin, "").want(http.StatusOK)
	r := e.call("GET", "/v1/jobs", e.admin, "").wantError(http.StatusTooManyRequests, "rate_limited")
	if r.header.Get("Retry-After") == "" {
		t.Error("429 without Retry-After")
	}
}
