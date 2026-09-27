package api

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

// noCache makes quota, pending and backlog changes visible to the next request.
func (e *env) noCache() {
	e.api.admission.quotaTTL, e.api.admission.pendingTTL, e.api.admission.backlogTTL = 0, 0, 0
}

func TestQuotaManagement(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)

	own := e.call("GET", "/v1/quotas", e.admin, "").want(http.StatusOK)
	if own.get("rate_limit") != float64(1000) || own.get("max_payload_bytes") != float64(domain.MaxPayloadBytes) ||
		own.str("min_schedule_interval") != "1m0s" || own.get("max_pending") != nil {
		t.Errorf("default effective quotas = %v", own.body)
	}
	tenants, _ := e.call("GET", "/v1/tenants", root, "").want(http.StatusOK).get("items").([]any)
	if len(tenants) != 2 {
		t.Errorf("tenants = %v", tenants)
	}
	e.call("GET", "/v1/tenants", e.admin, "").wantError(http.StatusForbidden, "permission_denied")

	path := "/v1/tenants/" + string(e.tenant) + "/quotas"
	for body, field := range map[string]string{
		`{"rate_limit": 0}`:                  "rate_limit",
		`{"max_pending": 2000000}`:           "max_pending",
		`{"max_schedules": 0}`:               "max_schedules",
		`{"min_schedule_interval": "100ms"}`: "min_schedule_interval",
		`{"max_payload_bytes": 100000}`:      "max_payload_bytes",
	} {
		if r := e.call("PUT", path, root, body).wantError(http.StatusUnprocessableEntity, "invalid_argument"); r.fieldError(field) == nil {
			t.Errorf("%s: no error for %s", body, field)
		}
	}
	set := e.call("PUT", path, root, `{"max_pending": 10, "max_running": 3, "min_schedule_interval": "5m", "max_payload_bytes": 1024}`).
		want(http.StatusOK)
	if set.get("max_pending") != float64(10) || set.str("min_schedule_interval") != "5m0s" || set.get("rate_limit") != nil {
		t.Errorf("set quotas = %v", set.body)
	}
	if got := e.call("GET", path, root, "").want(http.StatusOK); got.get("max_running") != float64(3) {
		t.Errorf("configured quotas = %v", got.body)
	}
	e.call("PUT", "/v1/tenants/0191f000-0000-7000-8000-000000000001/quotas", root, `{}`).wantError(http.StatusNotFound, "not_found")

	var audits int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE action = 'tenant.quotas' AND tenant_id = $1`,
		string(platform)).Scan(&audits); err != nil || audits != 1 {
		t.Errorf("quota audit rows under the platform tenant = %d (%v), want 1", audits, err)
	}
}

func TestQuotasAreEnforced(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.noCache()
	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	e.call("PUT", "/v1/tenants/"+string(e.tenant)+"/quotas", root,
		`{"max_pending": 2, "max_schedules": 1, "min_schedule_interval": "10m", "max_payload_bytes": 64}`).want(http.StatusOK)

	big := `{"type": "email.send", "payload": {"text": "` + strings.Repeat("x", 100) + `"}}`
	e.call("POST", "/v1/jobs", e.admin, big).wantError(http.StatusRequestEntityTooLarge, "payload_too_large")
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated)
	r := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).wantError(http.StatusTooManyRequests, "quota_exceeded")
	if r.header.Get("Retry-After") == "" {
		t.Error("quota_exceeded without Retry-After")
	}

	schedule := func(interval string) response {
		return e.call("POST", "/v1/schedules", e.admin, `{"name": "s`+interval+`", "job_type": "email.send",
			"trigger": {"kind": "fixed_rate", "interval": "`+interval+`"}}`)
	}
	if r := schedule("5m").wantError(http.StatusUnprocessableEntity, "invalid_argument"); r.fieldError("trigger.interval") == nil {
		t.Errorf("an interval below the tenant's minimum was accepted: %v", r.body)
	}
	schedule("10m").want(http.StatusCreated)
	schedule("20m").wantError(http.StatusTooManyRequests, "quota_exceeded")
}

func TestPerTenantRateLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.noCache()
	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	e.call("PUT", "/v1/tenants/"+string(e.tenant)+"/quotas", root, `{"rate_limit": 1}`).want(http.StatusOK)
	e.call("GET", "/v1/jobs", e.admin, "").want(http.StatusOK)
	e.call("GET", "/v1/jobs", e.admin, "").want(http.StatusOK) // burst of 2
	e.call("GET", "/v1/jobs", e.admin, "").wantError(http.StatusTooManyRequests, "rate_limited")
	e.call("GET", "/v1/tenants", root, "").want(http.StatusOK) // other tenants keep the default rate
}

// sampleBacklog records the default pool's backlog as its owner would (ADR-021); capped lists
// the tenants at their running cap.
func (e *env) sampleBacklog(capped ...domain.TenantID) {
	e.t.Helper()
	name := postgres.PoolLeaseName("default")
	l, err := e.store.GetLease(ctx, name)
	if errors.Is(err, domain.ErrNotFound) {
		l, _, err = e.store.AcquireLease(ctx, name, "test-owner", "", time.Hour)
	}
	if err != nil {
		e.t.Fatal(err)
	}
	g, err := e.store.SamplePoolGauges(ctx, "default", capped)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.store.RecordPoolBacklog(ctx, l, "default", g.OldestDue); err != nil {
		e.t.Fatal(err)
	}
}

func TestOverloadedPoolShedsLowThenNormal(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.noCache()
	id := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "priority": "HIGH"}`).want(http.StatusCreated).str("id")
	backlog := func(age time.Duration) {
		if _, err := e.pool.Exec(ctx, `UPDATE jobs SET run_at = now() - make_interval(secs => $2) WHERE id = $1`,
			id, age.Seconds()); err != nil {
			t.Fatal(err)
		}
		e.sampleBacklog()
	}
	operator := e.key(e.tenant, domain.RoleOperator)
	submit := func(priority string) response {
		return e.call("POST", "/v1/jobs", operator, `{"type": "email.send", "priority": "`+priority+`"}`)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE jobs SET run_at = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	submit("LOW").want(http.StatusCreated) // no owner has sampled the pool: its backlog is unknown

	backlog(10 * time.Minute) // past the default target (5 m)
	r := submit("LOW").wantError(http.StatusServiceUnavailable, "overloaded")
	if r.header.Get("Retry-After") != "30" {
		t.Errorf("Retry-After = %q, want 30", r.header.Get("Retry-After"))
	}
	submit("NORMAL").want(http.StatusCreated)

	backlog(20 * time.Minute) // past three times the target
	submit("NORMAL").wantError(http.StatusServiceUnavailable, "overloaded")
	submit("HIGH").want(http.StatusCreated)
	submit("CRITICAL").want(http.StatusCreated)

	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	e.call("PUT", "/v1/pools/default/settings", root, `{"backlog_target": "30m"}`).want(http.StatusOK)
	submit("LOW").want(http.StatusCreated) // 20 m is within the pool's own target
	e.call("PUT", "/v1/pools/default/settings", root, `{}`).want(http.StatusOK)
	submit("LOW").wantError(http.StatusServiceUnavailable, "overloaded")

	e.sampleBacklog(e.tenant)
	submit("LOW").want(http.StatusCreated) // a tenant at its running cap sheds nobody
	e.sampleBacklog()
	e.call("POST", "/v1/pools/default/pause", root, "").want(http.StatusOK)
	e.sampleBacklog()
	submit("LOW").want(http.StatusCreated) // a paused pool's backlog is expected, not overload

	if _, err := e.pool.Exec(ctx, `UPDATE pools SET sampled_at = now() - interval '2 minutes'`); err != nil {
		t.Fatal(err)
	}
	e.call("POST", "/v1/pools/default/resume", root, "").want(http.StatusOK)
	submit("LOW").want(http.StatusCreated) // a stale sample is ignored
}
