package api

import (
	"net/http"
	"testing"

	"jobscheduler/internal/domain"
)

func TestPlatformEndpointsNeedPlatformAdmin(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.call("GET", "/v1/pools", e.admin, "").wantError(http.StatusForbidden, "permission_denied")
	e.call("POST", "/v1/pools/default/pause", e.admin, "").wantError(http.StatusForbidden, "permission_denied")
	e.call("GET", "/v1/workers", e.admin, "").wantError(http.StatusForbidden, "permission_denied")
	e.call("POST", "/v1/api-keys", e.admin, `{"name": "escalate", "role": "platform-admin"}`).
		wantError(http.StatusForbidden, "permission_denied")

	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	pools, _ := e.call("GET", "/v1/pools", root, "").want(http.StatusOK).get("items").([]any)
	if len(pools) != 1 || pools[0].(map[string]any)["name"] != "default" {
		t.Fatalf("pools = %v", pools)
	}
	paused := e.call("POST", "/v1/pools/default/pause", root, "").want(http.StatusOK)
	if paused.get("paused") != true || paused.str("name") != "default" {
		t.Errorf("pause = %v", paused.body)
	}
	if e.call("POST", "/v1/pools/default/resume", root, "").want(http.StatusOK).get("paused") != false {
		t.Error("resume did not clear the hold")
	}
	e.call("POST", "/v1/pools/Bad%20Name/pause", root, "").wantError(http.StatusUnprocessableEntity, "invalid_argument")
	if pool := pools[0].(map[string]any); pool["backlog_target"] != "5m0s" || pool["backlog_age"] != nil {
		t.Errorf("pool before any owner sample = %v, want the default target and no backlog age", pool)
	}
	e.call("PUT", "/v1/pools/default/settings", e.admin, `{"backlog_target": "1h"}`).wantError(http.StatusForbidden, "permission_denied")
	for _, body := range []string{`{"backlog_target": "5s"}`, `{"backlog_target": "25h"}`, `{"backlog_target": "soon"}`} {
		if e.call("PUT", "/v1/pools/default/settings", root, body).wantError(http.StatusUnprocessableEntity, "invalid_argument").
			fieldError("backlog_target") == nil {
			t.Errorf("%s: no error for backlog_target", body)
		}
	}
	if set := e.call("PUT", "/v1/pools/default/settings", root, `{"backlog_target": "1h"}`).want(http.StatusOK); set.str("backlog_target") != "1h0m0s" {
		t.Errorf("pool after setting a target = %v", set.body)
	}
	if reset := e.call("PUT", "/v1/pools/default/settings", root, `{}`).want(http.StatusOK); reset.str("backlog_target") != "5m0s" {
		t.Errorf("pool after resetting the target = %v", reset.body)
	}
	e.call("PUT", "/v1/pools/batch/settings", root, `{"backlog_target": "2h"}`).want(http.StatusOK) // configured before any job type uses it
	if workers, _ := e.call("GET", "/v1/workers?pool=default", root, "").want(http.StatusOK).get("items").([]any); len(workers) != 0 {
		t.Errorf("workers = %v, want none", workers)
	}
	e.call("POST", "/v1/workers/0191f000-0000-7000-8000-000000000001/drain", root, "").wantError(http.StatusNotFound, "not_found")
	e.call("DELETE", "/v1/workers/0191f000-0000-7000-8000-000000000001", root, "").wantError(http.StatusNotFound, "not_found")
	e.call("POST", "/v1/api-keys", root, `{"name": "second root", "role": "platform-admin"}`).want(http.StatusCreated)
}

func TestJobTypePauseAndScheduleTrigger(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	operator := e.key(e.tenant, domain.RoleOperator)
	e.call("POST", "/v1/job-types/email.send/pause", e.key(e.tenant, domain.RoleSubmitter), "").
		wantError(http.StatusForbidden, "permission_denied")
	if e.call("POST", "/v1/job-types/email.send/pause", operator, "").want(http.StatusOK).get("paused") != true {
		t.Error("pause did not set paused")
	}
	if e.call("GET", "/v1/job-types/email.send", operator, "").get("paused") != true {
		t.Error("paused flag not visible on the job type")
	}
	e.call("POST", "/v1/job-types/email.send/resume", operator, "").want(http.StatusOK)
	e.call("POST", "/v1/job-types/missing/pause", operator, "").wantError(http.StatusNotFound, "not_found")

	id := e.call("POST", "/v1/schedules", operator,
		`{"name": "nightly", "job_type": "email.send", "trigger": {"kind": "cron", "cron": "0 3 * * *"}}`).want(http.StatusCreated).str("id")
	job := e.call("POST", "/v1/schedules/"+id+"/trigger", operator, "").want(http.StatusCreated)
	if job.str("schedule_id") != id || job.str("state") != "SCHEDULED" || job.str("fire_time") == "" {
		t.Errorf("triggered job = %v", job.body)
	}
	e.call("POST", "/v1/schedules/0191f000-0000-7000-8000-000000000001/trigger", operator, "").wantError(http.StatusNotFound, "not_found")
}
