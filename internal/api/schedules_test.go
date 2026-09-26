package api

import (
	"net/http"
	"testing"

	"jobscheduler/internal/domain"
)

// fieldError returns the validation message for a field; names may contain dots.
func (r response) fieldError(field string) any {
	fields, _ := r.get("error.details.fields").(map[string]any)
	return fields[field]
}

func TestScheduleLifecycle(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	operator := e.key(e.tenant, domain.RoleOperator)
	nightly := `{"name": "nightly", "job_type": "email.send", "payload": {"report": "sales"},
		"trigger": {"kind": "cron", "cron": "30 2 * * *", "time_zone": "America/New_York"}}`

	e.call("POST", "/v1/schedules", e.key(e.tenant, domain.RoleSubmitter), nightly).wantError(http.StatusForbidden, "permission_denied")
	created := e.call("POST", "/v1/schedules", operator, nightly).want(http.StatusCreated)
	id := created.str("id")
	upcoming, _ := created.get("upcoming").([]any)
	if created.str("state") != "ACTIVE" || created.str("misfire_policy") != "fire_once" || created.str("overlap_policy") != "skip" ||
		created.str("trigger.time_zone") != "America/New_York" || len(upcoming) != 5 || upcoming[0] != created.str("next_fire_at") {
		t.Errorf("created schedule = %v", created.body)
	}
	e.call("POST", "/v1/schedules", operator, nightly).wantError(http.StatusConflict, "conflict")

	for body, field := range map[string]string{
		`{"name": "a", "job_type": "email.send"}`:                                                                                        "trigger",
		`{"name": "a", "job_type": "nope", "trigger": {"kind": "fixed_rate", "interval": "1h"}}`:                                         "job_type",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "weekly"}}`:                                                         "trigger.kind",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "cron", "cron": "* * *"}}`:                                          "trigger",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "cron", "cron": "*/10 * * * * *"}}`:                                 "trigger.cron",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "cron", "cron": "@daily", "time_zone": "Mars/Olympus"}}`:            "trigger",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "10s"}}`:                                  "trigger.interval",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "1m"}, "jitter": "2m"}`:                   "jitter",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "1m"}, "end_at": "2020-01-01T00:00:00Z"}`: "end_at",
		`{"name": "a", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "1m"}, "overlap_policy": "queue"}`:        "overlap_policy",
		`{"name": "Bad Name", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "1m"}}`:                            "name",
	} {
		r := e.call("POST", "/v1/schedules", operator, body).wantError(http.StatusUnprocessableEntity, "invalid_argument")
		if r.fieldError(field) == nil {
			t.Errorf("%s: no error for %q: %v", body, field, r.body)
		}
	}

	if got := e.call("GET", "/v1/schedules/"+id, e.admin, "").want(http.StatusOK); got.str("name") != "nightly" || got.get("upcoming") == nil {
		t.Errorf("get = %v", got.body)
	}
	list := e.call("GET", "/v1/schedules", e.admin, "").want(http.StatusOK)
	if items, _ := list.get("items").([]any); len(items) != 1 || items[0].(map[string]any)["upcoming"] != nil {
		t.Errorf("list = %v", list.body)
	}

	patched := e.call("PATCH", "/v1/schedules/"+id, operator, `{"trigger": {"kind": "fixed_rate", "interval": "1h"}, "max_runs": 3, "priority": "HIGH"}`).
		want(http.StatusOK)
	if patched.str("trigger.kind") != "fixed_rate" || patched.str("trigger.interval") != "1h0m0s" ||
		patched.get("max_runs") != float64(3) || patched.str("priority") != "HIGH" || patched.str("payload.report") != "sales" {
		t.Errorf("patched = %v", patched.body)
	}
	cleared := e.call("PATCH", "/v1/schedules/"+id, operator, `{"max_runs": null, "priority": null}`).want(http.StatusOK)
	if cleared.get("max_runs") != nil || cleared.get("priority") != nil {
		t.Errorf("null did not clear: %v", cleared.body)
	}
	e.call("PATCH", "/v1/schedules/"+id, operator, `{"job_type": "other"}`).wantError(http.StatusUnprocessableEntity, "invalid_argument")

	if s := e.call("POST", "/v1/schedules/"+id+"/pause", operator, "").want(http.StatusOK).str("state"); s != "PAUSED" {
		t.Errorf("pause -> %s", s)
	}
	e.call("POST", "/v1/schedules/"+id+"/pause", operator, "").wantError(http.StatusConflict, "conflict")
	if s := e.call("POST", "/v1/schedules/"+id+"/resume", operator, "").want(http.StatusOK).str("state"); s != "ACTIVE" {
		t.Errorf("resume -> %s", s)
	}

	_, otherAdmin := e.newTenant("globex")
	e.call("GET", "/v1/schedules/"+id, otherAdmin, "").wantError(http.StatusNotFound, "not_found")
	e.call("DELETE", "/v1/schedules/"+id, otherAdmin, "").wantError(http.StatusNotFound, "not_found")

	e.call("DELETE", "/v1/schedules/"+id, operator, "").want(http.StatusNoContent)
	e.call("GET", "/v1/schedules/"+id, operator, "").wantError(http.StatusNotFound, "not_found")

	var audits int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE action LIKE 'schedule.%'`).Scan(&audits); err != nil || audits != 6 {
		t.Errorf("schedule audit rows = %d (%v), want 6", audits, err)
	}
}

func TestListJobsBySchedule(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	id := e.call("POST", "/v1/schedules", e.admin,
		`{"name": "digest", "job_type": "email.send", "trigger": {"kind": "fixed_rate", "interval": "1m"}}`).want(http.StatusCreated).str("id")
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated)
	if _, err := e.store.MaterializeDue(ctx, domain.DefaultPlanLimits, 10); err != nil {
		t.Fatal(err)
	}
	items, _ := e.call("GET", "/v1/jobs?schedule_id="+id, e.admin, "").want(http.StatusOK).get("items").([]any)
	if len(items) != 3 {
		t.Fatalf("%d jobs for the schedule, want 3 (2-minute lookahead)", len(items))
	}
	for _, it := range items {
		j := it.(map[string]any)
		if j["schedule_id"] != id || j["fire_time"] == nil || j["created_by"] != "schedule:"+id || j["state"] != "SCHEDULED" {
			t.Errorf("schedule job = %v", j)
		}
	}
	e.call("GET", "/v1/jobs?schedule_id=nope", e.admin, "").wantError(http.StatusUnprocessableEntity, "invalid_argument")
}
