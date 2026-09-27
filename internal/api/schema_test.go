package api

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/persistence/postgres"
)

const emailSchema = `{"type": "object", "required": ["to"], "properties": {"to": {"type": "string"}},
	"additionalProperties": false}`

func TestPayloadSchemaIsEnforced(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	created := e.call("POST", "/v1/job-types", e.admin, `{"name": "mail.typed", "payload_schema": `+emailSchema+`}`).
		want(http.StatusCreated)
	if created.get("payload_schema") == nil {
		t.Fatalf("schema not returned: %v", created.body)
	}
	e.call("POST", "/v1/jobs", e.admin, `{"type": "mail.typed", "payload": {"to": "a@example.com"}}`).want(http.StatusCreated)
	bad := e.call("POST", "/v1/jobs", e.admin, `{"type": "mail.typed", "payload": {"cc": "b@example.com"}}`).
		wantError(http.StatusUnprocessableEntity, "invalid_argument")
	if msg, _ := bad.fieldError("payload").(string); !strings.Contains(msg, "to") {
		t.Errorf("violation message = %q", msg)
	}
	e.call("POST", "/v1/schedules", e.admin, `{"name": "digest", "job_type": "mail.typed", "payload": {"cc": 1},
		"trigger": {"kind": "fixed_rate", "interval": "1h"}}`).wantError(http.StatusUnprocessableEntity, "invalid_argument")

	// An update takes effect at once, and null removes the schema.
	e.call("PATCH", "/v1/job-types/mail.typed", e.admin, `{"payload_schema": {"type": "object", "required": ["cc"]}}`).want(http.StatusOK)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "mail.typed", "payload": {"cc": "b@example.com"}}`).want(http.StatusCreated)
	if cleared := e.call("PATCH", "/v1/job-types/mail.typed", e.admin, `{"payload_schema": null}`).want(http.StatusOK); cleared.get("payload_schema") != nil {
		t.Errorf("null did not remove the schema: %v", cleared.body)
	}
	e.call("POST", "/v1/jobs", e.admin, `{"type": "mail.typed", "payload": 42}`).want(http.StatusCreated)
}

func TestPayloadSchemaRejectsInvalidAndExternalSchemas(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	secret := filepath.Join(t.TempDir(), "secret.json")
	if err := os.WriteFile(secret, []byte(`{"type": "object"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, schema := range map[string]string{
		"invalid":        `{"type": "strng"}`,
		"local file ref": `{"$ref": "` + (&url.URL{Scheme: "file", Path: secret}).String() + `"}`,
		"remote ref":     `{"$ref": "https://example.com/schema.json"}`,
	} {
		r := e.call("POST", "/v1/job-types", e.admin, `{"name": "t.`+strings.ReplaceAll(name, " ", "-")+`", "payload_schema": `+schema+`}`).
			wantError(http.StatusUnprocessableEntity, "invalid_argument")
		if r.fieldError("payload_schema") == nil {
			t.Errorf("%s: no payload_schema error: %v", name, r.body)
		}
	}
	// Same-document references still work.
	e.call("POST", "/v1/job-types", e.admin, `{"name": "t.defs", "payload_schema":
		{"$defs": {"addr": {"type": "string"}}, "type": "object", "properties": {"to": {"$ref": "#/$defs/addr"}}}}`).
		want(http.StatusCreated)
}

func TestListJobsByCreationTime(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	old := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated).str("id")
	if _, err := e.pool.Exec(ctx, `UPDATE jobs SET created_at = now() - interval '2 hours' WHERE id = $1`, old); err != nil {
		t.Fatal(err)
	}
	recent := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated).str("id")
	hourAgo := url.QueryEscape(time.Now().Add(-time.Hour).Format(time.RFC3339))
	ids := func(query string) []string {
		items, _ := e.call("GET", "/v1/jobs?"+query, e.admin, "").want(http.StatusOK).get("items").([]any)
		var out []string
		for _, it := range items {
			out = append(out, it.(map[string]any)["id"].(string))
		}
		return out
	}
	if got := ids("created_after=" + hourAgo); len(got) != 1 || got[0] != recent {
		t.Errorf("created_after = %v, want [%s]", got, recent)
	}
	if got := ids("created_before=" + hourAgo); len(got) != 1 || got[0] != old {
		t.Errorf("created_before = %v, want [%s]", got, old)
	}
	e.call("GET", "/v1/jobs?created_after=yesterday", e.admin, "").wantError(http.StatusUnprocessableEntity, "invalid_argument")
}

func TestAttemptsNameTheirWorker(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	id := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated).str("id")
	ws, err := e.store.CreateSession(ctx, domain.WorkerSession{Pool: "default", WorkerID: "host-1/42", Slots: 1}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	lease, _, err := e.store.AcquireLease(ctx, postgres.PoolLeaseName("default"), "test-node", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.ClaimReady(ctx, postgres.ClaimRequest{Lease: lease, Pool: "default", Priority: domain.PriorityNormal,
		Limit: 1, SessionID: ws.ID}); err != nil {
		t.Fatal(err)
	}
	attempts, _ := e.call("GET", "/v1/jobs/"+id+"/attempts", e.admin, "").want(http.StatusOK).get("items").([]any)
	if len(attempts) != 1 || attempts[0].(map[string]any)["worker_id"] != "host-1/42" ||
		attempts[0].(map[string]any)["session_id"] != string(ws.ID) {
		t.Errorf("attempts = %v", attempts)
	}
}
