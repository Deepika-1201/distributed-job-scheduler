package api

import (
	"net/http"
	"strings"
	"testing"

	"jobscheduler/internal/domain"
)

func TestRotateAndListAPIKeys(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	old := e.call("POST", "/v1/api-keys", e.admin, `{"name": "ci", "role": "submitter"}`).want(http.StatusCreated)
	oldKey, oldID := old.str("key"), old.str("id")

	for body, field := range map[string]string{`{"grace": "721h"}`: "grace", `{"grace": "-1s"}`: "grace",
		`{"grace": "soon"}`: "grace", `{"expires_at": "2000-01-01T00:00:00Z"}`: "expires_at"} {
		if e.call("POST", "/v1/api-keys/"+oldID+"/rotate", e.admin, body).
			wantError(http.StatusUnprocessableEntity, "invalid_argument").fieldError(field) == nil {
			t.Errorf("%s: no error for %s", body, field)
		}
	}
	e.call("POST", "/v1/api-keys/0191f000-0000-7000-8000-000000000001/rotate", e.admin, "").wantError(http.StatusNotFound, "not_found")
	e.call("POST", "/v1/api-keys/"+oldID+"/rotate", e.key(e.tenant, domain.RoleOperator), "").
		wantError(http.StatusForbidden, "permission_denied")
	_, otherAdmin := e.newTenant("other")
	e.call("POST", "/v1/api-keys/"+oldID+"/rotate", otherAdmin, "").wantError(http.StatusNotFound, "not_found")

	next := e.call("POST", "/v1/api-keys/"+oldID+"/rotate", e.admin, `{"grace": "1h"}`).want(http.StatusCreated)
	if next.str("id") == oldID || next.str("name") != "ci" || next.str("role") != "submitter" || !strings.HasPrefix(next.str("key"), "jsk_") {
		t.Fatalf("replacement = %v", next.body)
	}
	e.call("GET", "/v1/jobs", oldKey, "").want(http.StatusOK) // still in its grace period
	e.call("GET", "/v1/jobs", next.str("key"), "").want(http.StatusOK)

	items, _ := e.call("GET", "/v1/api-keys", e.admin, "").want(http.StatusOK).get("items").([]any)
	seen := map[string]map[string]any{}
	for _, it := range items {
		k := it.(map[string]any)
		if _, leaked := k["key"]; leaked {
			t.Errorf("listing returned a secret: %v", k)
		}
		seen[k["id"].(string)] = k
	}
	if seen[oldID]["expires_at"] == nil || seen[next.str("id")] == nil {
		t.Errorf("listing = %v, want the old key with an expiry and its replacement", items)
	}

	// No grace: the replaced key stops working at once (it was never cached, so no 30 s window).
	unused := e.call("POST", "/v1/api-keys", e.admin, `{"name": "leaked", "role": "viewer"}`).want(http.StatusCreated)
	e.call("POST", "/v1/api-keys/"+unused.str("id")+"/rotate", e.admin, `{"grace": "0s"}`).want(http.StatusCreated)
	e.call("GET", "/v1/jobs", unused.str("key"), "").wantError(http.StatusUnauthorized, "unauthenticated")
}

func TestWorkerTokenEndpoints(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	e.call("POST", "/v1/pools/batch/worker-tokens", e.admin, `{"name": "batch fleet"}`).wantError(http.StatusForbidden, "permission_denied")
	e.call("GET", "/v1/pools/batch/worker-tokens", e.admin, "").wantError(http.StatusForbidden, "permission_denied")

	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	e.call("POST", "/v1/pools/Bad%20Name/worker-tokens", root, `{"name": "x"}`).wantError(http.StatusUnprocessableEntity, "invalid_argument")
	if e.call("POST", "/v1/pools/batch/worker-tokens", root, `{}`).
		wantError(http.StatusUnprocessableEntity, "invalid_argument").fieldError("name") == nil {
		t.Error("no error for a missing name")
	}

	created := e.call("POST", "/v1/pools/batch/worker-tokens", root, `{"name": "batch fleet"}`).want(http.StatusCreated)
	if created.str("pool") != "batch" || !strings.HasPrefix(created.str("token"), "jsw_") {
		t.Fatalf("created = %v", created.body)
	}
	items, _ := e.call("GET", "/v1/pools/batch/worker-tokens", root, "").want(http.StatusOK).get("items").([]any)
	if len(items) != 1 || items[0].(map[string]any)["token"] != nil || items[0].(map[string]any)["revoked_at"] != nil {
		t.Fatalf("tokens = %v, want one, unrevoked, without its secret", items)
	}
	if others, _ := e.call("GET", "/v1/pools/default/worker-tokens", root, "").want(http.StatusOK).get("items").([]any); len(others) != 0 {
		t.Errorf("another pool lists %v", others)
	}
	e.call("DELETE", "/v1/pools/default/worker-tokens/"+created.str("id"), root, "").wantError(http.StatusNotFound, "not_found")
	e.call("DELETE", "/v1/pools/batch/worker-tokens/"+created.str("id"), root, "").want(http.StatusNoContent)
	items, _ = e.call("GET", "/v1/pools/batch/worker-tokens", root, "").want(http.StatusOK).get("items").([]any)
	if len(items) != 1 || items[0].(map[string]any)["revoked_at"] == nil {
		t.Errorf("tokens after revoking = %v", items)
	}
}
