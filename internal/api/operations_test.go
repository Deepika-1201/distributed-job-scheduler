package api

import (
	"net/http"
	"testing"

	"jobscheduler/internal/domain"
)

func TestBulkOperations(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1000)
	operator := e.key(e.tenant, domain.RoleOperator)
	for range 3 {
		e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "labels": {"batch": "7"}}`).want(http.StatusCreated)
	}
	keep := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated).str("id")

	e.call("POST", "/v1/operations", e.key(e.tenant, domain.RoleSubmitter), `{"kind": "cancel"}`).
		wantError(http.StatusForbidden, "permission_denied")
	for body, field := range map[string]string{
		`{"kind": "purge"}`:   "kind",
		`{"kind": "redrive"}`: "filter.state",
		`{"kind": "cancel", "filter": {"state": "SUCCEEDED"}}`: "filter.state",
		`{"kind": "cancel", "filter": {"label": "nocolon"}}`:   "filter.label",
	} {
		if r := e.call("POST", "/v1/operations", operator, body).wantError(http.StatusUnprocessableEntity, "invalid_argument"); r.fieldError(field) == nil {
			t.Errorf("%s: no error for %s: %v", body, field, r.body)
		}
	}

	created := e.call("POST", "/v1/operations", operator, `{"kind": "cancel", "filter": {"label": "batch:7"}}`).want(http.StatusAccepted)
	id := created.str("id")
	if created.str("state") != "PENDING" || created.header.Get("Location") != "/v1/operations/"+id || created.str("filter.label") != "batch:7" {
		t.Fatalf("created operation = %v", created.body)
	}
	for {
		found, err := e.store.ProcessOperation(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
	}
	done := e.call("GET", "/v1/operations/"+id, e.key(e.tenant, domain.RoleViewer), "").want(http.StatusOK)
	if done.str("state") != "SUCCEEDED" || done.get("succeeded") != float64(3) || done.str("finished_at") == "" {
		t.Errorf("finished operation = %v", done.body)
	}
	if s := e.call("GET", "/v1/jobs/"+keep, e.admin, "").str("state"); s != "READY" {
		t.Errorf("job outside the filter is %s", s)
	}
	_, otherAdmin := e.newTenant("globex")
	e.call("GET", "/v1/operations/"+id, otherAdmin, "").wantError(http.StatusNotFound, "not_found")
}
