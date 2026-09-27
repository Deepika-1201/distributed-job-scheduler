package api

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"

	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability/telemetrytest"
)

// clientTrace returns a new sampled traceparent, as a caller would send, and its trace ID.
func clientTrace() (traceParent, traceID string) {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	traceID = hex.EncodeToString(b[:16])
	return "00-" + traceID + "-" + hex.EncodeToString(b[16:]) + "-01", traceID
}

func TestSubmissionMetrics(t *testing.T) {
	t.Parallel()
	metrics, _ := telemetrytest.Install()
	e := newEnv(t, 1000)
	e.noCache()
	platform, _ := e.newTenant("platform")
	root := e.key(platform, domain.RolePlatformAdmin)
	e.call("PUT", "/v1/tenants/"+string(e.tenant)+"/quotas", root, `{"max_pending": 4, "max_payload_bytes": 64}`).
		want(http.StatusOK)

	big := `{"type": "email.send", "payload": {"text": "` + strings.Repeat("x", 100) + `"}}`
	e.call("POST", "/v1/jobs", e.admin, big).wantError(http.StatusRequestEntityTooLarge, "payload_too_large")
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send", "priority": "HIGH", "delay": "1h"}`).want(http.StatusCreated)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`, "Idempotency-Key", "k1").want(http.StatusCreated)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`, "Idempotency-Key", "k1").want(http.StatusOK)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).want(http.StatusCreated)
	e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`).wantError(http.StatusTooManyRequests, "quota_exceeded")
	e.call("POST", "/v1/jobs", e.admin, `{"type": "no.such.type"}`).want(http.StatusUnprocessableEntity)

	text := telemetrytest.Scrape(t, metrics)
	tenant := string(e.tenant)
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"jobs_submitted_total", map[string]string{"tenant": tenant, "type": "email.send"}, 4}, // not the replay
		{"jobs_submitted_total", map[string]string{"tenant": tenant, "priority": "HIGH"}, 1},
		{"jobs_scheduled_total", map[string]string{"tenant": tenant, "source": "delayed"}, 1},
		{"jobs_rejected_total", map[string]string{"tenant": tenant, "reason": "payload_too_large"}, 1},
		{"jobs_rejected_total", map[string]string{"tenant": tenant, "reason": "quota_exceeded"}, 1},
		{"jobs_rejected_total", map[string]string{"tenant": tenant}, 2}, // validation errors are not admission
	} {
		if got, _ := telemetrytest.Value(text, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	route := map[string]string{"http_route": "/v1/jobs", "http_request_method": "POST", "server_address": "api"}
	if n, _ := telemetrytest.Value(text, "http_server_request_duration_seconds_count", route); n < 8 {
		t.Errorf("HTTP request count for %v = %v, want at least 8", route, n)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSubmissionIsTraced(t *testing.T) {
	t.Parallel()
	_, spans := telemetrytest.Install()
	e := newEnv(t, 1000)
	logs := &syncBuffer{}
	traced := httptest.NewServer(New(e.store, slog.New(slog.NewJSONHandler(logs, nil)), Config{TenantRateLimit: 1000}).Handler())
	t.Cleanup(traced.Close)
	e.srv = traced

	clientParent, clientTraceID := clientTrace()
	job := e.call("POST", "/v1/jobs", e.admin, `{"type": "email.send"}`, "traceparent", clientParent).
		want(http.StatusCreated)

	var stored string
	if err := e.pool.QueryRow(ctx, `SELECT trace_parent FROM jobs WHERE id = $1`, job.str("id")).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stored, "00-"+clientTraceID+"-") || stored == clientParent {
		t.Errorf("stored traceparent = %q, want the server span in the client's trace", stored)
	}

	var server, tx bool
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID().String() != clientTraceID {
			continue
		}
		switch s.Name() {
		case "POST /v1/jobs":
			server = true
			if !hasAttr(s.Attributes(), attribute.String("http.route", "/v1/jobs")) {
				t.Errorf("server span attributes = %v, want http.route", s.Attributes())
			}
			if "00-"+clientTraceID+"-"+s.SpanContext().SpanID().String()+"-01" != stored {
				t.Errorf("stored traceparent %q is not the server span %s", stored, s.SpanContext().SpanID())
			}
		case "db.tx SubmitJob":
			tx = true
		}
	}
	if !server || !tx {
		t.Errorf("spans in the client's trace: server %v, transaction %v; want both", server, tx)
	}
	if !strings.Contains(logs.String(), `"trace_id":"`+clientTraceID+`"`) {
		t.Errorf("access log lacks the trace ID:\n%s", logs)
	}
}

func hasAttr(attrs []attribute.KeyValue, want attribute.KeyValue) bool {
	for _, a := range attrs {
		if a == want {
			return true
		}
	}
	return false
}
