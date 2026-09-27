package dispatch_test

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/observability"
	"jobscheduler/internal/observability/telemetrytest"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/pkg/workersdk"
)

// TestJobIsTracedAcrossTheQueue follows one job from its submission span to the worker's
// execution span and the engine's completion (LLD §17.5). A pool of its own keeps the
// pool-labeled series apart from parallel tests.
func TestJobIsTracedAcrossTheQueue(t *testing.T) {
	t.Parallel()
	metrics, spans := telemetrytest.Install()
	c := newCluster(t)
	if _, err := c.store.CreateJobType(ctx, domain.JobType{TenantID: c.tenant, Name: "traced.job", Pool: "traced",
		DefaultPriority: domain.PriorityNormal, AttemptTimeout: time.Minute, RetryPolicy: domain.DefaultRetryPolicy(),
		Enabled: true}, postgres.Audit{Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	addr := c.engine("node-a", func(cfg *dispatch.Config) { cfg.MetricsInterval = 50 * time.Millisecond })
	before := telemetrytest.Scrape(t, metrics) // counters are cumulative across -count runs

	subCtx, submission := otel.Tracer("test").Start(ctx, "submit")
	id := c.submit(func(nj *postgres.NewJob) {
		nj.Type, nj.Pool, nj.TraceParent = "traced.job", "traced", observability.TraceParent(subCtx)
	})
	submission.End()

	release := make(chan struct{})
	c.run(func(ctx context.Context) error {
		return workersdk.Run(ctx, workersdk.Config{Address: addr, Token: token, Pool: "traced", Slots: 2,
			PollWait: time.Second, DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler),
			Handlers: map[string]workersdk.Handler{"traced.job": func(ctx context.Context, _ workersdk.Job) ([]byte, error) {
				_, work := otel.Tracer("handler").Start(ctx, "handler work")
				work.End()
				<-release
				return nil, nil
			}}})
	})

	pool := map[string]string{"pool": "traced"}
	running := map[string]string{"pool": "traced", "tenant": string(c.tenant)}
	busy := map[string]string{"pool": "traced", "state": "busy"}
	text := eventually(t, metrics, func(text string) bool {
		n, _ := telemetrytest.Value(text, "jobs_running", running)
		return n == 1
	})
	if n, _ := telemetrytest.Value(text, "worker_slots", busy); n != 1 {
		t.Errorf("worker_slots%v = %v, want 1", busy, n)
	}
	if n, ok := telemetrytest.Value(text, "jobs_ready", pool); !ok || n != 0 {
		t.Errorf("jobs_ready%v = %v (reported %v), want 0 once claimed", pool, n, ok)
	}
	close(release)
	c.await(id, domain.StateSucceeded, 10*time.Second)

	var execute sdktrace.ReadOnlySpan
	eventuallySpan(t, func() bool {
		for _, s := range spans.Ended() {
			if s.Name() == "execute traced.job" && hasAttr(s.Attributes(), attribute.String("job.id", string(id))) {
				execute = s
				return true
			}
		}
		return false
	})
	if links := execute.Links(); len(links) != 1 || links[0].SpanContext.SpanID() != submission.SpanContext().SpanID() {
		t.Errorf("execute span links = %+v, want the submission span", links)
	}
	if execute.SpanContext().TraceID() == submission.SpanContext().TraceID() {
		t.Error("execute span shares the submission's trace; want a new root linked to it")
	}
	found := map[string]bool{}
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID() != execute.SpanContext().TraceID() {
			if strings.HasSuffix(s.Name(), "/Poll") || strings.HasSuffix(s.Name(), "/Heartbeat") {
				t.Errorf("span %q: polls and heartbeats should not be traced", s.Name())
			}
			continue
		}
		switch {
		case s.Name() == "handler work":
			found["handler"] = s.Parent().SpanID() == execute.SpanContext().SpanID()
		case strings.HasSuffix(s.Name(), "/Complete") && s.SpanKind() == trace.SpanKindServer:
			found["engine"] = true
		case s.Name() == "db.tx CompleteAttempt":
			found["transaction"] = true
		}
	}
	for _, want := range []string{"handler", "engine", "transaction"} {
		if !found[want] {
			t.Errorf("no %s span in the attempt's trace (found %v)", want, found)
		}
	}

	text = telemetrytest.Scrape(t, metrics)
	for name, labels := range map[string]map[string]string{
		"dispatch_latency_seconds_count": {"pool": "traced", "priority": "NORMAL"},
		"attempts_total":                 {"type": "traced.job", "outcome": "SUCCEEDED"},
		"jobs_completed_total":           {"type": "traced.job", "state": "SUCCEEDED"},
	} {
		n, _ := telemetrytest.Value(text, name, labels)
		prev, _ := telemetrytest.Value(before, name, labels)
		if n-prev != 1 {
			t.Errorf("%s%v rose by %v, want 1", name, labels, n-prev)
		}
	}
	if n, _ := telemetrytest.Value(text, "pool_owner_changes_total", pool); n < 1 {
		t.Errorf("pool_owner_changes_total%v = %v, want at least 1", pool, n)
	}
	for _, name := range []string{"pools_unowned", "db_clock_offset_seconds"} {
		if _, ok := telemetrytest.Value(text, name, nil); !ok {
			t.Errorf("%s is not reported", name)
		}
	}
	if !strings.Contains(text, `rpc_method="jobscheduler.worker.v1.WorkerService/Complete"`) ||
		strings.Contains(text, `WorkerService/Poll"`) || strings.Contains(text, `WorkerService/Heartbeat"`) {
		t.Error("want RPC metrics for Complete and none for polls or heartbeats")
	}
}

// eventually scrapes until ok accepts the exposition, and returns it.
func eventually(t *testing.T, metrics http.Handler, ok func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		text := telemetrytest.Scrape(t, metrics)
		if ok(text) {
			return text
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met; last scrape:\n%s", text)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func eventuallySpan(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("span not recorded")
		}
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
