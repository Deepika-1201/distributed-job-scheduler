package observability_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"jobscheduler/internal/observability"
	"jobscheduler/internal/observability/telemetrytest"
)

// Instruments are package variables created before any provider exists; they must still
// report through the provider installed later.
func TestInstrumentsBindToTheLaterProvider(t *testing.T) {
	metrics, _ := telemetrytest.Install()
	run := strconv.FormatInt(time.Now().UnixNano(), 36) // counters are cumulative across -count runs
	observability.JobsSubmitted.Add(context.Background(), 3, metric.WithAttributes(attribute.String("tenant", "t-"+run)))
	observability.SchedulingLag.Record(context.Background(), 0.2, metric.WithAttributes(attribute.String("pool", "p-"+run)))

	text := telemetrytest.Scrape(t, metrics)
	if v, ok := telemetrytest.Value(text, "jobs_submitted_total", map[string]string{"tenant": "t-" + run}); !ok || v != 3 {
		t.Errorf("jobs_submitted_total = %v (found %v), want 3", v, ok)
	}
	if v, ok := telemetrytest.Value(text, "scheduling_lag_seconds_count", map[string]string{"pool": "p-" + run}); !ok || v != 1 {
		t.Errorf("scheduling_lag_seconds_count = %v (found %v), want 1", v, ok)
	}
}

// Gauge callbacks are registered after the provider is installed, on instruments created
// before it: registration must accept them.
func TestGaugeCallbacksRegisterAfterTheProvider(t *testing.T) {
	metrics, _ := telemetrytest.Install()
	reg, err := observability.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveFloat64(observability.DBClockOffset, 0.25)
		return nil
	}, observability.DBClockOffset)
	if err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}
	defer func() { _ = reg.Unregister() }()
	if v, ok := telemetrytest.Value(telemetrytest.Scrape(t, metrics), "db_clock_offset_seconds", nil); !ok || v != 0.25 {
		t.Errorf("db_clock_offset_seconds = %v (found %v), want 0.25", v, ok)
	}
}

func TestTraceParentRoundTrip(t *testing.T) {
	telemetrytest.Install()
	ctx, span := observability.Tracer().Start(context.Background(), "submit")
	tp := observability.TraceParent(ctx)
	span.End()
	links := observability.LinkTo(tp)
	if tp == "" || len(links) != 1 || links[0].SpanContext.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("traceparent %q gave links %v", tp, links)
	}
	if observability.LinkTo("garbage") != nil || observability.LinkTo("") != nil {
		t.Error("invalid traceparents produced links")
	}
	if observability.TraceAttrs(context.Background()) != nil || len(observability.TraceAttrs(ctx)) != 4 {
		t.Error("trace log attributes wrong")
	}
}
