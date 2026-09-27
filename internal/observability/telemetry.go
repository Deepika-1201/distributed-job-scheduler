package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TelemetryConfig configures metric and trace export (LLD §17.1).
type TelemetryConfig struct {
	ServiceVersion string
	InstanceID     string
	OTLPEndpoint   string // host:port of an OTLP/gRPC collector; empty disables trace export
	OTLPInsecure   bool
	SampleRatio    float64 // for root spans; children follow their parent
	// Logger receives export errors, which the SDK otherwise prints unstructured.
	Logger *slog.Logger
}

// Telemetry holds the installed providers.
type Telemetry struct {
	Metrics  http.Handler // Prometheus exposition for the ops server
	shutdown []func(context.Context) error
}

// SetupTelemetry installs the global meter provider, and a tracer provider when an OTLP
// endpoint is configured (ADR-020).
func SetupTelemetry(ctx context.Context, cfg TelemetryConfig) (*Telemetry, error) {
	if cfg.Logger != nil {
		log := cfg.Logger.With("component", "telemetry")
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { log.Warn("telemetry error", "error", err) }))
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", "jobscheduler"),
		attribute.String("service.version", cfg.ServiceVersion),
		attribute.String("service.instance.id", cfg.InstanceID)))
	if err != nil {
		return nil, err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t := &Telemetry{Metrics: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), shutdown: []func(context.Context) error{mp.Shutdown}}

	if cfg.OTLPEndpoint != "" {
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		spans, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, err
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(spans), sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))))
		otel.SetTracerProvider(tp)
		t.shutdown = append(t.shutdown, tp.Shutdown)
	}
	return t, nil
}

// Shutdown flushes buffered spans and stops the providers.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range t.shutdown {
		errs = append(errs, fn(ctx))
	}
	return errors.Join(errs...)
}

// Tracer is the platform's tracer.
func Tracer() trace.Tracer { return otel.Tracer("jobscheduler") }

// TraceParent returns the W3C traceparent of ctx's span, or "" if it has none.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"]
}

// LinkTo returns span links to a stored traceparent; none if it is empty or invalid.
func LinkTo(traceParent string) []trace.Link {
	if traceParent == "" {
		return nil
	}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": traceParent}))
	if !sc.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: sc}}
}

// TraceAttrs returns trace_id and span_id log attributes for ctx's span, if any.
func TraceAttrs(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{"trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String()}
}

// RecordSpan records a span after the fact, e.g. for a batch that turned out to do work.
func RecordSpan(ctx context.Context, name string, start time.Time, err error, attrs ...attribute.KeyValue) {
	_, span := Tracer().Start(ctx, name, trace.WithTimestamp(start), trace.WithAttributes(attrs...))
	if err != nil {
		span.RecordError(err)
	}
	span.End()
}
