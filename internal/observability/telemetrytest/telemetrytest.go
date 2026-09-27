// Package telemetrytest installs in-memory telemetry for tests and reads it back.
package telemetrytest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	once    sync.Once
	handler http.Handler
	spans   *tracetest.SpanRecorder
)

// Install sets global providers that record every span and expose metrics, once per test
// binary: instruments bind to the first provider installed.
func Install() (metrics http.Handler, recorder *tracetest.SpanRecorder) {
	once.Do(func() {
		reg := prometheus.NewRegistry()
		exporter, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
		if err != nil {
			panic(err)
		}
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
		spans = tracetest.NewSpanRecorder()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans), sdktrace.WithSampler(sdktrace.AlwaysSample())))
		otel.SetTextMapPropagator(propagation.TraceContext{})
		handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	})
	return handler, spans
}

// Scrape returns the Prometheus exposition text.
func Scrape(t testing.TB, metrics http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return string(body)
}

// Value sums the samples of a series name whose labels include all of want.
func Value(text, name string, want map[string]string) (float64, bool) {
	var (
		sum   float64
		found bool
	)
	for line := range strings.SplitSeq(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sep := strings.LastIndexByte(line, ' ')
		series, value := line[:sep], line[sep+1:]
		metric, labels, _ := strings.Cut(series, "{")
		if metric != name || !hasLabels(strings.TrimSuffix(labels, "}"), want) {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err == nil {
			sum, found = sum+v, true
		}
	}
	return sum, found
}

func hasLabels(labels string, want map[string]string) bool {
	for k, v := range want {
		if !strings.Contains(","+labels+",", ","+k+`="`+v+`",`) {
			return false
		}
	}
	return true
}
