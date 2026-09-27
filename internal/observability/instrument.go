package observability

import (
	"net"
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/stats"
)

// HTTPHandler measures and traces an HTTP server listening on addr. The server name and
// port are fixed from addr: otherwise they come from the client's Host header, an unbounded
// label set. Spans are named after the method until SetRoute names the route.
func HTTPHandler(h http.Handler, name, addr string) http.Handler {
	_, port, _ := net.SplitHostPort(addr)
	return otelhttp.NewHandler(h, name, otelhttp.WithServerName(net.JoinHostPort(name, port)),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method }))
}

// SetRoute names the request's span after its route pattern, e.g. "POST /v1/jobs", and adds
// http.route to the span and to the request's HTTP metrics.
func SetRoute(r *http.Request, pattern string) {
	route := pattern
	if _, path, ok := strings.Cut(pattern, " "); ok {
		route = path
	}
	span := trace.SpanFromContext(r.Context())
	span.SetName(pattern)
	span.SetAttributes(attribute.String("http.route", route))
	if l, ok := otelhttp.LabelerFromContext(r.Context()); ok {
		l.Add(attribute.String("http.route", route))
	}
}

// GRPCServerHandler measures and traces the worker protocol, except long polls and
// heartbeats: they are constant background traffic, and a poll's duration is its wait.
func GRPCServerHandler() stats.Handler {
	return otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.None(
		filters.MethodName("Poll"), filters.MethodName("Heartbeat"))))
}
