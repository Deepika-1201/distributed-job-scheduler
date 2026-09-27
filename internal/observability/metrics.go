package observability

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Instruments of HLD §17.3, declared once so names, units and labels stay consistent.
// They are created on the global provider and bind to it whenever SetupTelemetry runs.
var (
	JobsSubmitted    = counter("jobs_submitted", "{job}", "Jobs accepted by the API (tenant, type, priority)")
	JobsScheduled    = counter("jobs_scheduled", "{job}", "Jobs created for a future run (tenant, type, source)")
	JobsRejected     = counter("jobs_rejected", "{job}", "Submissions rejected by admission control (tenant, reason)")
	JobsCompleted    = counter("jobs_completed", "{job}", "Jobs that reached a terminal state (type, state)")
	Attempts         = counter("attempts", "{attempt}", "Ended attempts (type, outcome)")
	JobsRetried      = counter("jobs_retried", "{job}", "Retries scheduled (type, reason)")
	JobsDeadLettered = counter("jobs_dead_lettered", "{job}", "Jobs dead-lettered (type, reason)")
	SessionsExpired  = counter("worker_sessions_expired", "{session}", "Worker sessions expired by the reaper (pool)")
	StaleCompletions = counter("stale_completions_rejected", "{report}", "Reports from attempts that are no longer current (pool)")
	PoolOwnerChanges = counter("pool_owner_changes", "{change}", "Pool leases acquired by this node (pool)")

	SchedulingLag     = histogram("scheduling_lag", "s", "Due time to READY (pool)", latencyBuckets)
	DispatchLatency   = histogram("dispatch_latency", "s", "READY to attempt start (pool, priority)", latencyBuckets)
	ExecutionDuration = histogram("execution_duration", "s", "Attempt run time (type, outcome)", durationBuckets)
	DBTransaction     = histogram("db_transaction_duration", "s", "Database transaction time (operation)", dbBuckets)

	JobsReady      = gauge("jobs_ready", "{job}", "READY jobs, reported by the pool owner (pool, priority)")
	OldestReadyAge = floatGauge("jobs_oldest_ready_age", "s", "Age of the oldest READY job, reported by the pool owner (pool)")
	JobsRunning    = gauge("jobs_running", "{job}", "Running jobs, reported by the pool owner (pool, tenant)")
	WorkerSlots    = gauge("worker_slots", "{slot}", "Worker slots, reported by the pool owner (pool, state)")
	PoolsUnowned   = gauge("pools_unowned", "{pool}", "Pools with live workers but no valid lease")
	DBPoolInUse    = gauge("db_pool_in_use", "{connection}", "Database connections in use (role)")
	DBPoolMax      = gauge("db_pool_max", "{connection}", "Database connection limit (role)")
	DBClockOffset  = floatGauge("db_clock_offset", "s", "Database clock minus this node's clock")
)

var (
	latencyBuckets  = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
	durationBuckets = []float64{0.01, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600}
	dbBuckets       = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5}
)

// meter is obtained once, before any provider is installed, so it is the global delegating
// meter: callbacks must be registered on the meter that created their instruments, and
// otel.Meter after SetupTelemetry would return the SDK's own meter instead.
var meter = otel.Meter("jobscheduler")

// Meter is the platform's meter, for registering gauge callbacks.
func Meter() metric.Meter { return meter }

func counter(name, unit, desc string) metric.Int64Counter {
	return must(Meter().Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc)))
}

func histogram(name, unit, desc string, buckets []float64) metric.Float64Histogram {
	return must(Meter().Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(desc),
		metric.WithExplicitBucketBoundaries(buckets...)))
}

func gauge(name, unit, desc string) metric.Int64ObservableGauge {
	return must(Meter().Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc)))
}

func floatGauge(name, unit, desc string) metric.Float64ObservableGauge {
	return must(Meter().Float64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc)))
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
