// Command gate judges the load-test gate (LLD §19.2). It scrapes every node's /metrics when the
// measured part of a scenario starts and again when it ends, and checks the changes in between.
// The burst scenario checks the accepted rate, dispatch latency (NFR-4) and whether dispatch
// kept up with submissions. The cron scenario checks that every schedule fired at each minute
// boundary, scheduling lag (NFR-3) and dispatch (LLD §23). Both report the CPU each component
// spent per job, to project the capacity a rate needs.
package main

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

func main() {
	urls := flag.String("metrics", "node=http://localhost:9090/metrics", "comma-separated name=URL of every node's /metrics")
	dbCPU := flag.String("db-cpu", "", "shell command printing the database's CPU seconds so far (optional)")
	wait := flag.Duration("wait", 37*time.Second, "time from start until the burst's steady part")
	window := flag.Duration("window", 50*time.Second, "length of the measured part of the burst")
	rate := flag.Float64("rate", 5000, "offered burst rate in jobs/s")
	scenario := flag.String("scenario", "burst", "burst: submissions at -rate; cron: -schedules schedules firing every minute")
	schedules := flag.Int("schedules", 0, "for cron, how many schedules fire at each minute boundary")
	flag.Parse()

	time.Sleep(*wait)
	start := time.Now()
	before, dbBefore, err := snapshot(strings.Split(*urls, ","), *dbCPU)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gate:", err)
		os.Exit(2)
	}
	time.Sleep(*window)
	after, dbAfter, err := snapshot(strings.Split(*urls, ","), *dbCPU)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gate:", err)
		os.Exit(2)
	}
	r := measure(before, after, *window, *rate)
	if *scenario == "cron" {
		r.schedules, r.boundaries = *schedules, minuteBoundaries(start, time.Now())
	}
	if *dbCPU != "" {
		r.cpu["database"] = dbAfter - dbBefore
	}
	fmt.Print(r.text())
	if !r.passed() {
		os.Exit(1)
	}
}

// snapshot scrapes every node and, given a command, reads the database's CPU seconds.
func snapshot(nodes []string, dbCPU string) ([]series, float64, error) {
	s, err := scrapeAll(nodes)
	if err != nil || dbCPU == "" {
		return s, 0, err
	}
	out, err := exec.Command("sh", "-c", dbCPU).Output()
	if err != nil {
		return nil, 0, fmt.Errorf("database CPU: %w", err)
	}
	cpu, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return s, cpu, err
}

type series struct {
	name   string
	labels map[string]string
	value  float64
}

// scrapeAll scrapes each name=URL node, labelling its series node=name.
func scrapeAll(nodes []string) ([]series, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	var all []series
	for _, node := range nodes {
		name, url, ok := strings.Cut(node, "=")
		if !ok {
			name, url = node, node
		}
		s, err := scrape(client, url)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", url, err)
		}
		for i := range s {
			s[i].labels["node"] = name
		}
		all = append(all, s...)
	}
	return all, nil
}

func scrape(client *http.Client, url string) ([]series, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}
	return parse(resp.Body)
}

// parse reads the Prometheus text exposition format.
func parse(r io.Reader) ([]series, error) {
	var out []series
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", line, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

func parseLine(line string) (series, error) {
	s := series{labels: map[string]string{}}
	i := strings.IndexAny(line, "{ ")
	if i <= 0 {
		return s, errors.New("no metric name")
	}
	s.name, line = line[:i], line[i:]
	if line[0] == '{' {
		line = line[1:]
		for {
			line = strings.TrimLeft(line, ", ")
			if strings.HasPrefix(line, "}") {
				line = line[1:]
				break
			}
			eq := strings.Index(line, `="`)
			if eq <= 0 {
				return s, errors.New("malformed label")
			}
			name := line[:eq]
			value, rest, err := unquote(line[eq+2:])
			if err != nil {
				return s, err
			}
			s.labels[name], line = value, rest
		}
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return s, errors.New("no value")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	s.value = v
	return s, err
}

// unquote reads a label value up to its closing quote, undoing the format's escapes.
func unquote(s string) (value, rest string, err error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			return b.String(), s[i+1:], nil
		case '\\':
			if i++; i == len(s) {
				return "", "", errors.New("unterminated escape")
			}
			if s[i] == 'n' {
				b.WriteByte('\n')
			} else {
				b.WriteByte(s[i])
			}
		default:
			b.WriteByte(c)
		}
	}
	return "", "", errors.New("unterminated label value")
}

// change sums how much a series rose between two scrapes of every node, grouped by the
// values of the given labels (joined with "|"); with no labels, everything is one group.
func change(before, after []series, name string, by ...string) map[string]float64 {
	key := func(s series) string {
		vals := make([]string, len(by))
		for i, l := range by {
			vals[i] = s.labels[l]
		}
		return strings.Join(vals, "|")
	}
	out := map[string]float64{}
	for _, s := range after {
		if s.name == name {
			out[key(s)] += s.value
		}
	}
	for _, s := range before {
		if s.name == name {
			out[key(s)] -= s.value
		}
	}
	return out
}

type bucket struct{ le, n float64 }

// buckets turns cumulative counts keyed by their upper bound into a sorted list.
func buckets(byBound map[string]float64) []bucket {
	var out []bucket
	for le, n := range byBound {
		if f, err := strconv.ParseFloat(le, 64); err == nil {
			out = append(out, bucket{f, n})
		}
	}
	slices.SortFunc(out, func(a, b bucket) int { return cmp.Compare(a.le, b.le) })
	return out
}

// quantile estimates the q-quantile from cumulative buckets, interpolating linearly within a
// bucket as PromQL's histogram_quantile does. It is NaN without observations.
func quantile(q float64, bs []bucket) float64 {
	if len(bs) == 0 || bs[len(bs)-1].n <= 0 {
		return math.NaN()
	}
	rank := q * bs[len(bs)-1].n
	lower, below := 0.0, 0.0
	for _, b := range bs {
		if b.n >= rank && b.n > below {
			if math.IsInf(b.le, 1) {
				return lower
			}
			return lower + (b.le-lower)*(rank-below)/(b.n-below)
		}
		lower, below = b.le, b.n
	}
	return lower
}

// atMost is the exact share of observations no greater than bound, which must be a bucket
// bound. It is NaN without observations.
func atMost(bound float64, bs []bucket) float64 {
	if len(bs) == 0 || bs[len(bs)-1].n <= 0 {
		return math.NaN()
	}
	for _, b := range bs {
		if b.le == bound {
			return b.n / bs[len(bs)-1].n
		}
	}
	return math.NaN()
}

type result struct {
	window     time.Duration
	offered    float64 // jobs/s
	accepted   float64 // jobs submitted through the API in the window
	dispatched float64
	promoted   float64 // jobs that became READY when due, each with its scheduling lag
	latency    []bucket
	lag        []bucket
	wait       []bucket
	completed  map[string]float64 // by state
	txCount    map[string]float64 // by operation
	txBuckets  map[string][]bucket
	cpu        map[string]float64 // CPU seconds by node, and "database" when measured
	schedules  int                // cron scenario: schedules firing at each minute boundary
	boundaries int                // cron scenario: minute boundaries in the window
}

// minuteBoundaries counts the minute boundaries in (from, to].
func minuteBoundaries(from, to time.Time) int {
	return int(to.Truncate(time.Minute).Sub(from.Truncate(time.Minute)) / time.Minute)
}

func measure(before, after []series, window time.Duration, offered float64) result {
	r := result{window: window, offered: offered,
		accepted:   change(before, after, "jobs_submitted_total")[""],
		dispatched: change(before, after, "dispatch_latency_seconds_count")[""],
		promoted:   change(before, after, "scheduling_lag_seconds_count")[""],
		latency:    buckets(change(before, after, "dispatch_latency_seconds_bucket", "le")),
		lag:        buckets(change(before, after, "scheduling_lag_seconds_bucket", "le")),
		wait:       buckets(change(before, after, "queue_wait_seconds_bucket", "le")),
		completed:  change(before, after, "jobs_completed_total", "state"),
		txCount:    change(before, after, "db_transaction_duration_seconds_count", "operation"),
		txBuckets:  map[string][]bucket{},
		cpu:        change(before, after, "process_cpu_seconds_total", "node"),
	}
	byOp := map[string]map[string]float64{}
	for k, n := range change(before, after, "db_transaction_duration_seconds_bucket", "operation", "le") {
		op, le, _ := strings.Cut(k, "|")
		if byOp[op] == nil {
			byOp[op] = map[string]float64{}
		}
		byOp[op][le] = n
	}
	for op, b := range byOp {
		r.txBuckets[op] = buckets(b)
	}
	return r
}

type check struct {
	name       string
	got, floor float64 // a share; the check passes at or above floor
	detail     string
}

func (r result) checks() []check {
	secs := r.window.Seconds()
	dispatch := check{"dispatch latency <= 1s (NFR-4)", atMost(1, r.latency), 0.99,
		fmt.Sprintf("p50 %s, p99 %s", seconds(quantile(0.5, r.latency)), seconds(quantile(0.99, r.latency)))}
	if r.cron() {
		return []check{
			{"every fire promoted", r.promoted / float64(r.schedules*r.boundaries), 0.99,
				fmt.Sprintf("%.0f of %d schedules x %d boundaries", r.promoted, r.schedules, r.boundaries)},
			{"scheduling lag <= 1s (NFR-3)", atMost(1, r.lag), 0.99,
				fmt.Sprintf("p50 %s, p99 %s", seconds(quantile(0.5, r.lag)), seconds(quantile(0.99, r.lag)))},
			dispatch,
			{"dispatch keeps up", r.dispatched / r.promoted, 0.95, fmt.Sprintf("%.0f dispatches", r.dispatched)},
		}
	}
	return []check{
		{"accepted rate", r.accepted / (r.offered * secs), 0.99,
			fmt.Sprintf("%.0f jobs/s accepted of %.0f offered", r.accepted/secs, r.offered)},
		dispatch,
		{"dispatch keeps up", r.dispatched / r.accepted, 0.95,
			fmt.Sprintf("%.0f dispatches/s", r.dispatched/secs)},
	}
}

func (r result) cron() bool { return r.schedules > 0 }

// jobs is how many jobs the scenario created in the window: submitted, or fired by schedules.
func (r result) jobs() float64 {
	if r.cron() {
		return r.promoted
	}
	return r.accepted
}

func (r result) passed() bool {
	for _, c := range r.checks() {
		if !(c.got >= c.floor) { // NaN fails
			return false
		}
	}
	return true
}

func (r result) text() string {
	var b strings.Builder
	if r.cron() {
		fmt.Fprintf(&b, "Load-test gate: %s with %d schedules firing every minute\n", r.window, r.schedules)
	} else {
		fmt.Fprintf(&b, "Load-test gate: %s of steady burst at %.0f jobs/s offered\n", r.window, r.offered)
	}
	for _, c := range r.checks() {
		verdict := "FAIL"
		if c.got >= c.floor {
			verdict = "PASS"
		}
		fmt.Fprintf(&b, "  %-4s %-31s %6.2f%% (needs %.0f%%)  %s\n", verdict, c.name, 100*c.got, 100*c.floor, c.detail)
	}
	fmt.Fprintf(&b, "  queue wait: p50 %s, p99 %s\n", seconds(quantile(0.5, r.wait)), seconds(quantile(0.99, r.wait)))
	var states []string
	for state, n := range r.completed {
		states = append(states, fmt.Sprintf("%s %.0f", state, n))
	}
	slices.Sort(states)
	fmt.Fprintf(&b, "  completed: %s\n", strings.Join(states, ", "))
	ops := make([]string, 0, len(r.txCount))
	for op := range r.txCount {
		ops = append(ops, op)
	}
	slices.SortFunc(ops, func(a, c string) int { return cmp.Compare(r.txCount[c], r.txCount[a]) })
	b.WriteString("  database transactions (per second, p99):\n")
	for _, op := range ops[:min(len(ops), 8)] {
		fmt.Fprintf(&b, "    %-28s %8.0f/s  %s\n", op, r.txCount[op]/r.window.Seconds(), seconds(quantile(0.99, r.txBuckets[op])))
	}
	if r.jobs() > 0 && len(r.cpu) > 0 {
		if r.cron() {
			b.WriteString("  CPU per job:\n")
		} else {
			fmt.Fprintf(&b, "  CPU per accepted job, and vCPUs needed at %.0f jobs/s:\n", r.offered)
		}
		nodes := make([]string, 0, len(r.cpu))
		for n := range r.cpu {
			nodes = append(nodes, n)
		}
		slices.Sort(nodes)
		for _, n := range nodes {
			per := r.cpu[n] / r.jobs()
			if r.cron() {
				fmt.Fprintf(&b, "    %-28s %8.3f ms\n", n, 1000*per)
			} else {
				fmt.Fprintf(&b, "    %-28s %8.3f ms  %5.2f vCPUs\n", n, 1000*per, per*r.offered)
			}
		}
	}
	verdict := "FAIL"
	if r.passed() {
		verdict = "PASS"
	}
	fmt.Fprintf(&b, "verdict: %s\n", verdict)
	return b.String()
}

func seconds(s float64) string {
	if math.IsNaN(s) {
		return "n/a"
	}
	return time.Duration(s * float64(time.Second)).Round(time.Millisecond).String()
}
