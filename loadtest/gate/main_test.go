package main

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mustParse(t *testing.T, text string) []series {
	t.Helper()
	s, err := parse(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParseReadsNamesLabelsAndValues(t *testing.T) {
	got := mustParse(t, `# HELP x help
# TYPE x counter
plain 3
labeled{pool="load-1",le="+Inf"} 7 1700000000000
escaped{msg="a \"quoted\" \\ value\nnext"} 1.5e3
`)
	if len(got) != 3 {
		t.Fatalf("got %d series, want 3", len(got))
	}
	if got[0].name != "plain" || got[0].value != 3 || len(got[0].labels) != 0 {
		t.Errorf("plain = %+v", got[0])
	}
	if got[1].labels["pool"] != "load-1" || got[1].labels["le"] != "+Inf" || got[1].value != 7 {
		t.Errorf("labeled = %+v", got[1])
	}
	if want := "a \"quoted\" \\ value\nnext"; got[2].labels["msg"] != want || got[2].value != 1500 {
		t.Errorf("escaped = %+v, want label %q", got[2], want)
	}
	for _, bad := range []string{`{a="b"} 1`, `x{a="b} 1`, `x{a=b} 1`, `x`, `x one`} {
		if _, err := parse(strings.NewReader(bad)); err == nil {
			t.Errorf("parse(%q) succeeded, want an error", bad)
		}
	}
}

func TestChangeSumsNodesAndGroupsByLabels(t *testing.T) {
	// Two nodes, scraped twice. The second node's series for priority HIGH appears only later.
	before := mustParse(t, `
dispatched{node="a",priority="NORMAL"} 10
dispatched{node="b",priority="NORMAL"} 5
`)
	after := mustParse(t, `
dispatched{node="a",priority="NORMAL"} 30
dispatched{node="b",priority="NORMAL"} 15
dispatched{node="b",priority="HIGH"} 4
other 99
`)
	if got := change(before, after, "dispatched")[""]; got != 34 {
		t.Errorf("total change = %v, want 34", got)
	}
	byPriority := change(before, after, "dispatched", "priority")
	if byPriority["NORMAL"] != 30 || byPriority["HIGH"] != 4 {
		t.Errorf("by priority = %v, want NORMAL 30, HIGH 4", byPriority)
	}
}

func TestQuantileInterpolatesLikePromQL(t *testing.T) {
	// 100 observations: 50 at most 0.1 s, 40 more by 0.5 s, 9 more by 1 s, 1 above.
	bs := buckets(map[string]float64{"0.1": 50, "0.5": 90, "1": 99, "+Inf": 100})
	for _, tc := range []struct{ q, want float64 }{
		{0.5, 0.1},             // the 50th observation is the last in the first bucket
		{0.7, 0.1 + 0.4*20/40}, // 20 of the second bucket's 40, across 0.1–0.5 s
		{0.99, 1},
		{0.999, 1}, // past every finite bucket: the highest finite bound
	} {
		if got := quantile(tc.q, bs); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("quantile(%v) = %v, want %v", tc.q, got, tc.want)
		}
	}
	if got := quantile(0.99, buckets(map[string]float64{"1": 0, "+Inf": 0})); !math.IsNaN(got) {
		t.Errorf("quantile without observations = %v, want NaN", got)
	}
}

func TestAtMostIsExactAtABucketBound(t *testing.T) {
	bs := buckets(map[string]float64{"0.5": 900, "1": 990, "2.5": 999, "+Inf": 1000})
	if got := atMost(1, bs); got != 0.99 {
		t.Errorf("atMost(1) = %v, want 0.99", got)
	}
	if got := atMost(0.75, bs); !math.IsNaN(got) {
		t.Errorf("atMost(0.75) = %v, want NaN: not a bucket bound", got)
	}
}

func TestVerdict(t *testing.T) {
	scrape := func(submitted, dispatched, underOne float64) []series {
		return mustParse(t, strings.NewReplacer("SUB", ftoa(submitted), "DIS", ftoa(dispatched), "LE1", ftoa(underOne)).Replace(`
jobs_submitted_total{tenant="t"} SUB
dispatch_latency_seconds_count{pool="load-0"} DIS
dispatch_latency_seconds_bucket{pool="load-0",le="1"} LE1
dispatch_latency_seconds_bucket{pool="load-0",le="+Inf"} DIS
`))
	}
	zero := scrape(0, 0, 0)
	window := 10 * time.Second
	for _, tc := range []struct {
		name                            string
		submitted, dispatched, underOne float64
		pass                            bool
	}{
		{"all good", 50000, 49000, 48600, true},
		{"api fell short of the offered rate", 49000, 48000, 48000, false},
		{"slow dispatch", 50000, 50000, 49000, false},
		{"dispatch fell behind", 50000, 47000, 47000, false},
		{"nothing dispatched", 50000, 0, 0, false},
	} {
		r := measure(zero, scrape(tc.submitted, tc.dispatched, tc.underOne), window, 5000)
		if got := r.passed(); got != tc.pass {
			t.Errorf("%s: passed = %v, want %v\n%s", tc.name, got, tc.pass, r.text())
		}
	}
}

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
