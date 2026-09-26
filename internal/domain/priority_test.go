package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPriorityTextRoundTrip(t *testing.T) {
	for _, p := range prioritiesByUrgency {
		b, err := p.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%v): %v", p, err)
		}
		var got Priority
		if err := got.UnmarshalText(b); err != nil || got != p {
			t.Errorf("round trip %v -> %q -> %v (%v)", p, b, got, err)
		}
	}

	var wrapped struct{ P Priority }
	if err := json.Unmarshal([]byte(`{"P":"high"}`), &wrapped); err != nil || wrapped.P != PriorityHigh {
		t.Errorf("JSON parse = %v, %v", wrapped.P, err)
	}
}

func TestPriorityRejectsUnknownValues(t *testing.T) {
	if _, err := ParsePriority("URGENT"); err == nil {
		t.Error("ParsePriority accepted URGENT")
	}
	if _, err := Priority(0).MarshalText(); err == nil {
		t.Error("MarshalText accepted the zero priority")
	}
	if Priority(9).Valid() || Priority(0).Valid() {
		t.Error("out-of-range priority reported valid")
	}
	if got := Priority(9).String(); got != "Priority(9)" {
		t.Errorf("String() = %q", got)
	}
}

func TestPriorityWeightsValidate(t *testing.T) {
	if err := DefaultPriorityWeights().Validate(); err != nil {
		t.Fatalf("default weights invalid: %v", err)
	}
	bad := PriorityWeights{PriorityCritical: 8, PriorityHigh: 0, PriorityNormal: -1, Priority(7): 3}
	err := bad.Validate()
	if err == nil {
		t.Fatal("invalid weights accepted")
	}
	for _, want := range []string{"HIGH must be positive", "NORMAL must be positive", "LOW must be positive", "unknown priority 7"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
	if _, err := NewPrioritySelector(bad); err == nil {
		t.Error("NewPrioritySelector accepted invalid weights")
	}
}

func busy(classes ...Priority) func(Priority) bool {
	return func(p Priority) bool {
		for _, c := range classes {
			if c == p {
				return true
			}
		}
		return false
	}
}

func picks(t *testing.T, s *PrioritySelector, n int, hasWork func(Priority) bool) []Priority {
	t.Helper()
	out := make([]Priority, 0, n)
	for range n {
		p, ok := s.Next(hasWork)
		if !ok {
			t.Fatal("Next found no work")
		}
		out = append(out, p)
	}
	return out
}

func newSelector(t *testing.T) *PrioritySelector {
	t.Helper()
	s, err := NewPrioritySelector(DefaultPriorityWeights())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSelectorCycleIsExactAndSmooth(t *testing.T) {
	s := newSelector(t)
	all := busy(prioritiesByUrgency[:]...)
	c, h, n, l := PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow
	want := []Priority{c, h, c, n, c, h, c, l, c, h, c, n, c, h, c}

	for cycle := range 3 {
		got := picks(t, s, len(want), all)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("cycle %d: sequence = %v, want %v", cycle, got, want)
			}
		}
	}
	if s.credit != [PriorityCritical + 1]int{} {
		t.Errorf("credits after whole cycles = %v, want all zero", s.credit)
	}
}

func TestSelectorIsWorkConserving(t *testing.T) {
	s := newSelector(t)
	counts := map[Priority]int{}
	for _, p := range picks(t, s, 900, busy(PriorityCritical, PriorityLow)) {
		counts[p]++
	}
	if counts[PriorityCritical] != 800 || counts[PriorityLow] != 100 || len(counts) != 2 {
		t.Errorf("counts = %v, want CRITICAL:800 LOW:100", counts)
	}
}

func TestSelectorNeverStarvesABusyClass(t *testing.T) {
	s := newSelector(t)
	last := map[Priority]int{}
	for i, p := range picks(t, s, 150, busy(prioritiesByUrgency[:]...)) {
		if prev, seen := last[p]; seen && i-prev > 15 {
			t.Errorf("%v waited %d picks, want at most 15", p, i-prev)
		}
		last[p] = i
	}
	for _, p := range prioritiesByUrgency {
		if _, seen := last[p]; !seen {
			t.Errorf("%v never picked", p)
		}
	}
}

func TestSelectorWithNoWork(t *testing.T) {
	if p, ok := newSelector(t).Next(busy()); ok {
		t.Errorf("Next = %v with no work, want none", p)
	}
}

func TestSelectorSingleBusyClassAlwaysWins(t *testing.T) {
	s := newSelector(t)
	for _, p := range picks(t, s, 20, busy(PriorityNormal)) {
		if p != PriorityNormal {
			t.Fatalf("picked %v, only NORMAL has work", p)
		}
	}
}
