package dispatch

import (
	"testing"

	"jobscheduler/internal/domain"
)

func TestAllocateSplitsSlotsByWeight(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		has  []domain.Priority
		want map[domain.Priority]int
	}{
		{"all classes, one full cycle", 15, []domain.Priority{domain.PriorityCritical, domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow},
			map[domain.Priority]int{domain.PriorityCritical: 8, domain.PriorityHigh: 4, domain.PriorityNormal: 2, domain.PriorityLow: 1}},
		{"only the classes with work", 6, []domain.Priority{domain.PriorityHigh, domain.PriorityLow},
			map[domain.Priority]int{domain.PriorityHigh: 5, domain.PriorityLow: 1}},
		{"no work", 4, nil, map[domain.Priority]int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := domain.NewPrioritySelector(domain.DefaultPriorityWeights())
			if err != nil {
				t.Fatal(err)
			}
			has := map[domain.Priority]bool{}
			for _, p := range tc.has {
				has[p] = true
			}
			got := allocate(s, tc.n, func(p domain.Priority) bool { return has[p] })
			if len(got) != len(tc.want) {
				t.Fatalf("allocate = %v, want %v", got, tc.want)
			}
			for p, n := range tc.want {
				if got[p] != n {
					t.Errorf("allocate = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestWaiterDeliversOnce(t *testing.T) {
	w := &waiter{ready: make(chan struct{})}
	jobs := []domain.Job{{ID: "a"}}
	if !w.deliver(jobs) {
		t.Fatal("first delivery refused")
	}
	if w.deliver(jobs) {
		t.Error("second delivery accepted")
	}
	if got := w.abandon(); len(got) != 1 {
		t.Errorf("abandon after delivery returned %d jobs, want the delivered one", len(got))
	}
	late := &waiter{ready: make(chan struct{})}
	late.abandon()
	if late.deliver(jobs) {
		t.Error("delivery to an abandoned waiter accepted; its jobs would never be released")
	}
}
