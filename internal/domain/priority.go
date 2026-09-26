package domain

import (
	"errors"
	"fmt"
	"strings"
)

// Priority is a job's priority class; a larger value is more urgent.
type Priority uint8

const (
	PriorityLow Priority = iota + 1
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// prioritiesByUrgency orders classes from most to least urgent; it also breaks ties.
var prioritiesByUrgency = [...]Priority{PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow}

var priorityNames = map[Priority]string{
	PriorityLow:      "LOW",
	PriorityNormal:   "NORMAL",
	PriorityHigh:     "HIGH",
	PriorityCritical: "CRITICAL",
}

func (p Priority) Valid() bool { return p >= PriorityLow && p <= PriorityCritical }

func (p Priority) String() string {
	if name, ok := priorityNames[p]; ok {
		return name
	}
	return fmt.Sprintf("Priority(%d)", uint8(p))
}

// ParsePriority parses a priority name such as "HIGH", ignoring case.
func ParsePriority(s string) (Priority, error) {
	for p, name := range priorityNames {
		if strings.EqualFold(s, name) {
			return p, nil
		}
	}
	return 0, fmt.Errorf("unknown priority %q (want CRITICAL, HIGH, NORMAL or LOW)", s)
}

func (p Priority) MarshalText() ([]byte, error) {
	if !p.Valid() {
		return nil, fmt.Errorf("invalid priority %d", uint8(p))
	}
	return []byte(p.String()), nil
}

func (p *Priority) UnmarshalText(b []byte) error {
	parsed, err := ParsePriority(string(b))
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

// PriorityWeights are the relative dispatch shares of the priority classes under contention.
type PriorityWeights map[Priority]int

// DefaultPriorityWeights returns CRITICAL:HIGH:NORMAL:LOW = 8:4:2:1.
func DefaultPriorityWeights() PriorityWeights {
	return PriorityWeights{PriorityCritical: 8, PriorityHigh: 4, PriorityNormal: 2, PriorityLow: 1}
}

// Validate requires a positive weight for every class and no unknown classes.
func (w PriorityWeights) Validate() error {
	var errs []error
	for _, p := range prioritiesByUrgency {
		if w[p] <= 0 {
			errs = append(errs, fmt.Errorf("weight for %s must be positive, got %d", p, w[p]))
		}
	}
	for p := range w {
		if !p.Valid() {
			errs = append(errs, fmt.Errorf("weight given for unknown priority %d", uint8(p)))
		}
	}
	return errors.Join(errs...)
}

// PrioritySelector chooses the next priority class to serve using smooth weighted
// round-robin over the classes that currently have work (LLD §7). It is not safe for
// concurrent use; each dispatcher pool owns one.
type PrioritySelector struct {
	weight [PriorityCritical + 1]int
	credit [PriorityCritical + 1]int
}

func NewPrioritySelector(w PriorityWeights) (*PrioritySelector, error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	s := &PrioritySelector{}
	for p, weight := range w {
		s.weight[p] = weight
	}
	return s, nil
}

// Next returns the class to serve among those for which hasWork reports true, and false
// if none has work. Classes without work keep their credit unchanged.
func (s *PrioritySelector) Next(hasWork func(Priority) bool) (Priority, bool) {
	var (
		best  Priority
		total int
	)
	for _, p := range prioritiesByUrgency {
		if !hasWork(p) {
			continue
		}
		s.credit[p] += s.weight[p]
		total += s.weight[p]
		if best == 0 || s.credit[p] > s.credit[best] {
			best = p
		}
	}
	if best == 0 {
		return 0, false
	}
	s.credit[best] -= total
	return best, true
}
