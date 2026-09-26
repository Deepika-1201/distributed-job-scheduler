package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"time"
)

// ErrInvalidSchedule reports a schedule definition that cannot be evaluated.
var ErrInvalidSchedule = errors.New("invalid schedule")

type ScheduleState string

const (
	ScheduleActive    ScheduleState = "ACTIVE"
	SchedulePaused    ScheduleState = "PAUSED"
	ScheduleCompleted ScheduleState = "COMPLETED"
	ScheduleDeleted   ScheduleState = "DELETED"
)

type TriggerKind string

const (
	TriggerCron       TriggerKind = "cron"
	TriggerFixedRate  TriggerKind = "fixed_rate"
	TriggerFixedDelay TriggerKind = "fixed_delay"
)

// MisfirePolicy decides what happens to fire times missed while the system was down or behind.
type MisfirePolicy string

const (
	MisfireFireOnce MisfirePolicy = "fire_once"
	MisfireSkip     MisfirePolicy = "skip"
	MisfireFireAll  MisfirePolicy = "fire_all"
)

// OverlapPolicy decides what happens when a fire is due while an earlier run is active.
type OverlapPolicy string

const (
	OverlapSkip           OverlapPolicy = "skip"
	OverlapBufferOne      OverlapPolicy = "buffer_one"
	OverlapAllow          OverlapPolicy = "allow"
	OverlapCancelPrevious OverlapPolicy = "cancel_previous"
)

func (p MisfirePolicy) Valid() bool {
	return p == MisfireFireOnce || p == MisfireSkip || p == MisfireFireAll
}

func (p OverlapPolicy) Valid() bool {
	return p == OverlapSkip || p == OverlapBufferOne || p == OverlapAllow || p == OverlapCancelPrevious
}

// Trigger defines when a schedule fires (LLD §10.2).
type Trigger struct {
	Kind     TriggerKind
	Cron     string
	TimeZone string        // IANA name, cron only; empty means UTC
	Interval time.Duration // fixed_rate and fixed_delay
}

type Schedule struct {
	ID         ScheduleID
	TenantID   TenantID
	Name       string
	JobType    string
	Payload    []byte
	Labels     map[string]string
	Priority   Priority // zero means the job type's default
	Trigger    Trigger
	StartAt    time.Time // zero means unset; also the fixed-rate anchor
	EndAt      time.Time // zero means unset
	MaxRuns    int       // zero means unlimited
	Jitter     time.Duration
	Misfire    MisfirePolicy
	Overlap    OverlapPolicy
	State      ScheduleState
	NextFireAt time.Time // zero when completed or waiting on a fixed-delay run
	LastFireAt time.Time
	FireCount  int
	CreatedBy  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Evaluator computes a time-based trigger's fire times.
type Evaluator interface {
	// Next returns the first fire time strictly after t, or zero if there is none.
	Next(t time.Time) time.Time
}

// LoadTimeZone resolves an IANA zone name; the host-dependent "Local" is rejected.
func LoadTimeZone(name string) (*time.Location, error) {
	if name == "" {
		return time.UTC, nil
	}
	if name == "Local" {
		return nil, fmt.Errorf("%w: time zone must be an IANA name", ErrInvalidSchedule)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown time zone %q", ErrInvalidSchedule, name)
	}
	return loc, nil
}

// Compile returns the trigger's evaluator. Fixed-delay triggers depend on run completion
// and have none.
func (t Trigger) Compile(anchor time.Time) (Evaluator, error) {
	switch t.Kind {
	case TriggerCron:
		loc, err := LoadTimeZone(t.TimeZone)
		if err != nil {
			return nil, err
		}
		return ParseCron(t.Cron, loc)
	case TriggerFixedRate, TriggerFixedDelay:
		if t.Interval <= 0 {
			return nil, fmt.Errorf("%w: interval must be positive", ErrInvalidSchedule)
		}
		if t.Kind == TriggerFixedDelay {
			return nil, nil
		}
		return fixedRate{anchor: anchor, interval: t.Interval}, nil
	}
	return nil, fmt.Errorf("%w: unknown trigger kind %q", ErrInvalidSchedule, t.Kind)
}

// fixedRate fires at anchor + k*interval for k >= 0.
type fixedRate struct {
	anchor   time.Time
	interval time.Duration
}

func (f fixedRate) Next(t time.Time) time.Time {
	if t.Before(f.anchor) {
		return f.anchor
	}
	return f.anchor.Add((t.Sub(f.anchor)/f.interval + 1) * f.interval)
}

// MinGap returns the smallest interval between consecutive fire times among the first n
// after from; it is used to enforce the minimum schedule interval.
func MinGap(e Evaluator, from time.Time, n int) time.Duration {
	gap := time.Duration(math.MaxInt64)
	prev := e.Next(from)
	for i := 1; i < n && !prev.IsZero(); i++ {
		next := e.Next(prev)
		if next.IsZero() {
			break
		}
		gap = min(gap, next.Sub(prev))
		prev = next
	}
	return gap
}

func (s Schedule) anchor() time.Time {
	if !s.StartAt.IsZero() {
		return s.StartAt
	}
	return s.CreatedAt
}

// FirstFire returns the schedule's first fire time after t, honoring StartAt. It is the
// cursor for a new or edited schedule.
func (s Schedule) FirstFire(t time.Time) (time.Time, error) {
	if !s.StartAt.IsZero() && s.StartAt.After(t) {
		t = s.StartAt.Add(-time.Nanosecond)
	}
	e, err := s.Trigger.Compile(s.anchor())
	if err != nil {
		return time.Time{}, err
	}
	if e == nil { // fixed delay: fire as soon as allowed
		return t.Add(time.Nanosecond), nil
	}
	return e.Next(t), nil
}

// Upcoming previews up to n fire times from the cursor, honoring EndAt and MaxRuns.
func (s Schedule) Upcoming(n int) []time.Time {
	if s.NextFireAt.IsZero() {
		return nil
	}
	e, err := s.Trigger.Compile(s.anchor())
	if err != nil {
		return nil
	}
	if e == nil {
		n = 1
	}
	if s.MaxRuns > 0 {
		n = min(n, s.MaxRuns-s.FireCount)
	}
	var out []time.Time
	for t := s.NextFireAt; len(out) < n && !t.IsZero() && (s.EndAt.IsZero() || !t.After(s.EndAt)); t = e.Next(t) {
		out = append(out, t)
		if e == nil {
			break
		}
	}
	return out
}

// PlanLimits bound one materialization pass (LLD §10.3).
type PlanLimits struct {
	Lookahead        time.Duration
	MisfireThreshold time.Duration
	MaxMisfires      int // fire_all keeps at most this many of the most recent missed fires
	MaxFires         int // per schedule per pass
}

var DefaultPlanLimits = PlanLimits{Lookahead: 2 * time.Minute, MisfireThreshold: time.Minute, MaxMisfires: 100, MaxFires: 1000}

// FirePlan is the materializer's decision for one schedule.
type FirePlan struct {
	Fires     []time.Time
	Next      time.Time // new cursor; zero when completed or waiting on a fixed-delay run
	Completed bool
}

// maxMissedScan bounds the walk over missed fire times; a longer backlog is worked off
// over several passes.
const maxMissedScan = 1_000_000

// PlanFires decides which fire times to materialize now for an active schedule with a cursor.
func PlanFires(s Schedule, now time.Time, lim PlanLimits) (FirePlan, error) {
	next := s.NextFireAt
	remaining := math.MaxInt
	if s.MaxRuns > 0 {
		remaining = s.MaxRuns - s.FireCount
	}
	inWindow := func(t time.Time) bool { return s.EndAt.IsZero() || !t.After(s.EndAt) }
	horizon := now.Add(lim.Lookahead)

	e, err := s.Trigger.Compile(s.anchor())
	switch {
	case err != nil:
		return FirePlan{}, err
	case next.IsZero():
		return FirePlan{}, nil
	case remaining <= 0 || !inWindow(next):
		return FirePlan{Completed: true}, nil
	case next.After(horizon):
		return FirePlan{Next: next}, nil
	case e == nil: // fixed delay fires once, even after a misfire, then waits for the run
		return FirePlan{Fires: []time.Time{next}, Completed: remaining == 1}, nil
	}

	var fires []time.Time
	if next.Before(now.Add(-lim.MisfireThreshold)) {
		switch s.Misfire {
		case MisfireSkip:
			next = e.Next(now)
		case MisfireFireAll:
			fires = lastMissed(e, next, now, max(lim.MaxMisfires, 1))
			next = e.Next(fires[len(fires)-1])
		default:
			last := lastMissed(e, next, now, 1)[0]
			fires = append(fires, last)
			next = e.Next(last)
		}
	}
	for !next.IsZero() && !next.After(horizon) && len(fires) < lim.MaxFires {
		fires = append(fires, next)
		next = e.Next(next)
	}

	var plan FirePlan
	for _, f := range fires {
		if !inWindow(f) || len(plan.Fires) >= remaining {
			plan.Completed = true
			break
		}
		plan.Fires = append(plan.Fires, f)
	}
	if !plan.Completed && (next.IsZero() || !inWindow(next) || len(plan.Fires) >= remaining) {
		plan.Completed = true
	}
	if !plan.Completed {
		plan.Next = next
	}
	return plan, nil
}

// lastMissed returns up to n of the most recent fire times in [from, now], oldest first.
// from must be a fire time at or before now.
func lastMissed(e Evaluator, from, now time.Time, n int) []time.Time {
	ring := make([]time.Time, 0, n)
	scanned := 0
	for t := from; !t.IsZero() && !t.After(now) && scanned < maxMissedScan; t = e.Next(t) {
		if len(ring) == n {
			copy(ring, ring[1:])
			ring = ring[:n-1]
		}
		ring = append(ring, t)
		scanned++
	}
	return ring
}

// JitterOffset is the deterministic offset of a fire's run_at within the jitter window,
// so materializing the same fire again yields the same run_at.
func JitterOffset(id ScheduleID, fire time.Time, window time.Duration) time.Duration {
	ms := uint64(window / time.Millisecond)
	if ms == 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(fire.UnixNano()))
	_, _ = h.Write(b[:])
	return time.Duration(h.Sum64()%ms) * time.Millisecond
}
