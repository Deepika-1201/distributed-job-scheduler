package domain

import (
	"slices"
	"testing"
	"time"
)

// t0 (retry_test.go) is on an exact hour, which the cron cases below rely on.

func everyMinute(mut func(*Schedule)) Schedule {
	s := Schedule{
		ID:         "0191f000-0000-7000-8000-00000000000a",
		Trigger:    Trigger{Kind: TriggerFixedRate, Interval: time.Minute},
		CreatedAt:  t0,
		Misfire:    MisfireFireOnce,
		Overlap:    OverlapSkip,
		State:      ScheduleActive,
		NextFireAt: t0,
	}
	if mut != nil {
		mut(&s)
	}
	return s
}

func mins(ms ...int) []time.Time {
	out := make([]time.Time, len(ms))
	for i, m := range ms {
		out[i] = t0.Add(time.Duration(m) * time.Minute)
	}
	return out
}

func TestFixedRateNext(t *testing.T) {
	f := fixedRate{anchor: t0, interval: 10 * time.Minute}
	for after, want := range map[time.Duration]time.Duration{-time.Hour: 0, 0: 10 * time.Minute, 5 * time.Minute: 10 * time.Minute, 10 * time.Minute: 20 * time.Minute} {
		if got := f.Next(t0.Add(after)); !got.Equal(t0.Add(want)) {
			t.Errorf("Next(t0%+v) = %v, want t0+%v", after, got.Sub(t0), want)
		}
	}
}

func TestPlanFires(t *testing.T) {
	lim := PlanLimits{Lookahead: 2 * time.Minute, MisfireThreshold: time.Minute, MaxMisfires: 5, MaxFires: 1000}
	late := t0.Add(10*time.Minute + 30*time.Second) // ten fires missed
	for _, tc := range []struct {
		name string
		s    Schedule
		now  time.Time
		want FirePlan
	}{
		{"fires within the lookahead", everyMinute(nil), t0,
			FirePlan{Fires: mins(0, 1, 2), Next: t0.Add(3 * time.Minute)}},
		{"cursor beyond the lookahead", everyMinute(nil), t0.Add(-time.Hour),
			FirePlan{Next: t0}},
		{"max runs completes the schedule", everyMinute(func(s *Schedule) { s.MaxRuns, s.FireCount = 5, 3 }), t0,
			FirePlan{Fires: mins(0, 1), Completed: true}},
		{"end time completes the schedule", everyMinute(func(s *Schedule) { s.EndAt = t0.Add(90 * time.Second) }), t0,
			FirePlan{Fires: mins(0, 1), Completed: true}},
		{"end time reached before the window", everyMinute(func(s *Schedule) { s.EndAt = t0.Add(-time.Second) }), t0,
			FirePlan{Completed: true}},
		{"late but within the misfire threshold", everyMinute(func(s *Schedule) { s.NextFireAt = t0.Add(10 * time.Minute) }), late,
			FirePlan{Fires: mins(10, 11, 12), Next: t0.Add(13 * time.Minute)}},
		{"misfire: fire once", everyMinute(nil), late,
			FirePlan{Fires: mins(10, 11, 12), Next: t0.Add(13 * time.Minute)}},
		{"misfire: skip", everyMinute(func(s *Schedule) { s.Misfire = MisfireSkip }), late,
			FirePlan{Fires: mins(11, 12), Next: t0.Add(13 * time.Minute)}},
		{"misfire: fire all, capped to the most recent", everyMinute(func(s *Schedule) { s.Misfire = MisfireFireAll }), late,
			FirePlan{Fires: mins(6, 7, 8, 9, 10, 11, 12), Next: t0.Add(13 * time.Minute)}},
		{"fixed delay fires once and waits", everyMinute(func(s *Schedule) { s.Trigger.Kind = TriggerFixedDelay }), late,
			FirePlan{Fires: mins(0)}},
		{"fixed delay's last run completes it", everyMinute(func(s *Schedule) { s.Trigger.Kind, s.MaxRuns = TriggerFixedDelay, 1 }), t0,
			FirePlan{Fires: mins(0), Completed: true}},
		{"completed schedule has no cursor", everyMinute(func(s *Schedule) { s.NextFireAt = time.Time{} }), t0,
			FirePlan{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PlanFires(tc.s, tc.now, lim)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.EqualFunc(got.Fires, tc.want.Fires, time.Time.Equal) || !got.Next.Equal(tc.want.Next) || got.Completed != tc.want.Completed {
				t.Errorf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

// A cron fire at a whole minute is materialized at the half minute before, not at the boundary
// where the fires due then are promoted (ADR-032).
func TestDefaultLookaheadMaterializesBetweenBoundaries(t *testing.T) {
	cron := everyMinute(func(s *Schedule) {
		s.Trigger = Trigger{Kind: TriggerCron, Cron: "* * * * *", TimeZone: "UTC"}
		s.NextFireAt = t0.Add(3 * time.Minute)
	})
	for _, tc := range []struct {
		now   time.Duration
		fires int
	}{{30*time.Second - time.Millisecond, 0}, {30 * time.Second, 1}, {time.Minute, 1}} {
		got, err := PlanFires(cron, t0.Add(tc.now), DefaultPlanLimits)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Fires) != tc.fires {
			t.Errorf("at t0+%v: %d fires, want %d", tc.now, len(got.Fires), tc.fires)
		}
	}
}

func TestPlanFiresCapsFiresPerPass(t *testing.T) {
	s := everyMinute(func(s *Schedule) { s.Trigger.Interval = time.Second })
	got, err := PlanFires(s, t0, PlanLimits{Lookahead: time.Hour, MisfireThreshold: time.Minute, MaxFires: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Fires) != 10 || !got.Next.Equal(t0.Add(10*time.Second)) || got.Completed {
		t.Errorf("got %d fires, next %v, completed %v", len(got.Fires), got.Next.Sub(t0), got.Completed)
	}
}

func TestFirstFireAndUpcoming(t *testing.T) {
	start := t0.Add(90 * time.Minute)
	cron := Schedule{Trigger: Trigger{Kind: TriggerCron, Cron: "0 * * * *", TimeZone: "Europe/Paris"}, CreatedAt: t0, StartAt: start}
	first, err := cron.FirstFire(t0)
	if err != nil || !first.Equal(t0.Add(2*time.Hour)) {
		t.Errorf("cron FirstFire = %v, %v; want t0+2h", first.Sub(t0), err)
	}
	cron.NextFireAt, cron.MaxRuns, cron.FireCount = first, 5, 2
	if got := cron.Upcoming(5); !slices.EqualFunc(got, []time.Time{first, first.Add(time.Hour), first.Add(2 * time.Hour)}, time.Time.Equal) {
		t.Errorf("Upcoming = %v", got)
	}

	rate := everyMinute(func(s *Schedule) { s.StartAt = start })
	if first, _ := rate.FirstFire(t0); !first.Equal(start) {
		t.Errorf("fixed-rate FirstFire = %v, want the start time", first)
	}
	delay := everyMinute(func(s *Schedule) { s.Trigger.Kind = TriggerFixedDelay })
	if first, _ := delay.FirstFire(t0); first.Sub(t0) > time.Millisecond {
		t.Errorf("fixed-delay FirstFire = t0+%v, want now", first.Sub(t0))
	}
}

func TestMinGap(t *testing.T) {
	for expr, want := range map[string]time.Duration{"*/10 * * * * *": 10 * time.Second, "0 * * * *": time.Hour, "0,1 0 * * *": time.Minute} {
		c, err := ParseCron(expr, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		if got := MinGap(c, t0, 100); got != want {
			t.Errorf("MinGap(%q) = %v, want %v", expr, got, want)
		}
	}
}

func TestJitterOffsetIsDeterministic(t *testing.T) {
	const id ScheduleID = "0191f000-0000-7000-8000-00000000000a"
	seen := map[time.Duration]bool{}
	for i := range 50 {
		fire := t0.Add(time.Duration(i) * time.Minute)
		d := JitterOffset(id, fire, 30*time.Second)
		if d < 0 || d >= 30*time.Second || d != JitterOffset(id, fire, 30*time.Second) {
			t.Fatalf("offset %v for fire %d is out of range or unstable", d, i)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct offsets in 50 fires", len(seen))
	}
	if JitterOffset(id, t0, 0) != 0 {
		t.Error("zero window must not jitter")
	}
}
