package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidCron reports a cron expression that cannot be parsed or never fires.
var ErrInvalidCron = errors.New("invalid cron expression")

// Cron is a parsed cron expression evaluated in a time zone (LLD §10.2).
type Cron struct {
	second, minute, hour, dom, month, dow bitset
	domAny, dowAny                        bool
	loc                                   *time.Location
}

type bitset uint64

func (b bitset) has(v int) bool { return b&(1<<uint(v)) != 0 }

type cronField struct {
	name     string
	min, max int
	names    map[string]int
	anyAlias bool // accepts "?" as "*"
}

var (
	secondField = cronField{name: "second", min: 0, max: 59}
	minuteField = cronField{name: "minute", min: 0, max: 59}
	hourField   = cronField{name: "hour", min: 0, max: 23}
	domField    = cronField{name: "day-of-month", min: 1, max: 31, anyAlias: true}
	monthField  = cronField{name: "month", min: 1, max: 12, names: map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}}
	dowField = cronField{name: "day-of-week", min: 0, max: 7, anyAlias: true, names: map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}}
)

var cronMacros = map[string]string{
	"@yearly":   "0 0 0 1 1 *",
	"@annually": "0 0 0 1 1 *",
	"@monthly":  "0 0 0 1 * *",
	"@weekly":   "0 0 0 * * 0",
	"@daily":    "0 0 0 * * *",
	"@midnight": "0 0 0 * * *",
	"@hourly":   "0 0 * * * *",
}

// cronHorizonYears bounds the search for a next fire time; every valid expression fires
// within it (a leap day recurs within 8 years).
const cronHorizonYears = 10

// ParseCron parses a 5-field (minute first) or 6-field (second first) expression, or a
// macro such as @daily, to be evaluated in loc.
func ParseCron(expr string, loc *time.Location) (*Cron, error) {
	spec := strings.TrimSpace(expr)
	if macro, ok := cronMacros[strings.ToLower(spec)]; ok {
		spec = macro
	}
	fields := strings.Fields(spec)
	switch len(fields) {
	case 5:
		fields = append([]string{"0"}, fields...)
	case 6:
	default:
		return nil, fmt.Errorf("%w: want 5 or 6 fields, got %d", ErrInvalidCron, len(fields))
	}
	c := &Cron{loc: loc}
	var err error
	for i, target := range []struct {
		set *bitset
		f   cronField
	}{{&c.second, secondField}, {&c.minute, minuteField}, {&c.hour, hourField},
		{&c.dom, domField}, {&c.month, monthField}, {&c.dow, dowField}} {
		if *target.set, err = target.f.parse(fields[i]); err != nil {
			return nil, err
		}
	}
	if c.dow.has(7) {
		c.dow = c.dow&^(1<<7) | 1
	}
	c.domAny = fields[3] == "*" || fields[3] == "?"
	c.dowAny = fields[5] == "*" || fields[5] == "?"
	if !c.satisfiable() {
		return nil, fmt.Errorf("%w: %q never fires", ErrInvalidCron, expr)
	}
	return c, nil
}

func (f cronField) parse(s string) (bitset, error) {
	var set bitset
	for part := range strings.SplitSeq(s, ",") {
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("%w: bad step %q in %s field", ErrInvalidCron, stepText, f.name)
			}
			step = n
		}
		lo, hi := f.min, f.max
		if rng != "*" && !(rng == "?" && f.anyAlias) {
			first, last, isRange := strings.Cut(rng, "-")
			var err error
			if lo, err = f.value(first); err != nil {
				return 0, err
			}
			switch {
			case isRange:
				if hi, err = f.value(last); err != nil {
					return 0, err
				}
			case !hasStep:
				hi = lo
			}
			if lo > hi {
				return 0, fmt.Errorf("%w: range %q is backwards in %s field", ErrInvalidCron, rng, f.name)
			}
		}
		for v := lo; v <= hi; v += step {
			set |= 1 << uint(v)
		}
	}
	return set, nil
}

func (f cronField) value(s string) (int, error) {
	if v, ok := f.names[strings.ToUpper(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < f.min || v > f.max {
		return 0, fmt.Errorf("%w: %q is not a valid %s (%d-%d)", ErrInvalidCron, s, f.name, f.min, f.max)
	}
	return v, nil
}

// satisfiable reports whether some calendar day can match, e.g. rejecting "0 0 30 2 *".
func (c *Cron) satisfiable() bool {
	if c.domAny || !c.dowAny {
		return true // any day of the month, or either day field may match
	}
	daysIn := [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	for m := 1; m <= 12; m++ {
		for d := 1; c.month.has(m) && d <= daysIn[m]; d++ {
			if c.dom.has(d) {
				return true
			}
		}
	}
	return false
}

// Next returns the first fire time strictly after t, or the zero time if there is none
// within the search horizon.
func (c *Cron) Next(t time.Time) time.Time {
	w := wallClock(t.In(c.loc)).Truncate(time.Second).Add(time.Second)
	limit := w.AddDate(cronHorizonYears, 0, 0)
	for {
		var ok bool
		if w, ok = c.nextWall(w, limit); !ok {
			return time.Time{}
		}
		if at := resolveWall(w, c.loc); at.After(t) {
			return at
		}
		w = w.Add(time.Second)
	}
}

// nextWall returns the earliest wall-clock time at or after w that matches every field.
// Wall-clock times are represented in UTC, which has no DST, so arithmetic on them is exact.
func (c *Cron) nextWall(w, limit time.Time) (time.Time, bool) {
	for w.Before(limit) {
		switch {
		case !c.month.has(int(w.Month())):
			w = time.Date(w.Year(), w.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		case !c.dayMatches(w):
			w = time.Date(w.Year(), w.Month(), w.Day()+1, 0, 0, 0, 0, time.UTC)
		case !c.hour.has(w.Hour()):
			w = w.Truncate(time.Hour).Add(time.Hour)
		case !c.minute.has(w.Minute()):
			w = w.Truncate(time.Minute).Add(time.Minute)
		case !c.second.has(w.Second()):
			w = w.Add(time.Second)
		default:
			return w, true
		}
	}
	return time.Time{}, false
}

// dayMatches follows Vixie cron: when both day fields are restricted, either may match.
func (c *Cron) dayMatches(w time.Time) bool {
	dom, dow := c.dom.has(w.Day()), c.dow.has(int(w.Weekday()))
	if c.domAny || c.dowAny {
		return dom && dow
	}
	return dom || dow
}

// resolveWall maps a wall-clock time in loc to the instant it fires: a time skipped by a
// DST gap fires when the gap ends, and a repeated time fires at its first occurrence.
func resolveWall(w time.Time, loc *time.Location) time.Time {
	t := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), 0, loc)
	if got := wallClock(t); !got.Equal(w) {
		start, end := t.ZoneBounds()
		if got.After(w) {
			return start // t lies in the zone that began at the end of the gap
		}
		return end
	}
	if start, _ := t.ZoneBounds(); !start.IsZero() {
		_, prevOffset := start.Add(-time.Nanosecond).Zone()
		if _, offset := t.Zone(); prevOffset > offset {
			alt := t.Add(-time.Duration(prevOffset-offset) * time.Second)
			if alt.Before(start) && wallClock(alt).Equal(w) {
				return alt
			}
		}
	}
	return t
}

func wallClock(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}
