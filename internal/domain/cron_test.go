package domain

import (
	"errors"
	"testing"
	"time"
)

func mustZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func TestParseCronRejectsInvalidExpressions(t *testing.T) {
	for _, expr := range []string{
		"", "* * * *", "* * * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "* * * * 8",
		"*/0 * * * *", "5-1 * * * *", "0 0 30 2 *", "0 0 31 4,6,9,11 *", "0 0 L * *", "0 0 1W * *",
		"0 0 * * 5#3", "? * * * *", "@every 5m", "0 0 * FOO *", "1,,2 * * * *",
	} {
		if _, err := ParseCron(expr, time.UTC); !errors.Is(err, ErrInvalidCron) {
			t.Errorf("ParseCron(%q) = %v, want ErrInvalidCron", expr, err)
		}
	}
}

func TestCronNext(t *testing.T) {
	utc := func(s string) time.Time {
		v, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, tc := range []struct{ expr, after, want string }{
		{"*/15 * * * *", "2026-01-02 10:07:00", "2026-01-02 10:15:00"},
		{"0 10 * * *", "2026-01-02 10:00:00", "2026-01-03 10:00:00"}, // strictly after
		{"0 9 * * MON-FRI", "2026-01-02 10:00:00", "2026-01-05 09:00:00"},
		{"*/30 * * * * *", "2026-01-02 10:00:10", "2026-01-02 10:00:30"},
		{"@daily", "2026-03-10 12:00:00", "2026-03-11 00:00:00"},
		{"@hourly", "2026-03-10 12:00:00", "2026-03-10 13:00:00"},
		{"0 0 13 * FRI", "2026-03-01 00:00:00", "2026-03-06 00:00:00"}, // either day field matches
		{"0 0 * * 7", "2026-03-02 00:00:00", "2026-03-08 00:00:00"},    // 7 is Sunday
		{"0 0 29 2 *", "2026-03-01 00:00:00", "2028-02-29 00:00:00"},
		{"0 0 1 JAN,jul *", "2026-02-01 00:00:00", "2026-07-01 00:00:00"},
		{"0 12 1-7 * 1", "2026-03-10 00:00:00", "2026-03-16 12:00:00"},
		{"5/20 * * * *", "2026-01-02 10:26:00", "2026-01-02 10:45:00"},
		{"0 0 ? * SUN", "2026-03-02 00:00:00", "2026-03-08 00:00:00"},
	} {
		c, err := ParseCron(tc.expr, time.UTC)
		if err != nil {
			t.Fatalf("%q: %v", tc.expr, err)
		}
		if got := c.Next(utc(tc.after)); !got.Equal(utc(tc.want)) {
			t.Errorf("%q after %s = %s, want %s", tc.expr, tc.after, got.UTC().Format(time.DateTime), tc.want)
		}
	}
}

func TestCronDST(t *testing.T) {
	for _, tc := range []struct {
		name, zone, expr string
		after            string // RFC 3339
		want             []string
	}{
		{"skipped time fires at the gap's end", "America/New_York", "30 2 * * *",
			"2026-03-08T00:00:00-05:00", []string{"2026-03-08T03:00:00-04:00", "2026-03-09T02:30:00-04:00"}},
		{"interval cron keeps real-time spacing across a gap", "America/New_York", "*/15 * * * *",
			"2026-03-08T01:45:00-05:00", []string{"2026-03-08T03:00:00-04:00", "2026-03-08T03:15:00-04:00"}},
		{"repeated time fires once, at its first occurrence", "America/New_York", "30 1 * * *",
			"2026-11-01T00:00:00-04:00", []string{"2026-11-01T01:30:00-04:00", "2026-11-02T01:30:00-05:00"}},
		{"interval cron pauses during the repeated hour", "America/New_York", "*/30 * * * *",
			"2026-11-01T01:30:00-04:00", []string{"2026-11-01T02:00:00-05:00"}},
		{"evaluating from inside the repeated hour", "America/New_York", "*/30 * * * *",
			"2026-11-01T01:10:00-05:00", []string{"2026-11-01T02:00:00-05:00"}},
		{"London gap", "Europe/London", "30 1 * * *",
			"2026-03-29T00:00:00Z", []string{"2026-03-29T02:00:00+01:00", "2026-03-30T01:30:00+01:00"}},
		{"London overlap", "Europe/London", "30 1 * * *",
			"2026-10-25T00:00:00Z", []string{"2026-10-25T01:30:00+01:00", "2026-10-26T01:30:00Z"}},
		{"30-minute DST shift", "Australia/Lord_Howe", "15 2 * * *",
			"2026-10-04T00:00:00+10:30", []string{"2026-10-04T02:30:00+11:00", "2026-10-05T02:15:00+11:00"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ParseCron(tc.expr, mustZone(t, tc.zone))
			if err != nil {
				t.Fatal(err)
			}
			at, _ := time.Parse(time.RFC3339, tc.after)
			for _, w := range tc.want {
				want, _ := time.Parse(time.RFC3339, w)
				at = c.Next(at)
				if !at.Equal(want) {
					t.Fatalf("got %s, want %s", at.Format(time.RFC3339), want.Format(time.RFC3339))
				}
			}
		})
	}
}

// Over a whole year of DST transitions an hourly cron never fires twice at one instant, and
// consecutive fires are 1 h apart except across the repeated hour (2 h).
func TestCronHourlyAcrossAYear(t *testing.T) {
	for _, zone := range []string{"America/New_York", "Europe/London", "Australia/Lord_Howe", "UTC"} {
		loc := mustZone(t, zone)
		c, err := ParseCron("0 * * * *", loc)
		if err != nil {
			t.Fatal(err)
		}
		at := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
		long := 0
		for range 365 * 24 {
			next := c.Next(at)
			gap := next.Sub(at)
			if gap < 30*time.Minute || gap > 2*time.Hour {
				t.Fatalf("%s: gap %s between %s and %s", zone, gap, at, next)
			}
			if gap > time.Hour {
				long++
			}
			at = next
		}
		if want := map[bool]int{true: 0, false: 1}[zone == "UTC"]; long != want {
			t.Errorf("%s: %d gaps longer than an hour, want %d", zone, long, want)
		}
	}
}
