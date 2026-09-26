package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewLoggerJSONIncludesAttrsAndRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, slog.LevelInfo, "json", slog.String("service", "jobscheduler"))

	log.Debug("hidden")
	log.Info("started", "port", 9090)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("want exactly one JSON record, got %q: %v", buf.String(), err)
	}
	if rec["msg"] != "started" || rec["service"] != "jobscheduler" || rec["port"] != float64(9090) {
		t.Errorf("record = %v", rec)
	}
}

func TestNewLoggerText(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelInfo, "text").Info("hello")
	if !bytes.Contains(buf.Bytes(), []byte("msg=hello")) {
		t.Errorf("text output = %q", buf.String())
	}
}
