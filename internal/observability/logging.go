// Package observability sets up logging; telemetry exporters are added in phase 11.
package observability

import (
	"io"
	"log/slog"
)

// NewLogger returns a structured logger writing JSON (or text for local use) to w,
// with attrs attached to every record.
func NewLogger(w io.Writer, level slog.Level, format string, attrs ...slog.Attr) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewJSONHandler(w, opts)
	if format == "text" {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h.WithAttrs(attrs))
}
