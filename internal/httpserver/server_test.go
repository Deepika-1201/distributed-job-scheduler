package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestServeAndGracefulShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		io.WriteString(w, "done")
	})

	s := New("test", "127.0.0.1:0", mux, 5*time.Second, discardLogger())
	addr, err := s.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	type result struct {
		body string
		err  error
	}
	resp := make(chan result, 1)
	go func() {
		r, err := http.Get("http://" + addr.String() + "/slow")
		if err != nil {
			resp <- result{err: err}
			return
		}
		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)
		resp <- result{string(b), err}
	}()

	<-started
	cancel()
	time.Sleep(50 * time.Millisecond) // give Shutdown time to close the listener
	if _, err := net.DialTimeout("tcp", addr.String(), 100*time.Millisecond); err == nil {
		t.Error("listener still accepting connections after shutdown began")
	}
	close(release)

	if r := <-resp; r.err != nil || r.body != "done" {
		t.Errorf("in-flight request = %q, %v; want it to complete during shutdown", r.body, r.err)
	}
	if err := <-runErr; err != nil {
		t.Errorf("Run = %v, want nil after clean shutdown", err)
	}
}

func TestListenReportsPortConflict(t *testing.T) {
	first := New("first", "127.0.0.1:0", http.NewServeMux(), time.Second, discardLogger())
	addr, err := first.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	second := New("second", addr.String(), http.NewServeMux(), time.Second, discardLogger())
	if _, err := second.Listen(); err == nil || !strings.Contains(err.Error(), "second: listen on") {
		t.Errorf("second Listen = %v, want port conflict error", err)
	}
}

func TestShutdownTimeoutIsReported(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stuck", func(http.ResponseWriter, *http.Request) {
		close(started)
		<-block
	})

	s := New("test", "127.0.0.1:0", mux, 50*time.Millisecond, discardLogger())
	addr, err := s.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	go http.Get("http://" + addr.String() + "/stuck")

	<-started
	cancel()
	if err := <-runErr; err == nil || !strings.Contains(err.Error(), "shutdown") {
		t.Errorf("Run = %v, want shutdown timeout error", err)
	}
}
