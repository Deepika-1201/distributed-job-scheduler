// Package httpserver runs an http.Server as an app.Component.
package httpserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

type Server struct {
	name            string
	srv             *http.Server
	shutdownTimeout time.Duration
	log             *slog.Logger

	mu sync.Mutex
	ln net.Listener
}

// New returns a server with conservative timeouts; it does not bind until Listen or Run.
func New(name, addr string, h http.Handler, shutdownTimeout time.Duration, log *slog.Logger) *Server {
	return &Server{
		name:            name,
		shutdownTimeout: shutdownTimeout,
		log:             log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           h,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		},
	}
}

func (s *Server) Name() string { return s.name }

// UseTLS serves TLS with c instead of plaintext.
func (s *Server) UseTLS(c *tls.Config) { s.srv.TLSConfig = c }

// Listen binds the address so port conflicts surface before any component starts.
// It is idempotent and returns the bound address.
func (s *Server) Listen() (net.Addr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		ln, err := net.Listen("tcp", s.srv.Addr)
		if err != nil {
			return nil, fmt.Errorf("%s: listen on %s: %w", s.name, s.srv.Addr, err)
		}
		s.ln = ln
	}
	return s.ln.Addr(), nil
}

// Run serves until ctx is cancelled, then drains in-flight requests within the shutdown timeout.
func (s *Server) Run(ctx context.Context) error {
	addr, err := s.Listen()
	if err != nil {
		return err
	}
	s.log.Info("http server listening", "server", s.name, "addr", addr.String(), "tls", s.srv.TLSConfig != nil)

	serveErr := make(chan error, 1)
	go func() {
		if s.srv.TLSConfig != nil {
			serveErr <- s.srv.ServeTLS(s.ln, "", "") // certificates come from TLSConfig
			return
		}
		serveErr <- s.srv.Serve(s.ln)
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	s.log.Info("http server stopped", "server", s.name)
	return nil
}
