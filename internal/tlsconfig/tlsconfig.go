// Package tlsconfig serves a certificate from files and reloads it when they change (ADR-026).
package tlsconfig

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// checkInterval spaces the checks for changed files; they run during handshakes.
const checkInterval = 30 * time.Second

type Reloader struct {
	certFile, keyFile string
	log               *slog.Logger
	now               func() time.Time

	mu              sync.Mutex
	cert            *tls.Certificate
	certMod, keyMod time.Time
	checked         time.Time
}

// NewReloader loads the key pair, failing if it doesn't load.
func NewReloader(certFile, keyFile string, log *slog.Logger) (*Reloader, error) {
	r := &Reloader{certFile: certFile, keyFile: keyFile, log: log, now: time.Now}
	if err := r.load(); err != nil {
		return nil, err
	}
	r.checked = r.now()
	return r, nil
}

// Config returns a server configuration for TLS 1.2 and later that serves the current certificate.
func (r *Reloader) Config() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: r.GetCertificate}
}

// GetCertificate returns the current certificate, reloading it first if the files changed
// since the last check, at most every 30 s. A pair that fails to load leaves the previous one in use.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now := r.now(); now.Sub(r.checked) >= checkInterval {
		r.checked = now
		certMod, keyMod, err := r.modTimes()
		if err == nil && (!certMod.Equal(r.certMod) || !keyMod.Equal(r.keyMod)) {
			if err = r.load(); err == nil {
				r.log.Info("TLS certificate reloaded", "cert_file", r.certFile, "not_after", r.cert.Leaf.NotAfter)
			}
		}
		if err != nil {
			r.log.Warn("TLS certificate reload failed; serving the previous one", "error", err)
		}
	}
	return r.cert, nil
}

func (r *Reloader) load() error {
	certMod, keyMod, err := r.modTimes()
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load TLS key pair: %w", err)
	}
	r.cert, r.certMod, r.keyMod = &cert, certMod, keyMod
	return nil
}

func (r *Reloader) modTimes() (cert, key time.Time, err error) {
	c, err := os.Stat(r.certFile)
	if err != nil {
		return cert, key, err
	}
	k, err := os.Stat(r.keyFile)
	if err != nil {
		return cert, key, err
	}
	return c.ModTime(), k.ModTime(), nil
}
