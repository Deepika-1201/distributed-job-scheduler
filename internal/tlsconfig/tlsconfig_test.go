package tlsconfig

import (
	"crypto/tls"
	"log/slog"
	"os"
	"testing"
	"time"

	"jobscheduler/internal/tlsconfig/tlstest"
)

func TestReloadsChangedFilesAtMostEvery30s(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := tlstest.WriteCert(t, dir, "first")
	r, err := NewReloader(certFile, keyFile, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r.now = func() time.Time { return now }
	served := func() string {
		t.Helper()
		c, err := r.GetCertificate(&tls.ClientHelloInfo{})
		if err != nil {
			t.Fatal(err)
		}
		return c.Leaf.Subject.CommonName
	}
	if got := served(); got != "first" {
		t.Fatalf("serving %q", got)
	}

	tlstest.WriteCert(t, dir, "second")
	future := time.Now().Add(time.Minute) // a distinct modification time even on coarse file systems
	for _, f := range []string{certFile, keyFile} {
		if err := os.Chtimes(f, future, future); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(29 * time.Second)
	if got := served(); got != "first" {
		t.Errorf("reloaded after 29 s: serving %q", got)
	}
	now = now.Add(2 * time.Second)
	if got := served(); got != "second" {
		t.Errorf("after 31 s serving %q, want the rewritten certificate", got)
	}

	if err := os.WriteFile(certFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if got := served(); got != "second" {
		t.Errorf("after a bad rewrite serving %q, want the previous certificate", got)
	}
}

func TestNewReloaderFailsOnBadFiles(t *testing.T) {
	if _, err := NewReloader("/nonexistent/cert.pem", "/nonexistent/key.pem", slog.New(slog.DiscardHandler)); err == nil {
		t.Error("missing files loaded")
	}
}

// Certificates passed in the environment serve TLS 1.2+ (LLD §22.3).
func TestFromPEM(t *testing.T) {
	certFile, keyFile, roots := tlstest.WriteCert(t, t.TempDir(), "from-env")
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FromPEM(certPEM, []byte("not a key")); err == nil {
		t.Error("a bad key loaded")
	}
	cfg, err := FromPEM(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: roots, ServerName: "localhost"})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer conn.Close()
	if cn := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; cn != "from-env" || conn.ConnectionState().Version < tls.VersionTLS12 {
		t.Errorf("served %q over version %x", cn, conn.ConnectionState().Version)
	}
}
