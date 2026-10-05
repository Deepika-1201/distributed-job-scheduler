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
