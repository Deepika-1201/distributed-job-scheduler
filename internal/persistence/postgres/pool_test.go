package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewPoolDoesNotConnectEagerly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Port 1 on loopback refuses connections, standing in for a database that is down.
	pool, err := NewPool(ctx, "postgres://jobs:secret@127.0.0.1:1/jobs?connect_timeout=1", 4)
	if err != nil {
		t.Fatalf("NewPool = %v, want lazy pool creation to succeed", err)
	}
	defer pool.Close()

	if got := pool.Config().MaxConns; got != 4 {
		t.Errorf("MaxConns = %d, want 4", got)
	}
	if err := pool.Ping(ctx); err == nil {
		t.Error("Ping succeeded against an unreachable database")
	}
}

func TestNewPoolRejectsInvalidURLWithoutLeakingPassword(t *testing.T) {
	_, err := NewPool(context.Background(), "postgres://jobs:s3cret@db:notaport/jobs", 4)
	if err == nil {
		t.Fatal("NewPool accepted an invalid URL")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks the password: %v", err)
	}
}
