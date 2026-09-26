// Package pgtest provides isolated PostgreSQL databases for integration tests. It uses the
// server in JS_TEST_DATABASE_URL if set, otherwise an embedded PostgreSQL 17 that is
// downloaded once and cached.
package pgtest

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

var (
	adminURL string
	template string
	ready    bool
	counter  atomic.Int64
)

// Main runs a package's tests with a migrated template database available; call it from
// TestMain as os.Exit(pgtest.Main(m, migrate)). In -short mode it runs tests without a server.
func Main(m *testing.M, migrate func(ctx context.Context, databaseURL string) error) int {
	flag.Parse()
	if testing.Short() {
		return m.Run()
	}
	stop, err := setup(migrate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		return 1
	}
	defer stop()
	return m.Run()
}

// NewDatabase creates a migrated database for t, dropped when t ends. It skips t in -short mode.
func NewDatabase(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs PostgreSQL (run without -short)")
	}
	if !ready {
		t.Fatal("pgtest: call pgtest.Main from TestMain")
	}
	name := fmt.Sprintf("test_%d_%d", os.Getpid(), counter.Add(1))
	if err := exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, template)); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}
	t.Cleanup(func() {
		if err := exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database: %v", err)
		}
	})
	return withDatabase(adminURL, name)
}

func setup(migrate func(context.Context, string) error) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stopServer := func() {}
	if u := os.Getenv("JS_TEST_DATABASE_URL"); u != "" {
		adminURL = u
	} else {
		var err error
		if adminURL, stopServer, err = startEmbedded(); err != nil {
			return nil, err
		}
	}

	template = fmt.Sprintf("jobscheduler_template_%d", os.Getpid())
	stop := func() {
		_ = exec(context.Background(), "DROP DATABASE IF EXISTS "+template+" WITH (FORCE)")
		stopServer()
	}
	if err := exec(ctx, "CREATE DATABASE "+template); err != nil {
		stop()
		return nil, fmt.Errorf("create template database: %w", err)
	}
	if err := migrate(ctx, withDatabase(adminURL, template)); err != nil {
		stop()
		return nil, fmt.Errorf("migrate template database: %w", err)
	}
	ready = true
	return stop, nil
}

func startEmbedded() (string, func(), error) {
	port, err := freePort()
	if err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp("", "pgtest-")
	if err != nil {
		return "", nil, err
	}
	cached, haveCache := binariesCache()
	binaries := cached
	if !haveCache {
		binaries = filepath.Join(dir, "binaries")
	}
	var logs bytes.Buffer
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V17).
		Port(uint32(port)).
		Username("postgres").
		Password("postgres").
		Database("postgres").
		RuntimePath(filepath.Join(dir, "run")).
		DataPath(filepath.Join(dir, "data")).
		BinariesPath(binaries).
		StartTimeout(2 * time.Minute).
		Logger(&logs))
	if err := pg.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("start embedded postgres: %w\n%s", err, logs.String())
	}
	stop := func() {
		_ = pg.Stop()
		if !haveCache && cached != "" {
			// Publish atomically; if another test process already did, keep theirs.
			if err := os.MkdirAll(filepath.Dir(cached), 0o755); err == nil {
				_ = os.Rename(binaries, cached)
			}
		}
		_ = os.RemoveAll(dir)
	}
	return fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port), stop, nil
}

// binariesCache returns where extracted PostgreSQL binaries are kept between runs, and
// whether they are already there. Extracting the archive takes far longer than starting.
func binariesCache() (string, bool) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", false
	}
	dir := filepath.Join(base, "jobscheduler", "pgtest", fmt.Sprintf("%s-%s-%s", embeddedpostgres.V17, runtime.GOOS, runtime.GOARCH))
	_, err = os.Stat(filepath.Join(dir, "bin", "pg_ctl"))
	return dir, err == nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func exec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, sql)
	return err
}

func withDatabase(serverURL, name string) string {
	u, err := url.Parse(serverURL)
	if err != nil {
		panic(fmt.Sprintf("pgtest: invalid database url: %v", err))
	}
	u.Path = "/" + name
	return u.String()
}
