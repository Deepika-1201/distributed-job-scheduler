// Command demo-worker runs example handlers against the job platform:
//
//	JS_WORKER_TOKEN=... demo-worker -addr localhost:7070 -pool default [-tls-ca ca.pem [-tls-server-name engine.example.internal]]
//
// JS_WORKER_TOKEN is the pool's worker token or the cluster token. JS_ENGINE_ADDR and
// JS_WORKER_POOL set the defaults of -addr and -pool.
//
// It handles "email.send" (logs the payload, sleeps briefly, sometimes fails retryably) and
// "demo.sleep" (sleeps for payload.seconds, honouring cancellation and timeouts).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"jobscheduler/pkg/workersdk"
)

func main() {
	addr := flag.String("addr", envOr("JS_ENGINE_ADDR", "localhost:7070"), "engine worker address")
	pool := flag.String("pool", envOr("JS_WORKER_POOL", "default"), "pool to serve")
	slots := flag.Int("slots", 4, "concurrent jobs")
	failRate := flag.Float64("fail-rate", 0.2, "share of email.send attempts that fail retryably")
	tlsCA := flag.String("tls-ca", "", "PEM file of the CA that signed the engines' certificate; turns on TLS (or set JS_WORKER_TLS_CA to the PEM)")
	serverName := flag.String("tls-server-name", os.Getenv("JS_WORKER_TLS_SERVER_NAME"),
		"name to verify in the engines' certificate, when redirects point at addresses it doesn't list")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var dial []grpc.DialOption
	caPEM, err := []byte(os.Getenv("JS_WORKER_TLS_CA")), error(nil)
	if *tlsCA != "" {
		caPEM, err = os.ReadFile(*tlsCA)
	}
	if len(caPEM) > 0 || err != nil {
		roots := x509.NewCertPool()
		if err != nil || !roots.AppendCertsFromPEM(caPEM) {
			log.Error("reading the TLS CA: no PEM certificates", "file", *tlsCA, "error", err)
			os.Exit(1)
		}
		dial = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{RootCAs: roots, ServerName: *serverName, MinVersion: tls.VersionTLS12}))}
	}

	err = workersdk.Run(ctx, workersdk.Config{
		Address:     *addr,
		Token:       os.Getenv("JS_WORKER_TOKEN"),
		Pool:        *pool,
		Slots:       *slots,
		Logger:      log,
		DialOptions: dial,
		Handlers: map[string]workersdk.Handler{
			"email.send": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
				log.Info("sending email", "job_id", job.ID, "attempt", job.Attempt, "payload", string(job.Payload))
				if !sleep(ctx, time.Duration(100+rand.IntN(400))*time.Millisecond) {
					return nil, ctx.Err()
				}
				if rand.Float64() < *failRate {
					return nil, errors.New("simulated transient SMTP failure")
				}
				return json.Marshal(map[string]any{"delivered_at": time.Now().UTC()})
			},
			"demo.sleep": func(ctx context.Context, job workersdk.Job) ([]byte, error) {
				var p struct {
					Seconds float64 `json:"seconds"`
				}
				if err := json.Unmarshal(job.Payload, &p); err != nil {
					return nil, workersdk.Permanent(fmt.Errorf("payload: %w", err))
				}
				if !sleep(ctx, time.Duration(p.Seconds*float64(time.Second))) {
					return nil, ctx.Err()
				}
				return []byte(`{"slept": true}`), nil
			},
		},
	})
	if errors.Is(err, workersdk.ErrDrained) {
		log.Info("worker drained by an operator")
		return
	}
	if err != nil {
		log.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
