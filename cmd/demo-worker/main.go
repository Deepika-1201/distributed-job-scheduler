// Command demo-worker runs example handlers against the job platform:
//
//	JS_WORKER_TOKEN=... demo-worker -addr localhost:7070 -pool default
//
// It handles "email.send" (logs the payload, sleeps briefly, sometimes fails retryably) and
// "demo.sleep" (sleeps for payload.seconds, honouring cancellation and timeouts).
package main

import (
	"context"
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

	"jobscheduler/pkg/workersdk"
)

func main() {
	addr := flag.String("addr", "localhost:7070", "engine worker address")
	pool := flag.String("pool", "default", "pool to serve")
	slots := flag.Int("slots", 4, "concurrent jobs")
	failRate := flag.Float64("fail-rate", 0.2, "share of email.send attempts that fail retryably")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := workersdk.Run(ctx, workersdk.Config{
		Address: *addr,
		Token:   os.Getenv("JS_WORKER_TOKEN"),
		Pool:    *pool,
		Slots:   *slots,
		Logger:  log,
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
