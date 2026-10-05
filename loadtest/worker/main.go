// Command worker runs a fleet of SDK workers for the load-test gate (LLD §19.1). Worker i
// serves pool load-(i mod pools) through engine address i mod len(addrs), and each job of type
// load.noop takes -work to finish.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"jobscheduler/pkg/workersdk"
)

func main() {
	addrs := flag.String("addr", "localhost:7070", "comma-separated engine worker addresses")
	pools := flag.Int("pools", 4, "pools, named load-0 to load-(pools-1)")
	workers := flag.Int("workers", 40, "workers in this process")
	slots := flag.Int("slots", 25, "slots per worker")
	work := flag.Duration("work", 20*time.Millisecond, "time each job takes")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler := func(ctx context.Context, _ workersdk.Job) ([]byte, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(*work):
			return nil, nil
		}
	}
	engines := strings.Split(*addrs, ",")
	var wg sync.WaitGroup
	for i := range *workers {
		wg.Go(func() {
			err := workersdk.Run(ctx, workersdk.Config{
				Address:  engines[i%len(engines)],
				Token:    os.Getenv("JS_WORKER_TOKEN"),
				Pool:     fmt.Sprintf("load-%d", i%*pools),
				Slots:    *slots,
				WorkerID: fmt.Sprintf("load-%d-%d", os.Getpid(), i),
				Logger:   log,
				Handlers: map[string]workersdk.Handler{"load.noop": handler},
			})
			if err != nil && !errors.Is(err, workersdk.ErrDrained) {
				log.Error("worker stopped", "worker", i, "error", err)
				stop()
			}
		})
	}
	wg.Wait()
}
