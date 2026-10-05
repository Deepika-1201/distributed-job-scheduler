package dispatch_test

import (
	"context"
	"crypto/tls"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/domain"
	"jobscheduler/internal/tlsconfig"
	"jobscheduler/internal/tlsconfig/tlstest"
	"jobscheduler/pkg/workerpb"
	"jobscheduler/pkg/workersdk"
)

// A worker that verifies the engine's certificate runs a job over TLS; a plaintext client is refused (ADR-026).
func TestWorkerProtocolOverTLS(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	certFile, keyFile, roots := tlstest.WriteCert(t, t.TempDir(), "engine")
	certs, err := tlsconfig.NewReloader(certFile, keyFile, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	addr := c.engine("node-a", func(cfg *dispatch.Config) { cfg.TLS = certs.Config() })
	c.run(func(ctx context.Context) error {
		return workersdk.Run(ctx, workersdk.Config{Address: addr, Token: token, Pool: "default", Slots: 1,
			Handlers: map[string]workersdk.Handler{
				"email.send": func(context.Context, workersdk.Job) ([]byte, error) { return []byte(`{"tls": true}`), nil },
			},
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(
				&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))},
			PollWait: time.Second, DrainTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
	})
	if job := c.await(c.submit(), domain.StateSucceeded, 15*time.Second); string(job.Result) != `{"tls": true}` {
		t.Errorf("result = %s", job.Result)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), 2*time.Second)
	defer cancel()
	if _, err := workerpb.NewWorkerServiceClient(conn).Register(callCtx,
		&workerpb.RegisterRequest{Pool: "default", Slots: 1, WorkerId: "plaintext"}); err == nil {
		t.Error("a plaintext client registered with a TLS engine")
	}
}
