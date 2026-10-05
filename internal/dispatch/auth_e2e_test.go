package dispatch_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"jobscheduler/internal/api"
	"jobscheduler/internal/dispatch"
	"jobscheduler/internal/persistence/postgres"
	"jobscheduler/pkg/workerpb"
)

// A pool token admits workers to its own pool only (ADR-025).
func TestPoolTokensAreScopedToTheirPool(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	noClusterToken := func(cfg *dispatch.Config) { cfg.Token = "" }
	addr := c.engine("node-a", noClusterToken)
	issue := func(pool string) (string, string) {
		plaintext, tok := api.NewWorkerToken(pool, pool+" fleet", time.Time{})
		if _, err := c.store.CreateWorkerToken(ctx, tok, c.tenant, postgres.Audit{Actor: "test"}); err != nil {
			t.Fatal(err)
		}
		return plaintext, tok.ID
	}
	own, ownID := issue("default")
	other, _ := issue("batch")

	client := func(addr string) workerpb.WorkerServiceClient {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return workerpb.NewWorkerServiceClient(conn)
	}
	as := func(tok string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
	}
	want := func(what string, err error, code codes.Code) {
		t.Helper()
		if status.Code(err) != code {
			t.Errorf("%s: %v, want %s", what, err, code)
		}
	}
	register := func(c workerpb.WorkerServiceClient, tok, worker string) (*workerpb.RegisterResponse, error) {
		return c.Register(as(tok), &workerpb.RegisterRequest{Pool: "default", Slots: 1, WorkerId: worker})
	}

	a := client(addr)
	reg, err := register(a, own, "own")
	if err != nil {
		t.Fatalf("register with the pool's token: %v", err)
	}
	_, err = a.Heartbeat(as(own), &workerpb.HeartbeatRequest{SessionId: reg.SessionId})
	want("heartbeat with the pool's token", err, codes.OK)

	_, err = register(a, other, "intruder")
	want("register with another pool's token", err, codes.PermissionDenied)
	_, err = a.Heartbeat(as(other), &workerpb.HeartbeatRequest{SessionId: reg.SessionId})
	want("heartbeat on another pool's session", err, codes.NotFound)
	_, err = a.Poll(as(other), &workerpb.PollRequest{SessionId: reg.SessionId})
	want("poll on another pool's session", err, codes.PermissionDenied)
	_, err = a.Deregister(as(other), &workerpb.DeregisterRequest{SessionId: reg.SessionId})
	want("deregister another pool's session", err, codes.NotFound)

	_, err = register(a, token, "cluster")
	want("register with a cluster token the engine was not given", err, codes.Unauthenticated)
	_, err = register(a, "jsw_nope_x", "forged")
	want("register with an unknown pool token", err, codes.Unauthenticated)

	if err := c.store.RevokeWorkerToken(ctx, "default", ownID, c.tenant, postgres.Audit{Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	// node-a may serve the token from its cache for up to 30 s; a node that has not seen it refuses at once.
	_, err = register(client(c.engine("node-b", noClusterToken)), own, "revoked")
	want("register with a revoked token", err, codes.Unauthenticated)
}

// The cluster token still serves every pool.
func TestClusterTokenServesEveryPool(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	conn, err := grpc.NewClient(c.engine("node-a"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	for _, pool := range []string{"default", "batch"} {
		if _, err := workerpb.NewWorkerServiceClient(conn).Register(callCtx,
			&workerpb.RegisterRequest{Pool: pool, Slots: 1, WorkerId: pool}); err != nil {
			t.Errorf("register in %s with the cluster token: %v", pool, err)
		}
	}
}
