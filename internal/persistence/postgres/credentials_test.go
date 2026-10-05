package postgres

import (
	"errors"
	"testing"
	"time"

	"jobscheduler/internal/domain"
)

func TestWorkerTokensRoundTrip(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tok := domain.WorkerToken{ID: newID(), Pool: "default", Name: "default workers", Prefix: "wtprefix", SecretHash: []byte("hash")}
	created, err := f.store.CreateWorkerToken(ctx, tok, f.tenant, testAudit)
	if err != nil || created.CreatedAt.IsZero() {
		t.Fatalf("CreateWorkerToken = %+v, %v", created, err)
	}
	found, err := f.store.FindWorkerToken(ctx, "wtprefix")
	if err != nil || found.ID != tok.ID || found.Pool != "default" || string(found.SecretHash) != "hash" || !found.Usable(time.Now()) {
		t.Fatalf("FindWorkerToken = %+v, %v", found, err)
	}
	if list, err := f.store.ListWorkerTokens(ctx, "default"); err != nil || len(list) != 1 {
		t.Errorf("ListWorkerTokens(default) = %d tokens, %v; want 1", len(list), err)
	}
	if list, _ := f.store.ListWorkerTokens(ctx, "batch"); len(list) != 0 {
		t.Errorf("ListWorkerTokens(batch) = %d tokens, want none", len(list))
	}
	if err := f.store.RevokeWorkerToken(ctx, "batch", tok.ID, f.tenant, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("revoke through another pool = %v, want ErrNotFound", err)
	}
	if err := f.store.TouchWorkerToken(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RevokeWorkerToken(ctx, "default", tok.ID, f.tenant, testAudit); err != nil {
		t.Fatal(err)
	}
	found, _ = f.store.FindWorkerToken(ctx, "wtprefix")
	if found.Usable(time.Now()) || found.LastUsedAt.IsZero() {
		t.Errorf("after revoke and touch: %+v, want unusable with a last use", found)
	}
	if n := f.count(`SELECT count(*) FROM audit_log WHERE action IN ('worker_token.create', 'worker_token.revoke')`); n != 2 {
		t.Errorf("%d audit rows, want 2", n)
	}
}

func TestRotateAPIKeyKeepsTheOldKeyUntilGraceEnds(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	old := domain.APIKey{ID: newID(), TenantID: f.tenant, Name: "ci", Prefix: "oldkey01", SecretHash: []byte("h1"), Role: domain.RoleSubmitter}
	if _, err := f.store.CreateAPIKey(ctx, old, nil); err != nil {
		t.Fatal(err)
	}
	next := domain.APIKey{ID: newID(), TenantID: f.tenant, Name: "ci", Prefix: "newkey01", SecretHash: []byte("h2"), Role: domain.RoleSubmitter}
	graceEnds := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := f.store.RotateAPIKey(ctx, f.addTenant("globex"), old.ID, next, graceEnds, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("rotate another tenant's key = %v, want ErrNotFound", err)
	}
	if _, err := f.store.RotateAPIKey(ctx, f.tenant, old.ID, next, graceEnds, testAudit); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.GetAPIKey(ctx, f.tenant, old.ID)
	if err != nil || !got.ExpiresAt.Equal(graceEnds) || !got.Usable(time.Now()) {
		t.Errorf("old key after rotation = %+v, %v; want usable until %s", got, err, graceEnds)
	}
	keys, err := f.store.ListAPIKeys(ctx, f.tenant)
	if err != nil || len(keys) < 2 || keys[0].ID != next.ID {
		t.Errorf("ListAPIKeys = %d keys, newest %v, %v; want the replacement first", len(keys), keys, err)
	}
	if err := f.store.TouchAPIKey(ctx, next.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.store.GetAPIKey(ctx, f.tenant, next.ID); got.LastUsedAt.IsZero() {
		t.Error("last use was not recorded")
	}

	// A shorter existing expiry wins, and a revoked key can't be rotated.
	sooner := time.Now().Add(time.Minute).Truncate(time.Microsecond)
	f.exec(`UPDATE api_keys SET expires_at = $2 WHERE id = $1`, next.ID, sooner)
	third := domain.APIKey{ID: newID(), TenantID: f.tenant, Name: "ci", Prefix: "thrdkey1", SecretHash: []byte("h3"), Role: domain.RoleSubmitter}
	if _, err := f.store.RotateAPIKey(ctx, f.tenant, next.ID, third, graceEnds, testAudit); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.store.GetAPIKey(ctx, f.tenant, next.ID); !got.ExpiresAt.Equal(sooner) {
		t.Errorf("expiry after rotation = %s, want the sooner %s kept", got.ExpiresAt, sooner)
	}
	if err := f.store.RevokeAPIKey(ctx, f.tenant, third.ID, testAudit); err != nil {
		t.Fatal(err)
	}
	fourth := domain.APIKey{ID: newID(), TenantID: f.tenant, Name: "ci", Prefix: "frthkey1", SecretHash: []byte("h4"), Role: domain.RoleSubmitter}
	if _, err := f.store.RotateAPIKey(ctx, f.tenant, third.ID, fourth, graceEnds, testAudit); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("rotate a revoked key = %v, want ErrNotFound", err)
	}
}
