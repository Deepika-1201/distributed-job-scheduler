package dispatch

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"jobscheduler/internal/domain"
)

const (
	authCacheTTL  = 30 * time.Second
	touchInterval = time.Minute
)

var errUnauthenticated = errors.New("a valid worker token is required")

// scope is what a worker's token authorizes (ADR-025). The zero value authorizes nothing.
type scope struct {
	all     bool   // the cluster token
	pool    string // a pool token's pool
	tokenID string
}

func (s scope) allows(pool string) bool { return s.all || (s.pool != "" && s.pool == pool) }

// storePool is the pool to pass to store calls: empty, matching any, for the cluster token. A
// zero scope gets a name no pool can have, so it matches nothing.
func (s scope) storePool() string {
	switch {
	case s.all:
		return ""
	case s.pool == "":
		return "-"
	}
	return s.pool
}

type scopeKey struct{}

func scopeOf(ctx context.Context) scope {
	s, _ := ctx.Value(scopeKey{}).(scope)
	return s
}

type tokenStore interface {
	FindWorkerToken(ctx context.Context, prefix string) (domain.WorkerToken, error)
	TouchWorkerToken(ctx context.Context, id string) error
}

// workerAuth verifies worker tokens: the optional cluster token, or per-pool tokens issued
// through the API. Successful checks are cached for authCacheTTL.
type workerAuth struct {
	cluster []byte
	tokens  tokenStore
	now     func() time.Time
	log     *slog.Logger

	mu      sync.Mutex
	cache   map[[32]byte]cached
	touched map[string]time.Time // token ID → when its last use was last written
}

type cached struct {
	scope   scope
	expires time.Time
}

func newWorkerAuth(cluster string, tokens tokenStore, now func() time.Time, log *slog.Logger) *workerAuth {
	a := &workerAuth{tokens: tokens, now: now, log: log, cache: map[[32]byte]cached{}, touched: map[string]time.Time{}}
	if cluster != "" {
		a.cluster = []byte(cluster)
	}
	return a
}

func (a *workerAuth) authenticate(ctx context.Context, header string) (scope, error) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		return scope{}, errUnauthenticated
	}
	if a.cluster != nil && subtle.ConstantTimeCompare([]byte(token), a.cluster) == 1 {
		return scope{all: true}, nil
	}
	parts := strings.Split(token, "_")
	if len(parts) != 3 || parts[0] != "jsw" {
		return scope{}, errUnauthenticated
	}
	sum := sha256.Sum256([]byte(token))
	now := a.now()
	a.mu.Lock()
	c, hit := a.cache[sum]
	a.mu.Unlock()
	if hit && now.Before(c.expires) {
		return c.scope, nil
	}

	t, err := a.tokens.FindWorkerToken(ctx, parts[1])
	if errors.Is(err, domain.ErrNotFound) {
		return scope{}, errUnauthenticated
	}
	if err != nil {
		return scope{}, err
	}
	if subtle.ConstantTimeCompare(t.SecretHash, sum[:]) != 1 || !t.Usable(now) {
		return scope{}, errUnauthenticated
	}
	s := scope{pool: t.Pool, tokenID: t.ID}
	expires := now.Add(authCacheTTL)
	if !t.ExpiresAt.IsZero() && t.ExpiresAt.Before(expires) {
		expires = t.ExpiresAt
	}
	a.mu.Lock()
	a.cache[sum] = cached{scope: s, expires: expires}
	touch := now.Sub(a.touched[t.ID]) >= touchInterval
	if touch {
		a.touched[t.ID] = now
	}
	a.mu.Unlock()
	if touch {
		go a.touch(t.ID)
	}
	return s, nil
}

// touch records a token's last use; losing one write only makes last_used_at a minute staler.
func (a *workerAuth) touch(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.tokens.TouchWorkerToken(ctx, id); err != nil {
		a.log.Warn("recording a worker token's last use failed", "token_id", id, "error", err)
	}
}
