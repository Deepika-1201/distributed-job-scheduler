package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"jobscheduler/internal/domain"
)

// principal is the authenticated caller; the tenant comes only from the API key.
type principal struct {
	KeyID  string
	Tenant domain.TenantID
	Role   domain.Role
}

var keyEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func randomToken(bytes int) string {
	b := make([]byte, bytes)
	_, _ = rand.Read(b)
	return keyEncoding.EncodeToString(b)
}

// NewAPIKey returns a key's plaintext, to show once, and the form to store (LLD §9.3).
func NewAPIKey(tenant domain.TenantID, name string, role domain.Role, expires time.Time) (string, domain.APIKey) {
	prefix := randomToken(5)
	plaintext := "jsk_" + prefix + "_" + randomToken(32)
	sum := sha256.Sum256([]byte(plaintext))
	return plaintext, domain.APIKey{
		ID: uuid.Must(uuid.NewV7()).String(), TenantID: tenant, Name: name, Prefix: prefix,
		SecretHash: sum[:], Role: role, ExpiresAt: expires,
	}
}

type keyFinder interface {
	FindAPIKey(ctx context.Context, prefix string) (domain.APIKey, error)
}

// authenticator verifies API keys, caching successes briefly so most requests skip the database.
type authenticator struct {
	keys  keyFinder
	now   func() time.Time
	ttl   time.Duration
	mu    sync.Mutex
	cache map[[32]byte]cachedPrincipal
}

type cachedPrincipal struct {
	p       principal
	expires time.Time
}

func newAuthenticator(keys keyFinder, now func() time.Time) *authenticator {
	return &authenticator{keys: keys, now: now, ttl: 30 * time.Second, cache: map[[32]byte]cachedPrincipal{}}
}

func (a *authenticator) authenticate(ctx context.Context, header string) (principal, error) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	parts := strings.Split(token, "_")
	if !ok || len(parts) != 3 || parts[0] != "jsk" {
		return principal{}, errUnauthenticated
	}
	sum := sha256.Sum256([]byte(token))
	now := a.now()

	a.mu.Lock()
	cached, hit := a.cache[sum]
	a.mu.Unlock()
	if hit && now.Before(cached.expires) {
		return cached.p, nil
	}

	key, err := a.keys.FindAPIKey(ctx, parts[1])
	if errors.Is(err, domain.ErrNotFound) {
		return principal{}, errUnauthenticated
	}
	if err != nil {
		return principal{}, err
	}
	if subtle.ConstantTimeCompare(key.SecretHash, sum[:]) != 1 || !key.Usable(now) {
		return principal{}, errUnauthenticated
	}

	p := principal{KeyID: key.ID, Tenant: key.TenantID, Role: key.Role}
	expires := now.Add(a.ttl)
	if !key.ExpiresAt.IsZero() && key.ExpiresAt.Before(expires) {
		expires = key.ExpiresAt
	}
	a.mu.Lock()
	a.cache[sum] = cachedPrincipal{p: p, expires: expires}
	a.mu.Unlock()
	return p, nil
}

// rateLimiter is a token bucket per tenant for this node (LLD §9.6).
type rateLimiter struct {
	rate, burst float64
	now         func() time.Time
	mu          sync.Mutex
	buckets     map[domain.TenantID]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(rate float64, now func() time.Time) *rateLimiter {
	return &rateLimiter{rate: rate, burst: 2 * rate, now: now, buckets: map[domain.TenantID]*bucket{}}
}

// allow takes a token for tenant, or reports how long until one is available.
func (l *rateLimiter) allow(tenant domain.TenantID) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[tenant]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[tenant] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}
