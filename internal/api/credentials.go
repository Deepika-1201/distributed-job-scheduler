package api

import (
	"net/http"
	"time"

	"jobscheduler/internal/domain"
)

const (
	defaultKeyGrace = 24 * time.Hour
	maxKeyGrace     = 30 * 24 * time.Hour
)

type apiKeySummary struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	Role       domain.Role `json:"role"`
	CreatedAt  time.Time   `json:"created_at"`
	ExpiresAt  *time.Time  `json:"expires_at,omitempty"`
	RevokedAt  *time.Time  `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time  `json:"last_used_at,omitempty"`
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request, p principal) error {
	keys, err := s.store.ListAPIKeys(r.Context(), p.Tenant)
	if err != nil {
		return err
	}
	items := make([]apiKeySummary, len(keys))
	for i, k := range keys {
		items[i] = apiKeySummary{ID: k.ID, Name: k.Name, Role: k.Role, CreatedAt: k.CreatedAt,
			ExpiresAt: optTime(k.ExpiresAt), RevokedAt: optTime(k.RevokedAt), LastUsedAt: optTime(k.LastUsedAt)}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}

type rotateKeyRequest struct {
	Grace     string     `json:"grace"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// rotateAPIKey issues a key with the same name and role, and lets the old one expire after a
// grace period (LLD §20.2).
func (s *Server) rotateAPIKey(w http.ResponseWriter, r *http.Request, p principal) error {
	var req rotateKeyRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	fe := fieldErrors{}
	grace := defaultKeyGrace
	if req.Grace != "" {
		d, err := time.ParseDuration(req.Grace)
		if err != nil || d < 0 || d > maxKeyGrace {
			fe.add("grace", "must be a duration between 0s and %s", maxKeyGrace)
		}
		grace = d
	}
	var expires time.Time
	if req.ExpiresAt != nil {
		if expires = *req.ExpiresAt; !expires.After(s.now()) {
			fe.add("expires_at", "must be in the future")
		}
	}
	if err := fe.err(); err != nil {
		return err
	}
	old, err := s.store.GetAPIKey(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		return err
	}
	if !p.Role.Includes(old.Role) {
		return errPermission("a key cannot rotate a key with more than its own role (%s)", p.Role)
	}
	plaintext, next := NewAPIKey(p.Tenant, old.Name, old.Role, expires)
	created, err := s.store.RotateAPIKey(r.Context(), p.Tenant, old.ID, next, s.now().Add(grace), s.audit(r, p))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, apiKeyResponse{
		ID: created.ID, Name: created.Name, Role: created.Role, Key: plaintext,
		CreatedAt: created.CreatedAt, ExpiresAt: optTime(created.ExpiresAt),
	})
	return nil
}

type workerTokenRequest struct {
	Name      string     `json:"name"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type workerTokenResponse struct {
	ID         string     `json:"id"`
	Pool       string     `json:"pool"`
	Name       string     `json:"name"`
	Token      string     `json:"token,omitempty"` // only when issued
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

func toWorkerTokenResponse(t domain.WorkerToken) workerTokenResponse {
	return workerTokenResponse{ID: t.ID, Pool: t.Pool, Name: t.Name, CreatedAt: t.CreatedAt,
		ExpiresAt: optTime(t.ExpiresAt), RevokedAt: optTime(t.RevokedAt), LastUsedAt: optTime(t.LastUsedAt)}
}

func poolParam(r *http.Request) (string, error) {
	pool := r.PathValue("name")
	if !namePattern.MatchString(pool) {
		return "", fieldErrors{"name": "must match " + namePattern.String()}.err()
	}
	return pool, nil
}

// createWorkerToken issues a token for one pool's workers (ADR-025).
func (s *Server) createWorkerToken(w http.ResponseWriter, r *http.Request, p principal) error {
	pool, err := poolParam(r)
	if err != nil {
		return err
	}
	var req workerTokenRequest
	if _, err := readJSON(r, &req); err != nil {
		return err
	}
	fe := fieldErrors{}
	if req.Name == "" || len(req.Name) > 100 {
		fe.add("name", "must be 1-100 characters")
	}
	var expires time.Time
	if req.ExpiresAt != nil {
		if expires = *req.ExpiresAt; !expires.After(s.now()) {
			fe.add("expires_at", "must be in the future")
		}
	}
	if err := fe.err(); err != nil {
		return err
	}
	plaintext, tok := NewWorkerToken(pool, req.Name, expires)
	created, err := s.store.CreateWorkerToken(r.Context(), tok, p.Tenant, s.audit(r, p))
	if err != nil {
		return err
	}
	resp := toWorkerTokenResponse(created)
	resp.Token = plaintext
	writeJSON(w, http.StatusCreated, resp)
	return nil
}

func (s *Server) listWorkerTokens(w http.ResponseWriter, r *http.Request, _ principal) error {
	pool, err := poolParam(r)
	if err != nil {
		return err
	}
	tokens, err := s.store.ListWorkerTokens(r.Context(), pool)
	if err != nil {
		return err
	}
	items := make([]workerTokenResponse, len(tokens))
	for i, t := range tokens {
		items[i] = toWorkerTokenResponse(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}

func (s *Server) revokeWorkerToken(w http.ResponseWriter, r *http.Request, p principal) error {
	pool, err := poolParam(r)
	if err != nil {
		return err
	}
	if err := s.store.RevokeWorkerToken(r.Context(), pool, r.PathValue("id"), p.Tenant, s.audit(r, p)); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
