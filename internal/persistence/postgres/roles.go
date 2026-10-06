package postgres

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureLoginRole creates role as a login member of jobscheduler_runtime if it is missing and
// sets its password (LLD §22.5). The password leaves the process only as a SCRAM-SHA-256
// verifier, so the database's statement logs never hold it.
func EnsureLoginRole(ctx context.Context, pool *pgxpool.Pool, role, password string) error {
	verifier, err := scramVerifier(password)
	if err != nil {
		return err
	}
	ident := pgx.Identifier{role}.Sanitize()
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(ctx, `CREATE ROLE `+ident+` LOGIN`); err != nil {
				return fmt.Errorf("create role %s: %w", role, err)
			}
		}
		// DDL takes no parameters; the verifier holds only base64 characters, '$' and ':'.
		if _, err := tx.Exec(ctx, `ALTER ROLE `+ident+` WITH LOGIN PASSWORD '`+verifier+`'`); err != nil {
			return fmt.Errorf("set the password of %s: %w", role, err)
		}
		if _, err := tx.Exec(ctx, `GRANT jobscheduler_runtime TO `+ident); err != nil {
			return fmt.Errorf("grant jobscheduler_runtime to %s: %w", role, err)
		}
		return nil
	})
}

const scramIterations = 4096

// scramVerifier computes the verifier PostgreSQL stores for a SCRAM-SHA-256 password (RFC 7677).
func scramVerifier(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, scramIterations, sha256.Size)
	if err != nil {
		return "", err
	}
	mac := func(key []byte, msg string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(msg))
		return h.Sum(nil)
	}
	stored := sha256.Sum256(mac(salted, "Client Key"))
	b64 := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", scramIterations, b64(salt), b64(stored[:]), b64(mac(salted, "Server Key"))), nil
}
