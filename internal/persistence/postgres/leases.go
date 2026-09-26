package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"jobscheduler/internal/domain"
)

// Lease is a held lease row. Epoch increases with every change of holder and fences the
// holder's writes (LLD §11.2).
type Lease struct {
	Name      string
	Holder    string
	Address   string
	Epoch     int64
	ExpiresAt time.Time
}

const leaseColumns = `name, holder, address, epoch, expires_at`

func scanLease(row pgx.Row) (Lease, error) {
	var l Lease
	err := row.Scan(&l.Name, &l.Holder, &l.Address, &l.Epoch, &l.ExpiresAt)
	return l, err
}

// AcquireLease takes the named lease for holder if it is free or expired. It reports false,
// without error, when another holder's lease is still valid.
func (s *Store) AcquireLease(ctx context.Context, name, holder, address string, ttl time.Duration) (Lease, bool, error) {
	l, err := scanLease(s.pool.QueryRow(ctx, `
		INSERT INTO leases (name, holder, address, epoch, acquired_at, renewed_at, expires_at)
		VALUES ($1, $2, $3, 1, now(), now(), now() + make_interval(secs => $4))
		ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, address = excluded.address,
		    epoch = leases.epoch + 1, acquired_at = now(), renewed_at = now(), expires_at = excluded.expires_at
		WHERE leases.expires_at <= now()
		RETURNING `+leaseColumns, name, holder, address, ttl.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, false, nil
	}
	return l, err == nil, err
}

// RenewLease extends a lease the holder still owns at the same epoch, even after it expired
// if nobody took it over. It returns domain.ErrLeaseLost otherwise.
func (s *Store) RenewLease(ctx context.Context, l Lease, ttl time.Duration) (Lease, error) {
	renewed, err := scanLease(s.pool.QueryRow(ctx, `
		UPDATE leases SET renewed_at = now(), expires_at = now() + make_interval(secs => $4)
		WHERE name = $1 AND holder = $2 AND epoch = $3
		RETURNING `+leaseColumns, l.Name, l.Holder, l.Epoch, ttl.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, domain.ErrLeaseLost
	}
	return renewed, err
}

// ReleaseLease expires a held lease so another node can take it over immediately.
func (s *Store) ReleaseLease(ctx context.Context, l Lease) error {
	_, err := s.pool.Exec(ctx, `UPDATE leases SET expires_at = now() WHERE name = $1 AND holder = $2 AND epoch = $3`,
		l.Name, l.Holder, l.Epoch)
	return err
}

// GetLease returns the current row for a lease, valid or not.
func (s *Store) GetLease(ctx context.Context, name string) (Lease, error) {
	l, err := scanLease(s.pool.QueryRow(ctx, `SELECT `+leaseColumns+` FROM leases WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, domain.ErrNotFound
	}
	return l, err
}

// fenceSQL selects the lease row only while it is held at the given epoch. FOR SHARE makes
// a concurrent takeover wait for this write, and later writes see the new epoch (LLD §11.2).
// Parameters are the lease name, holder and epoch at the given positions.
func fenceSQL(name, holder, epoch string) string {
	return `SELECT 1 FROM leases WHERE name = ` + name + ` AND holder = ` + holder + ` AND epoch = ` + epoch + ` FOR SHARE`
}

func holdsLease(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, l Lease) (bool, error) {
	var held bool
	err := q.QueryRow(ctx, `SELECT EXISTS (`+fenceSQL("$1", "$2", "$3")+`)`, l.Name, l.Holder, l.Epoch).Scan(&held)
	return held, err
}

// PoolLeaseName is the lease that makes its holder the pool's dispatcher.
func PoolLeaseName(pool string) string { return "pool:" + pool }
