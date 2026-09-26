package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"jobscheduler/internal/domain"
)

const operationColumns = `id, tenant_id, kind, filter, state, succeeded, skipped, failed, last_error, created_by,
	request_id, created_at, updated_at, finished_at`

// maxOperationErrors ends an operation after this many consecutive failed batches.
const maxOperationErrors = 10

func scanOperation(row pgx.Row, extra ...any) (domain.Operation, error) {
	var (
		op                 domain.Operation
		id, tenant         pgtype.UUID
		kind, state        string
		lastErr, requestID pgtype.Text
		finished           pgtype.Timestamptz
	)
	err := row.Scan(append([]any{&id, &tenant, &kind, &op.Filter, &state, &op.Succeeded, &op.Skipped, &op.Failed,
		&lastErr, &op.CreatedBy, &requestID, &op.CreatedAt, &op.UpdatedAt, &finished}, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Operation{}, domain.ErrNotFound
	}
	op.ID, op.TenantID = uuidString(id), domain.TenantID(uuidString(tenant))
	op.Kind, op.State = domain.OperationKind(kind), domain.OperationState(state)
	op.LastError, op.RequestID, op.FinishedAt = lastErr.String, requestID.String, finished.Time
	return op, err
}

// CreateOperation records a validated bulk operation. It covers the jobs created before it.
func (s *Store) CreateOperation(ctx context.Context, op domain.Operation, audit Audit) (domain.Operation, error) {
	tenant, ok := canonicalUUID(string(op.TenantID))
	if !ok {
		return domain.Operation{}, domain.ErrNotFound
	}
	var created domain.Operation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		created, err = scanOperation(tx.QueryRow(ctx, `
			INSERT INTO operations (id, tenant_id, kind, filter, state, cursor_created_at, cursor_id, created_by, request_id)
			VALUES ($1, $2, $3, $4, 'PENDING', now(), 'ffffffff-ffff-ffff-ffff-ffffffffffff', $5, $6)
			RETURNING `+operationColumns,
			newID(), tenant, string(op.Kind), op.Filter, op.CreatedBy, nullText(op.RequestID)))
		if err != nil {
			return err
		}
		return writeAudit(ctx, tx, tenant, audit, "operation.create", created.ID, map[string]any{"kind": op.Kind, "filter": op.Filter})
	})
	return created, err
}

func (s *Store) GetOperation(ctx context.Context, tenantID domain.TenantID, id string) (domain.Operation, error) {
	tenant, ok1 := canonicalUUID(string(tenantID))
	opID, ok2 := canonicalUUID(id)
	if !ok1 || !ok2 {
		return domain.Operation{}, domain.ErrNotFound
	}
	return scanOperation(s.pool.QueryRow(ctx, `SELECT `+operationColumns+` FROM operations WHERE id = $1 AND tenant_id = $2`, opID, tenant))
}

// ProcessOperation runs one batch of the oldest open operation not locked by another node,
// and reports whether there was one. The operation row stays locked for the batch, and its
// cursor and counts are saved in the same transaction (LLD §13.3).
func (s *Store) ProcessOperation(ctx context.Context, batch int) (bool, error) {
	found := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var cursor JobCursor
		var cursorID pgtype.UUID
		op, err := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+`, cursor_created_at, cursor_id FROM operations
			WHERE state IN ('PENDING', 'RUNNING') ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`), &cursor.CreatedAt, &cursorID)
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		cursor.ID = domain.JobID(uuidString(cursorID))
		res := s.runBatch(ctx, op, cursor, batch)

		state, errText, errorsSQL := string(domain.OperationRunning), pgtype.Text{}, "0"
		switch {
		case res.err != nil:
			errText = pgtype.Text{String: truncateText(res.err.Error(), 1000), Valid: true}
			errorsSQL = "consecutive_errors + 1"
		case res.done:
			state = string(domain.OperationSucceeded)
		}
		next := res.cursor
		_, err = tx.Exec(ctx, fmt.Sprintf(`
			UPDATE operations SET cursor_created_at = $2, cursor_id = $3,
			    succeeded = succeeded + $4, skipped = skipped + $5, failed = failed + $6,
			    consecutive_errors = %[1]s, last_error = COALESCE($7, last_error),
			    state = CASE WHEN %[1]s >= %[2]d THEN 'FAILED' ELSE $8 END,
			    finished_at = CASE WHEN $8 = 'SUCCEEDED' OR %[1]s >= %[2]d THEN now() END,
			    updated_at = now()
			WHERE id = $1`, errorsSQL, maxOperationErrors),
			op.ID, next.CreatedAt, string(next.ID), res.succeeded, res.skipped, res.failed, errText, state)
		return err
	})
	return found, err
}

type batchResult struct {
	cursor                     JobCursor
	succeeded, skipped, failed int
	done                       bool
	err                        error
}

// runBatch applies the operation to the next page of matching jobs, newest first. It stops
// at the first unexpected error, leaving the cursor at the last job it handled.
func (s *Store) runBatch(ctx context.Context, op domain.Operation, from JobCursor, batch int) batchResult {
	res := batchResult{cursor: from}
	f := JobFilter{State: op.Filter.State, Type: op.Filter.Type, ScheduleID: op.Filter.ScheduleID,
		LabelKey: op.Filter.LabelKey, LabelValue: op.Filter.LabelValue, Limit: batch, After: &from,
		ActiveOnly: op.Kind == domain.OperationCancel}
	jobs, next, err := s.ListJobs(ctx, op.TenantID, f)
	if err != nil {
		res.err = err
		return res
	}
	audit := Audit{Actor: "operation:" + op.ID, RequestID: op.RequestID}
	for _, j := range jobs {
		var err error
		switch {
		case op.Kind == domain.OperationCancel && !j.CancelRequestedAt.IsZero():
			err = domain.ErrInvalidTransition // already being cancelled
		case op.Kind == domain.OperationCancel:
			_, err = s.RequestCancel(ctx, op.TenantID, j.ID, audit)
		default:
			_, err = s.RetryJob(ctx, op.TenantID, j.ID, audit)
		}
		switch {
		case err == nil:
			res.succeeded++
		case errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrNotFound):
			res.skipped++ // no longer eligible, e.g. finished or already re-driven meanwhile
		case errors.Is(err, domain.ErrConflict):
			res.failed++ // e.g. an active job already holds the re-driven job's dedupe key
		default:
			res.err = err
			return res
		}
		res.cursor = JobCursor{CreatedAt: j.CreatedAt, ID: j.ID}
	}
	res.done = next == nil
	return res
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
