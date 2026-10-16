package webhooks

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct{ db *pgxpool.Pool }

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

var (
	_ Store         = (*Repository)(nil)
	_ EndpointStore = (*Repository)(nil)
)

type scanner interface{ Scan(dest ...any) error }

const endpointCols = `id::text, owner_id, url, secret, event_types, active, created_at`

func scanEndpoint(s scanner) (Endpoint, error) {
	var e Endpoint
	err := s.Scan(&e.ID, &e.OwnerID, &e.URL, &e.Secret, &e.EventTypes, &e.Active, &e.CreatedAt)
	return e, err
}

func (r *Repository) Create(ctx context.Context, ep Endpoint) (Endpoint, error) {
	row := r.db.QueryRow(ctx,
		`INSERT INTO webhook_endpoints (owner_id, url, secret, event_types)
		 VALUES ($1,$2,$3,$4) RETURNING `+endpointCols,
		ep.OwnerID, ep.URL, ep.Secret, ep.EventTypes)
	return scanEndpoint(row)
}

func (r *Repository) ListByOwner(ctx context.Context, ownerID string) ([]Endpoint, error) {
	rows, err := r.db.Query(ctx,
		`SELECT `+endpointCols+` FROM webhook_endpoints WHERE owner_id=$1 ORDER BY created_at DESC`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *Repository) CountByOwner(ctx context.Context, ownerID string) (int, error) {
	var n int
	err := r.db.QueryRow(ctx, `SELECT count(*) FROM webhook_endpoints WHERE owner_id=$1`, ownerID).Scan(&n)
	return n, err
}

func (r *Repository) Delete(ctx context.Context, id, ownerID string) (bool, error) {
	tag, err := r.db.Exec(ctx, `DELETE FROM webhook_endpoints WHERE id=$1 AND owner_id=$2`, id, ownerID)
	return tag.RowsAffected() > 0, err
}

// ListActiveForOwners returns active endpoints owned by any recipient and subscribed to eventType.
func (r *Repository) ListActiveForOwners(ctx context.Context, ownerIDs []string, eventType string) ([]Endpoint, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}
	rows, err := r.db.Query(ctx,
		`SELECT `+endpointCols+` FROM webhook_endpoints
		 WHERE owner_id = ANY($1) AND active AND $2 = ANY(event_types)`, ownerIDs, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *Repository) GetActive(ctx context.Context, id string) (Endpoint, error) {
	e, err := scanEndpoint(r.db.QueryRow(ctx,
		`SELECT `+endpointCols+` FROM webhook_endpoints WHERE id=$1 AND active`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, ErrNotFound
	}
	return e, err
}

func (r *Repository) IsDelivered(ctx context.Context, endpointID, eventID string) (bool, error) {
	var ok bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM webhook_deliveries
		                WHERE endpoint_id=$1 AND event_id=$2 AND status='delivered')`,
		endpointID, eventID).Scan(&ok)
	return ok, err
}

func (r *Repository) RecordDelivery(ctx context.Context, d DeliveryRecord) error {
	if len(d.LastError) > 500 {
		d.LastError = d.LastError[:500]
	}
	_, err := r.db.Exec(ctx,
		`INSERT INTO webhook_deliveries
		   (endpoint_id, event_id, status, attempts, last_status_code, last_error, next_retry_at)
		 VALUES ($1,$2,$3,$4,NULLIF($5::int,0),NULLIF($6,''),$7)
		 ON CONFLICT (endpoint_id, event_id) DO UPDATE SET
		   status=EXCLUDED.status, attempts=EXCLUDED.attempts,
		   last_status_code=EXCLUDED.last_status_code, last_error=EXCLUDED.last_error,
		   next_retry_at=EXCLUDED.next_retry_at, updated_at=now()`,
		d.EndpointID, d.EventID, d.Status, d.Attempts, d.StatusCode, d.LastError, d.NextRetryAt)
	return err
}

func (r *Repository) ListDeliveries(ctx context.Context, endpointID, ownerID string, limit int) ([]DeliverySummary, error) {
	rows, err := r.db.Query(ctx,
		`SELECT d.event_id, d.status, d.attempts, COALESCE(d.last_status_code,0),
		        COALESCE(d.last_error,''), d.updated_at
		 FROM webhook_deliveries d
		 JOIN webhook_endpoints e ON e.id = d.endpoint_id
		 WHERE d.endpoint_id=$1 AND e.owner_id=$2
		 ORDER BY d.updated_at DESC LIMIT $3`, endpointID, ownerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeliverySummary
	for rows.Next() {
		var s DeliverySummary
		if err := rows.Scan(&s.EventID, &s.Status, &s.Attempts, &s.StatusCode, &s.LastError, &s.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}