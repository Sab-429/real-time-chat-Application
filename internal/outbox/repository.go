package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is satisfied by pgx.Tx, *pgxpool.Pool and *pgxpool.Conn, so Insert can
// run inside the caller's business transaction (that is the whole point of an outbox).
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Repository struct{ db DBTX }

func NewRepository(db DBTX) *Repository { return &Repository{db: db} }

// Insert enqueues an event. Call it with the SAME transaction that writes your
// business data so the event exists if and only if that data was committed.
func (r *Repository) Insert(ctx context.Context, topic, key string, payload []byte) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO outbox_events (topic, key, payload) VALUES ($1,$2,$3::jsonb)`,
		topic, key, string(payload))
	return err
}

// Backlog reports how many events are waiting and how old the oldest is.
// Alert on it: a growing backlog means the relay or Kafka is unhealthy.
func (r *Repository) Backlog(ctx context.Context) (count int64, oldest time.Duration, err error) {
	var secs float64
	err = r.db.QueryRow(ctx,
		`SELECT count(*), COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)
		   FROM outbox_events WHERE published_at IS NULL`).Scan(&count, &secs)
	return count, time.Duration(secs * float64(time.Second)), err
}

// Prune deletes events published more than olderThan ago.
func (r *Repository) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM outbox_events WHERE published_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	return tag.RowsAffected(), err
}
