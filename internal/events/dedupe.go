package events

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DedupeStore remembers which (consumer, event_id) pairs were fully processed.
//
// Use it for consumers that have NO natural idempotency of their own. The
// webhook worker (webhook_deliveries) and notification worker (notification_log)
// already dedupe per target and do not need this.
type DedupeStore interface {
	Seen(ctx context.Context, consumer, eventID string) (bool, error)
	Mark(ctx context.Context, consumer, eventID string) error
}

// Dedupe wraps a Handler: duplicates are skipped, and an event is marked only
// AFTER the handler succeeds (at-least-once; a crash in between re-runs the
// handler, so keep handlers idempotent where you can). Payloads that are not a
// valid envelope pass straight through so the handler can dead-letter them.
func Dedupe(store DedupeStore, consumer string, next Handler) Handler {
	return func(ctx context.Context, value []byte) error {
		env, err := DecodeEnvelope(value)
		if err != nil {
			return next(ctx, value)
		}
		seen, err := store.Seen(ctx, consumer, env.EventID)
		if err != nil {
			return err
		}
		if seen {
			return nil
		}
		if err := next(ctx, value); err != nil {
			return err
		}
		return store.Mark(ctx, consumer, env.EventID)
	}
}

// ---------------------------------------------------------------- Postgres

type PostgresDedupe struct{ db *pgxpool.Pool }

func NewPostgresDedupe(db *pgxpool.Pool) *PostgresDedupe { return &PostgresDedupe{db: db} }

var _ DedupeStore = (*PostgresDedupe)(nil)

func (d *PostgresDedupe) Seen(ctx context.Context, consumer, eventID string) (bool, error) {
	var ok bool
	err := d.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer = $1 AND event_id = $2)`,
		consumer, eventID).Scan(&ok)
	return ok, err
}

func (d *PostgresDedupe) Mark(ctx context.Context, consumer, eventID string) error {
	_, err := d.db.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumer, eventID)
	return err
}

// Prune deletes entries older than olderThan. Keep it comfortably above your
// Kafka retention so a replayed old event is still recognised.
func (d *PostgresDedupe) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := d.db.Exec(ctx,
		`DELETE FROM processed_events WHERE processed_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	return tag.RowsAffected(), err
}

// ---------------------------------------------------------------- memory (tests / dev)

type MemoryDedupe struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewMemoryDedupe() *MemoryDedupe { return &MemoryDedupe{seen: map[string]struct{}{}} }

func (m *MemoryDedupe) Seen(_ context.Context, consumer, eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.seen[consumer+"|"+eventID]
	return ok, nil
}

func (m *MemoryDedupe) Mark(_ context.Context, consumer, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen[consumer+"|"+eventID] = struct{}{}
	return nil
}
