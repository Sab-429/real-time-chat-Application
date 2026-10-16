package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Arbitrary constant: only one relay process in the fleet holds this lock.
const advisoryLockID int64 = 0x6368617401

type Producer interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

type Config struct {
	Pool      *pgxpool.Pool
	Producer  Producer
	BatchSize int           // default 200
	Interval  time.Duration // idle poll interval, default 200ms
	Retention time.Duration // published rows older than this are deleted, default 7 days
	Logger    *slog.Logger
}

// Relay moves committed outbox rows to Kafka.
//
// Only ONE relay instance publishes at a time (Postgres advisory lock; the
// others stand by and take over within ~2s if the leader dies). That is
// deliberate: with several concurrent publishers (e.g. FOR UPDATE SKIP LOCKED),
// batch B can reach Kafka before an earlier batch A, which breaks per-conversation
// ordering. A single ordered publisher keeps it; if you outgrow it, use Debezium
// CDC on the outbox table instead.
//
// Delivery is at-least-once (crash between publish and mark => republish), so
// every consumer dedupes on event_id.
type Relay struct{ cfg Config }

func NewRelay(c Config) *Relay {
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.Interval <= 0 {
		c.Interval = 200 * time.Millisecond
	}
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return &Relay{cfg: c}
}

// Run blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := r.lead(ctx); err != nil && ctx.Err() == nil {
			r.cfg.Logger.Error("outbox relay stopped; retrying", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *Relay) lead(ctx context.Context) error {
	conn, err := r.cfg.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, advisoryLockID).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return nil // standby
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, advisoryLockID)
	}()
	r.cfg.Logger.Info("outbox relay is leader")

	repo := NewRepository(conn)
	lastPrune, lastReport := time.Now(), time.Now()
	for {
		n, err := r.flush(ctx, conn)
		if err != nil {
			return err
		}
		if time.Since(lastPrune) > time.Hour {
			if _, err := repo.Prune(ctx, r.cfg.Retention); err != nil {
				r.cfg.Logger.Warn("outbox prune failed", "error", err)
			}
			lastPrune = time.Now()
		}
		if time.Since(lastReport) > time.Minute {
			// A backlog that keeps growing means Kafka (or this relay) is unhealthy: alert on it.
			if count, oldest, err := repo.Backlog(ctx); err != nil {
				r.cfg.Logger.Warn("outbox backlog check failed", "error", err)
			} else if oldest > 30*time.Second {
				r.cfg.Logger.Warn("outbox backlog is growing", "pending", count, "oldest", oldest.Round(time.Second))
			} else {
				r.cfg.Logger.Debug("outbox backlog", "pending", count)
			}
			lastReport = time.Now()
		}
		if n >= r.cfg.BatchSize {
			continue // backlog: go again immediately
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.Interval):
		}
	}
}

type row struct {
	id         int64
	topic, key string
	payload    string
}

// flush publishes one batch in id order and marks what succeeded. If a publish
// fails midway, the rows already sent are still marked, and the error is returned.
func (r *Relay) flush(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, topic, key, payload::text FROM outbox_events
		 WHERE published_at IS NULL ORDER BY id LIMIT $1`, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	var batch []row
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.id, &x.topic, &x.key, &x.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	done := make([]int64, 0, len(batch))
	var pubErr error
	for _, x := range batch {
		if err := r.cfg.Producer.Publish(ctx, x.topic, x.key, []byte(x.payload)); err != nil {
			pubErr = err
			break // never skip ahead: that would reorder
		}
		done = append(done, x.id)
	}
	if len(done) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)`, done); err != nil {
			return len(batch), err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return len(batch), err
	}
	return len(batch), pubErr
}
