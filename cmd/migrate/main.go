// Command migrate applies SQL migrations in order and creates Kafka topics.
// Run it as a one-shot job (init container / release step) BEFORE deploying
// the other binaries.
//
//	go run ./cmd/migrate                      # apply pending migrations + ensure topics
//	go run ./cmd/migrate -baseline 3          # DB already has 001-003 from the old runner: mark them applied
//	go run ./cmd/migrate -topics=false        # SQL only
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/events"
	"github.com/sabya/real-time-chat/internal/notifications"
	"github.com/sabya/real-time-chat/internal/observability"
	"github.com/sabya/real-time-chat/internal/webhooks"
)

const lockID = 7263548

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "migrations", "directory containing *.sql files")
	baseline := flag.Int("baseline", 0, "mark migrations up to this number as already applied (first run on an existing DB)")
	topics := flag.Bool("topics", true, "also create Kafka topics")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Require(config.PartDB); err != nil {
		return err
	}
	log := observability.NewLogger(cfg.LogLevel, !cfg.IsDev())
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := applySQL(ctx, pool, *dir, *baseline, log); err != nil {
		return err
	}

	if *topics && len(cfg.KafkaBrokers) > 0 {
		specs := []events.TopicSpec{
			{Name: webhooks.TopicEvents, Partitions: 12},
			{Name: webhooks.TopicDLQ, Partitions: 3},
			{Name: notifications.TopicDLQ, Partitions: 3},
		}
		for _, t := range webhooks.DefaultRetryPolicy().Topics() {
			specs = append(specs, events.TopicSpec{Name: t, Partitions: 6})
		}
		tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := events.EnsureTopics(tctx, cfg.KafkaBrokers, cfg.KafkaReplication, specs); err != nil {
			return fmt.Errorf("ensure kafka topics: %w", err)
		}
		log.Info("kafka topics ensured", "count", len(specs), "replication", cfg.KafkaReplication)
	}
	return nil
}

func applySQL(ctx context.Context, pool *pgxpool.Pool, dir string, baseline int, log *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Serialise concurrent runs (e.g. several replicas starting at once).
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(lockID)); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, int64(lockID))
	}()

	if _, err := conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
		     version    TEXT PRIMARY KEY,
		     applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return fmt.Errorf("no .sql files found in %q", dir)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()

	if len(applied) == 0 && baseline > 0 {
		for _, f := range files {
			name := filepath.Base(f)
			if n, ok := number(name); ok && n <= baseline {
				if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, name); err != nil {
					return err
				}
				applied[name] = true
				log.Info("baselined (not executed)", "migration", name)
			}
		}
	}

	for _, f := range files {
		name := filepath.Base(f)
		if applied[name] {
			continue
		}
		sqlText, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if err := applyOne(ctx, conn, name, string(sqlText)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
		log.Info("applied", "migration", name)
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgxpool.Conn, name, sqlText string) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sqlText); err != nil { // no args => simple protocol => multiple statements allowed
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func number(name string) (int, bool) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(prefix)
	return n, err == nil
}
