// Command worker-analytics projects message events into an analytics store in
// batches. It has its own consumer group, so it can lag without affecting
// live delivery, notifications or webhooks.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sabya/real-time-chat/internal/analytics"
	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/events"
	"github.com/sabya/real-time-chat/internal/observability"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Require(config.PartDB, config.PartKafka); err != nil {
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

	// Stand-in sink; swap for a ClickHouse/BigQuery implementation of analytics.Sink at scale.
	sink := analytics.NewPostgresSink(pool)

	g, gctx := errgroup.WithContext(ctx)
	observability.GoHealth(gctx, g, cfg.HealthAddr, pool.Ping, cfg.ShutdownTimeout)
	g.Go(func() error {
		return events.RunBatchConsumer(gctx, events.ConsumerConfig{
			Brokers: cfg.KafkaBrokers, GroupID: "analytics-cg",
			Topic: analytics.TopicEvents, Logger: log,
		}, 500, 2*time.Second, analytics.BatchHandler(sink, log))
	})
	return g.Wait()
}