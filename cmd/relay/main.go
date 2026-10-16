// Command relay publishes committed outbox rows to Kafka. Run 2+ replicas:
// one leads (Postgres advisory lock), the rest stand by.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/events"
	"github.com/sabya/real-time-chat/internal/observability"
	"github.com/sabya/real-time-chat/internal/outbox"
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

	producer := events.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	g, gctx := errgroup.WithContext(ctx)
	observability.GoHealth(gctx, g, cfg.HealthAddr, pool.Ping, cfg.ShutdownTimeout)
	g.Go(func() error {
		outbox.NewRelay(outbox.Config{Pool: pool, Producer: producer, Logger: log}).Run(gctx)
		return nil
	})
	return g.Wait()
}