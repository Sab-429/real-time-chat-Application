// Command worker-notification sends push notifications to OFFLINE recipients.
// Online users already received the message over WebSocket.
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

	"github.com/sabya/real-time-chat/internal/cache"
	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/events"
	"github.com/sabya/real-time-chat/internal/notifications"
	"github.com/sabya/real-time-chat/internal/observability"
	"github.com/sabya/real-time-chat/internal/realtime"
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
	if err := cfg.Require(config.PartDB, config.PartRedis, config.PartKafka); err != nil {
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

	rdb, err := cache.NewRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()

	producer := events.NewProducer(cfg.KafkaBrokers) // DLQ publishing
	defer producer.Close()

	// Dev without credentials: log pushes instead of sending them.
	var provider notifications.Provider = notifications.LogProvider{Log: log}
	if !cfg.IsDev() || cfg.FCMCredentialsFile != "" {
		fcm, err := notifications.NewFCMProviderFromCredentials(ctx, cfg.FCMCredentialsFile)
		if err != nil {
			return fmt.Errorf("init FCM: %w", err)
		}
		provider = fcm
	}

	repo := notifications.NewRepository(pool)
	proc := notifications.NewProcessor(notifications.ProcessorConfig{
		Store:          repo,
		Presence:       realtime.NewPresence(rdb, "", 60*time.Second), // read-only lookup
		Provider:       provider,
		Publisher:      producer,
		IncludePreview: cfg.PushIncludePreview,
		Logger:         log,
	})

	g, gctx := errgroup.WithContext(ctx)
	observability.GoHealth(gctx, g, cfg.HealthAddr, func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return rdb.Ping(ctx).Err()
	}, cfg.ShutdownTimeout)

	g.Go(func() error {
		notifications.RunPruner(gctx, repo, time.Hour, 60*24*time.Hour, 7*24*time.Hour, log)
		return nil
	})
	g.Go(func() error {
		return events.RunConsumer(gctx, events.ConsumerConfig{
			Brokers: cfg.KafkaBrokers, GroupID: "notification-cg",
			Topic: notifications.TopicEvents, Logger: log,
		}, proc.HandleEvent)
	})
	return g.Wait()
}