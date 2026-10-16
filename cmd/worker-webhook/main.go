// Command worker-webhook delivers events to customer webhook endpoints, with
// retry tiers (1m/5m/30m), a per-endpoint circuit breaker, and a DLQ.
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
	"github.com/sabya/real-time-chat/internal/webhooks"
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

	producer := events.NewProducer(cfg.KafkaBrokers) // retry tiers + DLQ
	defer producer.Close()

	policy := webhooks.DefaultRetryPolicy()
	proc := webhooks.NewProcessor(webhooks.ProcessorConfig{
		Store: webhooks.NewRepository(pool),
		Dispatcher: webhooks.NewDispatcher(webhooks.DispatcherConfig{
			AllowPrivateNetworks: cfg.WebhookAllowPrivate,
		}),
		Breakers:  webhooks.NewBreakers(webhooks.BreakerConfig{}),
		Publisher: producer,
		Policy:    policy,
		Logger:    log,
	})

	g, gctx := errgroup.WithContext(ctx)
	observability.GoHealth(gctx, g, cfg.HealthAddr, pool.Ping, cfg.ShutdownTimeout)

	// One consumer (and one consumer group) per topic: first attempts + each retry tier.
	topics := append([]string{webhooks.TopicEvents}, policy.Topics()...)
	for _, topic := range topics {
		var handler events.Handler = proc.HandleRetry
		if topic == webhooks.TopicEvents {
			handler = proc.HandleEvent
		}
		g.Go(func() error {
			return events.RunConsumer(gctx, events.ConsumerConfig{
				Brokers: cfg.KafkaBrokers, GroupID: "webhook-" + topic,
				Topic: topic, Logger: log,
			}, handler)
		})
	}
	return g.Wait()
}