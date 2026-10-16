package events

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	Brokers []string
	GroupID string
	Topic   string
	Logger  *slog.Logger
}

// Handler contract (same for every worker):
//
//	nil   -> the message is done (processed, scheduled for retry, or dead-lettered); commit it
//	error -> infrastructure failure; do NOT commit, retry with backoff
type Handler func(ctx context.Context, value []byte) error

type BatchHandler func(ctx context.Context, values [][]byte) error

func newReader(c ConsumerConfig) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           c.Brokers,
		GroupID:           c.GroupID,
		Topic:             c.Topic,
		MinBytes:          1,
		MaxBytes:          10 << 20,
		MaxWait:           500 * time.Millisecond,
		StartOffset:       kafka.FirstOffset, // only used when the group has no committed offset yet
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second, // background heartbeats: long handlers (retry sleeps) do not trigger rebalances
		CommitInterval:    0,               // synchronous commit after each message/batch
	})
}

func (c ConsumerConfig) logger() *slog.Logger {
	l := c.Logger
	if l == nil {
		l = slog.Default()
	}
	return l.With("topic", c.Topic, "group", c.GroupID)
}

// RunConsumer processes messages one at a time, in partition order, committing
// only after the handler succeeds. It returns nil when ctx is cancelled.
func RunConsumer(ctx context.Context, c ConsumerConfig, h Handler) error {
	r := newReader(c)
	defer r.Close()
	log := c.logger()
	log.Info("consumer started")

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", c.Topic, err)
		}
		if err := retryForever(ctx, log, func() error { return h(ctx, m.Value) }); err != nil {
			return nil // ctx cancelled while retrying
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit %s: %w", c.Topic, err)
		}
	}
}

// RunBatchConsumer collects up to maxBatch messages (or whatever arrives within
// maxWait of the first one), hands them to h together, then commits the batch.
func RunBatchConsumer(ctx context.Context, c ConsumerConfig, maxBatch int, maxWait time.Duration, h BatchHandler) error {
	r := newReader(c)
	defer r.Close()
	log := c.logger()
	log.Info("batch consumer started", "max_batch", maxBatch, "max_wait", maxWait)

	for {
		first, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", c.Topic, err)
		}
		msgs := []kafka.Message{first}

		bctx, cancel := context.WithTimeout(ctx, maxWait)
		for len(msgs) < maxBatch {
			m, err := r.FetchMessage(bctx)
			if err != nil {
				if ctx.Err() != nil {
					cancel()
					return nil
				}
				if bctx.Err() != nil {
					break // batch window elapsed
				}
				cancel()
				return fmt.Errorf("fetch %s: %w", c.Topic, err)
			}
			msgs = append(msgs, m)
		}
		cancel()

		values := make([][]byte, len(msgs))
		for i, m := range msgs {
			values[i] = m.Value
		}
		if err := retryForever(ctx, log, func() error { return h(ctx, values) }); err != nil {
			return nil
		}
		if err := r.CommitMessages(ctx, msgs...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit %s: %w", c.Topic, err)
		}
	}
}

func retryForever(ctx context.Context, log *slog.Logger, fn func() error) error {
	backoff := 500 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Error("handler failed; retrying without committing", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
