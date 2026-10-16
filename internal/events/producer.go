package events

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer is shared by the outbox relay and the workers (retry/DLQ topics).
// It satisfies the Publisher/Producer interfaces of the webhooks, notifications
// and outbox packages.
//
// Notes:
//   - Messages with the same key go to the same partition (conversation order).
//   - RequireAll + retries = no loss while >= min.insync.replicas brokers are up.
//   - kafka-go has no idempotent producer, so a retried write can duplicate.
//     Every consumer already dedupes on event_id.
type Producer struct{ w *kafka.Writer }

func NewProducer(brokers []string) *Producer {
	return &Producer{w: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		MaxAttempts:            10,
		BatchTimeout:           time.Millisecond,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		AllowAutoTopicCreation: false,
	}}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	return p.w.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: value})
}

func (p *Producer) Close() error { return p.w.Close() }
