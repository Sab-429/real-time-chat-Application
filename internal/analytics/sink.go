package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const TopicEvents = "chat.message.created"

// Event is the analytics projection of a message: metadata only, never the body.
type Event struct {
	EventID        string
	MessageID      string
	ConversationID string
	SenderID       string
	Seq            int64
	RecipientCount int32
	BodyBytes      int32
	OccurredAt     time.Time
}

// Sink is the seam for a real analytical store. PostgresSink below is a
// stand-in; for production volume implement this for ClickHouse/BigQuery.
type Sink interface {
	Write(ctx context.Context, events []Event) error // must be idempotent on EventID
}

type envelope struct {
	EventID      string          `json:"event_id"`
	Type         string          `json:"type"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RecipientIDs []string        `json:"recipient_ids"`
	Data         json.RawMessage `json:"data"`
}

type messageData struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SenderID       string `json:"sender_id"`
	Body           string `json:"body"`
}

var errSkip = errors.New("not a message event")

func Parse(payload []byte) (Event, error) {
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil || env.EventID == "" {
		return Event{}, errors.New("malformed event")
	}
	if env.Type != "chat.message.created" {
		return Event{}, errSkip
	}
	var d messageData
	if err := json.Unmarshal(env.Data, &d); err != nil || d.ConversationID == "" {
		return Event{}, errors.New("malformed message data")
	}
	return Event{
		EventID: env.EventID, MessageID: d.MessageID, ConversationID: d.ConversationID,
		SenderID: d.SenderID, Seq: d.Seq,
		RecipientCount: int32(len(env.RecipientIDs)), BodyBytes: int32(len(d.Body)),
		OccurredAt: env.OccurredAt,
	}, nil
}

// BatchHandler adapts a Sink to events.BatchHandler. Malformed messages are
// logged and skipped (an analytics worker must never wedge on bad input);
// only sink failures are returned, so the batch is retried and not committed.
func BatchHandler(sink Sink, log *slog.Logger) func(ctx context.Context, values [][]byte) error {
	return func(ctx context.Context, values [][]byte) error {
		batch := make([]Event, 0, len(values))
		for _, v := range values {
			e, err := Parse(v)
			if err != nil {
				if !errors.Is(err, errSkip) {
					log.Warn("skipping malformed analytics event", "error", err)
				}
				continue
			}
			batch = append(batch, e)
		}
		if len(batch) == 0 {
			return nil
		}
		return sink.Write(ctx, batch)
	}
}

type PostgresSink struct{ db *pgxpool.Pool }

func NewPostgresSink(db *pgxpool.Pool) *PostgresSink { return &PostgresSink{db: db} }

func (s *PostgresSink) Write(ctx context.Context, evs []Event) error {
	n := len(evs)
	var (
		eventIDs, messageIDs, convIDs, senderIDs = make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		seqs                                     = make([]int64, n)
		recips, bodies                           = make([]int32, n), make([]int32, n)
		occurred                                 = make([]time.Time, n)
	)
	for i, e := range evs {
		eventIDs[i], messageIDs[i], convIDs[i], senderIDs[i] = e.EventID, e.MessageID, e.ConversationID, e.SenderID
		seqs[i], recips[i], bodies[i], occurred[i] = e.Seq, e.RecipientCount, e.BodyBytes, e.OccurredAt
	}
	_, err := s.db.Exec(ctx,
		`INSERT INTO message_events
		   (event_id, message_id, conversation_id, sender_id, seq, recipient_count, body_bytes, occurred_at)
		 SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[],
		                      $5::bigint[], $6::int[], $7::int[], $8::timestamptz[])
		 ON CONFLICT (event_id) DO NOTHING`,
		eventIDs, messageIDs, convIDs, senderIDs, seqs, recips, bodies, occurred)
	return err
}