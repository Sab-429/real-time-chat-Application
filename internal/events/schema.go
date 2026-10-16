package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// The shared event contract. Producers (the chat service via the outbox) and
// every consumer (webhooks, notifications, analytics, future ones) agree on
// this shape. Rules for evolving it:
//   - Adding an optional field is fine and keeps the same version.
//   - Renaming/removing a field or changing its meaning needs SchemaVersion+1,
//     and consumers must handle both versions during the rollout.
const (
	TypeMessageCreated = "chat.message.created"
	SchemaVersion      = 1
)

var ErrMalformed = errors.New("events: malformed event")

type Envelope struct {
	EventID      string          `json:"event_id"` // consumers dedupe on this
	Type         string          `json:"type"`
	Version      int             `json:"version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RecipientIDs []string        `json:"recipient_ids"` // internal only, never sent to customers
	Data         json.RawMessage `json:"data"`
}

type MessageCreated struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SenderID       string `json:"sender_id"`
	SenderName     string `json:"sender_name"`
	Body           string `json:"body"`
}

// DecodeEnvelope parses and validates the envelope. Unknown fields are ignored
// (forward compatibility).
func DecodeEnvelope(payload []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	switch {
	case e.EventID == "":
		return Envelope{}, fmt.Errorf("%w: missing event_id", ErrMalformed)
	case e.Type == "":
		return Envelope{}, fmt.Errorf("%w: missing type", ErrMalformed)
	case e.Version < 1:
		return Envelope{}, fmt.Errorf("%w: invalid version", ErrMalformed)
	}
	return e, nil
}

func (e Envelope) DecodeMessageCreated() (MessageCreated, error) {
	if e.Type != TypeMessageCreated {
		return MessageCreated{}, fmt.Errorf("%w: type is %q", ErrMalformed, e.Type)
	}
	var m MessageCreated
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return MessageCreated{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if m.MessageID == "" || m.ConversationID == "" {
		return MessageCreated{}, fmt.Errorf("%w: incomplete message data", ErrMalformed)
	}
	return m, nil
}

// NewMessageCreated builds the wire payload for a chat.message.created event.
func NewMessageCreated(eventID string, occurredAt time.Time, recipients []string, m MessageCreated) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{
		EventID: eventID, Type: TypeMessageCreated, Version: SchemaVersion,
		OccurredAt: occurredAt.UTC(), RecipientIDs: recipients, Data: data,
	})
}
