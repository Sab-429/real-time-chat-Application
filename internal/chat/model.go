package chat

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	TopicMessageCreated     = "chat.message.created"
	EventTypeMessageCreated = "chat.message.created"
	EventVersion            = 1

	TypeDirect = "direct"
	TypeGroup  = "group"

	MaxBodyBytes    = 8 << 10
	MaxGroupMembers = 1000
	MaxTitleRunes   = 100
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrNotMember    = errors.New("not a member of this conversation")
	ErrNotFound     = errors.New("not found")
	ErrDuplicate    = errors.New("duplicate client_msg_id") // internal: lost an idempotency race
)

func invalid(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidInput, msg) }

// invalidMessage extracts the client-safe text of an ErrInvalidInput error.
func invalidMessage(err error) string {
	return strings.TrimPrefix(err.Error(), ErrInvalidInput.Error()+": ")
}

// Identity is the authenticated caller. Name comes from the verified token claims.
type Identity struct {
	ID   string
	Name string
}

type Message struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	Seq            int64     `json:"seq"`
	SenderID       string    `json:"sender_id"`
	ClientMsgID    string    `json:"client_msg_id,omitempty"`
	Body           string    `json:"body"`
	CreatedAt      time.Time `json:"created_at"`
}

type Conversation struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Title     string    `json:"title,omitempty"`
	LastSeq   int64     `json:"last_seq"`
	CreatedAt time.Time `json:"created_at"`
}

type ConversationSummary struct {
	Conversation
	LastReadSeq int64 `json:"last_read_seq"`
	Unread      int64 `json:"unread"`
	Muted       bool  `json:"muted"`
}

type NewConversation struct {
	Type      string
	Title     string
	CreatorID string
	MemberIDs []string // includes the creator
	DirectKey string   // set for direct conversations only
}

// EventEnvelope is written to the outbox and consumed by the webhook,
// notification and analytics workers. Keep it in sync with their envelope structs.
type EventEnvelope struct {
	EventID      string          `json:"event_id"`
	Type         string          `json:"type"`
	Version      int             `json:"version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RecipientIDs []string        `json:"recipient_ids"`
	Data         json.RawMessage `json:"data"`
}

type MessageEventData struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SenderID       string `json:"sender_id"`
	SenderName     string `json:"sender_name"`
	Body           string `json:"body"`
}

// directKey is order-independent: (a,b) and (b,a) map to the same conversation.
func directKey(ids []string) string {
	c := append([]string(nil), ids...)
	sort.Strings(c)
	return strings.Join(c, ":")
}

func uniqueIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}