package chat

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// ------------------------------------------------------------------ ports

// Tx is the set of writes that must commit atomically with each other.
type Tx interface {
	NextSeq(ctx context.Context, conversationID string) (int64, error)
	InsertMessage(ctx context.Context, m Message) (Message, error) // ErrDuplicate on client_msg_id race
	InsertOutbox(ctx context.Context, topic, key string, payload []byte) error
}

type Store interface {
	InTx(ctx context.Context, fn func(Tx) error) error
	Members(ctx context.Context, conversationID string) ([]string, error)
	IsMember(ctx context.Context, conversationID, userID string) (bool, error)
	MemberOf(ctx context.Context, userID string, conversationIDs []string) ([]string, error)
	FindByClientMsgID(ctx context.Context, senderID, clientMsgID string) (*Message, error) // ErrNotFound
	MessagesAfter(ctx context.Context, conversationID string, afterSeq int64, limit int) ([]Message, error)
	MessagesBefore(ctx context.Context, conversationID string, beforeSeq int64, limit int) ([]Message, error)
	ListConversations(ctx context.Context, userID string, limit int) ([]ConversationSummary, error)
	MarkRead(ctx context.Context, conversationID, userID string, seq int64) error // ErrNotMember
	CreateConversation(ctx context.Context, nc NewConversation) (*Conversation, error)
}

// Realtime pushes a committed message to online members (see adapter.go).
type Realtime interface {
	PublishMessage(ctx context.Context, userIDs []string, m Message) error
}

// ------------------------------------------------------------------ service

type Service struct {
	store Store
	rt    Realtime
	log   *slog.Logger
}

func NewService(store Store, rt Realtime, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, rt: rt, log: log}
}

type SendInput struct {
	ConversationID string
	SenderID       string
	SenderName     string
	ClientMsgID    string // client-generated; makes retries idempotent
	Body           string
}

type SendResult struct {
	Message   Message
	Duplicate bool // true when this was a retry of an already-stored message
}

// Send is the hot path:
//  1. validate, membership check
//  2. ONE transaction: next seq + message row + outbox row
//  3. after commit: publish to Redis for live delivery (best effort)
//
// Kafka is never touched here; the outbox relay does that asynchronously.
func (s *Service) Send(ctx context.Context, in SendInput) (SendResult, error) {
	if err := validateSend(in); err != nil {
		return SendResult{}, err
	}

	// Retry fast-path: the client resent after a timeout; return the original.
	if res, ok, err := s.existing(ctx, in); err != nil || ok {
		return res, err
	}

	members, err := s.store.Members(ctx, in.ConversationID)
	if err != nil {
		return SendResult{}, err
	}
	if !slices.Contains(members, in.SenderID) {
		return SendResult{}, ErrNotMember // also covers a conversation that does not exist
	}

	var msg Message
	err = s.store.InTx(ctx, func(tx Tx) error {
		seq, err := tx.NextSeq(ctx, in.ConversationID) // row-locks the conversation: per-conversation ordering
		if err != nil {
			return err
		}
		m, err := tx.InsertMessage(ctx, Message{
			ID: newID(), ConversationID: in.ConversationID, Seq: seq,
			SenderID: in.SenderID, ClientMsgID: in.ClientMsgID, Body: in.Body,
		})
		if err != nil {
			return err
		}
		payload, err := buildEvent(m, in.SenderName, members)
		if err != nil {
			return err
		}
		if err := tx.InsertOutbox(ctx, TopicMessageCreated, m.ConversationID, payload); err != nil {
			return err
		}
		msg = m
		return nil
	})
	if errors.Is(err, ErrDuplicate) {
		// Two retries raced; the other one committed first. Return its message.
		if res, ok, ferr := s.existing(ctx, in); ferr != nil || ok {
			return res, ferr
		}
		return SendResult{}, err
	}
	if err != nil {
		return SendResult{}, err
	}

	s.publish(ctx, members, msg)
	return SendResult{Message: msg}, nil
}

func (s *Service) existing(ctx context.Context, in SendInput) (SendResult, bool, error) {
	m, err := s.store.FindByClientMsgID(ctx, in.SenderID, in.ClientMsgID)
	if errors.Is(err, ErrNotFound) {
		return SendResult{}, false, nil
	}
	if err != nil {
		return SendResult{}, false, err
	}
	if m.ConversationID != in.ConversationID {
		return SendResult{}, false, invalid("client_msg_id already used in another conversation")
	}
	return SendResult{Message: *m, Duplicate: true}, true, nil
}

// publish is best effort by design: the DB is the source of truth, and a
// client that misses a live frame detects the seq gap and calls Sync.
func (s *Service) publish(ctx context.Context, members []string, m Message) {
	if s.rt == nil {
		return
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.rt.PublishMessage(pctx, members, m); err != nil {
		s.log.Warn("live publish failed; clients will resync by seq",
			"conversation_id", m.ConversationID, "seq", m.Seq, "error", err)
	}
}

func buildEvent(m Message, senderName string, members []string) ([]byte, error) {
	data, err := json.Marshal(MessageEventData{
		MessageID: m.ID, ConversationID: m.ConversationID, Seq: m.Seq,
		SenderID: m.SenderID, SenderName: senderName, Body: m.Body,
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(EventEnvelope{
		EventID: newID(), Type: EventTypeMessageCreated, Version: EventVersion,
		OccurredAt: m.CreatedAt.UTC(), RecipientIDs: members, Data: data,
	})
}

func validateSend(in SendInput) error {
	switch {
	case in.ConversationID == "":
		return invalid("conversation_id is required")
	case in.SenderID == "":
		return invalid("sender is required")
	case len(in.ClientMsgID) < 8 || len(in.ClientMsgID) > 64:
		return invalid("client_msg_id must be 8-64 characters")
	case strings.TrimSpace(in.Body) == "":
		return invalid("body is empty")
	case len(in.Body) > MaxBodyBytes:
		return invalid("body is too long")
	case !utf8.ValidString(in.Body) || strings.ContainsRune(in.Body, 0):
		return invalid("body contains invalid characters")
	}
	return nil
}

// ------------------------------------------------------------------ reads

// History returns messages newest-first. beforeSeq == 0 means "from the latest".
func (s *Service) History(ctx context.Context, userID, conversationID string, beforeSeq int64, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 50
	}
	limit = min(limit, 100)
	if beforeSeq < 0 {
		return nil, invalid("before_seq must be >= 0")
	}
	if beforeSeq == 0 {
		beforeSeq = math.MaxInt64
	}
	ok, err := s.store.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotMember
	}
	return s.store.MessagesBefore(ctx, conversationID, beforeSeq, limit)
}

func (s *Service) ListConversations(ctx context.Context, userID string, limit int) ([]ConversationSummary, error) {
	if limit <= 0 {
		limit = 50
	}
	out, err := s.store.ListConversations(ctx, userID, min(limit, 200))
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Unread = max(0, out[i].LastSeq-out[i].LastReadSeq)
	}
	return out, nil
}

// MarkRead advances (never rewinds) the caller's read pointer.
func (s *Service) MarkRead(ctx context.Context, userID, conversationID string, seq int64) error {
	if seq < 0 {
		return invalid("seq must be >= 0")
	}
	return s.store.MarkRead(ctx, conversationID, userID, seq)
}

type CreateConversationInput struct {
	CreatorID string
	Type      string
	Title     string
	MemberIDs []string // others; the creator is added automatically
}

// CreateConversation is idempotent for direct chats: asking twice for the same
// pair returns the same conversation.
func (s *Service) CreateConversation(ctx context.Context, in CreateConversationInput) (*Conversation, error) {
	if in.CreatorID == "" {
		return nil, invalid("creator is required")
	}
	members := uniqueIDs(append([]string{in.CreatorID}, in.MemberIDs...))
	nc := NewConversation{Type: in.Type, CreatorID: in.CreatorID, MemberIDs: members}

	switch in.Type {
	case TypeDirect:
		if len(members) != 2 {
			return nil, invalid("a direct conversation needs exactly one other member")
		}
		nc.DirectKey = directKey(members)
	case TypeGroup:
		title := strings.TrimSpace(in.Title)
		switch {
		case len(members) < 2:
			return nil, invalid("a group needs at least one other member")
		case len(members) > MaxGroupMembers:
			return nil, invalid("too many members")
		case title == "" || utf8.RuneCountInString(title) > MaxTitleRunes:
			return nil, invalid("title is required (max 100 characters)")
		}
		nc.Title = title
	default:
		return nil, invalid("type must be 'direct' or 'group'")
	}
	return s.store.CreateConversation(ctx, nc)
}

// newID returns a random UUIDv4.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
