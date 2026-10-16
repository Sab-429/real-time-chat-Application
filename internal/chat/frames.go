package chat

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	// Adjust to your go.mod module path.
	"github.com/sabya/real-time-chat/internal/realtime"
)

// NameResolver returns a display name for push titles ("" is fine).
// Back it with a small cache; it runs on every send.
type NameResolver func(ctx context.Context, userID string) string

// FrameRouter implements realtime.FrameHandler for the gateway.
type FrameRouter struct {
	svc   *Service
	names NameResolver
}

func NewFrameRouter(svc *Service, names NameResolver) *FrameRouter {
	return &FrameRouter{svc: svc, names: names}
}

var _ realtime.FrameHandler = (*FrameRouter)(nil)

func (r *FrameRouter) HandleFrame(ctx context.Context, c *realtime.Client, f realtime.Frame) error {
	switch f.Type {
	case realtime.TypeMessageSend:
		return r.send(ctx, c, f)
	case realtime.TypeSyncRequest:
		return r.sync(ctx, c, f)
	default:
		return &realtime.FrameError{Code: "unsupported_type", Message: "unsupported frame type"}
	}
}

type sendData struct {
	ConversationID string `json:"conversation_id"`
	ClientMsgID    string `json:"client_msg_id"`
	Body           string `json:"body"`
}

type ackData struct {
	MessageID      string    `json:"message_id"`
	ConversationID string    `json:"conversation_id"`
	Seq            int64     `json:"seq"`
	CreatedAt      time.Time `json:"created_at"`
	Duplicate      bool      `json:"duplicate"`
}

func (r *FrameRouter) send(ctx context.Context, c *realtime.Client, f realtime.Frame) error {
	var d sendData
	if err := json.Unmarshal(f.Data, &d); err != nil {
		return &realtime.FrameError{Code: "invalid_input", Message: "invalid data"}
	}
	name := ""
	if r.names != nil {
		name = r.names(ctx, c.UserID)
	}
	res, err := r.svc.Send(ctx, SendInput{
		ConversationID: d.ConversationID, SenderID: c.UserID, SenderName: name,
		ClientMsgID: d.ClientMsgID, Body: d.Body,
	})
	if err != nil {
		return frameError(err)
	}
	return reply(c, realtime.TypeMessageAck, f.ID, ackData{
		MessageID: res.Message.ID, ConversationID: res.Message.ConversationID,
		Seq: res.Message.Seq, CreatedAt: res.Message.CreatedAt, Duplicate: res.Duplicate,
	})
}

func (r *FrameRouter) sync(ctx context.Context, c *realtime.Client, f realtime.Frame) error {
	var d struct {
		Conversations []SyncRequest `json:"conversations"`
	}
	if err := json.Unmarshal(f.Data, &d); err != nil {
		return &realtime.FrameError{Code: "invalid_input", Message: "invalid data"}
	}
	results, err := r.svc.Sync(ctx, c.UserID, d.Conversations)
	if err != nil {
		return frameError(err)
	}
	return reply(c, realtime.TypeSyncResponse, f.ID, map[string]any{"results": results})
}

func reply(c *realtime.Client, typ, id string, data any) error {
	fr, err := realtime.NewFrame(typ, id, data)
	if err != nil {
		return err
	}
	b, err := json.Marshal(fr)
	if err != nil {
		return err
	}
	c.Enqueue(b) // a full buffer means the hub is about to drop this slow client anyway
	return nil
}

func frameError(err error) error {
	switch {
	case errors.Is(err, ErrInvalidInput):
		return &realtime.FrameError{Code: "invalid_input", Message: invalidMessage(err)}
	case errors.Is(err, ErrNotMember):
		return &realtime.FrameError{Code: "forbidden", Message: "forbidden"}
	case errors.Is(err, ErrNotFound):
		return &realtime.FrameError{Code: "not_found", Message: "not found"}
	default:
		return err // logged by the gateway, reported to the client as internal_error
	}
}