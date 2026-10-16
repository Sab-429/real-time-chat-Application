package chat

import (
	"context"

	// Adjust to your go.mod module path.
	"github.com/sabya/real-time-chat/internal/realtime"
)

// RealtimePublisher is satisfied by *realtime.Publisher.
type RealtimePublisher interface {
	PublishToUsers(ctx context.Context, userIDs []string, f realtime.Frame) (realtime.PublishResult, error)
}

// RealtimeAdapter implements chat.Realtime on top of the Redis publisher,
// wrapping the message in a "message.new" frame.
type RealtimeAdapter struct{ pub RealtimePublisher }

func NewRealtimeAdapter(pub RealtimePublisher) *RealtimeAdapter { return &RealtimeAdapter{pub: pub} }

var _ Realtime = (*RealtimeAdapter)(nil)

func (a *RealtimeAdapter) PublishMessage(ctx context.Context, userIDs []string, m Message) error {
	f, err := realtime.NewFrame(realtime.TypeMessageNew, "", m)
	if err != nil {
		return err
	}
	_, err = a.pub.PublishToUsers(ctx, userIDs, f)
	return err
}