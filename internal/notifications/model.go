package notifications

import (
	"errors"
	"time"
)

const (
	TopicEvents         = "chat.message.created"
	TopicDLQ            = "notification.dlq"
	EventMessageCreated = "chat.message.created"
)

const (
	PlatformAndroid = "android"
	PlatformIOS     = "ios"
	PlatformWeb     = "web"
)

// Provider errors. Providers wrap their SDK errors with exactly one of these
// so the processor can decide what to do without knowing about FCM.
var (
	ErrInvalidToken = errors.New("notifications: invalid or unregistered device token") // delete the token
	ErrTransient    = errors.New("notifications: transient provider error")             // retry, then DLQ
	ErrPermanent    = errors.New("notifications: permanent provider error")             // straight to DLQ
)

type Device struct {
	UserID   string `json:"user_id"`
	Platform string `json:"platform"`
	Token    string `json:"token"`
}

type Notification struct {
	Title          string `json:"title"`
	Body           string `json:"body"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	Seq            int64  `json:"seq"`
}

// DeadLetter is published to TopicDLQ when delivery to some of a user's
// devices failed for good. It is self-contained so a replay tool needs no lookups.
type DeadLetter struct {
	EventID      string       `json:"event_id"`
	UserID       string       `json:"user_id"`
	Devices      []Device     `json:"devices"`
	Notification Notification `json:"notification"`
	Error        string       `json:"error"`
	FailedAt     time.Time    `json:"failed_at"`
}