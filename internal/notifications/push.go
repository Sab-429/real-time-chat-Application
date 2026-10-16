package notifications

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// Provider sends one notification to one device.
type Provider interface {
	Send(ctx context.Context, d Device, n Notification) error
}

// ------------------------------------------------------------------ FCM

// FCMClient is satisfied by *messaging.Client (kept as an interface for tests).
type FCMClient interface {
	Send(ctx context.Context, message *messaging.Message) (string, error)
}

// FCMProvider uses Firebase Cloud Messaging for every platform: Android and
// web natively, iOS via FCM's APNs relay. One integration instead of two.
type FCMProvider struct{ client FCMClient }

func NewFCMProvider(client FCMClient) *FCMProvider { return &FCMProvider{client: client} }

// NewFCMProviderFromCredentials: pass "" to use Application Default Credentials
// (workload identity on GKE/Cloud Run), which is preferable to key files in production.
func NewFCMProviderFromCredentials(ctx context.Context, credentialsFile string) (*FCMProvider, error) {
	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}
	app, err := firebase.NewApp(ctx, nil, opts...)
	if err != nil {
		return nil, err
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, err
	}
	return NewFCMProvider(client), nil
}

func (p *FCMProvider) Send(ctx context.Context, d Device, n Notification) error {
	ttl := 24 * time.Hour
	apnsHeaders := map[string]string{"apns-priority": "10"}
	if len(n.MessageID) > 0 && len(n.MessageID) <= 64 { // apns-collapse-id limit
		apnsHeaders["apns-collapse-id"] = n.MessageID
	}

	msg := &messaging.Message{
		Token:        d.Token,
		Notification: &messaging.Notification{Title: n.Title, Body: n.Body},
		Data: map[string]string{
			"conversation_id": n.ConversationID,
			"message_id":      n.MessageID,
			"seq":             strconv.FormatInt(n.Seq, 10),
		},
		Android: &messaging.AndroidConfig{
			Priority: "high",
			TTL:      &ttl,
			Notification: &messaging.AndroidNotification{
				ChannelID: "messages",
				Tag:       n.MessageID, // a retried send replaces itself instead of stacking
			},
		},
		APNS: &messaging.APNSConfig{
			Headers: apnsHeaders,
			Payload: &messaging.APNSPayload{
				Aps: &messaging.Aps{Sound: "default", ThreadID: n.ConversationID},
			},
		},
	}

	_, err := p.client.Send(ctx, msg)
	return classifyFCM(err)
}

func classifyFCM(err error) error {
	switch {
	case err == nil:
		return nil
	// ONLY these two prove the token itself is dead. InvalidArgument can mean a
	// bad payload, and deleting every token because of our own bug would be a disaster.
	case messaging.IsUnregistered(err), messaging.IsSenderIDMismatch(err):
		return fmt.Errorf("%w: %v", ErrInvalidToken, err)
	case messaging.IsInvalidArgument(err), messaging.IsThirdPartyAuthError(err):
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	case messaging.IsUnavailable(err), messaging.IsInternal(err), messaging.IsQuotaExceeded(err),
		errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %v", ErrTransient, err)
	default:
		// Unknown (often network): retry a few times, then DLQ.
		return fmt.Errorf("%w: %v", ErrTransient, err)
	}
}

// ------------------------------------------------------------------ Log (dev)

// LogProvider prints notifications instead of sending them (local development).
type LogProvider struct{ Log *slog.Logger }

func (p LogProvider) Send(_ context.Context, d Device, n Notification) error {
	l := p.Log
	if l == nil {
		l = slog.Default()
	}
	l.Info("push (dev)", "user_id", d.UserID, "platform", d.Platform, "title", n.Title, "body", n.Body, "message_id", n.MessageID)
	return nil
}