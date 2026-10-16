package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ------------------------------------------------------------------ ports

type Store interface {
	NotifiableUsers(ctx context.Context, conversationID string, userIDs []string) ([]string, error) // not muted
	DevicesForUsers(ctx context.Context, userIDs []string) ([]Device, error)
	DeleteDevice(ctx context.Context, token string) error
	WasSent(ctx context.Context, eventID, userID string) (bool, error)
	MarkSent(ctx context.Context, eventID, userID string) error
}

// Presence is satisfied by *realtime.Presence.
type Presence interface {
	Gateways(ctx context.Context, userIDs []string) (map[string][]string, error)
}

type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// ------------------------------------------------------------------ event shapes

// Mirror of the outbox event. Replace with events.MessageCreated once it carries these fields.
type envelope struct {
	EventID      string          `json:"event_id"`
	Type         string          `json:"type"`
	Version      int             `json:"version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RecipientIDs []string        `json:"recipient_ids"`
	Data         json.RawMessage `json:"data"`
}

type messageData struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SenderID       string `json:"sender_id"`
	SenderName     string `json:"sender_name"`
	Body           string `json:"body"`
}

// ------------------------------------------------------------------ processor

type ProcessorConfig struct {
	Store          Store
	Presence       Presence
	Provider       Provider
	Publisher      Publisher
	Concurrency    int           // parallel users per event (default 16)
	MaxAttempts    int           // provider attempts per device incl. first (default 3)
	BaseBackoff    time.Duration // default 200ms, grows x3 per attempt with jitter
	IncludePreview bool          // false = generic "New message" body (privacy-friendly)
	PreviewRunes   int           // default 100
	Logger         *slog.Logger
}

type Processor struct{ cfg ProcessorConfig }

func NewProcessor(c ProcessorConfig) *Processor {
	if c.Concurrency <= 0 {
		c.Concurrency = 16
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 200 * time.Millisecond
	}
	if c.PreviewRunes <= 0 {
		c.PreviewRunes = 100
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return &Processor{cfg: c}
}

// HandleEvent is the Kafka handler for chat.message.created (group "notification-cg").
//   nil   -> commit the offset (pushed, skipped, or dead-lettered)
//   error -> do NOT commit; infrastructure failed (DB/Kafka), will be redelivered
//
// Delivery is at-least-once: notification_log skips users already notified, and
// the Android tag / apns-collapse-id make an accidental double push replace itself.
func (p *Processor) HandleEvent(ctx context.Context, payload []byte) error {
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil || env.EventID == "" {
		return p.deadLetterRaw(ctx, payload, "malformed event")
	}
	if env.Type != EventMessageCreated {
		return nil
	}
	var msg messageData
	if err := json.Unmarshal(env.Data, &msg); err != nil || msg.ConversationID == "" {
		return p.deadLetterRaw(ctx, payload, "malformed message data")
	}

	recipients := recipientsExcluding(env.RecipientIDs, msg.SenderID)
	if len(recipients) == 0 {
		return nil
	}
	notifiable, err := p.cfg.Store.NotifiableUsers(ctx, msg.ConversationID, recipients)
	if err != nil {
		return err
	}
	targets := p.offlineOnly(ctx, notifiable)
	if len(targets) == 0 {
		return nil // everyone is online (already got it live) or muted
	}

	devices, err := p.cfg.Store.DevicesForUsers(ctx, targets)
	if err != nil {
		return err
	}
	byUser := make(map[string][]Device)
	for _, d := range devices {
		byUser[d.UserID] = append(byUser[d.UserID], d)
	}
	users := make([]string, 0, len(byUser))
	for uid := range byUser {
		users = append(users, uid)
	}

	n := p.buildNotification(msg)
	return fanOut(ctx, p.cfg.Concurrency, users, func(uid string) error {
		return p.notifyUser(ctx, env.EventID, uid, byUser[uid], n)
	})
}

// offlineOnly drops users who are connected to a gateway right now.
// If the presence lookup fails it FAILS OPEN (pushes to everyone): a duplicate
// notification is far better than a missed one.
func (p *Processor) offlineOnly(ctx context.Context, users []string) []string {
	if len(users) == 0 {
		return nil
	}
	byGateway, err := p.cfg.Presence.Gateways(ctx, users)
	if err != nil {
		p.cfg.Logger.Warn("presence lookup failed; failing open", "error", err)
		return users
	}
	online := make(map[string]struct{})
	for _, us := range byGateway {
		for _, u := range us {
			online[u] = struct{}{}
		}
	}
	out := make([]string, 0, len(users))
	for _, u := range users {
		if _, ok := online[u]; !ok {
			out = append(out, u)
		}
	}
	return out
}

func (p *Processor) notifyUser(ctx context.Context, eventID, userID string, devices []Device, n Notification) error {
	sent, err := p.cfg.Store.WasSent(ctx, eventID, userID)
	if err != nil {
		return err
	}
	if sent || len(devices) == 0 {
		return nil
	}

	var (
		failed  []Device
		lastErr error
	)
	for _, d := range devices {
		err := p.sendWithRetry(ctx, d, n)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return ctx.Err() // shutting down: leave the offset uncommitted
		case errors.Is(err, ErrInvalidToken):
			if derr := p.cfg.Store.DeleteDevice(ctx, d.Token); derr != nil {
				p.cfg.Logger.Warn("failed to delete dead token", "user_id", userID, "error", derr)
			}
		default:
			failed = append(failed, d)
			lastErr = err
		}
	}

	if len(failed) > 0 {
		b, err := json.Marshal(DeadLetter{
			EventID: eventID, UserID: userID, Devices: failed,
			Notification: n, Error: lastErr.Error(), FailedAt: time.Now().UTC(),
		})
		if err != nil {
			return err
		}
		if err := p.cfg.Publisher.Publish(ctx, TopicDLQ, userID, b); err != nil {
			return err
		}
		p.cfg.Logger.Warn("push dead-lettered", "event_id", eventID, "user_id", userID, "devices", len(failed), "error", lastErr)
	}
	// Mark even after a DLQ: the failed devices live in the DLQ now, and re-processing
	// this event must not re-push to the devices that already succeeded.
	return p.cfg.Store.MarkSent(ctx, eventID, userID)
}

func (p *Processor) sendWithRetry(ctx context.Context, d Device, n Notification) error {
	var err error
	for attempt := 0; attempt < p.cfg.MaxAttempts; attempt++ {
		err = p.cfg.Provider.Send(ctx, d, n)
		if err == nil || !errors.Is(err, ErrTransient) {
			return err
		}
		if attempt == p.cfg.MaxAttempts-1 {
			break
		}
		wait := p.cfg.BaseBackoff
		for i := 0; i < attempt; i++ {
			wait *= 3
		}
		wait += time.Duration(rand.Float64() * 0.2 * float64(wait))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return err
}

func (p *Processor) buildNotification(m messageData) Notification {
	title := m.SenderName
	if title == "" {
		title = "New message"
	}
	body := "New message"
	if p.cfg.IncludePreview && m.Body != "" {
		body = truncateRunes(m.Body, p.cfg.PreviewRunes)
	}
	return Notification{Title: title, Body: body, ConversationID: m.ConversationID, MessageID: m.MessageID, Seq: m.Seq}
}

func (p *Processor) deadLetterRaw(ctx context.Context, payload []byte, reason string) error {
	p.cfg.Logger.Error("poison message sent to DLQ", "reason", reason)
	return p.cfg.Publisher.Publish(ctx, TopicDLQ, "poison", payload)
}

// ------------------------------------------------------------------ helpers

func recipientsExcluding(ids []string, exclude string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" || id == exclude {
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

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
}

func fanOut(ctx context.Context, concurrency int, items []string, fn func(string) error) error {
	sem := make(chan struct{}, concurrency)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
loop:
	for _, it := range items {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			break loop
		}
		wg.Add(1)
		go func(it string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(it); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(it)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// ------------------------------------------------------------------ housekeeping

type Pruner interface {
	PruneStaleDevices(ctx context.Context, olderThan time.Duration) (int64, error)
	PruneLog(ctx context.Context, olderThan time.Duration) (int64, error)
}

// RunPruner deletes device tokens not refreshed in deviceMaxAge and old
// notification_log rows. Run it inside the notification worker.
func RunPruner(ctx context.Context, p Pruner, every, deviceMaxAge, logMaxAge time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := p.PruneStaleDevices(ctx, deviceMaxAge); err != nil {
				log.Warn("prune devices failed", "error", err)
			} else if n > 0 {
				log.Info("pruned stale device tokens", "count", n)
			}
			if n, err := p.PruneLog(ctx, logMaxAge); err != nil {
				log.Warn("prune notification_log failed", "error", err)
			} else if n > 0 {
				log.Info("pruned notification_log rows", "count", n)
			}
		}
	}
}