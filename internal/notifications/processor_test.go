package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------- fakes

type fakeStore struct {
	mu      sync.Mutex
	muted   map[string]bool
	devices []Device
	sent    map[string]bool
	deleted []string
	err     error
}

func (s *fakeStore) NotifiableUsers(_ context.Context, _ string, ids []string) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []string
	for _, id := range ids {
		if !s.muted[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *fakeStore) DevicesForUsers(_ context.Context, ids []string) ([]Device, error) {
	var out []Device
	for _, d := range s.devices {
		if slices.Contains(ids, d.UserID) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (s *fakeStore) DeleteDevice(_ context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, token)
	return nil
}

func (s *fakeStore) WasSent(_ context.Context, ev, uid string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent[ev+"|"+uid], nil
}

func (s *fakeStore) MarkSent(_ context.Context, ev, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent[ev+"|"+uid] = true
	return nil
}

type fakePresence struct {
	online map[string]bool
	err    error
}

func (p *fakePresence) Gateways(_ context.Context, ids []string) (map[string][]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	var on []string
	for _, id := range ids {
		if p.online[id] {
			on = append(on, id)
		}
	}
	return map[string][]string{"gw1": on}, nil
}

type fakeProvider struct {
	mu       sync.Mutex
	attempts map[string]int
	fn       func(d Device, attempt int) error
}

func (f *fakeProvider) Send(_ context.Context, d Device, _ Notification) error {
	f.mu.Lock()
	f.attempts[d.Token]++
	a := f.attempts[d.Token]
	f.mu.Unlock()
	if f.fn == nil {
		return nil
	}
	return f.fn(d, a)
}

func (f *fakeProvider) tokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for t := range f.attempts {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

type fakePublisher struct {
	mu     sync.Mutex
	topics []string
}

func (p *fakePublisher) Publish(_ context.Context, topic, _ string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics = append(p.topics, topic)
	return nil
}

// ---------------------------------------------------------------- harness

type harness struct {
	store    *fakeStore
	presence *fakePresence
	provider *fakeProvider
	pub      *fakePublisher
	proc     *Processor
}

func newHarness() *harness {
	h := &harness{
		store: &fakeStore{
			muted: map[string]bool{},
			sent:  map[string]bool{},
			devices: []Device{
				{UserID: "u1", Platform: PlatformAndroid, Token: "tok-u1"},
				{UserID: "u2", Platform: PlatformAndroid, Token: "tok-u2"},
				{UserID: "u3", Platform: PlatformIOS, Token: "tok-u3"},
				{UserID: "u4", Platform: PlatformIOS, Token: "tok-u4"},
			},
		},
		presence: &fakePresence{online: map[string]bool{}},
		provider: &fakeProvider{attempts: map[string]int{}},
		pub:      &fakePublisher{},
	}
	h.proc = NewProcessor(ProcessorConfig{
		Store: h.store, Presence: h.presence, Provider: h.provider, Publisher: h.pub,
		MaxAttempts: 3, BaseBackoff: time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return h
}

func eventPayload(sender string, recipients ...string) []byte {
	data, _ := json.Marshal(messageData{
		MessageID: "m1", ConversationID: "c1", Seq: 7,
		SenderID: sender, SenderName: "Alice", Body: "hello",
	})
	b, _ := json.Marshal(envelope{
		EventID: "e1", Type: EventMessageCreated, Version: 1,
		OccurredAt: time.Now(), RecipientIDs: recipients, Data: data,
	})
	return b
}

// ---------------------------------------------------------------- tests

func TestSkipsSenderOnlineAndMuted(t *testing.T) {
	h := newHarness()
	h.presence.online["u2"] = true
	h.store.muted["u3"] = true

	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u1", "u2", "u3", "u4")); err != nil {
		t.Fatal(err)
	}
	if got := h.provider.tokens(); !slices.Equal(got, []string{"tok-u4"}) {
		t.Fatalf("pushed to %v, want only tok-u4", got)
	}
}

func TestAlreadyNotifiedIsSkipped(t *testing.T) {
	h := newHarness()
	h.store.sent["e1|u4"] = true
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err != nil {
		t.Fatal(err)
	}
	if len(h.provider.tokens()) != 0 {
		t.Fatal("must not push again for an already-notified event")
	}
}

func TestInvalidTokenIsDeleted(t *testing.T) {
	h := newHarness()
	h.provider.fn = func(Device, int) error { return ErrInvalidToken }
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.store.deleted, []string{"tok-u4"}) {
		t.Fatalf("deleted=%v", h.store.deleted)
	}
	if len(h.pub.topics) != 0 {
		t.Fatal("dead token must not go to the DLQ")
	}
}

func TestTransientErrorIsRetriedThenSucceeds(t *testing.T) {
	h := newHarness()
	h.provider.fn = func(_ Device, attempt int) error {
		if attempt < 3 {
			return ErrTransient
		}
		return nil
	}
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err != nil {
		t.Fatal(err)
	}
	if h.provider.attempts["tok-u4"] != 3 {
		t.Fatalf("attempts=%d, want 3", h.provider.attempts["tok-u4"])
	}
	if len(h.pub.topics) != 0 || !h.store.sent["e1|u4"] {
		t.Fatal("expected success with no DLQ")
	}
}

func TestExhaustedRetriesGoToDLQAndOffsetCommits(t *testing.T) {
	h := newHarness()
	h.provider.fn = func(Device, int) error { return ErrTransient }
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err != nil {
		t.Fatalf("must return nil so the offset commits, got %v", err)
	}
	if !slices.Equal(h.pub.topics, []string{TopicDLQ}) {
		t.Fatalf("topics=%v", h.pub.topics)
	}
	if !h.store.sent["e1|u4"] {
		t.Fatal("must mark sent so a redelivery does not re-push")
	}
}

func TestPresenceFailureFailsOpen(t *testing.T) {
	h := newHarness()
	h.presence.err = errors.New("redis down")
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.provider.tokens(), []string{"tok-u4"}) {
		t.Fatal("should still push when presence is unavailable")
	}
}

func TestStoreErrorIsReturnedSoOffsetIsNotCommitted(t *testing.T) {
	h := newHarness()
	h.store.err = errors.New("db down")
	if err := h.proc.HandleEvent(context.Background(), eventPayload("u1", "u4")); err == nil {
		t.Fatal("expected an error")
	}
}

func TestMalformedEventGoesToDLQ(t *testing.T) {
	h := newHarness()
	if err := h.proc.HandleEvent(context.Background(), []byte("not json")); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.pub.topics, []string{TopicDLQ}) {
		t.Fatalf("topics=%v", h.pub.topics)
	}
}

func TestPreviewIsGenericUnlessEnabled(t *testing.T) {
	p := NewProcessor(ProcessorConfig{})
	n := p.buildNotification(messageData{SenderName: "Alice", Body: "secret text"})
	if n.Body != "New message" || n.Title != "Alice" {
		t.Fatalf("got %+v", n)
	}
	p = NewProcessor(ProcessorConfig{IncludePreview: true, PreviewRunes: 5})
	if got := p.buildNotification(messageData{Body: "héllo wörld"}).Body; got != "héll…" {
		t.Fatalf("got %q", got)
	}
}