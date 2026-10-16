package events

import (
	"context"
	"errors"
	"testing"
	"time"
)

func payload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := NewMessageCreated(id, time.Now(), []string{"a"}, MessageCreated{MessageID: "m", ConversationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDedupeSkipsDuplicates(t *testing.T) {
	calls := 0
	h := Dedupe(NewMemoryDedupe(), "test", func(context.Context, []byte) error { calls++; return nil })

	for i := 0; i < 3; i++ {
		if err := h(context.Background(), payload(t, "evt-1")); err != nil {
			t.Fatal(err)
		}
	}
	_ = h(context.Background(), payload(t, "evt-2"))
	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2 (once per distinct event)", calls)
	}
}

func TestDedupeDoesNotMarkFailedEvents(t *testing.T) {
	fail := true
	calls := 0
	h := Dedupe(NewMemoryDedupe(), "test", func(context.Context, []byte) error {
		calls++
		if fail {
			return errors.New("boom")
		}
		return nil
	})

	if err := h(context.Background(), payload(t, "evt-1")); err == nil {
		t.Fatal("expected the handler error to propagate")
	}
	fail = false
	if err := h(context.Background(), payload(t, "evt-1")); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("a failed event must be retried, calls=%d", calls)
	}
}

func TestDedupeScopesByConsumer(t *testing.T) {
	store := NewMemoryDedupe()
	a, b := 0, 0
	ha := Dedupe(store, "a", func(context.Context, []byte) error { a++; return nil })
	hb := Dedupe(store, "b", func(context.Context, []byte) error { b++; return nil })
	p := payload(t, "evt-1")
	_ = ha(context.Background(), p)
	_ = hb(context.Background(), p)
	if a != 1 || b != 1 {
		t.Fatalf("a=%d b=%d; each consumer must process the event once", a, b)
	}
}

func TestMalformedPassesThrough(t *testing.T) {
	got := ""
	h := Dedupe(NewMemoryDedupe(), "test", func(_ context.Context, v []byte) error { got = string(v); return nil })
	_ = h(context.Background(), []byte("garbage"))
	if got != "garbage" {
		t.Fatal("malformed payloads must reach the handler so it can dead-letter them")
	}
}
