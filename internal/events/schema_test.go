package events

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"
)

// Golden test: these exact field names are the contract with every consumer.
func TestWireFormatIsStable(t *testing.T) {
	payload, err := NewMessageCreated("evt-1", time.Unix(1_700_000_000, 0), []string{"a", "b"}, MessageCreated{
		MessageID: "m1", ConversationID: "c1", Seq: 3, SenderID: "a", SenderName: "Alice", Body: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}

	var top map[string]json.RawMessage
	_ = json.Unmarshal(payload, &top)
	if got, want := keys(top), []string{"data", "event_id", "occurred_at", "recipient_ids", "type", "version"}; !equal(got, want) {
		t.Fatalf("envelope keys = %v, want %v", got, want)
	}
	var data map[string]json.RawMessage
	_ = json.Unmarshal(top["data"], &data)
	if got, want := keys(data), []string{"body", "conversation_id", "message_id", "sender_id", "sender_name", "seq"}; !equal(got, want) {
		t.Fatalf("data keys = %v, want %v", got, want)
	}
}

func TestRoundTrip(t *testing.T) {
	payload, _ := NewMessageCreated("evt-1", time.Unix(1_700_000_000, 0), []string{"a", "b"}, MessageCreated{
		MessageID: "m1", ConversationID: "c1", Seq: 3, SenderID: "a", SenderName: "Alice", Body: "hi",
	})
	env, err := DecodeEnvelope(payload)
	if err != nil || env.EventID != "evt-1" || env.Type != TypeMessageCreated || env.Version != SchemaVersion {
		t.Fatalf("env=%+v err=%v", env, err)
	}
	m, err := env.DecodeMessageCreated()
	if err != nil || m.Seq != 3 || m.Body != "hi" || m.SenderName != "Alice" {
		t.Fatalf("m=%+v err=%v", m, err)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for name, payload := range map[string]string{
		"not json":    `nope`,
		"no event id": `{"type":"x","version":1}`,
		"no type":     `{"event_id":"e","version":1}`,
		"bad version": `{"event_id":"e","type":"x","version":0}`,
	} {
		if _, err := DecodeEnvelope([]byte(payload)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
	env, _ := DecodeEnvelope([]byte(`{"event_id":"e","type":"other","version":1}`))
	if _, err := env.DecodeMessageCreated(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("wrong type must be rejected, got %v", err)
	}
}

func TestUnknownFieldsAreIgnored(t *testing.T) {
	payload := `{"event_id":"e","type":"chat.message.created","version":1,"future":true,
	              "data":{"message_id":"m","conversation_id":"c","extra":1}}`
	env, err := DecodeEnvelope([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.DecodeMessageCreated(); err != nil {
		t.Fatal(err)
	}
}

func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
