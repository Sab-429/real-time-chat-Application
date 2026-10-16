package webhooks

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1"}`)
	now := time.Unix(1_700_000_000, 0)
	sig := Sign(secret, now, body)
	ts := "1700000000"

	tests := []struct {
		name    string
		secret  string
		sig     string
		ts      string
		body    []byte
		now     time.Time
		wantErr error
	}{
		{"valid", secret, sig, ts, body, now, nil},
		{"tampered body", secret, sig, ts, []byte(`{"id":"evt_2"}`), now, ErrInvalidSignature},
		{"wrong secret", "other", sig, ts, body, now, ErrInvalidSignature},
		{"stale timestamp", secret, sig, ts, body, now.Add(10 * time.Minute), ErrTimestampOutOfRange},
		{"future timestamp", secret, sig, ts, body, now.Add(-10 * time.Minute), ErrTimestampOutOfRange},
		{"bad timestamp", secret, sig, "abc", body, now, ErrMalformedTimestamp},
		{"empty signature", secret, "", ts, body, now, ErrInvalidSignature},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.secret, tc.sig, tc.ts, tc.body, DefaultTolerance, tc.now)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestGenerateSecret(t *testing.T) {
	a, _ := GenerateSecret()
	b, _ := GenerateSecret()
	if !strings.HasPrefix(a, "whsec_") || a == b {
		t.Fatalf("bad secrets: %q %q", a, b)
	}
}