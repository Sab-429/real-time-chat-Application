package webhooks

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

const (
	HeaderSignature = "X-Webhook-Signature"
	HeaderTimestamp = "X-Webhook-Timestamp"
	HeaderID        = "X-Webhook-Id"      // event id: receivers dedupe on this
	HeaderAttempt   = "X-Webhook-Attempt" // 1-based

	DefaultTolerance = 5 * time.Minute
)

var (
	ErrInvalidSignature    = errors.New("webhooks: invalid signature")
	ErrTimestampOutOfRange = errors.New("webhooks: timestamp outside tolerance")
	ErrMalformedTimestamp  = errors.New("webhooks: malformed timestamp")
)

// Sign returns "v1=<hex>" = HMAC-SHA256(secret, "<unix-ts>.<body>").
// Binding the timestamp into the MAC lets receivers reject replays.
func Sign(secret string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts.Unix(), 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify is the receiver-side check (also useful for your own tests/docs).
func Verify(secret, signature, timestamp string, body []byte, tolerance time.Duration, now time.Time) error {
	sec, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrMalformedTimestamp
	}
	ts := time.Unix(sec, 0)
	if d := now.Sub(ts); d > tolerance || d < -tolerance {
		return ErrTimestampOutOfRange
	}
	if !hmac.Equal([]byte(Sign(secret, ts, body)), []byte(signature)) {
		return ErrInvalidSignature
	}
	return nil
}

// GenerateSecret returns a 256-bit random signing secret.
func GenerateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "whsec_" + base64.RawURLEncoding.EncodeToString(b), nil
}
