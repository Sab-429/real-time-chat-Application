package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef"

func newManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(testSecret, time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func TestNewManagerRejectsWeakSecrets(t *testing.T) {
	if _, err := NewManager("", time.Hour); err == nil {
		t.Fatal("empty secret accepted")
	}
	if _, err := NewManager("short", time.Hour); err == nil {
		t.Fatal("short secret accepted")
	}
	if _, err := NewManager(testSecret, 0); err != nil {
		t.Fatalf("zero TTL should fall back to the default: %v", err)
	}
}

func TestIssueParseRoundTrip(t *testing.T) {
	m := newManager(t)

	token, err := m.Issue("user-1", "alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if strings.Count(token, ".") != 2 {
		t.Fatalf("not a JWT: %q", token)
	}

	claims, err := m.Parse(token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if claims.UserID != "user-1" || claims.Username != "alice" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	if claims.ExpiresAt <= claims.IssuedAt {
		t.Fatalf("expiry must be after issue time: %+v", claims)
	}
}

func TestParseRejects(t *testing.T) {
	m := newManager(t)
	other, _ := NewManager("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", time.Hour)

	valid, err := m.Issue("user-1", "alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	tampered := valid[:len(valid)-1] + "x"

	cases := []struct {
		name  string
		token string
		parse func() (*Manager, string)
	}{
		{"empty", "", func() (*Manager, string) { return m, "" }},
		{"garbage", "not-a-token", func() (*Manager, string) { return m, "not-a-token" }},
		{"two parts", "a.b", func() (*Manager, string) { return m, "a.b" }},
		{"wrong signer", valid, func() (*Manager, string) { return other, valid }},
		{"tampered payload", valid, func() (*Manager, string) { return m, tampered }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, tok := tc.parse()
			if _, err := mgr.Parse(tok); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("got %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestParseRejectsExpiredToken(t *testing.T) {
	shortLived, err := NewManager(testSecret, time.Nanosecond)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	token, err := shortLived.Issue("user-1", "alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if _, err := newManager(t).Parse(token); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("got %v, want ErrExpiredToken", err)
	}
}

func TestPasswordHashing(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !CheckPassword(hash, "correct horse battery staple") {
		t.Fatal("correct password rejected")
	}
	if CheckPassword(hash, "wrong password") {
		t.Fatal("wrong password accepted")
	}
	if _, err := HashPassword("short"); !errors.Is(err, ErrWeakPassword) {
		t.Fatalf("weak password accepted: %v", err)
	}
	if _, err := HashPassword(strings.Repeat("x", 80)); err == nil {
		t.Fatal("over-long password accepted")
	}
}

func TestNewSecretIsUniqueAndLongEnough(t *testing.T) {
	a, err := NewSecret()
	if err != nil {
		t.Fatalf("NewSecret: %v", err)
	}
	b, err := NewSecret()
	if err != nil {
		t.Fatalf("NewSecret: %v", err)
	}
	if a == b {
		t.Fatal("secrets must differ")
	}
	if len(a) != 64 {
		t.Fatalf("secret length = %d, want 64 hex chars", len(a))
	}
}
