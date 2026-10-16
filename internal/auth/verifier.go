package auth

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// If your existing auth.go already declares these names, rename them here.
// Adapt tokenClaims to whatever your login handler issues: this expects an
// HS256 JWT with `sub` = user id, an `exp`, and an optional `name`.

var ErrTokenRejected = errors.New("auth: token rejected")

type Principal struct {
	ID   string
	Name string
}

type tokenClaims struct {
	Name string `json:"name"`
	jwt.RegisteredClaims
}

type TokenVerifier struct{ secret []byte }

func NewTokenVerifier(secret string) *TokenVerifier { return &TokenVerifier{secret: []byte(secret)} }

func (v *TokenVerifier) Verify(raw string) (Principal, error) {
	var c tokenClaims
	_, err := jwt.ParseWithClaims(raw, &c,
		func(*jwt.Token) (any, error) { return v.secret, nil },
		jwt.WithValidMethods([]string{"HS256"}), // blocks alg=none and algorithm-confusion tricks
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(5*time.Second),
	)
	if err != nil || c.Subject == "" {
		return Principal{}, ErrTokenRejected
	}
	return Principal{ID: c.Subject, Name: c.Name}, nil
}

// FromHeader reads "Authorization: Bearer <jwt>" (REST API).
func (v *TokenVerifier) FromHeader(r *http.Request) (Principal, error) {
	h := r.Header.Get("Authorization")
	if len(h) < 8 || !strings.EqualFold(h[:7], "bearer ") {
		return Principal{}, ErrTokenRejected
	}
	return v.Verify(strings.TrimSpace(h[7:]))
}

// FromHeaderOrQuery also accepts ?token= because browsers cannot set headers on
// WebSocket handshakes. Use a SHORT-LIVED token for this and keep query strings
// out of your access logs.
func (v *TokenVerifier) FromHeaderOrQuery(r *http.Request) (Principal, error) {
	if p, err := v.FromHeader(r); err == nil {
		return p, nil
	}
	if t := r.URL.Query().Get("token"); t != "" {
		return v.Verify(t)
	}
	return Principal{}, ErrTokenRejected
}