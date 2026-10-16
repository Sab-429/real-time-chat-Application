package auth

import (
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// defaultTTL is the lifetime used when the caller does not ask for a specific
// one, so a missing or zero value can never mint an already expired token.
const defaultTTL = 24 * time.Hour

// Manager issues the session tokens returned by /auth/register and
// /auth/login, and verifies the ones that come back on later requests.
type Manager struct {
	secret []byte
	ttl    time.Duration
}

// NewManager builds a Manager. The secret must be at least 32 bytes; a
// non-positive ttl falls back to defaultTTL.
func NewManager(secret string, ttl time.Duration) (*Manager, error) {
	if len(secret) < minSecretLen {
		return nil, errors.New("JWT secret must be at least 32 bytes")
	}
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &Manager{secret: []byte(secret), ttl: ttl}, nil
}

// TTL is the lifetime of the tokens this Manager issues.
func (m *Manager) TTL() time.Duration { return m.ttl }

// Issue signs a token for the given account. The account id is written to
// both user_id and sub: user_id is this package's own claim, sub is the
// standard one that TokenVerifier and the rest of the stack read.
func (m *Manager) Issue(userID, username string) (string, error) {
	if strings.TrimSpace(userID) == "" {
		return "", errors.New("user ID is required")
	}

	now := time.Now()
	claims := Claims{
		UserID:    userID,
		Username:  username,
		Subject:   userID,
		Issuer:    issuer,
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		ExpiresAt: now.Add(m.ttl).Unix(),
	}

	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}

// Parse verifies the signature and the standard claims of raw and returns the
// decoded identity. Only HS256 is accepted, so a token signed with another
// algorithm (or with alg=none) is rejected before any claim is trusted.
//
// Two deliberate choices:
//   - exp is enforced with no leeway, so a token that has run out is refused
//     the moment it runs out instead of lingering for a clock-skew window;
//   - the issuer claim is not required: the dev tools (cmd/devtoken,
//     cmd/smoke) mint tokens without it, and the signature plus exp are what
//     actually protect an endpoint.
func (m *Manager) Parse(raw string) (*Claims, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(
		raw,
		claims,
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, errors.New("unexpected signing method")
			}
			return m.secret, nil
		},
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, tokenError(err)
	}
	if !token.Valid {
		return nil, ErrInvalidToken
	}

	if strings.TrimSpace(claims.UserID) == "" || claims.Subject != claims.UserID {
		return nil, errors.New("invalid user identity")
	}
	return claims, nil
}
