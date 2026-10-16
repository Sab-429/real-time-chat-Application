package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDKey contextKey = "authenticated_user_id"

// issuer is written into every token this package issues and is checked by
// ValidateToken.
const issuer = "realtime-chat"

// minSecretLen is the shortest HMAC secret accepted when signing or verifying.
const minSecretLen = 32

// ErrInvalidToken reports a token that is malformed, wrongly signed or carries
// an identity that cannot be trusted.
var ErrInvalidToken = errors.New("invalid token")

// ErrExpiredToken reports a token whose exp is in the past.
var ErrExpiredToken = errors.New("token expired")

// Claims is the claim set carried by every token this package issues:
//
//	user_id / sub : the authenticated account id
//	name          : the username, read by TokenVerifier and shown to clients
//	iat, nbf, exp : unix seconds
//
// The registered claims are kept as plain unix seconds so callers can compare
// them directly; the jwt.Claims accessors below adapt them for the library's
// validator, which is what makes exp/nbf/iss actually enforced on parse.
type Claims struct {
	UserID    string           `json:"user_id"`
	Username  string           `json:"name"`
	Subject   string           `json:"sub"`
	Issuer    string           `json:"iss,omitempty"`
	Audience  jwt.ClaimStrings `json:"aud,omitempty"`
	IssuedAt  int64            `json:"iat"`
	NotBefore int64            `json:"nbf,omitempty"`
	ExpiresAt int64            `json:"exp"`
}

func (c Claims) GetExpirationTime() (*jwt.NumericDate, error) { return numericDate(c.ExpiresAt) }
func (c Claims) GetIssuedAt() (*jwt.NumericDate, error)       { return numericDate(c.IssuedAt) }
func (c Claims) GetNotBefore() (*jwt.NumericDate, error)      { return numericDate(c.NotBefore) }
func (c Claims) GetIssuer() (string, error)                   { return c.Issuer, nil }
func (c Claims) GetSubject() (string, error)                  { return c.Subject, nil }
func (c Claims) GetAudience() (jwt.ClaimStrings, error)       { return c.Audience, nil }

// numericDate adapts a unix timestamp to the JWT library's NumericDate. A zero
// timestamp means "claim absent", which is how a missing exp is reported to
// the validator (and then rejected by jwt.WithExpirationRequired).
func numericDate(unix int64) (*jwt.NumericDate, error) {
	if unix == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(unix, 0)), nil
}

// tokenError maps the library's error tree onto the package sentinels so that
// callers can distinguish "the clock ran out" from everything else.
func tokenError(err error) error {
	if errors.Is(err, jwt.ErrTokenExpired) {
		return fmt.Errorf("%w: %v", ErrExpiredToken, err)
	}
	return fmt.Errorf("%w: %v", ErrInvalidToken, err)
}

// GenerateToken creates a signed JWT.
// Use this only after your application has authenticated the user.
func GenerateToken(
	userID string,
	secret string,
	duration time.Duration,
) (string, error) {
	if strings.TrimSpace(userID) == "" {
		return "", errors.New("user ID is required")
	}

	if len(secret) < minSecretLen {
		return "", errors.New("JWT secret must be at least 32 bytes")
	}

	if duration <= 0 {
		return "", errors.New("token duration must be positive")
	}

	now := time.Now()

	claims := Claims{
		UserID:    userID,
		Subject:   userID,
		Issuer:    issuer,
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		ExpiresAt: now.Add(duration).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	return token.SignedString([]byte(secret))
}

// ValidateToken verifies the signature and standard JWT claims.
func ValidateToken(
	tokenString string,
	secret string,
) (*Claims, error) {
	if len(secret) < minSecretLen {
		return nil, errors.New("JWT secret must be at least 32 bytes")
	}

	claims := &Claims{}

	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, errors.New("unexpected signing method")
			}

			return []byte(secret), nil
		},
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer("realtime-chat"),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	if err != nil {
		return nil, tokenError(err)
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	if strings.TrimSpace(claims.UserID) == "" ||
		claims.Subject != claims.UserID {
		return nil, errors.New("invalid user identity")
	}

	return claims, nil
}

// Middleware protects an HTTP endpoint using a Bearer token.
func Middleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			header := r.Header.Get("Authorization")

			if !strings.HasPrefix(header, "Bearer ") {
				http.Error(w, "missing bearer token",
					http.StatusUnauthorized)
				return
			}

			tokenString := strings.TrimSpace(
				strings.TrimPrefix(header, "Bearer "),
			)

			if tokenString == "" {
				http.Error(w, "missing bearer token",
					http.StatusUnauthorized)
				return
			}

			claims, err := ValidateToken(tokenString, secret)
			if err != nil {
				http.Error(w, "invalid or expired token",
					http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(
				r.Context(),
				userIDKey,
				claims.UserID,
			)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// UserID returns the identity established by Middleware. It reports false when
// the request never went through authentication.
func UserID(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(userIDKey).(string)
	return userID, ok && userID != ""
}
