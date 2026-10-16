package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

const (
	// minPasswordLen is the shortest password accepted at registration.
	minPasswordLen = 8
	// maxPasswordLen mirrors bcrypt's hard limit: it only ever reads the first
	// 72 bytes of a password, so longer input would be silently truncated and
	// "password + padding" would compare equal to the bare password.
	maxPasswordLen = 72
)

var (
	// ErrWeakPassword rejects passwords outside the accepted length window.
	ErrWeakPassword = errors.New("password must be 8-72 characters")

	// ErrInvalidCredentials is the only answer login gives for an unknown
	// username and for a wrong password, so the response cannot be used to
	// enumerate accounts.
	ErrInvalidCredentials = errors.New("invalid username or password")
)

// HashPassword hashes a password with bcrypt at the default cost.
func HashPassword(password string) (string, error) {
	if len(password) < minPasswordLen || len(password) > maxPasswordLen {
		return "", ErrWeakPassword
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// CheckPassword reports whether password matches a hash produced by
// HashPassword. A malformed or non-bcrypt hash simply does not match.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// NewSecret returns a fresh 32-byte secret as 64 hex characters, ready to be
// used as JWT_SECRET.
func NewSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
