// Command devtoken prints a signed JWT for local testing (DEV ONLY).
//
//	go run ./cmd/devtoken -user alice -name Alice
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/sabya/real-time-chat/internal/config"
)

func main() {
	user := flag.String("user", "", "user id (JWT sub)")
	name := flag.String("name", "", "display name")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
	if !cfg.IsDev() {
		fmt.Fprintln(os.Stderr, "fatal: devtoken refuses to run unless ENV=dev")
		os.Exit(1)
	}
	if *user == "" {
		fmt.Fprintln(os.Stderr, "fatal: -user is required")
		os.Exit(2)
	}

	now := time.Now()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": *user, "name": *name, "iat": now.Unix(), "exp": now.Add(*ttl).Unix(),
	}).SignedString([]byte(cfg.JWTSecret))
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
	fmt.Println(tok)
}
