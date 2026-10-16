// Command api serves the REST API: conversations, history, sync, webhook and
// device-token management. Live WebSocket traffic is served by cmd/gateway.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sabya/real-time-chat/internal/auth"
	"github.com/sabya/real-time-chat/internal/cache"
	"github.com/sabya/real-time-chat/internal/chat"
	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/httpx"
	"github.com/sabya/real-time-chat/internal/notifications"
	"github.com/sabya/real-time-chat/internal/observability"
	"github.com/sabya/real-time-chat/internal/realtime"
	"github.com/sabya/real-time-chat/internal/webhooks"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Require(config.PartDB, config.PartRedis, config.PartJWT); err != nil {
		return err
	}
	log := observability.NewLogger(cfg.LogLevel, !cfg.IsDev())
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb, err := cache.NewRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()

	// ---- identity: every handler resolves the caller from the Bearer token
	verifier := auth.NewTokenVerifier(cfg.JWTSecret)
	identity := func(r *http.Request) (chat.Identity, bool) {
		p, err := verifier.FromHeader(r)
		if err != nil {
			return chat.Identity{}, false
		}
		return chat.Identity{ID: p.ID, Name: p.Name}, true
	}
	userID := func(r *http.Request) (string, bool) {
		p, err := verifier.FromHeader(r)
		return p.ID, err == nil
	}

	// ---- chat: the API only reads presence (empty gateway id) to route live frames
	presence := realtime.NewPresence(rdb, "", 60*time.Second)
	chatSvc := chat.NewService(
		chat.NewRepository(pool),
		chat.NewRealtimeAdapter(realtime.NewPublisher(rdb, presence)),
		log,
	)

	mux := http.NewServeMux()
	chat.NewHandler(chat.HandlerConfig{Service: chatSvc, Identity: identity, Logger: log}).Register(mux)
	webhooks.NewHandler(webhooks.HandlerConfig{
		Store: webhooks.NewRepository(pool), UserID: userID, AllowInsecure: cfg.IsDev(),
	}).Register(mux)
	notifications.NewHandler(notifications.HandlerConfig{
		Store: notifications.NewRepository(pool), UserID: userID,
	}).Register(mux)

	// Mount your existing login/register routes here (they issue the JWTs verified above):
	//   auth.NewHandler(...).Register(mux)

	ready := func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return rdb.Ping(ctx).Err()
	}
	mux.HandleFunc("GET /healthz", observability.LiveHandler)
	mux.HandleFunc("GET /readyz", observability.ReadyHandler(ready))

	// AccessLog must stay OUTSIDE Recoverer: a panic unwinds past the logger
	// otherwise and the request never reaches the access log. Recoverer is
	// inside CORS so a panic response still carries the CORS headers.
	handler := httpx.Chain(mux,
		httpx.AccessLog(log),
		httpx.Recoverer(log),
		httpx.CORS(cfg.AllowedOrigins),
	)

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	log.Info("api listening", "addr", cfg.APIAddr, "env", cfg.Env)
	return observability.RunHTTP(ctx, srv, cfg.ShutdownTimeout)
}