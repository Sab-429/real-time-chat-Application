// Command gateway terminates WebSocket connections. It scales on connection
// count, independently of the REST API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sabya/real-time-chat/internal/auth"
	"github.com/sabya/real-time-chat/internal/cache"
	"github.com/sabya/real-time-chat/internal/chat"
	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
	"github.com/sabya/real-time-chat/internal/httpx"
	"github.com/sabya/real-time-chat/internal/observability"
	"github.com/sabya/real-time-chat/internal/realtime"
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

	gwID := realtime.NewGatewayID()
	hub := realtime.NewHub()
	presence := realtime.NewPresence(rdb, gwID, 60*time.Second)

	// The gateway executes sends in-process (modular monolith). To split it from
	// the database later, put an RPC client behind chat.FrameRouter instead.
	chatSvc := chat.NewService(
		chat.NewRepository(pool),
		chat.NewRealtimeAdapter(realtime.NewPublisher(rdb, presence)),
		log,
	)

	// Display names for push titles, captured from the verified token at connect time.
	var names sync.Map
	router := chat.NewFrameRouter(chatSvc, func(_ context.Context, uid string) string {
		v, _ := names.Load(uid)
		s, _ := v.(string)
		return s
	})

	// Subscribe BEFORE accepting sockets, so nobody connects to a gateway that cannot receive.
	sub := realtime.NewSubscriber(rdb, gwID, hub)
	if err := sub.Start(ctx); err != nil {
		return fmt.Errorf("redis subscribe: %w", err)
	}
	defer sub.Close()

	// Background loops outlive the signal context so presence stays fresh while sockets drain.
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	go presence.RunHeartbeat(bgCtx, hub.Users)
	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-bgCtx.Done():
				return
			case <-t.C:
				names.Range(func(k, _ any) bool {
					if !hub.HasUser(k.(string)) {
						names.Delete(k)
					}
					return true
				})
			}
		}
	}()

	verifier := auth.NewTokenVerifier(cfg.JWTSecret)
	wsSrv, err := realtime.NewServer(realtime.ServerConfig{
		Hub:      hub,
		Presence: presence,
		Handler:  router,
		Auth: func(r *http.Request) (string, error) {
			p, err := verifier.FromHeaderOrQuery(r)
			if err != nil {
				return "", err
			}
			names.Store(p.ID, p.Name)
			return p.ID, nil
		},
		AllowedOrigins: cfg.AllowedOrigins,
		Logger:         log,
	})
	if err != nil {
		return err
	}

	var draining atomic.Bool
	mux := http.NewServeMux()
	mux.Handle("GET /ws", wsSrv)
	mux.HandleFunc("GET /healthz", observability.LiveHandler)
	mux.HandleFunc("GET /readyz", observability.ReadyHandler(func(ctx context.Context) error {
		if draining.Load() {
			return errors.New("draining")
		}
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return rdb.Ping(ctx).Err()
	}))

	// No Read/WriteTimeout: they would apply to hijacked WebSocket connections' handshake
	// phase only, and the realtime server manages its own deadlines (ping/pong).
	//
	// Only panic recovery wraps the mux. There is deliberately no access log
	// here: it would emit one line per WebSocket at the moment its socket
	// closes, minutes after the handshake. Recovery, on the other hand, turns
	// net/http's raw "http: panic serving ..." stack from a bad frame handler
	// into our structured log, and writes nothing when the socket is already
	// gone (a hijacked connection cannot take a 500 anyway).
	httpSrv := &http.Server{
		Addr:              cfg.GatewayAddr,
		Handler:           httpx.Chain(mux, httpx.Recoverer(log)),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", cfg.GatewayAddr, "gateway_id", gwID)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Graceful drain: fail readiness so the load balancer stops sending new
	// connections, then ask clients to reconnect (close code 1012) and wait.
	// Graceful drain: fail readiness so the load balancer stops sending new
	// connections, then ask clients to reconnect (close code 1012) and wait.
	log.Info("shutting down: draining connections", "open", hub.Total())
	draining.Store(true)
	time.Sleep(cfg.ShutdownDelay) // let the load balancer notice /readyz and stop routing here

	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := wsSrv.Shutdown(sctx); err != nil {
		log.Warn("connections did not drain in time", "remaining", hub.Total())
	}
	return httpSrv.Shutdown(sctx)
}