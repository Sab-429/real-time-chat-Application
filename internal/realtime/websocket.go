package realtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"github.com/sabya/real-time-chat/internal/httpx"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = pongWait * 9 / 10
)

// ---------------------------------------------------------------- Client

// Client is one WebSocket connection.
type Client struct {
	UserID  string
	ConnID  string
	conn    *websocket.Conn
	send    chan []byte
	done    chan struct{}
	once    sync.Once
	limiter *rate.Limiter
}

func newClient(userID string, conn *websocket.Conn, sendBuf int, lim *rate.Limiter) *Client {
	return &Client{
		UserID:  userID,
		ConnID:  randomID(),
		conn:    conn,
		send:    make(chan []byte, sendBuf),
		done:    make(chan struct{}),
		limiter: lim,
	}
}

// Enqueue is non-blocking. false means the client is closed or too slow.
func (c *Client) Enqueue(b []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.send <- b:
		return true
	default:
		return false
	}
}

// Close is idempotent and safe to call from any goroutine.
func (c *Client) Close(code int, reason string) {
	c.once.Do(func() {
		if c.conn != nil {
			_ = c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
		}
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				// The near universal cause is a client that closed the tab:
				// httpx.LogWriteFailure logs that as churn (debug), and only
				// reports an actual write fault at error level.
				httpx.LogWriteFailure("ws write", c.ConnID, err)
				c.Close(websocket.CloseInternalServerErr, "")
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				// No pong within pongWait, or the peer stopped reading: both
				// are the connection reaching its end, not a server fault.
				httpx.LogWriteFailure("ws ping", c.ConnID, err)
				c.Close(websocket.CloseInternalServerErr, "")
				return
			}
		}
	}
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------------- Server

// Authenticator validates the handshake and returns the user ID.
// Browsers cannot set an Authorization header on WebSocket requests, so verify
// a short-lived JWT from the query string (?token=) or Sec-WebSocket-Protocol.
type Authenticator func(r *http.Request) (userID string, err error)

// FrameHandler processes inbound frames (message.send, sync.request, typing...).
// Implement it in cmd/gateway by calling the chat service (gRPC or in-process).
// Return *FrameError for client-safe errors.
type FrameHandler interface {
	HandleFrame(ctx context.Context, c *Client, f Frame) error
}

// PresenceRegistry is satisfied by *Presence.
type PresenceRegistry interface {
	Register(ctx context.Context, userID string) error
	Unregister(ctx context.Context, userID string) error
}

type ServerConfig struct {
	Hub             *Hub
	Presence        PresenceRegistry
	Auth            Authenticator
	Handler         FrameHandler
	AllowedOrigins  []string // exact origins; "*" allows all (dev only); empty = same-origin only
	MaxConnsPerUser int      // default 5
	SendBuffer      int      // per-connection outbound queue, default 256
	MaxMessageBytes int64    // inbound frame limit, default 64 KiB
	FramesPerSecond float64  // inbound rate limit per connection, default 20
	FrameBurst      int      // default 40
	Logger          *slog.Logger
}

type Server struct {
	cfg      ServerConfig
	upgrader websocket.Upgrader
	log      *slog.Logger
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Hub == nil || cfg.Presence == nil || cfg.Auth == nil || cfg.Handler == nil {
		return nil, errors.New("realtime: Hub, Presence, Auth and Handler are required")
	}
	if cfg.MaxConnsPerUser <= 0 {
		cfg.MaxConnsPerUser = 5
	}
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = 256
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 64 << 10
	}
	if cfg.FramesPerSecond <= 0 {
		cfg.FramesPerSecond = 20
	}
	if cfg.FrameBurst <= 0 {
		cfg.FrameBurst = 40
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cfg: cfg,
		log: log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     originChecker(cfg.AllowedOrigins),
		},
	}, nil
}

func originChecker(allowed []string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // non-browser clients (mobile apps) send no Origin
		}
		if slices.Contains(allowed, "*") || slices.Contains(allowed, origin) {
			return true
		}
		u, err := url.Parse(origin)
		return err == nil && u.Host == r.Host
	}
}

// ServeHTTP is the /ws endpoint: authenticate BEFORE upgrading so failures are real HTTP errors.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID, err := s.cfg.Auth(r)
	if err != nil || userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if s.cfg.Hub.CountUser(userID) >= s.cfg.MaxConnsPerUser {
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the HTTP error
	}

	c := newClient(userID, conn, s.cfg.SendBuffer, rate.NewLimiter(rate.Limit(s.cfg.FramesPerSecond), s.cfg.FrameBurst))

	first, err := s.cfg.Hub.Add(c)
	if err != nil { // gateway is draining
		c.Close(websocket.CloseServiceRestart, "server restarting")
		return
	}
	if first {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		err := s.cfg.Presence.Register(ctx, userID)
		cancel()
		if err != nil {
			// Fail closed: without presence nobody can route live messages to this socket.
			s.log.Error("presence register failed", "user_id", userID, "error", err)
			c.Close(websocket.CloseInternalServerErr, "presence unavailable")
			s.leave(c)
			return
		}
	}

	go c.writePump()
	s.readPump(c) // blocks until the connection ends
	s.leave(c)
}

func (s *Server) readPump(c *Client) {
	c.conn.SetReadLimit(s.cfg.MaxMessageBytes)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				s.log.Debug("ws read ended", "user_id", c.UserID, "conn_id", c.ConnID, "error", err)
			}
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))

		if !c.limiter.Allow() {
			c.Close(websocket.ClosePolicyViolation, "rate limit exceeded")
			return
		}

		var f Frame
		if err := json.Unmarshal(data, &f); err != nil || f.Type == "" {
			c.Enqueue(ErrorFrame("", "bad_frame", "invalid frame"))
			continue
		}

		// Handled inline: preserves per-connection ordering of the client's own sends.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = s.cfg.Handler.HandleFrame(ctx, c, f)
		cancel()
		if err != nil {
			var fe *FrameError
			if errors.As(err, &fe) {
				c.Enqueue(ErrorFrame(f.ID, fe.Code, fe.Message))
			} else {
				s.log.Error("frame handler failed", "user_id", c.UserID, "type", f.Type, "error", err)
				c.Enqueue(ErrorFrame(f.ID, "internal_error", "internal error"))
			}
		}
	}
}

// leave removes the connection and clears presence if it was the user's last one here.
func (s *Server) leave(c *Client) {
	c.Close(websocket.CloseNormalClosure, "")
	if !s.cfg.Hub.Remove(c) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.cfg.Presence.Unregister(ctx, c.UserID); err != nil {
		s.log.Warn("presence unregister failed", "user_id", c.UserID, "error", err)
	}
	// Raced with a brand-new connection from the same user on this gateway:
	// put presence back (the heartbeat would also repair it within ttl/3).
	if s.cfg.Hub.HasUser(c.UserID) {
		_ = s.cfg.Presence.Register(ctx, c.UserID)
	}
}

// Shutdown stops accepting sockets, tells clients to reconnect elsewhere, and
// waits (until ctx expires) for connections to drain. Clients should reconnect
// with jittered backoff and then call sync.request with their last seq.
func (s *Server) Shutdown(ctx context.Context) error {
	s.cfg.Hub.CloseAll(websocket.CloseServiceRestart, "server restarting")
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for s.cfg.Hub.Total() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}
