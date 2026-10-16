package realtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis layout (move these builders into internal/cache/keys.go if you add that package):
//
//   conn:{userID}   ZSET  member = gatewayID, score = expiry (unix seconds)
//   gw:{gatewayID}  Pub/Sub channel carrying Delivery messages
//
// A sorted set (instead of a plain set + TTL) gives every gateway entry its own
// expiry, so a crashed gateway's stale entries age out on their own while other
// devices of the same user, connected elsewhere, stay valid.
// "Online" == the user has at least one non-expired gateway entry.

func ConnKey(userID string) string        { return "conn:" + userID }
func GatewayChannel(gatewayID string) string { return "gw:" + gatewayID }

// NewGatewayID is unique per process start, so a restarted pod never inherits
// a dead instance's identity.
func NewGatewayID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "gw"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

type Presence struct {
	rdb       redis.UniversalClient
	gatewayID string
	ttl       time.Duration
	log       *slog.Logger
	now       func() time.Time
}

// NewPresence: gatewayID may be "" for processes that only READ presence
// (chat service, notification worker).
func NewPresence(rdb redis.UniversalClient, gatewayID string, ttl time.Duration) *Presence {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &Presence{rdb: rdb, gatewayID: gatewayID, ttl: ttl, log: slog.Default(), now: time.Now}
}

func (p *Presence) queueTouch(ctx context.Context, pipe redis.Pipeliner, userID string, now time.Time) {
	key := ConnKey(userID)
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.Add(p.ttl).Unix()), Member: p.gatewayID})
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now.Unix(), 10)) // prune expired entries
	pipe.Expire(ctx, key, p.ttl*2)                                              // GC the whole key if everything dies
}

// Register marks the user as connected to this gateway.
func (p *Presence) Register(ctx context.Context, userID string) error {
	pipe := p.rdb.Pipeline()
	p.queueTouch(ctx, pipe, userID, p.now())
	_, err := pipe.Exec(ctx)
	return err
}

func (p *Presence) Unregister(ctx context.Context, userID string) error {
	return p.rdb.ZRem(ctx, ConnKey(userID), p.gatewayID).Err()
}

// Refresh extends the expiry for every locally connected user.
func (p *Presence) Refresh(ctx context.Context, userIDs []string) error {
	const chunk = 500
	for i := 0; i < len(userIDs); i += chunk {
		end := min(i+chunk, len(userIDs))
		now := p.now()
		pipe := p.rdb.Pipeline()
		for _, uid := range userIDs[i:end] {
			p.queueTouch(ctx, pipe, uid, now)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// RunHeartbeat refreshes presence every ttl/3 until ctx is cancelled.
// It also self-heals the rare race where a user's last socket closes while a
// new one connects (the new entry is re-added on the next tick).
func (p *Presence) RunHeartbeat(ctx context.Context, users func() []string) {
	t := time.NewTicker(p.ttl / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, p.ttl/3)
			if err := p.Refresh(rctx, users()); err != nil {
				p.log.Warn("presence refresh failed", "error", err)
			}
			cancel()
		}
	}
}

func (p *Presence) IsOnline(ctx context.Context, userID string) (bool, error) {
	n, err := p.rdb.ZCount(ctx, ConnKey(userID), strconv.FormatInt(p.now().Unix(), 10), "+inf").Result()
	return n > 0, err
}

// Gateways maps gatewayID -> users (from userIDs) currently connected there.
// Offline users do not appear in the result.
func (p *Presence) Gateways(ctx context.Context, userIDs []string) (map[string][]string, error) {
	out := make(map[string][]string)
	if len(userIDs) == 0 {
		return out, nil
	}
	now := strconv.FormatInt(p.now().Unix(), 10)
	pipe := p.rdb.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(userIDs))
	for i, uid := range userIDs {
		cmds[i] = pipe.ZRangeByScore(ctx, ConnKey(uid), &redis.ZRangeBy{Min: now, Max: "+inf"})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for i, cmd := range cmds {
		gws, err := cmd.Result()
		if err != nil {
			continue
		}
		for _, gw := range gws {
			out[gw] = append(out[gw], userIDs[i])
		}
	}
	return out, nil
}