package realtime

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// Delivery is what travels over a gateway's Redis channel: the frame is
// marshalled once, and each gateway gets ONE message listing its local users.
type Delivery struct {
	Users []string        `json:"users"`
	Frame json.RawMessage `json:"frame"`
}

type GatewayLookup interface {
	Gateways(ctx context.Context, userIDs []string) (map[string][]string, error)
}

type PublishResult struct {
	OnlineUsers int
	Gateways    int
}

// Publisher is used by the chat service AFTER the DB transaction commits.
// Redis Pub/Sub is fire-and-forget by design: callers must treat an error here
// as non-fatal (log + metric). The DB is the truth and clients resync by seq.
type Publisher struct {
	rdb      redis.UniversalClient
	presence GatewayLookup
	log      *slog.Logger
}

func NewPublisher(rdb redis.UniversalClient, presence GatewayLookup) *Publisher {
	return &Publisher{rdb: rdb, presence: presence, log: slog.Default()}
}

func (p *Publisher) PublishToUsers(ctx context.Context, userIDs []string, f Frame) (PublishResult, error) {
	var res PublishResult
	userIDs = dedupe(userIDs)
	if len(userIDs) == 0 {
		return res, nil
	}

	byGateway, err := p.presence.Gateways(ctx, userIDs)
	if err != nil {
		return res, err
	}
	if len(byGateway) == 0 {
		return res, nil // everyone offline: the notification worker handles push
	}

	raw, err := json.Marshal(f)
	if err != nil {
		return res, err
	}

	online := make(map[string]struct{}, len(userIDs))
	pipe := p.rdb.Pipeline()
	for gw, users := range byGateway {
		b, err := json.Marshal(Delivery{Users: users, Frame: raw})
		if err != nil {
			return res, err
		}
		pipe.Publish(ctx, GatewayChannel(gw), b)
		for _, u := range users {
			online[u] = struct{}{}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return res, err
	}
	res.OnlineUsers, res.Gateways = len(online), len(byGateway)
	return res, nil
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok || s == "" {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}