package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Sender is satisfied by *Hub.
type Sender interface {
	SendToUser(userID string, payload []byte) int
}

// Subscriber holds ONE Redis subscription per gateway (gw:{gatewayID}),
// not one per user, so Redis load stays flat as connections grow.
type Subscriber struct {
	rdb       redis.UniversalClient
	gatewayID string
	sink      Sender
	log       *slog.Logger
	ps        *redis.PubSub
	done      chan struct{}
}

func NewSubscriber(rdb redis.UniversalClient, gatewayID string, sink Sender) *Subscriber {
	return &Subscriber{rdb: rdb, gatewayID: gatewayID, sink: sink, log: slog.Default(), done: make(chan struct{})}
}

// Start subscribes and waits for Redis to confirm BEFORE returning, so the
// gateway can start accepting sockets only once it can actually receive.
// go-redis reconnects and resubscribes automatically; messages published
// during an outage are lost, which clients repair via sync-by-seq.
func (s *Subscriber) Start(ctx context.Context) error {
	ps := s.rdb.Subscribe(ctx, GatewayChannel(s.gatewayID))
	if _, err := ps.Receive(ctx); err != nil { // subscription confirmation
		_ = ps.Close()
		return err
	}
	s.ps = ps
	ch := ps.Channel(
		redis.WithChannelSize(4096),
		redis.WithChannelHealthCheckInterval(10*time.Second),
	)
	go func() {
		defer close(s.done)
		for m := range ch {
			s.handle(m)
		}
	}()
	return nil
}

func (s *Subscriber) handle(m *redis.Message) {
	var d Delivery
	if err := json.Unmarshal([]byte(m.Payload), &d); err != nil {
		s.log.Error("bad delivery payload", "error", err)
		return
	}
	for _, uid := range d.Users {
		// Not-connected-anymore users are simply skipped (they resync later).
		s.sink.SendToUser(uid, d.Frame)
	}
}

func (s *Subscriber) Close() error {
	if s.ps == nil {
		return nil
	}
	err := s.ps.Close()
	<-s.done
	return err
}