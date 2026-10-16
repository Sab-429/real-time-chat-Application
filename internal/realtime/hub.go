package realtime

import (
	"errors"
	"sync"

	"github.com/gorilla/websocket"
)

var ErrHubClosed = errors.New("realtime: hub closed")

// Hub tracks the connections that live on THIS gateway process only.
// Cross-gateway routing is Redis's job (see publisher.go / subscriber.go).
//
// A single RWMutex is plenty into the tens of thousands of connections per
// gateway; shard by hash(userID) only if profiling shows contention.
type Hub struct {
	mu     sync.RWMutex
	users  map[string]map[*Client]struct{}
	total  int
	closed bool
}

func NewHub() *Hub {
	return &Hub{users: make(map[string]map[*Client]struct{})}
}

// Add registers a connection. first is true when this is the user's first
// connection on this gateway (the caller should then register presence).
func (h *Hub) Add(c *Client) (first bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, ErrHubClosed
	}
	set, ok := h.users[c.UserID]
	if !ok {
		set = make(map[*Client]struct{})
		h.users[c.UserID] = set
	}
	set[c] = struct{}{}
	h.total++
	return !ok, nil
}

// Remove unregisters a connection. last is true when the user has no more
// connections on this gateway (the caller should then clear presence).
func (h *Hub) Remove(c *Client) (last bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.users[c.UserID]
	if !ok {
		return false
	}
	if _, ok := set[c]; !ok {
		return false
	}
	delete(set, c)
	h.total--
	if len(set) == 0 {
		delete(h.users, c.UserID)
		return true
	}
	return false
}

// SendToUser fans a payload out to every local connection of the user
// (phone + laptop + ...). It never blocks: a client whose buffer is full is
// disconnected and will resync by sequence number when it reconnects.
func (h *Hub) SendToUser(userID string, payload []byte) (delivered int) {
	var slow []*Client
	h.mu.RLock()
	for c := range h.users[userID] {
		if c.Enqueue(payload) {
			delivered++
		} else {
			slow = append(slow, c)
		}
	}
	h.mu.RUnlock()

	// Close outside the lock: Close does network I/O.
	for _, c := range slow {
		c.Close(websocket.CloseTryAgainLater, "slow consumer")
	}
	return delivered
}

func (h *Hub) CountUser(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.users[userID])
}

func (h *Hub) HasUser(userID string) bool { return h.CountUser(userID) > 0 }

// Users returns a snapshot of locally connected user IDs (used by the presence heartbeat).
func (h *Hub) Users() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.users))
	for id := range h.users {
		out = append(out, id)
	}
	return out
}

func (h *Hub) Total() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.total
}

// CloseAll rejects new connections and asks every client to reconnect
// (use websocket.CloseServiceRestart during deploys).
func (h *Hub) CloseAll(code int, reason string) {
	h.mu.Lock()
	h.closed = true
	all := make([]*Client, 0, h.total)
	for _, set := range h.users {
		for c := range set {
			all = append(all, c)
		}
	}
	h.mu.Unlock()
	for _, c := range all {
		c.Close(code, reason)
	}
}