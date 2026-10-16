package realtime

import (
	"fmt"
	"sync"
	"testing"
)

func newTestClient(user string, buf int) *Client {
	return &Client{
		UserID: user,
		ConnID: randomID(),
		send:   make(chan []byte, buf),
		done:   make(chan struct{}),
	}
}

func isClosed(c *Client) bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func TestAddRemoveFirstLast(t *testing.T) {
	h := NewHub()
	a, b := newTestClient("u1", 1), newTestClient("u1", 1)

	if first, err := h.Add(a); err != nil || !first {
		t.Fatalf("first add: first=%v err=%v", first, err)
	}
	if first, _ := h.Add(b); first {
		t.Fatal("second device must not be reported as first")
	}
	if h.Total() != 2 || h.CountUser("u1") != 2 {
		t.Fatalf("counts wrong: total=%d user=%d", h.Total(), h.CountUser("u1"))
	}
	if h.Remove(a) {
		t.Fatal("removing one of two devices must not be last")
	}
	if !h.Remove(b) {
		t.Fatal("removing final device must be last")
	}
	if h.Remove(b) {
		t.Fatal("double remove must be a no-op")
	}
	if h.Total() != 0 || h.HasUser("u1") {
		t.Fatal("hub should be empty")
	}
}

func TestSendToUserFansOutToAllDevices(t *testing.T) {
	h := NewHub()
	phone, laptop, other := newTestClient("u1", 4), newTestClient("u1", 4), newTestClient("u2", 4)
	for _, c := range []*Client{phone, laptop, other} {
		_, _ = h.Add(c)
	}

	if n := h.SendToUser("u1", []byte("hi")); n != 2 {
		t.Fatalf("delivered=%d, want 2", n)
	}
	for _, c := range []*Client{phone, laptop} {
		if got := string(<-c.send); got != "hi" {
			t.Fatalf("got %q", got)
		}
	}
	if len(other.send) != 0 {
		t.Fatal("other user must not receive the message")
	}
	if n := h.SendToUser("nobody", []byte("x")); n != 0 {
		t.Fatalf("unknown user delivered=%d", n)
	}
}

func TestSlowClientIsDroppedWithoutBlocking(t *testing.T) {
	h := NewHub()
	slow := newTestClient("u1", 1)
	_, _ = h.Add(slow)

	if n := h.SendToUser("u1", []byte("1")); n != 1 {
		t.Fatalf("first send delivered=%d", n)
	}
	if n := h.SendToUser("u1", []byte("2")); n != 0 { // buffer full
		t.Fatalf("second send delivered=%d, want 0", n)
	}
	if !isClosed(slow) {
		t.Fatal("slow consumer should have been closed")
	}
}

func TestCloseAllRejectsNewConnections(t *testing.T) {
	h := NewHub()
	c := newTestClient("u1", 1)
	_, _ = h.Add(c)

	h.CloseAll(1012, "restart")
	if !isClosed(c) {
		t.Fatal("existing client should be closed")
	}
	if _, err := h.Add(newTestClient("u2", 1)); err != ErrHubClosed {
		t.Fatalf("err=%v, want ErrHubClosed", err)
	}
}

func TestConcurrentUse(t *testing.T) { // run with: go test -race
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u := fmt.Sprintf("u%d", i%5)
			c := newTestClient(u, 8)
			_, _ = h.Add(c)
			for j := 0; j < 20; j++ {
				h.SendToUser(u, []byte("m"))
				select {
				case <-c.send:
				default:
				}
			}
			h.Remove(c)
		}(i)
	}
	wg.Wait()
	if h.Total() != 0 {
		t.Fatalf("total=%d after all removed", h.Total())
	}
}