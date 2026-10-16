// Command smoke is an end-to-end check of a running stack (DEV ONLY):
// REST + WebSocket + Redis live delivery + idempotency + sync + read pointers
// (+ with -pipeline: outbox -> Kafka -> analytics).
//
//	go run ./cmd/smoke
//	go run ./cmd/smoke -pipeline
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"

	"github.com/sabya/real-time-chat/internal/config"
	"github.com/sabya/real-time-chat/internal/database"
)

type frame struct {
	Type string          `json:"type"`
	ID   string          `json:"id,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func main() {
	api := flag.String("api", "http://localhost:8080", "API base URL")
	ws := flag.String("ws", "ws://localhost:8081/ws", "gateway WebSocket URL")
	pipeline := flag.Bool("pipeline", false, "also verify outbox -> Kafka -> analytics (needs relay + worker-analytics running)")
	flag.Parse()

	cfg, err := config.Load()
	check("load config", err)
	if !cfg.IsDev() {
		check("dev-only", fmt.Errorf("refusing to run with ENV=%s", cfg.Env))
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	alice, bob, carol := "alice-"+run, "bob-"+run, "carol-"+run
	aTok, bTok, cTok := mint(cfg.JWTSecret, alice, "Alice"), mint(cfg.JWTSecret, bob, "Bob"), mint(cfg.JWTSecret, carol, "Carol")

	// ---- REST: create a direct conversation
	var conv struct {
		ID string `json:"id"`
	}
	status, err := call(http.MethodPost, *api+"/v1/conversations", aTok,
		map[string]any{"type": "direct", "member_ids": []string{bob}}, &conv)
	step("create direct conversation", err == nil && status == 201 && conv.ID != "", fmt.Sprintf("status=%d err=%v", status, err))

	// ---- WebSocket: auth is enforced
	_, resp, err := websocket.DefaultDialer.Dial(*ws+"?token=garbage", nil)
	step("gateway rejects a bad token", err != nil && resp != nil && resp.StatusCode == 401, fmt.Sprintf("err=%v", err))

	aWS, err := dial(*ws, aTok)
	check("alice connects", err)
	defer aWS.Close()
	bWS, err := dial(*ws, bTok)
	check("bob connects", err)
	defer bWS.Close()
	time.Sleep(300 * time.Millisecond) // let presence registration land in Redis

	// ---- send, live delivery, ack
	cid1 := "smoke-" + run + "-1"
	send := func(reqID, clientMsgID, body string) {
		data, _ := json.Marshal(map[string]string{"conversation_id": conv.ID, "client_msg_id": clientMsgID, "body": body})
		check("write frame", aWS.WriteJSON(frame{Type: "message.send", ID: reqID, Data: data}))
	}
	send("req-1", cid1, "hello from smoke")

	ackF, err := waitFrame(aWS, 5*time.Second, func(f frame) bool { return f.Type == "message.ack" && f.ID == "req-1" })
	check("alice receives ack", err)
	var ack struct {
		MessageID string `json:"message_id"`
		Seq       int64  `json:"seq"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.Unmarshal(ackF.Data, &ack)
	step("ack carries seq=1", ack.Seq == 1 && !ack.Duplicate, fmt.Sprintf("%+v", ack))

	newF, err := waitFrame(bWS, 5*time.Second, func(f frame) bool { return f.Type == "message.new" })
	check("bob receives the message live (Redis path)", err)
	var live struct {
		Body string `json:"body"`
		Seq  int64  `json:"seq"`
	}
	_ = json.Unmarshal(newF.Data, &live)
	step("live frame has the right body and seq", live.Body == "hello from smoke" && live.Seq == 1, fmt.Sprintf("%+v", live))

	// ---- idempotent retry
	send("req-2", cid1, "hello from smoke")
	dupF, err := waitFrame(aWS, 5*time.Second, func(f frame) bool { return f.Type == "message.ack" && f.ID == "req-2" })
	check("retry is acked", err)
	var dup struct {
		MessageID string `json:"message_id"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.Unmarshal(dupF.Data, &dup)
	step("retry returns the original message (idempotent)", dup.Duplicate && dup.MessageID == ack.MessageID, fmt.Sprintf("%+v", dup))

	// ---- ordering: second message gets seq 2
	send("req-3", "smoke-"+run+"-2", "second message")
	ack2F, err := waitFrame(aWS, 5*time.Second, func(f frame) bool { return f.Type == "message.ack" && f.ID == "req-3" })
	check("second message acked", err)
	var ack2 struct {
		Seq int64 `json:"seq"`
	}
	_ = json.Unmarshal(ack2F.Data, &ack2)
	step("second message has seq=2", ack2.Seq == 2, fmt.Sprintf("seq=%d", ack2.Seq))

	// ---- REST: history (newest first)
	var hist []struct {
		Seq int64 `json:"seq"`
	}
	status, err = call(http.MethodGet, *api+"/v1/conversations/"+conv.ID+"/messages", bTok, nil, &hist)
	step("history returns both messages, newest first", err == nil && status == 200 && len(hist) == 2 && hist[0].Seq == 2, fmt.Sprintf("status=%d len=%d err=%v", status, len(hist), err))

	// ---- REST: sync (what a reconnecting client calls)
	var syncResp struct {
		Results []struct {
			ConversationID string `json:"conversation_id"`
			Messages       []struct {
				Seq int64 `json:"seq"`
			} `json:"messages"`
		} `json:"results"`
	}
	status, err = call(http.MethodPost, *api+"/v1/sync", bTok,
		map[string]any{"conversations": []map[string]any{{"conversation_id": conv.ID, "last_seq": 0}}}, &syncResp)
	step("sync replays missed messages", err == nil && status == 200 && len(syncResp.Results) == 1 && len(syncResp.Results[0].Messages) == 2, fmt.Sprintf("status=%d err=%v", status, err))

	// ---- unread + mark read
	var list []struct {
		ID     string `json:"id"`
		Unread int64  `json:"unread"`
	}
	_, err = call(http.MethodGet, *api+"/v1/conversations", bTok, nil, &list)
	check("list conversations", err)
	step("bob has 2 unread", len(list) == 1 && list[0].Unread == 2, fmt.Sprintf("%+v", list))

	status, err = call(http.MethodPost, *api+"/v1/conversations/"+conv.ID+"/read", bTok, map[string]any{"seq": 2}, nil)
	step("mark read", err == nil && status == 204, fmt.Sprintf("status=%d err=%v", status, err))
	list = nil
	_, _ = call(http.MethodGet, *api+"/v1/conversations", bTok, nil, &list)
	step("unread is now 0", len(list) == 1 && list[0].Unread == 0, fmt.Sprintf("%+v", list))

	// ---- authorization
	status, _ = call(http.MethodGet, *api+"/v1/conversations/"+conv.ID+"/messages", cTok, nil, nil)
	step("a non-member cannot read the conversation", status == 403, fmt.Sprintf("status=%d", status))

	// ---- optional: async pipeline
	if *pipeline {
		pool, err := database.Connect(context.Background(), cfg.DatabaseURL)
		check("connect database", err)
		defer pool.Close()

		deadline := time.Now().Add(20 * time.Second)
		var pending, analytics int
		for time.Now().Before(deadline) {
			_ = pool.QueryRow(context.Background(),
				`SELECT count(*) FROM outbox_events WHERE key = $1 AND published_at IS NULL`, conv.ID).Scan(&pending)
			_ = pool.QueryRow(context.Background(),
				`SELECT count(*) FROM message_events WHERE conversation_id = $1`, conv.ID).Scan(&analytics)
			if pending == 0 && analytics == 2 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		step("outbox drained to Kafka by the relay", pending == 0, fmt.Sprintf("pending=%d", pending))
		step("analytics worker consumed both events", analytics == 2, fmt.Sprintf("rows=%d", analytics))
	}

	fmt.Println("\nall checks passed")
}

// ------------------------------------------------------------------ helpers

func mint(secret, user, name string) string {
	now := time.Now()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": user, "name": name, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}).SignedString([]byte(secret))
	check("mint token", err)
	return tok
}

func dial(wsURL, token string) (*websocket.Conn, error) {
	c, resp, err := websocket.DefaultDialer.Dial(wsURL+"?token="+url.QueryEscape(token), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("%w (HTTP %d)", err, resp.StatusCode)
		}
		return nil, err
	}
	return c, nil
}

// waitFrame reads (and discards) frames until one matches or the timeout expires.
func waitFrame(c *websocket.Conn, timeout time.Duration, match func(frame) bool) (frame, error) {
	deadline := time.Now().Add(timeout)
	for {
		_ = c.SetReadDeadline(deadline)
		_, data, err := c.ReadMessage()
		if err != nil {
			return frame{}, err
		}
		var f frame
		if json.Unmarshal(data, &f) == nil && match(f) {
			return f, nil
		}
	}
}

func call(method, url, token string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func step(name string, ok bool, detail string) {
	if ok {
		fmt.Println("  ✓", name)
		return
	}
	fmt.Printf("  ✗ %s  (%s)\n", name, detail)
	os.Exit(1)
}

func check(name string, err error) {
	if err != nil {
		fmt.Printf("  ✗ %s: %v\n", name, err)
		os.Exit(1)
	}
	fmt.Println("  ✓", name)
}
