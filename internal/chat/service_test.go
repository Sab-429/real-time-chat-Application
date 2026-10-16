package chat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------- fakes

type outboxRow struct {
	topic, key string
	payload    []byte
}

type fakeStore struct {
	members       map[string][]string
	seq           map[string]int64
	msgs          []Message
	outbox        []outboxRow
	hideFirstFind bool // simulates losing an idempotency race
	outboxErr     error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		members: map[string][]string{"c1": {"alice", "bob", "carol"}, "c2": {"bob"}},
		seq:     map[string]int64{},
	}
}

// InTx restores state when fn fails, emulating a rolled-back transaction.
func (s *fakeStore) InTx(_ context.Context, fn func(Tx) error) error {
	seq := map[string]int64{}
	for k, v := range s.seq {
		seq[k] = v
	}
	msgs, ob := slices.Clone(s.msgs), slices.Clone(s.outbox)
	if err := fn(&fakeTx{s}); err != nil {
		s.seq, s.msgs, s.outbox = seq, msgs, ob
		return err
	}
	return nil
}

type fakeTx struct{ s *fakeStore }

func (t *fakeTx) NextSeq(_ context.Context, conv string) (int64, error) {
	t.s.seq[conv]++
	return t.s.seq[conv], nil
}

func (t *fakeTx) InsertMessage(_ context.Context, m Message) (Message, error) {
	for _, e := range t.s.msgs {
		if e.SenderID == m.SenderID && e.ClientMsgID == m.ClientMsgID {
			return Message{}, ErrDuplicate
		}
	}
	m.CreatedAt = time.Unix(1_700_000_000, 0)
	t.s.msgs = append(t.s.msgs, m)
	return m, nil
}

func (t *fakeTx) InsertOutbox(_ context.Context, topic, key string, payload []byte) error {
	if t.s.outboxErr != nil {
		return t.s.outboxErr
	}
	t.s.outbox = append(t.s.outbox, outboxRow{topic, key, payload})
	return nil
}

func (s *fakeStore) Members(_ context.Context, c string) ([]string, error) { return s.members[c], nil }

func (s *fakeStore) IsMember(_ context.Context, c, u string) (bool, error) {
	return slices.Contains(s.members[c], u), nil
}

func (s *fakeStore) MemberOf(_ context.Context, u string, ids []string) ([]string, error) {
	var out []string
	for _, id := range ids {
		if slices.Contains(s.members[id], u) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *fakeStore) FindByClientMsgID(_ context.Context, sender, cid string) (*Message, error) {
	if s.hideFirstFind {
		s.hideFirstFind = false
		return nil, ErrNotFound
	}
	for _, m := range s.msgs {
		if m.SenderID == sender && m.ClientMsgID == cid {
			mm := m
			return &mm, nil
		}
	}
	return nil, ErrNotFound
}

func (s *fakeStore) MessagesAfter(_ context.Context, c string, after int64, limit int) ([]Message, error) {
	var out []Message
	for _, m := range s.msgs {
		if m.ConversationID == c && m.Seq > after {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) MessagesBefore(_ context.Context, c string, before int64, limit int) ([]Message, error) {
	var out []Message
	for _, m := range s.msgs {
		if m.ConversationID == c && m.Seq < before {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) ListConversations(context.Context, string, int) ([]ConversationSummary, error) {
	return []ConversationSummary{{Conversation: Conversation{LastSeq: 10}, LastReadSeq: 4}}, nil
}
func (s *fakeStore) MarkRead(context.Context, string, string, int64) error { return nil }
func (s *fakeStore) CreateConversation(_ context.Context, nc NewConversation) (*Conversation, error) {
	return &Conversation{ID: "new", Type: nc.Type, Title: nc.Title}, nil
}

type fakeRealtime struct {
	users [][]string
	msgs  []Message
	err   error
}

func (r *fakeRealtime) PublishMessage(_ context.Context, ids []string, m Message) error {
	r.users = append(r.users, ids)
	r.msgs = append(r.msgs, m)
	return r.err
}

func newSvc(s Store, rt Realtime) *Service {
	return NewService(s, rt, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func input(cid string) SendInput {
	return SendInput{ConversationID: "c1", SenderID: "alice", SenderName: "Alice", ClientMsgID: cid, Body: "hi"}
}

// ---------------------------------------------------------------- Send

func TestSendAssignsSequenceWritesOutboxAndPublishes(t *testing.T) {
	st, rt := newFakeStore(), &fakeRealtime{}
	svc := newSvc(st, rt)

	r1, err := svc.Send(context.Background(), input("client-0001"))
	if err != nil || r1.Duplicate || r1.Message.Seq != 1 {
		t.Fatalf("first send: %+v err=%v", r1, err)
	}
	r2, _ := svc.Send(context.Background(), input("client-0002"))
	if r2.Message.Seq != 2 {
		t.Fatalf("second seq = %d, want 2", r2.Message.Seq)
	}

	if len(st.outbox) != 2 || st.outbox[0].topic != TopicMessageCreated || st.outbox[0].key != "c1" {
		t.Fatalf("outbox = %+v", st.outbox)
	}
	var env EventEnvelope
	if err := json.Unmarshal(st.outbox[0].payload, &env); err != nil {
		t.Fatal(err)
	}
	var data MessageEventData
	_ = json.Unmarshal(env.Data, &data)
	if env.Type != EventTypeMessageCreated || env.EventID == "" ||
		!slices.Equal(env.RecipientIDs, []string{"alice", "bob", "carol"}) ||
		data.Seq != 1 || data.SenderName != "Alice" || data.Body != "hi" {
		t.Fatalf("bad event: %+v %+v", env, data)
	}

	if len(rt.msgs) != 2 || !slices.Equal(rt.users[0], []string{"alice", "bob", "carol"}) {
		t.Fatalf("live publish = %+v", rt.users)
	}
}

func TestSendRejectsNonMemberAndWritesNothing(t *testing.T) {
	st := newFakeStore()
	in := input("client-0001")
	in.SenderID = "mallory"
	_, err := newSvc(st, &fakeRealtime{}).Send(context.Background(), in)
	if !errors.Is(err, ErrNotMember) || len(st.msgs) != 0 || len(st.outbox) != 0 {
		t.Fatalf("err=%v msgs=%d outbox=%d", err, len(st.msgs), len(st.outbox))
	}
}

func TestRetryIsIdempotent(t *testing.T) {
	st, rt := newFakeStore(), &fakeRealtime{}
	svc := newSvc(st, rt)
	first, _ := svc.Send(context.Background(), input("client-0001"))
	again, err := svc.Send(context.Background(), input("client-0001"))
	if err != nil || !again.Duplicate || again.Message.ID != first.Message.ID {
		t.Fatalf("retry: %+v err=%v", again, err)
	}
	if st.seq["c1"] != 1 || len(st.outbox) != 1 || len(rt.msgs) != 1 {
		t.Fatal("a retry must not consume a seq, write another event, or publish again")
	}
}

func TestConcurrentRetryRaceReturnsWinner(t *testing.T) {
	st := newFakeStore()
	st.msgs = []Message{{ID: "winner", ConversationID: "c1", Seq: 1, SenderID: "alice", ClientMsgID: "client-0001"}}
	st.seq["c1"] = 1
	st.hideFirstFind = true // our pre-check misses it; the insert then loses the race

	res, err := newSvc(st, nil).Send(context.Background(), input("client-0001"))
	if err != nil || !res.Duplicate || res.Message.ID != "winner" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if st.seq["c1"] != 1 {
		t.Fatalf("seq must roll back, got %d", st.seq["c1"])
	}
}

func TestOutboxFailureRollsBackTheMessage(t *testing.T) {
	st := newFakeStore()
	st.outboxErr = errors.New("db exploded")
	rt := &fakeRealtime{}
	if _, err := newSvc(st, rt).Send(context.Background(), input("client-0001")); err == nil {
		t.Fatal("expected error")
	}
	if len(st.msgs) != 0 || st.seq["c1"] != 0 || len(rt.msgs) != 0 {
		t.Fatal("message, seq and live publish must all be absent after rollback")
	}
}

func TestRealtimeFailureDoesNotFailSend(t *testing.T) {
	st := newFakeStore()
	res, err := newSvc(st, &fakeRealtime{err: errors.New("redis down")}).Send(context.Background(), input("client-0001"))
	if err != nil || res.Message.Seq != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestSendValidation(t *testing.T) {
	svc := newSvc(newFakeStore(), nil)
	cases := map[string]func(*SendInput){
		"empty body":      func(i *SendInput) { i.Body = "   " },
		"body too long":   func(i *SendInput) { i.Body = strings.Repeat("a", MaxBodyBytes+1) },
		"short client id": func(i *SendInput) { i.ClientMsgID = "x" },
		"nul byte":        func(i *SendInput) { i.Body = "a\x00b" },
		"no conversation": func(i *SendInput) { i.ConversationID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := input("client-0001")
			mutate(&in)
			if _, err := svc.Send(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("err=%v, want ErrInvalidInput", err)
			}
		})
	}
}

// ---------------------------------------------------------------- reads

func seeded(n int) *fakeStore {
	st := newFakeStore()
	for i := 1; i <= n; i++ {
		st.msgs = append(st.msgs, Message{ID: "m", ConversationID: "c1", Seq: int64(i), SenderID: "alice"})
	}
	return st
}

func TestHistoryPagesNewestFirst(t *testing.T) {
	svc := newSvc(seeded(5), nil)
	got, err := svc.History(context.Background(), "alice", "c1", 4, 2)
	if err != nil || len(got) != 2 || got[0].Seq != 3 || got[1].Seq != 2 {
		t.Fatalf("got %+v err=%v", got, err)
	}
	if _, err := svc.History(context.Background(), "mallory", "c1", 0, 10); !errors.Is(err, ErrNotMember) {
		t.Fatalf("err=%v", err)
	}
}

func TestSyncReplaysMissedAndSkipsForeignConversations(t *testing.T) {
	svc := newSvc(seeded(3), nil)
	res, err := svc.Sync(context.Background(), "alice", []SyncRequest{
		{ConversationID: "c1", LastSeq: 1},
		{ConversationID: "c2", LastSeq: 0}, // alice is not in c2
	})
	if err != nil || len(res) != 1 || res[0].ConversationID != "c1" ||
		len(res[0].Messages) != 2 || res[0].Messages[0].Seq != 2 || res[0].HasMore {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestSyncPaginatesAndOmitsUpToDate(t *testing.T) {
	svc := newSvc(seeded(150), nil)
	res, _ := svc.Sync(context.Background(), "alice", []SyncRequest{{ConversationID: "c1", LastSeq: 0}})
	if len(res) != 1 || len(res[0].Messages) != SyncPageSize || !res[0].HasMore {
		t.Fatalf("expected a full page with HasMore, got %+v", res)
	}
	res, _ = svc.Sync(context.Background(), "alice", []SyncRequest{{ConversationID: "c1", LastSeq: 150}})
	if len(res) != 0 {
		t.Fatalf("up-to-date conversation must be omitted, got %+v", res)
	}
}

func TestSyncRejectsTooManyConversations(t *testing.T) {
	reqs := make([]SyncRequest, MaxSyncConversations+1)
	for i := range reqs {
		reqs[i] = SyncRequest{ConversationID: "c1"}
	}
	if _, err := newSvc(newFakeStore(), nil).Sync(context.Background(), "alice", reqs); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err=%v", err)
	}
}

func TestListConversationsComputesUnread(t *testing.T) {
	out, _ := newSvc(newFakeStore(), nil).ListConversations(context.Background(), "alice", 10)
	if len(out) != 1 || out[0].Unread != 6 {
		t.Fatalf("out=%+v", out)
	}
}

// ---------------------------------------------------------------- conversations

func TestDirectKeyIsOrderIndependent(t *testing.T) {
	if directKey([]string{"bob", "alice"}) != directKey([]string{"alice", "bob"}) {
		t.Fatal("direct key must not depend on order")
	}
}

func TestCreateConversationValidation(t *testing.T) {
	svc := newSvc(newFakeStore(), nil)
	bad := []CreateConversationInput{
		{CreatorID: "alice", Type: TypeDirect, MemberIDs: []string{"alice"}},              // chat with yourself
		{CreatorID: "alice", Type: TypeDirect, MemberIDs: []string{"bob", "carol"}},       // direct needs exactly 1 other
		{CreatorID: "alice", Type: TypeGroup, Title: "", MemberIDs: []string{"bob"}},      // group needs a title
		{CreatorID: "alice", Type: TypeGroup, Title: "x", MemberIDs: nil},                 // group needs members
		{CreatorID: "alice", Type: "channel", MemberIDs: []string{"bob"}},                 // unknown type
	}
	for i, in := range bad {
		if _, err := svc.CreateConversation(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("case %d: err=%v", i, err)
		}
	}
	if _, err := svc.CreateConversation(context.Background(),
		CreateConversationInput{CreatorID: "alice", Type: TypeDirect, MemberIDs: []string{"bob"}}); err != nil {
		t.Fatal(err)
	}
}