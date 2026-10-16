package chat

import "context"

const (
	MaxSyncConversations = 50
	SyncPageSize         = 100
)

type SyncRequest struct {
	ConversationID string `json:"conversation_id"`
	LastSeq        int64  `json:"last_seq"` // highest seq the client already has
}

type SyncResult struct {
	ConversationID string    `json:"conversation_id"`
	Messages       []Message `json:"messages"` // ascending by seq
	HasMore        bool      `json:"has_more"` // call again with the new last_seq
}

// Sync replays what a client missed (reconnect, or it spotted a seq gap in live
// frames). This is what makes Redis Pub/Sub's lack of guarantees acceptable.
//
// Conversations with nothing new are omitted. Conversations the caller is not a
// member of are silently skipped so existence is not leaked.
func (s *Service) Sync(ctx context.Context, userID string, reqs []SyncRequest) ([]SyncResult, error) {
	if len(reqs) == 0 {
		return []SyncResult{}, nil
	}
	if len(reqs) > MaxSyncConversations {
		return nil, invalid("too many conversations in one sync request")
	}
	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		if r.ConversationID == "" || r.LastSeq < 0 {
			return nil, invalid("invalid sync request entry")
		}
		ids = append(ids, r.ConversationID)
	}

	member, err := s.store.MemberOf(ctx, userID, ids)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(member))
	for _, id := range member {
		allowed[id] = struct{}{}
	}

	results := make([]SyncResult, 0, len(reqs))
	for _, r := range reqs {
		if _, ok := allowed[r.ConversationID]; !ok {
			continue
		}
		msgs, err := s.store.MessagesAfter(ctx, r.ConversationID, r.LastSeq, SyncPageSize+1)
		if err != nil {
			return nil, err
		}
		hasMore := len(msgs) > SyncPageSize
		if hasMore {
			msgs = msgs[:SyncPageSize]
		}
		if len(msgs) == 0 {
			continue
		}
		results = append(results, SyncResult{ConversationID: r.ConversationID, Messages: msgs, HasMore: hasMore})
	}
	return results, nil
}