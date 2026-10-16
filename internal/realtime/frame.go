package realtime

import "encoding/json"

// Wire protocol: every WebSocket text message is one JSON Frame.
const (
	TypeMessageSend  = "message.send"  // client -> server
	TypeMessageNew   = "message.new"   // server -> client (live delivery)
	TypeMessageAck   = "message.ack"   // server -> client (carries seq for the sender)
	TypeSyncRequest  = "sync.request"  // client -> server (lastSeq per conversation)
	TypeSyncResponse = "sync.response" // server -> client
	TypeTyping       = "typing"        // ephemeral, never persisted
	TypeError        = "error"
)

type Frame struct {
	Type string          `json:"type"`
	ID   string          `json:"id,omitempty"` // client request id, echoed back in acks/errors
	Data json.RawMessage `json:"data,omitempty"`
}

func NewFrame(typ, id string, data any) (Frame, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: typ, ID: id, Data: raw}, nil
}

// FrameError is a client-safe error a FrameHandler may return.
// Any other error is logged and reported to the client as "internal_error".
type FrameError struct {
	Code    string
	Message string
}

func (e *FrameError) Error() string { return e.Code + ": " + e.Message }

func ErrorFrame(id, code, msg string) []byte {
	b, _ := json.Marshal(map[string]any{
		"type": TypeError,
		"id":   id,
		"data": map[string]string{"code": code, "message": msg},
	})
	return b
}