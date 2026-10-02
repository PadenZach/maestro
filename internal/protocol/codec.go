package protocol

import (
	"encoding/json"
	"fmt"
)

// DecodeEnvelope extracts the type + request_id from any inbound frame so the
// read loop can route it to the correct waiter without knowing the concrete
// message type.
func DecodeEnvelope(data []byte) (BaseMessage, error) {
	var env BaseMessage
	err := json.Unmarshal(data, &env)
	return env, err
}

// Request is a frame conductor sends to an executor. It is an open map so that
// the same primitive carries everything from a bare EXECUTOR_INFO request to a
// richly-parameterised LIST_WORKFLOWS body. The "type" field is required; the
// connection layer fills in "request_id". Typed builders use this primitive.
type Request map[string]any

// NewRequest builds a request frame of the given type.
func NewRequest(t MessageType) Request {
	return Request{"type": string(t)}
}

// Validate rejects malformed discriminators and unsupported body representations.
func (r Request) Validate() error {
	var typ string
	switch value := r["type"].(type) {
	case string:
		typ = value
	case MessageType:
		typ = string(value)
	}
	if typ == "" {
		return fmt.Errorf("invalid request type")
	}
	switch MessageType(typ) {
	case MsgListWorkflows, MsgListQueuedWorkflows, MsgListQueues:
		if _, ok := r["body"].(map[string]any); !ok && r["body"] != nil {
			return fmt.Errorf("unsupported %s body representation", typ)
		}
	}
	return nil
}
