package protocol

import (
	"encoding/json"

	"github.com/google/uuid"
)

// NewRequestID returns a fresh correlation id for a server-initiated request.
func NewRequestID() string {
	return uuid.NewString()
}

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
// connection layer fills in "request_id". Typed builders land in M2 on top of
// this primitive.
type Request map[string]any

// NewRequest builds a request frame of the given type.
func NewRequest(t MessageType) Request {
	return Request{"type": string(t)}
}
