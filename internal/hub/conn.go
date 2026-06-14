package hub

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/protocol"
)

const (
	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second
	pingTimeout      = 15 * time.Second
	sendBuffer       = 32
)

var errConnClosed = errors.New("conductor: executor connection closed")

// Conn is one executor WebSocket connection. It owns three goroutines (read,
// write, keepalive) and multiplexes many in-flight server→executor requests
// over the single socket, correlating responses by request_id.
//
// Concurrency model:
//   - All writes go through writeLoop (coder/websocket requires a single writer).
//   - readLoop is the single reader; every inbound frame is a response that it
//     routes to a waiting roundtrip via the pending map.
//   - keepalive pings independently (control frames are writer-safe in the lib).
type Conn struct {
	hub *Hub
	ws  *websocket.Conn
	app string

	ctx    context.Context
	cancel context.CancelFunc

	send chan []byte

	mu       sync.Mutex
	pending  map[string]chan []byte
	executor *Executor

	closeOnce sync.Once
	closeErr  error
}

func newConn(h *Hub, ws *websocket.Conn, app string) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		hub:     h,
		ws:      ws,
		app:     app,
		ctx:     ctx,
		cancel:  cancel,
		send:    make(chan []byte, sendBuffer),
		pending: make(map[string]chan []byte),
	}
}

func (c *Conn) start() {
	go c.writeLoop()
	go c.readLoop()
	go c.keepalive()
}

// roundtrip sends a request frame (filling in request_id) and blocks for the
// correlated response frame, honoring both the caller's ctx and connection
// teardown. The raw response bytes are returned so callers decode into their
// own response type.
func (c *Conn) roundtrip(ctx context.Context, req protocol.Request) ([]byte, error) {
	reqID := protocol.NewRequestID()
	req["request_id"] = reqID
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 1)
	c.mu.Lock()
	c.pending[reqID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, reqID)
		c.mu.Unlock()
	}()

	select {
	case c.send <- data:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// handshake performs the EXECUTOR_INFO exchange and records the executor's
// identity. Must succeed before the connection is registered with the hub.
func (c *Conn) handshake(ctx context.Context) error {
	resp, err := c.roundtrip(ctx, protocol.NewRequest(protocol.MsgExecutorInfo))
	if err != nil {
		return err
	}
	var info protocol.ExecutorInfoResponse
	if err := json.Unmarshal(resp, &info); err != nil {
		return err
	}
	c.mu.Lock()
	c.executor = &Executor{
		ID:          info.ExecutorID,
		Version:     info.ApplicationVersion,
		Hostname:    deref(info.Hostname),
		Language:    deref(info.Language),
		DBOSVersion: deref(info.DBOSVersion),
		Metadata:    info.ExecutorMetadata,
		ConnectedAt: time.Now().UTC(),
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) readLoop() {
	for {
		_, data, err := c.ws.Read(c.ctx)
		if err != nil {
			c.close(err)
			return
		}
		env, err := protocol.DecodeEnvelope(data)
		if err != nil {
			c.hub.log.Warn("dropping undecodable frame", "app", c.app, "err", err)
			continue
		}
		c.mu.Lock()
		ch := c.pending[env.RequestID]
		c.mu.Unlock()
		if ch == nil {
			// Executors only ever respond to our requests, so a frame with no
			// waiter means a late response (caller already timed out) or a
			// protocol violation. Either way, drop it.
			c.hub.log.Warn("response with no waiter", "app", c.app, "type", env.Type, "request_id", env.RequestID)
			continue
		}
		select {
		case ch <- data:
		default:
		}
	}
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.send:
			if err := c.ws.Write(c.ctx, websocket.MessageText, msg); err != nil {
				c.close(err)
				return
			}
		}
	}
}

func (c *Conn) keepalive() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(c.ctx, pingTimeout)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				c.close(err)
				return
			}
		}
	}
}

func (c *Conn) close(cause error) {
	c.closeOnce.Do(func() {
		c.closeErr = cause
		c.cancel()
		_ = c.ws.Close(websocket.StatusNormalClosure, "")
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
