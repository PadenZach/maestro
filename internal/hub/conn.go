package hub

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

type response struct {
	data []byte
	err  error
}

type pendingRequest struct {
	typ protocol.MessageType
	ch  chan response
}

// Conn is one executor WebSocket connection. It owns three goroutines (read,
// write, keepalive) and multiplexes many in-flight server→executor requests
// over the single socket, correlating responses by request_id.
//
// Concurrency model:
//   - All data writes go through writeLoop for ordered queue/backpressure ownership;
//     coder/websocket itself permits concurrent writes.
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

	mu         sync.Mutex
	writeFrame func(context.Context, []byte) error // test seam; nil uses WebSocket writer
	pending    map[string]pendingRequest
	executor   *Executor

	closeOnce   sync.Once
	closeErr    error
	workersDone chan struct{}
	workers     sync.WaitGroup
}

func newConn(h *Hub, ws *websocket.Conn, app string) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		hub:         h,
		ws:          ws,
		app:         app,
		ctx:         ctx,
		cancel:      cancel,
		send:        make(chan []byte, sendBuffer),
		pending:     make(map[string]pendingRequest),
		workersDone: make(chan struct{}),
	}
}

func (c *Conn) start() {
	c.workers.Add(3)
	go func() { defer c.workers.Done(); c.writeLoop() }()
	go func() { defer c.workers.Done(); c.readLoop() }()
	go func() { defer c.workers.Done(); c.keepalive() }()
	go func() { c.workers.Wait(); close(c.workersDone) }()
}

// roundtrip sends a request frame (filling in request_id) and blocks for the
// correlated response frame, honoring both the caller's ctx and connection
// teardown. The raw response bytes are returned so callers decode into their
// own response type.
func (c *Conn) roundtrip(ctx context.Context, req protocol.Request) ([]byte, error) {
	reqID := rand.Text()
	// The caller may reuse the same map concurrently; only the frame copy is ours.
	frame := make(protocol.Request, len(req)+1)
	maps.Copy(frame, req)
	frame["request_id"] = reqID
	typ, _ := frame["type"].(string)
	data, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}

	ch := make(chan response, 1)
	c.mu.Lock()
	c.pending[reqID] = pendingRequest{typ: protocol.MessageType(typ), ch: ch}
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
		return resp.data, resp.err
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
	if info.ErrorMessage != nil && *info.ErrorMessage != "" {
		return errors.New("executor_info rejected by executor")
	}
	if info.Type != protocol.MsgExecutorInfo || info.ExecutorID == "" {
		return errors.New("invalid executor_info response")
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
		frameType, data, err := c.ws.Read(c.ctx)
		if err != nil {
			c.close(err)
			return
		}
		if frameType != websocket.MessageText {
			c.close(errors.New("executor sent non-text frame"))
			return
		}
		env, err := protocol.DecodeEnvelope(data)
		if err != nil {
			// An undecodable frame cannot be correlated. Fail the socket so
			// its outstanding callers do not wait for unrelated timeouts.
			c.close(fmt.Errorf("invalid executor response: %w", err))
			return
		}
		if env.Type == "" || env.RequestID == "" {
			c.close(errors.New("invalid executor response envelope"))
			return
		}
		c.mu.Lock()
		pending, ok := c.pending[env.RequestID]
		if ok {
			delete(c.pending, env.RequestID) // duplicates cannot reach this waiter
		}
		c.mu.Unlock()
		if !ok {
			// Executors only ever respond to our requests, so a frame with no
			// waiter means a late response (caller already timed out) or a
			// protocol violation. Either way, drop it.
			c.hub.log.Warn("response with no waiter", "app", c.app, "type", env.Type, "request_id", env.RequestID)
			continue
		}
		if env.Type != pending.typ {
			pending.ch <- response{err: fmt.Errorf("executor response type %q, want %q", env.Type, pending.typ)}
		} else {
			pending.ch <- response{data: data}
		}
	}
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.send:
			c.mu.Lock()
			write := c.writeFrame
			c.mu.Unlock()
			var err error
			if write == nil {
				err = c.ws.Write(c.ctx, websocket.MessageText, msg)
			} else {
				err = write(c.ctx, msg)
			}
			if err != nil {
				c.close(err)
				return
			}
		}
	}
}

func (c *Conn) keepalive() {
	var ticks <-chan time.Time
	if c.hub.keepaliveTicks != nil {
		ticks = c.hub.keepaliveTicks(c.ctx)
	} else {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		ticks = t.C
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticks:
			pctx, cancel := context.WithTimeout(c.ctx, pingTimeout)
			err := c.hub.ping(pctx, c.ws)
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
		_ = c.ws.CloseNow() // Close waits for the peer's close handshake; an unresponsive peer must not delay shutdown.
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
