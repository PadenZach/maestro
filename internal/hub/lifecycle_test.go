package hub

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/protocol"
)

// During shutdown, Shutdown owns both pre-registration and registered sockets.
func TestHubShutdownClosesConnections(t *testing.T) {
	h, ts := testHub(t, 5*time.Second)
	active := peer(t, ts, "active")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	waiting := request(h, ctx, protocol.NewRequest(protocol.MsgCancel))
	_, _ = frame(t, ctx, active)
	handshaking, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer handshaking.CloseNow()
	_, _ = frame(t, ctx, handshaking) // do not answer executor_info
	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown did not join sessions: %v", err)
	}
	if got := await(t, waiting); !errors.Is(got.err, errConnClosed) {
		t.Fatalf("pending mutation: %v", got.err)
	}
	for name, c := range map[string]*websocket.Conn{"active": active, "handshaking": handshaking} {
		if _, _, err := c.Read(ctx); err == nil {
			t.Errorf("%s survived shutdown", name)
		}
	}
	if len(h.Executors()) != 0 {
		t.Fatal("registry retained closed peer")
	}
	if _, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil); err == nil {
		t.Fatal("socket accepted after shutdown")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("shutdown status %d", resp.StatusCode)
	}
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request(ctx, "app", protocol.NewRequest(protocol.MsgCancel)); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("request after shutdown: %v", err)
	}
}

func TestHubShutdownConcurrentAccept(t *testing.T) {
	h, ts := testHub(t, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Go(func() {
			c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
			if err == nil {
				defer c.CloseNow()
			}
		})
	}
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.Executors()) != 0 {
		t.Fatal("late registration")
	}
}

// Large-frame characterization: opaque request and response frames exceed coder/websocket's
// default 32 KiB read limit, but are never stored by the hub after the RPC.
func TestLargeResponse(t *testing.T) {
	h, ts := testHub(t, 3*time.Second)
	c := peer(t, ts, "large")
	c.SetReadLimit(-1) // SDK uses max_size=None; the default Go test client limit is 32 KiB.
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	blob := strings.Repeat("z", 1<<20)
	r := request(h, ctx, protocol.Request{"type": "get_workflow", "opaque": blob})
	_, outbound := frame(t, ctx, c)
	if outbound["opaque"] != blob {
		t.Fatal("large outbound payload changed")
	}
	data, _ := json.Marshal(map[string]any{"type": "get_workflow", "request_id": outbound["request_id"], "output": blob})
	if err := c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
	got := await(t, r)
	if got.err != nil {
		t.Fatal(got.err)
	}
	var inbound map[string]any
	if err := json.Unmarshal(got.data, &inbound); err != nil || inbound["output"] != blob {
		t.Fatalf("large inbound payload changed: %v", err)
	}
}

// Cancellation while the bounded send queue is full must release its pending
// waiter. Shutdown must unblock the writer and the older in-flight request.
func TestSendQueueCancelAndShutdown(t *testing.T) {
	h, ts := testHub(t, 5*time.Second)
	_ = peer(t, ts, "blocked")
	registered(t, h, 1)
	c := h.conns("app")[0]
	started := make(chan struct{})
	c.mu.Lock()
	c.writeFrame = func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := request(h, ctx, protocol.NewRequest(protocol.MsgCancel))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("writer did not block")
	}
	for i := 0; i < cap(c.send); i++ {
		c.send <- []byte("queued")
	}
	callCtx, stop := context.WithCancel(ctx)
	second := request(h, callCtx, protocol.NewRequest(protocol.MsgCancel))
	waitPending(t, c, 2)
	stop()
	if got := await(t, second); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("queued cancellation: %v", got.err)
	}
	waitPending(t, c, 1)
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := await(t, first); !errors.Is(got.err, errConnClosed) {
		t.Fatalf("in-flight shutdown: %v", got.err)
	}
	waitPending(t, c, 0)
}

func TestMissedPongDeregisters(t *testing.T) {
	h, ts := testHub(t, time.Second)
	ticks := make(chan time.Time, 1)
	h.keepaliveTicks = func(context.Context) <-chan time.Time { return ticks }
	h.ping = func(context.Context, *websocket.Conn) error { return context.DeadlineExceeded }
	c := peer(t, ts, "lost")
	registered(t, h, 1)
	ticks <- time.Now()
	registered(t, h, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("missed pong kept socket open")
	}
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// The clock tick is injected, while Ping/Pong uses real WebSocket control frames.
func TestWebSocketPongAndMissedPong(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		name := "missed"
		if healthy {
			name = "healthy"
		}
		t.Run(name, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			ticks := make(chan time.Time)
			pong := make(chan error, 1)
			h.keepaliveTicks = func(context.Context) <-chan time.Time { return ticks }
			h.ping = func(ctx context.Context, ws *websocket.Conn) error {
				probe, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				defer cancel()
				err := ws.Ping(probe)
				pong <- err
				return err
			}
			c := peer(t, ts, "pong")
			registered(t, h, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if healthy {
				c.CloseRead(ctx)
			} // reader replies to server Ping frames
			ticks <- time.Now()
			select {
			case err := <-pong:
				if healthy && err != nil {
					t.Fatalf("healthy peer missed pong: %v", err)
				}
				if !healthy && err == nil {
					t.Fatal("unread peer answered ping")
				}
			case <-ctx.Done():
				t.Fatal("ping did not complete")
			}
			if healthy {
				registered(t, h, 1)
			} else {
				registered(t, h, 0)
			}
			if err := h.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitReplacement(t *testing.T, h *Hub, old *Conn) *Conn {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current := h.conns("app")
		if len(current) == 1 && current[0] != old {
			return current[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("replacement not the sole live registration: %v", h.conns("app"))
	return nil
}

func TestHealthyPongAndReconnectChurn(t *testing.T) {
	h, ts := testHub(t, time.Second)
	ticks := make(chan time.Time)
	pinged := make(chan struct{}, 8)
	h.keepaliveTicks = func(context.Context) <-chan time.Time { return ticks }
	h.ping = func(context.Context, *websocket.Conn) error { pinged <- struct{}{}; return nil }
	first := peer(t, ts, "same")
	registered(t, h, 1)
	old := h.conns("app")[0]
	ticks <- time.Now()
	select {
	case <-pinged:
	case <-time.After(time.Second):
		t.Fatal("keepalive not run")
	}
	registered(t, h, 1)
	replacement := peer(t, ts, "same")
	old = waitReplacement(t, h, old) // new registration must atomically replace old
	if err := first.CloseNow(); err != nil {
		t.Fatal(err)
	}
	registered(t, h, 1) // stale disconnect must not evict replacement
	for i := 0; i < 5; i++ {
		next := peer(t, ts, "same")
		old = waitReplacement(t, h, old)
		_ = replacement.CloseNow()
		registered(t, h, 1)
		replacement = next
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
