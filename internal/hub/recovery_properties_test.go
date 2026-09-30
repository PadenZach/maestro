package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"pgregory.net/rapid"

	"github.com/zpaden/maestro/internal/protocol"
)

// These properties exercise recovery prerequisites that exist today. They do
// not stand in for a recovery coordinator or an SDK workflow-completion oracle.

// A registration is a socket generation, scoped to (application, executor ID).
// The oracle is an append-only history: only the latest accepted generation of
// each identity can remain visible, and removing it never revives an older one.
func TestRecoveryBoundaryRegistryGenerations(t *testing.T) {
	coverage := map[string]int{}
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertySockets(t)
		h := propertyHub()
		type generation struct {
			conn     *Conn
			app, id  string
			accepted bool
			removed  bool
		}
		var history []*generation
		apps := []string{"orders", "billing", "archive"}
		ids := []string{"same-executor", "second-executor", "replacement-process"}
		actions := rapid.SliceOfN(rapid.SampledFrom([]string{"connect", "connect", "disconnect", "closed_connect"}), 1, 40).Draw(t, "actions")
		for step, action := range actions {
			if action == "disconnect" && len(history) > 0 {
				g := history[rapid.IntRange(0, len(history)-1).Draw(t, "disconnected_generation")]
				if g.conn.ctx.Err() != nil {
					coverage["stale_disconnect"]++
				} else {
					coverage["current_disconnect"]++
				}
				g.conn.close(errConnClosed)
				h.deregister(g.conn)
				g.removed = true
			} else if action != "disconnect" {
				app := rapid.SampledFrom(apps).Draw(t, "app")
				id := rapid.SampledFrom(ids).Draw(t, "executor_id")
				server, _ := f.pair(t)
				c := newConn(h, server, app)
				t.Cleanup(c.cancel)
				c.executor = &Executor{ID: id, Version: fmt.Sprintf("generation-%d", len(history))}
				accepted := action != "closed_connect"
				coverage[action]++
				for _, prior := range history {
					if accepted && prior.app == app && prior.id == id && prior.conn.ctx.Err() == nil {
						coverage["replacement"]++
					}
				}
				if !accepted {
					c.cancel()
				}
				if got := h.register(c); got != accepted {
					t.Fatalf("step %d %s: registered=%v, want %v", step, action, got, accepted)
				}
				history = append(history, &generation{conn: c, app: app, id: id, accepted: accepted})
			}

			want := map[string]string{}
			for i, g := range history {
				latest := g.accepted && !g.removed
				for _, later := range history[i+1:] {
					if later.accepted && later.app == g.app && later.id == g.id {
						latest = false
					}
				}
				if latest {
					want[g.app+"/"+g.id] = g.conn.executor.Version
					if g.conn.ctx.Err() != nil {
						t.Fatalf("step %d: current generation %s was closed", step, g.conn.executor.Version)
					}
				} else if g.conn.ctx.Err() == nil {
					t.Fatalf("step %d: removed/rejected/superseded generation %s is still open", step, g.conn.executor.Version)
				}
			}
			got := map[string]string{}
			views := h.Executors()
			for _, view := range views {
				got[view.App+"/"+view.ExecutorID] = view.Version
			}
			if len(views) != len(want) || !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d %s: registered generations=%v (%d rows), want %v", step, action, got, len(views), want)
			}
			for _, app := range apps {
				n := 0
				for key := range want {
					if strings.HasPrefix(key, app+"/") {
						n++
					}
				}
				picked, ok := h.Pick(app)
				if ok != (n > 0) || (ok && want[app+"/"+picked.executor.ID] != picked.executor.Version) {
					t.Fatalf("step %d: Pick(%q) selected an unavailable or stale generation", step, app)
				}
			}
		}
	})
	t.Logf("generated registry transitions: %v", coverage)
}

func propertyHub() *Hub {
	h := New(slog.New(slog.NewTextHandler(io.Discard, nil)), 5*time.Second)
	// No wall-clock heartbeat can interfere with a generated trace.
	h.keepaliveTicks = func(context.Context) <-chan time.Time { return nil }
	return h
}

// This fixture owns real text-frame transport but leaves registration and
// disconnect notifications to the property so their order is deterministic.
// It does not bypass readLoop when a property starts connection workers.
type propertySockets struct {
	ctx      context.Context
	server   *httptest.Server
	accepted chan *websocket.Conn
}

func newPropertySockets(t *rapid.T) *propertySockets {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	f := &propertySockets{ctx: ctx, accepted: make(chan *websocket.Conn, 1)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			select {
			case f.accepted <- c:
			case <-ctx.Done():
				_ = c.CloseNow()
			}
		}
	}))
	t.Cleanup(f.server.Close)
	t.Cleanup(cancel)
	return f
}

func (f *propertySockets) pair(t *rapid.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	client, _, err := websocket.Dial(f.ctx, "ws"+strings.TrimPrefix(f.server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect property peer: %v", err)
	}
	t.Cleanup(func() { _ = client.CloseNow() })
	select {
	case server := <-f.accepted:
		t.Cleanup(func() { _ = server.CloseNow() })
		return server, client
	case <-f.ctx.Done():
		t.Fatalf("accept property peer: %v", f.ctx.Err())
		return nil, nil
	}
}

func propertyFrame(t *rapid.T, ctx context.Context, peer *websocket.Conn) protocol.Request {
	t.Helper()
	typ, data, err := peer.Read(ctx)
	if err != nil {
		t.Fatalf("read property frame: %v", err)
	}
	var frame protocol.Request
	if err := json.Unmarshal(data, &frame); err != nil || typ != websocket.MessageText {
		t.Fatalf("invalid property frame: type=%v, data=%s, error=%v", typ, data, err)
	}
	return frame
}

func propertyReply(t *rapid.T, ctx context.Context, peer *websocket.Conn, frame protocol.Request) []byte {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write property reply: %v", err)
	}
	return data
}

func propertyAwait(t *rapid.T, ctx context.Context, ch <-chan result) result {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-ctx.Done():
		t.Fatalf("property request did not finish: %v", ctx.Err())
		return result{}
	}
}
