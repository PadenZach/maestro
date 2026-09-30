package hub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"pgregory.net/rapid"

	"github.com/zpaden/maestro/internal/protocol"
)

// Responses, late/duplicate frames and caller cancellation are interleaved
// across multiple requests on the actual WebSocket read/write loops. A matching
// recovery acknowledgement is only a transport response, never completion.
func TestRecoveryBoundaryResponseOwnership(t *testing.T) {
	coverage := map[string]int{}
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertySockets(t)
		server, peer := f.pair(t)
		c := newConn(propertyHub(), server, "orders")
		c.start()
		var callers sync.WaitGroup
		t.Cleanup(func() {
			c.close(errConnClosed)
			callers.Wait()
			<-c.workersDone
		})

		type call struct {
			wire   protocol.Request
			done   <-chan result
			cancel context.CancelFunc
			active bool
		}
		var calls []*call
		seenIDs := map[string]bool{}
		start := func(typ string) *call {
			ctx, cancel := context.WithCancel(f.ctx)
			t.Cleanup(cancel)
			req := protocol.Request{"type": typ, "workflow_id": fmt.Sprintf("workflow-%d", len(seenIDs))}
			before := protocol.Request{"type": typ, "workflow_id": req["workflow_id"]}
			done := make(chan result, 1)
			callers.Go(func() {
				data, err := c.roundtrip(ctx, req)
				done <- result{data: data, err: err}
			})
			wire := propertyFrame(t, f.ctx, peer)
			id, ok := wire["request_id"].(string)
			if !ok || id == "" || seenIDs[id] || wire["type"] != typ || wire["workflow_id"] != req["workflow_id"] {
				t.Fatalf("request lost its independent identity or fields: %v", wire)
			}
			seenIDs[id] = true
			if !reflect.DeepEqual(req, before) {
				t.Fatalf("caller request map changed: got %v, want %v", req, before)
			}
			return &call{wire: wire, done: done, cancel: cancel, active: true}
		}
		reply := func(p *call, typ, output string) []byte {
			return propertyReply(t, f.ctx, peer, protocol.Request{
				"type": typ, "request_id": p.wire["request_id"], "output": output,
			})
		}
		complete := func(p *call) {
			want := reply(p, p.wire["type"].(string), "completed:"+p.wire["workflow_id"].(string))
			got := propertyAwait(t, f.ctx, p.done)
			if got.err != nil || !bytes.Equal(got.data, want) {
				t.Fatalf("response delivered to the wrong request for %s: error=%v", p.wire["workflow_id"], got.err)
			}
			p.active = false
			p.cancel()
		}
		// A completed roundtrip is a reader barrier: all earlier frames on this
		// socket were processed before its response, with no timing assumption.
		barrier := func() { complete(start("get_metrics")) }
		activeCalls := func() []*call {
			var active []*call
			for _, p := range calls {
				if p.active {
					active = append(active, p)
				}
			}
			return active
		}

		types := []string{"get_workflow", "exist_pending_workflows", "recovery", "cancel"}
		// At least two waiters make wrong-ID delivery observable even when the
		// generated trace shrinks to a single action.
		calls = append(calls, start(rapid.SampledFrom(types).Draw(t, "first_type")))
		calls = append(calls, start(rapid.SampledFrom(types).Draw(t, "second_type")))
		actions := rapid.SliceOfN(rapid.SampledFrom([]string{
			"send", "reply", "wrong_type", "cancel", "unknown", "late", "disconnect", "malformed",
		}), 1, 40).Draw(t, "actions")
		connected := true
		for step, action := range actions {
			active := activeCalls()
			switch action {
			case "send":
				calls = append(calls, start(rapid.SampledFrom(types).Draw(t, "request_type")))
			case "reply", "wrong_type", "cancel":
				if len(active) == 0 {
					continue
				}
				p := active[rapid.IntRange(0, len(active)-1).Draw(t, "waiting_request")]
				switch action {
				case "reply":
					complete(p)
				case "wrong_type":
					reply(p, "unexpected_response", "must not succeed")
					got := propertyAwait(t, f.ctx, p.done)
					if got.err == nil || !strings.Contains(got.err.Error(), "response type") || len(got.data) != 0 {
						t.Fatalf("wrong response type completed request: error=%v", got.err)
					}
					p.active = false
					p.cancel()
				case "cancel":
					p.cancel()
					got := propertyAwait(t, f.ctx, p.done)
					if !errors.Is(got.err, context.Canceled) || len(got.data) != 0 {
						t.Fatalf("canceled request completed incorrectly: error=%v", got.err)
					}
					p.active = false
				}
			case "unknown":
				propertyReply(t, f.ctx, peer, protocol.Request{
					"type": "recovery", "request_id": fmt.Sprintf("never-issued-%d", step), "success": true,
				})
				barrier()
			case "late":
				var finished []*call
				for _, p := range calls {
					if !p.active {
						finished = append(finished, p)
					}
				}
				if len(finished) == 0 {
					continue
				}
				p := finished[rapid.IntRange(0, len(finished)-1).Draw(t, "finished_request")]
				reply(p, p.wire["type"].(string), "late-or-duplicate")
				barrier()
			case "disconnect", "malformed":
				if action == "disconnect" {
					_ = peer.CloseNow()
				} else if err := peer.Write(f.ctx, websocket.MessageText, []byte(`{"type":`)); err != nil {
					t.Fatalf("send malformed frame: %v", err)
				}
				for _, p := range active {
					got := propertyAwait(t, f.ctx, p.done)
					if !errors.Is(got.err, errConnClosed) || len(got.data) != 0 {
						t.Fatalf("%s did not release pending request: error=%v", action, got.err)
					}
					p.active = false
				}
				connected = false
			}
			coverage[action]++
			if got, want := pendingCount(c), len(activeCalls()); got != want {
				t.Fatalf("step %d %s: pending waiters=%d, want %d", step, action, got, want)
			}
			if !connected {
				break
			}
		}
		if connected {
			for _, p := range activeCalls() {
				complete(p)
			}
			barrier() // cancellation/mismatch must not poison an otherwise live socket
		}
		if got := pendingCount(c); got != 0 {
			t.Fatalf("finished trace leaked %d waiters", got)
		}
	})
	t.Logf("generated response transitions: %v", coverage)
}
