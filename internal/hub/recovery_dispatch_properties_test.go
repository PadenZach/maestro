package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/zpaden/maestro/internal/protocol"
)

// The dispatch oracle declares a few contract-defined reads and mutations; it
// never calls retryableRead to calculate expected behavior. Each generated
// outcome applies to the next selected peer, independently of map iteration.
func TestRecoveryBoundaryDispatchReplay(t *testing.T) {
	coverage := map[string]int{}
	rapid.Check(t, func(t *rapid.T) {
		rapid.SyncTest(t, func(t *rapid.T) {
			command := rapid.SampledFrom([]struct {
				typ  protocol.MessageType
				read bool
			}{
				{protocol.MsgGetWorkflow, true},
				{protocol.MsgExistPendingWorkflows, true},
				{protocol.MsgListWorkflows, true},
				{protocol.MsgGetWorkflowEvents, true},
				{protocol.MsgRecovery, false},
				{protocol.MsgCancel, false},
				{protocol.MsgResume, false},
				{"unknown_extension", false},
			}).Draw(t, "command")
			outcomes := rapid.SliceOfN(rapid.SampledFrom([]string{
				"disconnect", "disconnect", "success", "refusal", "cancel", "timeout",
			}), 1, 6).Draw(t, "peer_outcomes")
			// One survivor is always available, so missing eligible peers cannot
			// accidentally make a forbidden replay look like correct behavior.
			outcomes = append(outcomes, "success")
			wantAttempts := 1
			for command.read && outcomes[wantAttempts-1] == "disconnect" {
				wantAttempts++
			}
			wantOutcome := outcomes[wantAttempts-1]

			req := protocol.NewRequest(command.typ)
			if rapid.Bool().Draw(t, "typed_discriminator") {
				req["type"] = command.typ
			}
			workflowID := rapid.StringMatching(`[a-z0-9_-]{1,20}`).Draw(t, "workflow_id")
			switch command.typ {
			case protocol.MsgRecovery:
				req["executor_ids"] = rapid.SliceOfN(rapid.SampledFrom([]string{"dead-owner", "other-owner"}), 0, 4).Draw(t, "executor_ids")
			case protocol.MsgExistPendingWorkflows:
				req["executor_id"] = workflowID
				req["application_version"] = "application-v2"
			case protocol.MsgListWorkflows:
				req["body"] = map[string]any{
					"executor_id": []string{workflowID}, "application_version": []string{"application-v2"},
					"load_input": false, "limit": rapid.IntRange(0, 10).Draw(t, "limit"),
				}
			default:
				req["workflow_id"] = workflowID
			}
			before, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			var wantWire protocol.Request
			if err := json.Unmarshal(before, &wantWire); err != nil {
				t.Fatal(err)
			}

			h := propertyHub()
			type attempt struct {
				conn *Conn
				data []byte
			}
			events := make(chan attempt, len(outcomes)+2)
			var peers []*Conn
			var workers sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				for _, c := range peers {
					c.cancel()
				}
				workers.Wait()
			})
			for i := 0; i <= len(outcomes); i++ {
				app := "orders"
				if i == len(outcomes) {
					app = "billing" // an available peer from another app is never eligible
				}
				// Transport mailboxes isolate Request/roundtrip from network and
				// let synctest advance the real per-attempt timeout virtually.
				// Response routing over real sockets is checked separately.
				c := newConn(h, nil, app)
				c.executor = &Executor{
					ID:          fmt.Sprintf("peer-%d", i),
					Language:    rapid.SampledFrom([]string{"", "python", "unreviewed"}).Draw(t, "language"),
					DBOSVersion: rapid.SampledFrom([]string{"", "3.1.0", "999.0.0"}).Draw(t, "sdk_version"),
				}
				if !h.register(c) {
					t.Fatal("register dispatch peer")
				}
				peers = append(peers, c)
				workers.Go(func() {
					for {
						select {
						case data := <-c.send:
							events <- attempt{conn: c, data: data}
						case <-ctx.Done():
							return
						}
					}
				})
			}
			done := make(chan result, 1)
			workers.Go(func() {
				data, err := h.Request(ctx, "orders", req)
				done <- result{data: data, err: err}
			})
			seenPeers := map[*Conn]bool{}
			seenIDs := map[string]bool{}
			attempts := 0
			var wantResponse []byte
			started := time.Now()
			for {
				select {
				case event := <-events:
					attempts++
					if attempts > wantAttempts {
						t.Fatalf("%s replayed after %s: attempt %d, want %d", command.typ, wantOutcome, attempts, wantAttempts)
					}
					if event.conn.app != "orders" || seenPeers[event.conn] {
						t.Fatalf("request retried a used peer or crossed applications: %s/%s", event.conn.app, event.conn.executor.ID)
					}
					seenPeers[event.conn] = true
					var wire protocol.Request
					if err := json.Unmarshal(event.data, &wire); err != nil {
						t.Fatal(err)
					}
					id, ok := wire["request_id"].(string)
					if !ok || id == "" || seenIDs[id] {
						t.Fatalf("attempt reused or omitted its correlation ID: %v", wire)
					}
					seenIDs[id] = true
					delete(wire, "request_id")
					if !reflect.DeepEqual(wire, wantWire) {
						t.Fatalf("attempt %d changed command/filters: got %v, want %v", attempts, wire, wantWire)
					}
					switch outcome := outcomes[attempts-1]; outcome {
					case "disconnect":
						event.conn.cancel()
					case "cancel":
						cancel()
					case "timeout":
						// No response: synctest advances the actual context timer
						// once every runnable goroutine reaches a channel barrier.
					case "success", "refusal":
						responseFrame := protocol.Request{"type": string(command.typ), "request_id": id, "success": true}
						if outcome == "refusal" {
							responseFrame["success"] = false
							responseFrame["error_message"] = "executor refused this request"
						}
						wantResponse, err = json.Marshal(responseFrame)
						if err != nil {
							t.Fatal(err)
						}
						event.conn.mu.Lock()
						pending, ok := event.conn.pending[id]
						event.conn.mu.Unlock()
						if !ok {
							t.Fatal("outbound request lost its response waiter")
						}
						pending.ch <- response{data: wantResponse}
					}
				case got := <-done:
					// Drain scheduling, not wall time, before asserting no hidden
					// extra frame was queued by the completed dispatcher.
					synctest.Wait()
					if attempts != wantAttempts || len(events) != 0 {
						t.Fatalf("%s after %v: attempts=%d plus %d queued, want %d", command.typ, outcomes, attempts, len(events), wantAttempts)
					}
					var wantErr error
					switch wantOutcome {
					case "disconnect":
						wantErr = errConnClosed
					case "cancel":
						wantErr = context.Canceled
					case "timeout":
						wantErr = context.DeadlineExceeded
					}
					if !errors.Is(got.err, wantErr) || !bytes.Equal(got.data, wantResponse) {
						t.Fatalf("%s after %v: got %s / %v, want %s / %v", command.typ, outcomes, got.data, got.err, wantResponse, wantErr)
					}
					if elapsed := time.Since(started); wantOutcome == "timeout" && elapsed != h.requestTimeout {
						t.Fatalf("timeout elapsed=%v, want one attempt budget %v", elapsed, h.requestTimeout)
					}
					for _, c := range peers {
						if n := pendingCount(c); n != 0 {
							t.Fatalf("dispatch left %d waiters on %s", n, c.executor.ID)
						}
					}
					after, err := json.Marshal(req)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("dispatcher modified the caller request: before=%s, after=%s, error=%v", before, after, err)
					}
					coverage[string(command.typ)+"/"+wantOutcome]++
					coverage["attempts"] += attempts
					coverage["disconnect_retries"] += attempts - 1
					return
				}
			}
		})
	})
	t.Logf("generated dispatch outcomes: %v", coverage)
}
