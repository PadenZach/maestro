package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
)

type recoveryCall struct {
	app, version string
	ids          []string
}

type fakeHub struct {
	mu           sync.Mutex
	executors    []hub.ExecutorView
	disconnected []hub.DisconnectedExecutor
	rows         map[string][]protocol.WorkflowAggregate
	readError    error
	response     []byte
	recoveryErr  error
	calls        []recoveryCall
	reads        []protocol.Request
	changes      chan struct{}
	afterRead    func()
}

func (h *fakeHub) Executors() []hub.ExecutorView { return h.executors }
func (h *fakeHub) Changes() <-chan struct{}      { return h.changes }
func (h *fakeHub) RecoverySnapshot() ([]hub.ExecutorView, []hub.DisconnectedExecutor) {
	select {
	case <-h.changes:
	default:
	}
	disconnected := h.disconnected
	h.disconnected = nil
	return h.executors, disconnected
}
func (h *fakeHub) Request(_ context.Context, app string, req protocol.Request) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reads = append(h.reads, req)
	if h.afterRead != nil {
		h.afterRead()
	}
	if h.readError != nil {
		return nil, h.readError
	}
	if h.response != nil {
		return h.response, nil
	}
	rows := h.rows[app]
	if rows == nil {
		rows = []protocol.WorkflowAggregate{}
	}
	return json.Marshal(map[string]any{"output": rows})
}
func (h *fakeHub) RequestVersion(_ context.Context, app, version string, req protocol.Request) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, recoveryCall{app, version, req["executor_ids"].([]string)})
	return []byte(`{"success":true}`), h.recoveryErr
}

func pending(id, version string, count int64) protocol.WorkflowAggregate {
	return protocol.WorkflowAggregate{Group: map[string]*string{"executor_id": &id, "application_version": &version}, Count: &count}
}

func fixture() (*Coordinator, *fakeHub) {
	h := &fakeHub{
		executors: []hub.ExecutorView{{App: "app", Version: "v1", ExecutorID: "replacement"}},
		rows:      map[string][]protocol.WorkflowAggregate{"app": {pending("dead", "v1", 1)}},
	}
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)), h, time.Minute), h
}

func TestTimeoutAndReplacement(t *testing.T) {
	for _, replacementAt := range []time.Duration{0, 30 * time.Second, 90 * time.Second} {
		t.Run(replacementAt.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, h := fixture()
				replacement := h.executors
				h.executors = nil
				h.disconnected = []hub.DisconnectedExecutor{{ExecutorView: hub.ExecutorView{App: "app", Version: "v1", ExecutorID: "dead"}, DisconnectedAt: time.Now()}}
				c.reconcile(context.Background())
				time.Sleep(replacementAt)
				h.executors = replacement
				c.reconcile(context.Background())
				if replacementAt < time.Minute {
					if len(h.calls) != 0 {
						t.Fatal("recovery before timeout")
					}
					time.Sleep(time.Minute - replacementAt - time.Nanosecond)
					c.reconcile(context.Background())
					if len(h.calls) != 0 {
						t.Fatal("recovery one nanosecond before timeout")
					}
					time.Sleep(time.Nanosecond)
					c.reconcile(context.Background())
				}
				if len(h.calls) != 1 || h.calls[0].app != "app" || h.calls[0].version != "v1" || len(h.calls[0].ids) != 1 || h.calls[0].ids[0] != "dead" {
					t.Fatalf("recovery calls: %+v", h.calls)
				}
			})
		})
	}
}

func TestRestartDiscoversOldOwnersAndWaitsForMatchingVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, h := fixture() // No disconnect history, as after a Maestro restart.
		h.executors[0].Version = "v2"
		h.rows["app"] = append(h.rows["app"], pending("replacement", "v2", 20))
		c.reconcile(context.Background())
		time.Sleep(time.Minute)
		c.reconcile(context.Background())
		if len(h.calls) != 0 {
			t.Fatal("recovered through a different application version")
		}
		h.executors = append(h.executors, hub.ExecutorView{App: "app", Version: "v1", ExecutorID: "new-v1"})
		c.reconcile(context.Background())
		if len(h.calls) != 1 || h.calls[0].ids[0] != "dead" {
			t.Fatalf("old owner was forgotten: %+v", h.calls)
		}
		for _, req := range h.reads {
			body := req["body"].(map[string]any)
			if _, exists := body["start_time"]; exists {
				t.Fatal("discovery excludes historical work")
			}
			if req["type"] != string(protocol.MsgGetWorkflowAggregates) || body["group_by_executor_id"] != true || body["group_by_application_version"] != true {
				t.Fatalf("discovery must use owner aggregates: %v", req)
			}
		}
	})
}

func TestReconnectedOwnerIsNotRecovered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, h := fixture()
		c.reconcile(context.Background())
		time.Sleep(30 * time.Second)
		h.executors = append(h.executors, hub.ExecutorView{App: "app", Version: "v1", ExecutorID: "dead"})
		time.Sleep(time.Minute)
		c.reconcile(context.Background())
		if len(h.calls) != 0 {
			t.Fatal("recovered a connected owner")
		}
	})
}

func TestDisconnectDuringDiscoveryGetsFullTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, h := fixture()
		h.changes = make(chan struct{}, 1)
		c.reconcile(context.Background())
		time.Sleep(time.Minute)
		h.afterRead = func() {
			// The original owner briefly reconnected and disconnected while
			// the aggregate read was running. Its new timeout must be observed.
			h.disconnected = []hub.DisconnectedExecutor{{ExecutorView: hub.ExecutorView{App: "app", Version: "v1", ExecutorID: "dead"}, DisconnectedAt: time.Now()}}
			h.changes <- struct{}{}
		}
		c.reconcile(context.Background())
		if len(h.calls) != 0 {
			t.Fatal("recovery dispatched before processing the new disconnect")
		}
		h.afterRead = nil
		c.reconcile(context.Background())
		time.Sleep(time.Minute - time.Nanosecond)
		c.reconcile(context.Background())
		if len(h.calls) != 0 {
			t.Fatal("new disconnect did not restart the timeout")
		}
		time.Sleep(time.Nanosecond)
		c.reconcile(context.Background())
		if len(h.calls) != 1 {
			t.Fatalf("recovery after new timeout: %v", h.calls)
		}
	})
}

func TestReadsMustSucceedBeforeRecovery(t *testing.T) {
	for _, response := range []string{`{}`, `{"output":null}`, `{"output":[],"error_message":"DB unavailable"}`, `{"output":[{"group":{"executor_id":"dead"},"count":1}]}`, `{"output":[{"group":{"executor_id":"dead","application_version":"v1"},"count":null}]}`} {
		t.Run(response, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, h := fixture()
				c.reconcile(context.Background())
				time.Sleep(time.Minute)
				h.response = []byte(response)
				c.reconcile(context.Background())
				if len(h.calls) != 0 {
					t.Fatal("dispatched after an invalid discovery response")
				}
				h.response = nil
				time.Sleep(time.Minute)
				c.reconcile(context.Background())
				if len(h.calls) != 1 {
					t.Fatal("invalid response erased the pending owner")
				}
			})
		})
	}
}

func TestAcknowledgementAndLostReplyAreReconciled(t *testing.T) {
	for _, err := range []error{nil, errors.New("reply lost")} {
		synctest.Test(t, func(t *testing.T) {
			c, h := fixture()
			h.recoveryErr = err
			c.reconcile(context.Background())
			time.Sleep(time.Minute)
			c.reconcile(context.Background())
			c.reconcile(context.Background())
			if len(h.calls) != 1 {
				t.Fatal("immediate repeat after acknowledgement or lost reply")
			}
			// The SDK may leave some workflows with the original owner even
			// after success; those must be retried after the recovery delay.
			time.Sleep(time.Minute)
			c.reconcile(context.Background())
			if len(h.calls) != 2 {
				t.Fatal("remaining pending work was forgotten")
			}
			h.rows["app"] = nil
			time.Sleep(10 * time.Minute)
			c.reconcile(context.Background())
			if len(h.calls) != 2 {
				t.Fatal("resent recovery after old owner's pending work cleared")
			}
		})
	}
}

func TestManyOwnersUseBoundedRequestsAndMakeProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, h := fixture()
		h.rows["app"] = nil
		for i := range 30000 {
			h.rows["app"] = append(h.rows["app"], pending(fmt.Sprintf("dead-%05d", i), "v1", 1))
		}
		c.reconcile(context.Background())
		time.Sleep(time.Minute)
		c.reconcile(context.Background())
		if len(h.calls) == 0 || len(h.calls) > 4 {
			t.Fatalf("unbounded requests in a pass: %d", len(h.calls))
		}
		seen := map[string]bool{}
		for _, call := range h.calls {
			if len(call.ids) > 64 {
				t.Fatalf("oversized owner batch: %d", len(call.ids))
			}
			for _, id := range call.ids {
				seen[id] = true
			}
		}
		h.calls = nil
		c.reconcile(context.Background())
		for _, call := range h.calls {
			for _, id := range call.ids {
				if seen[id] {
					t.Fatalf("retried %s while other owners still await recovery", id)
				}
			}
		}
	})
}

type blockedHub struct {
	*fakeHub
	active atomic.Int32
}

func (h *blockedHub) Request(ctx context.Context, _ string, _ protocol.Request) ([]byte, error) {
	h.active.Add(1)
	defer h.active.Add(-1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRunBoundsConcurrencyAndJoinsRequestsOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, backend := fixture()
		backend.executors = nil
		for i := range 20 {
			backend.executors = append(backend.executors, hub.ExecutorView{App: fmt.Sprintf("app-%d", i), ExecutorID: "live", Version: "v1"})
		}
		h := &blockedHub{fakeHub: backend}
		c.hub = h
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { c.Run(ctx); close(done) }()
		synctest.Wait()
		if n := h.active.Load(); n != 4 {
			t.Errorf("in-flight application reads = %d, want 4", n)
		}
		cancel()
		<-done
		if h.active.Load() != 0 {
			t.Fatal("recovery returned with requests still running")
		}
	})
}
