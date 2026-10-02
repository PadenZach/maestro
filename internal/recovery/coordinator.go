// Package recovery recovers pending workflows after an executor disconnects.
// All local bookkeeping is disposable; DBOS remains the source of workflow state.
package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
)

const (
	scanInterval     = 5 * time.Second
	concurrentApps   = 4
	ownersPerRequest = 64
	requestsPerPass  = 4
)

// Backend is the existing executor transport used by the coordinator.
type Backend interface {
	Executors() []hub.ExecutorView
	RecoverySnapshot() ([]hub.ExecutorView, []hub.DisconnectedExecutor)
	Changes() <-chan struct{}
	Request(context.Context, string, protocol.Request) ([]byte, error)
	RequestVersion(context.Context, string, string, protocol.Request) ([]byte, error)
}

type ownerKey struct{ id, version string }

type owner struct {
	absentSince       time.Time
	nextAttempt       time.Time
	attempts          int
	waitingForVersion bool
}

type application struct {
	owners   map[ownerKey]*owner
	retryAt  time.Time
	failures int
}

// Coordinator runs once per Maestro process. It does not require local storage.
type Coordinator struct {
	log     *slog.Logger
	hub     Backend
	timeout time.Duration
	apps    map[string]*application
}

func New(log *slog.Logger, backend Backend, timeout time.Duration) *Coordinator {
	if timeout <= 0 {
		timeout = time.Minute
	}
	return &Coordinator{log: log, hub: backend, timeout: timeout, apps: make(map[string]*application)}
}

// Run waits for connection changes and periodically reconciles pending owners.
// Passes never overlap. Cancellation joins all in-flight work before returning.
func (c *Coordinator) Run(ctx context.Context) {
	ticker := time.NewTicker(min(scanInterval, c.timeout))
	defer ticker.Stop()
	for ctx.Err() == nil {
		c.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-c.hub.Changes():
		case <-ticker.C:
		}
	}
}

func (c *Coordinator) app(name string) *application {
	state := c.apps[name]
	if state == nil {
		state = &application{owners: make(map[ownerKey]*owner)}
		c.apps[name] = state
	}
	return state
}

func (c *Coordinator) reconcile(ctx context.Context) {
	executors, disconnected := c.hub.RecoverySnapshot()
	for _, executor := range disconnected {
		state := c.app(executor.App)
		key := ownerKey{executor.ExecutorID, executor.Version}
		// A later disconnect of the same ID starts a new waiting period.
		state.owners[key] = &owner{absentSince: executor.DisconnectedAt}
	}
	connected := make(map[string][]hub.ExecutorView)
	for _, executor := range executors {
		connected[executor.App] = append(connected[executor.App], executor)
		c.app(executor.App)
	}
	jobs := make(chan string)
	var workers sync.WaitGroup
	for range min(concurrentApps, len(connected)) {
		workers.Go(func() {
			for app := range jobs {
				if ctx.Err() == nil {
					c.reconcileApp(ctx, app, c.apps[app], connected[app])
				}
			}
		})
	}
	for app := range connected {
		if ctx.Err() != nil {
			break
		}
		jobs <- app
	}
	close(jobs)
	workers.Wait()
}

func (c *Coordinator) reconcileApp(ctx context.Context, app string, state *application, executors []hub.ExecutorView) {
	now := time.Now()
	live, versions := make(map[string]bool), make(map[string]bool)
	for _, executor := range executors {
		live[executor.ExecutorID] = true
		versions[executor.Version] = true
	}
	for key := range state.owners {
		if live[key.id] {
			delete(state.owners, key)
		}
	}
	if now.Before(state.retryAt) {
		return
	}
	pending, err := c.discover(ctx, app)
	if err != nil {
		state.failures++
		state.retryAt = time.Now().Add(backoff(scanInterval, state.failures, time.Minute))
		if ctx.Err() == nil {
			c.log.Warn("workflow recovery discovery failed", "app", app, "err", err, "retry_at", state.retryAt)
		}
		return
	}
	state.failures = 0
	state.retryAt = time.Time{}
	for key := range state.owners {
		if _, exists := pending[key]; !exists {
			delete(state.owners, key)
			c.log.Info("executor pending workflows cleared", "app", app, "executor_id", key.id, "version", key.version)
		}
	}
	var ready []ownerKey
	for key, count := range pending {
		if live[key.id] {
			continue
		}
		candidate := state.owners[key]
		if candidate == nil {
			candidate = &owner{absentSince: now}
			state.owners[key] = candidate
			c.log.Info("abandoned workflow owner discovered", "app", app, "executor_id", key.id, "version", key.version, "pending", count, "recover_after", now.Add(c.timeout))
		}
		if !versions[key.version] {
			if !candidate.waitingForVersion {
				c.log.Info("workflow recovery waiting for application version", "app", app, "version", key.version, "executor_id", key.id, "pending", count)
			}
			candidate.waitingForVersion = true
			continue
		}
		candidate.waitingForVersion = false
		if !now.Before(candidate.absentSince.Add(c.timeout)) && !now.Before(candidate.nextAttempt) {
			ready = append(ready, key)
		}
	}
	// Prefer owners never attempted, then the oldest retry. Sorting also keeps
	// progress fair when a pass cannot dispatch every owner.
	sort.Slice(ready, func(i, j int) bool {
		a, b := state.owners[ready[i]], state.owners[ready[j]]
		if !a.nextAttempt.Equal(b.nextAttempt) {
			return a.nextAttempt.Before(b.nextAttempt)
		}
		if ready[i].version != ready[j].version {
			return ready[i].version < ready[j].version
		}
		return ready[i].id < ready[j].id
	})
	for sent := 0; len(ready) > 0 && sent < requestsPerPass && ctx.Err() == nil; sent++ {
		version := ready[0].version
		var batch []ownerKey
		var remaining []ownerKey
		for _, key := range ready {
			if key.version == version && len(batch) < ownersPerRequest {
				batch = append(batch, key)
			} else {
				remaining = append(remaining, key)
			}
		}
		ready = remaining
		// Discovery can take time. Exclude owners that reconnected during it.
		live = make(map[string]bool)
		for _, executor := range c.hub.Executors() {
			if executor.App == app {
				live[executor.ExecutorID] = true
			}
		}
		ids := make([]string, 0, len(batch))
		for _, key := range batch {
			if !live[key.id] {
				ids = append(ids, key.id)
			}
		}
		if len(ids) == 0 {
			continue
		}
		// Only Run and RecoverySnapshot consume this channel, outside a pass.
		// Apply intervening connection changes before making a recovery request.
		if len(c.hub.Changes()) > 0 {
			return
		}
		data, err := c.hub.RequestVersion(ctx, app, version, protocol.RecoveryRequest(ids))
		if err == nil {
			var response protocol.SuccessResponse
			if err = json.Unmarshal(data, &response); err == nil {
				err = response.Err()
			}
		}
		for _, id := range ids {
			candidate := state.owners[ownerKey{id, version}]
			candidate.attempts++
			candidate.nextAttempt = time.Now().Add(backoff(c.timeout, candidate.attempts, max(c.timeout, 5*time.Minute)))
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.log.Warn("workflow recovery request failed; will recheck pending work", "app", app, "version", version, "executor_ids", ids, "err", err)
		} else {
			c.log.Info("workflow recovery acknowledged; awaiting pending-work check", "app", app, "version", version, "executor_ids", ids)
		}
	}
}

// discover transfers one count per pending owner/version, with no workflow
// payloads and no history cutoff. It also verifies previous recovery attempts.
func (c *Coordinator) discover(ctx context.Context, app string) (map[ownerKey]int64, error) {
	yes := true
	data, err := c.hub.Request(ctx, app, protocol.GetWorkflowAggregatesRequest(protocol.WorkflowAggregatesBody{
		GroupByExecutorID: &yes, GroupByApplicationVersion: &yes, SelectCount: &yes,
		Status: []string{"PENDING"}, ApplicationName: []string{app},
	}))
	if err != nil {
		return nil, err
	}
	var response protocol.GetWorkflowAggregatesResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	if err := response.Err(); err != nil {
		return nil, err
	}
	if response.Output == nil {
		return nil, fmt.Errorf("pending owner aggregates missing output array")
	}
	owners := make(map[ownerKey]int64, len(response.Output))
	for _, row := range response.Output {
		id, hasID := row.Group["executor_id"]
		version, hasVersion := row.Group["application_version"]
		if !hasID || !hasVersion || row.Count == nil || *row.Count < 0 {
			return nil, fmt.Errorf("invalid pending owner aggregate")
		}
		if id == nil || *id == "" || version == nil {
			c.log.Warn("pending workflows lack an executor or application version", "app", app, "pending", *row.Count)
			continue
		}
		if *row.Count > 0 {
			owners[ownerKey{*id, *version}] = *row.Count
		}
	}
	return owners, nil
}

func backoff(initial time.Duration, attempts int, limit time.Duration) time.Duration {
	delay := initial
	for i := 1; i < attempts && delay < limit; i++ {
		delay += min(delay, limit-delay)
	}
	return delay
}
