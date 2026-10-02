package api

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

const overviewTimeFormat = "2006-01-02T15:04:05.000Z"

var overviewUnfinished = []string{"PENDING", "ENQUEUED", "DELAYED"}

type overviewWindow struct {
	Range, Start, End string
	BucketMS          int64
}

func newOverviewWindow(rangeName string, now time.Time) (overviewWindow, error) {
	var duration, bucket time.Duration
	switch rangeName {
	case "", "24h":
		rangeName = "24h"
		duration = 24 * time.Hour
		bucket = time.Hour
	case "7d":
		duration = 7 * 24 * time.Hour
		bucket = 4 * time.Hour
	case "30d":
		duration = 30 * 24 * time.Hour
		bucket = 12 * time.Hour
	default:
		return overviewWindow{}, errors.New("choose 24h, 7d, or 30d")
	}
	end := now.UTC().Truncate(30 * time.Second)
	return overviewWindow{rangeName, end.Add(-duration).Format(overviewTimeFormat), end.Format(overviewTimeFormat), bucket.Milliseconds()}, nil
}
func parseOverviewWindow(r *http.Request) (overviewWindow, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return overviewWindow{}, err
	}
	for name, values := range q {
		if (name != "range" && name != "start_time" && name != "end_time") || len(values) != 1 {
			return overviewWindow{}, errors.New("invalid overview query")
		}
	}
	win, err := newOverviewWindow(q.Get("range"), time.Now())
	if err != nil {
		return win, err
	}
	if q.Get("start_time") == "" && q.Get("end_time") == "" {
		return win, nil
	}
	start, e1 := time.Parse(overviewTimeFormat, q.Get("start_time"))
	end, e2 := time.Parse(overviewTimeFormat, q.Get("end_time"))
	if e1 != nil || e2 != nil || start.After(end) || end.Sub(start) > 30*24*time.Hour || end.Sub(start) != overviewDuration(win.Range) {
		return win, errors.New("invalid overview time window")
	}
	win.Start, win.End = q.Get("start_time"), q.Get("end_time")
	return win, nil
}
func overviewDuration(r string) time.Duration {
	switch r {
	case "7d":
		return 7 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	}
	return 24 * time.Hour
}
func overviewDrilldown(app string, statuses []string, start, end, queue string) string {
	q := url.Values{"children": {"true"}}
	for _, status := range statuses {
		q.Add("status", status)
	}
	if start != "" {
		q.Set("start_time", start)
		q.Set("end_time", end)
	}
	if queue != "" {
		q.Set("queue", queue)
	}
	path := applicationPath(app) + "/workflows"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	return path
}

type overviewCount struct {
	Key, Label, URL string
	Count           int64
	Statuses        []string
	Missing         bool
}
type overviewSegment struct {
	Key, Label, URL, Height string
	Count                   int64
}
type overviewBucket struct {
	Label, Start, End string
	Count             int64
	Segments          []overviewSegment
}
type overviewQueue struct {
	DetailsURL                        string
	Name, URL                         string
	Pending, Enqueued, Delayed, Total int64
}
type overviewActivity struct {
	Total           int64
	URL, Start, End string
	Counts          []overviewCount
	Buckets         []overviewBucket
	Peak            int64
	MissingStatus   bool
}
type overviewWorkload struct {
	Total, Queued  int64
	URL, QueuedURL string
	Counts         []overviewCount
	Queues         []overviewQueue
}
type overviewRecent struct {
	protocol.WorkflowsOutput
	CreatedUTC string
}

type overviewPanel struct {
	Available                     bool
	Connected                     int
	App, Kind, Error, LastSuccess string
	Stale, Loaded                 bool
	Window                        overviewWindow
	Activity                      *overviewActivity
	Workload                      *overviewWorkload
	Recent                        []overviewRecent
	ScheduleCount                 int
}

// One bounded cache per server coalesces identical reads; successful snapshots
// survive read failures and disconnects. No workflow records are persisted.
type overviewCacheEntry struct {
	data    overviewPanel
	attempt time.Time
	err     error
	pending chan struct{}
}
type overviewCache struct {
	mu      sync.Mutex
	entries map[string]*overviewCacheEntry
}

func (c *overviewCache) read(ctx context.Context, key string, ttl time.Duration, available bool, load func() (overviewPanel, error)) (overviewPanel, error) {
	for {
		c.mu.Lock()
		if c.entries == nil {
			c.entries = make(map[string]*overviewCacheEntry)
		}
		entry := c.entries[key]
		if !available {
			var data overviewPanel
			if entry != nil {
				data = entry.data
			}
			c.mu.Unlock()
			return data, hub.ErrAppUnavailable
		}
		if entry != nil && entry.pending != nil {
			done, retained := entry.pending, entry.data
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return retained, ctx.Err()
			case <-done:
				continue
			}
		}
		if entry != nil && time.Since(entry.attempt) < ttl && (entry.err == nil || time.Since(entry.attempt) < 5*time.Second) {
			data, err := entry.data, entry.err
			c.mu.Unlock()
			return data, err
		}
		if entry == nil {
			if len(c.entries) >= 128 {
				var oldestKey string
				var oldest time.Time
				for k, e := range c.entries {
					if e.pending == nil && (oldestKey == "" || e.attempt.Before(oldest)) {
						oldestKey, oldest = k, e.attempt
					}
				}
				if oldestKey != "" {
					delete(c.entries, oldestKey)
				} else {
					c.mu.Unlock()
					return overviewPanel{}, errors.New("overview is busy; refresh shortly")
				}
			}
			entry = &overviewCacheEntry{}
			c.entries[key] = entry
		}
		entry.pending = make(chan struct{})
		c.mu.Unlock()
		data, err := load()
		c.mu.Lock()
		entry.attempt = time.Now()
		entry.err = err
		if err == nil {
			data.Loaded = true
			data.LastSuccess = entry.attempt.UTC().Format(overviewTimeFormat)
			entry.data = data
		}
		data = entry.data
		close(entry.pending)
		entry.pending = nil
		c.mu.Unlock()
		return data, err
	}
}
func (s *Server) overviewAvailable(app string) bool {
	for _, e := range s.hub.Executors() {
		if e.App == app {
			return true
		}
	}
	return false
}
func (s *Server) handleApplicationOverview(w http.ResponseWriter, r *http.Request) {
	app, kind := r.PathValue("app"), r.PathValue("panel")
	if kind != "activity" && kind != "workload" && kind != "recent" && kind != "schedules" {
		http.NotFound(w, r)
		return
	}
	window, err := parseOverviewWindow(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	ttl := 30 * time.Second
	key := app + "\x00" + kind
	if kind == "activity" || kind == "recent" {
		key += "\x00" + window.Start + "\x00" + window.End
	}
	if kind == "schedules" {
		ttl = 60 * time.Second
	}
	data, err := s.overviewCache.read(r.Context(), key, ttl, s.overviewAvailable(app), func() (overviewPanel, error) {
		sharedContext, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 25*time.Second)
		defer cancel()
		return s.loadOverviewPanel(sharedContext, app, kind, window)
	})
	data.App, data.Kind = app, kind
	for _, executor := range s.hub.Executors() {
		if executor.App == app {
			data.Connected++
		}
	}
	data.Available = data.Connected > 0
	if err != nil {
		data.Error = htmlErrorText(err)
		data.Stale = data.Loaded
	}
	s.web.Partial(w, "application_panel", data)
}
func (s *Server) loadOverviewPanel(ctx context.Context, app, kind string, window overviewWindow) (overviewPanel, error) {
	data := overviewPanel{App: app, Kind: kind, Window: window}
	yes := true
	switch kind {
	case "activity", "workload":
		body := protocol.WorkflowAggregatesBody{GroupByStatus: &yes, SelectCount: &yes, ApplicationName: []string{app}}
		if kind == "activity" {
			body.StartTime = &window.Start
			body.EndTime = &window.End
			body.TimeBucketSizeMS = &window.BucketMS
		} else {
			body.GroupByQueueName = &yes
			body.Status = overviewUnfinished
		}
		raw, err := s.hub.Request(ctx, app, protocol.GetWorkflowAggregatesRequest(body))
		if err != nil {
			return data, err
		}
		records, err := localV2InspectionPayload(raw)
		if err != nil {
			return data, err
		}
		rows := make([]protocol.WorkflowAggregate, 0, len(records))
		for _, raw := range records {
			var row protocol.WorkflowAggregate
			if err := json.Unmarshal(raw, &row); err != nil {
				return data, err
			}
			if row.Count == nil || *row.Count < 0 || row.Group == nil {
				return data, errors.New("aggregate count or group unavailable")
			}
			rows = append(rows, row)
		}
		if kind == "activity" {
			data.Activity, err = buildOverviewActivity(app, window, rows)
		} else {
			data.Workload, err = buildOverviewWorkload(app, rows)
		}
		return data, err
	case "recent":
		limit := 10
		var response protocol.ListWorkflowsResponse
		err := s.dispatch(ctx, app, protocol.ListWorkflowsRequest(protocol.ListWorkflowsBody{ApplicationName: []string{app}, StartTime: window.Start, EndTime: window.End, Limit: &limit, SortDesc: true, LoadInput: false, LoadOutput: false}), &response)
		if err != nil {
			return data, err
		}
		if response.Output == nil {
			return data, errors.New("recent workflows unavailable")
		}
		if len(response.Output) > 10 {
			return data, errors.New("recent workflows exceeded requested limit")
		}
		for _, wf := range response.Output {
			if wf.ApplicationName != nil && *wf.ApplicationName != app {
				return data, errors.New("recent workflows did not preserve application filter")
			}
			created := overviewCreatedUTC(wf.CreatedAt)
			if created != "Unknown" {
				if created < window.Start || created > window.End {
					return data, errors.New("recent workflows did not preserve creation window")
				}
			}
			data.Recent = append(data.Recent, overviewRecent{WorkflowsOutput: wf, CreatedUTC: created})
		}
		return data, nil
	case "schedules":
		no := false
		var response protocol.ListSchedulesResponse
		err := s.dispatch(ctx, app, protocol.ListSchedulesRequest(protocol.ListSchedulesBody{Status: []string{"ACTIVE"}, ApplicationName: []string{app}, LoadContext: &no}), &response)
		if err != nil {
			return data, err
		}
		if response.Output == nil {
			return data, errors.New("active schedules unavailable")
		}
		for _, schedule := range response.Output {
			if schedule.Status != "ACTIVE" {
				return data, errors.New("schedule response did not preserve ACTIVE filter")
			}
		}
		data.ScheduleCount = len(response.Output)
		return data, nil
	}
	return data, errors.New("unknown overview panel")
}
func overviewGroups() []overviewCount {
	return []overviewCount{{Key: "success", Label: "Succeeded", Statuses: []string{"SUCCESS"}}, {Key: "failed", Label: "Failed", Statuses: []string{"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"}}, {Key: "progress", Label: "In progress", Statuses: overviewUnfinished}, {Key: "cancelled", Label: "Cancelled", Statuses: []string{"CANCELLED"}}, {Key: "other", Label: "Other / unknown"}}
}
func overviewGroup(status *string) int {
	if status == nil {
		return 4
	}
	switch *status {
	case "SUCCESS":
		return 0
	case "ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED":
		return 1
	case "PENDING", "ENQUEUED", "DELAYED":
		return 2
	case "CANCELLED":
		return 3
	}
	return 4
}
func addOverviewCount(total *int64, count int64) error {
	if count > math.MaxInt64-*total {
		return errors.New("aggregate count out of range")
	}
	*total += count
	return nil
}
func overviewBucketEpoch(value string) (int64, error) {
	if epoch, err := strconv.ParseInt(value, 10, 64); err == nil {
		return epoch, nil
	}
	stamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return 0, errors.New("aggregate time bucket unavailable")
	}
	return stamp.UnixMilli(), nil
}
func buildOverviewActivity(app string, window overviewWindow, rows []protocol.WorkflowAggregate) (*overviewActivity, error) {
	start, _ := time.Parse(overviewTimeFormat, window.Start)
	end, _ := time.Parse(overviewTimeFormat, window.End)
	first := start.UnixMilli() / window.BucketMS * window.BucketMS
	last := end.UnixMilli() / window.BucketMS * window.BucketMS
	counts := overviewGroups()
	a := &overviewActivity{Counts: counts, Start: window.Start, End: window.End, URL: overviewDrilldown(app, nil, window.Start, window.End, "")}
	values := map[int64][5]int64{}
	unknown := map[string]bool{}
	for _, row := range rows {
		if row.Count == nil || *row.Count < 0 {
			return nil, errors.New("aggregate count unavailable")
		}
		bucketValue := row.Group["time_bucket"]
		if bucketValue == nil {
			return nil, errors.New("aggregate time bucket unavailable")
		}
		epoch, err := overviewBucketEpoch(*bucketValue)
		if err != nil {
			return nil, err
		}
		if epoch < first || epoch > last || epoch%window.BucketMS != 0 {
			return nil, errors.New("aggregate bucket outside requested window")
		}
		status := row.Group["status"]
		group := overviewGroup(status)
		if group == 4 {
			if status == nil || *status == "" {
				a.MissingStatus = true
			} else {
				unknown[*status] = true
			}
		}
		if err := addOverviewCount(&a.Total, *row.Count); err != nil {
			return nil, err
		}
		if err := addOverviewCount(&counts[group].Count, *row.Count); err != nil {
			return nil, err
		}
		bucket := values[epoch]
		if err := addOverviewCount(&bucket[group], *row.Count); err != nil {
			return nil, err
		}
		values[epoch] = bucket
	}
	for status := range unknown {
		counts[4].Statuses = append(counts[4].Statuses, status)
	}
	sort.Strings(counts[4].Statuses)
	for i := range counts {
		counts[i].URL = overviewDrilldown(app, counts[i].Statuses, window.Start, window.End, "")
	}
	if a.MissingStatus || len(counts[4].Statuses) == 0 {
		counts[4].URL = ""
	}
	counts[4].Missing = a.MissingStatus
	for epoch := first; epoch <= last; epoch += window.BucketMS {
		bucketStart := time.UnixMilli(epoch).UTC()
		bucketEnd := time.UnixMilli(epoch + window.BucketMS - 1).UTC()
		if bucketStart.Before(start) {
			bucketStart = start
		}
		if bucketEnd.After(end) {
			bucketEnd = end
		}
		b := overviewBucket{Label: bucketStart.Format("Jan 02 15:04"), Start: bucketStart.Format(overviewTimeFormat), End: bucketEnd.Format(overviewTimeFormat)}
		for _, count := range values[epoch] {
			b.Count += count
		}
		if b.Count > a.Peak {
			a.Peak = b.Count
		}
		for i, count := range values[epoch] {
			if count == 0 {
				continue
			}
			group := counts[i]
			link := overviewDrilldown(app, group.Statuses, b.Start, b.End, "")
			if group.Missing {
				link = ""
			}
			b.Segments = append(b.Segments, overviewSegment{Key: group.Key, Label: group.Label, Count: count, URL: link})
		}
		a.Buckets = append(a.Buckets, b)
	}
	for i := range a.Buckets {
		for j := range a.Buckets[i].Segments {
			height := float64(a.Buckets[i].Segments[j].Count) / float64(a.Peak) * 100
			a.Buckets[i].Segments[j].Height = strconv.FormatFloat(height, 'f', 3, 64) + "%"
		}
	}
	return a, nil
}
func buildOverviewWorkload(app string, rows []protocol.WorkflowAggregate) (*overviewWorkload, error) {
	result := &overviewWorkload{URL: overviewDrilldown(app, overviewUnfinished, "", "", ""), QueuedURL: overviewDrilldown(app, []string{"ENQUEUED", "DELAYED"}, "", "", "")}
	counts := []overviewCount{{Key: "progress", Label: "Pending", Statuses: []string{"PENDING"}}, {Key: "progress", Label: "Enqueued", Statuses: []string{"ENQUEUED"}}, {Key: "progress", Label: "Delayed", Statuses: []string{"DELAYED"}}}
	queues := map[string]*overviewQueue{}
	for _, row := range rows {
		if row.Count == nil || *row.Count < 0 {
			return nil, errors.New("aggregate count unavailable")
		}
		status := row.Group["status"]
		queue, hasQueue := row.Group["queue_name"]
		if status == nil || !hasQueue {
			return nil, errors.New("workload status or queue unavailable")
		}
		index := -1
		for i, c := range counts {
			if c.Statuses[0] == *status {
				index = i
			}
		}
		if index < 0 {
			return nil, errors.New("workload aggregate did not preserve unfinished filter")
		}
		if err := addOverviewCount(&result.Total, *row.Count); err != nil {
			return nil, err
		}
		counts[index].Count += *row.Count
		if index > 0 {
			result.Queued += *row.Count
		}
		if queue != nil {
			q := queues[*queue]
			if q == nil {
				q = &overviewQueue{DetailsURL: queueDetailURL(app, *queue), Name: *queue, URL: overviewDrilldown(app, overviewUnfinished, "", "", *queue)}
				if *queue == "" {
					q.Name = "Empty queue name"
					q.URL = ""
					q.DetailsURL = ""
				}
				queues[*queue] = q
			}
			q.Total += *row.Count
			switch index {
			case 0:
				q.Pending += *row.Count
			case 1:
				q.Enqueued += *row.Count
			case 2:
				q.Delayed += *row.Count
			}
		}
	}
	for i := range counts {
		counts[i].URL = overviewDrilldown(app, counts[i].Statuses, "", "", "")
	}
	result.Counts = counts
	for _, q := range queues {
		result.Queues = append(result.Queues, *q)
	}
	sort.Slice(result.Queues, func(i, j int) bool { return result.Queues[i].Name < result.Queues[j].Name })
	return result, nil
}

func (d overviewPanel) SchedulesURL() string {
	return applicationPath(d.App) + "/schedules?status=ACTIVE"
}

func overviewCreatedUTC(value *string) string {
	if value == nil {
		return "Unknown"
	}
	if epoch, err := strconv.ParseInt(*value, 10, 64); err == nil {
		return time.UnixMilli(epoch).UTC().Format(overviewTimeFormat)
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, *value); err == nil {
		return timestamp.UTC().Format(overviewTimeFormat)
	}
	return "Unknown"
}
