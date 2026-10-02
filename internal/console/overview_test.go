package console

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
)

func overviewTestRow(epoch int64, status *string, count *int64) protocol.WorkflowAggregate {
	bucket := strconv.FormatInt(epoch, 10)
	return protocol.WorkflowAggregate{Group: map[string]*string{"time_bucket": &bucket, "status": status}, Count: count}
}

func overviewTestString(s string) *string { return &s }

func overviewTestCount(i int64) *int64 { return &i }

func TestApplicationOverviewActivityBucketsAndDrilldowns(t *testing.T) {
	win, _ := newOverviewWindow("24h", time.Date(2026, 10, 1, 12, 34, 56, 0, time.UTC))
	start, _ := time.Parse(overviewTimeFormat, win.Start)
	epoch := start.UnixMilli() / win.BucketMS * win.BucketMS
	rows := []protocol.WorkflowAggregate{overviewTestRow(epoch, overviewTestString("SUCCESS"), overviewTestCount(5)), overviewTestRow(epoch, overviewTestString("ERROR"), overviewTestCount(2)), overviewTestRow(epoch, overviewTestString("MAX_RECOVERY_ATTEMPTS_EXCEEDED"), overviewTestCount(3)), overviewTestRow(epoch+win.BucketMS, overviewTestString("CANCELLED"), overviewTestCount(1)), overviewTestRow(epoch+win.BucketMS, overviewTestString("NEW_STATUS"), overviewTestCount(4))}
	a, err := buildOverviewActivity("a/b", win, rows)
	if err != nil {
		t.Fatal(err)
	}
	if a.Total != 15 || a.Counts[1].Count != 5 || a.Counts[3].Count != 1 || a.Counts[4].Count != 4 || len(a.Buckets) != 25 {
		t.Fatalf("incorrect summary: %#v", a)
	}
	for i, bucket := range a.Buckets {
		bstart, _ := time.Parse(overviewTimeFormat, bucket.Start)
		bend, _ := time.Parse(overviewTimeFormat, bucket.End)
		if i == 0 && bucket.Start != win.Start {
			t.Fatal("partial first bucket lost")
		}
		if i > 0 {
			prev, _ := time.Parse(overviewTimeFormat, a.Buckets[i-1].End)
			if bstart.Sub(prev) != time.Millisecond {
				t.Fatal("inclusive buckets overlap or have gaps")
			}
		}
		if bend.Before(bstart) {
			t.Fatal("negative bucket")
		}
	}
	u, _ := url.Parse(a.Counts[1].URL)
	q := u.Query()
	if len(q["status"]) != 2 || q["status"][0] != "ERROR" || q["status"][1] != "MAX_RECOVERY_ATTEMPTS_EXCEEDED" || q.Get("start_time") != win.Start || q.Get("end_time") != win.End {
		t.Fatal(q)
	}
	u, _ = url.Parse(a.Counts[4].URL)
	if u.Query().Get("status") != "NEW_STATUS" {
		t.Fatal("unknown status lost")
	}
	rows = append(rows, overviewTestRow(epoch, nil, overviewTestCount(7)))
	a, err = buildOverviewActivity("a", win, rows)
	if err != nil || !a.MissingStatus || a.Counts[4].URL != "" || a.Total != 22 {
		t.Fatal("missing status broadened drilldown or lost count")
	}
}

func TestApplicationOverviewRejectsMissingCountsAndBuckets(t *testing.T) {
	win, _ := newOverviewWindow("24h", time.Now())
	start, _ := time.Parse(overviewTimeFormat, win.Start)
	epoch := start.UnixMilli() / win.BucketMS * win.BucketMS
	for _, row := range []protocol.WorkflowAggregate{overviewTestRow(epoch, nil, nil), overviewTestRow(epoch, nil, overviewTestCount(-1)), {Group: map[string]*string{"status": nil}, Count: overviewTestCount(3)}, overviewTestRow(epoch-1, nil, overviewTestCount(3))} {
		if _, err := buildOverviewActivity("a", win, []protocol.WorkflowAggregate{row}); err == nil {
			t.Fatal("invalid measure became zero")
		}
	}
	empty, err := buildOverviewActivity("a", win, nil)
	if err != nil || empty.Total != 0 || len(empty.Buckets) != 25 {
		t.Fatal("successful empty response not zero filled")
	}
	for _, r := range []string{"24h", "7d", "30d"} {
		win, _ := newOverviewWindow(r, time.Now())
		a, err := buildOverviewActivity("a", win, nil)
		if err != nil || len(a.Buckets) > 61 {
			t.Fatal("bucket budget exceeded")
		}
	}
}

func TestApplicationOverviewWorkloadQueueSubset(t *testing.T) {
	rows := []protocol.WorkflowAggregate{{Group: map[string]*string{"status": overviewTestString("PENDING"), "queue_name": nil}, Count: overviewTestCount(8)}, {Group: map[string]*string{"status": overviewTestString("PENDING"), "queue_name": overviewTestString("jobs")}, Count: overviewTestCount(2)}, {Group: map[string]*string{"status": overviewTestString("ENQUEUED"), "queue_name": overviewTestString("jobs")}, Count: overviewTestCount(3)}, {Group: map[string]*string{"status": overviewTestString("DELAYED"), "queue_name": overviewTestString("jobs")}, Count: overviewTestCount(4)}}
	data, err := buildOverviewWorkload("a", rows)
	if err != nil {
		t.Fatal(err)
	}
	if data.Total != 17 || data.Queued != 7 || len(data.Queues) != 1 || data.Queues[0].Total != 9 {
		t.Fatal(data)
	}
	for _, link := range []string{data.URL, data.QueuedURL, data.Queues[0].URL} {
		u, _ := url.Parse(link)
		if u.Query().Get("start_time") != "" || u.Query().Get("end_time") != "" {
			t.Fatal("workload drilldown has creation window")
		}
	}
	u, _ := url.Parse(data.Queues[0].URL)
	if u.Query().Get("queue") != "jobs" {
		t.Fatal("queue lost")
	}
}

func TestApplicationOverviewCacheCoalescesAndRetainsStale(t *testing.T) {
	var cache overviewCache
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	loader := func() (overviewPanel, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return overviewPanel{ScheduleCount: 3000}, nil
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := cache.read(context.Background(), "a", time.Minute, true, loader)
			if err != nil || data.ScheduleCount != 3000 {
				t.Error(data, err)
			}
		}()
	}
	<-started
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("reads not coalesced")
	}
	data, err := cache.read(context.Background(), "a", time.Minute, false, loader)
	if !errors.Is(err, hub.ErrAppUnavailable) || !data.Loaded || data.LastSuccess == "" || data.ScheduleCount != 3000 || calls.Load() != 1 {
		t.Fatal("disconnect hid retained data")
	}
	cache.mu.Lock()
	cache.entries["a"].attempt = time.Now().Add(-time.Hour)
	cache.mu.Unlock()
	data, err = cache.read(context.Background(), "a", time.Minute, true, func() (overviewPanel, error) { return overviewPanel{}, errors.New("unsupported") })
	if err == nil || data.ScheduleCount != 3000 {
		t.Fatal("unsupported refresh hid stale data")
	}
	for i := 0; i < 150; i++ {
		_, _ = cache.read(context.Background(), strconv.Itoa(i), time.Minute, true, func() (overviewPanel, error) { return overviewPanel{}, nil })
	}
	if len(cache.entries) > 128 {
		t.Fatal("cache unbounded")
	}
}

func TestApplicationOverviewQueryValidation(t *testing.T) {
	for _, q := range []string{"?range=forever", "?range=24h&start_time=bad", "?range=24h&range=7d", "?range=24h&unexpected=x", "?start_time=2026-01-01T00:00:00.000Z&end_time=2026-03-01T00:00:00.000Z"} {
		if _, err := parseOverviewWindow(httptest.NewRequest("GET", "/"+q, nil)); err == nil {
			t.Fatal("invalid query accepted", q)
		}
	}
}

func TestApplicationOverviewUnfilterableEmptyValues(t *testing.T) {
	win, _ := newOverviewWindow("24h", time.Now())
	start, _ := time.Parse(overviewTimeFormat, win.Start)
	epoch := start.UnixMilli() / win.BucketMS * win.BucketMS
	a, err := buildOverviewActivity("app", win, []protocol.WorkflowAggregate{overviewTestRow(epoch, overviewTestString(""), overviewTestCount(1))})
	if err != nil || !a.MissingStatus || a.Counts[4].URL != "" || a.Buckets[0].Segments[0].URL != "" {
		t.Fatal("empty raw status broadened drilldown", a, err)
	}
	workload, err := buildOverviewWorkload("app", []protocol.WorkflowAggregate{{Group: map[string]*string{"status": overviewTestString("ENQUEUED"), "queue_name": overviewTestString("")}, Count: overviewTestCount(2)}})
	if err != nil || workload.Queues[0].URL != "" || workload.Queues[0].DetailsURL != "" {
		t.Fatal("empty queue broadened drilldown", workload, err)
	}
	workload, err = buildOverviewWorkload("app", []protocol.WorkflowAggregate{{Group: map[string]*string{"status": overviewTestString("PENDING"), "queue_name": overviewTestString("..")}, Count: overviewTestCount(2)}})
	if err != nil || workload.Queues[0].DetailsURL != "/apps/app/queue?name=.." {
		t.Fatal("dot queue detail lost identity", workload, err)
	}
}

func TestApplicationOverviewCacheUnavailableAndCancelledDuringRefresh(t *testing.T) {
	retained := overviewPanel{Loaded: true, LastSuccess: "yesterday", ScheduleCount: 3000}
	pending := make(chan struct{})
	cache := overviewCache{entries: map[string]*overviewCacheEntry{"a": {data: retained, pending: pending}}}
	loader := func() (overviewPanel, error) { t.Fatal("unexpected load"); return overviewPanel{}, nil }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	data, err := cache.read(cancelled, "a", time.Minute, false, loader)
	if !errors.Is(err, hub.ErrAppUnavailable) || data.ScheduleCount != 3000 {
		t.Fatal("disconnect blocked behind inflight read", data, err)
	}
	data, err = cache.read(cancelled, "a", time.Minute, true, loader)
	if !errors.Is(err, context.Canceled) || data.ScheduleCount != 3000 {
		t.Fatal("cancelled waiter lost retained read", data, err)
	}
}
