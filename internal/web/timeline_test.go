package web

import (
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func sp(s string) *string { return &s }

func TestBuildTimeline_Geometry(t *testing.T) {
	steps := []protocol.WorkflowSteps{
		// Window is [1000, 3000]; this step starts at the left edge, half-width.
		{FunctionID: 0, FunctionName: "a", StartedAtEpochMS: sp("1000"), CompletedAtEpochMS: sp("2000")},
		// Second half.
		{FunctionID: 1, FunctionName: "b", StartedAtEpochMS: sp("2000"), CompletedAtEpochMS: sp("3000")},
	}
	tl := BuildTimeline("app", "wf", steps)
	if !tl.HasWindow {
		t.Fatal("expected a window")
	}
	if len(tl.Rows) != 2 {
		t.Fatalf("rows = %d", len(tl.Rows))
	}
	if tl.Rows[0].LeftPct != 0 || !approx(tl.Rows[0].WidthPct, 50) {
		t.Fatalf("row0 geometry: left=%v width=%v", tl.Rows[0].LeftPct, tl.Rows[0].WidthPct)
	}
	if !approx(tl.Rows[1].LeftPct, 50) || !approx(tl.Rows[1].WidthPct, 50) {
		t.Fatalf("row1 geometry: left=%v width=%v", tl.Rows[1].LeftPct, tl.Rows[1].WidthPct)
	}
	if tl.Rows[0].Duration != "1s" {
		t.Fatalf("duration = %q, want 1s", tl.Rows[0].Duration)
	}
}

func TestBuildTimeline_RunningStep(t *testing.T) {
	steps := []protocol.WorkflowSteps{
		{FunctionID: 0, FunctionName: "a", StartedAtEpochMS: sp("1000"), CompletedAtEpochMS: sp("2000")},
		{FunctionID: 1, FunctionName: "running", StartedAtEpochMS: sp("1500")}, // no completion
	}
	tl := BuildTimeline("app", "wf", steps)
	if tl.Rows[1].StatusClass != "running" {
		t.Fatalf("expected running status, got %q", tl.Rows[1].StatusClass)
	}
}

func TestBuildTimeline_NoTiming(t *testing.T) {
	steps := []protocol.WorkflowSteps{{FunctionID: 0, FunctionName: "a"}}
	tl := BuildTimeline("app", "wf", steps)
	if tl.HasWindow {
		t.Fatal("no timing should yield no window")
	}
	if len(tl.Rows) != 1 || tl.Rows[0].HasBar {
		t.Fatal("bar-less row expected")
	}
}

func TestBuildTimelineSharedWindowBounds(t *testing.T) {
	steps := []protocol.WorkflowSteps{
		{FunctionID: 1, StartedAtEpochMS: sp("500"), CompletedAtEpochMS: sp("2000")},
		{FunctionID: 2, StartedAtEpochMS: sp("4000"), CompletedAtEpochMS: sp("6000")},
		{FunctionID: 3, StartedAtEpochMS: sp("7000"), CompletedAtEpochMS: sp("8000")},
		{FunctionID: 4, StartedAtEpochMS: sp("2000")},
	}
	tl := BuildTimelineInWindow("app", "child", steps, 1000, 5000)
	if !approx(tl.Rows[0].LeftPct, 0) || !approx(tl.Rows[0].WidthPct, 25) || tl.Rows[0].Duration != "1s" {
		t.Fatalf("left clipping changed real duration or geometry: %+v", tl.Rows[0])
	}
	if !approx(tl.Rows[1].LeftPct, 75) || !approx(tl.Rows[1].WidthPct, 25) || tl.Rows[1].Duration != "2s" {
		t.Fatalf("right clipping changed real duration or geometry: %+v", tl.Rows[1])
	}
	if tl.Rows[2].HasBar {
		t.Fatal("out-of-window child must not be drawn at a false time")
	}
	if !approx(tl.Rows[3].LeftPct, 25) || !approx(tl.Rows[3].WidthPct, 75) || tl.Rows[3].StatusClass != "running" {
		t.Fatalf("running child must use shared right edge: %+v", tl.Rows[3])
	}
	unknown := BuildTimelineInWindow("app", "child", steps, 0, 0)
	if unknown.HasWindow {
		t.Fatal("unknown root window must not become a child-local axis")
	}
	for _, row := range unknown.Rows {
		if row.HasBar {
			t.Fatal("a shared unknown window must keep descendants bar-less")
		}
	}
}

func TestFormatDuration(t *testing.T) {
	cases := map[int64]string{1: "1ms", 522: "522ms", 35000: "35s", 182000: "3m 2s"}
	for ms, want := range cases {
		if got := FormatDuration(ms); got != want {
			t.Errorf("FormatDuration(%d) = %q, want %q", ms, got, want)
		}
	}
}

func approx(a, b float64) bool {
	d := a - b
	return d < 0.01 && d > -0.01
}
