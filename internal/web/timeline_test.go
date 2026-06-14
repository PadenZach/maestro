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
