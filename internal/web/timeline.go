package web

import (
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/zpaden/maestro/internal/protocol"
)

// funcMap is the template helper set shared by every page/partial.
func funcMap() template.FuncMap {
	return template.FuncMap{
		"deref":          deref,
		"derefOr":        derefOr,
		"formatDuration": FormatDuration,
		"statusClass":    statusClass,
		"truncate":       truncate,
		"workflowURL":    WorkflowURL,
		"opaqueValue": func(value string) string {
			if value == "" {
				return `""`
			}
			return value
		},
		"nullableValue": func(value *string) string { return field("", value).Value },
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefOr(s *string, def string) string {
	if s == nil || *s == "" {
		return def
	}
	return *s
}

func truncate(n int, s string) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// statusClass maps a DBOS workflow/step status to a CSS modifier used for the
// colored badge and timeline bar.
func statusClass(status string) string {
	switch strings.ToUpper(status) {
	case "SUCCESS":
		return "ok"
	case "ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "CANCELLED":
		return "err"
	case "PENDING", "ENQUEUED", "DELAYED":
		return "running"
	default:
		return "muted"
	}
}

// FormatDuration renders a millisecond span the way the DBOS console does:
// sub-second as "Nms", seconds as "Ns", minutes as "Nm Ms", hours as "Nh Mm".
func FormatDuration(ms int64) string {
	if ms < 0 {
		ms = 0
	}
	switch {
	case ms < 1000:
		return fmt.Sprintf("%dms", ms)
	case ms < 60_000:
		return fmt.Sprintf("%ds", ms/1000)
	case ms < 3_600_000:
		return fmt.Sprintf("%dm %ds", ms/60_000, (ms%60_000)/1000)
	default:
		return fmt.Sprintf("%dh %dm", ms/3_600_000, (ms%3_600_000)/60_000)
	}
}

// Timeline is the precomputed view model for the gantt step visualizer: a time
// window [T0,T1], axis ticks, and one bar per step. Geometry is computed here
// (not in templates) so it is unit-testable.
type Timeline struct {
	ChildStatus string
	Depth       int
	StartMS     int64
	EndMS       int64
	Branch      string
	WorkflowID  string
	App         string
	HasWindow   bool // false when no step has timing data; rows render bar-less
	Ticks       []Tick
	Rows        []StepRow
}

// Tick is one axis label, positioned by percentage across the window.
type Tick struct {
	Label   string
	LeftPct float64
}

// StepRow is one step's bar within the timeline.
type StepRow struct {
	Key             string
	StepID          string
	ChildURL        string
	ChildCycle      bool
	FunctionID      int
	Name            string
	StatusClass     string // "ok" | "err" | "running" | "muted"
	HasError        bool
	HasOutput       bool
	ChildWorkflowID string // "" when the step is not a sub-workflow call
	HasBar          bool
	LeftPct         float64
	WidthPct        float64
	Duration        string
}

const minBarPct = 0.6 // keep instantaneous steps visible

// BuildTimeline composes the step list into the gantt view model. The window is
// the span from the earliest step start to the latest step completion; steps
// still running have no completion and render as muted bars to "now"-less ends.
func BuildTimeline(app, workflowID string, steps []protocol.WorkflowSteps) Timeline {
	return buildTimeline(app, workflowID, steps, nil)
}

// BuildTimelineInWindow aligns a lazy child fragment with its root's axis.
// An empty window stays empty, rather than inventing a child-local time scale.
func BuildTimelineInWindow(app, workflowID string, steps []protocol.WorkflowSteps, start, end int64) Timeline {
	return buildTimeline(app, workflowID, steps, &[2]int64{start, end})
}

func buildTimeline(app, workflowID string, steps []protocol.WorkflowSteps, window *[2]int64) Timeline {
	tl := Timeline{App: app, WorkflowID: workflowID}

	var t0, t1 int64
	haveT0 := false
	for _, s := range steps {
		if start, ok := parseEpoch(s.StartedAtEpochMS); ok {
			if !haveT0 || start < t0 {
				t0, haveT0 = start, true
			}
			if start > t1 {
				t1 = start
			}
		}
		if end, ok := parseEpoch(s.CompletedAtEpochMS); ok && end > t1 {
			t1 = end
		}
	}
	if window != nil {
		t0, t1, haveT0 = window[0], window[1], window[1] > window[0]
	}
	tl.StartMS, tl.EndMS = t0, t1
	tl.HasWindow = haveT0 && t1 > t0
	span := float64(t1 - t0)

	for _, s := range steps {
		row := StepRow{
			FunctionID:      s.FunctionID,
			StepID:          stepID(s),
			Name:            s.FunctionName,
			StatusClass:     "ok",
			HasError:        s.Error != nil && *s.Error != "",
			HasOutput:       s.Output != nil && *s.Output != "",
			ChildWorkflowID: deref(s.ChildWorkflowID),
		}
		if row.HasError {
			row.StatusClass = "err"
		}

		start, hasStart := parseEpoch(s.StartedAtEpochMS)
		end, hasEnd := parseEpoch(s.CompletedAtEpochMS)
		if hasStart && hasEnd {
			row.Duration = FormatDuration(end - start)
		} else if hasStart && !hasEnd {
			row.StatusClass = "running"
		}

		if tl.HasWindow && hasStart {
			barEnd := end
			if !hasEnd {
				barEnd = t1
			}
			// Only draw the intersection with the shared axis. The duration
			// column retains the full measured duration, including clipped time.
			if start <= t1 && barEnd >= t0 {
				row.HasBar = true
				row.LeftPct = clampPct(float64(start-t0) / span * 100)
				right := clampPct(float64(barEnd-t0) / span * 100)
				row.WidthPct = min(100-row.LeftPct, max(minBarPct, right-row.LeftPct))
			}
		}
		tl.Rows = append(tl.Rows, row)
	}

	if tl.HasWindow {
		tl.Ticks = buildTicks(t1 - t0)
	}
	SetTimelineBranch(&tl, "", nil)
	return tl
}

// buildTicks produces ~5 evenly-spaced "+Ns" axis labels across the window.
func buildTicks(spanMS int64) []Tick {
	const n = 5
	ticks := make([]Tick, 0, n)
	for i := 0; i < n; i++ {
		frac := float64(i) / float64(n-1)
		offsetMS := int64(frac * float64(spanMS))
		ticks = append(ticks, Tick{
			Label:   "+" + FormatDuration(offsetMS),
			LeftPct: frac * 100,
		})
	}
	return ticks
}

func parseEpoch(s *string) (int64, bool) {
	if s == nil || *s == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(*s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func clampPct(p float64) float64 {
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}
