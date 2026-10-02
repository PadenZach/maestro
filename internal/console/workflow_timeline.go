package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/PadenZach/maestro/internal/protocol"
)

func (s *handler) readTimelinePage(ctx context.Context, app, id string, offset int) ([]protocol.WorkflowSteps, bool, error) {
	limit := FlowStepPageSize + 1
	steps, err := s.hub.Steps(ctx, app, id, true, &limit, &offset)
	if err != nil {
		return nil, false, err
	}
	if steps == nil {
		return nil, false, errors.New("recorded steps are unavailable")
	}
	if len(steps) > limit {
		return nil, false, errors.New("executor exceeded the step page limit")
	}
	more := len(steps) > FlowStepPageSize
	if more {
		steps = steps[:FlowStepPageSize]
	}
	return steps, more, nil
}

func setTimelinePage(tl *Timeline, offset int, more bool, branch string, ancestors []string) {
	tl.PageOffset = offset
	tl.Limited = more && offset >= flowMaxOffset
	if !more || tl.Limited {
		return
	}
	q := url.Values{"offset": {strconv.Itoa(offset + FlowStepPageSize)}, "branch": {branch}, "ancestor": ancestors,
		"window_start": {strconv.FormatInt(tl.StartMS, 10)}, "window_end": {strconv.FormatInt(tl.EndMS, 10)}}
	tl.MoreURL = WorkflowURL(tl.App, tl.WorkflowID) + "/timeline?" + q.Encode()
}

func (s *handler) handleWorkflowTimeline(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	query := r.URL.Query()
	offset, offsetErr := flowOffset(query)
	if offsetErr != nil {
		partialError(w, offsetErr)
		return
	}
	if len(query["ancestor"]) > 8 {
		partialMessage(w, "Timeline depth limit reached. Open this workflow to continue.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), flowReadTimeout)
	defer cancel()
	starts, hasStart := query["window_start"]
	ends, hasEnd := query["window_end"]
	var start, end int64
	if hasStart || hasEnd {
		if len(starts) != 1 || len(ends) != 1 {
			partialError(w, fmt.Errorf("invalid timeline window"))
			return
		}
		var startErr, endErr error
		start, startErr = strconv.ParseInt(starts[0], 10, 64)
		end, endErr = strconv.ParseInt(ends[0], 10, 64)
		if startErr != nil || endErr != nil || start < 0 || end < start {
			partialError(w, fmt.Errorf("invalid timeline window"))
			return
		}
	}
	wf, err := s.hub.Workflow(ctx, app, id, false, false)
	if err != nil {
		partialError(w, err)
		return
	}
	if wf == nil {
		partialError(w, fmt.Errorf("child workflow %q not found", id))
		return
	}
	ancestors := query["ancestor"]
	for _, ancestor := range ancestors {
		if ancestor == id {
			partialMessage(w, fmt.Sprintf("Workflow relationship cycle at %q", id))
			return
		}
	}
	steps, more, err := s.readTimelinePage(ctx, app, id, offset)
	if err != nil {
		partialError(w, err)
		return
	}
	var tl Timeline
	if hasStart {
		tl = BuildTimelineInWindow(app, id, steps, start, end)
	} else {
		tl = BuildTimeline(app, id, steps)
	}
	tl.ChildStatus = deref(wf.Status)
	SetTimelineBranch(&tl, query.Get("branch"), ancestors)
	setTimelinePage(&tl, offset, more, query.Get("branch"), ancestors)
	s.web.partial(w, "timeline_rows", tl)
}
