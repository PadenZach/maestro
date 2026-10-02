package api

import (
	"context"
	"errors"
	"net/url"
	"strconv"

	"github.com/zpaden/maestro/internal/protocol"
	"github.com/zpaden/maestro/internal/web"
)

func (s *Server) readTimelinePage(ctx context.Context, app, id string, offset int) ([]protocol.WorkflowSteps, bool, error) {
	limit := FlowStepPageSize + 1
	steps, err := s.readSteps(ctx, app, id, true, &limit, &offset)
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

func setTimelinePage(tl *web.Timeline, offset int, more bool, branch string, ancestors []string) {
	tl.PageOffset = offset
	tl.Limited = more && offset >= flowMaxOffset
	if !more || tl.Limited {
		return
	}
	q := url.Values{"offset": {strconv.Itoa(offset + FlowStepPageSize)}, "branch": {branch}, "ancestor": ancestors,
		"window_start": {strconv.FormatInt(tl.StartMS, 10)}, "window_end": {strconv.FormatInt(tl.EndMS, 10)}}
	tl.MoreURL = web.WorkflowURL(tl.App, tl.WorkflowID) + "/timeline?" + q.Encode()
}
