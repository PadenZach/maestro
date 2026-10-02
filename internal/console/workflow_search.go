package console

import (
	"context"
	"strings"
	"time"

	"github.com/PadenZach/maestro/internal/protocol"
)

// fetchConsoleRows applies literal name matching before Console pagination.
// Released SDK name filters are exact (Python 3.1.0 _sys_db.list_workflows),
// so candidate reads retain every other filter and never request user blobs.
// Each read is bounded; the request-wide deadline bounds sparse scans without
// silently claiming that a partially scanned result set is complete.
func (s *handler) fetchConsoleRows(ctx context.Context, app string, f filterState, offset int) (workflowRows, error) {
	f.Name = strings.TrimSpace(f.Name)
	if offset < 0 {
		offset = 0
	}
	if f.Name == "" {
		return s.fetchRows(ctx, app, f, offset)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	query := strings.ToLower(f.Name)
	candidateLimit := defaultPageSize + 1
	candidateOffset := 0
	body := protocol.ListWorkflowsBody{SortDesc: true, Limit: &candidateLimit, Offset: &candidateOffset}
	body.Status = f.Statuses
	body.StartTime, body.EndTime = f.StartTime, f.EndTime
	body.HasParent = f.HasParent
	if f.Queue != "" {
		body.QueueName = []string{f.Queue}
	}
	if f.IDPrefix != "" {
		body.WorkflowIDPrefix = []string{f.IDPrefix}
	}
	matches := make([]protocol.WorkflowsOutput, 0, defaultPageSize+1)
	skipped := 0
	for {
		if err := ctx.Err(); err != nil {
			return workflowRows{}, err
		}
		candidates, err := s.hub.Workflows(ctx, app, body)
		if err != nil {
			return workflowRows{}, err
		}
		for _, wf := range candidates {
			if wf.WorkflowName == nil || !strings.Contains(strings.ToLower(*wf.WorkflowName), query) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			matches = append(matches, wf)
			if len(matches) > defaultPageSize {
				break
			}
		}
		if len(matches) > defaultPageSize || len(candidates) < candidateLimit {
			break
		}
		candidateOffset += len(candidates)
	}
	return paginateRows(app, f, offset, matches), nil
}
