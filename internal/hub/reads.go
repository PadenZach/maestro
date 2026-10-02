package hub

import (
	"context"

	"github.com/PadenZach/maestro/internal/protocol"
)

// Call sends req to a healthy executor of app, decodes the reply into out,
// and returns any transport or executor-side error.
func (h *Hub) Call(ctx context.Context, app string, req protocol.Request, out protocol.Response) error {
	raw, err := h.Request(ctx, app, req)
	if err != nil {
		return err
	}
	return protocol.DecodeResponse(raw, out)
}

// Shared read operations also serve the existing UI/API; only the HTTP representation differs.
func (h *Hub) Workflow(ctx context.Context, app, id string, loadInput, loadOutput bool) (*protocol.WorkflowsOutput, error) {
	var resp protocol.GetWorkflowResponse
	if err := h.Call(ctx, app, protocol.GetWorkflowRequest(id, loadInput, loadOutput), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}

func (h *Hub) Steps(ctx context.Context, app, id string, loadOutput bool, limit, offset *int) ([]protocol.WorkflowSteps, error) {
	var resp protocol.ListStepsResponse
	if err := h.Call(ctx, app, protocol.ListStepsRequest(id, loadOutput, limit, offset), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}

func (h *Hub) Workflows(ctx context.Context, app string, b protocol.ListWorkflowsBody) ([]protocol.WorkflowsOutput, error) {
	var resp protocol.ListWorkflowsResponse
	if err := h.Call(ctx, app, protocol.ListWorkflowsRequest(b), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}
