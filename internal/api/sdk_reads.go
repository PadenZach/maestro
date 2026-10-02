package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/danielgtaylor/huma/v2"
)

// The /api reads retain executor DTOs and tolerant decoding. The /v2 endpoints
// map these into the Conductor HTTP representation.
type jsonOutput[T any] struct {
	ContentType string `header:"Content-Type"`
	Body        T      `nullable:"false"`
}

type jsonNullableOutput[T any] struct {
	ContentType string              `header:"Content-Type"`
	Body        jsonNullableBody[T] `doc:"The executor's local JSON result. Explicit null remains a successful 200 response; an empty array remains []."`
}

// Huma cannot apply a nullable field tag to a referenced object schema. This
// wrapper supplies an inline nullable schema and serializes only the typed value.
type jsonNullableBody[T any] struct {
	Value T
}

func (body jsonNullableBody[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(body.Value)
}

func (jsonNullableBody[T]) Schema(registry huma.Registry) *huma.Schema {
	schema := *registry.Schema(reflect.TypeFor[T](), false, "")
	schema.Nullable = true
	return &schema
}

func jsonResult[T any](body T) *jsonOutput[T] {
	return &jsonOutput[T]{ContentType: "application/json", Body: body}
}

func jsonNullableResult[T any](body T) *jsonNullableOutput[T] {
	// Return a nonnil response even for a nil body: local reads return JSON null.
	return &jsonNullableOutput[T]{ContentType: "application/json", Body: jsonNullableBody[T]{Value: body}}
}

type jsonAppInput struct {
	App string `path:"app" doc:"Application name."`
}

type jsonWorkflowInput struct {
	App string `path:"app" doc:"Application name."`
	ID  string `path:"id" doc:"Workflow ID."`
}

type jsonQueueInput struct {
	App  string `path:"app" doc:"Application name."`
	Name string `path:"name" doc:"Queue name."`
}

type jsonWorkflowListInput struct {
	App      string `path:"app" doc:"Application name."`
	Status   string `query:"status" doc:"Exact workflow status filter; an empty value omits the filter."`
	Name     string `query:"name" doc:"Exact workflow name filter; an empty value omits the filter."`
	IDPrefix string `query:"id_prefix" doc:"Workflow ID prefix filter; an empty value omits the filter."`
	Queue    string `query:"queue" doc:"Exact queue name filter; an empty value omits the filter."`
	Offset   string `query:"offset" doc:"Pagination offset parsed as a Go integer; defaults to zero when absent."`
}

// Resolve retains net/url parsing semantics, including bare parameters and
// malformed/repeated entries, while the field tags generate query documentation.
func (input *jsonWorkflowListInput) Resolve(ctx huma.Context) []error {
	u := ctx.URL()
	query := u.Query()
	input.Status = query.Get("status")
	input.Name = query.Get("name")
	input.IDPrefix = query.Get("id_prefix")
	input.Queue = query.Get("queue")
	input.Offset = query.Get("offset")
	return nil
}

// jsonAPIError preserves the local single-key error body rather than adopting
// the official HTTP adapter's RFC 9457 problem representation.
type jsonAPIError struct {
	Message string `json:"error"`
	status  int
}

func (failure *jsonAPIError) Error() string { return failure.Message }

func (failure *jsonAPIError) GetStatus() int { return failure.status }

func (failure *jsonAPIError) ContentType(string) string { return "application/json" }

func jsonFailure(err error) error {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	return &jsonAPIError{Message: err.Error(), status: status}
}

func (s *handler) jsonWorkflows(ctx context.Context, input *jsonWorkflowListInput) (*jsonNullableOutput[[]protocol.WorkflowsOutput], error) {
	offset, _ := strconv.Atoi(input.Offset)
	const pageSize = 25
	limit := pageSize + 1
	body := protocol.ListWorkflowsBody{SortDesc: true, Limit: &limit, Offset: &offset}
	if input.Status != "" {
		body.Status = []string{input.Status}
	}
	if input.Name != "" {
		body.WorkflowName = []string{input.Name}
	}
	if input.IDPrefix != "" {
		body.WorkflowIDPrefix = []string{input.IDPrefix}
	}
	if input.Queue != "" {
		body.QueueName = []string{input.Queue}
	}
	rows, err := s.hub.Workflows(ctx, input.App, body)
	if err != nil {
		return nil, jsonFailure(err)
	}
	if len(rows) > pageSize {
		rows = rows[:pageSize]
	}
	return jsonNullableResult(rows), nil
}

func (s *handler) jsonWorkflow(ctx context.Context, input *jsonWorkflowInput) (*jsonNullableOutput[*protocol.WorkflowsOutput], error) {
	workflow, err := s.hub.Workflow(ctx, input.App, input.ID, true, true)
	if err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(workflow), nil
}

func (s *handler) jsonSteps(ctx context.Context, input *jsonWorkflowInput) (*jsonNullableOutput[[]protocol.WorkflowSteps], error) {
	steps, err := s.hub.Steps(ctx, input.App, input.ID, true, nil, nil)
	if err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(steps), nil
}

func (s *handler) jsonEvents(ctx context.Context, input *jsonWorkflowInput) (*jsonNullableOutput[[]protocol.EventOutput], error) {
	var response protocol.GetWorkflowEventsResponse
	if err := s.hub.Call(ctx, input.App, protocol.GetWorkflowEventsRequest(input.ID), &response); err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(response.Events), nil
}

func (s *handler) jsonNotifications(ctx context.Context, input *jsonWorkflowInput) (*jsonNullableOutput[[]protocol.NotificationOutput], error) {
	var response protocol.GetWorkflowNotificationsResponse
	if err := s.hub.Call(ctx, input.App, protocol.GetWorkflowNotificationsRequest(input.ID), &response); err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(response.Notifications), nil
}

func (s *handler) jsonStreams(ctx context.Context, input *jsonWorkflowInput) (*jsonNullableOutput[[]protocol.StreamEntryOutput], error) {
	var response protocol.GetWorkflowStreamsResponse
	if err := s.hub.Call(ctx, input.App, protocol.GetWorkflowStreamsRequest(input.ID), &response); err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(response.Streams), nil
}

func (s *handler) jsonQueues(ctx context.Context, input *jsonAppInput) (*jsonNullableOutput[[]protocol.QueueOutput], error) {
	var response protocol.ListQueuesResponse
	if err := s.hub.Call(ctx, input.App, protocol.ListQueuesRequest(), &response); err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(response.Output), nil
}

func (s *handler) jsonQueue(ctx context.Context, input *jsonQueueInput) (*jsonNullableOutput[*protocol.QueueOutput], error) {
	var response protocol.GetQueueResponse
	if err := s.hub.Call(ctx, input.App, protocol.GetQueueRequest(input.Name), &response); err != nil {
		return nil, jsonFailure(err)
	}
	return jsonNullableResult(response.Output), nil
}
