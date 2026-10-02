package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/zpaden/maestro/internal/web"
)

// httpAPI keeps documentation and JSON route registration together. Console
// HTML and executor messages have separate contracts and route registrations.
func (s *Server) httpAPI() huma.API {
	version := web.Version
	if version == "" {
		version = "dev"
	}
	cfg := huma.DefaultConfig("Maestro HTTP API", version)
	// Existing JSON responses must retain their exact fields and escaping.
	cfg.CreateHooks = nil
	cfg.SchemasPath = ""
	cfg.DocsRenderer = huma.DocsRendererSwaggerUI
	cfg.DocsRendererConfig = map[string]any{"validatorUrl": nil}
	format := huma.Format{
		Marshal:   func(w io.Writer, v any) error { return json.NewEncoder(w).Encode(v) },
		Unmarshal: json.Unmarshal,
	}
	cfg.Formats = map[string]huma.Format{"application/json": format, "json": format}
	cfg.Info.Description = "Maestro's local JSON reads use /api paths and preserve SDK field names. " +
		"The [Console](/) serves HTML separately. Executors connect over WebSocket and answer server-initiated RPC requests; " +
		"see the [executor protocol](https://github.com/zpaden/maestro/blob/main/docs/EXECUTOR_PROTOCOL.md). " +
		"Authentication and deployment access policies belong to the external gateway."
	cfg.Info.Description += fmt.Sprintf(" The /v2 routes serve organization %q, documented from the Go request and response models. They expose 14 read operations; full Conductor compatibility is not claimed.", s.cfg.OrgName)
	if s.cfg.AllowRemote {
		cfg.Info.Description += " Remote access is enabled for deployment behind an external gateway."
	} else {
		cfg.Info.Description += " The listener and Conductor API clients must use loopback."
	}
	return humago.New(&documentationMux{ServeMux: s.mux}, cfg)
}

// Huma 2.39.1 lazily caches its four specification representations without
// synchronization. Serialize those handlers; API and Console traffic remains
// independent. This can be removed when the pinned dependency fixes its cache.
type documentationMux struct {
	*http.ServeMux
	specMu sync.Mutex
}

func (m *documentationMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	switch pattern {
	case "GET /openapi.json", "GET /openapi.yaml", "GET /openapi-3.0.json", "GET /openapi-3.0.yaml":
		m.ServeMux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			m.specMu.Lock()
			defer m.specMu.Unlock()
			handler(w, r)
		})
	default:
		m.ServeMux.HandleFunc(pattern, handler)
	}
}

func (s *Server) jsonRoutes(api huma.API) {
	registerJSONRead(api, "/healthz", "localHealth", "Check server health", s.jsonHealth, false)
	registerJSONRead(api, "/api/executors", "localListExecutors", "List connected executors", s.jsonExecutors, false)
	registerJSONRead(api, "/api/apps", "localListApplications", "List connected applications", s.jsonApps, false)
	registerJSONRead(api, "/api/{app}/workflows", "localListWorkflows", "List workflows", s.jsonWorkflows, true)
	registerJSONRead(api, "/api/{app}/workflows/{id}", "localGetWorkflow", "Get a workflow", s.jsonWorkflow, true)
	registerJSONRead(api, "/api/{app}/workflows/{id}/steps", "localListWorkflowSteps", "List workflow steps", s.jsonSteps, true)
	registerJSONRead(api, "/api/{app}/workflows/{id}/events", "localListWorkflowEvents", "List workflow events", s.jsonEvents, true)
	registerJSONRead(api, "/api/{app}/workflows/{id}/notifications", "localListWorkflowNotifications", "List workflow notifications", s.jsonNotifications, true)
	registerJSONRead(api, "/api/{app}/workflows/{id}/streams", "localListWorkflowStreams", "List workflow streams", s.jsonStreams, true)
	registerJSONRead(api, "/api/{app}/queues", "localListQueues", "List queues", s.jsonQueues, true)
	registerJSONRead(api, "/api/{app}/queues/{name}", "localGetQueue", "Get a queue", s.jsonQueue, true)
}

func registerJSONRead[I, O any](api huma.API, path, id, summary string, handler func(context.Context, *I) (*O, error), canFail bool) {
	op := huma.Operation{
		Method: http.MethodGet, Path: path, OperationID: id, Summary: summary,
		Tags: []string{"Local API"}, SkipValidateParams: true,
	}
	if canFail {
		op.Responses = map[string]*huma.Response{}
		errorSchema := api.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[jsonAPIError](), true, "")
		for _, status := range []int{http.StatusBadGateway, http.StatusServiceUnavailable} {
			op.Responses[strconv.Itoa(status)] = &huma.Response{
				Description: http.StatusText(status),
				Content:     map[string]*huma.MediaType{"application/json": {Schema: errorSchema}},
			}
		}
	}
	huma.Register(api, op, handler)
	if !canFail {
		// These in-memory reads have no application-error response. Huma's
		// inferred problem response does not describe their existing contract.
		delete(api.OpenAPI().Paths[path].Get.Responses, "default")
	}
}

func (s *Server) conductorRoutes(api huma.API) {
	const root = "/v2/orgs/{org}/apps/{app}"
	for _, route := range []struct {
		method, path, id, summary string
		response, query, body     reflect.Type
		handler                   http.HandlerFunc
	}{
		{http.MethodGet, "/schedules", "listSchedules", "List schedules", reflect.TypeFor[[]Schedule](), reflect.TypeFor[scheduleListQuery](), nil, s.localV2Schedules},
		{http.MethodGet, "/schedules/{name}", "getSchedule", "Get a schedule", reflect.TypeFor[Schedule](), nil, nil, s.localV2GetSchedule},
		{http.MethodGet, "/queues", "listQueues", "List queues", reflect.TypeFor[[]Queue](), nil, nil, s.localV2Queues},
		{http.MethodGet, "/queues/{name}", "getQueue", "Get a queue", reflect.TypeFor[Queue](), nil, nil, s.localV2GetQueue},
		{http.MethodGet, "/workflows", "listWorkflows", "List workflows", reflect.TypeFor[[]Workflow](), reflect.TypeFor[workflowListQuery](), nil, s.localV2ListWorkflows},
		{http.MethodPost, "/workflows/search", "searchWorkflows", "Search workflows", reflect.TypeFor[[]Workflow](), nil, reflect.TypeFor[WorkflowSearchBody](), s.localV2Search},
		{http.MethodPost, "/workflows/aggregates", "getWorkflowAggregates", "Aggregate workflows", reflect.TypeFor[[]WorkflowAggregate](), nil, reflect.TypeFor[WorkflowAggregatesBody](), s.aggregateHandler(s.localV2WorkflowAggregates)},
		{http.MethodPost, "/steps/aggregates", "getStepAggregates", "Aggregate steps", reflect.TypeFor[[]StepAggregate](), nil, reflect.TypeFor[StepAggregatesBody](), s.aggregateHandler(s.localV2StepAggregates)},
		{http.MethodGet, "/workflows/{id}", "getWorkflow", "Get a workflow", reflect.TypeFor[Workflow](), nil, nil, s.localV2Get},
		{http.MethodGet, "/workflows/{id}/export", "exportWorkflow", "Export a workflow", reflect.TypeFor[ExportWorkflowOutputBody](), reflect.TypeFor[exportQuery](), nil, s.localV2ExportWorkflow},
		{http.MethodGet, "/workflows/{id}/steps", "listWorkflowSteps", "List workflow steps", reflect.TypeFor[[]Step](), reflect.TypeFor[stepsQuery](), nil, s.localV2Steps},
		{http.MethodGet, "/workflows/{id}/events", "listWorkflowEvents", "List workflow events", reflect.TypeFor[[]Event](), nil, nil, s.localV2Events},
		{http.MethodGet, "/workflows/{id}/notifications", "listWorkflowNotifications", "List workflow notifications", reflect.TypeFor[[]Notification](), nil, nil, s.localV2Notifications},
		{http.MethodGet, "/workflows/{id}/streams", "listWorkflowStreams", "List workflow streams", reflect.TypeFor[[]StreamEntry](), nil, nil, s.localV2Streams},
	} {
		registry := api.OpenAPI().Components.Schemas
		response := registry.Schema(route.response, true, "")
		if route.response.Kind() == reflect.Slice {
			response.Nullable = false // Successful collection reads always return an array.
		}
		op := &huma.Operation{
			Method: route.method, Path: root + route.path, OperationID: route.id,
			Summary: route.summary, Tags: []string{"Conductor API"},
			Responses: map[string]*huma.Response{
				"200": {Description: "OK", Content: map[string]*huma.MediaType{"application/json": {Schema: response}}},
			},
		}
		for _, status := range []int{400, 403, 404, 502, 503} {
			op.Responses[strconv.Itoa(status)] = &huma.Response{
				Description: http.StatusText(status),
				Content:     map[string]*huma.MediaType{"application/problem+json": {Schema: registry.Schema(reflect.TypeFor[Problem](), true, "")}},
			}
		}
		if route.id == "getWorkflowAggregates" || route.id == "getStepAggregates" {
			op.Description = "Requires --enable-aggregates (or CONDUCTOR_ENABLE_AGGREGATES=true). Disabled by default; returns 404 without dispatching an executor query. Fixed application overview queries are independent of this setting."
		}
		for _, part := range strings.Split(op.Path, "/") {
			if strings.HasPrefix(part, "{") {
				op.Parameters = append(op.Parameters, &huma.Param{Name: strings.Trim(part, "{}"), In: "path", Required: true, Schema: &huma.Schema{Type: "string"}})
			}
		}
		if route.query != nil {
			for _, field := range reflect.VisibleFields(route.query) {
				name := strings.Split(field.Tag.Get("json"), ",")[0]
				schema := huma.SchemaFromField(registry, field, "")
				schema.Nullable = false
				op.Parameters = append(op.Parameters, &huma.Param{Name: name, In: "query", Schema: schema})
			}
		}
		if route.body != nil {
			op.RequestBody = &huma.RequestBody{Content: map[string]*huma.MediaType{"application/json": {Schema: registry.Schema(route.body, true, "")}}}
		}
		api.OpenAPI().AddOperation(op)
		api.Adapter().Handle(op, func(ctx huma.Context) {
			r, w := humago.Unwrap(ctx)
			route.handler(w, r)
		})
	}
}

func requestFields[T any]() map[string]struct{} {
	fields := map[string]struct{}{}
	for _, field := range reflect.VisibleFields(reflect.TypeFor[T]()) {
		if name := strings.Split(field.Tag.Get("json"), ",")[0]; name != "" && name != "-" {
			fields[name] = struct{}{}
		}
	}
	return fields
}
