package api

import (
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/hub"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

func (s *handler) jsonRoutes(api huma.API) {
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

func (s *handler) conductorRoutes(api huma.API) {
	const root = "/v2/orgs/{org}/apps/{app}"
	for _, route := range []struct {
		method, path, id, summary string
		response, query, body     reflect.Type
		handler                   http.HandlerFunc
	}{
		{http.MethodGet, "/schedules", "listSchedules", "List schedules", reflect.TypeFor[[]Schedule](), reflect.TypeFor[scheduleListQuery](), nil, s.listSchedules},
		{http.MethodGet, "/schedules/{name}", "getSchedule", "Get a schedule", reflect.TypeFor[Schedule](), nil, nil, s.getSchedule},
		{http.MethodGet, "/queues", "listQueues", "List queues", reflect.TypeFor[[]Queue](), nil, nil, s.listQueues},
		{http.MethodGet, "/queues/{name}", "getQueue", "Get a queue", reflect.TypeFor[Queue](), nil, nil, s.getQueue},
		{http.MethodGet, "/workflows", "listWorkflows", "List workflows", reflect.TypeFor[[]Workflow](), reflect.TypeFor[workflowListQuery](), nil, s.listWorkflows},
		{http.MethodPost, "/workflows/search", "searchWorkflows", "Search workflows", reflect.TypeFor[[]Workflow](), nil, reflect.TypeFor[WorkflowSearchBody](), s.search},
		{http.MethodPost, "/workflows/aggregates", "getWorkflowAggregates", "Aggregate workflows", reflect.TypeFor[[]WorkflowAggregate](), nil, reflect.TypeFor[WorkflowAggregatesBody](), s.aggregateHandler(s.workflowAggregates)},
		{http.MethodPost, "/steps/aggregates", "getStepAggregates", "Aggregate steps", reflect.TypeFor[[]StepAggregate](), nil, reflect.TypeFor[StepAggregatesBody](), s.aggregateHandler(s.stepAggregates)},
		{http.MethodGet, "/workflows/{id}", "getWorkflow", "Get a workflow", reflect.TypeFor[Workflow](), nil, nil, s.getWorkflow},
		{http.MethodGet, "/workflows/{id}/export", "exportWorkflow", "Export a workflow", reflect.TypeFor[ExportWorkflowOutputBody](), reflect.TypeFor[exportQuery](), nil, s.exportWorkflow},
		{http.MethodGet, "/workflows/{id}/steps", "listWorkflowSteps", "List workflow steps", reflect.TypeFor[[]Step](), reflect.TypeFor[stepsQuery](), nil, s.listSteps},
		{http.MethodGet, "/workflows/{id}/events", "listWorkflowEvents", "List workflow events", reflect.TypeFor[[]Event](), nil, nil, s.events},
		{http.MethodGet, "/workflows/{id}/notifications", "listWorkflowNotifications", "List workflow notifications", reflect.TypeFor[[]Notification](), nil, nil, s.notifications},
		{http.MethodGet, "/workflows/{id}/streams", "listWorkflowStreams", "List workflow streams", reflect.TypeFor[[]StreamEntry](), nil, nil, s.streams},
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
		for _, status := range []int{400, 404, 502, 503} {
			op.Responses[strconv.Itoa(status)] = &huma.Response{
				Description: http.StatusText(status),
				Content:     map[string]*huma.MediaType{"application/problem+json": {Schema: registry.Schema(reflect.TypeFor[Problem](), true, "")}},
			}
		}
		if route.id == "getWorkflowAggregates" || route.id == "getStepAggregates" {
			op.Description = "Requires --enable-aggregates (or MAESTRO_ENABLE_AGGREGATES=true). Disabled by default; returns 404 without dispatching an executor query. Fixed application overview queries are independent of this setting."
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

// Register adds the JSON API and its OpenAPI documentation to mux.
func Register(mux *http.ServeMux, cfg config.Config, h *hub.Hub) {
	if cfg.OrgName == "" {
		cfg.OrgName = "local"
	}
	s := &handler{cfg: cfg, hub: h, mux: mux}
	api := s.httpAPI()
	s.jsonRoutes(api)
	s.conductorRoutes(api)
}

type handler struct {
	cfg config.Config
	hub *hub.Hub
	mux *http.ServeMux
}

func (s *handler) aggregateHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg.EnableAggregates {
			writeProblem(w, http.StatusNotFound, "Advanced aggregate queries are disabled.")
			return
		}
		next(w, r)
	}
}
