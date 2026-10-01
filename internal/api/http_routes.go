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
	"github.com/zpaden/maestro/docs/reference"
	"github.com/zpaden/maestro/internal/apidocs"
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
	// Local DTOs and the upstream v2 schemas belong to different contracts.
	cfg.Components.Schemas = huma.NewMapRegistry("#/components/schemas/", func(t reflect.Type, hint string) string {
		return "Local" + huma.DefaultSchemaNamer(t, hint)
	})
	cfg.Info.Description = "Maestro's local JSON reads use /api paths and preserve SDK field names. " +
		"The [Console](/) serves HTML separately. Executors connect over WebSocket and answer server-initiated RPC requests; " +
		"see the [executor protocol](https://github.com/zpaden/maestro/blob/main/docs/EXECUTOR_PROTOCOL.md). " +
		"Authentication and deployment access policies belong to the external gateway."
	cfg.Info.Description += fmt.Sprintf(" The /v2 routes serve organization %q, derived from the pinned Conductor contract. They expose 14 read operations; full Conductor compatibility is not claimed.", s.cfg.OrgName)
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
	contract, err := apidocs.New(reference.ConductorOpenAPI)
	if err != nil {
		panic(err) // Embedded contract errors are programming errors.
	}
	const root = "/v2/orgs/{orgName}/apps/{appName}"
	parameterNames := strings.NewReplacer("{orgName}", "{org}", "{appName}", "{app}",
		"{workflowId}", "{id}", "{queueName}", "{name}", "{scheduleName}", "{name}")
	register := func(method, suffix string, handler http.HandlerFunc) {
		source := root + suffix
		op, err := contract.AddOperation(api.OpenAPI(), method, source, parameterNames.Replace(source))
		if err != nil {
			panic(err)
		}
		api.Adapter().Handle(op, func(ctx huma.Context) {
			r, w := humago.Unwrap(ctx)
			handler(w, r)
		})
	}
	register(http.MethodGet, "/schedules", s.localV2Schedules)
	register(http.MethodGet, "/schedules/{scheduleName}", s.localV2GetSchedule)
	register(http.MethodGet, "/queues", s.localV2Queues)
	register(http.MethodGet, "/queues/{queueName}", s.localV2GetQueue)
	register(http.MethodGet, "/workflows", s.localV2ListWorkflows)
	register(http.MethodPost, "/workflows/search", s.localV2Search)
	register(http.MethodPost, "/workflows/aggregates", s.localV2WorkflowAggregates)
	register(http.MethodPost, "/steps/aggregates", s.localV2StepAggregates)
	register(http.MethodGet, "/workflows/{workflowId}", s.localV2Get)
	register(http.MethodGet, "/workflows/{workflowId}/export", s.localV2ExportWorkflow)
	register(http.MethodGet, "/workflows/{workflowId}/steps", s.localV2Steps)
	register(http.MethodGet, "/workflows/{workflowId}/events", s.localV2Events)
	register(http.MethodGet, "/workflows/{workflowId}/notifications", s.localV2Notifications)
	register(http.MethodGet, "/workflows/{workflowId}/streams", s.localV2Streams)
	// Owner-approved SDK compatibility exceptions. Keep the pinned snapshot
	// immutable and document the actual response without fabricating values.
	workflow := api.OpenAPI().Components.Schemas.Map()["Workflow"].Extensions
	properties := workflow["properties"].(map[string]any)
	for name, kind := range map[string]string{"priority": "integer", "updatedAt": "string"} {
		field := properties[name].(map[string]any)
		field["type"] = []any{kind, "null"}
		field["description"] = "Preserves SDK null values; nullable in Maestro, unlike the pinned Conductor snapshot."
	}
}
