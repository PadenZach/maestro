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

	"github.com/PadenZach/maestro"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

// httpAPI keeps documentation and JSON route registration together. Console
// HTML and executor messages have separate contracts and route registrations.
func (s *handler) httpAPI() huma.API {
	cfg := huma.DefaultConfig("Maestro HTTP API", maestro.Version())
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
		"The [Console](/) serves HTML separately. Executors connect over WebSocket and answer server-initiated RPC requests. " +
		"Authentication and deployment access policies belong to the external gateway."
	cfg.Info.Description += fmt.Sprintf(" The /v2 routes serve organization %q, documented from the Go request and response models. They expose 14 read operations; full Conductor compatibility is not claimed.", s.cfg.OrgName)
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

func requestFields[T any]() map[string]struct{} {
	fields := map[string]struct{}{}
	for _, field := range reflect.VisibleFields(reflect.TypeFor[T]()) {
		if name := strings.Split(field.Tag.Get("json"), ",")[0]; name != "" && name != "-" {
			fields[name] = struct{}{}
		}
	}
	return fields
}
