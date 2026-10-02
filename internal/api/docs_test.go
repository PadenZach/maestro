package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
)

// feature_docs.md requests generated HTTP docs; CONTRACTS.md keeps Conductor
// reads distinct from Console HTML and SDK-owned executor WebSocket messages.
func TestAPIDocumentationRoutes(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, time.Second)
	s := api.New(config.Config{}, h, log)
	record := httptest.NewRecorder()
	s.Handler().ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if record.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json status = %d, want 200", record.Code)
	}
	var doc struct {
		OpenAPI string                                `json:"openapi"`
		Paths   map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(record.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.1.") {
		t.Fatalf("OpenAPI = %q, want 3.1", doc.OpenAPI)
	}
	operations := 0
	for path, item := range doc.Paths {
		if path != "/healthz" && !strings.HasPrefix(path, "/api/") && !strings.HasPrefix(path, "/v2/") {
			t.Errorf("unexpected documented path %q", path)
		}
		for method := range item {
			if method == "get" || method == "post" {
				operations++
			}
		}
	}
	want := 25
	if operations != want {
		t.Errorf("documented operations = %d, want %d", operations, want)
	}
	record = httptest.NewRecorder()
	s.Handler().ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if record.Code != http.StatusOK || !strings.Contains(record.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("GET /docs = %d %q, want HTML 200", record.Code, record.Header().Get("Content-Type"))
	}
	if !strings.Contains(record.Body.String(), "SwaggerUIBundle") || !strings.Contains(record.Body.String(), "/openapi.json") {
		t.Fatal("docs must initialize Swagger UI with this server's OpenAPI document")
	}
}

func TestConductorDocsDescribeRuntimeFailures(t *testing.T) {
	ts, _ := docsAcceptanceServer(t, "local")
	spec := docsAcceptanceSpec(t, ts)
	for path, item := range spec["paths"].(map[string]any) {
		if !strings.HasPrefix(path, "/v2/") {
			continue
		}
		for method, value := range item.(map[string]any) {
			op := value.(map[string]any)
			responses := op["responses"].(map[string]any)
			for _, status := range []string{"400", "404", "502", "503"} {
				response, ok := responses[status].(map[string]any)
				if !ok {
					t.Errorf("%s %s omits runtime status %s", method, path, status)
					continue
				}
				content, _ := response["content"].(map[string]any)
				if content["application/problem+json"] == nil {
					t.Errorf("%s %s status %s omits the problem response", method, path, status)
				}
			}
			if responses["401"] != nil || responses["403"] != nil {
				t.Errorf("%s %s documents authentication the server does not implement", method, path)
			}
		}
	}
}

// The live handler is the only source of the OpenAPI document in these tests.
func generatedOpenAPIJSON(t *testing.T) []byte {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := api.New(config.Config{}, hub.New(log, time.Second), log)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("OpenAPI status = %d", response.Code)
	}
	return response.Body.Bytes()
}
