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

// feature_docs.md requests generated HTTP docs; CONTRACTS.md keeps the optional
// v2 slice distinct from Console HTML and SDK-owned executor WebSocket messages.
func TestAPIDocumentationRoutes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "local"
		if enabled {
			name = "with_v2"
		}
		t.Run(name, func(t *testing.T) {
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			h := hub.New(log, time.Second)
			s := api.New(config.Config{LocalHTTPV2: enabled}, h, log)
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
				if path != "/healthz" && !strings.HasPrefix(path, "/api/") && !(enabled && strings.HasPrefix(path, "/v2/")) {
					t.Errorf("unexpected documented path %q", path)
				}
				for method := range item {
					if method == "get" || method == "post" {
						operations++
					}
				}
			}
			want := 11
			if enabled {
				want += 14
			}
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
		})
	}
}
