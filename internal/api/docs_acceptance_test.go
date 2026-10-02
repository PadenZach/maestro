package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	humayaml "github.com/danielgtaylor/huma/v2/yaml"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

// The registration log is emitted after the hub admits the peer. It supplies a
// readiness barrier without sleeping or assuming handshake writes are admission.
type docsAcceptanceLog struct {
	slog.Handler
	connected chan struct{}
}

func (h docsAcceptanceLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return docsAcceptanceLog{h.Handler.WithAttrs(attrs), h.connected}
}

func (h docsAcceptanceLog) WithGroup(name string) slog.Handler {
	return docsAcceptanceLog{h.Handler.WithGroup(name), h.connected}
}

func (h docsAcceptanceLog) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "executor connected" {
		select {
		case h.connected <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

func docsAcceptanceServer(t *testing.T, org string) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	connected := make(chan struct{}, 1)
	logger := slog.New(docsAcceptanceLog{slog.NewTextHandler(io.Discard, nil), connected})
	h := hub.New(logger, 2*time.Second)
	s := api.New(config.Config{OrgName: org, ListenAddr: "127.0.0.1:0"}, h, logger)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, connected
}

func docsAcceptanceJSON(t *testing.T, ts *httptest.Server, path string, wantStatus int) any {
	t.Helper()
	response, err := ts.Client().Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s: status %d, want %d; body %s", path, response.StatusCode, wantStatus, data)
	}
	var value any
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("GET %s JSON: %v", path, err)
	}
	return value
}

func docsAcceptanceSpec(t *testing.T, ts *httptest.Server) map[string]any {
	t.Helper()
	return docsAcceptanceJSON(t, ts, "/openapi.json", http.StatusOK).(map[string]any)
}

func docsAcceptanceResolve(t *testing.T, spec map[string]any, schema map[string]any) map[string]any {
	t.Helper()
	for depth := 0; depth < 20; depth++ {
		ref, ok := schema["$ref"].(string)
		if !ok {
			return schema
		}
		if !strings.HasPrefix(ref, "#/") {
			t.Fatalf("schema has nonlocal reference %q", ref)
		}
		var value any = spec
		for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
			key := strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
			object, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("reference %q traverses a nonobject", ref)
			}
			value, ok = object[key]
			if !ok {
				t.Fatalf("reference %q is dangling at %q", ref, key)
			}
		}
		var valid bool
		schema, valid = value.(map[string]any)
		if !valid {
			t.Fatalf("reference %q resolves to a nonobject", ref)
		}
	}
	t.Fatal("schema reference cycle")
	return nil
}

func docsAcceptanceResponseSchema(t *testing.T, spec map[string]any, path, status string) map[string]any {
	t.Helper()
	paths, _ := spec["paths"].(map[string]any)
	item, _ := paths[path].(map[string]any)
	op, _ := item["get"].(map[string]any)
	responses, _ := op["responses"].(map[string]any)
	response, _ := responses[status].(map[string]any)
	content, _ := response["content"].(map[string]any)
	media, _ := content["application/json"].(map[string]any)
	schema, _ := media["schema"].(map[string]any)
	if schema == nil {
		t.Fatalf("GET %s has no application/json response schema for %s", path, status)
	}
	return docsAcceptanceResolve(t, spec, schema)
}

func docsAcceptanceAllowsType(t *testing.T, schema map[string]any, want string) {
	t.Helper()
	if schema["type"] == want {
		return
	}
	if types, ok := schema["type"].([]any); ok {
		for _, typ := range types {
			if typ == want {
				return
			}
		}
	}
	t.Fatalf("schema does not allow %s: %v", want, schema)
}

func TestDocsAcceptanceLocalSchemasPreserveSDKResponses(t *testing.T) {
	ts, connected := docsAcceptanceServer(t, "other")
	spec := docsAcceptanceSpec(t, ts)
	workflowSchema := docsAcceptanceResponseSchema(t, spec, "/api/{app}/workflows/{id}", "200")
	properties, _ := workflowSchema["properties"].(map[string]any)
	for _, field := range []string{"WorkflowUUID", "Input", "Output", "WasForkedFrom"} {
		if _, ok := properties[field]; !ok {
			t.Fatalf("local workflow schema omits SDK field %s", field)
		}
	}
	for _, forbidden := range []string{"workflowId", "input", "output", "$schema"} {
		if _, ok := properties[forbidden]; ok {
			t.Errorf("local workflow schema invents field %s", forbidden)
		}
	}
	docsAcceptanceAllowsType(t, docsAcceptanceResolve(t, spec, properties["Output"].(map[string]any)), "null")
	outputRequired := false
	required, _ := workflowSchema["required"].([]any)
	for _, field := range required {
		outputRequired = outputRequired || field == "Output"
	}
	if !outputRequired {
		t.Fatal("local workflow schema must require the nullable Output key")
	}
	queueSchema := docsAcceptanceResponseSchema(t, spec, "/api/{app}/queues/{name}", "200")
	docsAcceptanceAllowsType(t, queueSchema, "null")
	eventsSchema := docsAcceptanceResponseSchema(t, spec, "/api/{app}/workflows/{id}/events", "200")
	docsAcceptanceAllowsType(t, eventsSchema, "array")
	docsAcceptanceAllowsType(t, eventsSchema, "null")

	dialFake(t, ts, "docs-fixture", "gateway", "docs-executor", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": map[string]any{"WorkflowUUID": "wf-null", "Status": nil, "Input": "opaque <&> value", "Output": nil, "WasForkedFrom": false}}
		},
		protocol.MsgGetQueue:          func(map[string]any) map[string]any { return map[string]any{"output": nil} },
		protocol.MsgListSteps:         func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any { return map[string]any{"events": nil} },
	})
	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("executor was not admitted")
	}
	workflow := docsAcceptanceJSON(t, ts, "/api/docs-fixture/workflows/wf-null", 200).(map[string]any)
	if value, exists := workflow["Output"]; !exists || value != nil {
		t.Fatalf("required nullable Output changed: %v", workflow)
	}
	if workflow["Input"] != "opaque <&> value" || workflow["WasForkedFrom"] != false || workflow["WorkflowUUID"] != "wf-null" {
		t.Fatalf("SDK workflow values changed: %v", workflow)
	}
	if _, exists := workflow["$schema"]; exists {
		t.Fatal("local workflow response gained a $schema field")
	}
	if value := docsAcceptanceJSON(t, ts, "/api/docs-fixture/queues/missing", 200); value != nil {
		t.Fatalf("null local queue changed to %v", value)
	}
	if value := docsAcceptanceJSON(t, ts, "/api/docs-fixture/workflows/wf-null/events", 200); value != nil {
		t.Fatalf("null local events changed to %v", value)
	}
	steps := docsAcceptanceJSON(t, ts, "/api/docs-fixture/workflows/wf-null/steps", 200)
	if !reflect.DeepEqual(steps, []any{}) {
		t.Fatalf("empty step array changed to %v", steps)
	}
	errorBody := docsAcceptanceJSON(t, ts, "/api/disconnected/workflows", 503).(map[string]any)
	if len(errorBody) != 1 || errorBody["error"] == nil {
		t.Fatalf("local error envelope changed: %v", errorBody)
	}
	errorSchema := docsAcceptanceResponseSchema(t, spec, "/api/{app}/workflows", "503")
	if fields, ok := errorSchema["properties"].(map[string]any); !ok || fields["error"] == nil {
		t.Fatalf("local error schema does not describe error envelope: %v", errorSchema)
	}
}

func TestDocsAcceptanceGeneratedSchemasAndServerIsolation(t *testing.T) {
	enabled, _ := docsAcceptanceServer(t, "local")
	first := docsAcceptanceSpec(t, enabled)
	wantIDs := map[string]bool{
		"listQueues": true, "getQueue": true, "listSchedules": true, "getSchedule": true,
		"listWorkflows": true, "searchWorkflows": true, "getWorkflow": true,
		"listWorkflowSteps": true, "listWorkflowEvents": true, "listWorkflowNotifications": true,
		"listWorkflowStreams": true, "getWorkflowAggregates": true, "getStepAggregates": true, "exportWorkflow": true,
	}
	gotIDs := map[string]bool{}
	for path, value := range first["paths"].(map[string]any) {
		if !strings.HasPrefix(path, "/v2/") {
			continue
		}
		for method, value := range value.(map[string]any) {
			if method != "get" && method != "post" {
				t.Fatalf("unsupported v2 method %s %s is documented", method, path)
			}
			op := value.(map[string]any)
			id, _ := op["operationId"].(string)
			if gotIDs[id] {
				t.Fatalf("duplicate v2 operation ID %q", id)
			}
			gotIDs[id] = true
		}
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("documented v2 operation IDs %v, want supported reads %v", gotIDs, wantIDs)
	}
	schemas := first["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"Workflow", "Step", "Queue", "Schedule", "Event", "Notification", "StreamEntry", "WorkflowSearchBody", "WorkflowAggregatesBody", "StepAggregatesBody", "WorkflowAggregate", "StepAggregate", "ExportWorkflowOutputBody"} {
		if schema, ok := schemas[name].(map[string]any); !ok || len(schema["properties"].(map[string]any)) == 0 {
			t.Errorf("missing generated model schema %s", name)
		}
	}
	other, _ := docsAcceptanceServer(t, "other")
	second := docsAcceptanceSpec(t, other)
	if !reflect.DeepEqual(first["paths"], second["paths"]) {
		t.Error("organization configuration changed the supported routes")
	}
	if !strings.Contains(second["info"].(map[string]any)["description"].(string), `"other"`) {
		t.Error("second server inherited the first organization")
	}
	if after := docsAcceptanceSpec(t, enabled); !reflect.DeepEqual(first, after) {
		t.Fatal("creating a second server changed the first server's documentation")
	}
	for _, spec := range []map[string]any{first, second} {
		if security, exists := spec["security"]; exists && !reflect.DeepEqual(security, []any{}) {
			t.Errorf("documentation inherited upstream security: %v", security)
		}
		if components, ok := spec["components"].(map[string]any); ok {
			if schemes, exists := components["securitySchemes"]; exists && len(schemes.(map[string]any)) > 0 {
				t.Errorf("documentation inherited upstream security schemes: %v", schemes)
			}
		}
		if servers, ok := spec["servers"].([]any); ok {
			for _, value := range servers {
				if server := value.(map[string]any); server["url"] != "/" {
					t.Errorf("documentation server URL is not local root: %v", server)
				}
			}
		}
		var checkRefs func(any)
		checkRefs = func(value any) {
			switch value := value.(type) {
			case map[string]any:
				if _, ok := value["$ref"]; ok {
					docsAcceptanceResolve(t, spec, value)
				}
				for _, child := range value {
					checkRefs(child)
				}
			case []any:
				for _, child := range value {
					checkRefs(child)
				}
			}
		}
		checkRefs(spec)
	}
}

func TestDocsAcceptanceSwaggerUsesLocalSpecification(t *testing.T) {
	ts, _ := docsAcceptanceServer(t, "other")
	response, err := ts.Client().Get(ts.URL + "/docs")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !strings.Contains(string(body), `data-url="/openapi.json"`) || !strings.Contains(string(body), "swagger-ui-dist@") {
		t.Fatalf("docs must render Swagger with the local spec: status %d, body %s", response.StatusCode, body)
	}
	if strings.Contains(string(body), "cloud.dbos.dev") || strings.Contains(string(body), "/conductor/openapi") {
		t.Fatalf("docs inherited an upstream specification URL: %s", body)
	}
}

// Fresh servers leave each specification cache empty. The start barrier puts
// the first reads in competition; go test -race observes cache initialization.
func TestDocsAcceptanceConcurrentFirstSpecificationRequests(t *testing.T) {
	for _, path := range []string{"/openapi.json", "/openapi-3.0.json", "/openapi.yaml", "/openapi-3.0.yaml"} {
		t.Run(path, func(t *testing.T) {
			ts, _ := docsAcceptanceServer(t, "local")
			const readers = 16
			start := make(chan struct{})
			var ready sync.WaitGroup
			ready.Add(readers)
			type result struct {
				body []byte
				err  error
			}
			results := make(chan result, readers)
			client := &http.Client{Timeout: 5 * time.Second}
			for range readers {
				go func() {
					ready.Done()
					<-start
					response, err := client.Get(ts.URL + path)
					if err != nil {
						results <- result{err: err}
						return
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err == nil && response.StatusCode != 200 {
						err = fmt.Errorf("status %d, want 200", response.StatusCode)
					}
					wantType := "application/openapi+json"
					if strings.HasSuffix(path, ".yaml") {
						wantType = "application/openapi+yaml"
					}
					if err == nil && response.Header.Get("Content-Type") != wantType {
						err = fmt.Errorf("content type %q, want %q", response.Header.Get("Content-Type"), wantType)
					}
					results <- result{body: body, err: err}
				}()
			}
			ready.Wait()
			close(start)
			bodies := make([][]byte, 0, readers)
			for range readers {
				result := <-results
				if result.err != nil {
					t.Fatal(result.err)
				}
				bodies = append(bodies, result.body)
			}
			wantVersion := "3.1.0"
			if strings.Contains(path, "-3.0.") {
				wantVersion = "3.0.3"
			}
			if strings.HasSuffix(path, ".json") {
				for _, body := range bodies {
					var document map[string]any
					if err := json.Unmarshal(body, &document); err != nil || document["openapi"] != wantVersion || document["paths"] == nil {
						t.Fatalf("invalid %s specification response: %s (%v)", wantVersion, body, err)
					}
				}
			} else {
				// Check the YAML representation against the corresponding parsed
				// JSON document, rather than accepting a nonempty string as YAML.
				jsonPath := strings.TrimSuffix(path, ".yaml") + ".json"
				response, err := client.Get(ts.URL + jsonPath)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				jsonBody, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				var document map[string]any
				if err := json.Unmarshal(jsonBody, &document); err != nil || document["openapi"] != wantVersion || document["paths"] == nil {
					t.Fatalf("invalid %s JSON specification used as YAML oracle (%v)", wantVersion, err)
				}
				var want bytes.Buffer
				if err := humayaml.Convert(&want, bytes.NewReader(jsonBody)); err != nil {
					t.Fatal(err)
				}
				for _, body := range bodies {
					if !bytes.Equal(body, want.Bytes()) {
						t.Fatalf("concurrent YAML response differs from its JSON specification")
					}
				}
			}
		})
	}
}
