package apidocs_test

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/zpaden/maestro/internal/apidocs"
)

const sourceRoot = "/v2/orgs/{orgName}/apps/{appName}"
const servedRoot = "/v2/orgs/{org}/apps/{app}"

var supportedRoutes = []struct{ method, source, served, id string }{
	{"GET", "/workflows", "/workflows", "listWorkflows"},
	{"POST", "/workflows/search", "/workflows/search", "searchWorkflows"},
	{"GET", "/workflows/{workflowId}", "/workflows/{id}", "getWorkflow"},
	{"GET", "/workflows/{workflowId}/steps", "/workflows/{id}/steps", "listWorkflowSteps"},
	{"GET", "/workflows/{workflowId}/events", "/workflows/{id}/events", "listWorkflowEvents"},
	{"GET", "/workflows/{workflowId}/notifications", "/workflows/{id}/notifications", "listWorkflowNotifications"},
	{"GET", "/workflows/{workflowId}/streams", "/workflows/{id}/streams", "listWorkflowStreams"},
	{"POST", "/workflows/aggregates", "/workflows/aggregates", "getWorkflowAggregates"},
	{"POST", "/steps/aggregates", "/steps/aggregates", "getStepAggregates"},
	{"GET", "/workflows/{workflowId}/export", "/workflows/{id}/export", "exportWorkflow"},
	{"GET", "/schedules", "/schedules", "listSchedules"},
	{"GET", "/schedules/{scheduleName}", "/schedules/{name}", "getSchedule"},
	{"GET", "/queues", "/queues", "listQueues"},
	{"GET", "/queues/{queueName}", "/queues/{name}", "getQueue"},
}

func snapshot(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../docs/reference/conductor-openapi-2026-09-25.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func object(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func document() *huma.OpenAPI {
	return &huma.OpenAPI{OpenAPI: "3.1.0", Info: &huma.Info{Title: "Maestro", Version: "dev"}, Components: &huma.Components{Schemas: huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)}}
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPinnedConductorOperationsPreserveContract(t *testing.T) {
	pin := snapshot(t)
	original := bytes.Clone(pin)
	c, err := apidocs.New(pin)
	if err != nil {
		t.Fatal(err)
	}
	doc := document()
	want := object(t, pin)
	wantPaths := want["paths"].(map[string]any)
	for _, route := range supportedRoutes {
		op, err := c.AddOperation(doc, route.method, sourceRoot+route.source, servedRoot+route.served)
		if err != nil {
			t.Fatal(err)
		}
		if op.Method != route.method || op.Path != servedRoot+route.served || op.OperationID != route.id {
			t.Fatalf("operation registration lost pinned identity: %#v", op)
		}
		operation := wantPaths[sourceRoot+route.source].(map[string]any)[strings.ToLower(route.method)].(map[string]any)
		for _, raw := range operation["parameters"].([]any) {
			parameter := raw.(map[string]any)
			if parameter["in"] != "path" {
				continue
			}
			switch parameter["name"] {
			case "orgName":
				parameter["name"] = "org"
			case "appName":
				parameter["name"] = "app"
			case "workflowId":
				parameter["name"] = "id"
			case "scheduleName", "queueName":
				parameter["name"] = "name"
			}
		}
		if got := object(t, marshal(t, op)); !reflect.DeepEqual(got, operation) {
			t.Fatalf("%s changed pinned query/body/response/metadata:\ngot %s\nwant %s", route.id, marshal(t, got), marshal(t, operation))
		}
	}
	got := object(t, marshal(t, doc))
	if len(got["paths"].(map[string]any)) != 14 {
		t.Fatalf("unexpected path count: %v", got["paths"])
	}
	gotComponents := got["components"].(map[string]any)
	gotSchemas := gotComponents["schemas"].(map[string]any)
	wantSchemas := want["components"].(map[string]any)["schemas"].(map[string]any)
	wantNames := []string{"ErrorDetail", "ErrorModel", "Event", "ExportWorkflowOutputBody", "Notification", "Queue", "Schedule", "Step", "StepAggregate", "StepAggregatesBody", "StreamEntry", "Workflow", "WorkflowAggregate", "WorkflowAggregatesBody", "WorkflowSearchBody"}
	var names []string
	for name, schema := range gotSchemas {
		names = append(names, name)
		if !reflect.DeepEqual(schema, wantSchemas[name]) {
			t.Fatalf("schema %s lost pinned nullable/required/value semantics", name)
		}
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("schema closure: got %v want %v", names, wantNames)
	}
	for _, field := range []string{"servers", "security"} {
		if _, ok := got[field]; ok {
			t.Fatalf("upstream %s leaked into local document", field)
		}
	}
	if _, ok := gotComponents["securitySchemes"]; ok {
		t.Fatal("upstream authentication schemes leaked")
	}
	if !bytes.Equal(pin, original) {
		t.Fatal("snapshot bytes changed")
	}
}

func TestConductorRejectsInvalidRegistrationWithoutChangingDocument(t *testing.T) {
	c, err := apidocs.New(snapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, method, source, served string }{
		{"missing-path", "GET", sourceRoot + "/missing", servedRoot + "/missing"},
		{"missing-method", "POST", sourceRoot + "/queues", servedRoot + "/queues"},
		{"invalid-method", "GET POST", sourceRoot + "/queues", servedRoot + "/queues"},
		{"changed-segment", "GET", sourceRoot + "/queues", servedRoot + "/schedules"},
		{"changed-shape", "GET", sourceRoot + "/queues", servedRoot + "/queues/{name}"},
		{"missing-placeholder", "GET", sourceRoot + "/queues/{queueName}", servedRoot + "/queues/fixed"},
		{"duplicate-placeholder", "GET", sourceRoot + "/queues", "/v2/orgs/{app}/apps/{app}/queues"},
		{"empty-placeholder", "GET", sourceRoot + "/queues", "/v2/orgs/{}/apps/{app}/queues"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := document()
			before := marshal(t, doc)
			if _, err := c.AddOperation(doc, tc.method, tc.source, tc.served); err == nil {
				t.Fatal("invalid registration accepted")
			}
			if !bytes.Equal(before, marshal(t, doc)) {
				t.Fatal("rejected operation changed document")
			}
		})
	}
	if _, err := c.AddOperation(nil, "GET", sourceRoot+"/queues", servedRoot+"/queues"); err == nil {
		t.Fatal("nil document accepted")
	}
}

func TestConductorRejectsDuplicateOperationsAndSchemaCollisions(t *testing.T) {
	c, err := apidocs.New(snapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	doc := document()
	if _, err := c.AddOperation(doc, "GET", sourceRoot+"/queues", servedRoot+"/queues"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{servedRoot + "/queues", "/v2/orgs/{organization}/apps/{application}/queues"} {
		before := marshal(t, doc)
		if _, err := c.AddOperation(doc, "GET", sourceRoot+"/queues", path); err == nil {
			t.Fatal("duplicate operation accepted")
		}
		if !bytes.Equal(before, marshal(t, doc)) {
			t.Fatal("duplicate operation changed document")
		}
	}
	doc = document()
	doc.Components.Schemas.Map()["Queue"] = &huma.Schema{Type: "string"}
	before := marshal(t, doc)
	if _, err := c.AddOperation(doc, "GET", sourceRoot+"/queues", servedRoot+"/queues"); err == nil || !strings.Contains(err.Error(), "Queue") {
		t.Fatalf("schema collision: %v", err)
	}
	if !bytes.Equal(before, marshal(t, doc)) {
		t.Fatal("schema collision partially registered operation")
	}
}

func TestConductorRejectsBrokenPinnedReferences(t *testing.T) {
	for _, reference := range []any{"#/components/schemas/Missing", "https://example.invalid/queue.json", 42} {
		pin := object(t, snapshot(t))
		pin["components"].(map[string]any)["schemas"].(map[string]any)["ErrorModel"].(map[string]any)["properties"].(map[string]any)["errors"].(map[string]any)["items"] = map[string]any{"$ref": reference}
		c, err := apidocs.New(marshal(t, pin))
		if err != nil {
			t.Fatal(err)
		}
		doc := document()
		before := marshal(t, doc)
		if _, err := c.AddOperation(doc, "GET", sourceRoot+"/queues", servedRoot+"/queues"); err == nil {
			t.Fatalf("broken reference accepted: %v", reference)
		}
		if !bytes.Equal(before, marshal(t, doc)) {
			t.Fatal("broken reference partially registered operation")
		}
	}
}

func TestConductorSnapshotRemainsIndependentOfRegisteredOperations(t *testing.T) {
	c, err := apidocs.New(snapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	op, err := c.AddOperation(document(), "GET", sourceRoot+"/queues", servedRoot+"/queues")
	if err != nil {
		t.Fatal(err)
	}
	op.Extensions["summary"] = "changed"
	op.Extensions["parameters"].([]any)[0].(map[string]any)["name"] = "changed"
	doc := document()
	op, err = c.AddOperation(doc, "GET", sourceRoot+"/queues", sourceRoot+"/queues")
	if err != nil {
		t.Fatal(err)
	}
	want := object(t, snapshot(t))["paths"].(map[string]any)[sourceRoot+"/queues"].(map[string]any)["get"]
	if !reflect.DeepEqual(object(t, marshal(t, op)), want) {
		t.Fatal("registered operation mutated the pinned source")
	}
}

func TestConductorPreservesExactSchemaNumbersAndPrunesUnusedSchemas(t *testing.T) {
	pin := object(t, snapshot(t))
	schemas := pin["components"].(map[string]any)["schemas"].(map[string]any)
	queue := schemas["Queue"].(map[string]any)
	queue["maximum"] = json.Number("9223372036854775807")
	c, err := apidocs.New(marshal(t, pin))
	if err != nil {
		t.Fatal(err)
	}
	doc := document()
	doc.Components.Schemas.Map()["UnusedLocal"] = &huma.Schema{Type: "string"}
	if _, err := c.AddOperation(doc, "GET", sourceRoot+"/queues", servedRoot+"/queues"); err != nil {
		t.Fatal(err)
	}
	gotSchemas := object(t, marshal(t, doc))["components"].(map[string]any)["schemas"].(map[string]any)
	if len(gotSchemas) != 3 || !reflect.DeepEqual(gotSchemas["Queue"], queue) {
		t.Fatalf("schema numbers or pruning changed: %s", marshal(t, gotSchemas))
	}
}

func TestConductorRejectsMalformedSnapshots(t *testing.T) {
	for _, pin := range []string{"null", "[]", "{", `{}`, `{"paths":{},"components":{"schemas":{}}}{}`, `{"paths":{},"components":{}}`} {
		if _, err := apidocs.New([]byte(pin)); err == nil {
			t.Fatalf("malformed snapshot accepted: %s", pin)
		}
	}
}
