package web

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestStepInspectionDoesNotInventMissingID(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{{`{"function_name":"missing"}`, "unavailable (not returned)"}, {`{"function_id":0,"function_name":"zero"}`, "0"}} {
		var step protocol.WorkflowSteps
		if err := json.Unmarshal([]byte(tc.raw), &step); err != nil {
			t.Fatal(err)
		}
		if got := StepFields(step)[0].Value; got != tc.want {
			t.Errorf("step ID for %s = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestWorkflowInspectionPreservesSDKFieldPresence(t *testing.T) {
	for _, tc := range []struct{ version, want string }{
		{"2.24.0", "unavailable (not returned)"},
		{"3.1.0", "null"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			data, err := os.ReadFile("../protocol/testdata/" + tc.version + "/wire.json")
			if err != nil {
				t.Fatal(err)
			}
			// Other responses have different output shapes; decode just the workflow envelope.
			var envelopes struct{ Responses map[string]json.RawMessage }
			if err := json.Unmarshal(data, &envelopes); err != nil {
				t.Fatal(err)
			}
			var response struct{ Output protocol.WorkflowsOutput }
			if err := json.Unmarshal(envelopes.Responses["get_workflow"], &response); err != nil {
				t.Fatal(err)
			}
			for _, field := range WorkflowFields(&response.Output) {
				if field.Name == "attributes" || field.Name == "scheduleName" || field.Name == "applicationName" {
					if field.Value != tc.want {
						t.Errorf("%s = %q, want %q", field.Name, field.Value, tc.want)
					}
				}
			}
		})
	}
}

func TestStepInspectionPreservesMissingNullAndEmpty(t *testing.T) {
	for _, tc := range []struct{ raw, name, output string }{
		{`{"function_id":1}`, "unavailable (not returned)", "unavailable (not returned)"},
		{`{"function_id":1,"function_name":null,"output":null}`, "null", "null"},
		{`{"function_id":1,"function_name":"","output":""}`, `""`, `""`},
	} {
		var step protocol.WorkflowSteps
		if err := json.Unmarshal([]byte(tc.raw), &step); err != nil {
			t.Fatal(err)
		}
		for _, field := range StepFields(step) {
			if field.Name == "stepName" && field.Value != tc.name {
				t.Errorf("name for %s = %q, want %q", tc.raw, field.Value, tc.name)
			}
			if field.Name == "output" && field.Value != tc.output {
				t.Errorf("output for %s = %q, want %q", tc.raw, field.Value, tc.output)
			}
		}
	}
}
