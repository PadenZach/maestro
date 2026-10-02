package web

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestRecordedFlowEvidenceAndRelationships(t *testing.T) {
	for _, tc := range []struct {
		name, wire, relation string
		known, failure       bool
		id                   *int
	}{
		{name: "returned error", wire: `{"function_id":4,"function_name":"DBOS.getResult","child_workflow_id":"child","error":"opaque-failure","output":"opaque-output"}`, relation: "return", known: true, failure: true},
		{name: "empty exception error", wire: `{"function_id":0,"function_name":"payment","child_workflow_id":"child","error":""}`, relation: "invocation", known: true, failure: true},
		{name: "returned empty output", wire: `{"function_id":0,"function_name":"payment","output":"","error":null}`, known: true},
		{name: "metadata only outcome unknown", wire: `{"function_id":0,"function_name":"payment","output":null,"error":null}`},
		{name: "unknown error", wire: `{"function_name":"DBOS.futureOperation","child_workflow_id":"child"}`, relation: "reference"},
		{name: "null identity", wire: `{"function_id":null,"function_name":"unknown","error":null}`},
		{name: "missing identity", wire: `{"function_name":"unknown","error":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var step protocol.WorkflowSteps
			if err := json.Unmarshal([]byte(tc.wire), &step); err != nil {
				t.Fatal(err)
			}
			v := RecordedFlowStep("my app", "root/id", 50, step)
			if v.HasError != tc.failure || v.ErrorKnown != tc.known || v.Relationship != tc.relation {
				t.Fatalf("evidence = %+v", v)
			}
			if step.HasFunctionID() {
				if v.ID == nil || *v.ID != step.FunctionID || !strings.Contains(v.InspectURL, "offset=50") || !strings.Contains(v.InspectURL, "root%2Fid") {
					t.Fatalf("inspection = %+v", v)
				}
			} else if v.ID != nil || v.InspectURL != "" {
				t.Fatalf("invented step identity: %+v", v)
			}
			raw, _ := json.Marshal(v)
			if strings.Contains(string(raw), "opaque-") {
				t.Fatalf("opaque payload leaked: %s", raw)
			}
		})
	}
}
