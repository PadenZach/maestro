package protocol

import (
	"encoding/json"
	"testing"
)

// The Python RecoveryRequest carries executor_ids at the top level, and its
// RecoveryResponse acknowledges dispatch with success, not workflow completion.
func TestRecoveryWireContract(t *testing.T) {
	data, err := json.Marshal(RecoveryRequest([]string{"old-1", "old-2"}))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"executor_ids":["old-1","old-2"],"type":"recovery"}` {
		t.Fatalf("recovery frame: %s", data)
	}
	for _, tc := range []struct {
		data string
		ok   bool
	}{
		{`{"success":true}`, true},
		{`{"success":false}`, false},
		{`{}`, false},
		{`{"success":null}`, false},
		{`{"success":true,"error_message":"failed"}`, false},
	} {
		var response SuccessResponse
		if err := json.Unmarshal([]byte(tc.data), &response); err != nil {
			t.Fatal(err)
		}
		if (response.Err() == nil) != tc.ok {
			t.Errorf("response %s: %v", tc.data, response.Err())
		}
	}
}
