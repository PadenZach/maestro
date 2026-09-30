package protocol

import "testing"

func TestReadCommandsDoNotRequireSDKVersionCapabilities(t *testing.T) {
	for _, command := range []MessageType{
		MsgListSchedules, MsgGetSchedule, MsgGetWorkflowAggregates,
		MsgGetStepAggregates, MsgExportWorkflow, MsgListWorkflows,
		MsgListQueuedWorkflows, MsgListQueues,
	} {
		for _, discriminator := range []any{string(command), command} {
			t.Run(string(command), func(t *testing.T) {
				features, err := RequiredFeatures(Request{"type": discriminator, "body": map[string]any{
					"attributes": map[string]any{"team": "only"}, "schedule_name": []string{},
					"application_name": []string{"app"}, "has_parent": false,
				}})
				if err != nil {
					t.Fatal(err)
				}
				if len(features) != 0 {
					t.Fatalf("%s blocked behind SDK version capabilities: %v", command, features)
				}
			})
		}
	}
}
