package protocol

import "testing"

func TestRelatedReadsDoNotRequireVersionCapabilities(t *testing.T) {
	for _, command := range []MessageType{
		MsgGetWorkflowEvents,
		MsgGetWorkflowNotifications,
		MsgGetWorkflowStreams,
	} {
		features, err := RequiredFeatures(NewRequest(command))
		if err != nil {
			t.Fatal(err)
		}
		if len(features) != 0 {
			t.Fatalf("%s features = %v, want no version gate", command, features)
		}
	}
}
