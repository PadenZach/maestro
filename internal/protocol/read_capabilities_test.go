package protocol

import "testing"

func TestInspectionReadCommandsRequireReviewedPython310Capabilities(t *testing.T) {
	commands := []MessageType{
		MsgListSchedules,
		MsgGetSchedule,
		MsgGetWorkflowAggregates,
		MsgGetStepAggregates,
		MsgExportWorkflow,
	}
	for _, command := range commands {
		for _, discriminator := range []any{string(command), command} {
			t.Run(string(command)+"/discriminator", func(t *testing.T) {
				features, err := RequiredFeatures(Request{"type": discriminator})
				if err != nil {
					t.Fatal(err)
				}
				if len(features) != 1 {
					t.Fatalf("required features = %v, want one command capability", features)
				}
				feature := features[0]
				if !SupportsFeature("python", "3.1.0", feature) {
					t.Fatalf("Python 3.1.0 does not support %q", feature)
				}
				for _, unsupported := range []struct {
					language string
					version  string
				}{
					{"python", "3.1.1"},
					{"python", "3.0.0"},
					{"python", "2.31.1"},
					{"typescript", "3.1.0"},
					{"", "3.1.0"},
				} {
					if SupportsFeature(unsupported.language, unsupported.version, feature) {
						t.Fatalf("unreviewed %q %q supports %q", unsupported.language, unsupported.version, feature)
					}
				}
			})
		}
	}
}
