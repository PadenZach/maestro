package protocol

import "testing"

func TestRelatedReadCapabilitiesRequireExactReviewedHandlers(t *testing.T) {
	for _, command := range []MessageType{
		MsgGetWorkflowEvents,
		MsgGetWorkflowNotifications,
		MsgGetWorkflowStreams,
	} {
		features, err := RequiredFeatures(NewRequest(command))
		if err != nil {
			t.Fatal(err)
		}
		if len(features) != 1 || features[0] != FeatureRelatedReads {
			t.Fatalf("%s features = %v, want %q", command, features, FeatureRelatedReads)
		}
	}
	for _, version := range []string{"2.24.0", "2.31.1", "3.1.0"} {
		if !SupportsFeature("python", version, FeatureRelatedReads) {
			t.Errorf("reviewed Python %s related reads rejected", version)
		}
	}
	for _, peer := range []struct{ language, version string }{
		{"python", "2.24.1"},
		{"python", "2.31.0"},
		{"python", "3.1.1"},
		{"typescript", "3.1.0"},
		{"", "3.1.0"},
	} {
		if SupportsFeature(peer.language, peer.version, FeatureRelatedReads) {
			t.Errorf("unreviewed %q %q supports related reads", peer.language, peer.version)
		}
	}
}
