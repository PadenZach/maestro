package protocol

import "fmt"

// Feature gates are reserved for mutations with known incompatible wire commands.
// Reads are attempted without using SDK identity as a proxy for support. The
// executor's actual response determines their outcome; filters are never dropped.
type Feature string

const (
	FeatureRestart Feature = "legacy restart"
	FeatureRewind  Feature = "rewind"
)

func SupportsFeature(language, sdkVersion string, feature Feature) bool {
	if language != "python" {
		return false
	}
	switch feature {
	case FeatureRestart:
		return sdkVersion == "2.24.0" || sdkVersion == "2.31.1"
	case FeatureRewind:
		return sdkVersion == "3.1.0"
	}
	return false
}

// RequiredFeatures validates the request discriminator and retains the existing
// mutation gates. Read commands have no SDK-version prerequisite.
func RequiredFeatures(req Request) ([]Feature, error) {
	var typ string
	switch value := req["type"].(type) {
	case string:
		typ = value
	case MessageType:
		typ = string(value)
	default:
		return nil, fmt.Errorf("invalid request type")
	}
	if typ == "" {
		return nil, fmt.Errorf("invalid request type")
	}
	switch MessageType(typ) {
	case MsgRestart:
		return []Feature{FeatureRestart}, nil
	case MsgRewindWorkflow:
		return []Feature{FeatureRewind}, nil
	case MsgListWorkflows, MsgListQueuedWorkflows, MsgListQueues:
		if _, ok := req["body"].(map[string]any); !ok && req["body"] != nil {
			return nil, fmt.Errorf("unsupported %s body representation", typ)
		}
	}
	return nil, nil
}
