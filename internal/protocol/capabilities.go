package protocol

import (
	"fmt"
	"regexp"
	"strconv"
)

// Feature names distinguish wire compatibility from application_version (which
// names user code). Only Python definition AND handler pairs at these exact
// reviewed release tags are eligible; no range or unreviewed SDK is inferred.
type Feature string

const (
	FeatureWorkflowFilters    Feature = "recent workflow filters"
	FeatureQueueAppFilter     Feature = "queue application filter"
	FeatureRestart            Feature = "legacy restart"
	FeatureRewind             Feature = "rewind"
	FeatureScheduleReads      Feature = "schedule reads"
	FeatureWorkflowAggregates Feature = "workflow aggregates"
	FeatureStepAggregates     Feature = "step aggregates"
	FeatureWorkflowExport     Feature = "workflow export"
)

var sdkSemver = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func SupportsFeature(language, sdkVersion string, feature Feature) bool {
	if language != "python" {
		return false
	}
	parts := sdkSemver.FindStringSubmatch(sdkVersion)
	if parts == nil {
		return false
	}
	major, e1 := strconv.Atoi(parts[1])
	minor, e2 := strconv.Atoi(parts[2])
	patch, e3 := strconv.Atoi(parts[3])
	if e1 != nil || e2 != nil || e3 != nil {
		return false
	}
	switch feature {
	case FeatureWorkflowFilters, FeatureQueueAppFilter:
		return major == 2 && minor == 31 && patch == 1 || major == 3 && minor == 1 && patch == 0
	case FeatureRestart:
		return major == 2 && minor == 24 && patch == 0 || major == 2 && minor == 31 && patch == 1
	case FeatureRewind:
		return major == 3 && minor == 1 && patch == 0
	case FeatureScheduleReads, FeatureWorkflowAggregates, FeatureStepAggregates, FeatureWorkflowExport:
		return major == 3 && minor == 1 && patch == 0
	}
	return false
}

// RequiredFeatures determines which additive fields/commands cannot be sent
// to a peer that ignores unknown inputs. It inspects the actual request frame,
// not a caller assertion, so selection and read retries cannot evade the gate.
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
	case MsgListSchedules, MsgGetSchedule:
		return []Feature{FeatureScheduleReads}, nil
	case MsgGetWorkflowAggregates:
		return []Feature{FeatureWorkflowAggregates}, nil
	case MsgGetStepAggregates:
		return []Feature{FeatureStepAggregates}, nil
	case MsgExportWorkflow:
		return []Feature{FeatureWorkflowExport}, nil
	case MsgListWorkflows, MsgListQueuedWorkflows, MsgListQueues:
		body, ok := req["body"].(map[string]any)
		if !ok && req["body"] != nil {
			return nil, fmt.Errorf("unsupported %s body representation", typ)
		}
		if MessageType(typ) == MsgListQueues {
			if _, present := body["application_name"]; present {
				return []Feature{FeatureQueueAppFilter}, nil
			}
			return nil, nil
		}
		for _, field := range []string{"attributes", "schedule_name", "application_name"} {
			if _, present := body[field]; present {
				return []Feature{FeatureWorkflowFilters}, nil
			}
		}
	}
	return nil, nil
}
