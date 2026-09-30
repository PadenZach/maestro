package web

import (
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"

	"github.com/zpaden/maestro/internal/protocol"
)

// InspectionField keeps SDK values as escaped text, including null and empty.
// Names correspond to the pinned Workflow/Step schema; epoch fields retain the
// SDK's millisecond representation rather than inventing an unavailable date.
type InspectionField struct{ Name, Value, Note string }

func field(name string, value *string) InspectionField {
	v := "null"
	if value != nil {
		v = *value
		if v == "" {
			v = `""`
		}
	}
	return InspectionField{Name: name, Value: v}
}

type inspectionSource interface {
	FieldPresent(string) bool
	FieldNull(string) bool
}

func returnedField(source inspectionSource, name, wireName string, value *string) InspectionField {
	if !source.FieldPresent(wireName) {
		return InspectionField{Name: name, Value: "unavailable (not returned)"}
	}
	if source.FieldNull(wireName) {
		value = nil
	}
	f := field(name, value)
	switch name {
	case "createdAt", "updatedAt", "deadline", "dequeuedAt", "delayUntil", "startedAt", "completedAt":
		f.Note = "SDK epoch milliseconds"
	}
	return f
}

func WorkflowFields(w *protocol.WorkflowsOutput) []InspectionField {
	f := func(name, wireName string, value *string) InspectionField {
		return returnedField(w, name, wireName, value)
	}
	forked := strconv.FormatBool(w.WasForkedFrom)
	return []InspectionField{
		f("workflowId", "WorkflowUUID", &w.WorkflowUUID),
		f("status", "Status", w.Status),
		f("workflowName", "WorkflowName", w.WorkflowName),
		f("workflowClass", "WorkflowClassName", w.WorkflowClassName),
		f("workflowConfig", "WorkflowConfigName", w.WorkflowConfigName),
		f("user", "AuthenticatedUser", w.AuthenticatedUser),
		f("assumedRole", "AssumedRole", w.AssumedRole),
		f("roles", "AuthenticatedRoles", w.AuthenticatedRoles),
		f("input", "Input", w.Input),
		f("output", "Output", w.Output),
		f("error", "Error", w.Error),
		f("createdAt", "CreatedAt", w.CreatedAt),
		f("updatedAt", "UpdatedAt", w.UpdatedAt),
		f("queueName", "QueueName", w.QueueName),
		f("appVersion", "ApplicationVersion", w.ApplicationVersion),
		f("executorId", "ExecutorID", w.ExecutorID),
		f("timeoutMs", "WorkflowTimeoutMS", w.WorkflowTimeoutMS),
		f("deadline", "WorkflowDeadlineEpochMS", w.WorkflowDeadlineEpochMS),
		f("deduplicationId", "DeduplicationID", w.DeduplicationID),
		f("priority", "Priority", w.Priority),
		f("queuePartitionKey", "QueuePartitionKey", w.QueuePartitionKey),
		f("forkedFrom", "ForkedFrom", w.ForkedFrom),
		f("wasForkedFrom", "WasForkedFrom", &forked),
		f("parentWorkflowId", "ParentWorkflowID", w.ParentWorkflowID),
		f("dequeuedAt", "DequeuedAt", w.DequeuedAt),
		f("delayUntil", "DelayUntilEpochMS", w.DelayUntilEpochMS),
		f("completedAt", "CompletedAt", w.CompletedAt),
		f("attributes", "Attributes", w.Attributes),
		f("scheduleName", "ScheduleName", w.ScheduleName),
		f("applicationName", "ApplicationName", w.ApplicationName),
	}
}

func stepID(s protocol.WorkflowSteps) string {
	if s.FieldNull("function_id") {
		return "null"
	}
	if !s.HasFunctionID() {
		return "unavailable (not returned)"
	}
	return strconv.Itoa(s.FunctionID)
}

func StepFields(s protocol.WorkflowSteps) []InspectionField {
	f := func(name, wireName string, value *string) InspectionField {
		return returnedField(s, name, wireName, value)
	}
	id := stepID(s)
	return []InspectionField{
		f("stepId", "function_id", &id),
		f("stepName", "function_name", &s.FunctionName),
		f("output", "output", s.Output),
		f("error", "error", s.Error),
		f("childWorkflowId", "child_workflow_id", s.ChildWorkflowID),
		f("startedAt", "started_at_epoch_ms", s.StartedAtEpochMS),
		f("completedAt", "completed_at_epoch_ms", s.CompletedAtEpochMS),
	}
}

func ApplicationURL(app string) string {
	return "/apps/" + url.PathEscape(app)
}

func WorkflowURL(app, id string) string {
	return ApplicationURL(app) + "/workflows/" + url.PathEscape(id)
}

// SetTimelineBranch assigns collision-free state keys using the entire invoking
// ancestry, not just the step number. Ancestors come only from reported IDs.
func SetTimelineBranch(tl *Timeline, branch string, ancestors []string) {
	tl.Depth = len(ancestors)
	ancestors = append(append([]string(nil), ancestors...), tl.WorkflowID)
	if branch == "" {
		branch = identity(tl.App, tl.WorkflowID)
	}
	tl.Branch = branch
	for i := range tl.Rows {
		row := &tl.Rows[i]
		row.Key = identity(branch, tl.App, tl.WorkflowID, row.StepID)
		if row.StepID == "unavailable (not returned)" {
			row.Key = identity(row.Key, strconv.Itoa(i))
		}
		if row.ChildWorkflowID == "" {
			continue
		}
		for _, ancestor := range ancestors {
			if row.ChildWorkflowID == ancestor {
				row.ChildCycle = true
			}
		}
		query := url.Values{
			"branch": {row.Key}, "ancestor": ancestors,
			"window_start": {strconv.FormatInt(tl.StartMS, 10)},
			"window_end":   {strconv.FormatInt(tl.EndMS, 10)},
		}
		row.ChildURL = WorkflowURL(tl.App, row.ChildWorkflowID) + "/timeline?" + query.Encode()
	}
}
func identity(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(b.String()))
}
