"""Independent unit coverage for the Postgres gate's Workflow oracle."""

import hashlib
import importlib.util
import json
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("postgres_gate.py")
MODULE_SPEC = importlib.util.spec_from_file_location("postgres_gate", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
postgres_gate = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(postgres_gate)

HTTP_WORKFLOW = {
    "workflowId": "wf-official",
    "status": "SUCCESS",
    "workflowName": "workflow_read_fixture",
    "workflowClass": None,
    "workflowConfig": None,
    "user": "alice",
    "assumedRole": None,
    "roles": '["reader"]',
    "input": "opaque-input",
    "output": "opaque-output",
    "error": None,
    "createdAt": "2024-01-01T00:00:00.123Z",
    "updatedAt": "2024-01-01T00:00:00.456+00:00",
    "queueName": "queue",
    "appVersion": "v1",
    "executorId": "exec",
    "timeoutMs": 0,
    "deadline": None,
    "deduplicationId": None,
    "priority": 7,
    "queuePartitionKey": None,
    "forkedFrom": None,
    "wasForkedFrom": False,
    "parentWorkflowId": None,
    "dequeuedAt": "2024-01-01T00:00:00.789Z",
    "delayUntil": None,
    "completedAt": "2024-01-01T00:00:00.999Z",
    "attributes": '{"tenant":"workflow-read"}',
    "scheduleName": None,
    "applicationName": "app",
}
SDK_WORKFLOW_WIRE = {
    "WorkflowUUID": "wf-official",
    "Status": "SUCCESS",
    "WorkflowName": "workflow_read_fixture",
    "WorkflowClassName": None,
    "WorkflowConfigName": None,
    "AuthenticatedUser": "alice",
    "AssumedRole": None,
    "AuthenticatedRoles": '["reader"]',
    "Input": "opaque-input",
    "Output": "opaque-output",
    "Error": None,
    "CreatedAt": "1704067200123",
    "UpdatedAt": "1704067200456",
    "QueueName": "queue",
    "ApplicationVersion": "v1",
    "ExecutorID": "exec",
    "WorkflowTimeoutMS": "0",
    "WorkflowDeadlineEpochMS": None,
    "DeduplicationID": None,
    "Priority": "7",
    "QueuePartitionKey": None,
    "ForkedFrom": None,
    "WasForkedFrom": False,
    "ParentWorkflowID": None,
    "DequeuedAt": "1704067200789",
    "DelayUntilEpochMS": None,
    "CompletedAt": "1704067200999",
    "Attributes": '{"tenant":"workflow-read"}',
    "ScheduleName": None,
    "ApplicationName": "app",
}


def sdk_digest(wire):
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresWorkflowReadValidationTests(unittest.TestCase):
    def test_preserves_sdk_null_priority_and_updated_at(self):
        workflow = dict(HTTP_WORKFLOW, priority=None, updatedAt=None)
        wire = dict(SDK_WORKFLOW_WIRE, Priority=None, UpdatedAt=None)
        postgres_gate.validate_official_workflow(workflow, sdk_digest(wire))
        for replacement in (
            dict(workflow, priority=0),
            dict(workflow, updatedAt=HTTP_WORKFLOW["createdAt"]),
        ):
            with self.assertRaisesRegex(AssertionError, "field values differ from SDK"):
                postgres_gate.validate_official_workflow(replacement, sdk_digest(wire))

    def test_accepts_exact_pinned_schema_and_sdk_converted_values(self):
        postgres_gate.validate_official_workflow(
            dict(HTTP_WORKFLOW), sdk_digest(SDK_WORKFLOW_WIRE)
        )

    def test_rejects_missing_and_extraneous_fields(self):
        missing = dict(HTTP_WORKFLOW)
        del missing["updatedAt"]
        extra = dict(HTTP_WORKFLOW, undocumented=True)
        for record in (missing, extra):
            with self.subTest(fields=sorted(record)):
                with self.assertRaisesRegex(
                    AssertionError,
                    "Official Workflow fields differ from pinned schema",
                ):
                    postgres_gate.validate_official_workflow(
                        record, sdk_digest(SDK_WORKFLOW_WIRE)
                    )

    def test_rejects_wrong_types_dates_and_integer_bounds(self):
        cases = {
            "nullable status is still required string": ("status", None),
            "priority boolean": ("priority", True),
            "priority overflow": ("priority", 2**31),
            "timeout overflow": ("timeoutMs", 2**63),
            "malformed timestamp": ("updatedAt", "not-a-date"),
            "missing required timestamp value": ("createdAt", None),
            "wasForkedFrom integer": ("wasForkedFrom", 0),
        }
        for name, (field, value) in cases.items():
            with self.subTest(name=name):
                record = dict(HTTP_WORKFLOW)
                record[field] = value
                with self.assertRaisesRegex(
                    AssertionError,
                    f"Official Workflow.{field}|Official Workflow timestamp",
                ):
                    postgres_gate.validate_official_workflow(
                        record, sdk_digest(SDK_WORKFLOW_WIRE)
                    )

    def test_rejects_actual_values_that_differ_from_sdk_wire_digest(self):
        corrupted = dict(HTTP_WORKFLOW)
        corrupted["priority"] += 1
        with self.assertRaisesRegex(
            AssertionError,
            "Official workflow field values differ from SDK WorkflowsOutput",
        ):
            postgres_gate.validate_official_workflow(
                corrupted, sdk_digest(SDK_WORKFLOW_WIRE)
            )


if __name__ == "__main__":
    unittest.main()
