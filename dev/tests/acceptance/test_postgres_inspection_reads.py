"""Independent unit coverage for aggregate and opaque-export acceptance oracles."""

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

HTTP_WORKFLOW_AGGREGATES = [
    {
        "group": {"status": "SUCCESS", "queue_name": None, "name": "gate_workflow"},
        "count": 0,
        "minCreatedAt": "2024-07-03T09:46:40.123Z",
        "maxQueueWaitMs": 0,
        "maxTotalLatencyMs": 0,
    }
]
SDK_WORKFLOW_AGGREGATES = [
    {
        "group": {"status": "SUCCESS", "queue_name": None, "name": "gate_workflow"},
        "count": 0,
        "min_created_at": 1720000000123,
        "max_queue_wait_ms": 0,
        "max_total_latency_ms": 0,
    }
]
HTTP_STEP_AGGREGATES = [
    {
        "group": {"function_name": "gate_step", "status": None},
        "count": 0,
        "maxDurationMs": 0,
    }
]
SDK_STEP_AGGREGATES = [
    {
        "group": {"function_name": "gate_step", "status": None},
        "count": 0,
        "max_duration_ms": 0,
    }
]


def sdk_digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresInspectionReadValidationTests(unittest.TestCase):
    def test_accepts_pinned_aggregate_schemas_and_sdk_conversions(self):
        postgres_gate.validate_official_workflow_aggregates(
            HTTP_WORKFLOW_AGGREGATES,
            sdk_digest(SDK_WORKFLOW_AGGREGATES),
        )
        postgres_gate.validate_official_step_aggregates(
            HTTP_STEP_AGGREGATES,
            sdk_digest(SDK_STEP_AGGREGATES),
        )

    def test_accepts_empty_groups_zero_measures_and_omitted_nullable_wire_measures(self):
        workflow_http = [{"group": {"nullable": None}, "count": 0}]
        workflow_wire = [
            {
                "group": {"nullable": None},
                "count": 0,
                "min_created_at": None,
                "max_queue_wait_ms": None,
                "max_total_latency_ms": None,
            }
        ]
        postgres_gate.validate_official_workflow_aggregates(
            workflow_http, sdk_digest(workflow_wire)
        )
        step_http = [{"group": {}, "maxDurationMs": 0}]
        step_wire = [{"group": {}, "count": None, "max_duration_ms": 0}]
        postgres_gate.validate_official_step_aggregates(
            step_http, sdk_digest(step_wire)
        )
        postgres_gate.validate_official_workflow_aggregates(
            [], sdk_digest([])
        )
        postgres_gate.validate_official_step_aggregates([], sdk_digest([]))

    def test_rejects_missing_extraneous_and_wrong_typed_aggregate_fields(self):
        cases = (
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"count": 1}],
                "fields differ from pinned schema",
            ),
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"group": {}, "undocumented": 1}],
                "fields differ from pinned schema",
            ),
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"group": None}],
                "WorkflowAggregate.group violates pinned schema",
            ),
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"group": {"status": 1}}],
                "WorkflowAggregate.group violates pinned schema",
            ),
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"group": {}, "count": False}],
                "WorkflowAggregate.count violates pinned schema",
            ),
            (
                postgres_gate.validate_official_workflow_aggregates,
                [{"group": {}, "minCreatedAt": "2024-07-03T09:46:40"}],
                "WorkflowAggregate.minCreatedAt violates pinned schema",
            ),
            (
                postgres_gate.validate_official_step_aggregates,
                [{"group": {}, "maxDurationMs": 1.5}],
                "StepAggregate.maxDurationMs violates pinned schema",
            ),
        )
        for validator, value, message in cases:
            with self.subTest(validator=validator.__name__, message=message):
                with self.assertRaisesRegex(AssertionError, message):
                    validator(value, "0" * 64)

    def test_rejects_aggregate_values_that_differ_from_sdk_wire_digests(self):
        corrupt_workflow = json.loads(json.dumps(HTTP_WORKFLOW_AGGREGATES))
        corrupt_workflow[0]["count"] = 1
        with self.assertRaisesRegex(
            AssertionError,
            "Official workflow aggregate field values differ from SDK WorkflowAggregateOutput",
        ):
            postgres_gate.validate_official_workflow_aggregates(
                corrupt_workflow, sdk_digest(SDK_WORKFLOW_AGGREGATES)
            )

        corrupt_step = json.loads(json.dumps(HTTP_STEP_AGGREGATES))
        corrupt_step[0]["maxDurationMs"] = 1
        with self.assertRaisesRegex(
            AssertionError,
            "Official step aggregate field values differ from SDK StepAggregateOutput",
        ):
            postgres_gate.validate_official_step_aggregates(
                corrupt_step, sdk_digest(SDK_STEP_AGGREGATES)
            )

    def test_export_validator_preserves_exact_opaque_string(self):
        opaque = "opaque<字>\\u0000/base64=="
        digest = hashlib.sha256(opaque.encode()).hexdigest()
        postgres_gate.validate_official_export(
            {"serializedWorkflow": opaque}, digest
        )
        for value, message in (
            ({}, "Official export fields differ from pinned schema"),
            (
                {"serializedWorkflow": opaque, "decoded": {}},
                "Official export fields differ from pinned schema",
            ),
            (
                {"serializedWorkflow": None},
                "Official export serializedWorkflow violates pinned schema",
            ),
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(AssertionError, message):
                    postgres_gate.validate_official_export(value, digest)
        with self.assertRaisesRegex(
            AssertionError,
            "Official export bytes differ from observed SDK export frame",
        ):
            postgres_gate.validate_official_export(
                {"serializedWorkflow": opaque + "corrupt"}, digest
            )

    def test_epoch_conversion_accepts_offsets_and_rejects_submilliseconds(self):
        offset_http = [
            {
                "group": {},
                "minCreatedAt": "2024-07-03T11:46:40.123+02:00",
            }
        ]
        offset_wire = [
            {
                "group": {},
                "count": None,
                "min_created_at": 1720000000123,
                "max_queue_wait_ms": None,
                "max_total_latency_ms": None,
            }
        ]
        postgres_gate.validate_official_workflow_aggregates(
            offset_http, sdk_digest(offset_wire)
        )
        bad = [{"group": {}, "minCreatedAt": "2024-07-03T09:46:40.123456Z"}]
        with self.assertRaisesRegex(
            AssertionError,
            "Official workflow aggregate field values differ from SDK WorkflowAggregateOutput",
        ):
            postgres_gate.validate_official_workflow_aggregates(
                bad, sdk_digest(offset_wire)
            )


if __name__ == "__main__":
    unittest.main()
