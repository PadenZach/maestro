"""Independent unit coverage for the Postgres gate's official Schedule oracle."""

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

HTTP_SCHEDULE = {
    "scheduleId": "schedule-id-1",
    "scheduleName": "gate-schedule-context",
    "workflowName": "gate_scheduled",
    "workflowClass": None,
    "cronExpression": "0 0 1 1 *",
    "status": "ACTIVE",
    "context": "opaque-sdk-context",
    "lastFiredAt": "2026-03-01T01:02:03.456-05:00",
    "automaticBackfill": False,
    "cronTimezone": None,
    "applicationName": None,
}
SDK_SCHEDULE_WIRE = {
    "schedule_id": "schedule-id-1",
    "schedule_name": "gate-schedule-context",
    "workflow_name": "gate_scheduled",
    "workflow_class_name": None,
    "schedule": "0 0 1 1 *",
    "status": "ACTIVE",
    "context": "opaque-sdk-context",
    "last_fired_at": "2026-03-01T01:02:03.456-05:00",
    "automatic_backfill": False,
    "cron_timezone": None,
    "application_name": None,
}


def sdk_digest(wire):
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresScheduleReadValidationTests(unittest.TestCase):
    def test_accepts_exact_schema_and_sdk_values_including_nullable_false_and_date(
        self,
    ):
        postgres_gate.validate_official_schedule(
            dict(HTTP_SCHEDULE), sdk_digest(SDK_SCHEDULE_WIRE)
        )

    def test_rejects_missing_and_extraneous_fields(self):
        missing = dict(HTTP_SCHEDULE)
        del missing["applicationName"]
        extra = dict(HTTP_SCHEDULE, queueName="not-in-pinned-http-schema")
        for record in (missing, extra):
            with self.subTest(fields=sorted(record)):
                with self.assertRaisesRegex(
                    AssertionError,
                    "Official Schedule fields differ from pinned schema",
                ):
                    postgres_gate.validate_official_schedule(
                        record, sdk_digest(SDK_SCHEDULE_WIRE)
                    )

    def test_rejects_wrong_types_and_date_format(self):
        cases = {
            "boolean is not a string": ("scheduleId", True),
            "integer is not a boolean": ("automaticBackfill", 0),
            "context type": ("context", 3),
            "workflow class type": ("workflowClass", False),
            "invalid date": ("lastFiredAt", "not-a-date"),
            "date lacks timezone": ("lastFiredAt", "2026-03-01T01:02:03"),
        }
        for name, (field, value) in cases.items():
            with self.subTest(name=name):
                record = dict(HTTP_SCHEDULE)
                record[field] = value
                with self.assertRaisesRegex(
                    AssertionError,
                    f"Official Schedule.{field} violates pinned schema",
                ):
                    postgres_gate.validate_official_schedule(
                        record, sdk_digest(SDK_SCHEDULE_WIRE)
                    )

    def test_rejects_values_that_differ_from_sdk_wire_digest(self):
        with self.assertRaisesRegex(
            AssertionError,
            "Official schedule field values differ from SDK ScheduleOutput",
        ):
            postgres_gate.validate_official_schedule(dict(HTTP_SCHEDULE), "0" * 64)


if __name__ == "__main__":
    unittest.main()
