"""Independent unit coverage for the Postgres gate's official Queue oracle."""

import hashlib
import importlib.util
import json
import math
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("postgres_gate.py")
MODULE_SPEC = importlib.util.spec_from_file_location("postgres_gate", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
postgres_gate = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(postgres_gate)

HTTP_QUEUE = {
    "name": "edge-queue",
    "concurrency": 0,
    "workerConcurrency": None,
    "rateLimitMax": 0,
    "rateLimitPeriodSecs": 1.25,
    "priorityEnabled": False,
    "partitionQueue": False,
    "pollingIntervalSecs": 0.125,
    "applicationName": None,
    "partitionConcurrency": None,
    "partitionWorkerConcurrency": None,
    "partitionRateLimitMax": None,
    "partitionRateLimitPeriodSecs": None,
}
SDK_QUEUE_WIRE = {
    "name": "edge-queue",
    "concurrency": 0,
    "worker_concurrency": None,
    "rate_limit_max": 0,
    "rate_limit_period_sec": 1.25,
    "priority_enabled": False,
    "partition_queue": False,
    "polling_interval_sec": 0.125,
    "application_name": None,
    "partition_concurrency": None,
    "partition_worker_concurrency": None,
    "partition_rate_limit_max": None,
    "partition_rate_limit_period_sec": None,
}


def sdk_digest(wire):
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresQueueReadValidationTests(unittest.TestCase):
    def test_accepts_exact_schema_and_sdk_values_including_edge_values(self):
        postgres_gate.validate_official_queue(
            dict(HTTP_QUEUE), sdk_digest(SDK_QUEUE_WIRE)
        )

    def test_rejects_missing_and_extraneous_fields(self):
        missing = dict(HTTP_QUEUE)
        del missing["partitionRateLimitPeriodSecs"]
        extra = dict(HTTP_QUEUE, undocumented=True)
        for record in (missing, extra):
            with self.subTest(fields=sorted(record)):
                with self.assertRaisesRegex(
                    AssertionError,
                    "Official Queue fields differ from pinned schema",
                ):
                    postgres_gate.validate_official_queue(
                        record, sdk_digest(SDK_QUEUE_WIRE)
                    )

    def test_rejects_wrong_types_and_formats(self):
        cases = {
            "boolean is not an integer": ("concurrency", True),
            "int32 overflow": ("partitionRateLimitMax", 2**31),
            "integer is not a boolean": ("priorityEnabled", 0),
            "nonfinite is not a JSON number": ("pollingIntervalSecs", math.inf),
            "application name type": ("applicationName", 3),
        }
        for name, (field, value) in cases.items():
            with self.subTest(name=name):
                record = dict(HTTP_QUEUE)
                record[field] = value
                with self.assertRaisesRegex(
                    AssertionError,
                    f"Official Queue.{field} violates pinned schema",
                ):
                    postgres_gate.validate_official_queue(
                        record, sdk_digest(SDK_QUEUE_WIRE)
                    )

    def test_rejects_values_that_differ_from_sdk_wire_digest(self):
        with self.assertRaisesRegex(
            AssertionError,
            "Official queue field values differ from SDK QueueOutput",
        ):
            postgres_gate.validate_official_queue(dict(HTTP_QUEUE), "0" * 64)


if __name__ == "__main__":
    unittest.main()
