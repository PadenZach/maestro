"""Independent unit coverage for official related-data acceptance oracles."""

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

HTTP_EVENT = {"key": "gate-event", "value": "{'opaque': 1}"}
SDK_EVENT_WIRE = dict(HTTP_EVENT)

HTTP_NOTIFICATION = {
    "topic": None,
    "message": "['opaque']",
    "createdAt": "2024-07-03T09:46:40.123Z",
    "consumed": False,
}
SDK_NOTIFICATION_WIRE = {
    "topic": None,
    "message": "['opaque']",
    "created_at_epoch_ms": 1720000000123,
    "consumed": False,
}

HTTP_STREAM = {
    "key": "gate-stream",
    "values": ["'first'", "{'page': 2}", "['page', 3]"],
}
SDK_STREAM_WIRE = dict(HTTP_STREAM)


def sdk_digest(wire):
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class PostgresRelatedReadValidationTests(unittest.TestCase):
    def test_accepts_exact_pinned_schemas_and_sdk_conversions(self):
        postgres_gate.validate_official_event(
            dict(HTTP_EVENT), sdk_digest(SDK_EVENT_WIRE)
        )
        postgres_gate.validate_official_notification(
            dict(HTTP_NOTIFICATION), sdk_digest(SDK_NOTIFICATION_WIRE)
        )
        postgres_gate.validate_official_stream(
            dict(HTTP_STREAM), sdk_digest(SDK_STREAM_WIRE)
        )

    def test_rejects_missing_and_extraneous_fields(self):
        cases = []
        for validator, record, digest in (
            (
                postgres_gate.validate_official_event,
                HTTP_EVENT,
                sdk_digest(SDK_EVENT_WIRE),
            ),
            (
                postgres_gate.validate_official_notification,
                HTTP_NOTIFICATION,
                sdk_digest(SDK_NOTIFICATION_WIRE),
            ),
            (
                postgres_gate.validate_official_stream,
                HTTP_STREAM,
                sdk_digest(SDK_STREAM_WIRE),
            ),
        ):
            missing = dict(record)
            del missing[next(iter(missing))]
            cases.extend(
                [
                    (validator, missing, digest),
                    (validator, dict(record, undocumented=True), digest),
                ]
            )
        for validator, record, digest in cases:
            with self.subTest(validator=validator.__name__, fields=sorted(record)):
                with self.assertRaisesRegex(
                    AssertionError, "fields differ from pinned schema"
                ):
                    validator(record, digest)

    def test_rejects_wrong_types_nullable_violations_and_bad_dates(self):
        cases = (
            (
                postgres_gate.validate_official_event,
                dict(HTTP_EVENT, key=None),
                sdk_digest(SDK_EVENT_WIRE),
                "Official Event.key violates pinned schema",
            ),
            (
                postgres_gate.validate_official_event,
                dict(HTTP_EVENT, value={"decoded": True}),
                sdk_digest(SDK_EVENT_WIRE),
                "Official Event.value violates pinned schema",
            ),
            (
                postgres_gate.validate_official_notification,
                dict(HTTP_NOTIFICATION, consumed=0),
                sdk_digest(SDK_NOTIFICATION_WIRE),
                "Official Notification.consumed violates pinned schema",
            ),
            (
                postgres_gate.validate_official_notification,
                dict(HTTP_NOTIFICATION, createdAt="2024-07-03T09:46:40"),
                sdk_digest(SDK_NOTIFICATION_WIRE),
                "Official Notification.createdAt violates pinned schema",
            ),
            (
                postgres_gate.validate_official_notification,
                dict(HTTP_NOTIFICATION, message=None),
                sdk_digest(SDK_NOTIFICATION_WIRE),
                "Official Notification.message violates pinned schema",
            ),
            (
                postgres_gate.validate_official_stream,
                dict(HTTP_STREAM, values=None),
                sdk_digest(SDK_STREAM_WIRE),
                "Official StreamEntry.values violates pinned schema",
            ),
            (
                postgres_gate.validate_official_stream,
                dict(HTTP_STREAM, values=["first", 2]),
                sdk_digest(SDK_STREAM_WIRE),
                "Official StreamEntry.values violates pinned schema",
            ),
        )
        for validator, record, digest, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(AssertionError, message):
                    validator(record, digest)

    def test_preserves_required_null_false_zero_and_empty_values(self):
        notification = dict(
            HTTP_NOTIFICATION,
            topic=None,
            message="",
            createdAt="1970-01-01T00:00:00.000Z",
            consumed=False,
        )
        notification_wire = {
            "topic": None,
            "message": "",
            "created_at_epoch_ms": 0,
            "consumed": False,
        }
        postgres_gate.validate_official_notification(
            notification, sdk_digest(notification_wire)
        )
        postgres_gate.validate_official_event(
            {"key": "", "value": ""}, sdk_digest({"key": "", "value": ""})
        )
        postgres_gate.validate_official_stream(
            {"key": "", "values": []}, sdk_digest({"key": "", "values": []})
        )

    def test_rejects_values_that_differ_from_sdk_wire_digests(self):
        for validator, record, message in (
            (
                postgres_gate.validate_official_event,
                HTTP_EVENT,
                "Official event field values differ from SDK EventOutput",
            ),
            (
                postgres_gate.validate_official_notification,
                HTTP_NOTIFICATION,
                "Official notification field values differ from SDK NotificationOutput",
            ),
            (
                postgres_gate.validate_official_stream,
                HTTP_STREAM,
                "Official stream field values differ from SDK StreamEntryOutput",
            ),
        ):
            with self.subTest(validator=validator.__name__):
                with self.assertRaisesRegex(AssertionError, message):
                    validator(dict(record), "0" * 64)


if __name__ == "__main__":
    unittest.main()
