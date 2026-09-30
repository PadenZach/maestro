import os
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from dbos import DBOS, SetWorkflowID

import app


class DemoRelatedDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = tempfile.TemporaryDirectory(prefix="maestro-demo-test-")
        cls.addClassCleanup(cls.database.cleanup)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        cls.addClassCleanup(environment.stop)
        cls.dbos = DBOS(config={
            "name": "maestro-demo-test",
            "system_database_url": f"sqlite:///{cls.database.name}/demo.sqlite",
            "log_level": "ERROR",
        })
        cls.addClassCleanup(DBOS.destroy)
        DBOS.launch()

    def run_order(self, scenario):
        order = {"id": uuid4().hex, "scenario": scenario, "quantity": 2}
        workflow_id = f"test-order-{order['id']}"
        # Exercise real SDK persistence without the demo's presentation delays.
        with patch.object(DBOS, "sleep", return_value=None):
            with SetWorkflowID(workflow_id):
                handle = DBOS.start_workflow(app.order_workflow, order)
            if scenario == "declined":
                with self.assertRaisesRegex(ValueError, "Demo payment declined"):
                    handle.get_result()
            else:
                result = handle.get_result()
                self.assertEqual(result["payment"]["amount_cents"], 5000)
                self.assertEqual(
                    result["shipment"]["service"],
                    "express" if scenario == "express" else "standard",
                )
        return workflow_id, order

    def assert_related_data(self, scenario):
        workflow_id, order = self.run_order(scenario)
        failed = scenario == "declined"
        terminal = "failed" if failed else "completed"
        events = DBOS.get_all_events(workflow_id)
        self.assertIn("status", events, "Orders must publish durable status events")
        self.assertEqual(events["status"], {"order_id": order["id"], "stage": terminal})
        self.assertEqual(events["order"]["scenario"], scenario)
        self.assertIn("error" if failed else "shipment", events)
        entries = list(DBOS.read_stream(workflow_id, "progress", timeout_seconds=1))
        expected = ["started", "validated", "inventory_ready"]
        if scenario == "review":
            expected.append("review_requested")
        expected += ["failed"] if failed else ["payment_ready", "shipping", "completed"]
        self.assertEqual([entry["stage"] for entry in entries], expected)
        self.assertTrue(all(entry["order_id"] == order["id"] for entry in entries))
        # These are the SDK reads used by its Conductor handlers, not SQL queries.
        messages = self.dbos._sys_db.get_all_notifications(workflow_id)
        self.assertEqual(len(messages), 2)
        self.assertTrue(all(message["topic"] == "customer-updates" for message in messages))
        self.assertTrue(all(not message["consumed"] for message in messages))
        self.assertEqual(
            {message["message"]["stage"] for message in messages}, {"started", terminal}
        )
        children = {
            step["child_workflow_id"] for step in DBOS.list_workflow_steps(workflow_id)
            if step["child_workflow_id"]
        }
        self.assertEqual(len(children), 2 if failed else 3)
        child_statuses = {child_id: DBOS.get_workflow_status(child_id) for child_id in children}
        expected_names = {"inventory_workflow", "payment_workflow"}
        if not failed:
            expected_names.add("shipping_workflow")
        self.assertEqual({status.name for status in child_statuses.values()}, expected_names)
        for child_id in children:
            status = child_statuses[child_id]
            child_terminal = "failed" if failed and status.name == "payment_workflow" else "completed"
            child_events = DBOS.get_all_events(child_id)
            self.assertIn("status", child_events)
            self.assertEqual(child_events["status"]["stage"], child_terminal)
            child_entries = list(DBOS.read_stream(child_id, "progress", timeout_seconds=1))
            self.assertEqual(child_entries[-1], child_events["status"])
            if scenario == "review" and status.name == "payment_workflow":
                self.assertEqual(
                    [entry["stage"] for entry in child_entries],
                    ["started", "awaiting_review", "review_approved", "completed"],
                )
                approval = self.dbos._sys_db.get_all_notifications(child_id)
                self.assertEqual(len(approval), 1)
                self.assertEqual(approval[0]["topic"], "payment-review")
                self.assertTrue(approval[0]["consumed"])
                self.assertEqual(approval[0]["message"], {"order_id": order["id"], "approved": True})

    def test_standard_order_has_events_notifications_and_streams(self):
        self.assert_related_data("standard")

    def test_express_order_has_events_notifications_and_streams(self):
        self.assert_related_data("express")

    def test_review_order_consumes_approval_notification(self):
        self.assert_related_data("review")

    def test_declined_order_retains_failure_data(self):
        self.assert_related_data("declined")


class DemoConnectionTests(unittest.TestCase):
    def test_demo_runs_without_a_configured_key(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("sys.argv", ["app.py", "--no-wait"]), patch.object(app, "DBOS") as dbos:
                dbos.launch.side_effect = RuntimeError("stop before launch")
                try:
                    with self.assertRaisesRegex(RuntimeError, "stop before launch"):
                        app.main()
                except KeyError:
                    self.fail("The local demo must not require a conductor key")
                self.assertEqual(dbos.call_args.kwargs["conductor_key"], "gateway")

    def connection_url(self, overrides):
        # Stop before launching workflows or opening any database/network connection.
        with patch.dict(os.environ, {"MAESTRO_DEMO_KEY": "test-key", **overrides}, clear=True):
            with patch("sys.argv", ["app.py", "--no-wait"]), patch.object(app, "DBOS") as dbos:
                dbos.launch.side_effect = RuntimeError("stop before launch")
                with self.assertRaisesRegex(RuntimeError, "stop before launch"):
                    app.main()
                return dbos.call_args.kwargs["conductor_url"]

    def test_default_connects_to_documented_local_maestro_port(self):
        self.assertEqual(self.connection_url({}), "ws://127.0.0.1:8090")

    def test_explicit_connection_url_is_preserved(self):
        self.assertEqual(
            self.connection_url({"MAESTRO_DEMO_WS_URL": "ws://127.0.0.1:18090"}),
            "ws://127.0.0.1:18090",
        )


if __name__ == "__main__":
    unittest.main()
