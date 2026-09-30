"""Application-side configuration checks for the DBOS Python SDK."""

import copy
import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "maestro_connection.py"
MODULE_SPEC = importlib.util.spec_from_file_location("maestro_connection", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
maestro_connection = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(maestro_connection)
configure_conductor = maestro_connection.configure_conductor


class ConfigureConductorTests(unittest.TestCase):
    def test_explicit_environment_overrides_connection_fields_and_preserves_config(self):
        workflow_settings = {"max_recovery_attempts": 12}
        config = {
            "name": "orders",
            "system_database_url": "postgresql://app@db/orders",
            "workflow_settings": workflow_settings,
            "conductor_url": "wss://stale.example.test",
            "conductor_key": "stale-key",
        }
        original_config = copy.deepcopy(config)
        supplied_environment = {
            "DBOS_CONDUCTOR_URL": "wss://maestro.example.test/conductor/v1alpha1",
            "DBOS_CONDUCTOR_KEY": "current-key",
            "POSTGRES_GATE_TEST_WS": "ws://fixture.invalid",
            "GATE_WS": "ws://fixture.invalid",
        }
        original_environment = supplied_environment.copy()

        with patch.dict(
            os.environ,
            {
                "DBOS_CONDUCTOR_URL": "wss://process.example.test",
                "DBOS_CONDUCTOR_KEY": "process-key",
            },
            clear=False,
        ):
            configured = configure_conductor(config, supplied_environment)

        self.assertIsNot(configured, config)
        self.assertEqual(
            configured["conductor_url"],
            "wss://maestro.example.test/conductor/v1alpha1",
        )
        self.assertEqual(configured["conductor_key"], "current-key")
        self.assertEqual(configured["name"], "orders")
        self.assertEqual(
            configured["system_database_url"], "postgresql://app@db/orders"
        )
        self.assertIs(configured["workflow_settings"], workflow_settings)
        self.assertEqual(config, original_config)
        self.assertEqual(supplied_environment, original_environment)

    def test_omitted_environment_reads_process_environment(self):
        with patch.dict(
            os.environ,
            {
                "DBOS_CONDUCTOR_URL": "wss://maestro.example.test",
                "DBOS_CONDUCTOR_KEY": "process-key",
            },
            clear=True,
        ):
            configured = configure_conductor({"name": "orders"})

        self.assertEqual(configured["conductor_url"], "wss://maestro.example.test")
        self.assertEqual(configured["conductor_key"], "process-key")

    def test_invalid_urls_fail_without_fallback_or_mutation_and_hide_secrets(self):
        invalid_urls = {
            "missing": None,
            "empty": "",
            "whitespace": " \t ",
            "hostless": "wss:///conductor",
            "malformed-host": "wss://[broken-host",
            "malformed-port": "wss://maestro.example.test:not-a-port",
            "query": "wss://maestro.example.test?token=url-secret",
            "empty-query": "wss://maestro.example.test?",
            "fragment": "wss://maestro.example.test#url-secret",
            "empty-fragment": "wss://maestro.example.test#",
            "plaintext-websocket": "ws://maestro.example.test",
            "https": "https://maestro.example.test",
        }
        for label, invalid_url in invalid_urls.items():
            with self.subTest(label=label):
                config = {
                    "name": "orders",
                    "conductor_url": "wss://must-not-be-used.example.test",
                    "conductor_key": "old-config-secret",
                    "database": {"password": "database-secret"},
                }
                supplied_environment = {
                    "DBOS_CONDUCTOR_KEY": "environment-secret",
                    "POSTGRES_GATE_TEST_WS": "ws://postgres-fixture.invalid",
                    "GATE_WS": "ws://gate-fixture.invalid",
                }
                if invalid_url is not None:
                    supplied_environment["DBOS_CONDUCTOR_URL"] = invalid_url
                original_config = copy.deepcopy(config)
                original_environment = supplied_environment.copy()

                with self.assertRaises(ValueError) as failure:
                    configure_conductor(config, supplied_environment)

                error = str(failure.exception)
                self.assertNotIn("environment-secret", error)
                self.assertNotIn("old-config-secret", error)
                self.assertNotIn("database-secret", error)
                self.assertNotIn("url-secret", error)
                if invalid_url:
                    self.assertNotIn(invalid_url, error)
                self.assertEqual(config, original_config)
                self.assertEqual(supplied_environment, original_environment)

    def test_truthy_metadata_only_values_are_refused_without_rewriting(self):
        truthy_values = (
            ("boolean", True),
            ("integer", 1),
            ("string", "true"),
            ("container", {"private": "metadata-secret"}),
        )
        supplied_environment = {
            "DBOS_CONDUCTOR_URL": "wss://maestro.example.test",
            "DBOS_CONDUCTOR_KEY": "environment-secret",
        }
        original_environment = supplied_environment.copy()

        for label, value in truthy_values:
            with self.subTest(label=label):
                config = {
                    "name": "orders",
                    "conductor_metadata_only_mode": value,
                    "database_password": "database-secret",
                }
                original_config = copy.deepcopy(config)

                with self.assertRaises(ValueError) as failure:
                    configure_conductor(config, supplied_environment)

                error = str(failure.exception)
                self.assertIn("Metadata-only mode", error)
                self.assertNotIn("environment-secret", error)
                self.assertNotIn("database-secret", error)
                self.assertNotIn("metadata-secret", error)
                self.assertEqual(config, original_config)
                self.assertEqual(supplied_environment, original_environment)

    def test_falsey_metadata_only_values_are_accepted_and_preserved(self):
        supplied_environment = {
            "DBOS_CONDUCTOR_URL": "wss://maestro.example.test",
            "DBOS_CONDUCTOR_KEY": "environment-key",
        }
        configs = (
            ("omitted", {"name": "omitted"}),
            ("false", {"name": "false", "conductor_metadata_only_mode": False}),
            ("none", {"name": "none", "conductor_metadata_only_mode": None}),
            ("zero", {"name": "zero", "conductor_metadata_only_mode": 0}),
        )

        for label, config in configs:
            with self.subTest(label=label):
                original_config = config.copy()
                configured = configure_conductor(config, supplied_environment)
                if "conductor_metadata_only_mode" in config:
                    self.assertIs(
                        configured["conductor_metadata_only_mode"],
                        config["conductor_metadata_only_mode"],
                    )
                else:
                    self.assertNotIn("conductor_metadata_only_mode", configured)
                self.assertEqual(config, original_config)
                self.assertIsNone(config.get("conductor_url"))
                self.assertIsNone(config.get("conductor_key"))

    def test_conductor_key_is_required_from_environment_without_format_rules(self):
        config = {"name": "orders"}
        supplied_environment = {
            "DBOS_CONDUCTOR_URL": "wss://maestro.example.test"
        }
        with self.assertRaises(KeyError) as failure:
            configure_conductor(config, supplied_environment)
        self.assertEqual(failure.exception.args, ("DBOS_CONDUCTOR_KEY",))
        self.assertEqual(config, {"name": "orders"})
        self.assertEqual(
            supplied_environment,
            {"DBOS_CONDUCTOR_URL": "wss://maestro.example.test"},
        )

        opaque_key = "not-a-jwt and not otherwise interpreted"
        configured = configure_conductor(
            config,
            {
                "DBOS_CONDUCTOR_URL": "wss://maestro.example.test",
                "DBOS_CONDUCTOR_KEY": opaque_key,
            },
        )
        self.assertEqual(configured["conductor_key"], opaque_key)


if __name__ == "__main__":
    unittest.main()
