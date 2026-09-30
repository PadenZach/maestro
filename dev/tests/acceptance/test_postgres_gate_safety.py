"""Safety and ownership controls for the opt-in Postgres 18 acceptance gate."""

import importlib.util
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).with_name("postgres_gate.py")
MODULE_SPEC = importlib.util.spec_from_file_location("postgres_gate", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
postgres_gate = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(postgres_gate)

LISTENER_CHILD = (
    "import json,os,pathlib,socket,sys,time; "
    "s=socket.socket(); s.bind(('127.0.0.1',0)); s.listen(); "
    "path=pathlib.Path(sys.argv[1]); tmp=path.with_suffix('.tmp'); "
    "tmp.write_text(json.dumps({'pid':os.getpid(),'port':s.getsockname()[1]})); "
    "tmp.replace(path); time.sleep(120)"
)


class PostgresGateSafetyTests(unittest.TestCase):
    def test_requires_both_explicit_tool_locations(self):
        with self.assertRaisesRegex(RuntimeError, "POSTGRES18_BIN"):
            postgres_gate.required_tool_paths({})
        with self.assertRaisesRegex(RuntimeError, "DBOS_SDK_PYTHON"):
            postgres_gate.required_tool_paths({"POSTGRES18_BIN": "/explicit/pg/bin"})
        pg_bin, python = postgres_gate.required_tool_paths(
            {
                "POSTGRES18_BIN": "/explicit/pg/bin",
                "DBOS_SDK_PYTHON": "/explicit/sdk/python",
            }
        )
        self.assertEqual(pg_bin, Path("/explicit/pg/bin"))
        self.assertEqual(python, Path("/explicit/sdk/python"))

    def test_sanitized_environment_drops_operator_configuration(self):
        inherited = {
            "PATH": "/safe/bin",
            "SYSTEMROOT": "/safe/root",
            "HTTP_PROXY": "http://operator-proxy.invalid",
            "HTTPS_PROXY": "http://operator-proxy.invalid",
            "ALL_PROXY": "socks://operator-proxy.invalid",
            "NO_PROXY": "",
            "PGHOST": "/operator/socket",
            "PGPORT": "5432",
            "PGDATABASE": "operator",
            "PGUSER": "operator",
            "PGPASSWORD": "secret",
            "DATABASE_URL": "postgresql://operator:secret@example.invalid/db",
            "DBOS_CONDUCTOR_URL": "wss://operator.invalid",
            "DBOS_CONDUCTOR_KEY": "secret",
            "PYTHONPATH": "/operator/python",
        }
        with tempfile.TemporaryDirectory(prefix="maestro-pg-env-") as path:
            clean = postgres_gate.sanitized_environment(inherited, Path(path))
        self.assertEqual(clean["PATH"], "/safe/bin")
        self.assertEqual(clean["SYSTEMROOT"], "/safe/root")
        self.assertEqual(clean["PYTHONNOUSERSITE"], "1")
        self.assertEqual(clean["LC_ALL"], "C")
        for name in inherited.keys() - {"PATH", "SYSTEMROOT"}:
            self.assertNotIn(name, clean, name)

    def test_database_url_targets_only_private_socket_and_is_not_sqlite(self):
        url = postgres_gate.postgres_database_url(
            Path("/private/socket"), 49123, "gate_db"
        )
        self.assertTrue(url.startswith("postgresql://gate@/gate_db?"), url)
        self.assertIn("host=%2Fprivate%2Fsocket", url)
        self.assertIn("port=49123", url)
        self.assertNotIn("sqlite", url.lower())
        self.assertNotIn(":password", url)

    def test_postgres_commands_are_cluster_scoped(self):
        pg_bin = Path("/explicit/pg/bin")
        cluster = Path("/owned/cluster")
        socket_dir = Path("/owned/socket")
        init = postgres_gate.initdb_command(pg_bin, cluster)
        start = postgres_gate.postgres_command(pg_bin, cluster, socket_dir, 49123)
        self.assertEqual(init[0], str(pg_bin / "initdb"))
        self.assertIn(str(cluster), init)
        self.assertEqual(start[0], str(pg_bin / "postgres"))
        self.assertEqual(start[start.index("-D") + 1], str(cluster))
        self.assertEqual(start[start.index("-k") + 1], str(socket_dir))
        self.assertEqual(start[start.index("-h") + 1], "")
        self.assertNotIn("127.0.0.1", start)
        self.assertEqual(start[start.index("-p") + 1], "49123")
        self.assertIn("--auth-host=reject", init)
        self.assertNotIn("--auth-host=trust", init)
        self.assertNotIn("pg_ctl", " ".join(init + start))

    def test_nonzero_inner_teardown_is_a_gate_failure(self):
        child = subprocess.Popen([sys.executable, "-c", "import sys; sys.exit(42)"])
        deadline = time.monotonic() + 3
        while child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(child.poll(), 42, "teardown control did not exit")
        with self.assertRaisesRegex(
            AssertionError, "owned child exited with status 42"
        ):
            postgres_gate.stop_process(child)

    def test_cleanup_failure_does_not_replace_primary_assertion(self):
        primary = AssertionError("primary read assertion")
        completed = []

        def fail_late():
            completed.append("failed")
            raise AssertionError("late teardown failure")

        postgres_gate.complete_cleanup(
            primary,
            (
                ("late child", fail_late),
                ("later child", lambda: completed.append("completed")),
            ),
        )
        self.assertEqual(str(primary), "primary read assertion")
        self.assertEqual(completed, ["failed", "completed"])
        self.assertTrue(
            any("late teardown failure" in note for note in primary.__notes__),
            primary.__notes__,
        )

    def test_version_validation_accepts_only_postgres_18_and_reviewed_sdk(self):
        with tempfile.TemporaryDirectory(prefix="maestro-pg-tools-") as path:
            pg_bin = Path(path)
            for name in postgres_gate.POSTGRES_PROGRAMS:
                executable = pg_bin / name
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
            with patch(
                "postgres_gate.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    [], 0, "postgres (PostgreSQL) 18.6\n", ""
                ),
            ):
                postgres_gate.validate_postgres_bin(pg_bin, {"PATH": "/safe/bin"})
            with patch(
                "postgres_gate.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    [], 0, "postgres (PostgreSQL) 17.9\n", ""
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "Postgres 18"):
                    postgres_gate.validate_postgres_bin(pg_bin, {"PATH": "/safe/bin"})

        good_probe = json.dumps(
            {
                "version": "3.1.0",
                "protocol_sha256": postgres_gate.SDK_PROTOCOL_SHA256,
                "handler_sha256": postgres_gate.SDK_HANDLER_SHA256,
            }
        )
        with patch(
            "postgres_gate.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, good_probe, ""),
        ):
            postgres_gate.validate_sdk_python(
                Path(sys.executable), {"PATH": "/safe/bin"}
            )
        bad_probe = json.dumps(
            {
                "version": "3.1.1",
                "protocol_sha256": postgres_gate.SDK_PROTOCOL_SHA256,
                "handler_sha256": postgres_gate.SDK_HANDLER_SHA256,
            }
        )
        with patch(
            "postgres_gate.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, bad_probe, ""),
        ):
            with self.assertRaisesRegex(RuntimeError, "dbos 3.1.0"):
                postgres_gate.validate_sdk_python(
                    Path(sys.executable), {"PATH": "/safe/bin"}
                )
        bad_probe = json.dumps(
            {
                "version": "3.1.0",
                "protocol_sha256": postgres_gate.SDK_PROTOCOL_SHA256,
                "handler_sha256": "0" * 64,
            }
        )
        with patch(
            "postgres_gate.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, bad_probe, ""),
        ):
            with self.assertRaisesRegex(RuntimeError, "definitions or handlers"):
                postgres_gate.validate_sdk_python(
                    Path(sys.executable), {"PATH": "/safe/bin"}
                )

    @unittest.skipUnless(
        os.name == "posix" and hasattr(os, "WNOWAIT"), "POSIX waitid required"
    )
    def test_outer_success_and_failure_reap_owned_process_groups(self):
        for exit_code in (0, 42):
            with self.subTest(exit_code=exit_code):
                parent_code = (
                    "import pathlib,subprocess,sys,time; "
                    f"subprocess.Popen([sys.executable,'-c',{LISTENER_CHILD!r},sys.argv[1]]); "
                    "end=time.monotonic()+3; "
                    "exec('while not pathlib.Path(sys.argv[1]).exists() and "
                    "time.monotonic()<end: time.sleep(0.01)\\n'); "
                    f"sys.exit({exit_code} if pathlib.Path(sys.argv[1]).exists() else 43)"
                )
                with tempfile.TemporaryDirectory(prefix="maestro-pg-exit-") as path:
                    ready = Path(path) / "listener.json"
                    pid = None
                    try:
                        if exit_code == 0:
                            postgres_gate.run_isolated_gate(
                                [sys.executable, "-c", parent_code, str(ready)],
                                {"PATH": os.environ.get("PATH", "")},
                                timeout=6,
                            )
                        else:
                            with self.assertRaises(
                                subprocess.CalledProcessError
                            ) as failure:
                                postgres_gate.run_isolated_gate(
                                    [sys.executable, "-c", parent_code, str(ready)],
                                    {"PATH": os.environ.get("PATH", "")},
                                    timeout=6,
                                )
                            self.assertEqual(failure.exception.returncode, exit_code)
                        info = json.loads(ready.read_text())
                        pid = info["pid"]
                        with socket.socket() as conn:
                            conn.settimeout(0.5)
                            self.assertNotEqual(
                                conn.connect_ex(("127.0.0.1", info["port"])),
                                0,
                                "owned listener survived gate exit",
                            )
                        deadline = time.monotonic() + 3
                        while time.monotonic() < deadline:
                            try:
                                os.kill(pid, 0)
                            except ProcessLookupError:
                                break
                            time.sleep(0.05)
                        else:
                            self.fail("owned child PID survived gate exit")
                    finally:
                        if pid is None and ready.exists():
                            pid = json.loads(ready.read_text())["pid"]
                        if pid is not None:
                            try:
                                os.kill(pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass

    @unittest.skipUnless(
        os.name == "posix" and hasattr(os, "WNOWAIT"), "POSIX waitid required"
    )
    def test_outer_timeout_reaps_owned_process_group(self):
        parent_code = (
            "import subprocess,sys,time; "
            f"subprocess.Popen([sys.executable,'-c',{LISTENER_CHILD!r},sys.argv[1]]); "
            "time.sleep(120)"
        )
        with tempfile.TemporaryDirectory(prefix="maestro-pg-timeout-") as path:
            ready = Path(path) / "listener.json"
            pid = None
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    postgres_gate.run_isolated_gate(
                        [sys.executable, "-c", parent_code, str(ready)],
                        {"PATH": os.environ.get("PATH", "")},
                        timeout=1.5,
                    )
                self.assertTrue(ready.is_file(), "owned listener never became ready")
                info = json.loads(ready.read_text())
                pid = info["pid"]
                with socket.socket() as conn:
                    conn.settimeout(0.5)
                    self.assertNotEqual(
                        conn.connect_ex(("127.0.0.1", info["port"])),
                        0,
                        "owned listener survived gate timeout",
                    )
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    self.fail("owned child PID survived gate timeout")
            finally:
                if pid is None and ready.exists():
                    pid = json.loads(ready.read_text())["pid"]
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
