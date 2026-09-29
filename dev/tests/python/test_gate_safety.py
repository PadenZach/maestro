"""Negative controls for isolated real-Python gate transport and process ownership."""

import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from install import main as install_gate
from install import run_gate

ROOT = Path(__file__).resolve().parents[3]
LISTENER_CHILD = (
    "import json,os,pathlib,socket,sys,time; "
    "s=socket.socket(); s.bind(('127.0.0.1',0)); s.listen(); "
    "path=pathlib.Path(sys.argv[1]); tmp=path.with_suffix('.tmp'); "
    "tmp.write_text(json.dumps({'pid':os.getpid(), 'port':s.getsockname()[1]})); "
    "tmp.replace(path); time.sleep(120)"
)


class Proxy(http.server.ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), ProxyHandler)
        self.paths = []


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        cast(Proxy, self.server).paths.append(self.path)
        self.send_response(418)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, format, *args):
        pass


class GateSafetyTests(unittest.TestCase):
    def test_nonzero_already_gone_group_preserves_status(self):
        with self.assertRaises(subprocess.CalledProcessError) as failure:
            run_gate(
                [sys.executable, "-c", "import sys; sys.exit(42)"],
                {"PATH": os.environ.get("PATH", "")},
                timeout=3,
            )
        self.assertEqual(failure.exception.returncode, 42)

    def test_success_without_children_remains_successful(self):
        run_gate(
            [sys.executable, "-c", "pass"],
            {"PATH": os.environ.get("PATH", "")},
            timeout=3,
        )

    def test_nonzero_gate_exit_reaps_owned_listener_and_pid(self):
        child_code = LISTENER_CHILD
        parent_code = (
            "import pathlib,subprocess,sys,time; "
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r},sys.argv[1]]); "
            "end=time.monotonic()+3; "
            "exec('while not pathlib.Path(sys.argv[1]).exists() and time.monotonic()<end: "
            "time.sleep(0.01)\\n'); "
            "sys.exit(42 if pathlib.Path(sys.argv[1]).exists() else 43)"
        )
        with tempfile.TemporaryDirectory(prefix="maestro-gate-nonzero-") as tmp:
            ready = Path(tmp) / "listener.json"
            pid = None
            try:
                with self.assertRaises(subprocess.CalledProcessError) as failure:
                    run_gate(
                        [sys.executable, "-c", parent_code, str(ready)],
                        {"PATH": os.environ.get("PATH", "")},
                        timeout=6,
                    )
                self.assertEqual(failure.exception.returncode, 42)
                info = json.loads(ready.read_text())
                pid = info["pid"]
                with socket.socket() as conn:
                    conn.settimeout(0.5)
                    self.assertNotEqual(
                        conn.connect_ex(("127.0.0.1", info["port"])),
                        0,
                        "owned loopback listener survived nonzero gate exit",
                    )
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    self.fail("owned child PID survived nonzero gate exit")
            finally:
                if pid is None and ready.exists():
                    pid = json.loads(ready.read_text())["pid"]
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_success_gate_exit_reaps_owned_listener_and_pid(self):
        child_code = LISTENER_CHILD
        parent_code = (
            "import pathlib,subprocess,sys,time; "
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r},sys.argv[1]]); "
            "end=time.monotonic()+3; "
            "exec('while not pathlib.Path(sys.argv[1]).exists() and time.monotonic()<end: "
            "time.sleep(0.01)\\n'); "
            "sys.exit(0 if pathlib.Path(sys.argv[1]).exists() else 43)"
        )
        with tempfile.TemporaryDirectory(prefix="maestro-gate-success-") as tmp:
            ready = Path(tmp) / "listener.json"
            pid = None
            try:
                run_gate(
                    [sys.executable, "-c", parent_code, str(ready)],
                    {"PATH": os.environ.get("PATH", "")},
                    timeout=6,
                )
                info = json.loads(ready.read_text())
                pid = info["pid"]
                with socket.socket() as conn:
                    conn.settimeout(0.5)
                    self.assertNotEqual(
                        conn.connect_ex(("127.0.0.1", info["port"])),
                        0,
                        "owned loopback listener survived successful gate exit",
                    )
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    self.fail("owned child PID survived successful gate exit")
            finally:
                if pid is None and ready.exists():
                    pid = json.loads(ready.read_text())["pid"]
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_interruption_reaps_owned_listener_and_pid(self):
        child_code = LISTENER_CHILD
        parent_code = (
            "import subprocess,sys,time; "
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r},sys.argv[1]]); "
            "time.sleep(120)"
        )
        with tempfile.TemporaryDirectory(prefix="maestro-gate-interrupt-") as tmp:
            ready = Path(tmp) / "listener.json"
            pid = None
            real_waitid = os.waitid

            def interrupt_after_ready(*args):
                end = time.monotonic() + 3
                while not ready.is_file() and time.monotonic() < end:
                    time.sleep(0.01)
                if not ready.is_file():
                    return real_waitid(*args)
                raise KeyboardInterrupt()

            try:
                with patch("install.os.waitid", side_effect=interrupt_after_ready):
                    with self.assertRaises(KeyboardInterrupt):
                        run_gate(
                            [sys.executable, "-c", parent_code, str(ready)],
                            {"PATH": os.environ.get("PATH", "")},
                            timeout=6,
                        )
                self.assertTrue(
                    ready.is_file(), "interrupt negative control child never listened"
                )
                info = json.loads(ready.read_text())
                pid = info["pid"]
                with socket.socket() as conn:
                    conn.settimeout(0.5)
                    self.assertNotEqual(
                        conn.connect_ex(("127.0.0.1", info["port"])),
                        0,
                        "owned loopback listener survived interruption",
                    )
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            finally:
                if pid is None and ready.exists():
                    pid = json.loads(ready.read_text())["pid"]
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_outer_deadline_reaps_owned_listener_and_pid(self):
        child_code = LISTENER_CHILD
        parent_code = (
            "import subprocess,sys,time; "
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r},sys.argv[1]]); "
            "time.sleep(120)"
        )
        with tempfile.TemporaryDirectory(prefix="maestro-gate-deadline-") as tmp:
            ready = Path(tmp) / "listener.json"
            pid = None
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    run_gate(
                        [sys.executable, "-c", parent_code, str(ready)],
                        {"PATH": os.environ.get("PATH", "")},
                        timeout=3,
                    )
                self.assertTrue(
                    ready.is_file(), "negative control child never listened"
                )
                info = json.loads(ready.read_text())
                pid = info["pid"]
                with socket.socket() as conn:
                    conn.settimeout(0.5)
                    self.assertNotEqual(
                        conn.connect_ex(("127.0.0.1", info["port"])),
                        0,
                        "owned loopback listener survived gate timeout",
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

    def test_actual_gate_does_not_contact_inherited_proxy(self):
        proxy = Proxy()
        server = threading.Thread(target=proxy.serve_forever)
        server.start()
        try:
            env = os.environ.copy()
            env.update(
                HTTP_PROXY=f"http://127.0.0.1:{proxy.server_port}",
                HTTPS_PROXY=f"http://127.0.0.1:{proxy.server_port}",
                ALL_PROXY=f"http://127.0.0.1:{proxy.server_port}",
                NO_PROXY="",
                no_proxy="",
            )
            run_gate(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    str(ROOT / "dev/tests/python/gate.py"),
                    "--version",
                    "2.24.0",
                ],
                env,
                timeout=125,
            )
            self.assertEqual(proxy.paths, [], "focused gate used operator proxy")
            # Run the installer in-process: its own session supervisor owns gate.py;
            # an additional subprocess.run timeout here would orphan that session.
            with patch.dict(os.environ, env, clear=True):
                install_gate()
            self.assertEqual(proxy.paths, [], "installer gate used operator proxy")
        finally:
            proxy.shutdown()
            server.join(timeout=3)
            proxy.server_close()


if __name__ == "__main__":
    unittest.main()
