"""Run the existing SDK transport checks against the latest stable 2.31.x/3.x releases."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from gate import ROOT, run
from install import run_gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--major", choices=("2", "3"), default=os.environ.get("DBOS_SDK_MAJOR")
    )
    parser.add_argument("--probe-bad-blob", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--python", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--version", help=argparse.SUPPRESS)
    parser.add_argument("--temp", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        binary = args.temp / "maestro"
        subprocess.run(
            ["go", "build", "-o", str(binary), "./cmd/maestro"],
            cwd=ROOT,
            check=True,
            timeout=120,
        )
        run(
            args.version,
            binary,
            args.temp / "data",
            False,
            False,
            args.probe_bad_blob,
            python=args.python,
        )
        if args.major == "3":
            run(
                args.version,
                binary,
                args.temp / "private",
                False,
                True,
                False,
                python=args.python,
            )
        return

    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required; use mise run sdk:test")
    for major in (args.major,) if args.major else ("2", "3"):
        if major not in ("2", "3"):
            raise ValueError("DBOS_SDK_MAJOR must be 2 or 3")
        with tempfile.TemporaryDirectory(prefix=f"maestro-sdk-{major}-") as scratch:
            temp = Path(scratch)
            clean = {
                k: os.environ[k] for k in ("PATH", "SYSTEMROOT") if k in os.environ
            }
            clean.update(
                HOME=scratch,
                XDG_CONFIG_HOME=str(temp / "config"),
                XDG_CACHE_HOME=str(temp / "cache"),
                TMPDIR=scratch,
                UV_PYTHON_INSTALL_DIR=str(temp / "python"),
            )
            subprocess.run(
                [
                    uv,
                    "--no-config",
                    "venv",
                    "--python",
                    "3.12",
                    "--managed-python",
                    str(temp / "venv"),
                ],
                env=clean,
                check=True,
                timeout=180,
            )
            python = temp / "venv/bin/python"
            subprocess.run(
                [
                    uv,
                    "--no-config",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    "--default-index",
                    "https://pypi.org/simple",
                    "--prerelease",
                    "disallow",
                    "dbos>=2.31,<2.32" if major == "2" else "dbos>=3,<4",
                ],
                env=clean,
                check=True,
                timeout=180,
            )
            version = subprocess.check_output(
                [
                    str(python),
                    "-I",
                    "-c",
                    "import importlib.metadata as m; print(m.version('dbos'))",
                ],
                env=clean,
                text=True,
                timeout=10,
            ).strip()
            assert version.split(".")[0] == major, (
                "resolved SDK outside requested major"
            )
            packages = json.loads(
                subprocess.check_output(
                    [
                        uv,
                        "--no-config",
                        "pip",
                        "list",
                        "--python",
                        str(python),
                        "--format",
                        "json",
                    ],
                    env=clean,
                    text=True,
                    timeout=10,
                )
            )
            (ROOT / "dist").mkdir(exist_ok=True)
            (ROOT / "dist" / f"sdk-{major}.json").write_text(
                json.dumps(
                    {"major": major, "dbos": version, "packages": packages}, indent=2
                )
                + "\n"
            )
            series = "2.31.x" if major == "2" else "3.x"
            print(f"Testing latest DBOS {series}: {version}", flush=True)
            command = [
                sys.executable,
                "-B",
                str(Path(__file__).resolve()),
                "--worker",
                "--major",
                major,
                "--python",
                str(python),
                "--version",
                version,
                "--temp",
                scratch,
            ]
            if args.probe_bad_blob:
                command.append("--probe-bad-blob")
            run_gate(command, clean, timeout=330)


if __name__ == "__main__":
    main()
