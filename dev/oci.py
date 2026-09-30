"""Build a local OCI layout and attach digest-bound Trivy reports using ORAS."""

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
REPORT_TYPE = "application/vnd.maestro.vulnerability-report.v1+json"
PLATFORMS = {"amd64", "arm64"}


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def output(*args):
    return run(*args, capture_output=True).stdout.strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def read_blob(layout, descriptor):
    algorithm, digest = descriptor["digest"].split(":", 1)
    assert algorithm == "sha256" and re.fullmatch(r"[0-9a-f]{64}", digest)
    data = (layout / "blobs" / algorithm / digest).read_bytes()
    assert len(data) == descriptor["size"], "OCI blob size mismatch"
    assert hashlib.sha256(data).hexdigest() == digest, "OCI blob digest mismatch"
    return data


def blob_json(layout, descriptor):
    return json.loads(read_blob(layout, descriptor))


def image_descriptor(layout):
    entries = json.loads((layout / "index.json").read_text())["manifests"]
    return next(
        d
        for d in entries
        if d.get("annotations", {}).get("org.opencontainers.image.ref.name")
        == "maestro"
    )


def reject_vulnerabilities(report):
    findings = [
        v["VulnerabilityID"]
        for result in report.get("Results", [])
        for v in result.get("Vulnerabilities", [])
        if v["Severity"] in {"HIGH", "CRITICAL"}
    ]
    if findings:
        raise AssertionError(
            "HIGH/CRITICAL vulnerabilities: " + ", ".join(sorted(set(findings)))
        )


def verify(layout):
    """Check artifacts independently of build/scan subprocess exit statuses."""
    assert (
        json.loads((layout / "oci-layout").read_text())["imageLayoutVersion"] == "1.0.0"
    )
    root = image_descriptor(layout)
    index = blob_json(layout, root)
    images = {
        d["platform"]["architecture"]: d
        for d in index["manifests"]
        if d.get("platform", {}).get("os") == "linux"
    }
    assert set(images) == PLATFORMS, "image must contain linux/amd64 and linux/arm64"
    attachments = []
    for descriptor in json.loads((layout / "index.json").read_text())["manifests"]:
        manifest = blob_json(layout, descriptor)
        if manifest.get("artifactType") == REPORT_TYPE:
            assert manifest["subject"]["digest"] == root["digest"], (
                "scan subject differs from image"
            )
            attachments.append(manifest)
    assert len(attachments) == 1, "image must have exactly one attached scan artifact"
    files = {
        layer["annotations"]["org.opencontainers.image.title"]: json.loads(
            read_blob(layout, layer)
        )
        for layer in attachments[0]["layers"]
    }
    summary = files["scan-summary.json"]
    assert summary["subject"] == root["digest"], (
        "scan summary subject differs from image"
    )
    assert summary["scanner"]["Version"], "missing scanner version"
    assert summary["scanner"]["VulnerabilityDB"]["UpdatedAt"], (
        "missing vulnerability database metadata"
    )
    assert set(summary["platforms"]) == PLATFORMS, "missing platform scan metadata"
    for arch, descriptor in images.items():
        manifest = blob_json(layout, descriptor)
        config = blob_json(layout, manifest["config"])
        for layer in manifest["layers"]:
            read_blob(layout, layer)
        assert config["os"] == "linux" and config["architecture"] == arch
        assert config["config"]["User"] == "65532:65532", "image must run as non-root"
        assert config["config"]["Entrypoint"] == ["/maestro"]
        labels = config["config"]["Labels"]
        for key in (
            "title",
            "description",
            "source",
            "url",
            "documentation",
            "version",
            "revision",
            "created",
            "licenses",
        ):
            assert labels.get("org.opencontainers.image." + key), (
                f"missing OCI label: {key}"
            )
        assert labels["org.opencontainers.image.licenses"] == "MIT"
        datetime.fromisoformat(
            labels["org.opencontainers.image.created"].replace("Z", "+00:00")
        )
        assert summary["platforms"][arch] == descriptor["digest"], (
            "scan platform digest mismatch"
        )
        report = files[f"trivy-{arch}.json"]
        assert report["Metadata"]["ImageID"] == manifest["config"]["digest"], (
            "scan image config mismatch"
        )
        binaries = [r for r in report["Results"] if r.get("Type") == "gobinary"]
        assert binaries, "scanner did not find the Go binary"
        packages = {p["Name"] for r in binaries for p in r["Packages"]}
        assert {
            "stdlib",
            "github.com/coder/websocket",
            "github.com/danielgtaylor/huma/v2",
        } <= packages, "scan omitted Go dependencies"
        reject_vulnerabilities(report)
    print(
        "Verified OCI labels, both platforms, Go inventory and attached scan digests",
        flush=True,
    )
    return root


def scan_source():
    DIST.mkdir(exist_ok=True)
    # Only dependency manifests enter this scan, never local demos or SDK checkouts.
    with tempfile.TemporaryDirectory(prefix="maestro-source-scan-") as scratch:
        for name in ("go.mod", "go.sum"):
            shutil.copy2(ROOT / name, scratch)
        run(
            "trivy",
            "fs",
            "--scanners",
            "vuln",
            "--no-progress",
            "--severity",
            "HIGH,CRITICAL",
            "--exit-code",
            "1",
            "--format",
            "json",
            "--output",
            str(DIST / "source-scan.json"),
            scratch,
        )


def build():
    DIST.mkdir(exist_ok=True)
    archive = DIST / "maestro.oci.tar"
    archive.unlink(missing_ok=True)
    revision = output("git", "rev-parse", "HEAD")
    version = os.environ.get("VERSION") or output(
        "git", "describe", "--tags", "--always", "--dirty"
    )
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", version):
        raise ValueError(
            "VERSION must contain only letters, digits, '.', '_', '+', '-'"
        )
    created = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    source = os.environ.get("IMAGE_SOURCE", "https://github.com/zpaden/maestro")
    layout = DIST / "oci"
    if layout.exists():
        shutil.rmtree(layout)
    builder = os.environ.get("BUILDX_BUILDER", "maestro-build")
    if not os.environ.get("BUILDX_BUILDER"):
        found = subprocess.run(
            ["docker", "buildx", "inspect", builder], capture_output=True
        )
        if found.returncode:
            run(
                "docker",
                "buildx",
                "create",
                "--name",
                builder,
                "--driver",
                "docker-container",
            )
    run(
        "docker",
        "buildx",
        "build",
        "--builder",
        builder,
        "--platform",
        "linux/amd64,linux/arm64",
        "--provenance=mode=min",
        "--build-arg",
        f"VERSION={version}",
        "--build-arg",
        f"REVISION={revision}",
        "--build-arg",
        f"CREATED={created}",
        "--build-arg",
        f"SOURCE={source}",
        "--annotation",
        f"index:org.opencontainers.image.source={source}",
        "--annotation",
        f"index:org.opencontainers.image.revision={revision}",
        "--annotation",
        f"index:org.opencontainers.image.version={version}",
        "--output",
        f"type=oci,dest={layout},tar=false",
        str(ROOT),
    )
    root = json.loads((layout / "index.json").read_text())["manifests"][0]
    run("oras", "tag", "--oci-layout", f"{layout}@{root['digest']}", "maestro")
    index = blob_json(layout, root)
    summary = {"subject": root["digest"], "created": created, "platforms": {}}
    reports = DIST / "reports"
    reports.mkdir(exist_ok=True)
    for descriptor in index["manifests"]:
        platform = descriptor.get("platform", {})
        if platform.get("os") != "linux":
            continue  # BuildKit provenance is preserved in the original index.
        arch = platform["architecture"]
        assert arch in PLATFORMS, "unexpected image platform"
        summary["platforms"][arch] = descriptor["digest"]
        # Give Trivy a single-manifest view so local OCI platform selection is
        # unambiguous, including versions that ignore --platform for --input.
        with tempfile.TemporaryDirectory(prefix="maestro-scan-", dir=DIST) as scratch:
            view = Path(scratch)
            (view / "blobs").symlink_to(layout / "blobs", target_is_directory=True)
            shutil.copy2(layout / "oci-layout", view)
            write_json(
                view / "index.json", {"schemaVersion": 2, "manifests": [descriptor]}
            )
            run(
                "trivy",
                "image",
                "--input",
                str(view),
                "--scanners",
                "vuln",
                "--no-progress",
                "--list-all-pkgs",
                "--format",
                "json",
                "--output",
                str(reports / f"trivy-{arch}.json"),
            )
    summary["scanner"] = json.loads(output("trivy", "version", "--format", "json"))
    write_json(reports / "scan-summary.json", summary)
    run(
        "oras",
        "attach",
        "--oci-layout",
        "--artifact-type",
        REPORT_TYPE,
        f"{layout}@{root['digest']}",
        "scan-summary.json:application/json",
        "trivy-amd64.json:application/vnd.aquasec.trivy.report+json",
        "trivy-arm64.json:application/vnd.aquasec.trivy.report+json",
        cwd=reports,
    )
    verify(layout)
    with tarfile.open(archive, "w") as tar:
        for name in ("oci-layout", "index.json", "blobs"):
            tar.add(layout / name, arcname=name)
    print(f"Built {archive.relative_to(ROOT)} ({root['digest']})", flush=True)


def load_image(arch, tag):
    """Import the verified image into classic Docker, which cannot load OCI tar."""
    layout = DIST / "oci"
    root = verify(layout)
    descriptor = next(
        d
        for d in blob_json(layout, root)["manifests"]
        if d.get("platform", {}) == {"architecture": arch, "os": "linux"}
    )
    manifest = blob_json(layout, descriptor)
    config = read_blob(layout, manifest["config"])
    config_name = manifest["config"]["digest"].split(":")[1] + ".json"
    with tempfile.TemporaryFile() as archive:
        with tarfile.open(fileobj=archive, mode="w") as tar:

            def add(name, data):
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                tar.addfile(entry, io.BytesIO(data))

            add(config_name, config)
            layers = []
            for i, layer in enumerate(manifest["layers"]):
                data = read_blob(layout, layer)
                if layer["mediaType"].endswith("+gzip"):
                    data = gzip.decompress(data)
                else:
                    assert (
                        layer["mediaType"] == "application/vnd.oci.image.layer.v1.tar"
                    )
                name = f"layer-{i}.tar"
                add(name, data)
                layers.append(name)
            add(
                "manifest.json",
                json.dumps(
                    [{"Config": config_name, "RepoTags": [tag], "Layers": layers}]
                ).encode(),
            )
        archive.seek(0)
        run("docker", "image", "load", stdin=archive)
    actual = output("docker", "image", "inspect", tag, "--format", "{{.Id}}")
    assert actual == manifest["config"]["digest"], (
        "loaded Docker image differs from scanned image"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "scan", "verify", "load"))
    parser.add_argument("--arch", choices=sorted(PLATFORMS))
    parser.add_argument("--tag", default="maestro:local")
    args = parser.parse_args()
    os.chdir(ROOT)
    if args.command == "build":
        build()
    elif args.command == "scan":
        scan_source()
    elif args.command == "load":
        arch = args.arch or output("docker", "version", "--format", "{{.Server.Arch}}")
        if arch not in PLATFORMS:
            raise ValueError(f"unsupported Docker architecture: {arch}")
        load_image(arch, args.tag)
    else:
        verify(DIST / "oci")


if __name__ == "__main__":
    main()
