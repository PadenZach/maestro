"""Build scanned images into Docker or publish one multi-platform registry image."""

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
PLATFORMS = {"amd64", "arm64"}
REPORTS_LABEL = "io.maestro.scan.reports"
SCANNER_LABEL = "io.maestro.scan.scanner"


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
    scan_labels = None
    for arch, descriptor in images.items():
        manifest = blob_json(layout, descriptor)
        config = blob_json(layout, manifest["config"])
        diff_ids = []
        for layer in manifest["layers"]:
            data = read_blob(layout, layer)
            if layer["mediaType"].endswith("+gzip"):
                data = gzip.decompress(data)
            else:
                assert layer["mediaType"] == "application/vnd.oci.image.layer.v1.tar"
            diff_ids.append("sha256:" + hashlib.sha256(data).hexdigest())
        assert config["rootfs"]["diff_ids"] == diff_ids, "image rootfs digest mismatch"
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
        assert REPORTS_LABEL in labels and SCANNER_LABEL in labels, (
            "missing embedded scan metadata"
        )
        current_labels = (labels[REPORTS_LABEL], labels[SCANNER_LABEL])
        if scan_labels is None:
            scan_labels = current_labels
        assert current_labels == scan_labels, "platform scan metadata differs"
        reports = json.loads(labels[REPORTS_LABEL])
        scanner = json.loads(labels[SCANNER_LABEL])
        assert set(reports) == PLATFORMS, "missing platform scan metadata"
        assert scanner["Version"], "missing scanner version"
        assert scanner["VulnerabilityDB"]["UpdatedAt"], (
            "missing vulnerability database metadata"
        )
        report = reports[arch]
        assert report["Metadata"]["ImageConfig"]["architecture"] == arch
        assert report["Metadata"]["ImageConfig"]["os"] == "linux"
        assert report["Metadata"]["DiffIDs"] == diff_ids, (
            "scan filesystem digest mismatch"
        )
        assert [layer["Digest"] for layer in report["Metadata"]["Layers"]] == [
            layer["digest"] for layer in manifest["layers"]
        ], "scan layer digest mismatch"
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
        "Verified OCI labels, both platforms, Go inventory and embedded scan digests",
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
    # Remove the obsolete generated deliverable from earlier builds.
    (DIST / "maestro.oci.tar").unlink(missing_ok=True)
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
    build_command = [
        "docker",
        "buildx",
        "build",
        "--builder",
        builder,
        "--platform",
        "linux/amd64,linux/arm64",
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
    ]
    run(*build_command, "--provenance=false", str(ROOT))
    root = json.loads((layout / "index.json").read_text())["manifests"][0]
    run("oras", "tag", "--oci-layout", f"{layout}@{root['digest']}", "maestro")
    index = blob_json(layout, root)
    reports = DIST / "reports"
    reports.mkdir(exist_ok=True)
    embedded = {}
    for descriptor in index["manifests"]:
        platform = descriptor.get("platform", {})
        if platform.get("os") != "linux":
            continue
        arch = platform["architecture"]
        assert arch in PLATFORMS, "unexpected image platform"
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
        report = json.loads((reports / f"trivy-{arch}.json").read_text())
        reject_vulnerabilities(report)
        embedded[arch] = report
    scanner = json.loads(output("trivy", "version", "--format", "json"))
    assert set(embedded) == PLATFORMS, "missing platform scan"
    # The second export adds config labels using the cached build. Its provenance
    # describes the final image. Verification below proves its filesystem layers
    # are byte-for-byte identical to those scanned before metadata was added.
    shutil.rmtree(layout)
    run(
        *build_command,
        "--provenance=mode=min",
        "--label",
        f"{REPORTS_LABEL}=" + json.dumps(embedded, separators=(",", ":")),
        "--label",
        f"{SCANNER_LABEL}=" + json.dumps(scanner, separators=(",", ":")),
        str(ROOT),
    )
    root = json.loads((layout / "index.json").read_text())["manifests"][0]
    run("oras", "tag", "--oci-layout", f"{layout}@{root['digest']}", "maestro")
    verify(layout)
    print(f"Built scanned multi-platform image ({root['digest']})", flush=True)


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


def publish():
    """Publish one GHCR tag, including both platforms and their scan labels."""
    repository = os.environ.get("GHCR_IMAGE") or (
        "ghcr.io/" + os.environ.get("GITHUB_REPOSITORY", "")
    )
    repository = repository.lower()
    if not re.fullmatch(
        r"ghcr\.io/[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._/-]*", repository
    ):
        raise ValueError("Set GHCR_IMAGE to ghcr.io/OWNER/IMAGE")
    tag = os.environ.get("IMAGE_TAG") or "sha-" + output("git", "rev-parse", "HEAD")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("IMAGE_TAG must be a valid container tag")
    root = verify(DIST / "oci")
    reference = f"{repository}:{tag}"
    with tempfile.TemporaryDirectory(prefix="maestro-registry-") as scratch:
        auth = []
        token = os.environ.get("GHCR_TOKEN")
        if token:
            username = os.environ.get("GHCR_USERNAME") or os.environ.get("GITHUB_ACTOR")
            if not username:
                raise ValueError("Set GHCR_USERNAME to the registry token's owner")
            auth = ["--registry-config", str(Path(scratch) / "config.json")]
            run(
                "oras",
                "login",
                *auth,
                "ghcr.io",
                "--username",
                username,
                "--password-stdin",
                input=token,
            )
        run(
            "oras",
            "copy",
            *auth,
            "--from-oci-layout",
            f"{DIST / 'oci'}:maestro",
            reference,
        )
        assert output("oras", "resolve", *auth, reference) == root["digest"], (
            "published image digest differs from the verified image"
        )
    print(f"Published {reference}@{root['digest']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("build", "scan", "verify", "load", "publish")
    )
    parser.add_argument("--arch", choices=sorted(PLATFORMS))
    parser.add_argument("--tag", default="maestro:local")
    args = parser.parse_args()
    os.chdir(ROOT)
    if args.command == "build":
        build()
    if args.command == "scan":
        scan_source()
    elif args.command in {"build", "load"}:
        arch = args.arch or output("docker", "version", "--format", "{{.Server.Arch}}")
        if arch not in PLATFORMS:
            raise ValueError(f"unsupported Docker architecture: {arch}")
        load_image(arch, args.tag)
    elif args.command == "publish":
        publish()
    else:
        verify(DIST / "oci")


if __name__ == "__main__":
    main()
