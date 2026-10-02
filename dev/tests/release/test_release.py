"""Guard release boundaries without touching GitHub or a container registry."""

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tarfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[2] / "release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

REVISION = "a" * 40
DIGEST = "sha256:" + "b" * 64


class ReleaseTests(unittest.TestCase):
    def test_semver_precedence_and_invalid_image_versions(self):
        ordered = ["0.1.0-alpha.2", "0.1.0-alpha.10", "0.1.0-beta", "0.1.0-rc.1", "0.1.0", "0.1.1", "0.2.0", "1.0.0"]
        self.assertEqual(sorted(reversed(ordered), key=release.version_key), ordered)
        for invalid in ["v1.0.0", "1.0", "01.0.0", "1.0.0-01", "1.0.0+build", "1.0.0\nextra"]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                release.version_key(invalid)

    def test_plan_compares_the_entire_push_and_only_releases_on_main(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.name", "Release test")
            git("config", "user.email", "release@example.invalid")
            git("commit", "--allow-empty", "-qm", "Before versioning")
            initial = git("rev-parse", "HEAD")
            (root / "VERSION").write_text("0.1.0\n")
            git("add", "VERSION")
            git("commit", "-qm", "Initial version")
            base = git("rev-parse", "HEAD")
            (root / "VERSION").write_text("0.2.0\n")
            git("commit", "-qam", "Bump version")
            git("commit", "--allow-empty", "-qm", "Another commit in the same push")
            env = {"BASE_SHA": base, "GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main"}
            with patch.object(release, "ROOT", root), patch.dict(os.environ, env, clear=True), patch.object(release, "emit") as emit:
                release.plan()
                emit.assert_called_with(version="0.2.0", release="true")
                os.environ["GITHUB_EVENT_NAME"] = "pull_request"
                release.plan()
                emit.assert_called_with(version="0.2.0", release="false")
                os.environ["GITHUB_EVENT_NAME"] = "push"
                os.environ["GITHUB_REF"] = "refs/heads/feature"
                release.plan()
                emit.assert_called_with(version="0.2.0", release="false")
                (root / "VERSION").write_text("0.1.0\n")
                os.environ["GITHUB_REF"] = "refs/heads/main"
                release.plan()
                emit.assert_called_with(version="0.1.0", release="false")
                (root / "VERSION").write_text("0.0.9\n")
                with self.assertRaisesRegex(ValueError, "must increase"):
                    release.plan()
                os.environ["BASE_SHA"] = initial
                (root / "VERSION").write_text("0.1.0\n")
                release.plan()
                emit.assert_called_with(version="0.1.0", release="true")

    def test_existing_image_is_never_overwritten(self):
        with patch.object(release, "image_digest", return_value="sha256:" + "c" * 64), patch.object(release, "run") as run:
            with self.assertRaisesRegex(ValueError, "Refusing to overwrite"):
                release.promote("ghcr.io/example/app", "0.1.0", DIGEST)
            run.assert_not_called()

    def test_image_promotion_retry_is_a_noop(self):
        with patch.object(release, "image_digest", return_value=DIGEST), patch.object(release, "run") as run:
            release.promote("ghcr.io/example/app", "0.1.0", DIGEST)
            run.assert_not_called()

    def test_registry_auth_failure_is_not_treated_as_a_missing_image(self):
        result = subprocess.CompletedProcess([], 1, "", "401 Unauthorized")
        with patch.object(release.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "401 Unauthorized"):
                release.image_digest("ghcr.io/example/app:0.1.0")

    def test_publication_retries_and_tag_conflicts(self):
        env = {"IMAGE_REPOSITORY": "ghcr.io/example/app", "IMAGE_DIGEST": DIGEST,
               "RELEASE": "true", "GITHUB_REPOSITORY": "example/app"}
        with patch.dict(os.environ, env, clear=True), patch.object(release, "output", return_value=REVISION), \
                patch.object(release, "version", return_value="0.1.0"), \
                patch.object(release, "tag_commit", return_value=REVISION) as tag_commit, \
                patch.object(release, "api", return_value=[[{"tag_name": "v0.1.0", "draft": False}]]), \
                patch.object(release, "image_digest", return_value=DIGEST), \
                patch.object(release, "run") as run:
            release.publish()
            run.assert_not_called()
            tag_commit.return_value = "d" * 40
            with self.assertRaisesRegex(ValueError, "Refusing to move"):
                release.publish()
            run.assert_not_called()

    def test_draft_resume_attaches_assets_before_publication(self):
        env = {"IMAGE_REPOSITORY": "ghcr.io/example/app", "IMAGE_DIGEST": DIGEST,
               "RELEASE": "true", "GITHUB_REPOSITORY": "example/app"}
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            with patch.object(release, "ROOT", root), patch.dict(os.environ, env, clear=True), \
                    patch.object(release, "output", return_value=REVISION), \
                    patch.object(release, "version", return_value="0.1.0"), \
                    patch.object(release, "tag_commit", return_value=REVISION), \
                    patch.object(release, "api", return_value=[[{"tag_name": "v0.1.0", "draft": True}]]), \
                    patch.object(release, "build_assets", return_value=[root / "maestro_0.1.0_linux_amd64.tar.gz", root / "checksums.txt"]), \
                    patch.object(release, "finalize_release") as finalize, \
                    patch.object(release, "promote"), patch.object(release, "run") as run:
                release.publish()
                self.assertEqual(run.call_args.args[:4], ("gh", "release", "upload", "v0.1.0"))
                self.assertFalse(any(str(arg).endswith(".json") for arg in run.call_args.args))
                finalize.assert_called_once_with("example/app", "v0.1.0", "0.1.0")

    def test_new_prerelease_targets_the_exact_commit(self):
        env = {"IMAGE_REPOSITORY": "ghcr.io/example/app", "IMAGE_DIGEST": DIGEST,
               "RELEASE": "true", "GITHUB_REPOSITORY": "example/app"}
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            with patch.object(release, "ROOT", root), patch.dict(os.environ, env, clear=True), \
                    patch.object(release, "output", return_value=REVISION), \
                    patch.object(release, "version", return_value="0.2.0-rc.1"), \
                    patch.object(release, "tag_commit", return_value=None), \
                    patch.object(release, "api", side_effect=[[[]], {}]) as api, \
                    patch.object(release, "build_assets", return_value=[root / "checksums.txt"]), \
                    patch.object(release, "finalize_release"), \
                    patch.object(release, "promote"), patch.object(release, "run") as run:
                release.publish()
                self.assertIn(f"sha={REVISION}", api.call_args.args)
                create = run.call_args_list[0].args
                self.assertEqual(create[:4], ("gh", "release", "create", "v0.2.0-rc.1"))
                self.assertIn("--draft", create)
                self.assertIn("--prerelease", create)
                self.assertIn("--verify-tag", create)
                self.assertEqual([call.args[:3] for call in run.call_args_list], [
                    ("gh", "release", "create"), ("gh", "release", "upload"),
                ])

    def test_archives_have_executable_at_root_and_are_repeatable(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            binary, license_file = root / "binary", root / "license"
            binary.write_bytes(b"executable bytes")
            license_file.write_text("MIT License")
            for system, arch in release.PLATFORMS:
                archive = root / release.archive_name("0.2.0", system, arch)
                release.write_archive(archive, binary, license_file, system, 1750000000)
                first = archive.read_bytes()
                release.write_archive(archive, binary, license_file, system, 1750000000)
                self.assertEqual(first, archive.read_bytes())
                if system == "windows":
                    with zipfile.ZipFile(archive) as contents:
                        self.assertEqual(contents.namelist(), ["maestro.exe", "LICENSE"])
                        self.assertEqual(contents.read("maestro.exe"), binary.read_bytes())
                else:
                    with tarfile.open(archive) as contents:
                        self.assertEqual(contents.getnames(), ["maestro", "LICENSE"])
                        self.assertEqual(contents.getmember("maestro").mode, 0o755)
                        self.assertEqual(contents.extractfile("maestro").read(), binary.read_bytes())

    def test_binary_identity_checks_platform_and_embedded_revision(self):
        with tempfile.TemporaryDirectory() as scratch:
            binary = Path(scratch) / "maestro"
            binary.write_bytes(b"executable bytes\x00" + REVISION.encode() + b"\x00")
            with patch.object(release, "output", return_value="GOOS=linux\nGOARCH=amd64\nCGO_ENABLED=0\n"):
                release.verify_binary(binary, "linux", "amd64", REVISION)
                with self.assertRaisesRegex(ValueError, "missing GOARCH=arm64"):
                    release.verify_binary(binary, "linux", "arm64", REVISION)
                with self.assertRaisesRegex(ValueError, "missing revision"):
                    release.verify_binary(binary, "linux", "amd64", "c" * 40)

    def test_platform_digest_ignores_attestations_and_selects_exact_architecture(self):
        amd64, arm64 = "sha256:" + "1" * 64, "sha256:" + "2" * 64
        index = {"manifests": [
            {"digest": amd64, "platform": {"os": "linux", "architecture": "amd64"}},
            {"digest": arm64, "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": DIGEST, "platform": {"os": "unknown", "architecture": "unknown"}},
        ]}
        with patch.object(release, "output", return_value=json.dumps(index)):
            self.assertEqual(release.platform_digest("ghcr.io/example/app", DIGEST, "amd64"), amd64)
            self.assertEqual(release.platform_digest("ghcr.io/example/app", DIGEST, "arm64"), arm64)
            with self.assertRaisesRegex(ValueError, "Expected one Linux"):
                release.platform_digest("ghcr.io/example/app", DIGEST, "arm")

    def test_notes_link_every_download_and_the_tagged_image(self):
        notes = release.release_notes("example/app", "ghcr.io/example/app", "0.2.0")
        for system, arch in release.PLATFORMS:
            self.assertIn("/releases/download/v0.2.0/" + release.archive_name("0.2.0", system, arch), notes)
        self.assertIn("/pkgs/container/app?tag=0.2.0", notes)
        self.assertIn("docker pull ghcr.io/example/app:0.2.0", notes)
        self.assertIn("mise use -g github:example/app@0.2.0", notes)
        self.assertNotIn(".json", notes)

    def test_repair_verifies_uploads_before_removing_old_jsons(self):
        env = {"IMAGE_REPOSITORY": "ghcr.io/example/app", "GITHUB_REPOSITORY": "example/app", "RELEASE_TAG": "v0.2.0"}
        with tempfile.TemporaryDirectory() as scratch:
            asset = Path(scratch) / "maestro_0.2.0_linux_amd64.tar.gz"
            asset.write_bytes(b"binary archive")
            existing = {"draft": False, "immutable": False, "assets": [{"name": "release.json", "id": 123}]}
            uploaded = {"assets": [{"name": asset.name, "digest": "sha256:" + hashlib.sha256(asset.read_bytes()).hexdigest()}]}
            with patch.dict(os.environ, env, clear=True), \
                    patch.object(release, "version", return_value="0.2.0"), \
                    patch.object(release, "output", return_value=REVISION), \
                    patch.object(release, "tag_commit", return_value=REVISION), \
                    patch.object(release, "image_digest", return_value=DIGEST), \
                    patch.object(release, "build_assets", return_value=[asset]), \
                    patch.object(release, "finalize_release"), \
                    patch.object(release, "api", side_effect=[existing, uploaded, None]) as api, \
                    patch.object(release, "run") as run:
                release.repair()
                self.assertEqual(run.call_args_list[0].args, ("gh", "release", "upload", "v0.2.0", str(asset)))
                self.assertEqual(api.call_args.args, ("repos/example/app/releases/assets/123", "--method", "DELETE"))
                uploaded["assets"][0]["digest"] = "sha256:" + "0" * 64
                api.reset_mock(side_effect=True)
                api.side_effect = [existing, uploaded]
                with self.assertRaisesRegex(ValueError, "checksum verification"):
                    release.repair()
                self.assertEqual(api.call_count, 2)


if __name__ == "__main__":
    unittest.main()
