"""Guard release boundaries without touching GitHub or a container registry."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

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
            (root / "dist").mkdir()
            for major in (2, 3):
                (root / "dist" / f"sdk-{major}.json").write_text("{}")
            with patch.object(release, "ROOT", root), patch.dict(os.environ, env, clear=True), \
                    patch.object(release, "output", return_value=REVISION), \
                    patch.object(release, "version", return_value="0.1.0"), \
                    patch.object(release, "tag_commit", return_value=REVISION), \
                    patch.object(release, "api", return_value=[[{"tag_name": "v0.1.0", "draft": True}]]), \
                    patch.object(release, "promote"), patch.object(release, "run") as run:
                release.publish()
                self.assertEqual([call.args[:3] for call in run.call_args_list], [
                    ("gh", "release", "upload"), ("gh", "release", "edit"),
                ])

    def test_new_prerelease_targets_the_exact_commit(self):
        env = {"IMAGE_REPOSITORY": "ghcr.io/example/app", "IMAGE_DIGEST": DIGEST,
               "RELEASE": "true", "GITHUB_REPOSITORY": "example/app"}
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "dist").mkdir()
            for major in (2, 3):
                (root / "dist" / f"sdk-{major}.json").write_text("{}")
            with patch.object(release, "ROOT", root), patch.dict(os.environ, env, clear=True), \
                    patch.object(release, "output", return_value=REVISION), \
                    patch.object(release, "version", return_value="0.2.0-rc.1"), \
                    patch.object(release, "tag_commit", return_value=None), \
                    patch.object(release, "api", side_effect=[[[]], {}]) as api, \
                    patch.object(release, "promote"), patch.object(release, "run") as run:
                release.publish()
                self.assertIn(f"sha={REVISION}", api.call_args.args)
                create = run.call_args_list[0].args
                self.assertEqual(create[:4], ("gh", "release", "create", "v0.2.0-rc.1"))
                self.assertIn("--draft", create)
                self.assertIn("--prerelease", create)
                self.assertIn("--verify-tag", create)
                self.assertEqual([call.args[:3] for call in run.call_args_list], [
                    ("gh", "release", "create"), ("gh", "release", "upload"), ("gh", "release", "edit"),
                ])


if __name__ == "__main__":
    unittest.main()
