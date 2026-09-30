"""Negative controls for scan policy and OCI content-addressed integrity."""

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "oci", Path(__file__).resolve().parents[2] / "oci.py"
)
oci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(oci)


class ScanPolicyTests(unittest.TestCase):
    def test_high_and_critical_fail_even_when_unfixed(self):
        for severity in ("HIGH", "CRITICAL"):
            with self.subTest(severity=severity):
                report = {
                    "Results": [
                        {
                            "Vulnerabilities": [
                                {
                                    "VulnerabilityID": "CVE-test",
                                    "Severity": severity,
                                    "InstalledVersion": "1.0",
                                    "FixedVersion": "",
                                }
                            ]
                        }
                    ]
                }
                with self.assertRaisesRegex(AssertionError, "CVE-test"):
                    oci.reject_vulnerabilities(report)

    def test_clean_and_lower_severity_pass(self):
        oci.reject_vulnerabilities({"Results": [{"Vulnerabilities": []}]})
        oci.reject_vulnerabilities(
            {
                "Results": [
                    {
                        "Vulnerabilities": [
                            {"VulnerabilityID": "CVE-test", "Severity": "MEDIUM"}
                        ]
                    }
                ]
            }
        )

    def test_modified_blob_is_rejected(self):
        original = b'{"schemaVersion":2}'
        digest = hashlib.sha256(original).hexdigest()
        descriptor = {"digest": "sha256:" + digest, "size": len(original)}
        with tempfile.TemporaryDirectory() as scratch:
            layout = Path(scratch)
            blobs = layout / "blobs/sha256"
            blobs.mkdir(parents=True)
            blob = blobs / digest
            blob.write_bytes(original)
            self.assertEqual(oci.read_blob(layout, descriptor), original)
            blob.write_bytes(original.replace(b"2", b"1"))
            with self.assertRaisesRegex(AssertionError, "digest mismatch"):
                oci.read_blob(layout, descriptor)
            blob.write_bytes(b"short")
            with self.assertRaisesRegex(AssertionError, "size mismatch"):
                oci.read_blob(layout, descriptor)


if __name__ == "__main__":
    unittest.main()
