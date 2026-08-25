#!/usr/bin/env python3
"""Regression tests for compliance result registries and aggregation."""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "test" / "compliance"))

import run_compliance as compliance  # noqa: E402


class GrpcTlsRegistryTest(unittest.TestCase):
    def test_missing_row_fails_closed(self):
        rows = [
            (name, True, "")
            for name in compliance.EXPECTED_GRPC_TLS_CHECKS[:-1]
        ]

        passed, total, valid = compliance.grpc_tls_result_summary(
            {"grpc-tls": rows}
        )

        self.assertEqual(passed, total - 1)
        self.assertFalse(valid)

    def test_duplicate_row_fails_closed(self):
        rows = [
            (name, True, "")
            for name in compliance.EXPECTED_GRPC_TLS_CHECKS
        ]
        rows.append(rows[0])

        passed, total, valid = compliance.grpc_tls_result_summary(
            {"grpc-tls": rows}
        )

        self.assertEqual(passed, total - 1)
        self.assertFalse(valid)

    def test_unexpected_row_fails_closed(self):
        rows = [
            (name, True, "")
            for name in compliance.EXPECTED_GRPC_TLS_CHECKS
        ]
        rows.append(("unregistered check", True, ""))

        passed, total, valid = compliance.grpc_tls_result_summary(
            {"grpc-tls": rows}
        )

        self.assertEqual(passed, total - 1)
        self.assertFalse(valid)


class PackageSuiteAggregationTest(unittest.TestCase):
    def setUp(self):
        self.saved_results = compliance.RESULTS
        compliance.RESULTS = {}

    def tearDown(self):
        compliance.RESULTS = self.saved_results

    def test_nonzero_runner_with_json_fails_umbrella_report(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "results.json"
            report.write_text(json.dumps({
                "sections": {"proto": [["reference check", True, ""]]},
            }))

            with contextlib.redirect_stdout(io.StringIO()):
                compliance.aggregate_package_suite("protomojo", report, 1)

        self.assertEqual(
            compliance.RESULTS["proto"],
            [("reference check", True, "")],
        )
        self.assertEqual(len(compliance.RESULTS["protomojo"]), 1)
        self.assertFalse(compliance.RESULTS["protomojo"][0][1])

if __name__ == "__main__":
    unittest.main()
