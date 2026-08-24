#!/usr/bin/env python3
"""Regression tests for the gRPC TLS compliance row registry."""

import sys
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


if __name__ == "__main__":
    unittest.main()
