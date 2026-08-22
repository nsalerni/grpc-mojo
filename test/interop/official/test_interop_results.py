#!/usr/bin/env python3
"""Regression checks for official interop result reporting."""

import json
import unittest

from interop_results import badge_payload, result_document, stable_json


CASES = ["empty_unary", "large_unary"]
DIRECTIONS = (
    "mojo-client-h2c",
    "mojo-client-tls",
    "grpcio-client-h2c",
    "grpcio-client-tls",
)


def complete_results() -> list[tuple[str, str, bool, str]]:
    return [
        (direction, case, True, "")
        for direction in DIRECTIONS
        for case in CASES
    ]


class InteropResultsTest(unittest.TestCase):
    def test_complete_matrix_reports_both_transport_scores(self):
        document = result_document(CASES, complete_results())

        self.assertEqual(badge_payload(document), {
            "schemaVersion": 1,
            "label": "gRPC interop",
            "message": "h2c 4/4 | TLS 4/4",
            "color": "brightgreen",
        })

    def test_failure_cannot_produce_a_green_badge(self):
        results = complete_results()
        direction, case, _, _ = results[0]
        results[0] = (direction, case, False, "reference mismatch")

        payload = badge_payload(result_document(CASES, results))

        self.assertEqual(payload["message"], "h2c 3/4 | TLS 4/4")
        self.assertEqual(payload["color"], "red")

    def test_missing_result_cannot_produce_a_green_badge(self):
        document = result_document(CASES, complete_results()[:-1])

        payload = badge_payload(document)

        self.assertEqual(payload["message"], "h2c 4/4 | TLS 3/4")
        self.assertEqual(payload["color"], "red")

    def test_serialization_is_stable(self):
        document = result_document(CASES, complete_results())

        serialized = stable_json(document)
        self.assertEqual(json.loads(serialized), document)
        self.assertEqual(serialized, stable_json(document))


if __name__ == "__main__":
    unittest.main()
