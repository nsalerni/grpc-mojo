#!/usr/bin/env python3
"""Stable result and badge formats for the official gRPC interop suite."""

import json


DIRECTIONS = (
    ("mojo-client-h2c", "grpc-mojo client vs grpcio server", "h2c"),
    ("mojo-client-tls", "grpc-mojo client vs grpcio server", "TLS"),
    ("grpcio-client-h2c", "grpcio client vs grpc-mojo server", "h2c"),
    ("grpcio-client-tls", "grpcio client vs grpc-mojo server", "TLS"),
)


def result_document(
    cases: list[str],
    results: list[tuple[str, str, bool, str]],
) -> dict[str, object]:
    """Build the machine-readable record written by the interop runner."""
    rows = []
    for direction, case, passed, detail in results:
        row: dict[str, object] = {
            "case": case,
            "direction": direction,
            "passed": bool(passed),
        }
        if detail and not passed:
            row["detail"] = detail
        rows.append(row)
    return {
        "schemaVersion": 1,
        "cases": list(cases),
        "results": rows,
    }


def evaluated_results(
    document: dict[str, object],
) -> tuple[list[dict[str, object]], list[str]]:
    """Return every expected result, making missing data an explicit failure."""
    if document.get("schemaVersion") != 1:
        raise ValueError("unsupported official interop result schema")

    cases = document.get("cases")
    raw_results = document.get("results")
    if not isinstance(cases, list) or not cases:
        raise ValueError("official interop result has no cases")
    if not all(isinstance(case, str) and case for case in cases):
        raise ValueError("official interop result has an invalid case name")
    if len(cases) != len(set(cases)):
        raise ValueError("official interop result has duplicate case names")
    if not isinstance(raw_results, list):
        raise ValueError("official interop result has no result rows")

    indexed: dict[tuple[str, str], list[dict[str, object]]] = {}
    problems: list[str] = []
    known_directions = {key for key, _, _ in DIRECTIONS}
    known_cases = set(cases)
    for raw in raw_results:
        if not isinstance(raw, dict):
            problems.append("result row is not an object")
            continue
        direction = raw.get("direction")
        case = raw.get("case")
        if direction not in known_directions or case not in known_cases:
            problems.append(f"unexpected result row: {direction!r} {case!r}")
            continue
        indexed.setdefault((direction, case), []).append(raw)

    evaluated: list[dict[str, object]] = []
    for direction, label, transport in DIRECTIONS:
        for case in cases:
            matches = indexed.get((direction, case), [])
            if len(matches) != 1:
                detail = "missing result" if not matches else "duplicate results"
                passed = False
            else:
                passed_value = matches[0].get("passed")
                if not isinstance(passed_value, bool):
                    detail = "result has no boolean passed value"
                    passed = False
                else:
                    passed = passed_value
                    detail_value = matches[0].get("detail", "")
                    detail = detail_value if isinstance(detail_value, str) else ""
            evaluated.append({
                "case": case,
                "detail": detail,
                "direction": direction,
                "label": label,
                "passed": passed,
                "transport": transport,
            })
    return evaluated, problems


def badge_payload(document: dict[str, object]) -> dict[str, object]:
    """Build a Shields endpoint payload from the recorded case outcomes."""
    try:
        rows, problems = evaluated_results(document)
    except ValueError:
        return {
            "schemaVersion": 1,
            "label": "gRPC interop",
            "message": "results invalid",
            "color": "red",
        }

    messages = []
    complete = not problems
    for transport in ("h2c", "TLS"):
        transport_rows = [row for row in rows if row["transport"] == transport]
        passed = sum(1 for row in transport_rows if row["passed"])
        total = len(transport_rows)
        messages.append(f"{transport} {passed}/{total}")
        complete = complete and passed == total
    return {
        "schemaVersion": 1,
        "label": "gRPC interop",
        "message": " | ".join(messages),
        "color": "brightgreen" if complete else "red",
    }


def stable_json(value: dict[str, object]) -> str:
    """Serialize a generated document deterministically."""
    return json.dumps(value, indent=2, sort_keys=True) + "\n"
