#!/usr/bin/env python3
"""Check deterministic gRPC message framing against grpcio."""

import argparse
import importlib
import json
import random
import socket
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import h2.events

import run_compliance as suite


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_ARTIFACT = ROOT / "build" / "grpc-framing-mismatch.json"
MAX_SEED = (1 << 32) - 1
MAX_CASE_COUNT = 10_000
MUTATIONS = (
    "valid",
    "compressed-without-encoding",
    "invalid-compressed-flag",
    "truncated-prefix",
    "truncated-body",
    "declared-length-too-large",
    "declared-length-too-small",
    "multiple-unary-messages",
    "trailing-partial-prefix",
)


@dataclass(frozen=True)
class FramingCase:
    index: int
    mutation: str
    body: bytes
    expected_accept: bool
    require_reference_agreement: bool
    message: str


@dataclass
class Outcome:
    accepted: bool
    status: int | None = None
    message: str | None = None
    reset: int | None = None
    error: str | None = None


def bounded_int(text: str, minimum: int, maximum: int) -> int:
    try:
        value = int(text, 0)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if value < minimum or value > maximum:
        raise argparse.ArgumentTypeError(f"must be between {minimum} and {maximum}")
    return value


def seed_int(text: str) -> int:
    return bounded_int(text, 0, MAX_SEED)


def case_count_int(text: str) -> int:
    return bounded_int(text, 1, MAX_CASE_COUNT)


def frame(payload: bytes, compressed_flag: int = 0) -> bytes:
    return bytes((compressed_flag,)) + len(payload).to_bytes(4, "big") + payload


def mutate_frame(
    base: bytes,
    second: bytes,
    index: int,
    rng: random.Random,
) -> tuple[str, bytes, bool, bool]:
    mutation = MUTATIONS[index % len(MUTATIONS)]
    if mutation == "valid":
        return mutation, base, True, True
    if mutation == "compressed-without-encoding":
        return mutation, b"\x01" + base[1:], False, False
    if mutation == "invalid-compressed-flag":
        return mutation, bytes((rng.randint(2, 255),)) + base[1:], False, False
    if mutation == "truncated-prefix":
        return mutation, base[: rng.randint(0, 4)], False, True
    if mutation == "truncated-body":
        return mutation, base[: -rng.randint(1, len(base) - 5)], False, True
    if mutation == "declared-length-too-large":
        declared = int.from_bytes(base[1:5], "big") + rng.randint(1, 64)
        return mutation, base[:1] + declared.to_bytes(4, "big") + base[5:], False, True
    if mutation == "declared-length-too-small":
        declared = int.from_bytes(base[1:5], "big")
        declared -= rng.randint(1, declared - 1)
        return mutation, base[:1] + declared.to_bytes(4, "big") + base[5:], False, True
    if mutation == "multiple-unary-messages":
        return mutation, base + second, False, False
    return mutation, base + b"\x00\x00", False, False


def random_message(rng: random.Random, index: int) -> str:
    alphabet = "abcdefghij0123456789 -_/%"
    text = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 128)))
    if index % 17 == 0:
        text += " résumé 你好"
    return text


def make_case(pb: Any, index: int, rng: random.Random) -> FramingCase:
    message = random_message(rng, index)
    payload = pb.EchoRequest(message=message).SerializeToString()
    second_payload = pb.EchoRequest(message=f"second-{index}").SerializeToString()
    mutation, body, expected_accept, require_reference_agreement = mutate_frame(
        frame(payload), frame(second_payload), index, rng
    )
    return FramingCase(
        index,
        mutation,
        body,
        expected_accept,
        require_reference_agreement,
        message,
    )


def _status(headers: list[tuple[bytes, bytes]]) -> int | None:
    for name, value in reversed(headers):
        if name == b"grpc-status":
            try:
                return int(value)
            except ValueError:
                return None
    return None


def invoke(port: int, pb: Any, body: bytes, timeout: float = 3.0) -> Outcome:
    client = suite._RawH2GrpcClient(port)
    try:
        stream_id = client.start_request("/probe.Probe/Echo", body)
        headers: list[tuple[bytes, bytes]] = []
        response_body = bytearray()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            client.sock.settimeout(max(0.05, deadline - time.monotonic()))
            data = client.sock.recv(65536)
            if not data:
                return Outcome(False, error="connection closed")
            for event in client.conn.receive_data(data):
                if isinstance(
                    event, (h2.events.ResponseReceived, h2.events.TrailersReceived)
                ):
                    if event.stream_id == stream_id:
                        headers.extend(event.headers)
                elif isinstance(event, h2.events.DataReceived):
                    if event.stream_id == stream_id:
                        response_body.extend(event.data)
                        client.conn.acknowledge_received_data(
                            event.flow_controlled_length, event.stream_id
                        )
                elif isinstance(event, h2.events.StreamReset):
                    if event.stream_id == stream_id:
                        return Outcome(False, reset=int(event.error_code))
                elif isinstance(event, h2.events.StreamEnded):
                    if event.stream_id != stream_id:
                        continue
                    status = _status(headers)
                    if status != 0:
                        return Outcome(False, status=status)
                    try:
                        response = suite._decode_echo_response(
                            pb,
                            {
                                "headers": dict(headers),
                                "trailers": {},
                                "body": bytes(response_body),
                            },
                        )
                    except (ValueError, TypeError) as error:
                        return Outcome(False, status=status, error=str(error))
                    return Outcome(True, status=0, message=response.message)
            pending = client.conn.data_to_send()
            if pending:
                client.sock.sendall(pending)
        return Outcome(False, error="stream timed out")
    except (OSError, socket.timeout, ValueError) as error:
        return Outcome(False, error=f"{type(error).__name__}: {error}")
    finally:
        client.close()


def write_artifact(
    path: Path,
    *,
    seed: int,
    case_count: int,
    case: FramingCase,
    grpcio: Outcome,
    mojo: Outcome,
    reason: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "seed": seed,
        "case_count": case_count,
        "case_index": case.index,
        "mutation": case.mutation,
        "expected_accept": case.expected_accept,
        "require_reference_agreement": case.require_reference_agreement,
        "body_hex": case.body.hex(),
        "grpcio": asdict(grpcio),
        "mojo": asdict(mojo),
        "reason": reason,
    }
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=seed_int, default=20260825)
    parser.add_argument("--case-count", type=case_count_int, default=250)
    parser.add_argument("--failure-artifact", type=Path, default=DEFAULT_ARTIFACT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifact = args.failure_artifact.resolve()
    artifact.unlink(missing_ok=True)
    rng = random.Random(args.seed)

    with tempfile.TemporaryDirectory(prefix="grpc-framing-sequences-") as temp:
        suite.compile_test_protos(Path(temp))
        pb = importlib.import_module("echo_pb2")

        mojo_process, mojo_port = suite._start_polling_server()
        grpcio_server = None
        grpcio_lenient = 0
        try:
            grpcio_server, grpcio_port = suite.make_grpcio_probe_server(pb)
            for index in range(args.case_count):
                case = make_case(pb, index, rng)
                grpcio_outcome = invoke(grpcio_port, pb, case.body)
                mojo_outcome = invoke(mojo_port, pb, case.body)

                reason = ""
                if mojo_outcome.accepted != case.expected_accept:
                    reason = "Mojo disagreed with the protocol expectation"
                elif (
                    case.require_reference_agreement
                    and grpcio_outcome.accepted != case.expected_accept
                ):
                    reason = "grpcio disagreed on a reference-judged case"
                elif case.expected_accept and (
                    grpcio_outcome.message != case.message
                    or mojo_outcome.message != case.message
                ):
                    reason = "accepted response changed the protobuf message"

                if reason:
                    write_artifact(
                        artifact,
                        seed=args.seed,
                        case_count=args.case_count,
                        case=case,
                        grpcio=grpcio_outcome,
                        mojo=mojo_outcome,
                        reason=reason,
                    )
                    print(f"FAIL case {index} {case.mutation}: {reason}")
                    print(f"reproduction: {artifact}")
                    return 1
                if not case.expected_accept and grpcio_outcome.accepted:
                    grpcio_lenient += 1
        finally:
            if grpcio_server is not None:
                grpcio_server.stop(0).wait()
            mojo_process.kill()
            mojo_process.wait()

    print(
        f"PASS gRPC framing compatibility: {args.case_count} cases "
        f"with seed {args.seed}; grpcio accepted {grpcio_lenient} "
        "spec-invalid cases"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
