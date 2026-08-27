#!/usr/bin/env python3
"""Compare unary and bidi loopback timings across grpc-mojo, grpcio, and tonic."""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from concurrent import futures
from pathlib import Path

import grpc


ROOT = Path(__file__).resolve().parent.parent
ECHO_PROTO = ROOT / "examples" / "echo.proto"
TONIC_DIR = ROOT / "bench" / "tonic-echo"
DEFAULT_ITERS = 200
SMOKE_ITERS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--iters", type=int, default=0)
    parser.add_argument(
        "--require-tonic",
        action="store_true",
        help="fail if the tonic sidecar cannot be built",
    )
    return parser.parse_args()


def iter_count(args: argparse.Namespace) -> int:
    if args.iters > 0:
        return args.iters
    return SMOKE_ITERS if args.smoke else DEFAULT_ITERS


def percentile_ns(samples: list[float], p: float) -> int:
    if not samples:
        return 0
    ordered = sorted(samples)
    rank = max(1, math.ceil(p / 100.0 * len(ordered)))
    return int(ordered[rank - 1])


def summarize(samples: list[float], *, per_op_divisor: int = 1) -> dict[str, int]:
    if not samples:
        return {"mean_ns": 0, "p99_ns": 0}
    mean = statistics.fmean(samples)
    p99 = percentile_ns(samples, 99)
    return {
        "mean_ns": int(mean / per_op_divisor),
        "p99_ns": int(p99 / per_op_divisor),
    }


def compile_grpcio_stubs(output_dir: Path):
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ECHO_PROTO.parent}",
            f"--python_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            str(ECHO_PROTO),
        ],
        check=True,
        cwd=ROOT,
    )
    sys.path.insert(0, str(output_dir))
    import echo_pb2
    import echo_pb2_grpc

    return echo_pb2, echo_pb2_grpc


def bench_grpcio(iters: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="grpcio-bench-stubs-") as tmp:
        echo_pb2, echo_pb2_grpc = compile_grpcio_stubs(Path(tmp))

        class Echo(echo_pb2_grpc.EchoServicer):
            def Say(self, request, context):
                return echo_pb2.EchoResponse(message=request.message)

            def Chat(self, request_iterator, context):
                for request in request_iterator:
                    yield echo_pb2.EchoResponse(message=request.message)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        echo_pb2_grpc.add_EchoServicer_to_server(Echo(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = echo_pb2_grpc.EchoStub(channel)
        small = echo_pb2.EchoRequest(message="hello bench")
        big = echo_pb2.EchoRequest(message="x" * 65536)

        def unary(request):
            samples = []
            for _ in range(iters):
                start = time.perf_counter_ns()
                stub.Say(request)
                samples.append(time.perf_counter_ns() - start)
            return samples

        def bidi():
            samples = []
            for _ in range(iters):
                start = time.perf_counter_ns()

                def messages():
                    for i in range(20):
                        yield echo_pb2.EchoRequest(message=str(i))

                responses = stub.Chat(messages())
                for _ in responses:
                    pass
                samples.append(time.perf_counter_ns() - start)
            return samples

        stub.Say(small)
        stub.Say(small)
        result = {
            "impl": "grpcio",
            "iters": iters,
            "unary_11b": summarize(unary(small)),
            "unary_64kib": summarize(unary(big)),
            "bidi_x20": summarize(bidi(), per_op_divisor=20),
        }
        channel.close()
        server.stop(0)
        return result


def mojo_command(iters: int, smoke: bool) -> list[str]:
    command = [
        "mojo",
        "run",
        "-I",
        "packages/mojo-net/src",
        "-I",
        "packages/mojo-http2/src",
        "-I",
        "packages/mojo-tls/src",
        "-I",
        "packages/protomojo/src",
        "-I",
        "src",
        "-I",
        "test",
        str(ROOT / "bench" / "compare_grpc.mojo"),
    ]
    if smoke or iters == SMOKE_ITERS:
        command.append("--smoke")
    return command


def parse_json_object(text: str) -> dict[str, object]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "impl" in value:
            return value
    raise RuntimeError(f"no JSON object in command output:\n{text[-500:]}")


def bench_mojo(iters: int, smoke: bool) -> dict[str, object]:
    completed = subprocess.run(
        mojo_command(iters, smoke),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_json_object(completed.stdout + completed.stderr)


def bench_tonic(iters: int, require: bool) -> dict[str, object] | None:
    cargo = shutil.which("cargo")
    if cargo is None:
        if require:
            raise RuntimeError("cargo is required for the tonic comparison")
        print("tonic: skipped (cargo not on PATH)", file=sys.stderr)
        return None
    build = subprocess.run(
        [cargo, "build", "--release", "--quiet"],
        cwd=TONIC_DIR,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        detail = (build.stderr or build.stdout).strip()
        if require:
            raise RuntimeError(f"tonic build failed: {detail[:400]}")
        print(f"tonic: skipped ({detail[:200]})", file=sys.stderr)
        return None
    binary = TONIC_DIR / "target" / "release" / "tonic-echo-bench"
    completed = subprocess.run(
        [str(binary), f"--iters={iters}"],
        cwd=TONIC_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_json_object(completed.stdout)


def print_table(rows: list[dict[str, object]]) -> None:
    shapes = ("unary_11b", "unary_64kib", "bidi_x20")
    print()
    print(f"{'impl':<12} {'shape':<12} {'mean ns/op':>12} {'p99 ns/op':>12}")
    for row in rows:
        for shape in shapes:
            stats = row[shape]
            print(
                f"{row['impl']:<12} {shape:<12} {stats['mean_ns']:>12} "
                f"{stats['p99_ns']:>12}"
            )


def main() -> int:
    args = parse_args()
    iters = iter_count(args)
    print(
        f"gRPC comparison: iters={iters} "
        f"(loopback, one client, one server, no git-diff of nanoseconds)",
        flush=True,
    )
    rows = [bench_grpcio(iters)]
    try:
        rows.append(bench_mojo(iters, args.smoke))
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"grpc-mojo: skipped ({error})", file=sys.stderr)
    tonic = bench_tonic(iters, args.require_tonic)
    if tonic is not None:
        rows.append(tonic)
    print_table(rows)
    print(json.dumps(rows, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
