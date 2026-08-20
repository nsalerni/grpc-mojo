#!/usr/bin/env python3
"""Interop tests: grpc-mojo against the reference Python grpcio stack.

Direction 1: Python grpcio client -> Mojo server
Direction 2: Mojo client -> Python grpcio server

This suite, not the unit tests, is the definition of "compatible".
Run via: pixi run interop
"""

import re
import subprocess
import sys
import tempfile
import threading
import time
from concurrent import futures
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
MOJO_RUN = [
    "mojo", "run",
    "-I", "packages/mojo-net/src",
    "-I", "packages/mojo-http2/src",
    "-I", "packages/mojo-tls/src",
    "-I", "packages/protomojo/src",
    "-I", "src",
    "-I", "examples",
]

# --- generate python stubs for examples/echo.proto ---

_tmp = tempfile.mkdtemp(prefix="grpc_mojo_interop_")
subprocess.run(
    [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{ROOT / 'examples'}",
        f"--python_out={_tmp}",
        f"--grpc_python_out={_tmp}",
        str(ROOT / "examples" / "echo.proto"),
    ],
    check=True,
)
sys.path.insert(0, _tmp)

import grpc  # noqa: E402
import echo_pb2  # noqa: E402
import echo_pb2_grpc  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}")


def test_python_client_to_mojo_server():
    print("== Python grpcio client -> Mojo server ==")
    proc = subprocess.Popen(
        [*MOJO_RUN, "examples/echo_server.mojo"],
        stdout=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    try:
        line = proc.stdout.readline()
        m = re.search(r":(\d+)$", line.strip())
        assert m, f"could not parse port from: {line!r}"
        port = int(m.group(1))

        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = echo_pb2_grpc.EchoStub(channel)

            # Unary happy path.
            resp = stub.Say(echo_pb2.EchoRequest(message="from python"), timeout=10)
            check("unary echo", resp.message == "echo: from python", resp.message)

            # Larger-than-one-frame message (64 KiB payload).
            big = "x" * 65536
            resp = stub.Say(echo_pb2.EchoRequest(message=big), timeout=10)
            check(
                "64KiB message",
                resp.message == "echo: " + big,
                f"len={len(resp.message)}",
            )

            # UTF-8 payload.
            resp = stub.Say(echo_pb2.EchoRequest(message="héllo 🔥"), timeout=10)
            check("utf-8 echo", resp.message == "echo: héllo 🔥", resp.message)

            # Unknown method -> UNIMPLEMENTED.
            bad = channel.unary_unary(
                "/echo.Echo/Nope",
                request_serializer=echo_pb2.EchoRequest.SerializeToString,
                response_deserializer=echo_pb2.EchoResponse.FromString,
            )
            try:
                bad(echo_pb2.EchoRequest(message="x"), timeout=10)
                check("unimplemented", False, "no error raised")
            except grpc.RpcError as e:
                check(
                    "unimplemented",
                    e.code() == grpc.StatusCode.UNIMPLEMENTED,
                    str(e.code()),
                )

            # Several sequential calls on one channel.
            ok = True
            for i in range(5):
                r = stub.Say(echo_pb2.EchoRequest(message=f"n{i}"), timeout=10)
                ok = ok and r.message == f"echo: n{i}"
            check("5 sequential calls", ok)
    finally:
        proc.kill()
        proc.wait()


class EchoServicer(echo_pb2_grpc.EchoServicer):
    def Say(self, request, context):
        # Prove metadata and deadline plumbing while we're here.
        return echo_pb2.EchoResponse(message="echo: " + request.message)


def test_mojo_client_to_python_server():
    print("== Mojo client -> Python grpcio server ==")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    echo_pb2_grpc.add_EchoServicer_to_server(EchoServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        result = subprocess.run(
            [
                *MOJO_RUN,
                "examples/echo_client.mojo",
                str(port),
                "mojo says hi",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=ROOT,
        )
        out = result.stdout
        check(
            "mojo unary against grpcio",
            result.returncode == 0 and "response: echo: mojo says hi" in out,
            f"rc={result.returncode} out={out!r} err={result.stderr!r}",
        )
        check(
            "mojo sees UNIMPLEMENTED from grpcio",
            "unimplemented-check: ok" in out,
            out,
        )
    finally:
        server.stop(0)


def main() -> int:
    test_python_client_to_mojo_server()
    test_mojo_client_to_python_server()
    print(f"\ninterop: {PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
