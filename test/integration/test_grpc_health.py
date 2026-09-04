#!/usr/bin/env python3
"""grpcio HealthStub against grpc-mojo Check; Watch is UNIMPLEMENTED."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
MOJO_RUN = [
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
    "test/integration",
]


def generate_stubs(work: Path) -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ROOT / 'src' / 'grpc'}",
            f"--python_out={work}",
            f"--grpc_python_out={work}",
            str(ROOT / "src" / "grpc" / "health.proto"),
        ],
        check=True,
    )


def start_server(mode: str) -> tuple[subprocess.Popen[str], int]:
    command = [*MOJO_RUN, "test/integration/health_server.mojo"]
    if mode == "polling":
        command.append("polling")
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    match = re.search(r":(\d+)$", line.strip())
    if not match:
        stderr = proc.stderr.read(400) if proc.stderr else ""
        proc.kill()
        proc.wait()
        raise RuntimeError(f"health server did not start: {line!r} {stderr!r}")
    return proc, int(match.group(1))


def run_stub_checks(port: int, health_pb2, health_pb2_grpc, grpc) -> None:
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        stub = health_pb2_grpc.HealthStub(channel)
        overall = stub.Check(health_pb2.HealthCheckRequest())
        assert overall.status == health_pb2.HealthCheckResponse.SERVING, overall
        named = stub.Check(health_pb2.HealthCheckRequest(service="echo.Echo"))
        assert named.status == health_pb2.HealthCheckResponse.SERVING, named
        down = stub.Check(health_pb2.HealthCheckRequest(service="down.Down"))
        assert down.status == health_pb2.HealthCheckResponse.NOT_SERVING, down
        try:
            stub.Check(health_pb2.HealthCheckRequest(service="missing.Svc"))
            raise AssertionError("unknown service must be NOT_FOUND")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.NOT_FOUND, exc.code()
        try:
            for _ in stub.Watch(health_pb2.HealthCheckRequest()):
                raise AssertionError("Watch must not stream a status")
            raise AssertionError("Watch must be UNIMPLEMENTED")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.UNIMPLEMENTED, exc.code()


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="grpc_mojo_health_"))
    generate_stubs(work)
    sys.path.insert(0, str(work))
    import grpc  # noqa: E402
    import health_pb2  # noqa: E402
    import health_pb2_grpc  # noqa: E402

    failed = 0
    for mode in ("blocking", "polling"):
        proc = None
        try:
            proc, port = start_server(mode)
            run_stub_checks(port, health_pb2, health_pb2_grpc, grpc)
            print(f"PASS grpcio HealthStub Check/Watch ({mode})")
        except Exception as exc:  # noqa: BLE001 - report and continue
            failed += 1
            print(f"FAIL grpcio HealthStub Check/Watch ({mode}): {exc}")
        finally:
            if proc is not None:
                proc.kill()
                proc.wait()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
