#!/usr/bin/env python3
"""grpc-mojo compliance suite (umbrella).

Every layer is differentially tested against an established reference
implementation:

  proto  vs  Python `protobuf` (the reference implementation)
  hpack  vs  python-hpack (the HPACK used by hyper-h2 / httpx)
  h2     vs  hyper-h2 + hyperframe (strict: raises ProtocolError on any
             protocol violation by our side)
  net    vs  CPython sockets (OS truth for TCP semantics)
  grpc   vs  grpcio (the reference gRPC implementation)

The proto, hpack, h2, and net sections are executed by each package's own
self-contained suite (packages/<pkg>/compliance/run_compliance.py) and
aggregated here; this script adds the gRPC sections, extraction isolation,
and the unit/interop suites, then writes the combined report.

Rerun with: pixi run compliance
Writes docs/COMPLIANCE.md and exits non-zero on any failure.
"""

import base64
import json
import multiprocessing
import platform
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
BUILD = ROOT / "build"
TOOLS = ROOT / "test" / "compliance" / "tools"
REPORT = ROOT / "docs" / "COMPLIANCE.md"
HTML_REPORT = ROOT / "docs" / "COMPLIANCE.html"
OFFICIAL_RESULTS_REPORT = ROOT / "docs" / "official-interop-results.json"
CERTS = BUILD / "certs"
MOJO_RUN = [
    "mojo", "run",
    "-I", "packages/mojo-net/src",
    "-I", "packages/mojo-http2/src",
    "-I", "packages/mojo-tls/src",
    "-I", "packages/protomojo/src",
    "-I", "src",
    "-I", "test",
]

RESULTS: dict[str, list[tuple[str, bool, str]]] = {}

# Package suites executed and aggregated by this umbrella (in report order).
PACKAGE_SUITES = ("protomojo", "mojo-http2", "mojo-net", "mojo-tls")

sys.path.insert(0, str(ROOT / "test" / "interop" / "official"))
from interop_results import evaluated_results  # noqa: E402


def record(section: str, name: str, ok: bool, detail: str = ""):
    RESULTS.setdefault(section, []).append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'} [{section}] {name}" + ("" if ok else f"  <- {detail}"))


def run_tool(binary: str, *args, timeout=60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*MOJO_RUN, str(TOOLS / f"{binary}.mojo"), *map(str, args)],
        capture_output=True, text=True, timeout=timeout, cwd=ROOT,
    )


def build_tools():
    print("== checking Mojo compliance tools ==")
    BUILD.mkdir(exist_ok=True)
    for src in sorted(TOOLS.glob("*.mojo")):
        print(f"  found {src.stem}")


# ------------------------------------------------------- package suites ---

def run_package_suites(tmp: Path):
    """Run each package's own compliance suite and aggregate its results.

    The package runners execute with this root interpreter. The root
    pixi env carries every reference dependency and reports their rows
    via --json under the same section keys the umbrella always used
    (proto, hpack, h2, net).
    """
    for pkg in PACKAGE_SUITES:
        pkg_dir = ROOT / "packages" / pkg
        json_path = tmp / f"{pkg}_compliance.json"
        print(f"== package suite: {pkg} ==", flush=True)
        r = subprocess.run(
            [sys.executable, "compliance/run_compliance.py", "--json", str(json_path)],
            cwd=pkg_dir,
        )
        if json_path.exists():
            data = json.loads(json_path.read_text())
            for section, rows in data["sections"].items():
                for name, ok, detail in rows:
                    RESULTS.setdefault(section, []).append((name, bool(ok), detail))
        else:
            record(pkg, f"{pkg} compliance suite produced results",
                   False, f"runner exited {r.returncode} without JSON output")


# ----------------------------------------------------------------- proto ---

def compile_test_protos(tmp: Path):
    subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc",
         f"-I{ROOT/'examples'}",
         f"--python_out={tmp}",
         str(ROOT / "examples" / "echo.proto")],
        check=True,
    )
    sys.path.insert(0, str(tmp))


def section_packaging(tmp: Path):
    """Each package must build and run staged with only its declared deps."""
    print("== packaging: extraction isolation ==")
    sys.path.insert(0, str(ROOT / "tools"))
    import check_extraction
    for pkg, deps in check_extraction.PACKAGES.items():
        ok, detail = check_extraction.check(pkg)
        dep_str = " + ".join(deps) if deps else "stdlib only"
        record("packaging",
               f"`{pkg}` builds and runs staged with {dep_str}", ok, detail)


# ----------------------------------------------------------------- grpc ---

def section_grpc_transport():
    """Check the wrapper's partial I/O against CPython TCP and TLS peers."""
    print("== grpc transport readiness vs CPython ==")
    size = 8 * 1024 * 1024

    def peer(listener, result, context=None):
        conn = None
        try:
            raw, _ = listener.accept()
            conn = (
                context.wrap_socket(raw, server_side=True)
                if context is not None
                else raw
            )
            conn.settimeout(30)
            if context is not None:
                result["version"] = conn.version()
                result["alpn"] = conn.selected_alpn_protocol()
            time.sleep(0.2)
            conn.sendall(b"R")
            time.sleep(0.3)
            received = bytearray()
            while len(received) < size:
                chunk = conn.recv(min(65536, size - len(received)))
                if not chunk:
                    break
                received.extend(chunk)
            result["bytes"] = len(received)
            result["payload_ok"] = received == b"\x5a" * size
            conn.sendall(received)
        except Exception as error:
            result["error"] = repr(error)
        finally:
            if conn is not None:
                conn.close()

    def run_case(mode, context=None):
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        result = {}
        thread = threading.Thread(
            target=peer, args=(listener, result, context), daemon=True
        )
        thread.start()
        args = [mode, listener.getsockname()[1], size]
        if mode == "tls":
            args.append(CERTS / "ca.pem")
        proc = run_tool(
            "grpc_transport_readiness_probe", *args, timeout=120
        )
        thread.join(timeout=35)
        peer_alive = thread.is_alive()
        listener.close()
        values = None
        parts = proc.stdout.split()
        if proc.returncode == 0 and len(parts) == 11 and parts[0] == "OK":
            try:
                values = tuple(map(int, parts[1:]))
            except ValueError:
                pass
        return values, result, proc, peer_alive

    values, peer_result, proc, peer_alive = run_case("tcp")
    tcp_ok = (
        values is not None
        and values[0] == size
        and values[1] == size
        and values[2] > 1
        and values[3] > 1
        and values[4] > 0
        and values[5] > 0
        and values[6:] == (0, 0, 0, 0)
        and peer_result.get("bytes") == size
        and peer_result.get("payload_ok") is True
        and "error" not in peer_result
        and not peer_alive
    )
    record(
        "grpc-transport",
        "h2c partial I/O and would-block match CPython sockets",
        tcp_ok,
        f"out={proc.stdout.strip()!r} peer={peer_result} "
        f"peer_alive={peer_alive} err={proc.stderr[:160]!r}",
    )

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(CERTS / "server.pem", CERTS / "server.key")
    context.set_alpn_protocols(["h2"])
    values, peer_result, proc, peer_alive = run_case("tls", context)
    tls_ok = (
        values is not None
        and values[0] == size
        and values[1] == size
        and values[2] > 1
        and values[3] > 1
        and values[4] > 0
        and values[5] > 0
        and values[6] + values[7] == values[4]
        and values[8] + values[9] == values[5]
        and peer_result.get("bytes") == size
        and peer_result.get("payload_ok") is True
        and str(peer_result.get("version", "")).startswith("TLSv1.")
        and peer_result.get("alpn") == "h2"
        and "error" not in peer_result
        and not peer_alive
    )
    record(
        "grpc-transport",
        "TLS partial I/O preserves OpenSSL-reported retry direction",
        tls_ok,
        f"out={proc.stdout.strip()!r} peer={peer_result} "
        f"peer_alive={peer_alive} err={proc.stderr[:160]!r}",
    )


def make_grpcio_probe_server(pb, use_tls: bool = False):
    import grpc
    code_by_num = {c.value[0]: c for c in grpc.StatusCode}

    def fail(req, ctx):
        num, _, details = req.message.partition("|")
        ctx.abort(code_by_num[int(num)], details)

    def meta_echo(req, ctx):
        pairs = []
        for k, v in ctx.invocation_metadata():
            if k.startswith("x-"):
                pairs.append(f"{k}={v.hex() if isinstance(v, bytes) else v}")
        ctx.send_initial_metadata((("x-initial", "from-python"),))
        ctx.set_trailing_metadata((("x-trailer", "python-trailer"),
                                   ("x-blob-bin", b"\xde\xad\xbe\xef")))
        return pb.EchoResponse(message=";".join(sorted(pairs)))

    def deadline(req, ctx):
        return pb.EchoResponse(message=str(int(ctx.time_remaining() * 1000)))

    def echo(req, ctx):
        return pb.EchoResponse(message=req.message)

    def sleep_5s(req, ctx):
        time.sleep(5.0)
        return pb.EchoResponse(message="finally")

    def fail_rich(req, ctx):
        from grpc_status import rpc_status
        from google.rpc import status_pb2, code_pb2
        st = status_pb2.Status(code=code_pb2.NOT_FOUND, message="rich error")
        ctx.abort_with_status(rpc_status.to_status(st))

    class Handler(grpc.GenericRpcHandler):
        def service(self, hcd):
            table = {"/probe.Probe/Fail": fail, "/probe.Probe/MetaEcho": meta_echo,
                     "/probe.Probe/Deadline": deadline, "/probe.Probe/Echo": echo,
                     "/probe.Probe/Sleep": sleep_5s,
                     "/probe.Probe/FailRich": fail_rich}
            fn = table.get(hcd.method)
            if fn is None:
                return None
            return grpc.unary_unary_rpc_method_handler(
                fn, request_deserializer=pb.EchoRequest.FromString,
                response_serializer=pb.EchoResponse.SerializeToString)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((Handler(),))
    if use_tls:
        credentials = grpc.ssl_server_credentials(((
            (CERTS / "server.key").read_bytes(),
            (CERTS / "server.pem").read_bytes(),
        ),))
        port = server.add_secure_port("127.0.0.1:0", credentials)
    else:
        port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def _polling_process_worker(args):
    """Run one independent grpcio channel for the process-load proof."""
    port, worker, calls, use_tls = args
    import grpc
    import echo_pb2 as pb

    if use_tls:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(CERTS / "ca.pem").read_bytes()
        )
        channel = grpc.secure_channel(f"localhost:{port}", credentials)
    else:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    with channel:
        echo = channel.unary_unary(
            "/probe.Probe/Echo",
            request_serializer=pb.EchoRequest.SerializeToString,
            response_deserializer=pb.EchoResponse.FromString,
        )
        for call in range(calls):
            value = f"worker={worker} call={call} " + "x" * 1000
            try:
                response = echo(pb.EchoRequest(message=value), timeout=20)
            except grpc.RpcError:
                return False
            if response.message != value:
                return False
    return True


def _start_polling_server(*limits, use_tls=False):
    command = [
        *MOJO_RUN,
        str(TOOLS / "grpc_polling_server_probe.mojo"),
        *map(str, limits),
    ]
    if use_tls:
        command.extend(
            ["tls", str(CERTS / "server.pem"), str(CERTS / "server.key")]
        )
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    line = proc.stdout.readline()
    if "listening on" not in line:
        stderr = proc.stderr.read(300)
        proc.kill()
        proc.wait()
        raise RuntimeError(f"PollingServer did not start: {line!r} {stderr!r}")
    return proc, int(line.strip().rsplit(":", 1)[-1])


def _grpc_frame(payload: bytes, compressed: int = 0) -> bytes:
    return bytes([compressed]) + len(payload).to_bytes(4, "big") + payload


class _RawH2GrpcClient:
    """Small hyper-h2 oracle for malformed and half-close cases."""

    def __init__(
        self,
        port,
        *,
        receive_window=65535,
        receive_buffer=None,
        use_tls=False,
    ):
        import h2.config
        import h2.connection

        sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        if use_tls:
            context = ssl.create_default_context(cafile=CERTS / "ca.pem")
            context.set_alpn_protocols(["h2"])
            sock = context.wrap_socket(sock, server_hostname="localhost")
            if sock.selected_alpn_protocol() != "h2":
                sock.close()
                raise RuntimeError("PollingServer TLS did not negotiate h2")
        self.sock = sock
        self.scheme = "https" if use_tls else "http"
        self.authority = "localhost" if use_tls else "127.0.0.1"
        if receive_buffer is not None:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.sock.settimeout(10)
        self.conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True)
        )
        self.conn.local_settings.initial_window_size = receive_window
        self.conn.initiate_connection()
        if receive_window > 65535:
            self.conn.increment_flow_control_window(receive_window - 65535)
        self.sock.sendall(self.conn.data_to_send())
        self.write_closed = False

    def close(self):
        self.sock.close()

    def start_request(
        self,
        path,
        body=None,
        *,
        method="POST",
        content_type="application/grpc",
        end_stream=True,
        extra_headers=(),
    ):
        sid = self.conn.get_next_available_stream_id()
        headers = [
            (":method", method),
            (":scheme", self.scheme),
            (":authority", self.authority),
            (":path", path),
            ("te", "trailers"),
            ("content-type", content_type),
            *extra_headers,
        ]
        self.conn.send_headers(sid, headers, end_stream=body is None and end_stream)
        if body is not None:
            self.conn.send_data(sid, body, end_stream=end_stream)
        self.sock.sendall(self.conn.data_to_send())
        return sid

    def finish(self, sid, timeout=10):
        import h2.events

        deadline = time.monotonic() + timeout
        headers = []
        trailers = []
        body = bytearray()
        resets = []
        while time.monotonic() < deadline:
            self.sock.settimeout(max(0.05, deadline - time.monotonic()))
            data = self.sock.recv(65536)
            if not data:
                break
            events = self.conn.receive_data(data)
            for event in events:
                if isinstance(event, h2.events.ResponseReceived) and event.stream_id == sid:
                    headers.extend(event.headers)
                elif isinstance(event, h2.events.TrailersReceived) and event.stream_id == sid:
                    trailers.extend(event.headers)
                elif isinstance(event, h2.events.DataReceived) and event.stream_id == sid:
                    body.extend(event.data)
                    self.conn.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, h2.events.StreamReset):
                    resets.append((event.stream_id, event.error_code))
                elif isinstance(event, h2.events.StreamEnded) and event.stream_id == sid:
                    pending = self.conn.data_to_send()
                    if pending and not self.write_closed:
                        self.sock.sendall(pending)
                    return {
                        "headers": dict(headers),
                        "trailers": dict(trailers),
                        "body": bytes(body),
                        "resets": resets,
                    }
            pending = self.conn.data_to_send()
            if pending and not self.write_closed:
                self.sock.sendall(pending)
        raise TimeoutError(f"stream {sid} did not finish")

    def request(self, path, body, **kwargs):
        sid = self.start_request(path, body, **kwargs)
        return sid, self.finish(sid)

    def expect_reset(self, sid, code, timeout=5):
        from hyperframe.frame import Frame, RstStreamFrame

        deadline = time.monotonic() + timeout
        buffered = bytearray()
        while time.monotonic() < deadline:
            self.sock.settimeout(max(0.05, deadline - time.monotonic()))
            data = self.sock.recv(65536)
            if not data:
                return False
            buffered.extend(data)
            found = False
            offset = 0
            while len(buffered) - offset >= 9:
                frame, length = Frame.parse_frame_header(
                    memoryview(buffered)[offset : offset + 9]
                )
                if len(buffered) - offset < 9 + length:
                    break
                frame.parse_body(
                    memoryview(buffered)[offset + 9 : offset + 9 + length]
                )
                if (
                    isinstance(frame, RstStreamFrame)
                    and frame.stream_id == sid
                    and frame.error_code == code
                ):
                    found = True
                offset += 9 + length
            if offset:
                del buffered[:offset]
            self.conn.receive_data(data)
            pending = self.conn.data_to_send()
            if pending:
                self.sock.sendall(pending)
            if found:
                return True
        return False


def _grpc_status(result):
    value = result["trailers"].get(b"grpc-status")
    if value is None:
        value = result["headers"].get(b"grpc-status")
    return None if value is None else int(value)


def _decode_echo_response(pb, result):
    body = result["body"]
    if len(body) < 5 or body[0] != 0:
        raise ValueError(f"invalid gRPC response frame: {body[:16]!r}")
    size = int.from_bytes(body[1:5], "big")
    if len(body) != size + 5:
        raise ValueError(f"invalid gRPC response length: {len(body)} != {size + 5}")
    return pb.EchoResponse.FromString(body[5:])


def section_grpc_client(tmp: Path):
    print("== grpc: mojo client vs grpcio server ==")
    import echo_pb2 as pb
    server, port = make_grpcio_probe_server(pb)
    try:
        # Every non-OK status code + unicode/percent details must map 1:1.
        all_ok, detail = True, ""
        for code in range(1, 17):
            details = f"détails for {code} 100%"
            r = run_tool("grpc_client_probe", port, "status", f"{code}|{details}")
            want = f"code={code} message={details}"
            if want not in r.stdout:
                all_ok, detail = False, f"code {code}: got {r.stdout.strip()!r} {r.stderr[:100]!r}"
                break
        record("grpc", "status-code mapping, all 16 codes + unicode/percent details", all_ok, detail)

        r = run_tool("grpc_client_probe", port, "meta")
        out = r.stdout
        checks = [
            ("request ascii metadata delivered", "x-ascii=hello meta" in out),
            ("request binary metadata (-bin) delivered", "x-payload-bin=0102ff00" in out),
            ("initial response metadata received", "initial=from-python" in out),
            ("trailing metadata received", "trailer=python-trailer" in out),
            ("trailing binary metadata decoded", "trailer-bin=deadbeef" in out),
        ]
        for name, ok in checks:
            record("grpc", name, ok, "" if ok else out.strip())

        r = run_tool("grpc_client_probe", port, "deadline", 2500)
        m = re.search(r"remaining_ms=(\d+) code=0", r.stdout)
        ok = bool(m) and 1500 <= int(m.group(1)) <= 2500
        record("grpc", "grpc-timeout propagated to grpcio deadline (2500ms sent)",
               ok, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "echo", 1_000_000, timeout=120)
        record("grpc", "1MB unary echo via grpcio server",
               "len=1000000 match=True code=0" in r.stdout, r.stdout.strip() + r.stderr[:150])

        r = run_tool("grpc_client_probe", port, "echo", 0)
        record("grpc", "empty message echo via grpcio server",
               "len=0 match=True code=0" in r.stdout, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "unicode")
        record("grpc", "unicode payload echo via grpcio server",
               "match=True code=0" in r.stdout, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "richstatus")
        from google.rpc import status_pb2
        m2 = re.search(r"code=5 details=([0-9a-f]+)", r.stdout)
        rich_ok = False
        if m2:
            st = status_pb2.Status.FromString(bytes.fromhex(m2.group(1)))
            rich_ok = st.code == 5 and st.message == "rich error"
        record("grpc", "rich error model: grpc-status-details-bin decoded from grpcio",
               rich_ok, r.stdout.strip())

        t0 = time.monotonic()
        r = run_tool("grpc_client_probe", port, "sleep", 300)
        took = time.monotonic() - t0
        record("grpc", "client deadline enforced vs sleeping server (300ms -> DEADLINE_EXCEEDED)",
               "code=4" in r.stdout and took < 4.0,
               f"out={r.stdout.strip()!r} took={took:.2f}s")
    finally:
        server.stop(0)


def section_grpc_server(tmp: Path):
    print("== grpc: grpcio client vs mojo server ==")
    import grpc
    import echo_pb2 as pb
    proc = subprocess.Popen(
        [*MOJO_RUN, str(TOOLS / "grpc_server_probe.mojo")],
        stdout=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    line = proc.stdout.readline()
    port = int(line.strip().rsplit(":", 1)[-1])
    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        def method(name):
            return channel.unary_unary(
                f"/probe.Probe/{name}",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString)

        big = "z" * 1_000_000
        resp = method("Echo")(pb.EchoRequest(message=big), timeout=60)
        record("grpc", "grpcio client: 1MB echo through mojo server", resp.message == big,
               f"len={len(resp.message)}")

        resp = method("Echo")(pb.EchoRequest(message=""), timeout=10)
        record("grpc", "grpcio client: empty message through mojo server", resp.message == "", "")

        resp = method("Timeout")(pb.EchoRequest(message="t"), timeout=3.0)
        ns = int(resp.message)
        # grpcio may round the wire timeout up slightly; allow 5% headroom.
        record("grpc", "grpcio deadline decoded by mojo server (3s sent)",
               2_000_000_000 <= ns <= 3_150_000_000, f"decoded {ns}ns")

        resp, call = method("MetaEcho").with_call(
            pb.EchoRequest(message="m"), timeout=10,
            metadata=(("x-py", "1"), ("x-raw-bin", b"\x00\xff")))
        seen = resp.message
        b64 = base64.b64encode(b"\x00\xff").decode().rstrip("=")
        meta_ok = "x-py=1" in seen and ("x-raw-bin=" + b64 in seen or "x-raw-bin=" + b64 + "=" in seen)
        record("grpc", "grpcio client metadata (ascii + -bin) visible in mojo handler", meta_ok, seen)
        imd = dict(call.initial_metadata())
        tmd = dict(call.trailing_metadata())
        record("grpc", "mojo server initial response metadata", imd.get("x-initial") == "from-mojo", str(imd))
        record("grpc", "mojo server trailing metadata", tmd.get("x-trailer") == "mojo-trailer", str(tmd))
        record("grpc", "mojo server binary trailing metadata (-bin, unpadded emit)",
               tmd.get("x-blob-bin") == b"\xde\xad\xbe\xef", str(tmd.get("x-blob-bin")))

        try:
            method("FailUnicode")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "mojo handler error -> grpcio UNKNOWN + unicode details", False, "no error raised")
        except grpc.RpcError as e:
            ok = (e.code() == grpc.StatusCode.UNKNOWN
                  and "falhou: résumé 100% 🔥" in e.details())
            record("grpc", "mojo handler error -> grpcio UNKNOWN + unicode details", ok,
                   f"{e.code()} {e.details()!r}")

        try:
            method("FailRich")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "mojo server rich error (grpc-status-details-bin)", False, "no error")
        except grpc.RpcError as e:
            from google.rpc import status_pb2
            tmd = dict(e.trailing_metadata())
            raw = tmd.get("grpc-status-details-bin")
            ok = False
            if raw:
                st = status_pb2.Status.FromString(raw)
                ok = st.code == 5 and st.message == "rich"
            record("grpc", "mojo server rich error (grpc-status-details-bin)", ok, str(tmd)[:150])

        try:
            method("Nope")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "unknown method -> UNIMPLEMENTED trailers-only", False, "no error")
        except grpc.RpcError as e:
            record("grpc", "unknown method -> UNIMPLEMENTED trailers-only",
                   e.code() == grpc.StatusCode.UNIMPLEMENTED, str(e.code()))

        ok = True
        for i in range(10):
            r = method("Echo")(pb.EchoRequest(message=f"seq{i}"), timeout=10)
            ok = ok and r.message == f"seq{i}"
        record("grpc", "10 sequential calls on one connection", ok, "")
        channel.close()
    finally:
        proc.kill(); proc.wait()


def section_grpc_polling_server(tmp: Path):
    """Judge the bounded unary h2c event loop with grpcio and hyper-h2."""
    print("== grpc polling server with grpcio and hyper-h2 peers ==")
    import grpc
    import echo_pb2 as pb

    proc, port = _start_polling_server(128, 4 * 1024 * 1024, 64 * 1024, 64)
    try:
        stalled = socket.create_connection(("127.0.0.1", port), timeout=5)
        stalled.sendall(b"PRI * HTTP/2.0\r\n")
        try:
            work = [(port, worker, 25, False) for worker in range(16)]
            with ProcessPoolExecutor(
                max_workers=16, mp_context=multiprocessing.get_context("spawn")
            ) as pool:
                outcomes = list(pool.map(_polling_process_worker, work))
            record(
                "grpc-polling",
                "16 grpcio processes complete 25 unary calls each while a partial preface stalls",
                outcomes == [True] * 16,
                f"passed={sum(outcomes)}/16 calls={16 * 25}",
            )
        finally:
            stalled.close()

        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            echo = channel.unary_unary(
                "/probe.Probe/Echo",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString,
            )
            large = "z" * 1_000_000
            response = echo(pb.EchoRequest(message=large), timeout=30)
            record(
                "grpc-polling",
                "1 MiB grpcio unary response completes with a 64 KiB configured output limit",
                response.message == large,
                f"response={len(response.message)} bytes bound=65536",
            )

            empty = echo(pb.EchoRequest(message=""), timeout=10)
            metadata = channel.unary_unary(
                "/probe.Probe/Metadata",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString,
            )
            meta_response, meta_call = metadata.with_call(
                pb.EchoRequest(message="metadata"), timeout=10
            )
            initial = dict(meta_call.initial_metadata())
            trailing = dict(meta_call.trailing_metadata())
            fail = channel.unary_unary(
                "/probe.Probe/Fail",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString,
            )
            unknown = False
            try:
                fail(pb.EchoRequest(message="fail"), timeout=10)
            except grpc.RpcError as error:
                unknown = error.code() == grpc.StatusCode.UNKNOWN
            supported_ok = (
                empty.message == ""
                and meta_response.message == "metadata"
                and initial.get("x-polling-initial") == "ready"
                and trailing.get("x-polling-trailer") == "done"
                and unknown
            )
            record(
                "grpc-polling",
                "grpcio observes empty unary, metadata, trailers, and handler UNKNOWN",
                supported_ok,
                f"initial={initial} trailing={trailing} unknown={unknown}",
            )

        stalled_output = _RawH2GrpcClient(
            port, receive_window=4 * 1024 * 1024, receive_buffer=1024
        )
        try:
            stalled_output.start_request(
                "/probe.Probe/Large",
                _grpc_frame(pb.EchoRequest(message="large").SerializeToString()),
                extra_headers=(("grpc-timeout", "100m"),),
            )
            time.sleep(0.3)
            started = time.monotonic()
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                echo = channel.unary_unary(
                    "/probe.Probe/Echo",
                    request_serializer=pb.EchoRequest.SerializeToString,
                    response_deserializer=pb.EchoResponse.FromString,
                )
                response = echo(
                    pb.EchoRequest(message="not stalled"), timeout=10
                )
            elapsed = time.monotonic() - started
            stalled_ok = response.message == "not stalled" and elapsed < 2.0
        finally:
            stalled_output.close()
        record(
            "grpc-polling",
            "an expired large response with a stalled reader does not spin the loop",
            stalled_ok,
            f"healthy_elapsed={elapsed:.3f}s",
        )

        raw = _RawH2GrpcClient(port)
        try:
            split_ok = True
            for split in range(1, 5):
                message = f"split-{split}"
                framed = _grpc_frame(
                    pb.EchoRequest(message=message).SerializeToString()
                )
                sid = raw.start_request(
                    "/probe.Probe/Echo", None, end_stream=False
                )
                raw.conn.send_data(sid, framed[:split], end_stream=False)
                raw.conn.send_data(sid, framed[split:], end_stream=True)
                raw.sock.sendall(raw.conn.data_to_send())
                result = raw.finish(sid)
                split_ok &= (
                    _grpc_status(result) == 0
                    and _decode_echo_response(pb, result).message == message
                )
            record(
                "grpc-polling",
                "all four internal gRPC prefix split boundaries complete",
                split_ok,
                "boundaries=1,2,3,4",
            )

            invalid_cases = [
                ("compressed", _grpc_frame(b"x", compressed=1), 13),
                ("multiple", _grpc_frame(b"a") + _grpc_frame(b"b"), 13),
                ("truncated", b"\x00\x00\x00\x00\x03x", 13),
            ]
            invalid_ok = True
            details = []
            for name, body, expected in invalid_cases:
                _, result = raw.request("/probe.Probe/Fail", body)
                status = _grpc_status(result)
                invalid_ok &= status == expected
                _, healthy = raw.request(
                    "/probe.Probe/Echo",
                    _grpc_frame(pb.EchoRequest(message=name).SerializeToString()),
                )
                response = _decode_echo_response(pb, healthy)
                invalid_ok &= _grpc_status(healthy) == 0 and response.message == name
                details.append(f"{name}={status}")
            record(
                "grpc-polling",
                "wire grammar rejects compressed, multiple, and truncated unary input",
                invalid_ok,
                " ".join(details),
            )

            _, get_result = raw.request(
                "/probe.Probe/Fail", _grpc_frame(b""), method="GET"
            )
            _, type_result = raw.request(
                "/probe.Probe/Fail",
                _grpc_frame(b""),
                content_type="application/grpcx",
            )
            _, empty_subtype = raw.request(
                "/probe.Probe/Fail",
                _grpc_frame(b""),
                content_type="application/grpc+",
            )
            _, missing_result = raw.request(
                "/probe.Probe/Missing", _grpc_frame(b"")
            )
            strict_ok = (
                _grpc_status(get_result) == 13
                and type_result["headers"].get(b":status") == b"415"
                and empty_subtype["headers"].get(b":status") == b"415"
                and _grpc_status(missing_result) == 12
            )
            record(
                "grpc-polling",
                "protocol grammar enforces POST, content type, and registered routes",
                strict_ok,
                f"GET={_grpc_status(get_result)} "
                f"content-type={type_result['headers'].get(b':status')} "
                f"empty-subtype={empty_subtype['headers'].get(b':status')} "
                f"missing={_grpc_status(missing_result)}",
            )

        finally:
            raw.close()

        deadline = _RawH2GrpcClient(port)
        try:
            sid = deadline.conn.get_next_available_stream_id()
            deadline.conn.send_headers(
                sid,
                [
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":authority", "127.0.0.1"),
                    (":path", "/probe.Probe/Fail"),
                    ("te", "trailers"),
                    ("content-type", "application/grpc"),
                    ("grpc-timeout", "100m"),
                ],
                end_stream=False,
            )
            deadline.sock.sendall(deadline.conn.data_to_send())
            started = time.monotonic()
            result = deadline.finish(sid, timeout=5)
            elapsed = time.monotonic() - started
            record(
                "grpc-polling",
                "idle grpc-timeout wakes the poller before invoking the handler",
                _grpc_status(result) == 4 and 0.05 <= elapsed < 2.0,
                f"status={_grpc_status(result)} elapsed={elapsed:.3f}s",
            )
        finally:
            deadline.close()

        complete = _RawH2GrpcClient(port)
        try:
            sid = complete.start_request(
                "/probe.Probe/Echo",
                _grpc_frame(pb.EchoRequest(message="final").SerializeToString()),
            )
            complete.sock.shutdown(socket.SHUT_WR)
            complete.write_closed = True
            result = complete.finish(sid)
            final_ok = (
                _grpc_status(result) == 0
                and _decode_echo_response(pb, result).message == "final"
            )
        finally:
            complete.close()

        truncated = _RawH2GrpcClient(port)
        try:
            sid = truncated.start_request(
                "/probe.Probe/Fail", b"\x00\x00\x00\x00\x03x"
            )
            truncated.sock.shutdown(socket.SHUT_WR)
            truncated.write_closed = True
            result = truncated.finish(sid)
            truncated_ok = _grpc_status(result) == 13
        finally:
            truncated.close()
        record(
            "grpc-polling",
            "readable final bytes beat FIN and truncated FIN skips the handler",
            final_ok and truncated_ok,
            f"complete={final_ok} truncated={truncated_ok}",
        )
    finally:
        proc.kill()
        proc.wait()

    proc, port = _start_polling_server(
        4, 4 * 1024 * 1024, 1024, 64, 300, 300
    )
    try:
        partial = socket.create_connection(("127.0.0.1", port), timeout=5)
        partial.sendall(b"PRI * HTTP/2.0\r\n")
        magic = socket.create_connection(("127.0.0.1", port), timeout=5)
        magic.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        preface = _RawH2GrpcClient(port)
        unfinished = _RawH2GrpcClient(port)
        unfinished.start_request(
            "/probe.Probe/Fail", None, end_stream=False
        )

        def pending_call():
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                echo = channel.unary_unary(
                    "/probe.Probe/Echo",
                    request_serializer=pb.EchoRequest.SerializeToString,
                    response_deserializer=pb.EchoResponse.FromString,
                )
                return echo(pb.EchoRequest(message="recovered"), timeout=10).message

        with futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(pending_call)
            time.sleep(0.1)
            paused = not future.done()
            recovered = future.result(timeout=10) == "recovered"
        partial.close()
        magic.close()
        preface.close()
        unfinished.close()
        error_response = False
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            missing = channel.unary_unary(
                "/probe.Probe/Missing",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString,
            )
            try:
                missing(pb.EchoRequest(message="missing"), timeout=10)
            except grpc.RpcError as error:
                error_response = error.code() == grpc.StatusCode.UNIMPLEMENTED
        record(
            "grpc-polling",
            "idle and incomplete peers leave a full cap, and 1 KiB emits both statuses",
            paused and recovered and error_response,
            f"paused={paused} recovered={recovered} error={error_response}",
        )
    finally:
        proc.kill()
        proc.wait()

    proc, port = _start_polling_server(8, 32, 64 * 1024, 1)
    try:
        raw = _RawH2GrpcClient(port)
        try:
            declared = b"x" * 33
            _, rejected = raw.request(
                "/probe.Probe/Fail", _grpc_frame(declared)
            )
            _, healthy = raw.request(
                "/probe.Probe/Echo",
                _grpc_frame(pb.EchoRequest(message="ok").SerializeToString()),
            )
            response = _decode_echo_response(pb, healthy)
            bounded_ok = (
                _grpc_status(rejected) == 8
                and _grpc_status(healthy) == 0
                and response.message == "ok"
            )

            retired_sid = raw.conn.get_next_available_stream_id()
            raw.conn.send_headers(
                retired_sid,
                [
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":authority", "127.0.0.1"),
                    (":path", "/probe.Probe/Echo"),
                    ("te", "trailers"),
                    ("content-type", "application/grpc"),
                ],
            )
            raw.conn.send_data(
                retired_sid,
                _grpc_frame(pb.EchoRequest(message="retire").SerializeToString()),
                end_stream=True,
            )
            raw.sock.sendall(raw.conn.data_to_send())
            retired_result = raw.finish(retired_sid)

            from hyperframe.frame import DataFrame
            old = DataFrame(retired_sid)
            old.data = b"late"
            raw.sock.sendall(old.serialize())
            reset_old = raw.expect_reset(retired_sid, 5)
            later_sid = raw.start_request(
                "/probe.Probe/Echo",
                _grpc_frame(pb.EchoRequest(message="later").SerializeToString()),
            )
            later = raw.finish(later_sid)
            later_response = _decode_echo_response(pb, later)
            retired_ok = (
                _grpc_status(retired_result) == 0
                and reset_old
                and _grpc_status(later) == 0
                and later_response.message == "later"
            )

        finally:
            raw.close()

        concurrent = _RawH2GrpcClient(port)
        try:
            # Send both streams before reading the server SETTINGS that
            # advertises the one-active-stream limit. The server must still
            # enforce that limit on the wire.
            first_sid = concurrent.start_request(
                "/probe.Probe/Echo", None, end_stream=False
            )
            refused_sid = concurrent.start_request(
                "/probe.Probe/Echo",
                _grpc_frame(pb.EchoRequest(message="refused").SerializeToString()),
            )
            refused = concurrent.expect_reset(refused_sid, 7)
            concurrent.conn.send_data(
                first_sid,
                _grpc_frame(pb.EchoRequest(message="first").SerializeToString()),
                end_stream=True,
            )
            concurrent.sock.sendall(concurrent.conn.data_to_send())
            first = concurrent.finish(first_sid)
            _, next_result = concurrent.request(
                "/probe.Probe/Echo",
                _grpc_frame(pb.EchoRequest(message="next").SerializeToString()),
            )
            concurrent_ok = (
                refused
                and _grpc_status(first) == 0
                and _decode_echo_response(pb, first).message == "first"
                and _grpc_status(next_result) == 0
                and _decode_echo_response(pb, next_result).message == "next"
            )
        finally:
            concurrent.close()
        record(
            "grpc-polling",
            "oversized input recovers with one-frame turns and retired ids stay closed",
            bounded_ok and retired_ok and concurrent_ok,
            f"bounded={bounded_ok} retired={retired_ok} "
            f"concurrent={concurrent_ok} "
            f"first={_grpc_status(retired_result)} resets={later['resets']} "
            f"later={_grpc_status(later)} body={later_response.message!r}",
        )
    finally:
        proc.kill()
        proc.wait()


def _polling_secure_channel(grpc, port):
    credentials = grpc.ssl_channel_credentials(
        root_certificates=(CERTS / "ca.pem").read_bytes()
    )
    return grpc.secure_channel(f"localhost:{port}", credentials)


def _polling_echo_method(grpc, pb, channel):
    return channel.unary_unary(
        "/probe.Probe/Echo",
        request_serializer=pb.EchoRequest.SerializeToString,
        response_deserializer=pb.EchoResponse.FromString,
    )


def section_grpc_polling_tls(tmp: Path):
    """Judge PollingServer TLS with grpcio and CPython ssl peers."""
    print("== grpc polling server TLS with grpcio and CPython ssl peers ==")
    import grpc
    import echo_pb2 as pb

    limits = (128, 4 * 1024 * 1024, 64 * 1024, 64, 300_000, 30_000, 10_000)
    proc, port = _start_polling_server(*limits, use_tls=True)
    try:
        stalled_handshake = socket.create_connection(("127.0.0.1", port), timeout=5)
        stalled_handshake.sendall(b"\x16\x03\x01\x00")
        try:
            work = [(port, worker, 10, True) for worker in range(8)]
            with ProcessPoolExecutor(
                max_workers=8, mp_context=multiprocessing.get_context("spawn")
            ) as pool:
                outcomes = list(pool.map(_polling_process_worker, work))
            with _polling_secure_channel(grpc, port) as channel:
                echo = _polling_echo_method(grpc, pb, channel)
                large = "t" * 1_000_000
                response = echo(pb.EchoRequest(message=large), timeout=30)
            record(
                "grpc-polling-tls",
                "8 grpcio TLS processes and a 1 MiB unary call complete beside a stalled handshake",
                outcomes == [True] * 8 and response.message == large,
                f"workers={sum(outcomes)}/8 response={len(response.message)} bytes",
            )
        finally:
            stalled_handshake.close()

        deadline = _RawH2GrpcClient(port, use_tls=True)
        try:
            sid = deadline.conn.get_next_available_stream_id()
            deadline.conn.send_headers(
                sid,
                [
                    (":method", "POST"),
                    (":scheme", "https"),
                    (":authority", "localhost"),
                    (":path", "/probe.Probe/Fail"),
                    ("te", "trailers"),
                    ("content-type", "application/grpc"),
                    ("grpc-timeout", "100m"),
                ],
                end_stream=False,
            )
            deadline.sock.sendall(deadline.conn.data_to_send())
            started = time.monotonic()
            result = deadline.finish(sid, timeout=5)
            elapsed = time.monotonic() - started
        finally:
            deadline.close()
        record(
            "grpc-polling-tls",
            "idle TLS grpc-timeout writes a deadline response without more peer input",
            _grpc_status(result) == 4 and 0.05 <= elapsed < 2.0,
            f"status={_grpc_status(result)} elapsed={elapsed:.3f}s",
        )

        stalled_output = _RawH2GrpcClient(
            port,
            receive_window=4 * 1024 * 1024,
            receive_buffer=1024,
            use_tls=True,
        )
        try:
            stalled_output.start_request(
                "/probe.Probe/Large",
                _grpc_frame(pb.EchoRequest(message="large").SerializeToString()),
                extra_headers=(("grpc-timeout", "100m"),),
            )
            time.sleep(0.3)
            started = time.monotonic()
            with _polling_secure_channel(grpc, port) as channel:
                echo = _polling_echo_method(grpc, pb, channel)
                response = echo(pb.EchoRequest(message="healthy"), timeout=10)
            elapsed = time.monotonic() - started
            backpressure_ok = response.message == "healthy" and elapsed < 2.0
        finally:
            stalled_output.close()
        record(
            "grpc-polling-tls",
            "TLS output backpressure does not spin or starve a healthy grpcio call",
            backpressure_ok,
            f"healthy_elapsed={elapsed:.3f}s",
        )

        def rejected_alpn(protocols):
            context = ssl.create_default_context(cafile=CERTS / "ca.pem")
            if protocols is not None:
                context.set_alpn_protocols(protocols)
            try:
                raw = socket.create_connection(("127.0.0.1", port), timeout=5)
                with context.wrap_socket(raw, server_hostname="localhost") as peer:
                    peer.settimeout(2)
                    selected = peer.selected_alpn_protocol()
                    try:
                        closed = peer.recv(1) == b""
                    except OSError:
                        closed = True
                    return selected != "h2" and closed
            except (ssl.SSLError, ConnectionResetError):
                return True

        no_overlap = rejected_alpn(["http/1.1"])
        no_alpn = rejected_alpn(None)

        wrong_hostname = False
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(CERTS / "ca.pem").read_bytes()
        )
        with grpc.secure_channel(
            f"localhost:{port}",
            credentials,
            options=(("grpc.ssl_target_name_override", "wrong.example"),),
        ) as channel:
            echo = _polling_echo_method(grpc, pb, channel)
            try:
                echo(pb.EchoRequest(message="wrong-host"), timeout=3)
            except grpc.RpcError:
                wrong_hostname = True

        wrong_ca = False
        with grpc.secure_channel(
            f"localhost:{port}", grpc.ssl_channel_credentials()
        ) as channel:
            echo = _polling_echo_method(grpc, pb, channel)
            try:
                echo(pb.EchoRequest(message="wrong-ca"), timeout=3)
            except grpc.RpcError:
                wrong_ca = True

        with _polling_secure_channel(grpc, port) as channel:
            echo = _polling_echo_method(grpc, pb, channel)
            recovered = echo(
                pb.EchoRequest(message="recovered"), timeout=10
            ).message == "recovered"
        record(
            "grpc-polling-tls",
            "ALPN, hostname, and trust failures are isolated from later TLS RPCs",
            no_overlap and no_alpn and wrong_hostname and wrong_ca and recovered,
            f"no_overlap={no_overlap} no_alpn={no_alpn} "
            f"wrong_hostname={wrong_hostname} wrong_ca={wrong_ca} "
            f"recovered={recovered}",
        )
    finally:
        proc.kill()
        proc.wait()

    timeout_limits = (
        4,
        4 * 1024 * 1024,
        64 * 1024,
        64,
        300_000,
        30_000,
        300,
    )
    proc, port = _start_polling_server(*timeout_limits, use_tls=True)
    attackers = []
    try:
        attackers = [
            socket.create_connection(("127.0.0.1", port), timeout=5)
            for _ in range(4)
        ]
        started = time.monotonic()
        with _polling_secure_channel(grpc, port) as channel:
            echo = _polling_echo_method(grpc, pb, channel)
            response = echo(pb.EchoRequest(message="timeout"), timeout=5)
        elapsed = time.monotonic() - started
        record(
            "grpc-polling-tls",
            "absolute handshake timeout frees a full connection cap without attacker closes",
            response.message == "timeout" and 0.2 <= elapsed < 3.0,
            f"elapsed={elapsed:.3f}s response={response.message!r}",
        )
    finally:
        for attacker in attackers:
            attacker.close()
        proc.kill()
        proc.wait()


def section_grpc_tls(tmp: Path):
    print("== grpc TLS: grpc-mojo vs grpcio ==")
    import grpc
    import echo_pb2 as pb

    server, port = make_grpcio_probe_server(pb, use_tls=True)
    try:
        r = run_tool(
            "grpc_client_probe",
            port,
            "echo",
            65536,
            "tls",
            CERTS / "ca.pem",
            "localhost",
            timeout=120,
        )
        record(
            "grpc-tls",
            "Mojo TLS client: 64 KiB unary echo via grpcio server",
            "len=65536 match=True code=0" in r.stdout,
            r.stdout.strip() + r.stderr[-200:],
        )
        r = run_tool(
            "grpc_client_probe",
            port,
            "echo",
            1,
            "tls",
            CERTS / "ca.pem",
            "wrong.example",
            timeout=120,
        )
        record(
            "grpc-tls",
            "Mojo TLS client rejects a grpcio certificate hostname mismatch",
            r.returncode != 0,
            r.stdout.strip() + r.stderr[-200:],
        )
    finally:
        server.stop(0)

    proc = subprocess.Popen(
        [
            *MOJO_RUN,
            str(TOOLS / "grpc_server_probe.mojo"),
            str(CERTS / "server.pem"),
            str(CERTS / "server.key"),
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    line = proc.stdout.readline()
    port = int(line.strip().rsplit(":", 1)[-1])
    try:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(CERTS / "ca.pem").read_bytes()
        )
        with grpc.secure_channel(
            f"localhost:{port}", credentials
        ) as channel:
            echo = channel.unary_unary(
                "/probe.Probe/Echo",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString,
            )
            response = echo(pb.EchoRequest(message="secure"), timeout=30)
            record(
                "grpc-tls",
                "grpcio TLS client: unary echo via Mojo server",
                response.message == "secure",
                response.message,
            )
    finally:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------- units ---

def section_units():
    print("== unit suites (spec vectors + reference goldens) ==")
    r = subprocess.run([sys.executable, "tools/run_tests.py"], cwd=ROOT,
                       capture_output=True, text=True)
    m = re.search(r"(\d+)/(\d+) test files passed", r.stdout)
    ok = r.returncode == 0 and m and m.group(1) == m.group(2)
    record("units", f"unit test files ({m.group(0) if m else 'no summary'})", ok,
           r.stdout[-300:] if not ok else "")
    r = subprocess.run([sys.executable, "test/interop/run_interop.py"], cwd=ROOT,
                       capture_output=True, text=True)
    m = re.search(r"interop: (\d+) passed, (\d+) failed", r.stdout)
    ok = r.returncode == 0 and m and m.group(2) == "0"
    record("units", f"interop suite ({m.group(0) if m else 'no summary'})", ok,
           r.stdout[-300:] if not ok else "")


def section_official_interop():
    """Load the canonical outcomes emitted by the official interop runner."""
    print("== official gRPC interoperability results ==")
    try:
        document = json.loads(OFFICIAL_RESULTS_REPORT.read_text())
        rows, problems = evaluated_results(document)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        record("grpc-official", "official interop result source", False, str(exc))
        return

    for row in rows:
        record(
            "grpc-official",
            f"{row['label']} ({row['transport']}): {row['case']}",
            bool(row["passed"]),
            str(row["detail"]),
        )
    for problem in problems:
        record("grpc-official", "official interop result source", False, problem)


# --------------------------------------------------------------- report ---

def versions() -> dict[str, str]:
    import google.protobuf, grpc, hpack, h2, hyperframe
    mojo = subprocess.run(["mojo", "--version"], capture_output=True, text=True, cwd=ROOT).stdout.strip()
    return {
        "mojo": mojo,
        "python": platform.python_version(),
        "protobuf (reference for proto)": google.protobuf.__version__,
        "grpcio (reference for grpc)": grpc.__version__,
        "hpack (reference for hpack)": hpack.__version__,
        "h2/hyper-h2 (reference for h2)": h2.__version__,
        "hyperframe (reference for h2 frames)": hyperframe.__version__,
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
    }


def section_order() -> list[str]:
    """Canonical report order, plus any unexpected sections at the end."""
    known = [
        "proto", "hpack", "h2", "net", "tls", "grpc", "grpc-tls",
        "grpc-transport", "grpc-polling", "grpc-polling-tls",
        "grpc-official", "packaging", "units",
    ]
    return known + [s for s in RESULTS if s not in known]


SECTION_TITLES = {
    "proto": ("`proto` vs Python `protobuf`",
              "Randomized differential testing: the reference implementation encodes seeded random messages; grpc-mojo decodes and re-encodes them; the reference parses the result and compares for semantic equality (byte equality where the encoding is deterministic). Malformed inputs must be accepted/rejected in agreement with the reference."),
    "hpack": ("`hpack` vs python-hpack",
              "Sequential header blocks encoded by one implementation and decoded by the other, in both directions, with dynamic-table state carried across blocks. Plus RFC 7541 Appendix C unit vectors (see `units`)."),
    "h2": ("`h2` vs hyper-h2 / hyperframe / h2spec",
           "Frame codec cross-checked byte-for-byte against hyperframe in both directions. Live connections run against hyper-h2, which raises ProtocolError on any protocol violation by the peer. h2spec (the standard RFC 9113/7541 conformance tool) runs its full suite against our server."),
    "tls": ("`tls` vs CPython `ssl`",
            "Live TLS connections with CPython on the other end in both roles: version negotiation, ALPN agreement and no-overlap behavior, chain and hostname verification, bulk transfer, and a bad-certificate corpus both implementations must reject."),
    "net": ("`net` vs CPython sockets",
            "1 MiB echo in both directions between grpc-mojo TCP and CPython sockets, including half-close (shutdown) and clean-EOF semantics."),
    "grpc": ("`grpc` vs grpcio",
             "Behavioral compliance against the reference gRPC implementation in both directions: status-code mapping (all 16 codes), unicode/percent status details, ascii and binary (-bin) metadata in requests, initial response metadata and trailers, deadline (grpc-timeout) propagation, empty and 1 MB messages, sequential calls."),
    "grpc-tls": ("`grpc` over TLS vs grpcio",
                 "TLS connections run in both directions with strict certificate verification and h2 ALPN negotiation. Payloads cross the reference boundary through grpcio and grpc-mojo."),
    "grpc-transport": ("`GrpcTransport` vs CPython sockets and ssl",
                       "The readiness wrapper transfers a large payload through CPython TCP and TLS peers with partial reads, partial writes, and would-block handling. TLS retries preserve the direction reported by OpenSSL across same-operation retries."),
    "grpc-polling": ("`PollingServer` with grpcio and hyper-h2 external peers",
                     "Independent grpcio processes exercise supported unary behavior, while hyper-h2 drives edge cases and stricter policy grounded in the gRPC HTTP/2 wire grammar. The checks cover configured output limits, fairness, deadlines, rejected-request isolation, connection-cap recovery, half-close ordering, and retired stream ids."),
    "grpc-polling-tls": ("`PollingServer` TLS with grpcio and CPython ssl peers",
                         "Independent grpcio processes and CPython ssl exercise non-blocking TLS handshakes, strict h2 ALPN, certificate verification, handshake-cap recovery, large responses, and output backpressure."),
    "grpc-official": ("Official gRPC interoperability vs grpcio",
                      "All 12 canonical interoperability cases run with grpc-mojo in both client and server roles over h2c, verified TLS, and Unix domain sockets. These rows and the published badge come from the same machine-readable result document."),
    "packaging": ("Extraction isolation",
                  "Each package is staged into a scratch directory with only its declared dependencies (docs/ARCHITECTURE.md), then compiled and executed there. A package that reaches outside its dependency set fails this check. This is the mechanical proof behind independent open-sourcing."),
    "units": ("Spec-vector unit suites",
              "The repo's unit tests are themselves reference-anchored: protobuf goldens generated by Python protobuf, all RFC 7541 Appendix C vectors, and the grpcio interop suite."),
}




HTML_HEAD = """<!-- GENERATED by test/compliance/run_compliance.py - regenerate with: pixi run compliance -->
<title>grpc-mojo Compliance</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans+Condensed:wght@600&display=swap">
<style>
:root {
  --paper: #FAFAF8; --ink: #22262B; --muted: #6E6A62; --accent: #C2551F;
  --pass: #2E7D4F; --fail: #B3362B; --line: #E4E0D8; --panel: #F2F0EA;
  --mono: "IBM Plex Mono", ui-monospace, "SF Mono", Menlo, monospace;
  --sans: "IBM Plex Sans", -apple-system, "Segoe UI", sans-serif;
  --cond: "IBM Plex Sans Condensed", "Arial Narrow", var(--sans);
}
:root:not([data-theme="light"]) { }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --paper: #16181C; --ink: #E8E6E1; --muted: #98938A; --accent: #E0663A;
    --pass: #5EC08D; --fail: #E5776C; --line: #2C2F35; --panel: #1D2025;
  }
}
:root[data-theme="dark"] {
  --paper: #16181C; --ink: #E8E6E1; --muted: #98938A; --accent: #E0663A;
  --pass: #5EC08D; --fail: #E5776C; --line: #2C2F35; --panel: #1D2025;
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--paper); color: var(--ink);
  font: 16px/1.6 var(--sans); -webkit-font-smoothing: antialiased;
}
main { max-width: 76ch; margin: 0 auto; padding: 3.5rem 1.5rem 5rem; }
header { border-bottom: 2px solid var(--ink); padding-bottom: 1.75rem; margin-bottom: 2.5rem; }
.eyebrow {
  font: 500 0.72rem/1 var(--mono); letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--accent); margin: 0 0 0.9rem;
}
h1 {
  font: 600 clamp(1.9rem, 5vw, 2.6rem)/1.1 var(--cond);
  margin: 0 0 1.1rem; text-wrap: balance; letter-spacing: -0.01em;
}
.verdict { display: flex; align-items: baseline; gap: 0.75rem; flex-wrap: wrap; }
.verdict .score {
  font: 500 2rem/1 var(--mono); font-variant-numeric: tabular-nums;
  color: var(--pass);
}
.verdict .score.failing { color: var(--fail); }
.verdict .when { color: var(--muted); font-size: 0.85rem; }
.thesis { color: var(--muted); margin: 0.9rem 0 0; max-width: 62ch; }
.scorecard {
  display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 1.5rem; padding: 0; list-style: none;
}
.scorecard li {
  font: 400 0.78rem/1 var(--mono); padding: 0.45rem 0.7rem;
  border: 1px solid var(--line); border-radius: 3px; background: var(--panel);
  display: flex; gap: 0.55rem; align-items: center;
}
.scorecard .n { font-variant-numeric: tabular-nums; color: var(--pass); font-weight: 500; }
.scorecard .n.failing { color: var(--fail); }
section { margin: 2.75rem 0; }
h2 { font: 600 1.15rem/1.3 var(--sans); margin: 0 0 0.35rem; text-wrap: balance; }
h2 .pkg { font-family: var(--mono); font-weight: 500; color: var(--accent); }
.vs { color: var(--muted); font-weight: 400; }
.method { color: var(--muted); font-size: 0.88rem; margin: 0 0 1.1rem; max-width: 68ch; }
.tablewrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 0.88rem; }
th {
  text-align: left; font: 500 0.7rem/1 var(--mono); letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--muted); padding: 0 0.75rem 0.5rem 0;
  border-bottom: 1px solid var(--ink);
}
td { padding: 0.5rem 0.75rem 0.5rem 0; border-bottom: 1px solid var(--line); vertical-align: top; }
td.result { white-space: nowrap; font: 500 0.78rem/1.8 var(--mono); }
.pass { color: var(--pass); }
.fail { color: var(--fail); }
td .detail { display: block; color: var(--muted); font-size: 0.8rem; }
.envtable td:first-child { color: var(--muted); width: 40%; }
.envtable td { font-family: var(--mono); font-size: 0.8rem; }
.gaps { border-left: 3px solid var(--accent); background: var(--panel); padding: 1.1rem 1.4rem; }
.gaps h2 { margin-top: 0; }
.gaps ul { margin: 0.5rem 0 0; padding-left: 1.1rem; }
.gaps li { margin: 0.45rem 0; font-size: 0.9rem; }
.gaps strong { font-weight: 600; }
footer { margin-top: 3rem; color: var(--muted); font: 400 0.78rem/1.6 var(--mono); border-top: 1px solid var(--line); padding-top: 1rem; }
code { font-family: var(--mono); font-size: 0.92em; }
</style>
"""


def esc(t: str) -> str:
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def write_html_report():
    total = sum(len(v) for v in RESULTS.values())
    passed = sum(1 for v in RESULTS.values() for _, ok, _ in v if ok)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    all_ok = passed == total
    h = [HTML_HEAD, "<main>", "<header>"]
    h.append('<p class="eyebrow">grpc-mojo &middot; differential compliance run</p>')
    h.append("<h1>Every layer, checked against the reference implementation</h1>")
    h.append(
        f'<div class="verdict"><span class="score{"" if all_ok else " failing"}">'
        f"{passed}/{total}</span><span>checks passed</span>"
        f'<span class="when">{now}</span></div>'
    )
    h.append(
        '<p class="thesis">No self-grading: protobuf bytes are judged by Python '
        "<code>protobuf</code>, header compression by python-hpack, HTTP/2 by "
        "hyper-h2 (which rejects any protocol violation), sockets by CPython, "
        "and gRPC behavior by <code>grpcio</code> in both directions. The "
        "proto, hpack, h2, and net sections run in each package&rsquo;s own "
        "compliance suite and are aggregated here.</p>"
    )
    h.append('<ul class="scorecard">')
    for section in section_order():
        if section not in RESULTS:
            continue
        rows = RESULTS[section]
        p = sum(1 for _, ok, _ in rows if ok)
        cls = "" if p == len(rows) else " failing"
        h.append(
            f'<li>{esc(section)} <span class="n{cls}">{p}/{len(rows)}</span></li>'
        )
    h.append("</ul></header>")

    for section in section_order():
        if section not in RESULTS:
            continue
        title, blurb = SECTION_TITLES.get(section, (f"`{section}`", ""))
        pkg, _, ref = title.replace("`", "").partition(" vs ")
        rows = RESULTS[section]
        h.append("<section>")
        if ref:
            h.append(
                f'<h2><span class="pkg">{esc(pkg)}</span> '
                f'<span class="vs">vs</span> {esc(ref)}</h2>'
            )
        else:
            h.append(f"<h2>{esc(pkg)}</h2>")
        h.append(f'<p class="method">{esc(blurb)}</p>')
        h.append('<div class="tablewrap"><table>')
        h.append("<tr><th>Check</th><th>Result</th></tr>")
        for name, ok, detail in rows:
            cell = '<span class="pass">PASS</span>' if ok else '<span class="fail">FAIL</span>'
            extra = "" if ok else f'<span class="detail">{esc(detail[:200])}</span>'
            h.append(
                f"<tr><td>{esc(name)}</td><td class=\"result\">{cell}{extra}</td></tr>"
            )
        h.append("</table></div></section>")

    h.append("<section><h2>Environment</h2>")
    h.append('<div class="tablewrap"><table class="envtable">')
    for k, v in versions().items():
        h.append(f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>")
    h.append("</table></div></section>")

    h.append('<section class="gaps"><h2>Known gaps (tracked, not silent)</h2><ul>')
    gaps = [

        ("Compression", "mojo-zlib exists, but grpc-encoding gzip integration remains pending (PRIMITIVES.md #4). Compressed messages are rejected, never mis-decoded."),
        ("Concurrency", "PollingServer overlaps bounded unary h2c or TLS connection I/O; handlers remain serialized. Streaming RPCs use the blocking server (PRIMITIVES.md #7)."),
        ("hpack value encoding", "header values are UTF-8 Strings; arbitrary octets are out of scope for now (gRPC uses base64 -bin metadata)."),
    ]
    for k, v in gaps:
        h.append(f"<li><strong>{esc(k)}</strong>: {esc(v)}</li>")
    h.append("</ul></section>")
    h.append(
        "<footer>Generated by test/compliance/run_compliance.py &middot; "
        "rerun with <code>pixi run compliance</code> &middot; canonical copy: "
        "docs/COMPLIANCE.md</footer>"
    )
    h.append("</main>")
    HTML_REPORT.write_text("\n".join(h))
    print(f"report: {HTML_REPORT.relative_to(ROOT)}")


def write_report():
    total = sum(len(v) for v in RESULTS.values())
    passed = sum(1 for v in RESULTS.values() for _, ok, _ in v if ok)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Compliance & Compatibility Report",
        "",
        "<!-- GENERATED by test/compliance/run_compliance.py; do not edit. -->",
        "<!-- Regenerate with: pixi run compliance -->",
        "",
        f"**Result: {passed}/{total} checks passed.** Generated {now}.",
        "",
        "Every check compares grpc-mojo against an established reference",
        "implementation, never against itself. The proto, hpack, h2, and net",
        "sections are executed by each package's own compliance suite",
        "(`packages/<pkg>/compliance/run_compliance.py`) and aggregated here;",
        "the umbrella adds the gRPC, packaging, and unit-suite sections.",
        "",
        "## Environment",
        "",
        "| Component | Version |",
        "|---|---|",
    ]
    for k, v in versions().items():
        lines.append(f"| {k} | {v} |")
    for section in section_order():
        if section not in RESULTS:
            continue
        title, blurb = SECTION_TITLES.get(section, (f"`{section}`", ""))
        rows = RESULTS[section]
        p = sum(1 for _, ok, _ in rows if ok)
        lines += ["", f"## {title}: {p}/{len(rows)}", "", blurb, "",
                  "| Check | Result |", "|---|---|"]
        for name, ok, detail in rows:
            status = "✅ pass" if ok else f"❌ **fail**: {detail[:160]}"
            lines.append(f"| {name} | {status} |")
    lines += [
        "",
        "## Known gaps (tracked, not silent)",
        "",

        "- **Compression**: `mojo-zlib` exists, but `grpc-encoding: gzip` integration remains pending (docs/PRIMITIVES.md item 4). Compressed messages are rejected, not mis-decoded.",
        "- **Concurrency**: `PollingServer` overlaps bounded unary h2c or TLS connection I/O; handlers remain serialized until Mojo exposes threads/async (PRIMITIVES.md item 7). Streaming RPCs remain on the blocking server.",
        "",
    ]
    REPORT.write_text("\n".join(lines))
    print(f"\ncompliance: {passed}/{total} checks passed")
    print(f"report: {REPORT.relative_to(ROOT)}")
    write_html_report()
    return passed == total


def main() -> int:
    build_tools()
    with tempfile.TemporaryDirectory(prefix="grpc_mojo_compliance_") as tmp_s:
        tmp = Path(tmp_s)
        run_package_suites(tmp)
        compile_test_protos(tmp)
        section_packaging(tmp)
        section_grpc_transport()
        section_grpc_client(tmp)
        section_grpc_server(tmp)
        section_grpc_polling_server(tmp)
        section_grpc_polling_tls(tmp)
        section_grpc_tls(tmp)
    section_units()
    section_official_interop()
    return 0 if write_report() else 1


if __name__ == "__main__":
    sys.exit(main())
