#!/usr/bin/env python3
"""Official gRPC interop test cases, run in both directions:

  A) grpc-mojo interop_client  vs  grpcio reference TestService server
  B) grpcio reference client   vs  grpc-mojo interop_server

Case semantics follow grpc/grpc doc/interop-test-descriptions.md.
Run via: pixi run interop-official
"""

import re
import subprocess
import sys
import tempfile
import time
from concurrent import futures
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
BUILD = ROOT / "build"
HERE = Path(__file__).resolve().parent
CERTS = BUILD / "certs"
MOJO_RUN = [
    "mojo", "run",
    "-I", "packages/mojo-net/src",
    "-I", "packages/mojo-http2/src",
    "-I", "packages/mojo-tls/src",
    "-I", "packages/protomojo/src",
    "-I", "src",
    "-I", "test",
    "-I", "packages/protomojo/test",
]

CASES = [
    "empty_unary",
    "large_unary",
    "client_streaming",
    "server_streaming",
    "ping_pong",
    "empty_stream",
    "custom_metadata",
    "status_code_and_message",
    "special_status_message",
    "unimplemented_method",
    "timeout_on_sleeping_server",
    "cancel_after_begin",
]

PASS = 0
FAIL = 0
RESULTS: list[tuple[str, str, bool, str]] = []


def record(direction: str, case: str, ok: bool, detail: str = ""):
    global PASS, FAIL
    if ok:
        PASS += 1
    else:
        FAIL += 1
    RESULTS.append((direction, case, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'} [{direction}] {case}"
          + ("" if ok else f"  <- {detail}"))


# --- proto setup -----------------------------------------------------------

_tmp = tempfile.mkdtemp(prefix="grpc_mojo_official_")
subprocess.run(
    [sys.executable, "-m", "grpc_tools.protoc", f"-I{HERE}",
     f"--python_out={_tmp}",
     str(HERE / "messages.proto"), str(HERE / "empty.proto")],
    check=True,
)
sys.path.insert(0, _tmp)

import grpc  # noqa: E402
import messages_pb2 as m  # noqa: E402
import empty_pb2 as e  # noqa: E402

SVC = "/grpc.testing.TestService/"
code_by_num = {c.value[0]: c for c in grpc.StatusCode}


# --- reference TestService (grpcio) ---------------------------------------

def _echo_metadata(ctx):
    imd = dict(ctx.invocation_metadata())
    initial = imd.get("x-grpc-test-echo-initial")
    if initial is not None:
        ctx.send_initial_metadata((("x-grpc-test-echo-initial", initial),))
    trailing = imd.get("x-grpc-test-echo-trailing-bin")
    if trailing is not None:
        ctx.set_trailing_metadata((("x-grpc-test-echo-trailing-bin", trailing),))


def _maybe_echo_status(status, ctx):
    if status.code != 0 or status.message:
        ctx.abort(code_by_num[status.code], status.message)


class RefTestService(grpc.GenericRpcHandler):
    def service(self, hcd):
        name = hcd.method.rsplit("/", 1)[-1]

        if name == "EmptyCall":
            return grpc.unary_unary_rpc_method_handler(
                lambda req, ctx: e.Empty(),
                request_deserializer=e.Empty.FromString,
                response_serializer=e.Empty.SerializeToString)

        if name == "UnaryCall":
            def unary(req, ctx):
                _echo_metadata(ctx)
                if req.HasField("response_status"):
                    _maybe_echo_status(req.response_status, ctx)
                return m.SimpleResponse(
                    payload=m.Payload(body=b"\x00" * req.response_size))
            return grpc.unary_unary_rpc_method_handler(
                unary,
                request_deserializer=m.SimpleRequest.FromString,
                response_serializer=m.SimpleResponse.SerializeToString)

        if name == "StreamingInputCall":
            def sin(request_iter, ctx):
                total = sum(len(r.payload.body) for r in request_iter)
                return m.StreamingInputCallResponse(aggregated_payload_size=total)
            return grpc.stream_unary_rpc_method_handler(
                sin,
                request_deserializer=m.StreamingInputCallRequest.FromString,
                response_serializer=m.StreamingInputCallResponse.SerializeToString)

        if name == "StreamingOutputCall":
            def sout(req, ctx):
                for p in req.response_parameters:
                    if p.interval_us:
                        time.sleep(p.interval_us / 1e6)
                    yield m.StreamingOutputCallResponse(
                        payload=m.Payload(body=b"\x00" * p.size))
            return grpc.unary_stream_rpc_method_handler(
                sout,
                request_deserializer=m.StreamingOutputCallRequest.FromString,
                response_serializer=m.StreamingOutputCallResponse.SerializeToString)

        if name == "FullDuplexCall":
            def duplex(request_iter, ctx):
                _echo_metadata(ctx)
                for req in request_iter:
                    if req.HasField("response_status"):
                        _maybe_echo_status(req.response_status, ctx)
                    for p in req.response_parameters:
                        if p.interval_us:
                            time.sleep(p.interval_us / 1e6)
                        yield m.StreamingOutputCallResponse(
                            payload=m.Payload(body=b"\x00" * p.size))
            return grpc.stream_stream_rpc_method_handler(
                duplex,
                request_deserializer=m.StreamingOutputCallRequest.FromString,
                response_serializer=m.StreamingOutputCallResponse.SerializeToString)

        return None  # -> UNIMPLEMENTED


# --- direction A: mojo client vs grpcio server ------------------------------

def run_direction_a(use_tls: bool):
    mode = "TLS" if use_tls else "h2c"
    print(f"== mojo interop_client vs grpcio reference server ({mode}) ==")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers((RefTestService(),))
    if use_tls:
        credentials = grpc.ssl_server_credentials(((
            (CERTS / "server.key").read_bytes(),
            (CERTS / "server.pem").read_bytes(),
        ),))
        port = server.add_secure_port("127.0.0.1:0", credentials)
    else:
        port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        for case in CASES:
            command = [
                *MOJO_RUN,
                str(HERE / "interop_client.mojo"),
                str(port),
                case,
            ]
            if use_tls:
                command.extend([
                    "tls", str(CERTS / "ca.pem"), "localhost",
                ])
            r = subprocess.run(
                command,
                capture_output=True, text=True, timeout=120, cwd=ROOT)
            ok = r.returncode == 0 and f"CASE-OK {case}" in r.stdout
            record(f"mojo-client-{mode.lower()}", case, ok,
                   f"rc={r.returncode} out={r.stdout.strip()!r} err={r.stderr[-200:]!r}")
    finally:
        server.stop(0)


# --- direction B: grpcio client vs mojo server -------------------------------

def duplex_req(size, payload_size, interval_us=0):
    return m.StreamingOutputCallRequest(
        response_parameters=[m.ResponseParameters(size=size, interval_us=interval_us)],
        payload=m.Payload(body=b"\x00" * payload_size))


def run_case_b(case, channel):
    unary = channel.unary_unary(
        SVC + "UnaryCall",
        request_serializer=m.SimpleRequest.SerializeToString,
        response_deserializer=m.SimpleResponse.FromString)
    empty_call = channel.unary_unary(
        SVC + "EmptyCall",
        request_serializer=e.Empty.SerializeToString,
        response_deserializer=e.Empty.FromString)
    sin = channel.stream_unary(
        SVC + "StreamingInputCall",
        request_serializer=m.StreamingInputCallRequest.SerializeToString,
        response_deserializer=m.StreamingInputCallResponse.FromString)
    sout = channel.unary_stream(
        SVC + "StreamingOutputCall",
        request_serializer=m.StreamingOutputCallRequest.SerializeToString,
        response_deserializer=m.StreamingOutputCallResponse.FromString)
    duplex = channel.stream_stream(
        SVC + "FullDuplexCall",
        request_serializer=m.StreamingOutputCallRequest.SerializeToString,
        response_deserializer=m.StreamingOutputCallResponse.FromString)

    if case == "empty_unary":
        assert empty_call(e.Empty(), timeout=20) == e.Empty()
    elif case == "large_unary":
        resp = unary(m.SimpleRequest(
            response_size=314159, payload=m.Payload(body=b"\x00" * 271828)),
            timeout=60)
        assert len(resp.payload.body) == 314159
    elif case == "client_streaming":
        reqs = (m.StreamingInputCallRequest(payload=m.Payload(body=b"\x00" * n))
                for n in (27182, 8, 1828, 45904))
        resp = sin(reqs, timeout=60)
        assert resp.aggregated_payload_size == 74922, resp
    elif case == "server_streaming":
        req = m.StreamingOutputCallRequest(response_parameters=[
            m.ResponseParameters(size=s) for s in (31415, 9, 2653, 58979)])
        sizes = [len(r.payload.body) for r in sout(req, timeout=60)]
        assert sizes == [31415, 9, 2653, 58979], sizes
    elif case == "ping_pong":
        import queue
        q = queue.Queue()
        def req_iter():
            while True:
                item = q.get()
                if item is None:
                    return
                yield item
        call = duplex(req_iter(), timeout=60)
        for rs, ps in zip((31415, 9, 2653, 58979), (27182, 8, 1828, 45904)):
            q.put(duplex_req(rs, ps))
            resp = next(call)
            assert len(resp.payload.body) == rs
        q.put(None)
        assert next(call, None) is None
    elif case == "empty_stream":
        call = duplex(iter(()), timeout=20)
        assert next(call, None) is None
    elif case == "custom_metadata":
        md = (("x-grpc-test-echo-initial", "test_initial_metadata_value"),
              ("x-grpc-test-echo-trailing-bin", b"\xab\xab\xab"))
        resp, call = unary.with_call(
            m.SimpleRequest(response_size=314159,
                            payload=m.Payload(body=b"\x00" * 271828)),
            metadata=md, timeout=60)
        imd = dict(call.initial_metadata())
        tmd = dict(call.trailing_metadata())
        assert imd.get("x-grpc-test-echo-initial") == "test_initial_metadata_value", imd
        assert tmd.get("x-grpc-test-echo-trailing-bin") == b"\xab\xab\xab", tmd
        call2 = duplex(iter([duplex_req(314159, 271828)]), metadata=md, timeout=60)
        assert len(next(call2).payload.body) == 314159
        assert next(call2, None) is None
        imd2 = dict(call2.initial_metadata())
        tmd2 = dict(call2.trailing_metadata())
        assert imd2.get("x-grpc-test-echo-initial") == "test_initial_metadata_value"
        assert tmd2.get("x-grpc-test-echo-trailing-bin") == b"\xab\xab\xab"
    elif case == "status_code_and_message":
        msg = "test status message"
        req = m.SimpleRequest(response_status=m.EchoStatus(code=2, message=msg))
        try:
            unary(req, timeout=20)
            raise AssertionError("no error raised")
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.UNKNOWN and err.details() == msg
        call = duplex(iter([m.StreamingOutputCallRequest(
            response_status=m.EchoStatus(code=2, message=msg))]), timeout=20)
        try:
            list(call)
            raise AssertionError("no duplex error raised")
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.UNKNOWN and err.details() == msg
    elif case == "special_status_message":
        msg = "\t\ntest with whitespace\r\nand Unicode BMP ☺ and non-BMP 😈\t\n"
        req = m.SimpleRequest(response_status=m.EchoStatus(code=2, message=msg))
        try:
            unary(req, timeout=20)
            raise AssertionError("no error raised")
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.UNKNOWN and err.details() == msg, err.details()
    elif case == "unimplemented_method":
        try:
            channel.unary_unary(
                SVC + "UnimplementedCall",
                request_serializer=e.Empty.SerializeToString,
                response_deserializer=e.Empty.FromString)(e.Empty(), timeout=20)
            raise AssertionError("no error raised")
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.UNIMPLEMENTED, err.code()
    elif case == "timeout_on_sleeping_server":
        call = duplex(iter([duplex_req(31415, 27182, interval_us=1_000_000)]),
                      timeout=0.2)
        try:
            list(call)
            raise AssertionError("no deadline error")
        except grpc.RpcError as err:
            assert err.code() == grpc.StatusCode.DEADLINE_EXCEEDED, err.code()
    elif case == "cancel_after_begin":
        import queue
        q = queue.Queue()
        def req_iter():
            while True:
                item = q.get()
                if item is None:
                    return
                yield item
        call = sin.future(req_iter(), timeout=20)
        time.sleep(0.05)
        call.cancel()
        assert call.cancelled()
        q.put(None)
        # The connection must remain usable afterwards.
        assert empty_call(e.Empty(), timeout=20) == e.Empty()
    else:
        raise AssertionError(f"unknown case {case}")


def run_direction_b(use_tls: bool):
    mode = "TLS" if use_tls else "h2c"
    print(f"== grpcio reference client vs mojo interop_server ({mode}) ==")
    command = [*MOJO_RUN, str(HERE / "interop_server.mojo")]
    if use_tls:
        command.extend([
            str(CERTS / "server.pem"), str(CERTS / "server.key"),
        ])
    proc = subprocess.Popen(command,
                            stdout=subprocess.PIPE, text=True, cwd=ROOT)
    line = proc.stdout.readline()
    port = int(line.strip().rsplit(":", 1)[-1])
    try:
        if use_tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=(CERTS / "ca.pem").read_bytes()
            )
            channel_context = grpc.secure_channel(
                f"localhost:{port}", credentials
            )
        else:
            channel_context = grpc.insecure_channel(f"127.0.0.1:{port}")
        with channel_context as channel:
            for case in CASES:
                try:
                    run_case_b(case, channel)
                    record(f"grpcio-client-{mode.lower()}", case, True)
                except Exception as exc:
                    record(
                        f"grpcio-client-{mode.lower()}",
                        case,
                        False,
                        repr(exc)[:200],
                    )
    finally:
        proc.kill()
        proc.wait()


def main() -> int:
    BUILD.mkdir(exist_ok=True)
    run_direction_a(False)
    run_direction_a(True)
    run_direction_b(False)
    run_direction_b(True)
    print(f"\nofficial interop: {PASS} passed, {FAIL} failed "
          f"({len(CASES)} cases x 2 directions x 2 transports)")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
