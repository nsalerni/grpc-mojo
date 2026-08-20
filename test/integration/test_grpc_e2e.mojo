# End-to-end gRPC test: the real GrpcChannel talking to the real Server
# dispatch over a loopback TCP connection, driven in one thread.

from std.testing import assert_equal, assert_true
from std.time import sleep

from h2 import Http2Connection
from grpc import GrpcChannel, Metadata, Server, ServerContext, StatusCode
from net import TCPStream, TCPListener
from proto import decode, encode
from echo_messages import EchoRequest, EchoResponse


def echo_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    return EchoResponse(message=String("echo: ") + req.message)


def meta_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    # Echo back a request metadata value to prove it arrived.
    var trace = ctx.metadata.get("x-trace")
    var suffix = String("<none>")
    if trace:
        suffix = trace.value()
    return EchoResponse(message=req.message + "|" + suffix)


def failing_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    raise Error("intentional failure: " + req.message)


def slow_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    sleep(0.12)
    return EchoResponse(message=String("too late"))


@fieldwise_init
struct TestRig(Movable):
    var channel: GrpcChannel
    var server: Server
    var server_conn: Http2Connection[TCPStream]
    var handled: List[UInt32]

    def pump_server_until_reply(mut self) raises:
        while True:
            self.server_conn.process_next_frame()
            if self.server.dispatch_ready(self.server_conn, self.handled) > 0:
                return


def make_rig() raises -> TestRig:
    var server = Server("127.0.0.1", 0)
    server.register_unary[echo_handler]("/echo.Echo/Say")
    server.register_unary[meta_handler]("/echo.Echo/Meta")
    server.register_unary[failing_handler]("/echo.Echo/Fail")
    server.register_unary[slow_handler]("/echo.Echo/Slow")

    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var server_conn = Http2Connection(server_tcp^, is_client=False)
    listener.close()
    return TestRig(
        channel=channel^,
        server=server^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def test_unary_ok() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="ping"))), last=True
    )
    rig.pump_server_until_reply()

    rig.channel.conn.wait_headers(sid)
    var msg = rig.channel.recv_response_bytes(sid)
    assert_true(Bool(msg), "response message present")
    var resp = decode[EchoResponse](Span(msg.value()))
    assert_equal(resp.message, "echo: ping")
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), "status OK")
    rig.channel.close()
    rig.server_conn.close()


def test_unary_with_metadata_and_timeout() raises:
    var rig = make_rig()
    var md = Metadata()
    md.add(String("x-trace"), String("trace-42"))
    var sid = rig.channel.start_call(
        "/echo.Echo/Meta", md, timeout_ns=5_000_000_000
    )
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="m"))), last=True
    )
    rig.pump_server_until_reply()

    rig.channel.conn.wait_headers(sid)
    var msg = rig.channel.recv_response_bytes(sid)
    var resp = decode[EchoResponse](Span(msg.value()))
    assert_equal(resp.message, "m|trace-42")
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), "status OK")
    rig.channel.close()
    rig.server_conn.close()


def test_unimplemented_method() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Nope", Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="x"))), last=True
    )
    rig.pump_server_until_reply()

    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.UNIMPLEMENTED)
    # Trailers-only: no message should be readable.
    var msg = rig.channel.recv_response_bytes(sid)
    assert_true(not Bool(msg), "no response message on trailers-only")
    rig.channel.close()
    rig.server_conn.close()


def test_handler_error_becomes_unknown() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Fail", Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="boom"))), last=True
    )
    rig.pump_server_until_reply()

    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.UNKNOWN)
    assert_true(
        result.status.message.find("intentional failure: boom") >= 0,
        "error message propagated (got: " + result.status.message + ")",
    )
    rig.channel.close()
    rig.server_conn.close()


def test_multiple_calls_one_connection() raises:
    var rig = make_rig()
    for i in range(3):
        var sid = rig.channel.start_call("/echo.Echo/Say", Metadata())
        rig.channel.send_request_bytes(
            sid,
            Span(encode(EchoRequest(message=String("n") + String(i)))),
            last=True,
        )
        rig.pump_server_until_reply()
        rig.channel.conn.wait_headers(sid)
        var msg = rig.channel.recv_response_bytes(sid)
        var resp = decode[EchoResponse](Span(msg.value()))
        assert_equal(resp.message, String("echo: n") + String(i))
        var result = rig.channel.finish(sid)
        assert_true(result.status.is_ok(), "status OK")
    # Stream ids advance by 2 per call.
    assert_equal(rig.channel.conn.next_stream_id, 7)
    rig.channel.close()
    rig.server_conn.close()


def test_server_enforces_deadline() raises:
    var rig = make_rig()
    # 50ms grpc-timeout, handler sleeps 120ms: server must abort with
    # DEADLINE_EXCEEDED instead of sending the response.
    var sid = rig.channel.start_call(
        "/echo.Echo/Slow", Metadata(), timeout_ns=50_000_000
    )
    rig.channel.deadline_ns = 0  # test the SERVER path, not client timeout
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="x"))), last=True
    )
    rig.pump_server_until_reply()
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.DEADLINE_EXCEEDED)
    rig.channel.close()
    rig.server_conn.close()


def main() raises:
    test_unary_ok()
    test_unary_with_metadata_and_timeout()
    test_unimplemented_method()
    test_handler_error_becomes_unknown()
    test_multiple_calls_one_connection()
    test_server_enforces_deadline()
    print("test_grpc_e2e: all tests passed")
