from std.ffi import c_int, external_call
from std.testing import assert_equal, assert_true
from std.time import sleep

from echo_pb import EchoRequest, EchoResponse
from grpc import (
    GrpcChannel,
    GrpcTransport,
    Metadata,
    PollingServer,
    Server,
    ServerContext,
    StatusCode,
)
from h2 import Http2Connection
from hpack import HeaderField
from net import TCPListener
from proto import decode, encode


def header_value(
    fields: Span[HeaderField, _], name: String
) -> Optional[String]:
    for field in fields:
        if field.name == name:
            return field.value.copy()
    return None


def require_value(
    found: Optional[String], expected: String, label: String
) raises:
    assert_true(Bool(found), label + " present")
    assert_equal(found.value(), expected, label)


def require_absent(found: Optional[String], label: String) raises:
    assert_true(not Bool(found), label + " absent")


@fieldwise_init
struct RawPeer(Movable):
    var channel: GrpcChannel
    var server_conn: Http2Connection[GrpcTransport]

    def pump_until_request_end(mut self, sid: UInt32) raises:
        while True:
            if (
                sid in self.server_conn.streams
                and self.server_conn.streams[sid].end_stream
            ):
                return
            self.server_conn.process_next_frame()


def make_raw_peer() raises -> RawPeer:
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return RawPeer(channel=channel^, server_conn=server_conn^)


def test_client_reads_trailers_only_block_as_trailing() raises:
    var peer = make_raw_peer()
    var sid = peer.channel.start_call("/probe.Probe/Fail", Metadata())
    var request = EchoRequest()
    peer.channel.send_request_bytes(sid, Span(encode(request)), last=True)
    peer.pump_until_request_end(sid)
    var block = [
        HeaderField(name=String(":status"), value=String("200")),
        HeaderField(
            name=String("content-type"), value=String("application/grpc")
        ),
        HeaderField(name=String("grpc-status"), value=String("5")),
        HeaderField(name=String("grpc-message"), value=String("not-found")),
        HeaderField(name=String("x-error-reason"), value=String("quota")),
    ]
    peer.server_conn.send_headers(sid, Span(block), end_stream=True)
    var result = peer.channel.finish(sid)

    assert_equal(result.status.code, StatusCode.NOT_FOUND)
    assert_equal(result.status.message, "not-found")
    assert_equal(len(result.initial_metadata), 0)
    require_absent(
        result.initial_metadata.get("x-error-reason"),
        "initial x-error-reason",
    )
    require_value(
        result.trailing_metadata.get("x-error-reason"),
        "quota",
        "trailing x-error-reason",
    )
    assert_equal(len(result.trailing_metadata), 1)
    assert_true(
        not peer.channel.conn.streams[sid].trailers_done,
        "one HEADERS block stays on the headers slot",
    )
    peer.channel.close()
    peer.server_conn.close()


def deny_with_metadata(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    _ = req
    ctx.response_metadata.add(String("x-request-id"), String("abc123"))
    ctx.response_trailers.add(String("x-trailer"), String("t"))
    ctx.abort(StatusCode.PERMISSION_DENIED, String("denied"))
    return EchoResponse()


def deny_without_metadata(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    _ = req
    ctx.abort(StatusCode.NOT_FOUND, String("absent"))
    return EchoResponse()


def ok_with_metadata(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    _ = req
    ctx.response_metadata.add(String("x-request-id"), String("abc123"))
    ctx.response_trailers.add(String("x-trailer"), String("t"))
    var response = EchoResponse()
    response.message = "pong"
    return response^


@fieldwise_init
struct BlockingRig(Movable):
    var server: Server
    var channel: GrpcChannel
    var server_conn: Http2Connection[GrpcTransport]
    var handled: List[UInt32]

    def dispatch_one(mut self) raises:
        while True:
            self.server_conn.process_next_frame()
            if self.server.dispatch_ready(self.server_conn, self.handled) > 0:
                return


def make_blocking_rig() raises -> BlockingRig:
    var server = Server("127.0.0.1", 0)
    server.register_unary[deny_with_metadata]("/probe.Probe/Deny")
    server.register_unary[deny_without_metadata]("/probe.Probe/Bare")
    server.register_unary[ok_with_metadata]("/probe.Probe/Ok")
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return BlockingRig(
        server=server^,
        channel=channel^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def test_server_abort_splits_response_metadata() raises:
    var rig = make_blocking_rig()

    var sid = rig.channel.start_call("/probe.Probe/Deny", Metadata())
    var request = EchoRequest()
    rig.channel.send_request_bytes(sid, Span(encode(request)), last=True)
    rig.dispatch_one()
    var result = rig.channel.finish(sid)

    assert_equal(result.status.code, StatusCode.PERMISSION_DENIED)
    assert_equal(result.status.message, "denied")
    require_value(
        result.initial_metadata.get("x-request-id"),
        "abc123",
        "initial x-request-id",
    )
    require_absent(
        result.initial_metadata.get("x-trailer"), "initial x-trailer"
    )
    require_value(
        result.trailing_metadata.get("x-trailer"), "t", "trailing x-trailer"
    )
    require_absent(
        result.trailing_metadata.get("x-request-id"),
        "trailing x-request-id",
    )
    assert_equal(len(result.response), 0)
    assert_true(
        rig.channel.conn.streams[sid].trailers_done, "status is trailers"
    )
    require_value(
        header_value(
            Span(rig.channel.conn.streams[sid].headers), "x-request-id"
        ),
        "abc123",
        "wire initial x-request-id",
    )
    require_absent(
        header_value(
            Span(rig.channel.conn.streams[sid].headers), "grpc-status"
        ),
        "wire initial grpc-status",
    )
    require_value(
        header_value(
            Span(rig.channel.conn.streams[sid].trailers), "grpc-status"
        ),
        "7",
        "wire trailer grpc-status",
    )
    require_value(
        header_value(Span(rig.channel.conn.streams[sid].trailers), "x-trailer"),
        "t",
        "wire trailer x-trailer",
    )
    require_absent(
        header_value(
            Span(rig.channel.conn.streams[sid].trailers), "x-request-id"
        ),
        "wire trailer x-request-id",
    )

    var bare = rig.channel.start_call("/probe.Probe/Bare", Metadata())
    rig.channel.send_request_bytes(bare, Span(encode(request)), last=True)
    rig.dispatch_one()
    var bare_result = rig.channel.finish(bare)
    assert_equal(bare_result.status.code, StatusCode.NOT_FOUND)
    assert_equal(bare_result.status.message, "absent")
    assert_equal(len(bare_result.initial_metadata), 0)
    assert_equal(len(bare_result.trailing_metadata), 0)
    assert_true(
        not rig.channel.conn.streams[bare].trailers_done,
        "empty metadata stays Trailers-Only",
    )
    require_value(
        header_value(
            Span(rig.channel.conn.streams[bare].headers), "grpc-status"
        ),
        "5",
        "bare wire grpc-status",
    )

    var ok_sid = rig.channel.start_call("/probe.Probe/Ok", Metadata())
    rig.channel.send_request_bytes(ok_sid, Span(encode(request)), last=True)
    rig.dispatch_one()
    rig.channel.conn.wait_headers(ok_sid)
    var msg = rig.channel.recv_response_bytes(ok_sid)
    assert_true(Bool(msg), "ok response present")
    var decoded = decode[EchoResponse](Span(msg.value()))
    assert_equal(decoded.message, "pong")
    var ok_result = rig.channel.finish(ok_sid)
    assert_true(ok_result.status.is_ok(), "ok status")
    require_value(
        ok_result.initial_metadata.get("x-request-id"),
        "abc123",
        "ok initial x-request-id",
    )
    require_value(
        ok_result.trailing_metadata.get("x-trailer"),
        "t",
        "ok trailing x-trailer",
    )

    rig.channel.close()
    rig.server_conn.close()


def bind_port() raises -> UInt16:
    var probe = TCPListener("127.0.0.1", 0)
    var port = probe.local_port
    probe.close()
    return port


def wait_channel(port: UInt16) raises -> GrpcChannel:
    for _ in range(200):
        try:
            return GrpcChannel.connect("127.0.0.1", port)
        except:
            sleep(0.01)
    raise Error("PollingServer did not accept")


def stop_child(pid: c_int):
    _ = external_call["kill", c_int](pid, c_int(9))
    var status = c_int(0)
    _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))


def test_polling_abort_splits_response_metadata() raises:
    var port = bind_port()
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer("127.0.0.1", port)
            server.register_unary[deny_with_metadata]("/probe.Probe/Deny")
            server.register_unary[deny_without_metadata]("/probe.Probe/Bare")
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(1))

    try:
        var channel = wait_channel(port)
        var request = EchoRequest()
        var result = channel.unary_bytes(
            "/probe.Probe/Deny",
            Span(encode(request)),
            Metadata(),
            timeout_ns=5_000_000_000,
        )
        assert_equal(result.status.code, StatusCode.PERMISSION_DENIED)
        assert_equal(result.status.message, "denied")
        require_value(
            result.initial_metadata.get("x-request-id"),
            "abc123",
            "polling initial x-request-id",
        )
        require_absent(
            result.initial_metadata.get("x-trailer"),
            "polling initial x-trailer",
        )
        require_value(
            result.trailing_metadata.get("x-trailer"),
            "t",
            "polling trailing x-trailer",
        )
        require_absent(
            result.trailing_metadata.get("x-request-id"),
            "polling trailing x-request-id",
        )
        assert_equal(len(result.response), 0)

        var bare = channel.unary_bytes(
            "/probe.Probe/Bare",
            Span(encode(request)),
            Metadata(),
            timeout_ns=5_000_000_000,
        )
        assert_equal(bare.status.code, StatusCode.NOT_FOUND)
        assert_equal(bare.status.message, "absent")
        assert_equal(len(bare.initial_metadata), 0)
        assert_equal(len(bare.trailing_metadata), 0)
        channel.close()
    except e:
        stop_child(pid)
        raise e

    stop_child(pid)


def main() raises:
    test_client_reads_trailers_only_block_as_trailing()
    test_server_abort_splits_response_metadata()
    test_polling_abort_splits_response_metadata()
    print("test_trailers_only_metadata: all tests passed")
