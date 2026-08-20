# Streaming e2e: server-streaming, client-streaming, and bidi over a real
# loopback connection, driven single-threaded (client writes buffer in the
# kernel; the server handler then runs to completion; client reads after).

from std.testing import assert_equal, assert_true

from h2 import Http2Connection
from grpc import (
    GrpcChannel,
    GrpcTransport,
    Metadata,
    Server,
    ServerCall,
    ServerContext,
    StatusCode,
)
from net import TCPStream, TCPListener
from proto import decode, encode
from echo_messages import EchoRequest, EchoResponse


def split_handler(
    req: EchoRequest, mut ctx: ServerContext, mut call: ServerCall
) raises:
    for word in req.message.split(" "):
        if word.byte_length() == 0:
            continue
        call.send[EchoResponse](ctx, EchoResponse(message=String(word)))


def join_handler(
    mut ctx: ServerContext, mut call: ServerCall
) raises -> EchoResponse:
    var parts = String()
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        if parts.byte_length() > 0:
            parts += "+"
        parts += msg.value().message
    return EchoResponse(message=parts^)


def chat_handler(mut ctx: ServerContext, mut call: ServerCall) raises:
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        call.send[EchoResponse](
            ctx, EchoResponse(message=String("pong: ") + msg.value().message)
        )


def fail_stream_handler(
    req: EchoRequest, mut ctx: ServerContext, mut call: ServerCall
) raises:
    call.send[EchoResponse](ctx, EchoResponse(message="one"))
    raise Error("stream exploded")


@fieldwise_init
struct TestRig(Movable):
    var channel: GrpcChannel
    var server: Server
    var server_conn: Http2Connection[GrpcTransport]
    var handled: List[UInt32]

    def pump_server_until_reply(mut self) raises:
        while True:
            self.server_conn.process_next_frame()
            if self.server.dispatch_ready(self.server_conn, self.handled) > 0:
                return


def make_rig() raises -> TestRig:
    var server = Server("127.0.0.1", 0)
    server.register_server_streaming[split_handler]("/echo.Echo/Split")
    server.register_client_streaming[join_handler]("/echo.Echo/Join")
    server.register_bidi[chat_handler]("/echo.Echo/Chat")
    server.register_server_streaming[fail_stream_handler](
        "/echo.Echo/FailStream"
    )

    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return TestRig(
        channel=channel^,
        server=server^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def test_server_streaming() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Split", Metadata())
    rig.channel.send_msg[EchoRequest](
        sid, EchoRequest(message="alpha beta gamma"), last=True
    )
    rig.pump_server_until_reply()

    var words = List[String]()
    while True:
        var msg = rig.channel.recv_msg[EchoResponse](sid)
        if not msg:
            break
        words.append(msg.value().message.copy())
    assert_equal(len(words), 3)
    assert_equal(words[0], "alpha")
    assert_equal(words[1], "beta")
    assert_equal(words[2], "gamma")
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), "server-streaming OK")
    rig.channel.close()
    rig.server_conn.close()


def test_client_streaming() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Join", Metadata())
    for word in ["a", "bb", "ccc"]:
        rig.channel.send_msg[EchoRequest](
            sid, EchoRequest(message=String(word))
        )
    rig.channel.close_send(sid)
    rig.pump_server_until_reply()

    var msg = rig.channel.recv_msg[EchoResponse](sid)
    assert_true(Bool(msg), "client-streaming response present")
    assert_equal(msg.value().message, "a+bb+ccc")
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), "client-streaming OK")
    rig.channel.close()
    rig.server_conn.close()


def test_bidi_sequenced() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/Chat", Metadata())
    # Sequenced pattern: send everything, close, then read the echoes.
    for word in ["x", "y"]:
        rig.channel.send_msg[EchoRequest](
            sid, EchoRequest(message=String(word))
        )
    rig.channel.close_send(sid)
    rig.pump_server_until_reply()

    var got = List[String]()
    while True:
        var msg = rig.channel.recv_msg[EchoResponse](sid)
        if not msg:
            break
        got.append(msg.value().message.copy())
    assert_equal(len(got), 2)
    assert_equal(got[0], "pong: x")
    assert_equal(got[1], "pong: y")
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), "bidi OK")
    rig.channel.close()
    rig.server_conn.close()


def test_streaming_handler_error() raises:
    var rig = make_rig()
    var sid = rig.channel.start_call("/echo.Echo/FailStream", Metadata())
    rig.channel.send_msg[EchoRequest](sid, EchoRequest(message="x"), last=True)
    rig.pump_server_until_reply()

    # One message arrives, then the stream ends with UNKNOWN trailers.
    var first = rig.channel.recv_msg[EchoResponse](sid)
    assert_true(Bool(first), "message before failure delivered")
    assert_equal(first.value().message, "one")
    var second = rig.channel.recv_msg[EchoResponse](sid)
    assert_true(not Bool(second), "stream ended after failure")
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.UNKNOWN)
    assert_true(
        result.status.message.find("stream exploded") >= 0,
        "error message propagated",
    )
    rig.channel.close()
    rig.server_conn.close()


def main() raises:
    test_server_streaming()
    test_client_streaming()
    test_bidi_sequenced()
    test_streaming_handler_error()
    print("test_grpc_streaming: all tests passed")
