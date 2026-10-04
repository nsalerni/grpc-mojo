# Call-state invariants on one GrpcChannel: every blocking step of a call,
# sends included, runs under that call's own deadline rather than a socket
# timeout left behind by another call, and a call that fails on a stream the
# peer still has open is reset so the next call on the channel can proceed.

from std.testing import assert_equal, assert_true
from std.time import monotonic

from echo_messages import EchoRequest, EchoResponse
from grpc import (
    ClientStreamingCall,
    GrpcChannel,
    GrpcTransport,
    Metadata,
    ServerStreamingCall,
    frame_message,
)
from hpack import HeaderField
from h2 import ERR_CANCEL, Http2Connection
from net import TCPListener
from proto import encode


@fieldwise_init
struct Rig(Movable):
    """A GrpcChannel client against a hand-driven server-side connection."""

    var channel: GrpcChannel
    var server_conn: Http2Connection[GrpcTransport]

    def pump_until_headers(mut self, sid: UInt32) raises:
        while True:
            self.server_conn.process_next_frame()
            if sid in self.server_conn.streams:
                if self.server_conn.streams[sid].headers_done:
                    return

    def pump_until_reset(mut self, sid: UInt32) raises -> UInt32:
        while True:
            var code = self.server_conn.streams[sid].reset_code
            if code:
                return code.value()
            self.server_conn.process_next_frame()

    def respond(
        mut self, sid: UInt32, payload: Span[Byte, _], *, end: Bool
    ) raises:
        var hdrs = [
            HeaderField(name=String(":status"), value=String("200")),
            HeaderField(
                name=String("content-type"), value=String("application/grpc")
            ),
        ]
        self.server_conn.send_headers(sid, Span(hdrs), end_stream=False)
        self.server_conn.send_data(sid, payload, end_stream=False)
        if end:
            var trailers = [
                HeaderField(name=String("grpc-status"), value=String("0"))
            ]
            self.server_conn.send_headers(
                sid, Span(trailers), end_stream=True
            )


def make_rig(*, max_concurrent_streams: Int = 256) raises -> Rig:
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(
        transport^,
        is_client=False,
        max_concurrent_streams=max_concurrent_streams,
    )
    listener.close()
    return Rig(channel=channel^, server_conn=server_conn^)


def test_blocked_send_uses_its_own_deadline() raises:
    var rig = make_rig()
    var first = ServerStreamingCall[EchoResponse].start[EchoRequest](
        rig.channel,
        "/echo.Echo/Split",
        EchoRequest(message="a"),
        timeout_ns=100_000_000,
    )
    rig.pump_until_headers(first.sid)
    var reply = frame_message(Span(encode(EchoResponse(message="a"))))
    rig.respond(first.sid, Span(reply), end=False)
    assert_true(Bool(first.recv()), "first call delivered a message")

    # The peer never grants flow-control credit, so the send blocks reading
    # for WINDOW_UPDATE. It must fail on its own 1s budget with
    # DEADLINE_EXCEEDED, not on the first call's 100ms socket timeout.
    var second = ClientStreamingCall[EchoRequest, EchoResponse].start(
        rig.channel, "/echo.Echo/Join", timeout_ns=1_000_000_000
    )
    var big = EchoRequest(message=String("x") * 200_000)
    var started = Int64(monotonic())
    var err = String("<no error>")
    try:
        second.send(big)
    except e:
        err = String(e)
    var elapsed = Int64(monotonic()) - started
    assert_true("DEADLINE_EXCEEDED" in err, err)
    assert_true(
        elapsed >= 800_000_000,
        "blocked send failed after "
        + String(elapsed // 1_000_000)
        + "ms, before its own 1s deadline",
    )
    assert_equal(
        rig.channel.conn.streams[second.sid].reset_code.value(), ERR_CANCEL
    )
    rig.channel.close()
    rig.server_conn.close()


def check_next_call_after_failed_recv(
    bad_payload: List[Byte], expected: StringSpan
) raises:
    # The server allows one stream at a time, so a failed call that is left
    # half-open blocks every later call on the channel.
    var rig = make_rig(max_concurrent_streams=1)
    var sid = rig.channel.start_call(
        "/echo.Echo/Split", Metadata(), timeout_ns=5_000_000_000
    )
    rig.channel.send_msg[EchoRequest](sid, EchoRequest(message="a"), last=True)
    rig.pump_until_headers(sid)
    rig.respond(sid, Span(bad_payload), end=False)
    var err = String("<no error>")
    try:
        _ = rig.channel.recv_msg[EchoResponse](sid)
    except e:
        err = String(e)
    assert_true(String(expected) in err, err)
    assert_equal(rig.channel.deadline_ns, 0)
    assert_equal(rig.pump_until_reset(sid), ERR_CANCEL)

    var next = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.send_msg[EchoRequest](next, EchoRequest(message="b"), last=True)
    rig.pump_until_headers(next)
    var reply = frame_message(Span(encode(EchoResponse(message="b"))))
    rig.respond(next, Span(reply), end=True)
    var msg = rig.channel.recv_msg[EchoResponse](next)
    assert_true(Bool(msg), "next call on the channel must complete")
    assert_equal(msg.value().message, "b")
    var result = rig.channel.finish(next)
    assert_true(result.status.is_ok(), result.status.message)
    rig.channel.close()
    rig.server_conn.close()


def test_failed_recv_frees_the_stream() raises:
    var bad_flag: List[Byte] = [2, 0, 0, 0, 0]
    check_next_call_after_failed_recv(bad_flag, "invalid compressed flag")

    var not_gzip = frame_message("abc".as_bytes(), compressed=True)
    check_next_call_after_failed_recv(not_gzip, "gzip decompress failed")

    # Field 1 declares 5 bytes but carries 1: the message does not decode.
    var truncated: List[Byte] = [0x0A, 0x05, 0x41]
    check_next_call_after_failed_recv(
        frame_message(Span(truncated)), "proto"
    )


def test_finish_closes_our_side() raises:
    # The server ends the call while the request stream is still open.
    var rig = make_rig(max_concurrent_streams=1)
    var sid = rig.channel.start_call("/echo.Echo/Chat", Metadata())
    rig.channel.send_msg[EchoRequest](sid, EchoRequest(message="a"))
    rig.pump_until_headers(sid)
    var reply = frame_message(Span(encode(EchoResponse(message="a"))))
    rig.respond(sid, Span(reply), end=True)
    assert_true(Bool(rig.channel.recv_msg[EchoResponse](sid)))
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok(), result.status.message)
    assert_true(rig.channel.conn.streams[sid].local_end, "finish half-closes")

    var next = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.send_msg[EchoRequest](next, EchoRequest(message="b"), last=True)
    rig.pump_until_headers(next)
    rig.respond(next, Span(reply), end=True)
    assert_true(Bool(rig.channel.recv_msg[EchoResponse](next)))
    result = rig.channel.finish(next)
    assert_true(result.status.is_ok(), result.status.message)
    rig.channel.close()
    rig.server_conn.close()


def main() raises:
    test_blocked_send_uses_its_own_deadline()
    test_failed_recv_frees_the_stream()
    test_finish_closes_our_side()
    print("test_grpc_call_state: all tests passed")
