# Official gRPC interop test cases, client side, implemented on grpc-mojo.
# Usage:
#   interop_client <port> <case_name> [tls <ca_file> <server_name>]
#   interop_client unix <path> <case_name>
# Prints "CASE-OK <name>" on success; raises (non-zero exit) on failure.
#
# Case semantics follow grpc/grpc doc/interop-test-descriptions.md.

from std.sys import argv

from empty_pb import Empty
from messages_pb import (
    EchoStatus,
    Payload,
    ResponseParameters,
    SimpleRequest,
    SimpleResponse,
    StreamingInputCallRequest,
    StreamingInputCallResponse,
    StreamingOutputCallRequest,
    StreamingOutputCallResponse,
)
from grpc import GrpcChannel, Metadata, StatusCode
from proto import decode, encode

comptime UNARY_PATH = "/grpc.testing.TestService/UnaryCall"
comptime EMPTY_PATH = "/grpc.testing.TestService/EmptyCall"
comptime SIN_PATH = "/grpc.testing.TestService/StreamingInputCall"
comptime SOUT_PATH = "/grpc.testing.TestService/StreamingOutputCall"
comptime DUPLEX_PATH = "/grpc.testing.TestService/FullDuplexCall"


def zeros(n: Int) -> List[Byte]:
    return List[Byte](length=n, fill=0)


def expect(cond: Bool, msg: StaticString) raises:
    if not cond:
        raise Error("interop failure: " + String(msg))


def simple_request(response_size: Int, payload_size: Int) -> SimpleRequest:
    var req = SimpleRequest()
    req.response_size = Int32(response_size)
    var p = Payload()
    p.body = zeros(payload_size)
    req.payload = p^
    return req^


def duplex_request(size: Int, payload_size: Int) -> StreamingOutputCallRequest:
    var req = StreamingOutputCallRequest()
    var param = ResponseParameters()
    param.size = Int32(size)
    req.response_parameters.append(param^)
    var p = Payload()
    p.body = zeros(payload_size)
    req.payload = p^
    return req^


def case_empty_unary(mut channel: GrpcChannel) raises:
    var resp = channel.unary[Empty, Empty](EMPTY_PATH, Empty())
    _ = resp^


def case_large_unary(mut channel: GrpcChannel) raises:
    var resp = channel.unary[SimpleRequest, SimpleResponse](
        UNARY_PATH, simple_request(314159, 271828)
    )
    expect(Bool(resp.payload), "payload present")
    expect(len(resp.payload.value().body) == 314159, "payload is 314159 bytes")


def case_client_streaming(mut channel: GrpcChannel) raises:
    var sid = channel.start_call(SIN_PATH, Metadata())
    for size in [27182, 8, 1828, 45904]:
        var req = StreamingInputCallRequest()
        var p = Payload()
        p.body = zeros(size)
        req.payload = p^
        channel.send_msg[StreamingInputCallRequest](sid, req)
    channel.close_send(sid)
    var msg = channel.recv_msg[StreamingInputCallResponse](sid)
    expect(Bool(msg), "response present")
    expect(Int(msg.value().aggregated_payload_size) == 74922, "sum == 74922")
    var result = channel.finish(sid)
    expect(result.status.is_ok(), "OK status")


def case_server_streaming(mut channel: GrpcChannel) raises:
    var req = StreamingOutputCallRequest()
    for size in [31415, 9, 2653, 58979]:
        var param = ResponseParameters()
        param.size = Int32(size)
        req.response_parameters.append(param^)
    var sid = channel.start_call(SOUT_PATH, Metadata())
    channel.send_msg[StreamingOutputCallRequest](sid, req, last=True)
    var sizes = [31415, 9, 2653, 58979]
    var i = 0
    while True:
        var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
        if not msg:
            break
        expect(i < 4, "no extra responses")
        expect(Bool(msg.value().payload), "payload present")
        expect(
            len(msg.value().payload.value().body) == sizes[i],
            "streamed payload size matches",
        )
        i += 1
    expect(i == 4, "got 4 responses")
    var result = channel.finish(sid)
    expect(result.status.is_ok(), "OK status")


def case_ping_pong(mut channel: GrpcChannel) raises:
    var sid = channel.start_call(DUPLEX_PATH, Metadata())
    var response_sizes = [31415, 9, 2653, 58979]
    var payload_sizes = [27182, 8, 1828, 45904]
    for i in range(4):
        channel.send_msg[StreamingOutputCallRequest](
            sid, duplex_request(response_sizes[i], payload_sizes[i])
        )
        var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
        expect(Bool(msg), "interleaved response arrives")
        expect(
            len(msg.value().payload.value().body) == response_sizes[i],
            "interleaved size matches",
        )
    channel.close_send(sid)
    var tail = channel.recv_msg[StreamingOutputCallResponse](sid)
    expect(not Bool(tail), "stream ends after close")
    var result = channel.finish(sid)
    expect(result.status.is_ok(), "OK status")


def case_empty_stream(mut channel: GrpcChannel) raises:
    var sid = channel.start_call(DUPLEX_PATH, Metadata())
    channel.close_send(sid)
    var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
    expect(not Bool(msg), "no responses")
    var result = channel.finish(sid)
    expect(result.status.is_ok(), "OK status")


def case_custom_metadata(mut channel: GrpcChannel) raises:
    var md = Metadata()
    md.add(
        String("x-grpc-test-echo-initial"),
        String("test_initial_metadata_value"),
    )
    var bin_value: List[Byte] = [0xAB, 0xAB, 0xAB]
    md.add_binary(String("x-grpc-test-echo-trailing-bin"), Span(bin_value))

    # Unary leg.
    var result = channel.unary_bytes(
        UNARY_PATH, Span(encode(simple_request(314159, 271828))), md
    )
    expect(result.status.is_ok(), "unary OK")
    var initial = result.initial_metadata.get("x-grpc-test-echo-initial")
    expect(Bool(initial), "initial metadata echoed")
    expect(initial.value() == "test_initial_metadata_value", "initial value")
    var trailing = result.trailing_metadata.get_binary(
        "x-grpc-test-echo-trailing-bin"
    )
    expect(Bool(trailing), "trailing-bin echoed")
    expect(len(trailing.value()) == 3, "trailing-bin length")
    expect(Int(trailing.value()[0]) == 0xAB, "trailing-bin content")

    # Full-duplex leg.
    var sid = channel.start_call(DUPLEX_PATH, md)
    channel.send_msg[StreamingOutputCallRequest](
        sid, duplex_request(314159, 271828), last=True
    )
    var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
    expect(Bool(msg), "duplex response")
    var dresult = channel.finish(sid)
    expect(dresult.status.is_ok(), "duplex OK")
    var dinitial = dresult.initial_metadata.get("x-grpc-test-echo-initial")
    expect(Bool(dinitial), "duplex initial metadata echoed")
    var dtrailing = dresult.trailing_metadata.get_binary(
        "x-grpc-test-echo-trailing-bin"
    )
    expect(Bool(dtrailing), "duplex trailing-bin echoed")


def _status_request(code: Int, var message: String) -> SimpleRequest:
    var req = SimpleRequest()
    var st = EchoStatus()
    st.code = Int32(code)
    st.message = message^
    req.response_status = st^
    return req^


def case_status_code_and_message(mut channel: GrpcChannel) raises:
    comptime MSG = "test status message"
    var result = channel.unary_bytes(
        UNARY_PATH, Span(encode(_status_request(2, String(MSG)))), Metadata()
    )
    expect(result.status.code == StatusCode.UNKNOWN, "unary code 2")
    expect(result.status.message == String(MSG), "unary message matches")

    var sid = channel.start_call(DUPLEX_PATH, Metadata())
    var dreq = StreamingOutputCallRequest()
    var st = EchoStatus()
    st.code = 2
    st.message = String(MSG)
    dreq.response_status = st^
    channel.send_msg[StreamingOutputCallRequest](sid, dreq, last=True)
    var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
    expect(not Bool(msg), "no response before error")
    var dresult = channel.finish(sid)
    expect(dresult.status.code == StatusCode.UNKNOWN, "duplex code 2")
    expect(dresult.status.message == String(MSG), "duplex message matches")


def case_special_status_message(mut channel: GrpcChannel) raises:
    var msg = String(
        "\t\ntest with whitespace\r\nand Unicode BMP ☺ and non-BMP 😈\t\n"
    )
    var result = channel.unary_bytes(
        UNARY_PATH, Span(encode(_status_request(2, msg.copy()))), Metadata()
    )
    expect(result.status.code == StatusCode.UNKNOWN, "code 2")
    expect(result.status.message == msg, "special message roundtrips")


def case_unimplemented_method(mut channel: GrpcChannel) raises:
    var result = channel.unary_bytes(
        "/grpc.testing.TestService/UnimplementedCall",
        Span(encode(Empty())),
        Metadata(),
    )
    expect(result.status.code == StatusCode.UNIMPLEMENTED, "UNIMPLEMENTED")


def case_timeout_on_sleeping_server(mut channel: GrpcChannel) raises:
    var sid = channel.start_call(
        DUPLEX_PATH, Metadata(), timeout_ns=100_000_000
    )
    var req = duplex_request(31415, 27182)
    req.response_parameters[0].interval_us = 1_000_000  # server sleeps 1s
    channel.send_msg[StreamingOutputCallRequest](sid, req)
    # DEADLINE_EXCEEDED may be enforced by our client (socket timeout ->
    # raised error) or by the server first (trailers with status 4);
    # both satisfy the case.
    var deadline_hit = False
    var detail = String("recv returned a message")
    try:
        var msg = channel.recv_msg[StreamingOutputCallResponse](sid)
        if not msg:
            var r = channel.finish(sid)
            deadline_hit = r.status.code == StatusCode.DEADLINE_EXCEEDED
            detail = String("stream ended, status ") + String(r.status)
    except e:
        deadline_hit = String(e).find("DEADLINE_EXCEEDED") >= 0
        detail = String(e)
    if not deadline_hit:
        raise Error("interop failure: expected deadline; " + detail)


def case_cancel_after_begin(mut channel: GrpcChannel) raises:
    var sid = channel.start_call(SIN_PATH, Metadata())
    channel.cancel(sid)
    # The channel must remain usable for subsequent calls.
    var resp = channel.unary[Empty, Empty](EMPTY_PATH, Empty())
    _ = resp^


def main() raises:
    var args = argv()
    var channel: GrpcChannel
    var case_name: String
    if args[1] == "unix":
        case_name = args[3]
        channel = GrpcChannel.connect_unix(args[2])
    elif len(args) >= 6 and args[3] == "tls":
        var port = UInt16(Int(args[1]))
        case_name = args[2]
        channel = GrpcChannel.connect_tls(
            "127.0.0.1",
            port,
            ca_file=args[4],
            server_name=args[5],
        )
    else:
        var port = UInt16(Int(args[1]))
        case_name = args[2]
        channel = GrpcChannel.connect("127.0.0.1", port)

    if case_name == "empty_unary":
        case_empty_unary(channel)
    elif case_name == "large_unary":
        case_large_unary(channel)
    elif case_name == "client_streaming":
        case_client_streaming(channel)
    elif case_name == "server_streaming":
        case_server_streaming(channel)
    elif case_name == "ping_pong":
        case_ping_pong(channel)
    elif case_name == "empty_stream":
        case_empty_stream(channel)
    elif case_name == "custom_metadata":
        case_custom_metadata(channel)
    elif case_name == "status_code_and_message":
        case_status_code_and_message(channel)
    elif case_name == "special_status_message":
        case_special_status_message(channel)
    elif case_name == "unimplemented_method":
        case_unimplemented_method(channel)
    elif case_name == "timeout_on_sleeping_server":
        case_timeout_on_sleeping_server(channel)
    elif case_name == "cancel_after_begin":
        case_cancel_after_begin(channel)
    else:
        raise Error("unknown case: " + String(case_name))
    print("CASE-OK ", case_name, sep="")
    channel.close()
