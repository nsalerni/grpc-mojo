# Edge-case and error-path tests for the grpc package: framing errors,
# grpc-timeout coding boundaries, metadata semantics, status mapping, and
# protocol-abuse handling (malformed timeout headers, wrong content-type),
# plus the public unary API driven against a forked in-process server.

from std.ffi import c_int, external_call
from std.testing import assert_equal, assert_false, assert_true

from common import from_hex, to_hex
from echo_messages import EchoRequest, EchoResponse
from grpc import (
    DEFAULT_MAX_RECV_MESSAGE_SIZE,
    GRPC_MOJO_USER_AGENT,
    GrpcChannel,
    GrpcTransport,
    Metadata,
    Server,
    ServerCall,
    ServerContext,
    Status,
    StatusCode,
    decode_bin_value,
    decode_timeout,
    encode_bin_value,
    encode_timeout,
    frame_message,
    http_status_to_grpc,
    is_binary_key,
    is_valid_metadata_key,
    percent_decode_message,
    percent_encode_message,
    rst_code_to_grpc,
    status_code_name,
)
from hpack import HeaderField
from h2 import Http2Connection
from net import TCPListener, TCPStream
from proto import decode, encode


def expect_error[F: def() raises](f: F, why: StringSpan) raises -> String:
    try:
        f()
    except e:
        return String(e)
    raise Error("expected a raise: " + String(why))


# --- grpc-timeout coding boundaries ---


def test_timeout_encode_units() raises:
    # Unit selection degrades precision at each 8-digit boundary.
    assert_equal(encode_timeout(99_999_999), "99999999n")
    assert_equal(encode_timeout(100_000_000), "100000u")
    assert_equal(encode_timeout(99_999_999_999), "99999999u")
    assert_equal(encode_timeout(100_000_000_000), "100000m")
    assert_equal(encode_timeout(99_999_999_000_000), "99999999m")
    assert_equal(encode_timeout(100_000_000_000_000), "100000S")
    assert_equal(encode_timeout(0), "0n")

    def negative() raises:
        _ = encode_timeout(-1)

    assert_true("negative timeout" in expect_error(negative, "negative"))

    # Int64.MAX nanoseconds is ~2.56 million hours: still 8 digits, so
    # every representable duration encodes (the "too large" guard is
    # unreachable within Int64 by construction).
    var huge = encode_timeout(Int64.MAX)
    assert_true(huge.endswith("H"), huge)
    assert_equal(decode_timeout(huge), 2_562_047 * 3_600_000_000_000)


def test_timeout_decode_edges() raises:
    assert_equal(decode_timeout("5u"), 5_000)
    assert_equal(decode_timeout("0n"), 0)
    assert_equal(decode_timeout("2562047H"), 2_562_047 * 3_600_000_000_000)

    def unknown_unit() raises:
        _ = decode_timeout("100x")

    def lowercase_s() raises:
        _ = decode_timeout("100s")  # seconds is upper-case S

    def empty() raises:
        _ = decode_timeout("")

    def unit_only() raises:
        _ = decode_timeout("S")

    def non_digit() raises:
        _ = decode_timeout("12a4S")

    def negative_digits() raises:
        _ = decode_timeout("-10n")

    assert_true("unknown" in expect_error(unknown_unit, "unknown unit"))
    assert_true("unknown" in expect_error(lowercase_s, "lowercase s"))
    assert_true("malformed" in expect_error(empty, "empty"))
    assert_true("malformed" in expect_error(unit_only, "unit only"))
    _ = expect_error(non_digit, "non-digit body")
    assert_true("malformed" in expect_error(negative_digits, "negative"))


# --- metadata semantics ---


def test_metadata_key_validation() raises:
    assert_false(is_valid_metadata_key(""))
    assert_false(is_valid_metadata_key("a b"))
    assert_false(is_valid_metadata_key("a:b"))
    assert_false(is_valid_metadata_key("a/b"))
    assert_false(is_valid_metadata_key("UPPER"))
    assert_true(is_valid_metadata_key("abc-123_x.y"))
    assert_true(is_binary_key("x-bin"))
    assert_false(is_binary_key("x-binary"))
    assert_true(is_binary_key("-bin"))


def test_metadata_container_semantics() raises:
    var md = Metadata()
    md.add(String("k"), String("v1"))
    md.add(String("k"), String("v2"))
    md.add(String("other"), String("x"))
    assert_equal(len(md), 3, "duplicates count separately")
    var all_k = md.get_all("k")
    assert_equal(len(all_k), 2)
    assert_equal(all_k[0], "v1")
    assert_equal(all_k[1], "v2")
    assert_equal(md.get("k").value(), "v1", "get returns the first")
    assert_true(not md.get("absent"), "absent key is None")
    assert_true(not md.get_binary("absent-bin"), "absent binary is None")
    var rendered = String(md)
    assert_true("k" in rendered and "v2" in rendered, rendered)
    assert_equal(String(Metadata()), "Metadata()")


def test_binary_metadata_edges() raises:
    # Padding-boundary round trips: lengths 0..5 cover every mod-3 class.
    for n in range(6):
        var data = List[Byte]()
        for i in range(n):
            data.append(UInt8(0xF0 + i))
        var coded = encode_bin_value(Span(data))
        assert_true(not coded.endswith("="), "unpadded emit")
        var back = decode_bin_value(coded)
        assert_equal(to_hex(back), to_hex(data))

    def bad_b64() raises:
        _ = decode_bin_value("!!!!")

    _ = expect_error(bad_b64, "invalid base64 must raise")

    # Spec: a comma-joined -bin value decodes its first element only.
    var payload: List[Byte] = [0xDE, 0xAD, 0xBE, 0xEF]
    var one = encode_bin_value(Span(payload))
    var md = Metadata()
    md.entries.append(
        HeaderField(name=String("x-data-bin"), value=one + "," + one)
    )
    assert_equal(to_hex(md.get_binary("x-data-bin").value()), "deadbeef")

    def non_bin_key() raises:
        var m = Metadata()
        var data: List[Byte] = [1]
        m.add_binary(String("x-data"), Span(data))

    def invalid_bin_key() raises:
        var m = Metadata()
        var data: List[Byte] = [1]
        m.add_binary(String("X-Data-bin"), Span(data))

    assert_true("-bin" in expect_error(non_bin_key, "non -bin key"))
    _ = expect_error(invalid_bin_key, "invalid uppercase -bin key")

    # Empty binary values survive the round trip.
    var empty = List[Byte]()
    var md2 = Metadata()
    md2.add_binary(String("e-bin"), Span(empty))
    assert_equal(len(md2.get_binary("e-bin").value()), 0)


def test_from_headers_reserved() raises:
    var headers = [
        HeaderField(name=String(":status"), value=String("200")),
        HeaderField(name=String("grpc-status"), value=String("0")),
        HeaderField(
            name=String("content-type"), value=String("application/grpc")
        ),
        HeaderField(name=String("te"), value=String("trailers")),
        HeaderField(name=String("user-agent"), value=String("ua")),
        HeaderField(name=String("x-custom"), value=String("keep")),
    ]
    var md = Metadata.from_headers(Span(headers))
    assert_equal(len(md), 1, "only application metadata survives")
    assert_equal(md.get("x-custom").value(), "keep")
    var raw = Metadata.from_headers(Span(headers), skip_reserved=False)
    assert_equal(len(raw), 5, "pseudo-headers still drop")
    assert_equal(raw.get("grpc-status").value(), "0")


# --- status mapping and percent coding ---


def test_status_edges() raises:
    assert_equal(String(status_code_name(-1)), "UNKNOWN_CODE")
    assert_equal(String(status_code_name(17)), "UNKNOWN_CODE")
    assert_equal(String(Status.ok()), "OK (0)")
    var err = Status(code=5, message=String("x")).to_error()
    assert_true("NOT_FOUND (5): x" in String(err), String(err))

    assert_equal(http_status_to_grpc(400), StatusCode.INTERNAL)
    assert_equal(http_status_to_grpc(403), StatusCode.PERMISSION_DENIED)
    assert_equal(http_status_to_grpc(429), StatusCode.UNAVAILABLE)
    assert_equal(http_status_to_grpc(502), StatusCode.UNAVAILABLE)
    assert_equal(http_status_to_grpc(504), StatusCode.UNAVAILABLE)
    assert_equal(http_status_to_grpc(418), StatusCode.UNKNOWN)
    assert_equal(rst_code_to_grpc(0xB), StatusCode.RESOURCE_EXHAUSTED)
    assert_equal(rst_code_to_grpc(0xC), StatusCode.PERMISSION_DENIED)
    assert_equal(rst_code_to_grpc(0x0), StatusCode.INTERNAL)


def test_percent_boundaries() raises:
    # 0x20..0x7E pass through except %; everything else percent-codes.
    assert_equal(percent_encode_message(" "), " ")
    assert_equal(percent_encode_message("~"), "~")
    assert_equal(percent_encode_message(String(chr(0x1F))), "%1F")
    assert_equal(percent_encode_message(String(chr(0x7F))), "%7F")
    assert_equal(percent_encode_message(String(chr(0))), "%00")
    # Decoder accepts lower-case hex even though we emit upper-case.
    assert_equal(percent_decode_message("%c3%a9"), "é")
    # A '%' with only one following byte passes through leniently.
    assert_equal(percent_decode_message("ab%4"), "ab%4")


def test_frame_compressed_flag() raises:
    var payload: List[Byte] = [0xAA]
    var framed = frame_message(Span(payload), compressed=True)
    assert_equal(to_hex(framed), "0100000001aa")


# --- framing error paths over a real connection ---


@fieldwise_init
struct RawRig(Movable):
    """A GrpcChannel client against a hand-driven server-side connection."""

    var channel: GrpcChannel
    var server_conn: Http2Connection[GrpcTransport]

    def pump_until_headers(mut self, sid: UInt32) raises:
        while True:
            self.server_conn.process_next_frame()
            if sid in self.server_conn.streams:
                if self.server_conn.streams[sid].headers_done:
                    return

    def send_response_headers(mut self, sid: UInt32) raises:
        var hdrs = [
            HeaderField(name=String(":status"), value=String("200")),
            HeaderField(
                name=String("content-type"), value=String("application/grpc")
            ),
        ]
        self.server_conn.send_headers(sid, Span(hdrs), end_stream=False)


def make_raw_rig() raises -> RawRig:
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return RawRig(channel=channel^, server_conn=server_conn^)


def recv_framing_error(server_payload: List[Byte], end: Bool) raises -> String:
    """Starts a call, injects raw response DATA, returns the client error."""
    var rig = make_raw_rig()
    var sid = rig.channel.start_call("/x/Y", Metadata())
    rig.channel.send_request_bytes(sid, "r".as_bytes(), last=True)
    rig.pump_until_headers(sid)
    rig.send_response_headers(sid)
    rig.server_conn.send_data(sid, Span(server_payload), end_stream=end)
    rig.channel.conn.wait_headers(sid)
    var msg: String
    try:
        _ = rig.channel.recv_response_bytes(sid)
        msg = String("<no error>")
    except e:
        msg = String(e)
    rig.channel.close()
    rig.server_conn.close()
    return msg^


def test_recv_message_errors() raises:
    # Compressed-Flag byte outside {0, 1}.
    var bad_flag: List[Byte] = [2, 0, 0, 0, 0]
    assert_true("invalid compressed flag" in recv_framing_error(bad_flag, True))

    # Compressed-Flag set: no codecs are implemented.
    var compressed = frame_message("abc".as_bytes(), compressed=True)
    assert_true(
        "compressed messages not supported"
        in recv_framing_error(compressed, True)
    )

    # Declared length above the 4 MiB default cap (no body needed).
    var oversize: List[Byte] = [0, 0x00, 0x50, 0x00, 0x00]  # 5 MiB
    assert_true("exceeds max size" in recv_framing_error(oversize, False))

    # Stream ends mid-prefix.
    var short_prefix: List[Byte] = [0, 0, 0]
    assert_true(
        "truncated message prefix" in recv_framing_error(short_prefix, True)
    )

    # Prefix declares 10 bytes; stream ends after 4.
    var short_body: List[Byte] = [0, 0, 0, 0, 10, 1, 2, 3, 4]
    assert_true(
        "truncated message body" in recv_framing_error(short_body, True)
    )


# --- protocol abuse against the real Server dispatch ---


def raw_grpc_request_headers(
    path: StringSpan, content_type: StringSpan, timeout: StringSpan
) -> List[HeaderField]:
    var headers = List[HeaderField]()
    headers.append(HeaderField(name=String(":method"), value=String("POST")))
    headers.append(HeaderField(name=String(":scheme"), value=String("http")))
    headers.append(HeaderField(name=String(":path"), value=String(path)))
    headers.append(
        HeaderField(name=String(":authority"), value=String("test.local"))
    )
    headers.append(HeaderField(name=String("te"), value=String("trailers")))
    if content_type.byte_length() > 0:
        headers.append(
            HeaderField(name=String("content-type"), value=String(content_type))
        )
    if timeout.byte_length() > 0:
        headers.append(
            HeaderField(name=String("grpc-timeout"), value=String(timeout))
        )
    return headers^


def dummy_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    return EchoResponse(message=String("ok"))


def test_server_max_message_size() raises:
    def negative() raises:
        var server = Server("127.0.0.1", 0)
        server.set_max_message_size(-1)

    def too_wide() raises:
        var server = Server("127.0.0.1", 0)
        server.set_max_message_size(0x100000000)

    assert_true("non-negative" in expect_error(negative, "negative"))
    assert_true("prefix" in expect_error(too_wide, "prefix"))

    var server = Server("127.0.0.1", 0)
    server.set_max_message_size(4)
    server.register_unary[dummy_handler]("/echo.Echo/Say")
    var listener = TCPListener("127.0.0.1", 0)
    var client_tcp = TCPStream.connect("127.0.0.1", listener.local_port)
    var client = Http2Connection(client_tcp^, is_client=True)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    var handled = List[UInt32]()

    var sid = client.open_stream()
    var headers = raw_grpc_request_headers(
        "/echo.Echo/Say", "application/grpc", ""
    )
    client.send_headers(sid, Span(headers), end_stream=False)
    var framed = frame_message(Span(encode(EchoRequest(message="hello"))))
    client.send_data(sid, Span(framed), end_stream=True)
    while True:
        server_conn.process_next_frame()
        if server.dispatch_ready(server_conn, handled) > 0:
            break
    client.wait_stream_end(sid)
    var status = String()
    var message = String()
    for f in client.streams[sid].headers:
        if f.name == "grpc-status":
            status = f.value.copy()
        if f.name == "grpc-message":
            message = f.value.copy()
    for f in client.streams[sid].trailers:
        if f.name == "grpc-status":
            status = f.value.copy()
        if f.name == "grpc-message":
            message = f.value.copy()
    assert_equal(status, String(StatusCode.RESOURCE_EXHAUSTED), message)
    assert_true("exceeds max size" in message, message)
    client.close()
    server_conn.close()


def test_malformed_grpc_timeout_fails_call_only() raises:
    var server = Server("127.0.0.1", 0)
    server.register_unary[dummy_handler]("/echo.Echo/Say")
    var listener = TCPListener("127.0.0.1", 0)
    var client_tcp = TCPStream.connect("127.0.0.1", listener.local_port)
    var client = Http2Connection(client_tcp^, is_client=True)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    var handled = List[UInt32]()

    var sid = client.open_stream()
    var headers = raw_grpc_request_headers(
        "/echo.Echo/Say", "application/grpc", "100x"
    )
    client.send_headers(sid, Span(headers), end_stream=True)
    while True:
        server_conn.process_next_frame()
        if server.dispatch_ready(server_conn, handled) > 0:
            break
    client.wait_stream_end(sid)
    var status = String()
    for f in client.streams[sid].headers:
        if f.name == "grpc-status":
            status = f.value.copy()
    assert_equal(status, String(StatusCode.INTERNAL), "INTERNAL trailers-only")

    # The connection survives: a well-formed call on the same connection
    # succeeds afterwards.
    var sid2 = client.open_stream()
    var good = raw_grpc_request_headers(
        "/echo.Echo/Say", "application/grpc", "1S"
    )
    client.send_headers(sid2, Span(good), end_stream=False)
    var framed = frame_message(Span(encode(EchoRequest(message="hi"))))
    client.send_data(sid2, Span(framed), end_stream=True)
    while True:
        server_conn.process_next_frame()
        if server.dispatch_ready(server_conn, handled) > 0:
            break
    client.wait_stream_end(sid2)
    var status2 = String()
    for f in client.streams[sid2].trailers:
        if f.name == "grpc-status":
            status2 = f.value.copy()
    assert_equal(status2, "0", "next call on the connection is healthy")
    client.close()
    server_conn.close()


def test_content_type_gate_415() raises:
    var server = Server("127.0.0.1", 0)
    server.register_unary[dummy_handler]("/echo.Echo/Say")
    var listener = TCPListener("127.0.0.1", 0)
    var client_tcp = TCPStream.connect("127.0.0.1", listener.local_port)
    var client = Http2Connection(client_tcp^, is_client=True)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    var handled = List[UInt32]()

    # Wrong content-type and absent content-type both get :status 415.
    var cases = [String("text/plain"), String("")]
    for i in range(len(cases)):
        var sid = client.open_stream()
        var headers = raw_grpc_request_headers("/echo.Echo/Say", cases[i], "")
        client.send_headers(sid, Span(headers), end_stream=True)
        while True:
            server_conn.process_next_frame()
            if server.dispatch_ready(server_conn, handled) > 0:
                break
        client.wait_stream_end(sid)
        var status = String()
        for f in client.streams[sid].headers:
            if f.name == ":status":
                status = f.value.copy()
        assert_equal(status, "415", "gate rejects: " + cases[i])
    client.close()
    server_conn.close()


def test_missing_request_message() raises:
    var rig = make_e2e_rig()
    var sid = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.close_send(sid)  # END_STREAM with no DATA
    rig.pump_until_reply()
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.INTERNAL)
    assert_true(
        "missing request message" in result.status.message,
        result.status.message,
    )
    rig.channel.close()
    rig.server_conn.close()


def test_user_agent_and_dispatch_idempotency() raises:
    var rig = make_e2e_rig()
    var sid = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="x"))), last=True
    )
    rig.pump_until_reply()
    # The channel's user agent went out on the wire.
    var ua = String()
    for f in rig.server_conn.streams[sid].headers:
        if f.name == "user-agent":
            ua = f.value.copy()
    assert_equal(ua, String(GRPC_MOJO_USER_AGENT))
    # A second dispatch pass finds nothing new to do.
    assert_equal(rig.server.dispatch_ready(rig.server_conn, rig.handled), 0)
    var result = rig.channel.finish(sid)
    assert_true(result.status.is_ok())
    rig.channel.close()
    rig.server_conn.close()


def test_cancel_marks_stream() raises:
    var rig = make_e2e_rig()
    var sid = rig.channel.start_call("/echo.Echo/Say", Metadata())
    rig.channel.cancel(sid)
    assert_true(
        Bool(rig.channel.conn.streams[sid].reset_code),
        "cancel records the reset locally",
    )
    rig.channel.close()
    rig.server_conn.close()


def streaming_abort_handler(
    req: EchoRequest, mut ctx: ServerContext, mut call: ServerCall
) raises:
    ctx.abort(StatusCode.PERMISSION_DENIED, String("not yours"))


@fieldwise_init
struct E2ERig(Movable):
    var channel: GrpcChannel
    var server: Server
    var server_conn: Http2Connection[GrpcTransport]
    var handled: List[UInt32]

    def pump_until_reply(mut self) raises:
        while True:
            self.server_conn.process_next_frame()
            if self.server.dispatch_ready(self.server_conn, self.handled) > 0:
                return


def make_e2e_rig() raises -> E2ERig:
    var server = Server("127.0.0.1", 0)
    server.register_unary[dummy_handler]("/echo.Echo/Say")
    server.register_server_streaming[streaming_abort_handler](
        "/echo.Echo/Denied"
    )
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return E2ERig(
        channel=channel^,
        server=server^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def test_streaming_abort() raises:
    var rig = make_e2e_rig()
    var sid = rig.channel.start_call("/echo.Echo/Denied", Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(EchoRequest(message="x"))), last=True
    )
    rig.pump_until_reply()
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.PERMISSION_DENIED)
    assert_equal(result.status.message, "not yours")
    rig.channel.close()
    rig.server_conn.close()


# --- public unary API against a forked server process ---


def fork_echo_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    return EchoResponse(message=String("echo: ") + req.message)


def fork_notfound_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    ctx.abort(StatusCode.NOT_FOUND, String("no such thing"))
    return EchoResponse()


def fork_details_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    var details: List[Byte] = [0x08, 0x05]  # any opaque bytes
    ctx.abort_with_details(
        StatusCode.FAILED_PRECONDITION, String("rich"), Span(details)
    )
    return EchoResponse()


def fork_silent_handler(mut ctx: ServerContext, mut call: ServerCall) raises:
    # Finishes OK without ever sending a response message.
    pass


def test_public_unary_api() raises:
    var listener = TCPListener("127.0.0.1", 0)
    var port = listener.local_port
    var pid = external_call["fork", c_int]()
    if pid == 0:
        # Child: dispatch the parent's single connection until it closes.
        var server = Server("127.0.0.1", 0)
        server.register_unary[fork_echo_handler]("/echo.Echo/Say")
        server.register_unary[fork_notfound_handler]("/echo.Echo/Missing")
        server.register_unary[fork_details_handler]("/echo.Echo/Details")
        server.register_bidi[fork_silent_handler]("/echo.Echo/Silent")
        try:
            var tcp = listener.accept()
            var transport = GrpcTransport.plaintext(tcp^)
            var conn = Http2Connection(transport^, is_client=False)
            var handled = List[UInt32]()
            while True:
                conn.process_next_frame()
                _ = server.dispatch_ready(conn, handled)
        except:
            pass
        external_call["_exit", NoneType](c_int(0))

    listener.close()
    var channel = GrpcChannel.connect("127.0.0.1", port)

    # Typed unary happy path.
    var resp = channel.unary[EchoRequest, EchoResponse](
        "/echo.Echo/Say", EchoRequest(message="hi"), timeout_ns=5_000_000_000
    )
    assert_equal(resp.message, "echo: hi")

    # Typed unary raises Status.to_error() on a non-OK status.
    var raised = False
    try:
        _ = channel.unary[EchoRequest, EchoResponse](
            "/echo.Echo/Missing",
            EchoRequest(message="x"),
            timeout_ns=5_000_000_000,
        )
    except e:
        raised = True
        assert_true("NOT_FOUND (5): no such thing" in String(e), String(e))
    assert_true(raised, "unary must raise on abort")

    # unary_bytes returns (not raises) non-OK statuses, with details_bin.
    var r = channel.unary_bytes(
        "/echo.Echo/Details",
        Span(encode(EchoRequest(message="x"))),
        Metadata(),
        timeout_ns=5_000_000_000,
    )
    assert_equal(r.status.code, StatusCode.FAILED_PRECONDITION)
    assert_equal(r.status.message, "rich")
    assert_equal(to_hex(r.status.details_bin), "0805")
    assert_equal(len(r.response), 0)

    # OK with no response message synthesizes INTERNAL.
    var r2 = channel.unary_bytes(
        "/echo.Echo/Silent",
        Span(encode(EchoRequest(message="x"))),
        Metadata(),
        timeout_ns=5_000_000_000,
    )
    assert_equal(r2.status.code, StatusCode.INTERNAL)
    assert_true("no response message" in r2.status.message, r2.status.message)

    channel.close()
    _ = external_call["kill", c_int](pid, c_int(9))


def main() raises:
    test_timeout_encode_units()
    test_timeout_decode_edges()
    test_metadata_key_validation()
    test_metadata_container_semantics()
    test_binary_metadata_edges()
    test_from_headers_reserved()
    test_status_edges()
    test_percent_boundaries()
    test_frame_compressed_flag()
    test_recv_message_errors()
    test_server_max_message_size()
    test_malformed_grpc_timeout_fails_call_only()
    test_content_type_gate_415()
    test_missing_request_message()
    test_user_agent_and_dispatch_idempotency()
    test_cancel_marks_stream()
    test_streaming_abort()
    test_public_unary_api()
    print("test_grpc_edges: all tests passed")
