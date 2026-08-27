# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Client channel: gRPC calls over one TCP, TLS, or Unix socket connection.

`GrpcChannel` multiplexes calls onto a single `Http2Connection`. Unary
calls go through `unary` (typed, raises on non-OK) or `unary_bytes`
(returns a full `CallResult`). Streaming calls use `ServerStreamingCall`,
`ClientStreamingCall`, and `BidiStreamingCall` (generated stubs return
those), or the lower-level primitives `start_call`, `send_msg`,
`recv_msg`, `close_send`, and `finish`.

Deadlines are enforced client-side: `start_call(timeout_ns=...)` sends the
`grpc-timeout` header and records an absolute monotonic deadline, receive
paths arm the socket read timeout (SO_RCVTIMEO) with the remaining budget,
and on expiry the call is cancelled with RST_STREAM(CANCEL) and surfaces
`DEADLINE_EXCEEDED`.
"""

from std.time import monotonic

from hpack import HeaderField
from h2 import ERR_CANCEL, Http2Connection
from net import TCPStream, UnixStream, is_timeout_error
from tls import TLSContext
from proto import ProtoMessage, decode, encode

from .framing import DEFAULT_MAX_RECV_MESSAGE_SIZE, recv_message, send_message
from .metadata import Metadata, decode_bin_value
from .status import (
    Status,
    StatusCode,
    http_status_to_grpc,
    percent_decode_message,
    rst_code_to_grpc,
)
from .timeout import encode_timeout
from .transport import GrpcTransport

comptime GRPC_MOJO_USER_AGENT = "grpc-mojo/0.2.2"
"""Value sent in the user-agent request header."""


@fieldwise_init
struct CallResult(Movable):
    """Everything a finished unary call produced."""

    var status: Status
    """The call's final status, from trailers (or synthesized)."""
    var initial_metadata: Metadata
    """Custom metadata from the initial response headers."""
    var trailing_metadata: Metadata
    """Custom metadata from the trailers."""
    var response: List[Byte]
    """Serialized response message; empty when status is not OK."""


@fieldwise_init
struct GrpcChannel(Movable):
    """A client channel over TCP, TLS, or a Unix domain socket.

    Owns one HTTP/2 connection and issues calls over it. Create with
    `connect`; make unary calls with `unary`/`unary_bytes`, or drive
    streaming calls with `ServerStreamingCall` / `ClientStreamingCall` /
    `BidiStreamingCall`, or with `start_call`, `send_msg`/`send_request_bytes`,
    `recv_msg`/`recv_response_bytes`, `close_send`, and `finish`.
    `set_max_message_size` caps serialized request and response payloads;
    the default is 4 MiB.
    """

    var conn: Http2Connection[GrpcTransport]
    """The underlying HTTP/2 connection."""
    var authority: String
    """Value for the `:authority` pseudo-header (host:port)."""
    var scheme: String
    """Value for the `:scheme` pseudo-header (`http` or `https`)."""
    var deadline_ns: Int64
    """Absolute monotonic deadline for the current call; 0 = none."""
    var max_message_size: Int
    """Maximum serialized request or response size, default 4 MiB."""

    @staticmethod
    def connect(host: StringSpan, port: UInt16) raises -> GrpcChannel:
        """Opens a TCP connection and performs the HTTP/2 client preface.

        Args:
            host: Server host name or address.
            port: Server TCP port.

        Returns:
            A ready channel with `authority` set to `host:port`.

        Raises:
            On connection failure or an HTTP/2 handshake error.
        """
        var tcp = TCPStream.connect(host, port)
        var transport = GrpcTransport.plaintext(tcp^)
        var conn = Http2Connection(transport^, is_client=True)
        return GrpcChannel(
            conn=conn^,
            authority=String(host) + ":" + String(port),
            scheme=String("http"),
            deadline_ns=0,
            max_message_size=DEFAULT_MAX_RECV_MESSAGE_SIZE,
        )

    @staticmethod
    def connect_unix(
        path: StringSpan, *, authority: StringSpan = "localhost"
    ) raises -> GrpcChannel:
        """Opens a Unix domain socket and performs the HTTP/2 preface.

        Args:
            path: Filesystem path of the listening Unix domain socket.
            authority: Value for the HTTP/2 `:authority` pseudo-header.
                Defaults to `localhost`.

        Returns:
            A ready plaintext channel over the Unix domain socket.

        Raises:
            If `authority` is empty, or on connection failure or an HTTP/2
            handshake error.
        """
        if authority == "":
            raise Error("grpc: Unix channel authority cannot be empty")
        var unix = UnixStream.connect(path)
        var transport = GrpcTransport.local(unix^)
        var conn = Http2Connection(transport^, is_client=True)
        return GrpcChannel(
            conn=conn^,
            authority=String(authority),
            scheme=String("http"),
            deadline_ns=0,
            max_message_size=DEFAULT_MAX_RECV_MESSAGE_SIZE,
        )

    @staticmethod
    def connect_tls(
        host: StringSpan,
        port: UInt16,
        *,
        server_name: String = "",
        ca_file: String = "",
        cert_chain_pem: String = "",
        key_pem: String = "",
    ) raises -> GrpcChannel:
        """Opens a verified TLS connection with mandatory `h2` ALPN.

        Args:
            host: Server host name or address used for the TCP connection.
            port: Server TCP port.
            server_name: Name used for SNI and hostname verification;
                empty uses `host`.
            ca_file: PEM trust bundle; empty uses the system trust store.
            cert_chain_pem: Path to the client certificate chain PEM file.
                Must be paired with `key_pem`.
            key_pem: Path to the unencrypted private key PEM file for
                `cert_chain_pem`.

        Returns:
            A ready TLS channel with `:scheme` set to `https`.

        Raises:
            On TCP or TLS failure, certificate rejection, missing `h2`
            ALPN negotiation, or an HTTP/2 handshake error.
        """
        var context = TLSContext.client(
            verify=True,
            ca_file=ca_file,
            cert_chain_pem=cert_chain_pem,
            key_pem=key_pem,
            alpn=[String("h2")],
        )
        var tcp = TCPStream.connect(host, port)
        var name = server_name
        if name == "":
            name = String(host)
        var tls = context.connect(tcp^, name)
        if tls.negotiated_alpn() != "h2":
            tls.close()
            raise Error(
                "grpc: TLS peer did not negotiate the required h2 ALPN token"
            )
        var transport = GrpcTransport.secure(tls^)
        var conn = Http2Connection(transport^, is_client=True)
        return GrpcChannel(
            conn=conn^,
            authority=name.copy() + ":" + String(port),
            scheme=String("https"),
            deadline_ns=0,
            max_message_size=DEFAULT_MAX_RECV_MESSAGE_SIZE,
        )

    def set_max_message_size(mut self, size: Int) raises:
        """Sets the serialized request and response size limit.

        Args:
            size: Maximum message bytes. `0` rejects every non-empty
                payload. Must fit in the 32-bit gRPC length prefix.

        Raises:
            If `size` is negative or greater than 4_294_967_295.
        """
        if size < 0:
            raise Error("grpc: max_message_size must be non-negative")
        if size > 0xFFFFFFFF:
            raise Error("grpc: max_message_size exceeds the gRPC prefix")
        self.max_message_size = size

    def _arm_deadline(mut self) raises -> Bool:
        """Set the socket read timeout to the remaining call budget.
        Returns False if the deadline has already passed."""
        if self.deadline_ns == 0:
            return True
        var remaining = self.deadline_ns - Int64(monotonic())
        if remaining <= 0:
            return False
        self.conn.stream.set_read_timeout(remaining)
        return True

    def _clear_deadline(mut self) raises:
        if self.deadline_ns != 0:
            self.deadline_ns = 0
            self.conn.stream.set_read_timeout(0)

    def cancel(mut self, sid: UInt32) raises:
        """Cancels an in-flight call with RST_STREAM(CANCEL).

        The stream is also marked reset locally so later reads on it fail
        fast instead of waiting for the server.

        Args:
            sid: The stream id returned by `start_call`.

        Raises:
            On connection I/O errors while sending the reset.
        """
        self.conn.send_rst_stream(sid, ERR_CANCEL)
        self.conn._ensure_stream(sid)
        self.conn.streams[sid].reset_code = ERR_CANCEL

    def _reset_on_size_error(mut self, sid: UInt32):
        try:
            self.cancel(sid)
        except:
            pass

    def _recv_capped(mut self, sid: UInt32) raises -> Optional[List[Byte]]:
        try:
            return recv_message(
                self.conn, sid, max_size=self.max_message_size
            )
        except e:
            if String(e) == "grpc: message exceeds max size":
                self._reset_on_size_error(sid)
            raise e

    def start_call(
        mut self,
        path: StringSpan,
        metadata: Metadata,
        *,
        timeout_ns: Int64 = 0,
    ) raises -> UInt32:
        """Opens a stream and sends the gRPC Request-Headers.

        Sends the pseudo-headers, `te: trailers`, `content-type:
        application/grpc+proto`, the user agent, an optional `grpc-timeout`,
        and the caller's custom metadata. With a timeout, the channel also
        records an absolute deadline that the receive paths enforce.

        Args:
            path: Full method path, e.g. `/echo.Echo/Say`.
            metadata: Custom metadata to send with the request headers.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            The stream id for use with the other call methods.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        var sid = self.conn.open_stream()
        var headers = List[HeaderField]()
        headers.append(
            HeaderField(name=String(":method"), value=String("POST"))
        )
        headers.append(
            HeaderField(name=String(":scheme"), value=self.scheme.copy())
        )
        headers.append(HeaderField(name=String(":path"), value=String(path)))
        headers.append(
            HeaderField(name=String(":authority"), value=self.authority.copy())
        )
        headers.append(HeaderField(name=String("te"), value=String("trailers")))
        if timeout_ns > 0:
            headers.append(
                HeaderField(
                    name=String("grpc-timeout"),
                    value=encode_timeout(timeout_ns),
                )
            )
        headers.append(
            HeaderField(
                name=String("content-type"),
                value=String("application/grpc+proto"),
            )
        )
        headers.append(
            HeaderField(
                name=String("user-agent"), value=String(GRPC_MOJO_USER_AGENT)
            )
        )
        for e in metadata.entries:
            headers.append(e.copy())
        self.conn.send_headers(sid, Span(headers), end_stream=False)
        if timeout_ns > 0:
            self.deadline_ns = Int64(monotonic()) + timeout_ns
        else:
            self.deadline_ns = 0
        return sid

    def send_request_bytes(
        mut self, sid: UInt32, payload: Span[Byte, _], *, last: Bool
    ) raises:
        """Sends one serialized request message on a call.

        Args:
            sid: The stream id returned by `start_call`.
            payload: The serialized message bytes.
            last: Whether this is the final request message; if True the
                request stream is half-closed (END_STREAM).

        Raises:
            If `payload` exceeds `max_message_size`, or on connection I/O
            or HTTP/2 protocol errors. An oversized payload resets `sid`
            with RST_STREAM(CANCEL) before raising.
        """
        if len(payload) > self.max_message_size:
            self._reset_on_size_error(sid)
            raise Error("grpc: message exceeds max size")
        send_message(self.conn, sid, payload, end_stream=last)

    def close_send(mut self, sid: UInt32) raises:
        """Half-closes the request stream without a message.

        Sends an empty DATA frame with END_STREAM, as the spec prescribes
        for ending the request stream after the last message.

        Args:
            sid: The stream id returned by `start_call`.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        var empty = List[Byte]()
        self.conn.send_data(sid, Span(empty), end_stream=True)

    def recv_response_bytes(
        mut self, sid: UInt32
    ) raises -> Optional[List[Byte]]:
        """Receives the next serialized response message.

        Unlike `recv_msg`, this does not arm the call deadline; use it when
        managing timeouts manually.

        Args:
            sid: The stream id returned by `start_call`.

        Returns:
            The message bytes, or None when the response stream ends.

        Raises:
            On framing or connection errors, or when the message exceeds
            `max_message_size`. An oversized response resets `sid` with
            RST_STREAM(CANCEL) before raising.
        """
        return self._recv_capped(sid)

    def send_msg[
        M: ProtoMessage
    ](mut self, sid: UInt32, msg: M, *, last: Bool = False) raises:
        """Sends one typed message on a streaming call.

        Parameters:
            M: The request message type.

        Args:
            sid: The stream id returned by `start_call`.
            msg: The message to encode and send.
            last: Whether this is the final request message; if True the
                request stream is half-closed.

        Raises:
            On encoding or connection errors, or when the encoded message
            exceeds `max_message_size`.
        """
        self.send_request_bytes(sid, Span(encode(msg)), last=last)

    def recv_msg[M: ProtoMessage](mut self, sid: UInt32) raises -> Optional[M]:
        """Receives the next typed message; None when the stream ends.

        Honors the call deadline set in `start_call`: the socket read
        timeout is armed with the remaining budget, and on expiry the call
        is cancelled with RST_STREAM(CANCEL) and DEADLINE_EXCEEDED is
        raised.

        Parameters:
            M: The response message type.

        Args:
            sid: The stream id returned by `start_call`.

        Returns:
            The decoded message, or None on end of the response stream.

        Raises:
            `grpc: DEADLINE_EXCEEDED` on deadline expiry; otherwise on
            decoding, framing, or connection errors, or when the message
            exceeds `max_message_size`.
        """
        if not self._arm_deadline():
            self.cancel(sid)
            raise Error("grpc: DEADLINE_EXCEEDED")
        try:
            var raw = self._recv_capped(sid)
            if raw:
                return decode[M](Span(raw.value()))
            return None
        except e:
            if is_timeout_error(e):
                self.cancel(sid)
                raise Error("grpc: DEADLINE_EXCEEDED")
            raise e

    def finish(mut self, sid: UInt32) raises -> CallResult:
        """Waits for the stream to end and assembles status plus metadata.

        Extracts `grpc-status`, `grpc-message` (percent-decoded), and
        `grpc-status-details-bin` from the trailers — or from the only
        HEADERS block of a Trailers-Only response — and maps RST_STREAM or
        bare HTTP errors to gRPC codes when the server sent no status.

        Args:
            sid: The stream id returned by `start_call`.

        Returns:
            The call result; its `response` field is left empty (streaming
            responses are consumed via `recv_msg`).

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        self.conn.wait_stream_end(sid)
        var status = self._extract_status(sid)
        var initial = Metadata()
        var trailing = Metadata()
        if self.conn.streams[sid].headers_done:
            initial = Metadata.from_headers(
                Span(self.conn.streams[sid].headers)
            )
        if self.conn.streams[sid].trailers_done:
            trailing = Metadata.from_headers(
                Span(self.conn.streams[sid].trailers)
            )
        return CallResult(
            status=status^,
            initial_metadata=initial^,
            trailing_metadata=trailing^,
            response=List[Byte](),
        )

    def _find_header(
        self, fields: Span[HeaderField, _], name: StaticString
    ) -> Optional[String]:
        for f in fields:
            if f.name == String(name):
                return f.value.copy()
        return None

    def _extract_status(mut self, sid: UInt32) raises -> Status:
        # RST_STREAM → mapped code.
        if self.conn.streams[sid].reset_code:
            var rst = self.conn.streams[sid].reset_code.value()
            return Status(
                code=rst_code_to_grpc(rst),
                message=String("stream reset by server (RST_STREAM)"),
            )
        # Prefer trailers; a Trailers-Only response carries grpc-status in
        # the (only) HEADERS block, which we stored as headers.
        var source: List[HeaderField]
        if self.conn.streams[sid].trailers_done:
            source = self.conn.streams[sid].trailers.copy()
        elif self.conn.streams[sid].headers_done:
            source = self.conn.streams[sid].headers.copy()
        else:
            return Status(
                code=StatusCode.INTERNAL,
                message=String("stream ended without headers"),
            )
        # Broken/proxy responses: non-200 :status without grpc-status.
        var grpc_status = self._find_header(Span(source), "grpc-status")
        if not grpc_status:
            var http_status = self._find_header(
                Span(self.conn.streams[sid].headers), ":status"
            )
            var code = StatusCode.UNKNOWN
            var msg = String("missing grpc-status")
            if http_status:
                var hs = 200
                try:
                    hs = Int(http_status.value())
                except:
                    pass  # non-numeric :status: keep UNKNOWN
                if hs != 200:
                    code = http_status_to_grpc(hs)
                    msg = String("HTTP status ") + http_status.value()
            return Status(code=code, message=msg^)
        var code: Int
        try:
            code = Int(grpc_status.value())
        except:
            # A peer that sends a non-numeric grpc-status is broken; map
            # it to UNKNOWN rather than surfacing a parse error.
            return Status(
                code=StatusCode.UNKNOWN,
                message=String("invalid grpc-status: ") + grpc_status.value(),
            )
        var message = String()
        var raw_msg = self._find_header(Span(source), "grpc-message")
        if raw_msg:
            message = percent_decode_message(raw_msg.value())
        var status = Status(code=code, message=message^)
        var details = self._find_header(Span(source), "grpc-status-details-bin")
        if details:
            try:
                status.details_bin = decode_bin_value(details.value())
            except:
                pass  # tolerate broken encodings, like grpc-message
        return status^

    def unary_bytes(
        mut self,
        path: StringSpan,
        request: Span[Byte, _],
        metadata: Metadata,
        *,
        timeout_ns: Int64 = 0,
    ) raises -> CallResult:
        """One request message in, one response message out, as bytes.

        Enforces the deadline: on expiry the call is cancelled with
        RST_STREAM(CANCEL) and DEADLINE_EXCEEDED is returned. Non-OK
        statuses are returned in the result, not raised, so callers can
        inspect trailing metadata and `Status.details_bin`. An OK status
        with no response message is reported as INTERNAL.

        Args:
            path: Full method path, e.g. `/echo.Echo/Say`.
            request: The serialized request message.
            metadata: Custom metadata to send with the request headers.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            The final status, both metadata sets, and the serialized
            response (empty when the status is not OK).

        Raises:
            If `request` exceeds `max_message_size`, or on connection I/O
            or HTTP/2 protocol errors other than a deadline expiry.
            Oversized unary requests fail before request headers are sent.
        """
        if len(request) > self.max_message_size:
            raise Error("grpc: message exceeds max size")
        var sid = self.start_call(path, metadata, timeout_ns=timeout_ns)
        self.send_request_bytes(sid, request, last=True)
        var response = List[Byte]()
        var had_msg: Bool
        try:
            if not self._arm_deadline():
                raise Error("net: timeout")
            self.conn.wait_headers(sid)
            if not self._arm_deadline():
                raise Error("net: timeout")
            # A failed call may carry no response message; try to read one
            # but treat a clean end as "no message"; the status decides.
            var msg = self.recv_response_bytes(sid)
            had_msg = Bool(msg)
            if msg:
                response = msg.take()
        except e:
            if is_timeout_error(e):
                try:
                    self.cancel(sid)
                except:
                    pass
                self._clear_deadline()
                return CallResult(
                    status=Status(
                        code=StatusCode.DEADLINE_EXCEEDED,
                        message=String("Deadline Exceeded"),
                    ),
                    initial_metadata=Metadata(),
                    trailing_metadata=Metadata(),
                    response=List[Byte](),
                )
            self._clear_deadline()
            raise e
        var result = self.finish(sid)
        self._clear_deadline()
        if result.status.is_ok() and not had_msg:
            result.status = Status(
                code=StatusCode.INTERNAL,
                message=String("OK status but no response message"),
            )
        result.response = response^
        return result^

    def unary[
        Req: ProtoMessage, Resp: ProtoMessage
    ](
        mut self,
        path: StringSpan,
        request: Req,
        *,
        timeout_ns: Int64 = 0,
    ) raises -> Resp:
        """Typed unary call; raises on non-OK status.

        Convenience wrapper over `unary_bytes` with empty metadata. Use
        `unary_bytes` directly to send metadata or inspect the trailers of
        a failed call.

        Parameters:
            Req: The request message type.
            Resp: The response message type.

        Args:
            path: Full method path, e.g. `/echo.Echo/Say`.
            request: The request message.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            The decoded response message.

        Raises:
            The call's `Status` converted to an error when it is not OK
            (including DEADLINE_EXCEEDED); otherwise on decoding or
            connection errors.
        """
        var md = Metadata()
        var result = self.unary_bytes(
            path, Span(encode(request)), md, timeout_ns=timeout_ns
        )
        if not result.status.is_ok():
            raise result.status.to_error()
        return decode[Resp](Span(result.response))

    def close(mut self):
        """Closes the underlying connection; the channel is unusable after."""
        self.conn.close()


struct ServerStreamingCall[Resp: ProtoMessage](Movable):
    """Typed handle for one server-streaming RPC.

    Holds an untracked pointer to the `GrpcChannel` that started the call.
    Do not store it after the channel is closed or moved.

    Parameters:
        Resp: The response message type.
    """

    var _channel: Pointer[GrpcChannel, MutUntrackedOrigin]
    """Untracked pointer to the owning channel; valid only while it lives."""
    var sid: UInt32
    """The HTTP/2 stream id of this call."""
    var _recv_done: Bool
    """True after the response stream has ended."""

    def __init__(
        out self,
        _channel: Pointer[GrpcChannel, MutUntrackedOrigin],
        sid: UInt32,
        _recv_done: Bool,
    ):
        """Stores the channel pointer and stream id.

        Args:
            _channel: Untracked pointer to the owning channel.
            sid: The HTTP/2 stream id of this call.
            _recv_done: True after the response stream has ended.
        """
        self._channel = _channel
        self.sid = sid
        self._recv_done = _recv_done

    @staticmethod
    def start[
        Req: ProtoMessage
    ](
        mut channel: GrpcChannel,
        path: StringSpan,
        request: Req,
        *,
        timeout_ns: Int64 = 0,
    ) raises -> Self:
        """Sends the request and returns a handle for response messages.

        Parameters:
            Req: The request message type.

        Args:
            channel: The channel that owns the connection.
            path: Full method path, e.g. `/echo.Echo/Split`.
            request: The single request message.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            A handle whose `recv` yields response messages.

        Raises:
            On connection I/O or HTTP/2 protocol errors, or when the
            encoded request exceeds `max_message_size`.
        """
        var sid = channel.start_call(path, Metadata(), timeout_ns=timeout_ns)
        channel.send_msg[Req](sid, request, last=True)
        return Self(
            _channel=Pointer(to=channel).unsafe_origin_cast[MutUntrackedOrigin](),
            sid=sid,
            _recv_done=False,
        )

    def recv(mut self) raises -> Optional[Self.Resp]:
        """Receives the next response message; None when the stream ends.

        Returns:
            The decoded message, or None on end of the response stream.

        Raises:
            `grpc: DEADLINE_EXCEEDED` on deadline expiry; otherwise on
            decoding, framing, or connection errors.
        """
        if self._recv_done:
            return None
        var msg = self._channel[].recv_msg[Self.Resp](self.sid)
        if not msg:
            self._recv_done = True
        return msg^

    def finish(mut self) raises -> CallResult:
        """Waits for trailers and returns the call status.

        Returns:
            The call result; `response` is empty because messages were
            consumed through `recv`.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        return self._channel[].finish(self.sid)

    def cancel(mut self) raises:
        """Cancels the call with RST_STREAM(CANCEL)."""
        self._recv_done = True
        self._channel[].cancel(self.sid)


struct ClientStreamingCall[Req: ProtoMessage, Resp: ProtoMessage](Movable):
    """Typed handle for one client-streaming RPC.

    Holds an untracked pointer to the `GrpcChannel` that started the call.
    Do not store it after the channel is closed or moved.

    Parameters:
        Req: The request message type.
        Resp: The response message type.
    """

    var _channel: Pointer[GrpcChannel, MutUntrackedOrigin]
    """Untracked pointer to the owning channel; valid only while it lives."""
    var sid: UInt32
    """The HTTP/2 stream id of this call."""
    var _send_closed: Bool
    """True after `close_send` or `finish` half-closed the request stream."""

    def __init__(
        out self,
        _channel: Pointer[GrpcChannel, MutUntrackedOrigin],
        sid: UInt32,
        _send_closed: Bool,
    ):
        """Stores the channel pointer and stream id.

        Args:
            _channel: Untracked pointer to the owning channel.
            sid: The HTTP/2 stream id of this call.
            _send_closed: True after the request stream is half-closed.
        """
        self._channel = _channel
        self.sid = sid
        self._send_closed = _send_closed

    @staticmethod
    def start(
        mut channel: GrpcChannel, path: StringSpan, *, timeout_ns: Int64 = 0
    ) raises -> Self:
        """Opens the call without sending a request message.

        Args:
            channel: The channel that owns the connection.
            path: Full method path, e.g. `/echo.Echo/Join`.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            A handle for `send` / `finish`.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        var sid = channel.start_call(path, Metadata(), timeout_ns=timeout_ns)
        return Self(
            _channel=Pointer(to=channel).unsafe_origin_cast[MutUntrackedOrigin](),
            sid=sid,
            _send_closed=False,
        )

    def send(mut self, msg: Self.Req) raises:
        """Sends one request message.

        Args:
            msg: The message to encode and send.

        Raises:
            If the request stream is already half-closed, the encoded
            message exceeds `max_message_size`, or on connection errors.
        """
        if self._send_closed:
            raise Error("grpc: send after close_send")
        self._channel[].send_msg[Self.Req](self.sid, msg, last=False)

    def close_send(mut self) raises:
        """Half-closes the request stream without sending a message."""
        if self._send_closed:
            return
        self._channel[].close_send(self.sid)
        self._send_closed = True

    def finish(mut self) raises -> Self.Resp:
        """Half-closes if needed, receives the response, and checks status.

        Returns:
            The decoded response message.

        Raises:
            The call's `Status` when it is not OK, or if the server sent
            no response message.
        """
        self.close_send()
        var msg = self._channel[].recv_msg[Self.Resp](self.sid)
        var result = self._channel[].finish(self.sid)
        if not result.status.is_ok():
            raise result.status.to_error()
        if not msg:
            raise Error("grpc: missing response message")
        return msg.take()

    def cancel(mut self) raises:
        """Cancels the call with RST_STREAM(CANCEL)."""
        self._send_closed = True
        self._channel[].cancel(self.sid)


struct BidiStreamingCall[Req: ProtoMessage, Resp: ProtoMessage](Movable):
    """Typed handle for one bidirectional streaming RPC.

    Recv-driven ping-pong works on one thread: `recv` pumps the connection
    until the next message. Full-duplex firehose needs threads, which Mojo
    1.0 does not expose. Holds an untracked pointer to the `GrpcChannel`
    that started the call; do not store it after the channel is closed or
    moved.

    Parameters:
        Req: The request message type.
        Resp: The response message type.
    """

    var _channel: Pointer[GrpcChannel, MutUntrackedOrigin]
    """Untracked pointer to the owning channel; valid only while it lives."""
    var sid: UInt32
    """The HTTP/2 stream id of this call."""
    var _send_closed: Bool
    """True after `close_send` half-closed the request stream."""
    var _recv_done: Bool
    """True after the response stream has ended."""

    def __init__(
        out self,
        _channel: Pointer[GrpcChannel, MutUntrackedOrigin],
        sid: UInt32,
        _send_closed: Bool,
        _recv_done: Bool,
    ):
        """Stores the channel pointer and stream id.

        Args:
            _channel: Untracked pointer to the owning channel.
            sid: The HTTP/2 stream id of this call.
            _send_closed: True after the request stream is half-closed.
            _recv_done: True after the response stream has ended.
        """
        self._channel = _channel
        self.sid = sid
        self._send_closed = _send_closed
        self._recv_done = _recv_done

    @staticmethod
    def start(
        mut channel: GrpcChannel, path: StringSpan, *, timeout_ns: Int64 = 0
    ) raises -> Self:
        """Opens the call without sending a message.

        Args:
            channel: The channel that owns the connection.
            path: Full method path, e.g. `/echo.Echo/Chat`.
            timeout_ns: Call deadline in nanoseconds; 0 means none.

        Returns:
            A handle for `send` / `recv` / `finish`.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        var sid = channel.start_call(path, Metadata(), timeout_ns=timeout_ns)
        return Self(
            _channel=Pointer(to=channel).unsafe_origin_cast[MutUntrackedOrigin](),
            sid=sid,
            _send_closed=False,
            _recv_done=False,
        )

    def send(mut self, msg: Self.Req, *, last: Bool = False) raises:
        """Sends one request message.

        Args:
            msg: The message to encode and send.
            last: If True, half-closes the request stream after this
                message.

        Raises:
            If the request stream is already half-closed, the encoded
            message exceeds `max_message_size`, or on connection errors.
        """
        if self._send_closed:
            raise Error("grpc: send after close_send")
        self._channel[].send_msg[Self.Req](self.sid, msg, last=last)
        if last:
            self._send_closed = True

    def recv(mut self) raises -> Optional[Self.Resp]:
        """Receives the next response message; None when the stream ends.

        Returns:
            The decoded message, or None on end of the response stream.

        Raises:
            `grpc: DEADLINE_EXCEEDED` on deadline expiry; otherwise on
            decoding, framing, or connection errors.
        """
        if self._recv_done:
            return None
        var msg = self._channel[].recv_msg[Self.Resp](self.sid)
        if not msg:
            self._recv_done = True
        return msg^

    def close_send(mut self) raises:
        """Half-closes the request stream without sending a message."""
        if self._send_closed:
            return
        self._channel[].close_send(self.sid)
        self._send_closed = True

    def finish(mut self) raises -> CallResult:
        """Waits for trailers and returns the call status.

        Returns:
            The call result; `response` is empty because messages were
            consumed through `recv`.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        return self._channel[].finish(self.sid)

    def cancel(mut self) raises:
        """Cancels the call with RST_STREAM(CANCEL)."""
        self._send_closed = True
        self._recv_done = True
        self._channel[].cancel(self.sid)
