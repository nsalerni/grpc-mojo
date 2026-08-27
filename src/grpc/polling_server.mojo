# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""A bounded readiness-driven server for unary h2c or TLS RPCs.

`PollingServer` is an opt-in alternative to the blocking `Server`. It uses
`mojo-net`'s kqueue/epoll `Poller` so one thread can make progress on many
TCP connections. Handler functions still run serially and must return
promptly. Streaming RPCs, Unix sockets, and graceful shutdown remain on the
blocking server path. TLS handshakes advance through the same Poller and
strictly require the `h2` ALPN token. Optional HTTP/2 keepalive PINGs are
driven from the Poller loop when `keepalive_interval_ns` is positive.

Every connection has one active RPC stream and explicit inactivity, request,
read, frame, write, message, and output limits. The HTTP/2 output queue and
the socket write buffer are never populated at the same time, so retained
wire output is bounded by `max_pending_output_size` per connection and by
`max_connections * max_pending_output_size` for the server. Application
responses have a separate server-wide bound of
`max_connections * (max_message_size + 5)` bytes.

TLS handshake sessions count toward `max_connections`, have a separate
admission cap and absolute timeout, and advance through a bounded number of
OpenSSL steps per Poller turn. OpenSSL's per-session allocations are outside
the plaintext HTTP/2 and gRPC byte limits.
"""

from std.ffi import c_int
from std.time import monotonic

from hpack import HeaderField
from h2 import H2_ALPN, Http2Connection, get_u32_be
from net import PollEvent, Poller, TCPListener, is_would_block
from proto import ProtoMessage, decode, encode
from tls import PeerCertificate, TLSContext, TLSHandshake

from .framing import GRPC_MESSAGE_PREFIX_LEN, frame_message
from .metadata import Metadata, encode_bin_value
from .server import ServerContext, UnaryBytesHandler
from .status import Status, StatusCode, percent_encode_message
from .timeout import decode_timeout
from .transport import GrpcTransport


struct PollingServerConfig(Copyable, Movable):
    """Resource and fairness limits for `PollingServer`.

    Defaults cap one connection's retained request or response message at
    4 MiB and its unsent wire output at 64 KiB. Accept, TLS handshake, read,
    HTTP/2 dispatch, and write work are bounded for each Poller turn.
    """

    var max_connections: Int
    """Maximum accepted connections owned by the event loop."""
    var max_accepts_per_event: Int
    """Maximum new connections accepted for one listener event."""
    var max_pending_handshakes: Int
    """Maximum TLS handshakes owned by the event loop."""
    var max_handshake_steps_per_event: Int
    """Maximum TLS handshake steps in one Poller turn."""
    var max_message_size: Int
    """Maximum serialized unary request or response size."""
    var max_read_bytes_per_event: Int
    """Maximum socket bytes read for one connection event."""
    var max_frames_per_event: Int
    """Maximum complete HTTP/2 frames dispatched per connection event."""
    var max_write_bytes_per_event: Int
    """Maximum socket bytes written for one connection event."""
    var max_pending_output_size: Int
    """Maximum unsent wire bytes retained per connection."""
    var idle_timeout_ms: Int
    """Maximum time without socket progress before eviction."""
    var incomplete_request_timeout_ms: Int
    """Maximum first-request or active-request setup time."""
    var tls_handshake_timeout_ms: Int
    """Maximum time allowed for a TLS handshake."""
    var keepalive_interval_ns: Int64
    """Idle nanoseconds before an HTTP/2 keepalive PING; 0 disables PINGs."""

    def __init__(
        out self,
        *,
        max_connections: Int = 128,
        max_accepts_per_event: Int = 32,
        max_pending_handshakes: Int = 32,
        max_handshake_steps_per_event: Int = 32,
        max_message_size: Int = 4 * 1024 * 1024,
        max_read_bytes_per_event: Int = 64 * 1024,
        max_frames_per_event: Int = 64,
        max_write_bytes_per_event: Int = 64 * 1024,
        max_pending_output_size: Int = 64 * 1024,
        idle_timeout_ms: Int = 300_000,
        incomplete_request_timeout_ms: Int = 30_000,
        tls_handshake_timeout_ms: Int = 10_000,
        keepalive_interval_ns: Int64 = 0,
    ):
        """Constructs a limit set, using safe bounded defaults.

        Args:
            max_connections: Maximum accepted connections.
            max_accepts_per_event: Maximum accepts for one listener event.
            max_pending_handshakes: Maximum in-progress TLS handshakes. The
                effective limit cannot exceed `max_connections`.
            max_handshake_steps_per_event: Maximum TLS handshake work for one
                Poller turn.
            max_message_size: Maximum serialized request or response size.
            max_read_bytes_per_event: Maximum bytes read per connection event.
            max_frames_per_event: Maximum HTTP/2 frames dispatched per event.
            max_write_bytes_per_event: Maximum bytes written per event.
            max_pending_output_size: Maximum retained wire output per
                connection.
            idle_timeout_ms: Maximum milliseconds without socket progress.
            incomplete_request_timeout_ms: Maximum milliseconds allowed for
                first-request setup or an active incomplete request.
            tls_handshake_timeout_ms: Maximum milliseconds allowed for a TLS
                handshake. This limit is unused by h2c connections.
            keepalive_interval_ns: Idle nanoseconds before
                `Http2Connection.maybe_keepalive_ping` queues a PING. 0
                disables keepalive. PINGs are caller-driven from the Poller
                loop; there is no internal timer.
        """
        self.max_connections = max_connections
        self.max_accepts_per_event = max_accepts_per_event
        self.max_pending_handshakes = max_pending_handshakes
        self.max_handshake_steps_per_event = max_handshake_steps_per_event
        self.max_message_size = max_message_size
        self.max_read_bytes_per_event = max_read_bytes_per_event
        self.max_frames_per_event = max_frames_per_event
        self.max_write_bytes_per_event = max_write_bytes_per_event
        self.max_pending_output_size = max_pending_output_size
        self.idle_timeout_ms = idle_timeout_ms
        self.incomplete_request_timeout_ms = incomplete_request_timeout_ms
        self.tls_handshake_timeout_ms = tls_handshake_timeout_ms
        self.keepalive_interval_ns = keepalive_interval_ns

    def validate(self) raises:
        """Rejects limits that cannot make safe forward progress.

        Raises:
            If any limit is outside its supported range.
        """
        if self.max_connections <= 0:
            raise Error("grpc: max_connections must be positive")
        if self.max_accepts_per_event <= 0:
            raise Error("grpc: max_accepts_per_event must be positive")
        if self.max_pending_handshakes <= 0:
            raise Error("grpc: max_pending_handshakes must be positive")
        if self.max_handshake_steps_per_event <= 0:
            raise Error("grpc: max_handshake_steps_per_event must be positive")
        if self.max_message_size < 0:
            raise Error("grpc: max_message_size must be non-negative")
        if self.max_message_size > 0xFFFFFFFF:
            raise Error("grpc: max_message_size exceeds the gRPC prefix")
        if self.max_read_bytes_per_event <= 0:
            raise Error("grpc: max_read_bytes_per_event must be positive")
        if self.max_read_bytes_per_event > 0x7FFFFFFF:
            raise Error("grpc: max_read_bytes_per_event exceeds c_int")
        if self.max_frames_per_event <= 0:
            raise Error("grpc: max_frames_per_event must be positive")
        if self.max_write_bytes_per_event <= 0:
            raise Error("grpc: max_write_bytes_per_event must be positive")
        if self.max_write_bytes_per_event > 0x7FFFFFFF:
            raise Error("grpc: max_write_bytes_per_event exceeds c_int")
        # This admits automatic HTTP/2 responses plus every built-in gRPC
        # success and fixed-size error header block.
        if self.max_pending_output_size < 1024:
            raise Error("grpc: max_pending_output_size must be at least 1024")
        if self.max_pending_output_size > 0x7FFFFFFF:
            raise Error("grpc: max_pending_output_size exceeds c_int")
        if self.idle_timeout_ms <= 0:
            raise Error("grpc: idle_timeout_ms must be positive")
        if self.incomplete_request_timeout_ms <= 0:
            raise Error("grpc: incomplete_request_timeout_ms must be positive")
        if self.tls_handshake_timeout_ms <= 0:
            raise Error("grpc: tls_handshake_timeout_ms must be positive")
        if self.keepalive_interval_ns < 0:
            raise Error("grpc: keepalive_interval_ns must be non-negative")
        # Poller forwards milliseconds to epoll_wait/kqueue through c_int.
        var max_timeout_ms = 0x7FFFFFFF
        if (
            self.idle_timeout_ms > max_timeout_ms
            or self.incomplete_request_timeout_ms > max_timeout_ms
            or self.tls_handshake_timeout_ms > max_timeout_ms
        ):
            raise Error("grpc: timeout exceeds Poller millisecond range")
        if self.keepalive_interval_ns > Int64(max_timeout_ms) * 1_000_000:
            raise Error("grpc: keepalive interval exceeds Poller millisecond range")


@fieldwise_init
struct _ReadyEvent(Copyable, Movable):
    var fd: c_int
    var readable: Bool
    var writable: Bool
    var hangup: Bool
    var error: Bool


def _coalesce_poll_events(events: Span[PollEvent, _]) -> List[_ReadyEvent]:
    """Combines kqueue's per-filter events into one record per descriptor."""
    var out = List[_ReadyEvent]()
    for event in events:
        var found = False
        for i in range(len(out)):
            if out[i].fd == event.fd:
                out[i].readable |= event.readable
                out[i].writable |= event.writable
                out[i].hangup |= event.hangup
                out[i].error |= event.error
                found = True
                break
        if not found:
            out.append(
                _ReadyEvent(
                    fd=event.fd,
                    readable=event.readable,
                    writable=event.writable,
                    hangup=event.hangup,
                    error=event.error,
                )
            )
    return out^


def _can_move_http2_output(io_blocked_read: Bool, io_wants_write: Bool) -> Bool:
    """Keeps one output store while SSL_read awaits writable readiness."""
    return not (io_blocked_read and io_wants_write)


struct _PendingWrite(Movable, Sized):
    """Owned socket output plus the first unsent byte offset."""

    var data: List[Byte]
    var offset: Int

    def __init__(out self):
        self.data = List[Byte]()
        self.offset = 0

    def __len__(self) -> Int:
        return len(self.data) - self.offset

    def replace(mut self, var data: List[Byte]) raises:
        if len(self) != 0:
            raise Error("grpc: cannot replace pending socket output")
        self.data = data^
        self.offset = 0

    def advance(mut self, count: Int) raises:
        if len(self) > 0 and count <= 0:
            raise Error("grpc: socket write made no progress")
        if count < 0 or count > len(self):
            raise Error("grpc: invalid socket write progress")
        self.offset += count
        if self.offset == len(self.data):
            self.data = List[Byte]()
            self.offset = 0

    def copy_suffix(self) -> List[Byte]:
        return List[Byte](Span(self.data)[self.offset : len(self.data)])


@fieldwise_init
struct _PollingRoute(Movable):
    var handler: UnaryBytesHandler


@fieldwise_init
struct _PollingHandshake(Movable):
    """One bounded readiness-driven TLS server handshake."""

    var tls: TLSHandshake
    var accepted_ns: Int64

    def remaining(self, now_ns: Int64, timeout_ms: Int) -> Int64:
        return Int64(timeout_ms) * 1_000_000 - (now_ns - self.accepted_ns)

    def finish(
        deinit self, config: PollingServerConfig
    ) raises -> _PollingConnection:
        var tls = self.tls^.finish()
        if tls.negotiated_alpn() != H2_ALPN:
            tls.close()
            raise Error("grpc: TLS peer did not negotiate h2 ALPN")
        var peer_certificate = tls.peer_certificate()
        return _PollingConnection(
            GrpcTransport.secure(tls^), config, peer_certificate^
        )


struct _PollingConnection(Movable):
    """All mutable protocol and application state for one descriptor."""

    var h2: Http2Connection[GrpcTransport]
    var peer_certificate: Optional[PeerCertificate]
    var accepted_ns: Int64
    var last_activity_ns: Int64
    var served_request: Bool
    var output: _PendingWrite
    var active_sid: UInt32
    var call_start_ns: Int64
    var ctx: ServerContext
    var prefix: List[Byte]
    var request: List[Byte]
    var request_length: Int
    var request_complete: Bool
    var discard_request: Bool
    var request_error: Optional[Status]
    var unsupported_media_type: Bool
    var handler_called: Bool
    var response: List[Byte]
    var response_offset: Int
    var response_status: Status
    var headers_queued: Bool
    var trailers_queued: Bool
    var peer_eof: Bool
    var close_after_flush: Bool
    var resume_read: Bool
    var io_wants_read: Bool
    var io_wants_write: Bool
    var io_blocked_read: Bool

    def __init__(
        out self,
        var transport: GrpcTransport,
        config: PollingServerConfig,
        var peer_certificate: Optional[PeerCertificate] = None,
    ) raises:
        self.h2 = Http2Connection(transport^, is_client=False)
        self.peer_certificate = peer_certificate^
        self.accepted_ns = Int64(monotonic())
        self.last_activity_ns = self.accepted_ns
        self.h2.touch_keepalive(self.accepted_ns)
        self.served_request = False
        self.h2.max_concurrent_streams = 1
        self.h2.max_pending_output_size = config.max_pending_output_size
        self.output = _PendingWrite()
        self.active_sid = 0
        self.call_start_ns = 0
        self.ctx = ServerContext(self.peer_certificate)
        self.prefix = List[Byte]()
        self.request = List[Byte]()
        self.request_length = -1
        self.request_complete = False
        self.discard_request = False
        self.request_error = None
        self.unsupported_media_type = False
        self.handler_called = False
        self.response = List[Byte]()
        self.response_offset = 0
        self.response_status = Status.ok()
        self.headers_queued = False
        self.trailers_queued = False
        self.peer_eof = False
        self.close_after_flush = False
        self.resume_read = False
        self.io_wants_read = False
        self.io_wants_write = False
        self.io_blocked_read = False

    def descriptor(self) -> c_int:
        return self.h2.stream.descriptor()

    def deadline_expired(self, now_ns: Int64) -> Bool:
        return (
            self.active_sid != 0
            and self.ctx.timeout_ns > 0
            and now_ns - self.call_start_ns >= self.ctx.timeout_ns
        )

    def deadline_remaining(self, now_ns: Int64) -> Int64:
        if self.active_sid == 0 or self.ctx.timeout_ns <= 0:
            return -1
        return self.ctx.timeout_ns - (now_ns - self.call_start_ns)

    def idle_remaining(self, now_ns: Int64, timeout_ms: Int) -> Int64:
        return Int64(timeout_ms) * 1_000_000 - (now_ns - self.last_activity_ns)

    def incomplete_request_remaining(
        self, now_ns: Int64, timeout_ms: Int
    ) raises -> Int64:
        if self.has_incomplete_request():
            var start = (
                self.call_start_ns if self.active_sid != 0 else self.accepted_ns
            )
            return Int64(timeout_ms) * 1_000_000 - (now_ns - start)
        return -1

    def has_incomplete_request(self) raises -> Bool:
        if self.active_sid == 0:
            return not self.served_request
        return (
            self.active_sid in self.h2.streams
            and not self.h2.streams[self.active_sid].end_stream
        )

    def reset_call(mut self):
        self.served_request = True
        self.active_sid = 0
        self.call_start_ns = 0
        self.ctx = ServerContext(self.peer_certificate)
        self.prefix = List[Byte]()
        self.request = List[Byte]()
        self.request_length = -1
        self.request_complete = False
        self.discard_request = False
        self.request_error = None
        self.unsupported_media_type = False
        self.handler_called = False
        self.response = List[Byte]()
        self.response_offset = 0
        self.response_status = Status.ok()
        self.headers_queued = False
        self.trailers_queued = False


def _find_header(
    fields: Span[HeaderField, _], name: StringSpan
) -> Optional[String]:
    for field in fields:
        if field.name == name:
            return field.value.copy()
    return None


def _grpc_content_type(value: String) -> Bool:
    return value == "application/grpc" or (
        value.startswith("application/grpc+")
        and value.byte_length()
        > StaticString("application/grpc+").byte_length()
    )


def _initial_headers(ctx: ServerContext) -> List[HeaderField]:
    var headers = List[HeaderField]()
    headers.append(HeaderField(name=String(":status"), value=String("200")))
    headers.append(
        HeaderField(
            name=String("content-type"),
            value=String("application/grpc+proto"),
        )
    )
    for entry in ctx.response_metadata.entries:
        headers.append(entry.copy())
    return headers^


def _status_headers(
    status: Status, ctx: ServerContext, *, trailers_only: Bool
) -> List[HeaderField]:
    var headers = List[HeaderField]()
    if trailers_only:
        headers.append(HeaderField(name=String(":status"), value=String("200")))
        headers.append(
            HeaderField(
                name=String("content-type"),
                value=String("application/grpc+proto"),
            )
        )
    headers.append(
        HeaderField(name=String("grpc-status"), value=String(status.code))
    )
    if status.message.byte_length() > 0:
        headers.append(
            HeaderField(
                name=String("grpc-message"),
                value=percent_encode_message(status.message),
            )
        )
    if len(status.details_bin) > 0 and status.code != StatusCode.OK:
        headers.append(
            HeaderField(
                name=String("grpc-status-details-bin"),
                value=encode_bin_value(Span(status.details_bin)),
            )
        )
    for entry in ctx.response_trailers.entries:
        headers.append(entry.copy())
    return headers^


struct PollingServer(Movable):
    """A bounded single-threaded Poller server for unary gRPC methods.

    Socket I/O progresses concurrently across connections. Unary handlers
    execute one at a time on the event-loop thread, so they should be short
    and non-blocking. The default constructor serves h2c; `tls` performs
    non-blocking TLS handshakes and requires `h2` ALPN. Use `Server` for
    streaming RPCs. Set `config.keepalive_interval_ns` to send HTTP/2
    keepalive PINGs from the event loop; the default of 0 sends none.
    """

    var host: String
    """Host or address to bind, such as `127.0.0.1`."""
    var port: UInt16
    """TCP port to bind; 0 selects an ephemeral port."""
    var config: PollingServerConfig
    """Resource and fairness limits used by the event loop."""
    var routes: Dict[String, _PollingRoute]
    """Routing table from full method path to unary handler."""
    var _tls_context: Optional[TLSContext]
    """Reusable TLS context; None selects plaintext h2c."""

    def __init__(
        out self,
        host: StringSpan,
        port: UInt16,
        config: PollingServerConfig = PollingServerConfig(),
    ) raises:
        """Constructs an h2c server with an empty routing table.

        Args:
            host: Host or address to bind.
            port: TCP port to bind; 0 selects an ephemeral port.
            config: Resource and fairness limits for the event loop.

        Raises:
            If a configured limit is outside its supported range.
        """
        config.validate()
        self.host = String(host)
        self.port = port
        self.config = config.copy()
        self.routes = Dict[String, _PollingRoute]()
        self._tls_context = None

    @staticmethod
    def tls(
        host: StringSpan,
        port: UInt16,
        cert_chain_pem: String,
        key_pem: String,
        config: PollingServerConfig = PollingServerConfig(),
        *,
        client_ca_file: String = "",
        require_client_cert: Bool = False,
    ) raises -> PollingServer:
        """Constructs a TLS polling server that accepts only `h2` ALPN.

        The server uses one certificate chain for every connection. It can
        require a client certificate before HTTP/2 request dispatch.

        Args:
            host: Host or address to bind.
            port: TCP port to bind; 0 selects an ephemeral port.
            cert_chain_pem: Path to the PEM certificate chain.
            key_pem: Path to the matching PEM private key.
            config: Resource, fairness, and timeout limits.
            client_ca_file: Path to a PEM bundle of client trust anchors.
                Must be paired with `require_client_cert=True`.
            require_client_cert: Require and verify a client certificate
                against `client_ca_file` before HTTP/2 dispatch.

        Returns:
            A polling server configured for TLS.

        Raises:
            If a limit, certificate, key, or TLS context is invalid.
        """
        var out = PollingServer(host, port, config)
        out._tls_context = TLSContext.server(
            cert_chain_pem,
            key_pem,
            alpn=[String(H2_ALPN)],
            client_ca_file=client_ca_file,
            require_client_cert=require_client_cert,
        )
        return out^

    def _validate_route(self, path: StringSpan) raises:
        var owned = String(path)
        var parts = owned.split("/")
        if (
            len(parts) != 3
            or parts[0].byte_length() != 0
            or parts[1].byte_length() == 0
            or parts[2].byte_length() == 0
        ):
            raise Error("grpc: method path must be '/service/method'")
        if owned in self.routes:
            raise Error("grpc: duplicate method path " + owned)

    def register_unary_bytes[
        handler: UnaryBytesHandler
    ](mut self, path: StringSpan) raises:
        """Registers one byte-level unary method.

        Duplicate or malformed method paths are rejected so registration
        cannot silently replace a live handler.

        Parameters:
            handler: Handler taking request bytes and a call context, and
                returning response bytes.

        Args:
            path: Full method path, such as `/echo.Echo/Say`.

        Raises:
            If the path is malformed or already registered.
        """
        self._validate_route(path)
        self.routes[String(path)] = _PollingRoute(handler=handler)

    def register_unary[
        Req: ProtoMessage,
        Resp: ProtoMessage,
        //,
        handler: def(Req, mut ServerContext) raises thin -> Resp,
    ](mut self, path: StringSpan) raises:
        """Registers a typed unary method.

        Parameters:
            Req: Request message type inferred from the handler.
            Resp: Response message type inferred from the handler.
            handler: Handler mapping a request message to a response message.

        Args:
            path: Full method path, such as `/echo.Echo/Say`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(
            request: List[Byte], mut ctx: ServerContext
        ) raises -> List[Byte]:
            var req = decode[Req](Span(request))
            var resp = handler(req, ctx)
            return encode(resp)

        self.register_unary_bytes[wrapped](path)

    def _set_request_error(
        self, mut connection: _PollingConnection, status: Status
    ):
        if not connection.request_error:
            connection.request_error = status.copy()
        connection.discard_request = True
        connection.request = List[Byte]()

    def _start_call(
        self, mut connection: _PollingConnection, sid: UInt32
    ) raises:
        var headers = connection.h2.streams[sid].headers.copy()
        connection.active_sid = sid
        connection.call_start_ns = Int64(monotonic())
        connection.ctx = ServerContext(connection.peer_certificate)

        var method = _find_header(Span(headers), ":method")
        if not method or method.value() != "POST":
            self._set_request_error(
                connection,
                Status(
                    code=StatusCode.INTERNAL,
                    message=String("gRPC requests require POST"),
                ),
            )

        var content_type = _find_header(Span(headers), "content-type")
        if not content_type or not _grpc_content_type(content_type.value()):
            connection.unsupported_media_type = True
            connection.discard_request = True
            connection.request = List[Byte]()

        var path = _find_header(Span(headers), ":path")
        if path:
            connection.ctx.path = path.value()
        connection.ctx.metadata = Metadata.from_headers(Span(headers))

        var timeout = _find_header(Span(headers), "grpc-timeout")
        if timeout:
            try:
                connection.ctx.timeout_ns = decode_timeout(timeout.value())
            except:
                self._set_request_error(
                    connection,
                    Status(
                        code=StatusCode.INTERNAL,
                        message=String("malformed grpc-timeout header"),
                    ),
                )

        if connection.ctx.path not in self.routes:
            self._set_request_error(
                connection,
                Status(
                    code=StatusCode.UNIMPLEMENTED,
                    message=String("unknown method"),
                ),
            )

    def _discover_stream(self, mut connection: _PollingConnection) raises:
        if connection.active_sid != 0:
            return
        var ids = connection.h2.stream_ids.copy()
        for sid in ids:
            if connection.h2.streams[sid].reset_code:
                _ = connection.h2.retire_stream(sid)
                continue
            if connection.h2.streams[sid].headers_done:
                self._start_call(connection, sid)
                return

    def _consume_request(
        self, mut connection: _PollingConnection
    ) raises -> Bool:
        """Consumes one bounded chunk and reports whether output was queued."""
        var sid = connection.active_sid
        if sid == 0 or connection.h2.buffered_data_len(sid) == 0:
            return False

        if connection.request_complete:
            self._set_request_error(
                connection,
                Status(
                    code=StatusCode.INTERNAL,
                    message=String("multiple messages in unary request"),
                ),
            )

        if connection.discard_request:
            var available = connection.h2.buffered_data_len(sid)
            _ = connection.h2.take_buffered_data(sid, available)
            return connection.h2.pending_output_len() > 0

        if len(connection.prefix) < GRPC_MESSAGE_PREFIX_LEN:
            var need = GRPC_MESSAGE_PREFIX_LEN - len(connection.prefix)
            var take = min(need, connection.h2.buffered_data_len(sid))
            connection.prefix.extend(
                Span(connection.h2.take_buffered_data(sid, take))
            )
            if len(connection.prefix) == GRPC_MESSAGE_PREFIX_LEN:
                var flag = connection.prefix[0]
                connection.request_length = Int(
                    get_u32_be(Span(connection.prefix), 1)
                )
                if flag > 1:
                    self._set_request_error(
                        connection,
                        Status(
                            code=StatusCode.INTERNAL,
                            message=String("invalid compressed flag"),
                        ),
                    )
                elif flag == 1:
                    self._set_request_error(
                        connection,
                        Status(
                            code=StatusCode.INTERNAL,
                            message=String("compressed messages not supported"),
                        ),
                    )
                elif connection.request_length > self.config.max_message_size:
                    self._set_request_error(
                        connection,
                        Status(
                            code=StatusCode.RESOURCE_EXHAUSTED,
                            message=String("request message exceeds max size"),
                        ),
                    )
                elif connection.request_length == 0:
                    connection.request_complete = True
            return connection.h2.pending_output_len() > 0

        var need = connection.request_length - len(connection.request)
        var take = min(need, connection.h2.buffered_data_len(sid))
        if take > 0:
            connection.request.extend(
                Span(connection.h2.take_buffered_data(sid, take))
            )
        if len(connection.request) == connection.request_length:
            connection.request_complete = True
        return connection.h2.pending_output_len() > 0

    def _request_ended(self, connection: _PollingConnection) raises -> Bool:
        if connection.active_sid == 0:
            return False
        return connection.h2.streams[connection.active_sid].end_stream

    def _invoke_handler(mut self, mut connection: _PollingConnection) raises:
        if connection.handler_called:
            return
        connection.handler_called = True
        if connection.request_error:
            connection.response_status = connection.request_error.value().copy()
            return
        if not connection.request_complete:
            connection.response_status = Status(
                code=StatusCode.INTERNAL,
                message=String("truncated unary request"),
            )
            return
        if connection.deadline_expired(Int64(monotonic())):
            connection.response_status = Status(
                code=StatusCode.DEADLINE_EXCEEDED,
                message=String("Deadline Exceeded"),
            )
            return

        var handler = self.routes[connection.ctx.path].handler
        try:
            var payload = handler(connection.request^, connection.ctx)
            if connection.ctx.abort_status:
                connection.response_status = (
                    connection.ctx.abort_status.value().copy()
                )
            elif len(payload) > self.config.max_message_size:
                connection.response_status = Status(
                    code=StatusCode.RESOURCE_EXHAUSTED,
                    message=String("response message exceeds max size"),
                )
            else:
                connection.response = frame_message(Span(payload))
                connection.response_status = Status.ok()
        except error:
            connection.response_status = Status(
                code=StatusCode.UNKNOWN, message=String("handler failed")
            )

    def _queue_http_415(self, mut connection: _PollingConnection) raises:
        var headers = [HeaderField(name=String(":status"), value=String("415"))]
        connection.h2.queue_headers(
            connection.active_sid, Span(headers), end_stream=True
        )
        connection.headers_queued = True
        connection.trailers_queued = True

    def _queue_response_step(self, mut connection: _PollingConnection) raises:
        if connection.deadline_expired(Int64(monotonic())):
            connection.response = List[Byte]()
            connection.response_offset = 0
            connection.handler_called = True
            connection.response_status = Status(
                code=StatusCode.DEADLINE_EXCEEDED,
                message=String("Deadline Exceeded"),
            )
            # The timeout has been converted into a terminal response. Do not
            # keep waking the poller while the peer finishes its request side.
            connection.ctx.timeout_ns = 0

        if connection.unsupported_media_type:
            if not connection.trailers_queued:
                self._queue_http_415(connection)
            return

        if connection.response_status.code != StatusCode.OK:
            if not connection.trailers_queued:
                var trailers = _status_headers(
                    connection.response_status,
                    connection.ctx,
                    trailers_only=True,
                )
                connection.h2.queue_headers(
                    connection.active_sid, Span(trailers), end_stream=True
                )
                connection.headers_queued = True
                connection.trailers_queued = True
            return

        if not connection.headers_queued:
            var headers = _initial_headers(connection.ctx)
            connection.h2.queue_headers(
                connection.active_sid, Span(headers), end_stream=False
            )
            connection.headers_queued = True
            return

        if connection.response_offset < len(connection.response):
            var consumed = connection.h2.queue_data(
                connection.active_sid,
                Span(connection.response)[
                    connection.response_offset : len(connection.response)
                ],
                end_stream=False,
            )
            connection.response_offset += consumed
            return

        if not connection.trailers_queued:
            var trailers = _status_headers(
                connection.response_status,
                connection.ctx,
                trailers_only=False,
            )
            connection.h2.queue_headers(
                connection.active_sid, Span(trailers), end_stream=True
            )
            connection.trailers_queued = True

    def _move_http2_output(
        self, mut connection: _PollingConnection
    ) raises -> Bool:
        if len(connection.output) != 0:
            return True
        if not _can_move_http2_output(
            connection.io_blocked_read, connection.io_wants_write
        ):
            return False
        if connection.h2.pending_output_len() == 0:
            return False
        var output = connection.h2.take_pending_output()
        if len(output) > self.config.max_pending_output_size:
            raise Error("grpc: pending output invariant exceeded")
        connection.output.replace(output^)
        # SSL_read WANT_READ accepts no application bytes. An independent
        # response, such as an expired RPC deadline, may proceed through
        # SSL_write without waiting for more peer input.
        if (
            connection.io_blocked_read
            and connection.io_wants_read
            and not connection.io_wants_write
        ):
            connection.io_wants_read = False
            connection.io_blocked_read = False
        return True

    def _drive_connection(mut self, mut connection: _PollingConnection) raises:
        # Never queue HTTP/2 output behind an unsent socket suffix. This is
        # the per-connection output bound and partial-write invariant.
        if self._move_http2_output(connection):
            return

        self._discover_stream(connection)
        if connection.active_sid == 0:
            return

        if connection.h2.streams[connection.active_sid].reset_code:
            _ = connection.h2.retire_stream(connection.active_sid)
            connection.reset_call()
            return

        while connection.h2.buffered_data_len(connection.active_sid) > 0:
            if self._consume_request(connection):
                _ = self._move_http2_output(connection)
                return

        if connection.deadline_expired(Int64(monotonic())):
            connection.response = List[Byte]()
            connection.response_offset = 0
            connection.response_status = Status(
                code=StatusCode.DEADLINE_EXCEEDED,
                message=String("Deadline Exceeded"),
            )
            connection.handler_called = True
            connection.discard_request = True
            connection.ctx.timeout_ns = 0
            self._queue_response_step(connection)
            _ = self._move_http2_output(connection)
            return

        if not self._request_ended(connection):
            return

        self._invoke_handler(connection)
        self._queue_response_step(connection)
        if self._move_http2_output(connection):
            return

        if connection.trailers_queued:
            var sid = connection.active_sid
            if connection.h2.retire_stream(sid):
                connection.reset_call()

    def _read_ready(mut self, mut connection: _PollingConnection) raises:
        connection.resume_read = False
        var remaining_bytes = self.config.max_read_bytes_per_event
        var remaining_frames = self.config.max_frames_per_event
        while remaining_bytes > 0 and len(connection.output) == 0:
            if connection.h2.pending_input_frame_count() > 0:
                var processed = connection.h2.feed_input(
                    Span(List[Byte]()), remaining_frames
                )
                remaining_frames -= processed
                self._drive_connection(connection)
                if (
                    remaining_frames == 0
                    or len(connection.output) > 0
                    or connection.h2.pending_input_frame_count() > 0
                ):
                    return

            var chunk_size = min(remaining_bytes, 64 * 1024)
            var chunk = List[Byte](length=chunk_size, fill=0)
            try:
                var count = connection.h2.stream.read(chunk)
                if count == 0:
                    connection.resume_read = False
                    connection.io_wants_read = False
                    connection.io_wants_write = False
                    connection.io_blocked_read = False
                    connection.peer_eof = True
                    connection.close_after_flush = True
                    return
                connection.last_activity_ns = Int64(monotonic())
                connection.h2.touch_keepalive(connection.last_activity_ns)
                remaining_bytes -= count
                connection.resume_read = True
                connection.io_wants_read = False
                connection.io_wants_write = False
                connection.io_blocked_read = False
                var processed = connection.h2.feed_input(
                    Span(chunk), remaining_frames
                )
                remaining_frames -= processed
                self._drive_connection(connection)
                if remaining_frames == 0 or len(connection.output) > 0:
                    return
            except error:
                if is_would_block(error):
                    connection.resume_read = False
                    connection.io_wants_read = connection.h2.stream.wants_read()
                    connection.io_wants_write = (
                        connection.h2.stream.wants_write()
                    )
                    connection.io_blocked_read = (
                        connection.io_wants_read or connection.io_wants_write
                    )
                    if (
                        connection.h2.stream._is_secure()
                        and not connection.io_blocked_read
                    ):
                        raise Error("grpc: blocked read has no TLS direction")
                    return
                if connection.h2.pending_output_len() > 0:
                    connection.close_after_flush = True
                    _ = self._move_http2_output(connection)
                    return
                raise error

    def _write_ready(self, mut connection: _PollingConnection) raises:
        var remaining = self.config.max_write_bytes_per_event
        while remaining > 0 and len(connection.output) > 0:
            var offered = min(remaining, len(connection.output))
            var start = connection.output.offset
            var end = start + offered
            try:
                var count = connection.h2.stream.write_some(
                    Span(connection.output.data)[start:end]
                )
                if count > 0:
                    connection.last_activity_ns = Int64(monotonic())
                    connection.h2.touch_keepalive(connection.last_activity_ns)
                    connection.io_wants_read = False
                    connection.io_wants_write = False
                    connection.io_blocked_read = False
                connection.output.advance(count)
                remaining -= count
            except error:
                if is_would_block(error):
                    connection.io_wants_read = connection.h2.stream.wants_read()
                    connection.io_wants_write = (
                        connection.h2.stream.wants_write()
                    )
                    connection.io_blocked_read = False
                    if (
                        connection.h2.stream._is_secure()
                        and not connection.io_wants_read
                        and not connection.io_wants_write
                    ):
                        raise Error("grpc: blocked write has no TLS direction")
                    return
                raise error

    def _connection_readable_interest(
        self, connection: _PollingConnection
    ) raises -> Bool:
        if connection.io_wants_read or connection.io_wants_write:
            return connection.io_wants_read
        return len(connection.output) == 0 and not connection.peer_eof

    def _connection_writable_interest(
        self, connection: _PollingConnection
    ) raises -> Bool:
        if connection.io_wants_read or connection.io_wants_write:
            return connection.io_wants_write
        return len(connection.output) > 0

    def _update_connection_interest(
        self,
        mut poller: Poller,
        fd: c_int,
        connection: _PollingConnection,
    ) raises:
        poller.modify(
            fd,
            readable=self._connection_readable_interest(connection),
            writable=self._connection_writable_interest(connection),
        )

    def _poll_timeout_ms(
        self,
        connections: Dict[c_int, _PollingConnection],
        handshakes: Dict[c_int, _PollingHandshake],
        fds: List[c_int],
    ) raises -> Int:
        var now = Int64(monotonic())
        var closest: Int64 = -1
        for fd in fds:
            if fd in handshakes:
                var handshake = handshakes[fd].remaining(
                    now, self.config.tls_handshake_timeout_ms
                )
                if handshake <= 0:
                    return 0
                if closest < 0 or handshake < closest:
                    closest = handshake
                continue
            if fd not in connections:
                continue
            if (
                connections[fd].h2.pending_input_frame_count() > 0
                and len(connections[fd].output) == 0
            ):
                return 0
            if connections[fd].resume_read and len(connections[fd].output) == 0:
                return 0
            var idle = connections[fd].idle_remaining(
                now, self.config.idle_timeout_ms
            )
            if closest < 0 or idle < closest:
                closest = idle
            var incomplete = connections[fd].incomplete_request_remaining(
                now, self.config.incomplete_request_timeout_ms
            )
            if connections[fd].has_incomplete_request():
                if incomplete <= 0:
                    return 0
                if closest < 0 or incomplete < closest:
                    closest = incomplete
            # An unsent suffix already has writable interest. Deferring the
            # RPC deadline until that suffix moves avoids a zero-timeout spin;
            # no later response bytes are queued before the next check.
            var remaining: Int64 = -1
            if len(connections[fd].output) == 0:
                remaining = connections[fd].deadline_remaining(now)
            if remaining >= 0 and (closest < 0 or remaining < closest):
                closest = remaining
            if self.config.keepalive_interval_ns > 0:
                if (
                    closest < 0
                    or self.config.keepalive_interval_ns < closest
                ):
                    closest = self.config.keepalive_interval_ns
        if closest < 0:
            return -1
        if closest <= 0:
            return 0
        return Int((closest + 999_999) // 1_000_000)

    def _remove_fd(self, mut fds: List[c_int], target: c_int):
        var remaining = List[c_int](capacity=len(fds))
        for fd in fds:
            if fd != target:
                remaining.append(fd)
        fds = remaining^

    def _contains_fd(self, fds: List[c_int], target: c_int) -> Bool:
        for fd in fds:
            if fd == target:
                return True
        return False

    def serve(mut self) raises:
        """Runs the h2c or TLS event loop forever.

        The bound address is printed after the listener is registered. The
        method does not return during normal operation.

        Raises:
            If listener setup, polling, socket I/O, or protocol processing
            fails.
        """
        var listener = TCPListener(self.host, self.port)
        listener.set_nonblocking(True)
        var listener_fd = listener.descriptor()
        var poller = Poller()
        poller.register(listener_fd, readable=True, writable=False)
        var listener_registered = True
        var connections = Dict[c_int, _PollingConnection]()
        var handshakes = Dict[c_int, _PollingHandshake]()
        var fds = List[c_int]()
        print(
            "grpc-mojo polling server listening on ",
            self.host,
            ":",
            listener.local_port,
            sep="",
        )

        while True:
            var events = _coalesce_poll_events(
                Span(
                    poller.wait(
                        self._poll_timeout_ms(connections, handshakes, fds)
                    )
                )
            )
            var accept_ready = False
            var close_fds = List[c_int]()
            var closing = Dict[c_int, _PollingConnection]()
            var processed_fds = List[c_int]()
            var handshake_steps = 0

            # Existing descriptors are processed before any close or accept.
            # This prevents a stale duplicate kqueue event from reaching a
            # newly accepted connection that reused the same descriptor.
            for event in events:
                if event.fd == listener_fd:
                    accept_ready |= event.readable
                    continue
                if event.fd in handshakes:
                    if (
                        handshake_steps
                        >= self.config.max_handshake_steps_per_event
                    ):
                        continue
                    handshake_steps += 1
                    processed_fds.append(event.fd)
                    var handshake = handshakes.pop(event.fd)
                    var close_now = event.error
                    try:
                        if (
                            handshake.remaining(
                                Int64(monotonic()),
                                self.config.tls_handshake_timeout_ms,
                            )
                            <= 0
                        ):
                            raise Error("grpc: TLS handshake timed out")
                        if event.readable or event.writable:
                            if handshake.tls.advance():
                                try:
                                    var connection = handshake^.finish(
                                        self.config
                                    )
                                    # Consume plaintext already buffered by
                                    # libssl before waiting on the kernel again.
                                    self._read_ready(connection)
                                    self._drive_connection(connection)
                                    self._update_connection_interest(
                                        poller, event.fd, connection
                                    )
                                    connections[event.fd] = connection^
                                except:
                                    close_fds.append(event.fd)
                                continue
                        if event.hangup:
                            close_now = True
                        if not close_now:
                            var wants_read = handshake.tls.wants_read()
                            var wants_write = handshake.tls.wants_write()
                            if not wants_read and not wants_write:
                                raise Error(
                                    "grpc: TLS handshake made no progress"
                                )
                            poller.modify(
                                event.fd,
                                readable=wants_read,
                                writable=wants_write,
                            )
                    except:
                        close_now = True
                    if close_now:
                        close_fds.append(event.fd)
                    else:
                        handshakes[event.fd] = handshake^
                    continue
                if event.fd not in connections:
                    continue
                processed_fds.append(event.fd)
                var connection = connections.pop(event.fd)
                var close_now = False
                try:
                    if event.readable or event.writable:
                        if (
                            connection.io_wants_read
                            or connection.io_wants_write
                        ) and connection.io_blocked_read:
                            self._read_ready(connection)
                        elif len(connection.output) > 0:
                            self._write_ready(connection)
                        else:
                            self._read_ready(connection)
                    self._drive_connection(connection)
                    if event.hangup and not event.readable:
                        connection.close_after_flush = True
                    if event.error and not event.readable:
                        close_now = True
                    if (
                        connection.close_after_flush
                        and len(connection.output) == 0
                        and connection.h2.pending_input_frame_count() == 0
                    ):
                        close_now = True
                    if not close_now:
                        self._update_connection_interest(
                            poller, event.fd, connection
                        )
                except:
                    close_now = True
                if close_now:
                    closing[event.fd] = connection^
                    close_fds.append(event.fd)
                else:
                    connections[event.fd] = connection^

            # Handshake and RPC deadlines must wake even when a socket itself
            # produced no readiness event.
            var now = Int64(monotonic())
            for fd in fds:
                if fd in handshakes:
                    if (
                        handshakes[fd].remaining(
                            now, self.config.tls_handshake_timeout_ms
                        )
                        <= 0
                    ):
                        close_fds.append(fd)
                    continue
                if fd not in connections:
                    continue
                var idle_expired = (
                    connections[fd].idle_remaining(
                        now, self.config.idle_timeout_ms
                    )
                    <= 0
                )
                var incomplete = connections[fd].incomplete_request_remaining(
                    now, self.config.incomplete_request_timeout_ms
                )
                if idle_expired or (
                    connections[fd].has_incomplete_request() and incomplete <= 0
                ):
                    var connection = connections.pop(fd)
                    closing[fd] = connection^
                    close_fds.append(fd)
                    continue
                if connections[fd].deadline_expired(now):
                    var connection = connections.pop(fd)
                    try:
                        self._drive_connection(connection)
                        self._update_connection_interest(poller, fd, connection)
                        connections[fd] = connection^
                    except:
                        closing[fd] = connection^
                        close_fds.append(fd)
                        continue
                if (
                    self.config.keepalive_interval_ns > 0
                    and fd in connections
                ):
                    var connection = connections.pop(fd)
                    try:
                        if (
                            connection.h2.input_preface_complete()
                            and connection.h2.maybe_keepalive_ping(
                                now, self.config.keepalive_interval_ns
                            )
                        ):
                            _ = self._move_http2_output(connection)
                            self._update_connection_interest(
                                poller, fd, connection
                            )
                        connections[fd] = connection^
                    except:
                        closing[fd] = connection^
                        close_fds.append(fd)

            # A frame or plaintext budget may leave work buffered above the
            # kernel. Resume it once on the next turn without waiting for a
            # socket event that may never arrive.
            for fd in fds:
                if (
                    fd not in connections
                    or self._contains_fd(processed_fds, fd)
                    or len(connections[fd].output) != 0
                    or (
                        connections[fd].h2.pending_input_frame_count() == 0
                        and not connections[fd].resume_read
                    )
                ):
                    continue
                var connection = connections.pop(fd)
                var close_now = False
                try:
                    if connection.h2.pending_input_frame_count() > 0:
                        _ = connection.h2.feed_input(
                            Span(List[Byte]()), self.config.max_frames_per_event
                        )
                    if connection.resume_read and len(connection.output) == 0:
                        self._read_ready(connection)
                    self._drive_connection(connection)
                    if (
                        connection.close_after_flush
                        and len(connection.output) == 0
                        and connection.h2.pending_input_frame_count() == 0
                    ):
                        close_now = True
                except:
                    if connection.h2.pending_output_len() > 0:
                        connection.close_after_flush = True
                        try:
                            _ = self._move_http2_output(connection)
                        except:
                            close_now = True
                    else:
                        close_now = True
                if not close_now:
                    try:
                        self._update_connection_interest(poller, fd, connection)
                    except:
                        close_now = True
                if close_now:
                    closing[fd] = connection^
                    close_fds.append(fd)
                else:
                    connections[fd] = connection^

            for fd in close_fds:
                if fd in handshakes:
                    _ = handshakes.pop(fd)
                if fd in connections:
                    var connection = connections.pop(fd)
                    closing[fd] = connection^
                if fd in closing:
                    var connection = closing.pop(fd)
                    connection.h2.close()
                self._remove_fd(fds, fd)

            if (
                not listener_registered
                and len(fds) < self.config.max_connections
                and (
                    not self._tls_context
                    or len(handshakes) < self.config.max_pending_handshakes
                )
            ):
                poller.register(listener_fd, readable=True, writable=False)
                listener_registered = True

            if accept_ready and listener_registered:
                var accepts = 0
                while (
                    len(fds) < self.config.max_connections
                    and accepts < self.config.max_accepts_per_event
                    and (
                        not self._tls_context
                        or (
                            len(handshakes) < self.config.max_pending_handshakes
                            and handshake_steps
                            < self.config.max_handshake_steps_per_event
                        )
                    )
                ):
                    try:
                        var tcp = listener.accept()
                        tcp.set_nonblocking(True)
                        accepts += 1
                        if self._tls_context:
                            handshake_steps += 1
                            try:
                                var accepted_ns = Int64(monotonic())
                                var handshake = _PollingHandshake(
                                    tls=self._tls_context.value().start_accept(
                                        tcp^
                                    ),
                                    accepted_ns=accepted_ns,
                                )
                                if (
                                    handshake.remaining(
                                        Int64(monotonic()),
                                        self.config.tls_handshake_timeout_ms,
                                    )
                                    <= 0
                                ):
                                    raise Error("grpc: TLS handshake timed out")
                                var fd = handshake.tls.descriptor()
                                if handshake.tls.advance():
                                    try:
                                        var connection = handshake^.finish(
                                            self.config
                                        )
                                        poller.register(
                                            fd, readable=True, writable=False
                                        )
                                        # A completed handshake may leave
                                        # HTTP/2 bytes buffered inside libssl.
                                        self._read_ready(connection)
                                        self._drive_connection(connection)
                                        self._update_connection_interest(
                                            poller, fd, connection
                                        )
                                        connections[fd] = connection^
                                        fds.append(fd)
                                    except:
                                        pass
                                else:
                                    var wants_read = handshake.tls.wants_read()
                                    var wants_write = (
                                        handshake.tls.wants_write()
                                    )
                                    if not wants_read and not wants_write:
                                        raise Error(
                                            "grpc: TLS handshake made no"
                                            " progress"
                                        )
                                    poller.register(
                                        fd,
                                        readable=wants_read,
                                        writable=wants_write,
                                    )
                                    handshakes[fd] = handshake^
                                    fds.append(fd)
                            except:
                                # A malformed or incompatible handshake does
                                # not stop the listener.
                                pass
                        else:
                            var transport = GrpcTransport.plaintext(tcp^)
                            var connection = _PollingConnection(
                                transport^, self.config
                            )
                            var fd = connection.descriptor()
                            poller.register(fd, readable=True, writable=False)
                            connections[fd] = connection^
                            fds.append(fd)
                    except error:
                        if is_would_block(error):
                            break
                        raise error
                if len(fds) == self.config.max_connections or (
                    self._tls_context
                    and len(handshakes) == self.config.max_pending_handshakes
                ):
                    poller.unregister(listener_fd)
                    listener_registered = False
