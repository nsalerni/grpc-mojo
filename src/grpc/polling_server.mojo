# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""A bounded readiness-driven server for h2c, TLS, or Unix RPCs.

`PollingServer` is an opt-in alternative to the blocking `Server`. It uses
`mojo-net`'s kqueue/epoll `Poller` so one thread can make progress on many
connections. Handler functions still run serially on the event-loop
thread. Unary handlers should return promptly. Streaming handlers run to
completion through a temporary blocking `ServerCall` and stall other
connections while they run — the same contract as unary handlers and as
the blocking `Server`. Long-lived streams belong on process-per-connection
`Server` until Mojo ships threads or async. `request_stop()` and optional
SIGTERM/SIGINT handlers send GOAWAY and drain live streams on this thread.
TLS handshakes advance through the same Poller and strictly require the
`h2` ALPN token. Unix sockets are plaintext only. Optional HTTP/2
keepalive PINGs are driven from the Poller loop when
`keepalive_interval_ns` is positive.

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

from std.ffi import OwnedDLHandle, c_int
from std.time import monotonic

from hpack import HeaderField
from h2 import (
    DEFAULT_WINDOW_SIZE,
    ERR_CANCEL,
    H2_ALPN,
    Http2Connection,
    get_u32_be,
)
from net import (
    PollEvent,
    Poller,
    TCPListener,
    UnixListener,
    Wakeup,
    is_would_block,
)
from proto import ProtoMessage, decode, encode
from tls import PeerCertificate, TLSContext, TLSHandshake

from .framing import GRPC_MESSAGE_PREFIX_LEN, frame_message
from .health import HEALTH_CHECK_PATH, Health
from .metadata import Metadata, encode_bin_value
from .server import (
    MethodKind,
    RawHandler,
    Route,
    ServerCall,
    ServerContext,
    UnaryBytesHandler,
)
from .status import Status, StatusCode, percent_encode_message
from .stop_signals import install_wakeup_signal_handlers
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
    var initial_window_size: UInt32
    """Per-stream receive window advertised as SETTINGS_INITIAL_WINDOW_SIZE."""
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
    var shutdown_drain_timeout_ms: Int
    """Maximum time after GOAWAY to wait for live streams to finish."""

    def __init__(
        out self,
        *,
        max_connections: Int = 128,
        max_accepts_per_event: Int = 32,
        max_pending_handshakes: Int = 32,
        max_handshake_steps_per_event: Int = 32,
        max_message_size: Int = 4 * 1024 * 1024,
        initial_window_size: UInt32 = DEFAULT_WINDOW_SIZE,
        max_read_bytes_per_event: Int = 64 * 1024,
        max_frames_per_event: Int = 64,
        max_write_bytes_per_event: Int = 64 * 1024,
        max_pending_output_size: Int = 64 * 1024,
        idle_timeout_ms: Int = 300_000,
        incomplete_request_timeout_ms: Int = 30_000,
        tls_handshake_timeout_ms: Int = 10_000,
        keepalive_interval_ns: Int64 = 0,
        shutdown_drain_timeout_ms: Int = 10_000,
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
            initial_window_size: Per-stream receive window advertised as
                SETTINGS_INITIAL_WINDOW_SIZE. Default 65,535. Must not
                exceed 2^31-1.
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
                loop; there is no internal timer. The poll timeout uses
                time remaining since last activity, not the full interval.
            shutdown_drain_timeout_ms: After `request_stop` or a stop
                signal, maximum milliseconds to wait for accepted
                connections and TLS handshakes to close before forcing
                the remaining sockets shut.
        """
        self.max_connections = max_connections
        self.max_accepts_per_event = max_accepts_per_event
        self.max_pending_handshakes = max_pending_handshakes
        self.max_handshake_steps_per_event = max_handshake_steps_per_event
        self.max_message_size = max_message_size
        self.initial_window_size = initial_window_size
        self.max_read_bytes_per_event = max_read_bytes_per_event
        self.max_frames_per_event = max_frames_per_event
        self.max_write_bytes_per_event = max_write_bytes_per_event
        self.max_pending_output_size = max_pending_output_size
        self.idle_timeout_ms = idle_timeout_ms
        self.incomplete_request_timeout_ms = incomplete_request_timeout_ms
        self.tls_handshake_timeout_ms = tls_handshake_timeout_ms
        self.keepalive_interval_ns = keepalive_interval_ns
        self.shutdown_drain_timeout_ms = shutdown_drain_timeout_ms

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
        if self.initial_window_size > 0x7FFFFFFF:
            raise Error("grpc: initial_window_size exceeds 2^31-1")
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
        if self.shutdown_drain_timeout_ms <= 0:
            raise Error("grpc: shutdown_drain_timeout_ms must be positive")
        # Poller forwards milliseconds to epoll_wait/kqueue through c_int.
        var max_timeout_ms = 0x7FFFFFFF
        if (
            self.idle_timeout_ms > max_timeout_ms
            or self.incomplete_request_timeout_ms > max_timeout_ms
            or self.tls_handshake_timeout_ms > max_timeout_ms
            or self.shutdown_drain_timeout_ms > max_timeout_ms
        ):
            raise Error("grpc: timeout exceeds Poller millisecond range")
        if self.keepalive_interval_ns > Int64(max_timeout_ms) * 1_000_000:
            raise Error(
                "grpc: keepalive interval exceeds Poller millisecond range"
            )


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


def _keepalive_remaining_ns(
    interval_ns: Int64, last_activity_ns: Int64, now_ns: Int64
) -> Int64:
    """Nanoseconds until a keepalive PING is due; 0 if already due."""
    var ka = interval_ns - (now_ns - last_activity_ns)
    if ka < 0:
        return 0
    return ka


def _merge_poll_remaining(closest: Int64, remaining: Int64) -> Int64:
    """Keeps the soonest remaining timeout.

    `closest` of -1 means none yet. `remaining` below 0 is ignored so a
    disabled timer does not change the current bound. A due timeout of 0
    is not replaced by a later positive remaining.
    """
    if remaining < 0:
        return closest
    if closest < 0 or remaining < closest:
        return remaining
    return closest


def _can_schedule_keepalive(
    output_len: Int, pending_h2: Int, preface_complete: Bool
) -> Bool:
    """PINGs wait for preface and an empty output store.

    An unsent socket suffix already has writable interest. A zero poll
    timeout here would busy-spin the event loop until the peer drains.
    """
    return output_len == 0 and pending_h2 == 0 and preface_complete


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
        self.h2 = Http2Connection(
            transport^,
            is_client=False,
            initial_window_size=config.initial_window_size,
        )
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
    """A bounded single-threaded Poller server for gRPC methods.

    Socket I/O progresses concurrently across connections. Handlers
    execute one at a time on the event-loop thread. Unary handlers should
    be short. Streaming handlers temporarily switch the connection to
    blocking I/O and stall other connections until they return. The default
    constructor serves h2c; `tls` performs non-blocking TLS handshakes and
    requires `h2` ALPN; `unix` binds a Unix domain socket. Set
    `config.keepalive_interval_ns` to send HTTP/2 keepalive PINGs from the
    event loop; the default of 0 sends none. `request_stop()` or a stop
    signal sends GOAWAY and returns after live streams drain.
    """

    var host: String
    """Host or address to bind, such as `127.0.0.1`."""
    var port: UInt16
    """TCP port to bind; 0 selects an ephemeral port."""
    var config: PollingServerConfig
    """Resource and fairness limits used by the event loop."""
    var routes: Dict[String, _PollingRoute]
    """Routing table from full method path to unary handler."""
    var streaming_routes: Dict[String, Route]
    """Routing table from full method path to a streaming `ServerCall` handler."""
    var _tls_context: Optional[TLSContext]
    """Reusable TLS context; None selects plaintext h2c."""
    var _unix_path: Optional[String]
    """Unix domain socket path; None selects a TCP listener."""
    var _unix_remove_existing: Bool
    """Whether a Unix listener may remove an existing socket file."""
    var health: Optional[Health]
    """Health registry for Check; None leaves the method UNIMPLEMENTED."""
    var _stop_requested: Bool
    """True once `request_stop` or a stop signal has been observed."""
    var _wakeup: Optional[Wakeup]
    """Self-pipe used to wake `Poller.wait`; created on first use."""
    var _shutdown_deadline_ns: Int64
    """Monotonic deadline for GOAWAY drain; 0 means shutdown has not begun."""
    var _stop_shim: List[OwnedDLHandle]
    """Keeps the SIGTERM/SIGINT handler mapped after install."""

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
        self.streaming_routes = Dict[String, Route]()
        self._tls_context = None
        self._unix_path = None
        self._unix_remove_existing = False
        self.health = None
        self._stop_requested = False
        self._wakeup = None
        self._shutdown_deadline_ns = 0
        self._stop_shim = List[OwnedDLHandle]()

    @staticmethod
    def tls(
        host: StringSpan,
        port: UInt16,
        cert_chain_pem: StringSpan,
        key_pem: StringSpan,
        config: PollingServerConfig = PollingServerConfig(),
        *,
        client_ca_file: StringSpan = "",
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
            String(cert_chain_pem),
            String(key_pem),
            alpn=[String(H2_ALPN)],
            client_ca_file=String(client_ca_file),
            require_client_cert=require_client_cert,
        )
        return out^

    @staticmethod
    def unix(
        path: StringSpan,
        *,
        remove_existing: Bool = False,
        config: PollingServerConfig = PollingServerConfig(),
    ) raises -> PollingServer:
        """Constructs a plaintext polling server on a Unix domain socket.

        TLS is not supported on Unix listeners. An existing socket file
        is refused unless `remove_existing=True`.

        Args:
            path: Filesystem path to bind.
            remove_existing: Remove an existing socket file before bind.
                The default refuses to replace any existing path.
            config: Resource and fairness limits for the event loop.

        Returns:
            A polling server configured for the Unix domain socket.

        Raises:
            If a configured limit is outside its supported range.
        """
        var out = PollingServer("", 0, config)
        out._unix_path = String(path)
        out._unix_remove_existing = remove_existing
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
        if owned in self.routes or owned in self.streaming_routes:
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

    def register_server_streaming_bytes[
        handler: def(
            List[Byte], mut ServerContext, mut ServerCall
        ) raises thin -> None,
    ](mut self, path: StringSpan) raises:
        """Registers a byte-level server-streaming method.

        Parameters:
            handler: Handler taking one request payload and streaming
                responses through `ServerCall`.

        Args:
            path: Full method path, such as `/echo.Echo/Split`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            var msg = call.recv_bytes()
            if call.client_cancelled():
                call.trailers_sent = True
                return
            if not msg:
                call.finish(
                    Status(
                        code=StatusCode.INTERNAL,
                        message=String("missing request message"),
                    ),
                    ctx,
                )
                return
            handler(msg.take(), ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            call.finish_ok(ctx)

        self._validate_route(path)
        self.streaming_routes[String(path)] = Route(
            kind=MethodKind.SERVER_STREAMING, handler=wrapped
        )

    def register_server_streaming[
        Req: ProtoMessage,
        //,
        handler: def(
            Req, mut ServerContext, mut ServerCall
        ) raises thin -> None,
    ](mut self, path: StringSpan) raises:
        """Registers a typed server-streaming method.

        The wrapper waits for the full request, then the handler sends
        responses through `ServerCall`. The handler runs to completion on
        the event-loop thread and stalls other connections while it runs.

        Parameters:
            Req: Request message type inferred from the handler.
            handler: Handler receiving the request and a call handle.

        Args:
            path: Full method path, such as `/echo.Echo/Split`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(
            request: List[Byte], mut ctx: ServerContext, mut call: ServerCall
        ) raises:
            var req = decode[Req](Span(request))
            handler(req, ctx, call)

        self.register_server_streaming_bytes[wrapped](path)

    def register_client_streaming_bytes[
        handler: def(mut ServerContext, mut ServerCall) raises thin -> List[
            Byte
        ],
    ](mut self, path: StringSpan) raises:
        """Registers a byte-level client-streaming method.

        Parameters:
            handler: Handler draining requests through `ServerCall` and
                returning one response payload.

        Args:
            path: Full method path, such as `/echo.Echo/Join`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            var payload = handler(ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            if call.client_cancelled():
                call.trailers_sent = True
                return
            call.send_bytes(ctx, Span(payload))
            call.finish_ok(ctx)

        self._validate_route(path)
        self.streaming_routes[String(path)] = Route(
            kind=MethodKind.CLIENT_STREAMING, handler=wrapped
        )

    def register_client_streaming[
        Resp: ProtoMessage,
        //,
        handler: def(mut ServerContext, mut ServerCall) raises thin -> Resp,
    ](mut self, path: StringSpan) raises:
        """Registers a typed client-streaming method.

        Invoked once headers and the first window of request data are
        ready. `ServerCall.recv` drives the blocking frame loop. The
        handler stalls other connections until it returns.

        Parameters:
            Resp: Response message type inferred from the handler.
            handler: Handler consuming the request stream.

        Args:
            path: Full method path, such as `/echo.Echo/Join`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(
            mut ctx: ServerContext, mut call: ServerCall
        ) raises -> List[Byte]:
            var resp = handler(ctx, call)
            return encode(resp)

        self.register_client_streaming_bytes[wrapped](path)

    def register_bidi_bytes[
        handler: def(mut ServerContext, mut ServerCall) raises thin -> None,
    ](mut self, path: StringSpan) raises:
        """Registers a byte-level bidirectional method.

        Parameters:
            handler: Handler driving both directions through `ServerCall`.

        Args:
            path: Full method path, such as `/echo.Echo/Chat`.

        Raises:
            If the path is malformed or already registered.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            handler(ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            call.finish_ok(ctx)

        self._validate_route(path)
        self.streaming_routes[String(path)] = Route(
            kind=MethodKind.BIDI, handler=wrapped
        )

    def register_bidi[
        handler: def(mut ServerContext, mut ServerCall) raises thin -> None,
    ](mut self, path: StringSpan) raises:
        """Registers a bidirectional-streaming method.

        Recv-driven ping-pong: `ServerCall.recv` drives the frame loop.
        The handler runs to completion on the event-loop thread. There is
        no concurrent send+recv firehose.

        Parameters:
            handler: Handler driving both directions through `ServerCall`.

        Args:
            path: Full method path, such as `/echo.Echo/Chat`.

        Raises:
            If the path is malformed or already registered.
        """
        self.register_bidi_bytes[handler](path)

    def add_health_service(mut self, var registry: Health):
        """Registers `grpc.health.v1` Check on this polling server.

        Watch is not registered. Clients receive UNIMPLEMENTED and fall
        back to Check. After this returns, further `set_status` calls
        must go through `self.health`.

        Args:
            registry: Serving-status map. The empty name defaults to SERVING.
        """
        self.health = registry^

    def request_stop(mut self) raises:
        """Asks `serve()` to send GOAWAY, drain live streams, and return.

        Safe to call from a handler on the event-loop thread. A self-pipe
        wakes `Poller.wait` so SIGTERM/SIGINT can stop the loop. This is
        not a cross-thread API.

        Raises:
            If the wakeup pipe cannot be created.
        """
        self._stop_requested = True
        self._ensure_wakeup()
        self._wakeup.value().notify()

    def install_stop_signals(mut self) raises:
        """Writes the wakeup pipe from SIGTERM and SIGINT.

        The C handler calls only `write(2)`. Install before `serve()`.
        Not safe to drive from another Mojo thread.

        Raises:
            If the wakeup pipe or `sigaction` installer fails.
        """
        self._ensure_wakeup()
        self._stop_shim.append(
            install_wakeup_signal_handlers(self._wakeup.value().write_fd)
        )

    def _ensure_wakeup(mut self) raises:
        if not self._wakeup:
            self._wakeup = Wakeup()

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

        if (
            connection.ctx.path not in self.routes
            and (connection.ctx.path not in self.streaming_routes)
            and not (self.health and connection.ctx.path == HEALTH_CHECK_PATH)
        ):
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

        try:
            var payload: List[Byte]
            if self.health and connection.ctx.path == HEALTH_CHECK_PATH:
                var outcome = self.health.value().check_bytes(
                    Span(connection.request)
                )
                var grpc_status = outcome.grpc_status.copy()
                payload = outcome.payload.copy()
                if not grpc_status.is_ok():
                    connection.response_status = grpc_status^
                    return
            else:
                var handler = self.routes[connection.ctx.path].handler
                payload = handler(connection.request^, connection.ctx)
                if connection.ctx.stop_server:
                    self._stop_requested = True
                if connection.ctx.abort_status:
                    connection.response_status = (
                        connection.ctx.abort_status.value().copy()
                    )
                    return
            if len(payload) > self.config.max_message_size:
                connection.response_status = Status(
                    code=StatusCode.RESOURCE_EXHAUSTED,
                    message=String("response message exceeds max size"),
                )
            else:
                connection.response = frame_message(Span(payload))
                connection.response_status = Status.ok()
        except:
            connection.response_status = Status(
                code=StatusCode.UNKNOWN, message=String("handler failed")
            )
        if connection.ctx.stop_server:
            self._stop_requested = True

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

    def _incomplete_wire(self, connection: _PollingConnection) -> Bool:
        return (
            connection.h2.pending_input_frame_count() > 0
            or connection.h2._input_decoder.buffered_len() > 0
        )

    def _flush_connection_blocking(
        self, mut connection: _PollingConnection
    ) raises:
        connection.h2.stream.set_nonblocking(False)
        while len(connection.output) > 0:
            var start = connection.output.offset
            var count = connection.h2.stream.write_some(
                Span(connection.output.data)[
                    start : len(connection.output.data)
                ]
            )
            if count <= 0:
                raise Error("grpc: blocking flush made no progress")
            connection.output.advance(count)
        connection.h2.flush_output()
        connection.last_activity_ns = Int64(monotonic())
        connection.h2.touch_keepalive(connection.last_activity_ns)
        connection.io_wants_read = False
        connection.io_wants_write = False
        connection.io_blocked_read = False

    def _restore_polling_io(self, mut connection: _PollingConnection) raises:
        try:
            connection.h2.stream.set_read_timeout(0)
        except:
            pass
        try:
            connection.h2.stream.set_write_timeout(0)
        except:
            pass
        connection.h2.stream.set_nonblocking(True)
        connection.io_wants_read = False
        connection.io_wants_write = False
        connection.io_blocked_read = False
        connection.resume_read = False

    def _finish_streaming_call(self, mut connection: _PollingConnection) raises:
        var sid = connection.active_sid
        if sid == 0:
            return
        if not connection.h2.retire_stream(sid):
            try:
                connection.h2.send_rst_stream(sid, ERR_CANCEL)
            except:
                pass
            _ = connection.h2.retire_stream(sid)
        connection.handler_called = True
        connection.headers_queued = True
        connection.trailers_queued = True
        connection.reset_call()

    def _run_blocking_stream(
        mut self, mut connection: _PollingConnection
    ) raises:
        var remaining = connection.deadline_remaining(Int64(monotonic()))
        self._flush_connection_blocking(connection)
        if remaining > 0:
            connection.h2.stream.set_read_timeout(remaining)
        elif connection.ctx.timeout_ns > 0 and remaining <= 0:
            var call = ServerCall(
                _conn=Pointer(to=connection.h2).unsafe_origin_cast[
                    MutUntrackedOrigin
                ](),
                sid=connection.active_sid,
                headers_sent=False,
                trailers_sent=False,
                call_start_ns=connection.call_start_ns,
                max_message_size=self.config.max_message_size,
                _oversized_message=False,
            )
            call.finish(
                Status(
                    code=StatusCode.DEADLINE_EXCEEDED,
                    message=String("Deadline Exceeded"),
                ),
                connection.ctx,
            )
            self._restore_polling_io(connection)
            self._finish_streaming_call(connection)
            return

        var call = ServerCall(
            _conn=Pointer(to=connection.h2).unsafe_origin_cast[
                MutUntrackedOrigin
            ](),
            sid=connection.active_sid,
            headers_sent=False,
            trailers_sent=False,
            call_start_ns=connection.call_start_ns,
            max_message_size=self.config.max_message_size,
            _oversized_message=False,
        )
        var handler = self.streaming_routes[connection.ctx.path].handler
        try:
            handler(call, connection.ctx)
        except e:
            if not call.trailers_sent:
                var message = String(e)
                var code = StatusCode.UNKNOWN
                if call._oversized_message:
                    code = StatusCode.RESOURCE_EXHAUSTED
                try:
                    call.finish(
                        Status(code=code, message=message), connection.ctx
                    )
                except:
                    pass
        if connection.ctx.stop_server:
            self._stop_requested = True
        self._restore_polling_io(connection)
        self._finish_streaming_call(connection)

    def _drive_streaming(mut self, mut connection: _PollingConnection) raises:
        if connection.h2.pending_input_frame_count() > 0:
            _ = connection.h2.feed_input(
                Span(List[Byte]()), self.config.max_frames_per_event
            )
            if connection.h2.pending_output_len() > 0:
                _ = self._move_http2_output(connection)
                return
        if self._incomplete_wire(connection):
            return

        if connection.unsupported_media_type or connection.request_error:
            if not self._request_ended(connection):
                return
            connection.handler_called = True
            if connection.request_error:
                connection.response_status = (
                    connection.request_error.value().copy()
                )
            self._queue_response_step(connection)
            _ = self._move_http2_output(connection)
            if connection.trailers_queued:
                var sid = connection.active_sid
                if connection.h2.retire_stream(sid):
                    connection.reset_call()
            return

        if connection.deadline_expired(Int64(monotonic())):
            connection.handler_called = True
            connection.response_status = Status(
                code=StatusCode.DEADLINE_EXCEEDED,
                message=String("Deadline Exceeded"),
            )
            self._queue_response_step(connection)
            _ = self._move_http2_output(connection)
            if connection.trailers_queued:
                var sid = connection.active_sid
                if connection.h2.retire_stream(sid):
                    connection.reset_call()
            return

        var kind = self.streaming_routes[connection.ctx.path].kind
        if kind == MethodKind.SERVER_STREAMING:
            if not self._request_ended(connection):
                return
        else:
            var has_data = (
                connection.h2.buffered_data_len(connection.active_sid) > 0
            )
            if not has_data and not self._request_ended(connection):
                return

        self._run_blocking_stream(connection)

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

        if connection.ctx.path in self.streaming_routes:
            self._drive_streaming(connection)
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
            if idle <= 0:
                return 0
            closest = _merge_poll_remaining(closest, idle)
            var incomplete = connections[fd].incomplete_request_remaining(
                now, self.config.incomplete_request_timeout_ms
            )
            if connections[fd].has_incomplete_request():
                if incomplete <= 0:
                    return 0
                closest = _merge_poll_remaining(closest, incomplete)
            # An unsent suffix already has writable interest. Deferring the
            # RPC deadline until that suffix moves avoids a zero-timeout spin;
            # no later response bytes are queued before the next check.
            if len(connections[fd].output) == 0:
                if connections[fd].deadline_expired(now):
                    return 0
                closest = _merge_poll_remaining(
                    closest, connections[fd].deadline_remaining(now)
                )
            if (
                self.config.keepalive_interval_ns > 0
                and _can_schedule_keepalive(
                    len(connections[fd].output),
                    connections[fd].h2.pending_output_len(),
                    connections[fd].h2.input_preface_complete(),
                )
            ):
                closest = _merge_poll_remaining(
                    closest,
                    _keepalive_remaining_ns(
                        self.config.keepalive_interval_ns,
                        connections[fd].last_activity_ns,
                        now,
                    ),
                )
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

    def _drain_complete(
        self,
        connections: Dict[c_int, _PollingConnection],
        fds: List[c_int],
        handshakes: Dict[c_int, _PollingHandshake],
    ) -> Bool:
        if len(handshakes) != 0:
            return False
        for fd in fds:
            if fd in connections:
                return False
        return True

    def _begin_goaway_drain(
        mut self,
        mut connections: Dict[c_int, _PollingConnection],
        fds: List[c_int],
        mut poller: Poller,
    ) raises:
        if self._shutdown_deadline_ns != 0:
            return
        self._shutdown_deadline_ns = Int64(monotonic()) + (
            Int64(self.config.shutdown_drain_timeout_ms) * 1_000_000
        )
        for fd in fds:
            if fd not in connections:
                continue
            var connection = connections.pop(fd)
            try:
                connection.h2.begin_graceful_shutdown()
                _ = self._move_http2_output(connection)
                self._update_connection_interest(poller, fd, connection)
            except:
                pass
            connections[fd] = connection^

    def serve(mut self) raises:
        """Runs the h2c, TLS, or Unix event loop until stop or a fatal error.

        The bound address is printed after the listener is registered.
        `request_stop()` or SIGTERM/SIGINT (after `install_stop_signals`)
        send GOAWAY and return once live streams drain or the drain
        deadline expires. There is no cross-thread stop API.

        Raises:
            If listener setup, polling, socket I/O, or protocol processing
            fails, or if TLS and Unix are configured together.
        """
        if self._unix_path and self._tls_context:
            raise Error("grpc: PollingServer does not support TLS over Unix")

        var tcp_listener: Optional[TCPListener] = None
        var unix_listener: Optional[UnixListener] = None
        var listener_fd: c_int
        if self._unix_path:
            var path = self._unix_path.value().copy()
            var unix = UnixListener(
                path, remove_existing=self._unix_remove_existing
            )
            unix.set_nonblocking(True)
            listener_fd = unix.descriptor()
            unix_listener = unix^
            print("grpc-mojo polling server listening on unix:", path)
        else:
            var tcp = TCPListener(self.host, self.port)
            tcp.set_nonblocking(True)
            listener_fd = tcp.descriptor()
            print(
                "grpc-mojo polling server listening on ",
                self.host,
                ":",
                tcp.local_port,
                sep="",
            )
            tcp_listener = tcp^
        self._ensure_wakeup()
        var wakeup_fd = self._wakeup.value().descriptor()
        var poller = Poller()
        poller.register(listener_fd, readable=True, writable=False)
        poller.register(wakeup_fd, readable=True, writable=False)
        var listener_registered = True
        var connections = Dict[c_int, _PollingConnection]()
        var handshakes = Dict[c_int, _PollingHandshake]()
        var fds = List[c_int]()

        while True:
            if self._stop_requested:
                if listener_registered:
                    poller.unregister(listener_fd)
                    listener_registered = False
                self._begin_goaway_drain(connections, fds, poller)
                var drained = self._drain_complete(connections, fds, handshakes)
                var expired = Int64(monotonic()) >= self._shutdown_deadline_ns
                if drained or expired:
                    var leftover = List[c_int](capacity=len(fds))
                    for fd in fds:
                        leftover.append(fd)
                    for fd in leftover:
                        if fd in handshakes:
                            _ = handshakes.pop(fd)
                        if fd in connections:
                            var connection = connections.pop(fd)
                            connection.h2.close()
                        self._remove_fd(fds, fd)
                    poller.unregister(wakeup_fd)
                    self._wakeup.value().close()
                    return

            var timeout_ms = self._poll_timeout_ms(connections, handshakes, fds)
            if self._shutdown_deadline_ns > 0:
                var remain_ns = self._shutdown_deadline_ns - Int64(monotonic())
                var remain_ms = 0
                if remain_ns > 0:
                    remain_ms = Int((remain_ns + 999_999) // 1_000_000)
                if timeout_ms < 0 or remain_ms < timeout_ms:
                    timeout_ms = remain_ms
            var events = _coalesce_poll_events(Span(poller.wait(timeout_ms)))
            var accept_ready = False
            var close_fds = List[c_int]()
            var closing = Dict[c_int, _PollingConnection]()
            var processed_fds = List[c_int]()
            var handshake_steps = 0

            # Existing descriptors are processed before any close or accept.
            # This prevents a stale duplicate kqueue event from reaching a
            # newly accepted connection that reused the same descriptor.
            for event in events:
                if event.fd == wakeup_fd:
                    try:
                        self._wakeup.value().drain()
                    except:
                        pass
                    self._stop_requested = True
                    continue
                if event.fd == listener_fd:
                    if not self._stop_requested:
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
                if self.config.keepalive_interval_ns > 0 and fd in connections:
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
                and not self._stop_requested
                and len(fds) < self.config.max_connections
                and (
                    not self._tls_context
                    or len(handshakes) < self.config.max_pending_handshakes
                )
            ):
                poller.register(listener_fd, readable=True, writable=False)
                listener_registered = True

            if (
                accept_ready
                and listener_registered
                and not self._stop_requested
            ):
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
                        if unix_listener:
                            var unix = unix_listener.value().accept()
                            unix.set_nonblocking(True)
                            accepts += 1
                            var transport = GrpcTransport.local(unix^)
                            var connection = _PollingConnection(
                                transport^, self.config
                            )
                            var fd = connection.descriptor()
                            poller.register(fd, readable=True, writable=False)
                            connections[fd] = connection^
                            fds.append(fd)
                            continue
                        var tcp = tcp_listener.value().accept()
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

            if self._stop_requested:
                if listener_registered:
                    poller.unregister(listener_fd)
                    listener_registered = False
                self._begin_goaway_drain(connections, fds, poller)
                var drained = self._drain_complete(connections, fds, handshakes)
                var expired = Int64(monotonic()) >= self._shutdown_deadline_ns
                if drained or expired:
                    var leftover = List[c_int](capacity=len(fds))
                    for fd in fds:
                        leftover.append(fd)
                    for fd in leftover:
                        if fd in handshakes:
                            _ = handshakes.pop(fd)
                        if fd in connections:
                            var connection = connections.pop(fd)
                            connection.h2.close()
                        self._remove_fd(fds, fd)
                    poller.unregister(wakeup_fd)
                    self._wakeup.value().close()
                    return
