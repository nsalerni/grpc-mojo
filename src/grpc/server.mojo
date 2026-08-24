# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Server: gRPC over blocking HTTP/2 on TCP, TLS, or Unix sockets.

Handlers are thin function pointers registered at compile time via
`Server.register_unary`, `register_server_streaming`,
`register_client_streaming`, and `register_bidi`; the typed signatures are
wrapped into a common `RawHandler` shape by the `register_*` methods. Each
handler receives a `ServerContext` (request metadata, deadline, response
metadata, abort) and — for streaming kinds — a `ServerCall` to receive and
send messages on.

The v1 concurrency model is single-threaded: connections are handled
sequentially, and within a connection a stream's handler runs to completion
when its request HEADERS arrive. Because `ServerCall.recv` drives the frame
loop, recv-driven ping-pong bidi works; concurrent full-duplex firehose
needs threads, which Mojo 1.0 does not expose yet (docs/PRIMITIVES.md
item 7). `Server.dispatch_ready` is the seam where concurrency will land.

Protocol conduct per
[PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
non-gRPC content types get an HTTP 415, unknown methods get UNIMPLEMENTED,
immediate errors use Trailers-Only responses, a blown `grpc-timeout`
deadline turns an otherwise-OK finish into DEADLINE_EXCEEDED, and handler
exceptions surface as UNKNOWN.
"""

from std.time import monotonic

from hpack import HeaderField
from h2 import ERR_NO_ERROR, Http2Connection
from net import TCPListener, UnixListener
from proto import ProtoMessage, decode, encode
from tls import TLSContext

from .framing import recv_message, send_message
from .metadata import Metadata, encode_bin_value
from .status import Status, StatusCode, percent_encode_message
from .timeout import decode_timeout
from .transport import GrpcTransport


struct MethodKind:
    """RPC method kinds, as integer constants used in the routing table."""

    comptime UNARY = 0
    """One request message, one response message."""
    comptime SERVER_STREAMING = 1
    """One request message, a stream of response messages."""
    comptime CLIENT_STREAMING = 2
    """A stream of request messages, one response message."""
    comptime BIDI = 3
    """Both directions stream (recv-driven ping-pong in v1)."""


struct ServerContext(Movable):
    """Per-call information passed to handlers.

    Carries the request side (path, metadata, decoded `grpc-timeout`) and
    collects the response side (custom headers and trailers, abort status)
    for the dispatch code to send.
    """

    var path: String
    """The request `:path`, e.g. `/echo.Echo/Say`."""
    var metadata: Metadata
    """Custom metadata from the request headers."""
    var timeout_ns: Int64
    """0 means no deadline was sent."""
    var response_metadata: Metadata
    """Custom metadata the handler wants in the initial response headers."""
    var response_trailers: Metadata
    """Custom metadata the handler wants in the trailers."""
    var abort_status: Optional[Status]
    """Set by a handler to end the call with a specific non-OK status
    (the equivalent of grpcio's context.abort)."""

    def __init__(out self):
        """Constructs an empty context; dispatch fills the request fields."""
        self.path = String()
        self.metadata = Metadata()
        self.timeout_ns = 0
        self.response_metadata = Metadata()
        self.response_trailers = Metadata()
        self.abort_status = None

    def abort(mut self, code: Int, var message: String):
        """Ends the call with a specific status once the handler returns.

        Any response messages already sent stand; the recorded status is
        sent in the trailers instead of OK.

        Args:
            code: A `StatusCode` value; should be non-OK.
            message: Status message for the `grpc-message` trailer.
        """
        self.abort_status = Status(code=code, message=message^)

    def abort_with_details(
        mut self, code: Int, var message: String, details: Span[Byte, _]
    ):
        """Aborts with rich error details in `grpc-status-details-bin`.

        Like `abort`, but also attaches a serialized `google.rpc.Status`
        (the rich error model) that the trailers carry base64-coded.

        Args:
            code: A `StatusCode` value; should be non-OK.
            message: Status message for the `grpc-message` trailer.
            details: Serialized `google.rpc.Status` bytes.
        """
        var st = Status(code=code, message=message^)
        st.details_bin = List[Byte](details)
        self.abort_status = st^


@fieldwise_init
struct ServerCall(Movable):
    """Handle for one RPC stream, passed to streaming handlers.

    Wraps the connection with an untracked pointer: the handle never
    outlives the dispatch call that created it (single-threaded server).
    Do not store a `ServerCall` beyond the handler invocation.
    """

    var _conn: Pointer[Http2Connection[GrpcTransport], MutUntrackedOrigin]
    """Untracked pointer to the connection; valid only during dispatch."""
    var sid: UInt32
    """The HTTP/2 stream id of this call."""
    var headers_sent: Bool
    """Whether the initial response HEADERS have gone out."""
    var trailers_sent: Bool
    """Whether the trailers have gone out (the call is finished)."""
    var call_start_ns: Int64
    """Monotonic time the request HEADERS were dispatched, for deadlines."""

    def client_cancelled(mut self) -> Bool:
        """Reports whether the client reset the stream (RST_STREAM).

        Returns:
            True if the stream carries a reset code; False otherwise,
            including when stream state is missing.
        """
        try:
            return Bool(self._conn[].streams[self.sid].reset_code)
        except:
            return False

    def recv_bytes(mut self) raises -> Optional[List[Byte]]:
        """Receives the next serialized request message.

        Blocks driving the connection's frame loop, which is what lets
        recv-driven ping-pong bidi handlers work single-threaded.

        Returns:
            The message bytes, or None when the client half-closed.

        Raises:
            On framing or connection errors.
        """
        return recv_message(self._conn[], self.sid)

    def recv[M: ProtoMessage](mut self) raises -> Optional[M]:
        """Receives and decodes the next typed request message.

        Parameters:
            M: The request message type.

        Returns:
            The decoded message, or None when the client half-closed.

        Raises:
            On decoding, framing, or connection errors.
        """
        var raw = self.recv_bytes()
        if raw:
            return decode[M](Span(raw.value()))
        return None

    def send_headers_once(mut self, ctx: ServerContext) raises:
        """Sends the initial response HEADERS if not already sent.

        Emits `:status 200`, the gRPC content type, and the handler's
        `ctx.response_metadata`. Idempotent; the message send paths call it
        implicitly, so handlers rarely need to.

        Args:
            ctx: The call context supplying response metadata.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        if self.headers_sent:
            return
        self.headers_sent = True
        var headers = List[HeaderField]()
        headers.append(HeaderField(name=String(":status"), value=String("200")))
        headers.append(
            HeaderField(
                name=String("content-type"),
                value=String("application/grpc+proto"),
            )
        )
        for m in ctx.response_metadata.entries:
            headers.append(m.copy())
        self._conn[].send_headers(self.sid, Span(headers), end_stream=False)

    def send_bytes(mut self, ctx: ServerContext, payload: Span[Byte, _]) raises:
        """Sends one serialized response message.

        Sends the initial response headers first when still pending.

        Args:
            ctx: The call context supplying response metadata.
            payload: The serialized message bytes.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        self.send_headers_once(ctx)
        send_message(self._conn[], self.sid, payload, end_stream=False)

    def send[M: ProtoMessage](mut self, ctx: ServerContext, msg: M) raises:
        """Encodes and sends one typed response message.

        Parameters:
            M: The response message type.

        Args:
            ctx: The call context supplying response metadata.
            msg: The message to encode and send.

        Raises:
            On encoding or connection errors.
        """
        self.send_bytes(ctx, Span(encode(msg)))

    def deadline_blown(self, ctx: ServerContext) -> Bool:
        """Reports whether the client's `grpc-timeout` deadline has elapsed.

        Args:
            ctx: The call context carrying the decoded timeout.

        Returns:
            True if a deadline was sent and the elapsed call time meets or
            exceeds it; always False when no deadline was sent.
        """
        if ctx.timeout_ns <= 0:
            return False
        return Int64(monotonic()) - self.call_start_ns >= ctx.timeout_ns

    def finish(mut self, status: Status, ctx: ServerContext) raises:
        """Sends trailers (or Trailers-Only) exactly once.

        When no response headers have gone out yet, emits a Trailers-Only
        response: a single HEADERS block carrying `:status 200`, the
        content type, and the status trailers. The trailers carry
        `grpc-status`, a percent-encoded `grpc-message` when non-empty, a
        base64-coded `grpc-status-details-bin` for non-OK statuses with
        details, and the handler's `ctx.response_trailers`. Idempotent.

        Args:
            status: The final call status.
            ctx: The call context supplying trailer metadata.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        if self.trailers_sent:
            return
        self.trailers_sent = True
        var trailers = List[HeaderField]()
        if not self.headers_sent:
            # Trailers-Only response.
            self.headers_sent = True
            trailers.append(
                HeaderField(name=String(":status"), value=String("200"))
            )
            trailers.append(
                HeaderField(
                    name=String("content-type"),
                    value=String("application/grpc+proto"),
                )
            )
        trailers.append(
            HeaderField(name=String("grpc-status"), value=String(status.code))
        )
        if status.message.byte_length() > 0:
            trailers.append(
                HeaderField(
                    name=String("grpc-message"),
                    value=percent_encode_message(status.message),
                )
            )
        if len(status.details_bin) > 0 and status.code != StatusCode.OK:
            trailers.append(
                HeaderField(
                    name=String("grpc-status-details-bin"),
                    value=encode_bin_value(Span(status.details_bin)),
                )
            )
        for m in ctx.response_trailers.entries:
            trailers.append(m.copy())
        self._conn[].send_headers(self.sid, Span(trailers), end_stream=True)

    def finish_ok(mut self, ctx: ServerContext) raises:
        """Finishes with OK — unless the deadline is blown or the client left.

        A cancelled client gets nothing (the trailers are marked sent so
        `finish` becomes a no-op); a blown deadline turns the finish into
        DEADLINE_EXCEEDED, as the spec requires the server to enforce the
        decoded `grpc-timeout`.

        Args:
            ctx: The call context supplying deadline and trailer metadata.

        Raises:
            On connection I/O or HTTP/2 protocol errors.
        """
        if self.client_cancelled():
            self.trailers_sent = True  # nobody is listening
            return
        if self.deadline_blown(ctx):
            self.finish(
                Status(
                    code=StatusCode.DEADLINE_EXCEEDED,
                    message=String("Deadline Exceeded"),
                ),
                ctx,
            )
            return
        self.finish(Status.ok(), ctx)


comptime RawHandler = def(mut ServerCall, mut ServerContext) raises thin -> None
"""The common handler shape every registration wraps into: a thin function
pointer driving one call via `ServerCall` and `ServerContext`."""

comptime UnaryBytesHandler = def(
    List[Byte], mut ServerContext
) raises thin -> List[Byte]
"""Byte-level unary handler shape (typed handlers wrap into this): request
message bytes in, response message bytes out."""


@fieldwise_init
struct Route(Movable):
    """One routing-table entry: a method kind and its wrapped handler."""

    var kind: Int
    """A `MethodKind` value."""
    var handler: RawHandler
    """The wrapped handler invoked for this path."""


def _find_header(
    fields: Span[HeaderField, _], name: StaticString
) -> Optional[String]:
    for f in fields:
        if f.name == String(name):
            return f.value.copy()
    return None


struct Server(Movable):
    """A gRPC server: a routing table plus a blocking accept loop.

    Register handlers with the `register_*` methods (handler functions are
    compile-time parameters, so registration builds thin function pointers
    with no boxing), then call `serve`. See examples/echo_server.mojo for a
    complete service.
    """

    var host: String
    """Host or address to bind, e.g. `127.0.0.1`."""
    var port: UInt16
    """TCP port to bind; 0 picks an ephemeral port."""
    var routes: Dict[String, Route]
    """Routing table from full method path to handler."""
    var _tls_context: Optional[TLSContext]
    """Reusable server TLS context; None selects plaintext h2c."""
    var _unix_path: Optional[String]
    """Unix domain socket path; None selects a TCP listener."""
    var _unix_remove_existing: Bool
    """Whether a Unix listener may remove an existing socket file."""

    def __init__(out self, host: StringSpan, port: UInt16):
        """Constructs a server with an empty routing table.

        Args:
            host: Host or address to bind.
            port: TCP port to bind; 0 picks an ephemeral port (printed by
                `serve`).
        """
        self.host = String(host)
        self.port = port
        self.routes = Dict[String, Route]()
        self._tls_context = None
        self._unix_path = None
        self._unix_remove_existing = False

    @staticmethod
    def tls(
        host: StringSpan,
        port: UInt16,
        cert_chain_pem: String,
        key_pem: String,
    ) raises -> Server:
        """Constructs a TLS server that accepts only `h2` with ALPN.

        Args:
            host: Host or address to bind.
            port: TCP port to bind; 0 picks an ephemeral port.
            cert_chain_pem: Path to the PEM certificate chain.
            key_pem: Path to the matching PEM private key.

        Returns:
            A server configured for TLS connections.

        Raises:
            If the TLS context, certificate, or key cannot be loaded.
        """
        var out = Server(host, port)
        out._tls_context = TLSContext.server(
            cert_chain_pem, key_pem, alpn=[String("h2")]
        )
        return out^

    @staticmethod
    def unix(path: StringSpan, *, remove_existing: Bool = False) -> Server:
        """Constructs a plaintext server on a Unix domain socket.

        Args:
            path: Filesystem path to bind.
            remove_existing: Remove an existing socket file before bind.
                The default refuses to replace any existing path.

        Returns:
            A server configured for the Unix domain socket.
        """
        var out = Server("", 0)
        out._unix_path = String(path)
        out._unix_remove_existing = remove_existing
        return out^

    # --- registration (typed handlers wrapped at compile time) ---

    def register_unary_bytes[
        handler: UnaryBytesHandler
    ](mut self, path: StringSpan):
        """Registers a byte-level unary handler for a method path.

        The wrapper receives the request message, invokes the handler, and
        finishes the call — honoring `ctx.abort`, client cancellation, and
        the deadline. A missing request message finishes as INTERNAL.

        Parameters:
            handler: The unary handler taking request bytes and the call
                context, returning response bytes.

        Args:
            path: Full method path, e.g. `/echo.Echo/Say`.
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
            var response = handler(msg.take(), ctx)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            if call.deadline_blown(ctx) or call.client_cancelled():
                call.finish_ok(ctx)  # resolves to DEADLINE_EXCEEDED / silent
                return
            call.send_bytes(ctx, Span(response))
            call.finish_ok(ctx)

        self.routes[String(path)] = Route(
            kind=MethodKind.UNARY, handler=wrapped
        )

    def register_unary[
        Req: ProtoMessage,
        Resp: ProtoMessage,
        //,
        handler: def(Req, mut ServerContext) raises thin -> Resp,
    ](mut self, path: StringSpan):
        """Registers a typed unary handler for a method path.

        Wraps decode/encode around the handler and delegates to
        `register_unary_bytes` for call conduct.

        Parameters:
            Req: The request message type (inferred from the handler).
            Resp: The response message type (inferred from the handler).
            handler: The unary handler mapping a request to a response.

        Args:
            path: Full method path, e.g. `/echo.Echo/Say`.
        """

        def wrapped(
            request: List[Byte], mut ctx: ServerContext
        ) raises -> List[Byte]:
            var req = decode[Req](Span(request))
            var resp = handler(req, ctx)
            return encode(resp)

        self.register_unary_bytes[wrapped](path)

    def register_server_streaming[
        Req: ProtoMessage,
        //,
        handler: def(
            Req, mut ServerContext, mut ServerCall
        ) raises thin -> None,
    ](mut self, path: StringSpan):
        """Registers a server-streaming handler for a method path.

        The wrapper reads the single request message, then hands the call
        to the handler to `call.send` any number of responses; on return
        the call finishes OK unless `ctx.abort` was used. A missing request
        message finishes as INTERNAL.

        Parameters:
            Req: The request message type (inferred from the handler).
            handler: The handler receiving the request and streaming
                responses via the call handle.

        Args:
            path: Full method path, e.g. `/echo.Echo/Split`.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            var msg = call.recv[Req]()
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
            var req = msg.take()
            handler(req, ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            call.finish_ok(ctx)

        self.routes[String(path)] = Route(
            kind=MethodKind.SERVER_STREAMING, handler=wrapped
        )

    def register_client_streaming[
        Resp: ProtoMessage,
        //,
        handler: def(mut ServerContext, mut ServerCall) raises thin -> Resp,
    ](mut self, path: StringSpan):
        """Registers a client-streaming handler for a method path.

        The handler drains requests with `call.recv` until None (client
        half-closed) and returns the single response, which the wrapper
        sends before finishing OK — unless `ctx.abort` was used or the
        client cancelled.

        Parameters:
            Resp: The response message type (inferred from the handler).
            handler: The handler consuming the request stream and
                returning one response.

        Args:
            path: Full method path, e.g. `/echo.Echo/Join`.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            var resp = handler(ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            if call.client_cancelled():
                call.trailers_sent = True
                return
            call.send[Resp](ctx, resp)
            call.finish_ok(ctx)

        self.routes[String(path)] = Route(
            kind=MethodKind.CLIENT_STREAMING, handler=wrapped
        )

    def register_bidi[
        handler: def(mut ServerContext, mut ServerCall) raises thin -> None,
    ](mut self, path: StringSpan):
        """Registers a bidirectional-streaming handler for a method path.

        The handler owns the whole conversation via `call.recv` and
        `call.send`. Because `recv` drives the frame loop, the natural v1
        shape is recv-driven ping-pong: read a message, send responses,
        repeat until `recv` returns None (no threads in Mojo 1.0 yet). On
        return the call finishes OK unless `ctx.abort` was used.

        Parameters:
            handler: The handler driving both directions via the call
                handle.

        Args:
            path: Full method path, e.g. `/echo.Echo/Chat`.
        """

        def wrapped(mut call: ServerCall, mut ctx: ServerContext) raises:
            handler(ctx, call)
            if ctx.abort_status:
                call.finish(ctx.abort_status.value().copy(), ctx)
                return
            call.finish_ok(ctx)

        self.routes[String(path)] = Route(kind=MethodKind.BIDI, handler=wrapped)

    # --- dispatch ---

    def _handle_stream(
        mut self, mut conn: Http2Connection[GrpcTransport], sid: UInt32
    ) raises:
        var headers = conn.streams[sid].headers.copy()
        var call = ServerCall(
            _conn=Pointer(to=conn).unsafe_origin_cast[MutUntrackedOrigin](),
            sid=sid,
            headers_sent=False,
            trailers_sent=False,
            call_start_ns=Int64(monotonic()),
        )

        # Content-type gate (spec: 415 for non-gRPC).
        var content_type = _find_header(Span(headers), "content-type")
        var ct_ok = False
        if content_type:
            ct_ok = content_type.value().startswith("application/grpc")
        if not ct_ok:
            var resp = [
                HeaderField(name=String(":status"), value=String("415"))
            ]
            conn.send_headers(sid, Span(resp), end_stream=True)
            return

        var ctx = ServerContext()
        var path = _find_header(Span(headers), ":path")
        if path:
            ctx.path = path.value()
        ctx.metadata = Metadata.from_headers(Span(headers))
        var timeout = _find_header(Span(headers), "grpc-timeout")
        if timeout:
            try:
                ctx.timeout_ns = decode_timeout(timeout.value())
            except:
                # A malformed grpc-timeout fails this one call; it must not
                # tear down the connection carrying other calls.
                call.finish(
                    Status(
                        code=StatusCode.INTERNAL,
                        message=String("malformed grpc-timeout header"),
                    ),
                    ctx,
                )
                return

        if ctx.path not in self.routes:
            call.finish(
                Status(
                    code=StatusCode.UNIMPLEMENTED,
                    message=String("unknown method ") + ctx.path,
                ),
                ctx,
            )
            return

        var handler = self.routes[ctx.path].handler
        try:
            handler(call, ctx)
        except e:
            # Handler errors surface as UNKNOWN, like other gRPC servers.
            if not call.trailers_sent:
                call.finish(
                    Status(code=StatusCode.UNKNOWN, message=String(e)), ctx
                )

    def dispatch_ready(
        mut self,
        mut conn: Http2Connection[GrpcTransport],
        mut handled: List[UInt32],
    ) raises -> Int:
        """Dispatches every stream whose request HEADERS have arrived.

        Each ready stream's handler runs to completion before the next is
        dispatched. This is the seam where concurrency will land once Mojo
        exposes threads (docs/PRIMITIVES.md item 7) — keep protocol logic
        out of the accept loop and behind this method.

        Args:
            conn: The connection whose streams to dispatch.
            handled: Per-connection record of already-dispatched stream
                ids; updated in place.

        Returns:
            How many streams were handled in this pass.

        Raises:
            On connection I/O or HTTP/2 protocol errors during dispatch.
        """
        var count = 0
        var ids = conn.stream_ids.copy()
        for sid in ids:
            var done = False
            for h in handled:
                if h == sid:
                    done = True
            if done:
                continue
            if conn.streams[sid].headers_done:
                handled.append(sid)
                self._handle_stream(conn, sid)
                count += 1
        return count

    def _serve_connection_impl(
        mut self, mut conn: Http2Connection[GrpcTransport]
    ) raises:
        var handled = List[UInt32]()
        while True:
            conn.process_next_frame()
            _ = self.dispatch_ready(conn, handled)

    def serve(mut self) raises:
        """Accepts and serves connections forever (sequentially in v1).

        Binds the listener, prints the bound address (useful with port 0),
        and loops: each accepted connection is served until the client
        disconnects or commits a protocol error, then the next is accepted.
        Does not return under normal operation.

        Raises:
            If the TCP listener cannot bind the configured host and port,
            or the Unix listener cannot bind its configured path.
        """
        if self._unix_path:
            var path = self._unix_path.value().copy()
            var listener = UnixListener(
                path, remove_existing=self._unix_remove_existing
            )
            print("grpc-mojo server listening on unix:", path)
            while True:
                var unix = listener.accept()
                try:
                    var transport = GrpcTransport.local(unix^)
                    var conn = Http2Connection(transport^, is_client=False)
                    try:
                        self._serve_connection_impl(conn)
                    except:
                        # Client disconnected or committed a protocol error.
                        conn.close()
                except:
                    # A failed connection does not stop the listener.
                    pass

        var listener = TCPListener(self.host, self.port)
        print(
            "grpc-mojo server listening on ",
            self.host,
            ":",
            listener.local_port,
            sep="",
        )
        while True:
            var tcp = listener.accept()
            try:
                var transport: GrpcTransport
                if self._tls_context:
                    var tls = self._tls_context.value().accept(tcp^)
                    if tls.negotiated_alpn() != "h2":
                        tls.close()
                        continue
                    transport = GrpcTransport.secure(tls^)
                else:
                    transport = GrpcTransport.plaintext(tcp^)
                var conn = Http2Connection(transport^, is_client=False)
                try:
                    self._serve_connection_impl(conn)
                except:
                    # Client disconnected or committed a protocol error.
                    conn.close()
            except:
                # A failed TLS handshake does not stop the listener.
                pass
