# Official gRPC interop TestService implemented on grpc-mojo.
# Semantics follow grpc/grpc doc/interop-test-descriptions.md.
#
# Prints its endpoint, then serves forever.

from echo_pb import EchoRequest  # noqa: keeps -I test module path warm
from empty_pb import Empty
from messages_pb import (
    Payload,
    SimpleRequest,
    SimpleResponse,
    StreamingInputCallRequest,
    StreamingInputCallResponse,
    StreamingOutputCallRequest,
    StreamingOutputCallResponse,
)
from std.time import sleep
from std.sys import argv

from grpc import Server, ServerCall, ServerContext


def zeros(n: Int) -> List[Byte]:
    return List[Byte](length=n, fill=0)


def echo_metadata(mut ctx: ServerContext) raises:
    """The custom_metadata case: echo the test keys back."""
    var initial = ctx.metadata.get("x-grpc-test-echo-initial")
    if initial:
        ctx.response_metadata.add(
            String("x-grpc-test-echo-initial"), initial.value()
        )
    var trailing = ctx.metadata.get_binary("x-grpc-test-echo-trailing-bin")
    if trailing:
        ctx.response_trailers.add_binary(
            String("x-grpc-test-echo-trailing-bin"), Span(trailing.value())
        )


def empty_call(req: Empty, mut ctx: ServerContext) raises -> Empty:
    return Empty()


def unary_call(
    req: SimpleRequest, mut ctx: ServerContext
) raises -> SimpleResponse:
    echo_metadata(ctx)
    if req.response_status:
        ctx.abort(
            Int(req.response_status.value().code),
            req.response_status.value().message.copy(),
        )
        return SimpleResponse()
    var resp = SimpleResponse()
    var p = Payload()
    p.body = zeros(Int(req.response_size))
    resp.payload = p^
    return resp^


def streaming_input(
    mut ctx: ServerContext, mut call: ServerCall
) raises -> StreamingInputCallResponse:
    var total = 0
    while True:
        var msg = call.recv[StreamingInputCallRequest]()
        if not msg:
            break
        if msg.value().payload:
            total += len(msg.value().payload.value().body)
    var resp = StreamingInputCallResponse()
    resp.aggregated_payload_size = Int32(total)
    return resp^


def streaming_output(
    req: StreamingOutputCallRequest,
    mut ctx: ServerContext,
    mut call: ServerCall,
) raises:
    for param in req.response_parameters:
        if param.interval_us > 0:
            sleep(Float64(param.interval_us) / 1_000_000.0)
        var resp = StreamingOutputCallResponse()
        var p = Payload()
        p.body = zeros(Int(param.size))
        resp.payload = p^
        call.send[StreamingOutputCallResponse](ctx, resp)


def full_duplex(mut ctx: ServerContext, mut call: ServerCall) raises:
    echo_metadata(ctx)
    while True:
        var msg = call.recv[StreamingOutputCallRequest]()
        if not msg:
            break
        var req = msg.take()
        if req.response_status:
            ctx.abort(
                Int(req.response_status.value().code),
                req.response_status.value().message.copy(),
            )
            return
        for param in req.response_parameters:
            if param.interval_us > 0:
                sleep(Float64(param.interval_us) / 1_000_000.0)
            var resp = StreamingOutputCallResponse()
            var p = Payload()
            p.body = zeros(Int(param.size))
            resp.payload = p^
            call.send[StreamingOutputCallResponse](ctx, resp)


def main() raises:
    var args = argv()
    var server: Server
    if len(args) >= 3 and args[1] == "unix":
        server = Server.unix(args[2], remove_existing=True)
    elif len(args) >= 3:
        server = Server.tls("127.0.0.1", 0, args[1], args[2])
    else:
        server = Server("127.0.0.1", 0)
    server.register_unary[empty_call]("/grpc.testing.TestService/EmptyCall")
    server.register_unary[unary_call]("/grpc.testing.TestService/UnaryCall")
    server.register_client_streaming[streaming_input](
        "/grpc.testing.TestService/StreamingInputCall"
    )
    server.register_server_streaming[streaming_output](
        "/grpc.testing.TestService/StreamingOutputCall"
    )
    server.register_bidi[full_duplex](
        "/grpc.testing.TestService/FullDuplexCall"
    )
    server.serve()
