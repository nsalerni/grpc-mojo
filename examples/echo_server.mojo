# Echo server example: one handler for each of the four gRPC method kinds —
# unary (say), server-streaming (split), client-streaming (join), and
# bidirectional (chat). The service is wired up with add_echo_service from
# the generated stubs in echo_pb.mojo (tools/protoc-gen-mojo output for
# echo.proto).
#
# Usage:
#   pixi run mojo run -I src -I examples examples/echo_server.mojo [port]
#
# With no argument it binds an ephemeral port and prints it; pass that port
# to echo_client.mojo. The server runs until interrupted.

from std.sys import argv

from echo_pb import EchoRequest, EchoResponse, add_echo_service
from grpc import Server, ServerCall, ServerContext


def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("echo: ") + req.message
    return resp^


def split(
    req: EchoRequest, mut ctx: ServerContext, mut call: ServerCall
) raises:
    """Server-streaming: one response per whitespace-separated word."""
    for word in req.message.split(" "):
        if word.byte_length() == 0:
            continue
        var resp = EchoResponse()
        resp.message = String(word)
        call.send[EchoResponse](ctx, resp)


def join(mut ctx: ServerContext, mut call: ServerCall) raises -> EchoResponse:
    """Client-streaming: concatenate every request message with '+'."""
    var parts = String()
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        if parts.byte_length() > 0:
            parts += "+"
        parts += msg.value().message
    var resp = EchoResponse()
    resp.message = parts^
    return resp^


def chat(mut ctx: ServerContext, mut call: ServerCall) raises:
    """Bidi: echo each message as it arrives (ping-pong friendly)."""
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        var resp = EchoResponse()
        resp.message = String("pong: ") + msg.value().message
        call.send[EchoResponse](ctx, resp)


def main() raises:
    var port: UInt16 = 0
    var args = argv()
    if len(args) > 1:
        port = UInt16(Int(args[1]))
    var server = Server("127.0.0.1", port)
    add_echo_service[say, split, join, chat](server)
    server.serve()
