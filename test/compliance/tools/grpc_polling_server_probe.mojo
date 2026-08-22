# Compliance tool: bounded unary h2c PollingServer exercised by grpcio.

from std.sys import argv

from echo_pb import EchoRequest, EchoResponse
from grpc import PollingServer, PollingServerConfig, ServerContext


def echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = req.message.copy()
    return resp^


def fail(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    raise Error("polling handler failed")


def large(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("z") * (3 * 1024 * 1024)
    return resp^


def metadata(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    ctx.response_metadata.add(String("x-polling-initial"), String("ready"))
    ctx.response_trailers.add(String("x-polling-trailer"), String("done"))
    var resp = EchoResponse()
    resp.message = req.message.copy()
    return resp^


def main() raises:
    var args = argv()
    var max_connections = 128
    var max_message_size = 4 * 1024 * 1024
    var max_pending_output_size = 64 * 1024
    var max_frames_per_event = 64
    var idle_timeout_ms = 300_000
    var incomplete_request_timeout_ms = 30_000
    if len(args) > 1:
        max_connections = Int(args[1])
    if len(args) > 2:
        max_message_size = Int(args[2])
    if len(args) > 3:
        max_pending_output_size = Int(args[3])
    if len(args) > 4:
        max_frames_per_event = Int(args[4])
    if len(args) > 5:
        idle_timeout_ms = Int(args[5])
    if len(args) > 6:
        incomplete_request_timeout_ms = Int(args[6])
    var config = PollingServerConfig(
        max_connections=max_connections,
        max_message_size=max_message_size,
        max_pending_output_size=max_pending_output_size,
        max_frames_per_event=max_frames_per_event,
        idle_timeout_ms=idle_timeout_ms,
        incomplete_request_timeout_ms=incomplete_request_timeout_ms,
    )
    var server = PollingServer("127.0.0.1", 0, config)
    server.register_unary[echo]("/probe.Probe/Echo")
    server.register_unary[fail]("/probe.Probe/Fail")
    server.register_unary[large]("/probe.Probe/Large")
    server.register_unary[metadata]("/probe.Probe/Metadata")
    server.serve()
