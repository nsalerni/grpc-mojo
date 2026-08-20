# Echo client example: calls the unary Echo.Say method through the generated
# stub in echo_pb.mojo, then exercises the error path by invoking an unknown
# method and checking for UNIMPLEMENTED. Start echo_server.mojo first and
# pass its port.
#
# Usage:
#   pixi run mojo run -I src -I examples examples/echo_client.mojo <port> [message]

from std.sys import argv

from echo_pb import ECHO_SAY_PATH, EchoClient, EchoRequest, EchoResponse
from grpc import Metadata, StatusCode
from proto import encode


def main() raises:
    var args = argv()
    if len(args) < 2:
        raise Error("usage: echo_client <port> [message]")
    var port = UInt16(Int(args[1]))
    var message = String("hello from mojo")
    if len(args) > 2:
        message = String(args[2])

    # Generated stub (tools/protoc-gen-mojo output).
    var client = EchoClient.connect("127.0.0.1", port)
    var req = EchoRequest()
    req.message = message.copy()
    var resp = client.say(req, timeout_ns=10_000_000_000)
    print("response:", resp.message)

    # Exercise the error path too: unknown method must be UNIMPLEMENTED.
    var sid = client.channel.start_call("/echo.Echo/DoesNotExist", Metadata())
    var bad = EchoRequest()
    bad.message = "x"
    client.channel.send_request_bytes(sid, Span(encode(bad)), last=True)
    var result = client.channel.finish(sid)
    if result.status.code != StatusCode.UNIMPLEMENTED:
        raise Error("expected UNIMPLEMENTED, got " + String(result.status))
    print("unimplemented-check: ok (", result.status, ")")
    client.close()
