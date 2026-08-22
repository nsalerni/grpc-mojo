# Bounded unary TLS server driven by one kqueue/epoll Poller.

from std.sys import argv

from echo_pb import ECHO_SAY_PATH, EchoRequest, EchoResponse
from grpc import PollingServer, PollingServerConfig, ServerContext


def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("echo: ") + req.message
    return resp^


def main() raises:
    var args = argv()
    if len(args) != 3:
        raise Error(
            "usage: polling_tls_server <certificate-chain.pem>"
            " <private-key.pem>"
        )

    var config = PollingServerConfig(
        max_connections=256,
        max_pending_handshakes=32,
        tls_handshake_timeout_ms=10_000,
        max_pending_output_size=64 * 1024,
    )
    var server = PollingServer.tls("127.0.0.1", 50051, args[1], args[2], config)
    server.register_unary[say](ECHO_SAY_PATH)
    server.serve()
