# Compliance tool: Mojo gRPC server with probe handlers, exercised by the
# reference grpcio client from run_compliance.py.
#
# Methods (all unary, echo.proto message shapes):
#   /probe.Probe/Echo        echo message verbatim
#   /probe.Probe/Timeout     returns the grpc-timeout the server decoded (ns)
#   /probe.Probe/MetaEcho    returns request metadata as "k=v;...", sets
#                            response metadata + trailers (ascii and -bin)
#   /probe.Probe/FailUnicode raises an error with unicode + '%' in it

from std.sys import argv

from common import to_hex
from echo_pb import EchoRequest, EchoResponse
from grpc import Server, ServerContext


def echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var marker = open("build/grpc_server_probe_dispatch", "a")
    marker.write_all("echo\n".as_bytes())
    marker.close()
    var resp = EchoResponse()
    resp.message = req.message.copy()
    return resp^


def timeout(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String(ctx.timeout_ns)
    return resp^


def peer_certificate(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    var resp = EchoResponse()
    if not ctx.peer_certificate:
        resp.message = String("none")
        return resp^
    var peer = ctx.peer_certificate.value().copy()
    var prefix = String("unverified:")
    if peer.verified:
        prefix = String("verified:")
    resp.message = prefix + to_hex(Span(peer.leaf_der))
    return resp^


def meta_echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    var parts = String()
    for e in ctx.metadata.entries:
        if parts.byte_length() > 0:
            parts += ";"
        parts += e.name + "=" + e.value
    resp.message = parts^
    ctx.response_metadata.add(String("x-initial"), String("from-mojo"))
    ctx.response_trailers.add(String("x-trailer"), String("mojo-trailer"))
    var bin_data: List[Byte] = [0xDE, 0xAD, 0xBE, 0xEF]
    ctx.response_trailers.add_binary(String("x-blob-bin"), Span(bin_data))
    return resp^


def fail_unicode(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    raise Error("falhou: résumé 100% 🔥")


def fail_rich(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    # google.rpc.Status{code: 5, message: "rich"} hand-encoded.
    var details: List[Byte] = [0x08, 0x05, 0x12, 0x04, 0x72, 0x69, 0x63, 0x68]
    ctx.abort_with_details(5, String("rich error"), Span(details))
    return EchoResponse()


def main() raises:
    var args = argv()
    var server: Server
    if len(args) >= 4:
        server = Server.tls(
            "127.0.0.1",
            0,
            args[1],
            args[2],
            client_ca_file=args[3],
            require_client_cert=True,
        )
    elif len(args) >= 3:
        server = Server.tls("127.0.0.1", 0, args[1], args[2])
    else:
        server = Server("127.0.0.1", 0)
    server.register_unary[echo]("/probe.Probe/Echo")
    server.register_unary[timeout]("/probe.Probe/Timeout")
    server.register_unary[peer_certificate]("/probe.Probe/PeerCertificate")
    server.register_unary[meta_echo]("/probe.Probe/MetaEcho")
    server.register_unary[fail_unicode]("/probe.Probe/FailUnicode")
    server.register_unary[fail_rich]("/probe.Probe/FailRich")
    server.serve()
