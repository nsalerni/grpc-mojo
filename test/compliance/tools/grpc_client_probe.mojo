# Compliance tool: Mojo gRPC client probes against the reference grpcio
# server run by run_compliance.py.
#
# Usage: grpc_client_probe <port> <mode> [arg]
#   status <code>   call /probe.Probe/Fail, expect abort with <code>;
#                   prints "code=<n> message=<details>"
#   meta            call /probe.Probe/MetaEcho with ascii+binary metadata;
#                   prints response + initial/trailing metadata seen
#   deadline <ms>   call /probe.Probe/Deadline with that timeout;
#                   prints server-observed remaining ms
#   echo <nbytes>   unary echo of an n-byte ASCII payload; prints length
#                   and match
#   unicode         unary echo of a unicode string; prints response

from std.sys import argv

from common import to_hex
from echo_pb import EchoRequest, EchoResponse
from grpc import GrpcChannel, Metadata
from proto import decode, encode


def unary_text(
    mut channel: GrpcChannel,
    path: StringSpan,
    var text: String,
    metadata: Metadata,
    timeout_ns: Int64,
) raises -> Tuple[String, Int, String]:
    """Returns (response_message, status_code, status_message)."""
    var req = EchoRequest()
    req.message = text^
    var result = channel.unary_bytes(
        path, Span(encode(req)), metadata, timeout_ns=timeout_ns
    )
    var msg = String()
    if len(result.response) > 0:
        var resp = decode[EchoResponse](Span(result.response))
        msg = resp.message.copy()
    return (msg^, result.status.code, result.status.message.copy())


def main() raises:
    var args = argv()
    var port = UInt16(Int(args[1]))
    var mode = args[2]
    var channel: GrpcChannel
    if len(args) >= 8 and args[len(args) - 5] == "tls":
        channel = GrpcChannel.connect_tls(
            "127.0.0.1",
            port,
            ca_file=args[len(args) - 4],
            server_name=args[len(args) - 3],
            cert_chain_pem=args[len(args) - 2],
            key_pem=args[len(args) - 1],
        )
    elif len(args) >= 6 and args[len(args) - 3] == "tls":
        channel = GrpcChannel.connect_tls(
            "127.0.0.1",
            port,
            ca_file=args[len(args) - 2],
            server_name=args[len(args) - 1],
        )
    else:
        channel = GrpcChannel.connect("127.0.0.1", port)

    if mode == "status":
        var r = unary_text(
            channel, "/probe.Probe/Fail", String(args[3]), Metadata(), 0
        )
        print("code=", r[1], " message=", r[2], sep="")

    elif mode == "meta":
        var md = Metadata()
        md.add(String("x-ascii"), String("hello meta"))
        var blob: List[Byte] = [0x01, 0x02, 0xFF, 0x00]
        md.add_binary(String("x-payload-bin"), Span(blob))
        var sid = channel.start_call("/probe.Probe/MetaEcho", md)
        var req = EchoRequest()
        req.message = "m"
        channel.send_request_bytes(sid, Span(encode(req)), last=True)
        channel.conn.wait_headers(sid)
        var msg = channel.recv_response_bytes(sid)
        var text = String()
        if msg:
            text = decode[EchoResponse](Span(msg.value())).message.copy()
        var result = channel.finish(sid)
        print("response=", text, sep="")
        var initial = result.initial_metadata.get("x-initial")
        print(
            "initial=",
            initial.value() if initial else String("-"),
            sep="",
        )
        var trailer = result.trailing_metadata.get("x-trailer")
        print(
            "trailer=",
            trailer.value() if trailer else String("-"),
            sep="",
        )
        var blob_back = result.trailing_metadata.get_binary("x-blob-bin")
        print(
            "trailer-bin=",
            to_hex(blob_back.value()) if blob_back else String("-"),
            sep="",
        )
        print("status=", result.status.code, sep="")

    elif mode == "deadline":
        var ms = Int(args[3])
        var r = unary_text(
            channel,
            "/probe.Probe/Deadline",
            String("d"),
            Metadata(),
            Int64(ms) * 1_000_000,
        )
        print("remaining_ms=", r[0], " code=", r[1], sep="")

    elif mode == "echo":
        var n = Int(args[3])
        var payload = String(capacity=n)
        for i in range(n):
            payload += "abcdefghij"[byte = i % 10 : i % 10 + 1]
        var r = unary_text(
            channel, "/probe.Probe/Echo", payload.copy(), Metadata(), 0
        )
        print(
            "len=",
            r[0].byte_length(),
            " match=",
            r[0] == payload,
            " code=",
            r[1],
            sep="",
        )

    elif mode == "sleep":
        var ms = Int(args[3])
        var r = unary_text(
            channel,
            "/probe.Probe/Sleep",
            String("s"),
            Metadata(),
            Int64(ms) * 1_000_000,
        )
        print("code=", r[1], " message=", r[2], sep="")

    elif mode == "richstatus":
        var req = EchoRequest()
        req.message = "r"
        var result = channel.unary_bytes(
            "/probe.Probe/FailRich", Span(encode(req)), Metadata()
        )
        print(
            "code=",
            result.status.code,
            " details=",
            to_hex(result.status.details_bin),
            sep="",
        )

    elif mode == "unicode":
        var text = String("héllo wörld 你好 🔥 %25 100%")
        var r = unary_text(
            channel, "/probe.Probe/Echo", text.copy(), Metadata(), 0
        )
        print("match=", r[0] == text, " code=", r[1], sep="")

    else:
        raise Error("unknown mode")
    channel.close()
