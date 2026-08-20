# gRPC over TLS functional test with a real Mojo client and server.

from std.ffi import c_int, external_call
from std.testing import assert_equal
from std.time import sleep

from echo_pb import EchoRequest, EchoResponse
from grpc import GrpcChannel, Server, ServerContext
from net import TCPListener


comptime CA = "build/certs/ca.pem"
comptime SERVER_CERT = "build/certs/server.pem"
comptime SERVER_KEY = "build/certs/server.key"
comptime SAY_PATH = "/echo.Echo/Say"


def echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var response = EchoResponse()
    response.message = String("tls: ") + req.message
    return response^


def test_unary_over_tls() raises:
    # Reserve an ephemeral port before the fork. The child binds it after
    # the temporary listener closes.
    var probe = TCPListener("127.0.0.1", 0)
    var port = probe.local_port
    probe.close()

    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = Server.tls(
                "127.0.0.1", port, String(SERVER_CERT), String(SERVER_KEY)
            )
            server.register_unary[echo](SAY_PATH)
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(1))

    sleep(1.0)
    var channel = GrpcChannel.connect_tls(
        "127.0.0.1",
        port,
        server_name=String("localhost"),
        ca_file=String(CA),
    )
    var request = EchoRequest()
    request.message = "hello"
    var response = channel.unary[EchoRequest, EchoResponse](SAY_PATH, request)
    assert_equal(response.message, "tls: hello")
    assert_equal(channel.scheme, "https")
    channel.close()

    _ = external_call["kill", c_int](pid, c_int(15))
    var status = c_int(0)
    _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))


def main() raises:
    test_unary_over_tls()
    print("test_grpc_tls: ok")
