# PollingServer streaming: all four RPC kinds on h2c, plus a TLS smoke.

from std.ffi import c_int, external_call
from std.testing import assert_equal, assert_true
from std.time import sleep

from echo_pb import (
    EchoRequest,
    EchoResponse,
    ECHO_CHAT_PATH,
    ECHO_JOIN_PATH,
    ECHO_SAY_PATH,
    ECHO_SPLIT_PATH,
)
from grpc import (
    BidiStreamingCall,
    ClientStreamingCall,
    GrpcChannel,
    PollingServer,
    ServerCall,
    ServerContext,
    ServerStreamingCall,
)
from net import TCPListener


comptime CA = "build/certs/ca.pem"
comptime SERVER_CERT = "build/certs/server.pem"
comptime SERVER_KEY = "build/certs/server.key"


def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    _ = ctx
    var response = EchoResponse()
    response.message = String("polling: ") + req.message
    return response^


def split(
    req: EchoRequest, mut ctx: ServerContext, mut call: ServerCall
) raises:
    for word in req.message.split(" "):
        if word.byte_length() == 0:
            continue
        var resp = EchoResponse()
        resp.message = String(word)
        call.send[EchoResponse](ctx, resp)


def join(mut ctx: ServerContext, mut call: ServerCall) raises -> EchoResponse:
    _ = ctx
    var parts = String()
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        if parts.byte_length() > 0:
            parts += "+"
        parts += msg.value().message
    var out = EchoResponse()
    out.message = parts^
    return out^


def chat(mut ctx: ServerContext, mut call: ServerCall) raises:
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            break
        var resp = EchoResponse()
        resp.message = String("pong: ") + msg.value().message
        call.send[EchoResponse](ctx, resp)


def echo_req(text: StringSpan) -> EchoRequest:
    var request = EchoRequest()
    request.message = String(text)
    return request^


def bind_port() raises -> UInt16:
    var probe = TCPListener("127.0.0.1", 0)
    var port = probe.local_port
    probe.close()
    return port


def wait_channel(host: StringSpan, port: UInt16) raises -> GrpcChannel:
    for _ in range(200):
        try:
            return GrpcChannel.connect(host, port)
        except:
            sleep(0.01)
    raise Error("PollingServer h2c did not accept")


def wait_tls_channel(port: UInt16) raises -> GrpcChannel:
    for _ in range(200):
        try:
            return GrpcChannel.connect_tls(
                "127.0.0.1",
                port,
                server_name=String("localhost"),
                ca_file=String(CA),
            )
        except:
            sleep(0.01)
    raise Error("PollingServer TLS did not accept")


def stop_child(pid: c_int):
    _ = external_call["kill", c_int](pid, c_int(9))
    var status = c_int(0)
    _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))


def register_echo(mut server: PollingServer) raises:
    server.register_unary[say](ECHO_SAY_PATH)
    server.register_server_streaming[split](ECHO_SPLIT_PATH)
    server.register_client_streaming[join](ECHO_JOIN_PATH)
    server.register_bidi[chat](ECHO_CHAT_PATH)


def test_h2c_four_kinds() raises:
    var port = bind_port()
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer("127.0.0.1", port)
            register_echo(server)
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(1))

    try:
        var channel = wait_channel("127.0.0.1", port)

        var unary_req = EchoRequest()
        unary_req.message = "hello"
        var unary = channel.unary[EchoRequest, EchoResponse](
            ECHO_SAY_PATH,
            unary_req,
            timeout_ns=5_000_000_000,
        )
        assert_equal(unary.message, "polling: hello")

        var split_req = EchoRequest()
        split_req.message = "alpha beta gamma"
        var split_call = ServerStreamingCall[EchoResponse].start[EchoRequest](
            channel,
            ECHO_SPLIT_PATH,
            split_req,
            timeout_ns=5_000_000_000,
        )
        var words = List[String]()
        while True:
            var msg = split_call.recv()
            if not msg:
                break
            words.append(msg.value().message.copy())
        assert_equal(len(words), 3)
        assert_equal(words[0], "alpha")
        assert_equal(words[1], "beta")
        assert_equal(words[2], "gamma")
        var split_status = split_call.finish()
        assert_true(split_status.status.is_ok(), "server-streaming OK")

        var join_call = ClientStreamingCall[EchoRequest, EchoResponse].start(
            channel, ECHO_JOIN_PATH, timeout_ns=5_000_000_000
        )
        join_call.send(echo_req("a"))
        join_call.send(echo_req("bb"))
        join_call.send(echo_req("ccc"))
        var joined = join_call.finish()
        assert_equal(joined.message, "a+bb+ccc")

        var chat_call = BidiStreamingCall[EchoRequest, EchoResponse].start(
            channel, ECHO_CHAT_PATH, timeout_ns=5_000_000_000
        )
        chat_call.send(echo_req("x"))
        var first = chat_call.recv()
        assert_true(Bool(first), "bidi first response")
        assert_equal(first.value().message, "pong: x")
        chat_call.send(echo_req("y"))
        var second = chat_call.recv()
        assert_true(Bool(second), "bidi second response")
        assert_equal(second.value().message, "pong: y")
        chat_call.close_send()
        var chat_status = chat_call.finish()
        assert_true(chat_status.status.is_ok(), "bidi OK")

        channel.close()
    except e:
        stop_child(pid)
        raise e

    stop_child(pid)


def test_tls_server_streaming() raises:
    var port = bind_port()
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer.tls(
                "127.0.0.1", port, String(SERVER_CERT), String(SERVER_KEY)
            )
            register_echo(server)
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(1))

    try:
        var channel = wait_tls_channel(port)
        var tls_req = echo_req("one two")
        var call = ServerStreamingCall[EchoResponse].start[EchoRequest](
            channel,
            ECHO_SPLIT_PATH,
            tls_req,
            timeout_ns=5_000_000_000,
        )
        var words = List[String]()
        while True:
            var msg = call.recv()
            if not msg:
                break
            words.append(msg.value().message.copy())
        assert_equal(len(words), 2)
        assert_equal(words[0], "one")
        assert_equal(words[1], "two")
        var status = call.finish()
        assert_true(status.status.is_ok(), "tls server-streaming OK")
        assert_equal(channel.scheme, "https")
        channel.close()
    except e:
        stop_child(pid)
        raise e

    stop_child(pid)


def main() raises:
    test_h2c_four_kinds()
    test_tls_server_streaming()
    print("test_polling_streaming: ok")
