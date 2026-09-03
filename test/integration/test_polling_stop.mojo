# PollingServer.request_stop and SIGTERM drain.

from std.ffi import c_int, external_call
from std.testing import assert_equal
from std.time import sleep

from echo_pb import EchoRequest, EchoResponse
from grpc import GrpcChannel, PollingServer, ServerContext
from net import UnixStream


comptime SAY_PATH = "/echo.Echo/Say"
comptime SIGTERM = 15


def socket_path(tag: StringSpan) -> String:
    var pid = external_call["getpid", c_int]()
    return (
        String("/tmp/grpc-mojo-stop-")
        + String(tag)
        + "-"
        + String(Int(pid))
        + ".sock"
    )


def remove_socket(var path: String):
    _ = external_call["unlink", c_int](path.as_c_string_slice())


def wait_for_socket(path: String) raises:
    for _ in range(100):
        try:
            var probe = UnixStream.connect(path)
            probe.close()
            return
        except:
            sleep(0.01)
    raise Error("PollingServer stop test did not create its socket")


def stop_echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    ctx.request_stop()
    var response = EchoResponse()
    response.message = String("bye: ") + req.message
    return response^


def idle_echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    _ = ctx
    var response = EchoResponse()
    response.message = String("idle: ") + req.message
    return response^


def test_handler_request_stop() raises:
    var path = socket_path("handler")
    remove_socket(path.copy())
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer.unix(path, remove_existing=True)
            server.register_unary[stop_echo](SAY_PATH)
            server.serve()
            external_call["_exit", NoneType](c_int(0))
        except:
            external_call["_exit", NoneType](c_int(1))

    var status = c_int(0)
    try:
        wait_for_socket(path)
        var channel = GrpcChannel.connect_unix(path)
        var request = EchoRequest()
        request.message = "stop"
        var response = channel.unary[EchoRequest, EchoResponse](
            SAY_PATH, request, timeout_ns=5_000_000_000
        )
        assert_equal(response.message, "bye: stop")
        channel.close()
        var child_done = False
        for _ in range(100):
            var waited = external_call["waitpid", c_int](
                pid, Pointer(to=status), c_int(1)
            )
            if waited == pid:
                child_done = True
                break
            sleep(0.01)
        if not child_done:
            _ = external_call["kill", c_int](pid, c_int(9))
            _ = external_call["waitpid", c_int](
                pid, Pointer(to=status), c_int(0)
            )
            raise Error("PollingServer.serve did not return after request_stop")
    except e:
        _ = external_call["kill", c_int](pid, c_int(9))
        _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))
        remove_socket(path.copy())
        raise e

    remove_socket(path^)
    assert_equal(Int(status), 0)


def test_sigterm_wakes_serve() raises:
    var path = socket_path("sigterm")
    remove_socket(path.copy())
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer.unix(path, remove_existing=True)
            server.register_unary[idle_echo](SAY_PATH)
            server.install_stop_signals()
            server.serve()
            external_call["_exit", NoneType](c_int(0))
        except:
            external_call["_exit", NoneType](c_int(1))

    var status = c_int(0)
    try:
        wait_for_socket(path)
        _ = external_call["kill", c_int](pid, c_int(SIGTERM))
        var child_done = False
        for _ in range(100):
            var waited = external_call["waitpid", c_int](
                pid, Pointer(to=status), c_int(1)
            )
            if waited == pid:
                child_done = True
                break
            sleep(0.01)
        if not child_done:
            _ = external_call["kill", c_int](pid, c_int(9))
            _ = external_call["waitpid", c_int](
                pid, Pointer(to=status), c_int(0)
            )
            raise Error("SIGTERM did not stop PollingServer.serve")
    except e:
        _ = external_call["kill", c_int](pid, c_int(9))
        _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))
        remove_socket(path.copy())
        raise e

    remove_socket(path^)
    assert_equal(Int(status), 0)


def main() raises:
    test_handler_request_stop()
    test_sigterm_wakes_serve()
    print("test_polling_stop: ok")
