# PollingServer over Unix domain sockets, including stale-path handling.

from std.ffi import c_int, external_call
from std.testing import assert_equal
from std.time import sleep

from echo_pb import EchoRequest, EchoResponse
from grpc import GrpcChannel, PollingServer, ServerContext
from net import UnixListener, UnixStream


comptime SAY_PATH = "/echo.Echo/Say"


def socket_path(tag: StringSpan) -> String:
    var pid = external_call["getpid", c_int]()
    return (
        String("/tmp/grpc-mojo-polling-")
        + String(tag)
        + "-"
        + String(Int(pid))
        + ".sock"
    )


def remove_socket(var path: String):
    _ = external_call["unlink", c_int](path.as_c_string_slice())


def stop_child(pid: c_int, var path: String):
    _ = external_call["kill", c_int](pid, c_int(9))
    var status = c_int(0)
    _ = external_call["waitpid", c_int](pid, Pointer(to=status), c_int(0))
    remove_socket(path^)


def wait_for_socket(path: String) raises:
    for _ in range(100):
        try:
            var probe = UnixStream.connect(path)
            probe.close()
            return
        except:
            sleep(0.01)
    raise Error("PollingServer.unix did not create its socket")


def echo(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var response = EchoResponse()
    response.message = String("polling-unix: ") + req.message
    return response^


def test_unary_over_polling_unix() raises:
    var path = socket_path("unary")
    remove_socket(path.copy())
    var stale = UnixListener(path)
    stale.close()
    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer.unix(path, remove_existing=True)
            server.register_unary[echo](SAY_PATH)
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(1))
        external_call["_exit", NoneType](c_int(2))

    try:
        wait_for_socket(path)
        var channel = GrpcChannel.connect_unix(path)
        var request = EchoRequest()
        request.message = "hello"
        var response = channel.unary[EchoRequest, EchoResponse](
            SAY_PATH, request, timeout_ns=5_000_000_000
        )
        assert_equal(response.message, "polling-unix: hello")
        assert_equal(channel.authority, "localhost")
        assert_equal(channel.scheme, "http")
        channel.close()
    except e:
        stop_child(pid, path.copy())
        raise e

    stop_child(pid, path^)


def test_stale_path_requires_opt_in() raises:
    var path = socket_path("stale")
    var stale = UnixListener(path, remove_existing=True)
    stale.close()

    var pid = external_call["fork", c_int]()
    if pid == 0:
        try:
            var server = PollingServer.unix(path)
            server.serve()
        except:
            external_call["_exit", NoneType](c_int(0))
        external_call["_exit", NoneType](c_int(2))

    var status = c_int(0)
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
        stop_child(pid, path.copy())
        raise Error("PollingServer.unix unexpectedly accepted a stale path")
    remove_socket(path^)
    assert_equal(Int(status), 0)


def main() raises:
    test_unary_over_polling_unix()
    test_stale_path_requires_opt_in()
    print("test_polling_unix: ok")
