# Focused readiness API checks for the gRPC transport wrapper.

from std.testing import assert_equal, assert_false, assert_true

from grpc import GrpcTransport
from net import (
    IOStream,
    Poller,
    ReadinessStream,
    TCPListener,
    TCPStream,
    is_would_block,
)


def accepts_io_stream[S: IOStream](stream: S):
    """Compile-time proof that readiness retains the blocking API."""
    _ = stream


def accepts_readiness_stream[S: ReadinessStream](stream: S):
    """Compile-time proof that `GrpcTransport` satisfies the trait."""
    _ = stream


def test_plaintext_readiness_and_blocking_apis() raises:
    var listener = TCPListener("127.0.0.1", 0)
    var client_tcp = TCPStream.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var client_fd = client_tcp.descriptor()
    var server_fd = server_tcp.descriptor()
    var client = GrpcTransport.plaintext(client_tcp^)
    var server = GrpcTransport.plaintext(server_tcp^)

    accepts_io_stream(client)
    accepts_readiness_stream(client)
    assert_equal(client.descriptor(), client_fd)
    assert_equal(server.descriptor(), server_fd)

    client.set_nonblocking(True)
    server.set_nonblocking(True)
    var empty = List[Byte](length=32, fill=0)
    var blocked = False
    try:
        _ = server.read(empty)
    except error:
        blocked = True
        assert_true(is_would_block(error), String(error))
        assert_false(server.wants_read())
        assert_false(server.wants_write())
    assert_true(blocked, "empty h2c read reports would-block")

    var payload = String("partial h2c transport")
    assert_equal(client.write_some(payload.as_bytes()), payload.byte_length())
    var poller = Poller()
    poller.register(server.descriptor(), readable=True, writable=False)
    assert_true(len(poller.wait(2000)) > 0, "server becomes readable")
    var buf = List[Byte](length=64, fill=0)
    assert_equal(server.read(buf), payload.byte_length())
    assert_equal(String(from_utf8=buf), payload)
    poller.close()

    # Switching back proves the inherited blocking API still delegates to
    # the same owned stream.
    client.set_nonblocking(False)
    server.set_nonblocking(False)
    client.write_all("ok".as_bytes())
    assert_equal(String(from_utf8=server.read_exact(2)), "ok")

    client.close()
    server.close()
    listener.close()


def main() raises:
    test_plaintext_readiness_and_blocking_apis()
    print("test_grpc_transport: all tests passed")
