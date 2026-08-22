# Drive GrpcTransport partial I/O against a CPython TCP or TLS peer.
#
# Usage: grpc_transport_readiness_probe <tcp|tls> <port> <size> [ca-file]

from std.sys import argv

from grpc import GrpcTransport
from net import Poller, ReadinessStream, TCPStream, is_would_block
from tls import TLSContext


def accepts_readiness_stream[S: ReadinessStream](stream: S):
    """Compile-time proof that the wrapper satisfies the readiness trait."""
    _ = stream


def wait_for(
    mut poller: Poller,
    transport: GrpcTransport,
    readable: Bool,
    writable: Bool,
) raises:
    """Waits until the active descriptor satisfies the requested direction."""
    poller.modify(transport.descriptor(), readable=readable, writable=writable)
    for _ in range(20):
        var events = poller.wait(5000)
        for event in events:
            if event.fd != transport.descriptor():
                continue
            if (readable and event.readable) or (writable and event.writable):
                return
            if event.error or event.hangup:
                raise Error("transport readiness peer closed early")
    raise Error("transport readiness wait timed out")


def main() raises:
    var args = argv()
    if len(args) < 4 or len(args) > 5:
        raise Error(
            "usage: grpc_transport_readiness_probe <tcp|tls> <port> <size> "
            "[ca-file]"
        )
    var mode = String(args[1])
    var port = UInt16(Int(args[2]))
    var size = Int(args[3])
    var tcp = TCPStream.connect("127.0.0.1", port)
    var transport: GrpcTransport
    if mode == "tls":
        if len(args) != 5:
            raise Error("TLS mode requires a CA file")
        var context = TLSContext.client(ca_file=String(args[4]), alpn=["h2"])
        var stream = context.connect(tcp^, "localhost")
        transport = GrpcTransport.secure(stream^)
    elif mode == "tcp":
        transport = GrpcTransport.plaintext(tcp^)
    else:
        raise Error("unknown transport mode")

    accepts_readiness_stream(transport)
    transport.set_nonblocking(True)
    var poller = Poller()
    poller.register(transport.descriptor(), readable=True, writable=True)

    var read_waits = 0
    var write_waits = 0
    var read_need_read = 0
    var read_need_write = 0
    var write_need_read = 0
    var write_need_write = 0

    # The peer delays this marker. Retry the exact read buffer after the
    # readiness direction is observed before starting any other TLS operation.
    var marker = List[Byte](length=1, fill=0)
    while True:
        try:
            var got = transport.read(marker)
            if got != 1 or marker[0] != 0x52:
                raise Error("invalid readiness marker")
            break
        except error:
            if not is_would_block(error):
                raise error
            read_waits += 1
            var need_read = True
            var need_write = False
            if mode == "tls":
                need_read = transport.wants_read()
                need_write = transport.wants_write()
                if need_read == need_write:
                    raise Error("TLS read did not preserve one retry direction")
                read_need_read += Int(need_read)
                read_need_write += Int(need_write)
            wait_for(poller, transport, need_read, need_write)

    var payload = List[Byte](length=size, fill=0x5A)
    var sent = 0
    var writes = 0
    while sent < size:
        try:
            var n = transport.write_some(Span(payload)[sent:size])
            if n <= 0:
                raise Error("partial write made no progress")
            sent += n
            writes += 1
        except error:
            if not is_would_block(error):
                raise error
            write_waits += 1
            var need_read = False
            var need_write = True
            if mode == "tls":
                need_read = transport.wants_read()
                need_write = transport.wants_write()
                if need_read == need_write:
                    raise Error(
                        "TLS write did not preserve one retry direction"
                    )
                write_need_read += Int(need_read)
                write_need_write += Int(need_write)
            wait_for(poller, transport, need_read, need_write)

    var received = 0
    var reads = 0
    while received < size:
        var chunk = List[Byte](length=min(65536, size - received), fill=0)
        while True:
            try:
                var got = transport.read(chunk)
                if got == 0:
                    raise Error("transport closed before echo completed")
                for byte in chunk:
                    if byte != 0x5A:
                        raise Error("echo payload mismatch")
                received += got
                reads += 1
                break
            except error:
                if not is_would_block(error):
                    raise error
                read_waits += 1
                var need_read = True
                var need_write = False
                if mode == "tls":
                    need_read = transport.wants_read()
                    need_write = transport.wants_write()
                    if need_read == need_write:
                        raise Error(
                            "TLS read did not preserve one retry direction"
                        )
                    read_need_read += Int(need_read)
                    read_need_write += Int(need_write)
                wait_for(poller, transport, need_read, need_write)

    poller.close()
    transport.close()
    print(
        "OK",
        sent,
        received,
        writes,
        reads,
        write_waits,
        read_waits,
        write_need_read,
        write_need_write,
        read_need_read,
        read_need_write,
    )
