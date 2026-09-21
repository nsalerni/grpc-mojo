# gRPC server reflection on Server; v1alpha shares the same handler.

from std.testing import assert_equal, assert_true

from echo_messages import EchoRequest, EchoResponse
from grpc import (
    REFLECTION_V1_PATH,
    REFLECTION_V1ALPHA_PATH,
    GrpcChannel,
    GrpcTransport,
    Metadata,
    ReflectionRegistry,
    Server,
    ServerContext,
    ServerReflectionRequest,
    ServerReflectionResponse,
    StatusCode,
)
from h2 import Http2Connection
from net import TCPListener
from proto import decode, encode


@fieldwise_init
struct ReflectionRig(Movable):
    var channel: GrpcChannel
    var server: Server
    var server_conn: Http2Connection[GrpcTransport]
    var handled: List[UInt32]

    def pump(mut self) raises:
        while True:
            self.server_conn.process_next_frame()
            if self.server.dispatch_ready(self.server_conn, self.handled) > 0:
                return


def echo_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    _ = ctx
    return EchoResponse(message=req.message.copy())


def make_rig() raises -> ReflectionRig:
    var registry = ReflectionRegistry()
    var proto: List[Byte] = [0x0A, 0x0A, 0x65, 0x63, 0x68, 0x6F, 0x2E, 0x70]
    proto.append(0x72)
    proto.append(0x6F)
    proto.append(0x74)
    proto.append(0x6F)
    registry.add_file(
        String("echo.proto"),
        proto^,
        [String("echo.Echo"), String("echo.EchoRequest")],
    )
    registry.add_service(String("echo.Echo"))
    var server = Server("127.0.0.1", 0)
    server.add_reflection(registry^)
    server.register_unary[echo_handler]("/echo.Echo/Say")
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return ReflectionRig(
        channel=channel^,
        server=server^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def reflect(
    mut rig: ReflectionRig, request: ServerReflectionRequest, path: StringSpan
) raises -> ServerReflectionResponse:
    var sid = rig.channel.start_call(path, Metadata())
    rig.channel.send_request_bytes(sid, Span(encode(request)), last=True)
    rig.pump()
    rig.channel.conn.wait_headers(sid)
    var msg = rig.channel.recv_response_bytes(sid)
    var result = rig.channel.finish(sid)
    if not result.status.is_ok():
        raise result.status.to_error()
    assert_true(Bool(msg), "reflection must return a response message")
    return decode[ServerReflectionResponse](Span(msg.value()))


def test_list_services() raises:
    var rig = make_rig()
    var req = ServerReflectionRequest()
    req.kind = 7
    var resp = reflect(rig, req, REFLECTION_V1_PATH)
    assert_equal(resp.kind, 6)
    var found_echo = False
    var found_reflection = False
    for name in resp.service_names:
        if name == "echo.Echo":
            found_echo = True
        if name == "grpc.reflection.v1.ServerReflection":
            found_reflection = True
    assert_true(found_echo, "list_services includes echo.Echo")
    assert_true(
        found_reflection, "list_services includes ServerReflection"
    )
    rig.channel.close()
    rig.server_conn.close()


def test_file_by_filename() raises:
    var rig = make_rig()
    var req = ServerReflectionRequest()
    req.kind = 3
    req.file_by_filename = String("echo.proto")
    var resp = reflect(rig, req, REFLECTION_V1ALPHA_PATH)
    assert_equal(resp.kind, 4)
    assert_equal(len(resp.file_descriptor_proto), 1)
    assert_equal(len(resp.file_descriptor_proto[0]), 12)
    rig.channel.close()
    rig.server_conn.close()


def test_unknown_symbol() raises:
    var rig = make_rig()
    var req = ServerReflectionRequest()
    req.kind = 4
    req.file_containing_symbol = String("no.Such")
    var resp = reflect(rig, req, REFLECTION_V1_PATH)
    assert_equal(resp.kind, 7)
    assert_equal(resp.error_code, StatusCode.NOT_FOUND)
    rig.channel.close()
    rig.server_conn.close()


def main() raises:
    test_list_services()
    test_file_by_filename()
    test_unknown_symbol()
    print("test_grpc_reflection: all tests passed")
