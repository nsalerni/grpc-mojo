# grpc.health.v1 Check on Server; Watch stays UNIMPLEMENTED.

from std.testing import assert_equal, assert_true

from echo_messages import EchoRequest, EchoResponse
from grpc import (
    HEALTH_CHECK_PATH,
    HEALTH_WATCH_PATH,
    GrpcChannel,
    GrpcTransport,
    Health,
    HealthCheckRequest,
    HealthCheckResponse,
    Metadata,
    Server,
    ServerContext,
    ServingStatus,
    StatusCode,
)
from h2 import Http2Connection
from net import TCPListener
from proto import decode, encode


@fieldwise_init
struct HealthRig(Movable):
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


def make_health_rig() raises -> HealthRig:
    var registry = Health()
    registry.set_status("echo.Echo", ServingStatus.SERVING)
    registry.set_status("down.Down", ServingStatus.NOT_SERVING)
    var server = Server("127.0.0.1", 0)
    server.add_health_service(registry^)
    server.register_unary[echo_handler]("/echo.Echo/Say")
    var listener = TCPListener("127.0.0.1", 0)
    var channel = GrpcChannel.connect("127.0.0.1", listener.local_port)
    var server_tcp = listener.accept()
    var transport = GrpcTransport.plaintext(server_tcp^)
    var server_conn = Http2Connection(transport^, is_client=False)
    listener.close()
    return HealthRig(
        channel=channel^,
        server=server^,
        server_conn=server_conn^,
        handled=List[UInt32](),
    )


def call_check(
    mut rig: HealthRig, var service: String
) raises -> HealthCheckResponse:
    var sid = rig.channel.start_call(HEALTH_CHECK_PATH, Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(HealthCheckRequest(service^))), last=True
    )
    rig.pump()
    rig.channel.conn.wait_headers(sid)
    var msg = rig.channel.recv_response_bytes(sid)
    var result = rig.channel.finish(sid)
    if not result.status.is_ok():
        raise result.status.to_error()
    assert_true(Bool(msg), "Check must return a response message")
    return decode[HealthCheckResponse](Span(msg.value()))


def test_registry_defaults() raises:
    var health = Health()
    assert_equal(health.status("").value(), ServingStatus.SERVING)
    assert_true(not health.status("missing"))
    var overall = health.check(HealthCheckRequest())
    assert_true(overall.grpc_status.is_ok())
    var missing = health.check(HealthCheckRequest(String("nope")))
    assert_equal(missing.grpc_status.code, StatusCode.NOT_FOUND)


def test_check_overall_and_named() raises:
    var rig = make_health_rig()
    var overall = call_check(rig, String(""))
    assert_equal(overall.status, ServingStatus.SERVING)
    var named = call_check(rig, String("echo.Echo"))
    assert_equal(named.status, ServingStatus.SERVING)
    var down = call_check(rig, String("down.Down"))
    assert_equal(down.status, ServingStatus.NOT_SERVING)
    rig.channel.close()
    rig.server_conn.close()


def test_check_unknown_service_not_found() raises:
    var rig = make_health_rig()
    var sid = rig.channel.start_call(HEALTH_CHECK_PATH, Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(HealthCheckRequest(String("missing.Svc")))), last=True
    )
    rig.pump()
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.NOT_FOUND)
    rig.channel.close()
    rig.server_conn.close()


def test_watch_is_unimplemented() raises:
    var rig = make_health_rig()
    var sid = rig.channel.start_call(HEALTH_WATCH_PATH, Metadata())
    rig.channel.send_request_bytes(
        sid, Span(encode(HealthCheckRequest())), last=True
    )
    rig.pump()
    var result = rig.channel.finish(sid)
    assert_equal(result.status.code, StatusCode.UNIMPLEMENTED)
    rig.channel.close()
    rig.server_conn.close()


def main() raises:
    test_registry_defaults()
    test_check_overall_and_named()
    test_check_unknown_service_not_found()
    test_watch_is_unimplemented()
    print("test_grpc_health: ok")
