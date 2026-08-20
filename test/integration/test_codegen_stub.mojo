# Generated gRPC service stubs: compile-level and constant checks.
# (The message-level codegen goldens live in packages/protomojo/test.)

from std.testing import assert_equal

from common import to_hex
from echo_pb import ECHO_SAY_PATH, EchoClient, EchoRequest
from proto import encode


def test_generated_service_stub() raises:
    assert_equal(String(ECHO_SAY_PATH), "/echo.Echo/Say")
    var req = EchoRequest()
    req.message = "x"
    assert_equal(to_hex(encode(req)), "0a0178")


def main() raises:
    test_generated_service_stub()
    print("test_codegen_stub: all tests passed")
