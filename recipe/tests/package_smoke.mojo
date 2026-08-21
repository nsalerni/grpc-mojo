from std.testing import assert_equal, assert_true

from grpc import (
    Metadata,
    Status,
    StatusCode,
    decode_timeout,
    encode_timeout,
    frame_message,
    status_code_name,
)
from net import resolve
from proto import ProtoMessage, WireReader, WireWriter, decode, encode
from tls import TLSContext


struct PackageMessage(Copyable, Defaultable, Movable, ProtoMessage):
    var value: String
    var _unknown: List[Byte]

    def __init__(out self):
        self.value = String()
        self._unknown = List[Byte]()

    def __init__(out self, var value: String):
        self.value = value^
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        if self.value.byte_length() != 0:
            writer.string_field(1, self.value)
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        while not reader.done():
            var tag = reader.read_tag()
            if tag[0] == 1:
                self.value = reader.string_value()
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


def main() raises:
    assert_true(Status.ok().is_ok())
    assert_equal(String(status_code_name(StatusCode.UNAVAILABLE)), "UNAVAILABLE")
    assert_equal(encode_timeout(1_500), "1500n")
    assert_equal(decode_timeout("2m"), 2_000_000)

    var encoded = encode(PackageMessage(value="installed"))
    var decoded = decode[PackageMessage](Span(encoded))
    assert_equal(decoded.value, "installed")

    var framed = frame_message(Span(encoded))
    assert_equal(len(framed), len(encoded) + 5)
    assert_equal(framed[0], 0)

    var metadata = Metadata()
    metadata.add("x-package", "installed")
    assert_equal(metadata.get("x-package").value(), "installed")
    assert_equal(len(metadata.entries), 1)

    var addresses = resolve("127.0.0.1", 443)
    assert_true(len(addresses) >= 1)
    assert_equal(addresses[0].port, 443)

    _ = TLSContext.client(verify=False, alpn=["h2"])
    print("grpc-mojo package smoke test passed")
