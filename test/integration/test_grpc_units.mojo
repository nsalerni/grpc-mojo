# Unit tests for the pure-function parts of the grpc package.

from std.testing import assert_equal, assert_true

from common import from_hex, to_hex
from grpc import (
    Metadata,
    Status,
    StatusCode,
    decode_bin_value,
    decode_timeout,
    encode_bin_value,
    encode_timeout,
    frame_message,
    http_status_to_grpc,
    percent_decode_message,
    percent_encode_message,
    rst_code_to_grpc,
    status_code_name,
)


def test_status_names() raises:
    assert_equal(String(status_code_name(0)), "OK")
    assert_equal(String(status_code_name(12)), "UNIMPLEMENTED")
    assert_equal(String(status_code_name(16)), "UNAUTHENTICATED")
    assert_true(Status.ok().is_ok(), "OK is ok")
    assert_equal(
        String(Status(code=5, message=String("x"))), "NOT_FOUND (5): x"
    )


def test_percent_encoding() raises:
    assert_equal(percent_encode_message("plain text"), "plain text")
    assert_equal(percent_encode_message("50% off"), "50%25 off")
    assert_equal(percent_encode_message("newline\n"), "newline%0A")
    # UTF-8 multibyte gets percent-coded byte-wise.
    assert_equal(percent_encode_message("é"), "%C3%A9")
    assert_equal(percent_decode_message("%C3%A9"), "é")
    assert_equal(percent_decode_message("50%25 off"), "50% off")
    # Lenient: broken escapes pass through.
    assert_equal(percent_decode_message("bad%zz"), "bad%zz")
    assert_equal(percent_decode_message("trail%"), "trail%")


def test_timeout_coding() raises:
    assert_equal(encode_timeout(1_000_000_000), "1000000u")
    assert_equal(encode_timeout(1), "1n")
    assert_equal(encode_timeout(99_999_999), "99999999n")
    assert_equal(encode_timeout(100_000_000), "100000u")
    assert_equal(decode_timeout("1S"), 1_000_000_000)
    assert_equal(decode_timeout("1M"), 60_000_000_000)
    assert_equal(decode_timeout("1H"), 3_600_000_000_000)
    assert_equal(decode_timeout("250m"), 250_000_000)
    assert_equal(decode_timeout("7n"), 7)
    # Roundtrip
    var encoded = encode_timeout(30_000_000_000)
    assert_equal(decode_timeout(encoded), 30_000_000_000)
    var raised = False
    try:
        _ = decode_timeout("123456789S")  # 9 digits
    except:
        raised = True
    assert_true(raised, "9-digit timeout must raise")


def test_binary_metadata() raises:
    var data: List[Byte] = [0x00, 0x01, 0xFE, 0xFF]
    var coded = encode_bin_value(Span(data))
    assert_true(not coded.endswith("="), "emit unpadded")
    var back = decode_bin_value(coded)
    assert_equal(to_hex(back), "0001feff")
    # Accept padded too (grpcio pads; the spec says accept both).
    var back2 = decode_bin_value(coded + "==")
    assert_equal(to_hex(back2), "0001feff")

    var md = Metadata()
    md.add(String("x-trace"), String("abc"))
    md.add_binary(String("x-data-bin"), Span(data))
    assert_equal(md.get("x-trace").value(), "abc")
    assert_equal(to_hex(md.get_binary("x-data-bin").value()), "0001feff")

    var raised = False
    try:
        md.add(String("UPPER"), String("x"))
    except:
        raised = True
    assert_true(raised, "uppercase metadata key must raise")

    raised = False
    try:
        md.add(String("grpc-custom"), String("x"))
    except:
        raised = True
    assert_true(raised, "grpc- prefixed key must raise")


def test_message_framing() raises:
    var payload: List[Byte] = [0xAA, 0xBB, 0xCC]
    var framed = frame_message(Span(payload))
    assert_equal(to_hex(framed), "0000000003aabbcc")
    var no_bytes = List[Byte]()
    var empty = frame_message(Span(no_bytes))
    assert_equal(to_hex(empty), "0000000000")


def test_code_mappings() raises:
    assert_equal(http_status_to_grpc(401), StatusCode.UNAUTHENTICATED)
    assert_equal(http_status_to_grpc(404), StatusCode.UNIMPLEMENTED)
    assert_equal(http_status_to_grpc(503), StatusCode.UNAVAILABLE)
    assert_equal(http_status_to_grpc(500), StatusCode.UNKNOWN)
    assert_equal(rst_code_to_grpc(0x7), StatusCode.UNAVAILABLE)
    assert_equal(rst_code_to_grpc(0x8), StatusCode.CANCELLED)
    assert_equal(rst_code_to_grpc(0x1), StatusCode.INTERNAL)


def main() raises:
    test_status_names()
    test_percent_encoding()
    test_timeout_coding()
    test_binary_metadata()
    test_message_framing()
    test_code_mappings()
    print("test_grpc_units: all tests passed")
