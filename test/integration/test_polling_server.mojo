# Focused API and event-loop invariant checks for PollingServer.

from std.ffi import c_int
from std.testing import assert_equal, assert_true

from grpc import PollingServer, PollingServerConfig, ServerContext
from grpc.polling_server import (
    _PendingWrite,
    _can_move_http2_output,
    _coalesce_poll_events,
)
from net import PollEvent


def echo_bytes(
    request: List[Byte], mut ctx: ServerContext
) raises -> List[Byte]:
    _ = ctx
    return request.copy()


def test_config_and_registration() raises:
    var config = PollingServerConfig(
        max_connections=4,
        max_message_size=1024,
        max_read_bytes_per_event=2048,
        max_frames_per_event=8,
        max_write_bytes_per_event=4096,
        max_pending_output_size=64 * 1024,
    )
    config.validate()
    var server = PollingServer("127.0.0.1", 0, config)
    server.register_unary_bytes[echo_bytes]("/probe.Probe/Echo")

    var raised = False
    try:
        server.register_unary_bytes[echo_bytes]("/probe.Probe/Echo")
    except:
        raised = True
    assert_true(raised, "duplicate routes must raise")

    raised = False
    try:
        var invalid = PollingServerConfig(max_pending_output_size=1023)
        invalid.validate()
    except:
        raised = True
    assert_true(raised, "output must admit built-in response headers")

    for path in ["/", "/service", "/service/", "/service/method/extra"]:
        raised = False
        try:
            server.register_unary_bytes[echo_bytes](path)
        except:
            raised = True
        assert_true(raised, "invalid route shape must raise: " + path)

    raised = False
    try:
        var too_large = PollingServerConfig(max_message_size=0x100000000)
        too_large.validate()
    except:
        raised = True
    assert_true(raised, "message limit must fit the 32-bit gRPC prefix")

    raised = False
    try:
        var no_accepts = PollingServerConfig(max_accepts_per_event=0)
        no_accepts.validate()
    except:
        raised = True
    assert_true(raised, "accept work must be bounded and positive")

    raised = False
    try:
        var no_idle = PollingServerConfig(idle_timeout_ms=0)
        no_idle.validate()
    except:
        raised = True
    assert_true(raised, "connection inactivity must have a positive bound")

    raised = False
    try:
        var no_request_bound = PollingServerConfig(
            incomplete_request_timeout_ms=0
        )
        no_request_bound.validate()
    except:
        raised = True
    assert_true(raised, "incomplete requests must have a positive bound")

    raised = False
    try:
        var no_handshakes = PollingServerConfig(max_pending_handshakes=0)
        no_handshakes.validate()
    except:
        raised = True
    assert_true(raised, "pending TLS handshakes must have a positive bound")

    raised = False
    try:
        var no_handshake_steps = PollingServerConfig(
            max_handshake_steps_per_event=0
        )
        no_handshake_steps.validate()
    except:
        raised = True
    assert_true(raised, "TLS handshake work must have a positive bound")

    raised = False
    try:
        var no_handshake_timeout = PollingServerConfig(
            tls_handshake_timeout_ms=0
        )
        no_handshake_timeout.validate()
    except:
        raised = True
    assert_true(raised, "TLS handshakes must have an absolute timeout")

    raised = False
    try:
        var idle_overflow = PollingServerConfig(idle_timeout_ms=0x80000000)
        idle_overflow.validate()
    except:
        raised = True
    assert_true(raised, "idle timeout must fit the Poller millisecond ABI")

    raised = False
    try:
        var request_overflow = PollingServerConfig(
            incomplete_request_timeout_ms=0x80000000
        )
        request_overflow.validate()
    except:
        raised = True
    assert_true(raised, "request timeout must fit the Poller millisecond ABI")

    raised = False
    try:
        var handshake_overflow = PollingServerConfig(
            tls_handshake_timeout_ms=0x80000000
        )
        handshake_overflow.validate()
    except:
        raised = True
    assert_true(raised, "handshake timeout must fit the Poller millisecond ABI")

    var tls_server = PollingServer.tls(
        "127.0.0.1",
        0,
        "build/certs/server.pem",
        "build/certs/server.key",
        config,
    )
    tls_server.register_unary_bytes[echo_bytes]("/probe.Probe/Echo")


def test_duplicate_poll_events_coalesce() raises:
    var events = [
        PollEvent(
            fd=c_int(7),
            readable=True,
            writable=False,
            hangup=True,
            error=False,
        ),
        PollEvent(
            fd=c_int(7),
            readable=False,
            writable=True,
            hangup=False,
            error=True,
        ),
        PollEvent(
            fd=c_int(9),
            readable=True,
            writable=False,
            hangup=False,
            error=False,
        ),
    ]
    var merged = _coalesce_poll_events(Span(events))
    assert_equal(len(merged), 2)
    assert_equal(merged[0].fd, c_int(7))
    assert_true(merged[0].readable)
    assert_true(merged[0].writable)
    assert_true(merged[0].hangup)
    assert_true(merged[0].error)


def test_partial_write_keeps_exact_suffix() raises:
    var pending = _PendingWrite()
    var wire: List[Byte] = [1, 2, 3, 4, 5, 6]
    var complete = wire.copy()
    pending.replace(wire^)
    var raised = False
    try:
        pending.advance(0)
    except:
        raised = True
    assert_true(raised, "a non-empty zero-byte write is terminal")
    assert_equal(pending.copy_suffix(), complete)
    pending.advance(2)
    var suffix: List[Byte] = [3, 4, 5, 6]
    assert_equal(pending.copy_suffix(), suffix)
    pending.advance(3)
    suffix = [6]
    assert_equal(pending.copy_suffix(), suffix)
    pending.advance(1)
    assert_equal(len(pending), 0)


def test_blocked_tls_read_preserves_one_output_store() raises:
    assert_true(_can_move_http2_output(False, False))
    assert_true(_can_move_http2_output(True, False))
    assert_true(not _can_move_http2_output(True, True))


def main() raises:
    test_config_and_registration()
    test_duplicate_poll_events_coalesce()
    test_partial_write_keeps_exact_suffix()
    test_blocked_tls_read_preserves_one_output_store()
    print("test_polling_server: all tests passed")
