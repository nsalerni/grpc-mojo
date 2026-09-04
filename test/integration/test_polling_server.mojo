# Focused API and event-loop invariant checks for PollingServer.

from std.ffi import c_int
from std.testing import assert_equal, assert_true

from grpc import PollingServer, PollingServerConfig, ServerContext
from grpc.polling_server import (
    _PendingWrite,
    _can_move_http2_output,
    _can_schedule_keepalive,
    _coalesce_poll_events,
    _keepalive_remaining_ns,
    _merge_poll_remaining,
)
from h2 import H2_ALPN
from net import PollEvent
from tls import TLSContext


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
        var huge_window = PollingServerConfig(initial_window_size=0x80000000)
        huge_window.validate()
    except:
        raised = True
    assert_true(raised, "initial window must fit the RFC 31-bit maximum")

    var wide = PollingServerConfig(initial_window_size=1048576)
    wide.validate()
    assert_equal(Int(wide.initial_window_size), 1048576)

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

    raised = False
    try:
        var negative_keepalive = PollingServerConfig(keepalive_interval_ns=-1)
        negative_keepalive.validate()
    except:
        raised = True
    assert_true(raised, "keepalive interval must be non-negative")

    var keepalive_off = PollingServerConfig(keepalive_interval_ns=0)
    keepalive_off.validate()
    var keepalive_on = PollingServerConfig(keepalive_interval_ns=30_000_000_000)
    keepalive_on.validate()

    raised = False
    try:
        var keepalive_overflow = PollingServerConfig(
            keepalive_interval_ns=Int64(0x80000000) * 1_000_000
        )
        keepalive_overflow.validate()
    except:
        raised = True
    assert_true(
        raised, "keepalive interval must fit the Poller millisecond ABI"
    )

    var tls_server = PollingServer.tls(
        "127.0.0.1",
        0,
        "build/certs/server.pem",
        "build/certs/server.key",
        config,
        client_ca_file="build/certs/ca.pem",
        require_client_cert=True,
    )
    tls_server.register_unary_bytes[echo_bytes]("/probe.Probe/Echo")

    var unix_server = PollingServer.unix(
        "/tmp/grpc-mojo-polling-unused.sock",
        config=config,
    )
    assert_true(unix_server._unix_path)
    assert_equal(
        unix_server._unix_path.value(),
        "/tmp/grpc-mojo-polling-unused.sock",
    )
    assert_true(not unix_server._unix_remove_existing)
    assert_true(not unix_server._tls_context)
    unix_server.register_unary_bytes[echo_bytes]("/probe.Probe/Echo")

    var unix_tls = PollingServer.unix(
        "/tmp/grpc-mojo-polling-unused.sock",
        config=config,
    )
    unix_tls._tls_context = TLSContext.server(
        "build/certs/server.pem",
        "build/certs/server.key",
        alpn=[String(H2_ALPN)],
    )
    raised = False
    try:
        unix_tls.serve()
    except e:
        raised = "does not support TLS over Unix" in String(e)
    assert_true(raised, "PollingServer must refuse TLS over Unix")


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


def test_keepalive_poll_remaining() raises:
    var interval: Int64 = 30_000_000_000
    var last: Int64 = 1_000_000_000
    var elapsed: Int64 = 1_000_000_000
    assert_equal(
        _keepalive_remaining_ns(interval, last, last + elapsed),
        interval - elapsed,
    )
    assert_equal(_keepalive_remaining_ns(interval, last, last + interval), 0)
    assert_equal(
        _keepalive_remaining_ns(interval, last, last + interval + 5_000_000),
        0,
    )

    var none_yet: Int64 = -1
    var due: Int64 = 0
    var idle: Int64 = 60_000_000_000
    var keepalive: Int64 = 5_000_000_000
    var skipped: Int64 = -1
    assert_equal(_merge_poll_remaining(none_yet, interval), interval)
    assert_equal(_merge_poll_remaining(idle, keepalive), keepalive)
    assert_equal(_merge_poll_remaining(due, interval), due)
    assert_equal(_merge_poll_remaining(keepalive, skipped), keepalive)
    assert_equal(_merge_poll_remaining(due, skipped), due)

    assert_true(_can_schedule_keepalive(0, 0, True))
    assert_true(not _can_schedule_keepalive(1, 0, True))
    assert_true(not _can_schedule_keepalive(0, 1, True))
    assert_true(not _can_schedule_keepalive(0, 0, False))


def main() raises:
    test_config_and_registration()
    test_duplicate_poll_events_coalesce()
    test_partial_write_keeps_exact_suffix()
    test_blocked_tls_read_preserves_one_output_store()
    test_keepalive_poll_remaining()
    print("test_polling_server: all tests passed")
