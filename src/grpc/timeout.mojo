# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Coding of the `grpc-timeout` request header.

The gRPC HTTP/2 protocol specification
([PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md))
defines a timeout as a TimeoutValue of at most 8 ASCII digits followed by a
single-letter unit: `H` (hours), `M` (minutes), `S` (seconds), `m`
(milliseconds), `u` (microseconds), or `n` (nanoseconds). This module converts
between that wire form and a plain nanosecond count.

Clients attach the header via `GrpcChannel.start_call(timeout_ns=...)`; the
server decodes it into `ServerContext.timeout_ns` and aborts the call with
`DEADLINE_EXCEEDED` when the decoded timeout elapses.
"""


def encode_timeout(nanos: Int64) raises -> String:
    """Encodes a duration in nanoseconds as a `grpc-timeout` header value.

    Chooses the finest unit whose value still fits in the spec's 8-digit
    limit, so precision degrades gracefully for long timeouts (for example
    `100_000_000_000` nanoseconds encodes as `"100S"`).

    Args:
        nanos: Duration in nanoseconds. Must be non-negative.

    Returns:
        The wire value, e.g. `"500m"` for 500 milliseconds.

    Raises:
        If `nanos` is negative, or too large to express even in hours.
    """
    if nanos < 0:
        raise Error("grpc: negative timeout")
    comptime LIMIT = 99999999  # 8 digits
    var v = nanos
    if v <= LIMIT:
        return String(v) + "n"
    v = nanos // 1000
    if v <= LIMIT:
        return String(v) + "u"
    v = nanos // 1_000_000
    if v <= LIMIT:
        return String(v) + "m"
    v = nanos // 1_000_000_000
    if v <= LIMIT:
        return String(v) + "S"
    v = nanos // 60_000_000_000
    if v <= LIMIT:
        return String(v) + "M"
    v = nanos // 3_600_000_000_000
    if v <= LIMIT:
        return String(v) + "H"
    raise Error("grpc: timeout too large")


def decode_timeout(value: StringSpan) raises -> Int64:
    """Decodes a `grpc-timeout` header value to nanoseconds.

    Args:
        value: The header value: 1 to 8 ASCII digits followed by one of the
            unit letters `H`, `M`, `S`, `m`, `u`, or `n`.

    Returns:
        The duration in nanoseconds.

    Raises:
        If the value is empty, longer than 9 bytes, not digits-then-unit, or
        uses an unknown unit letter.
    """
    var n = value.byte_length()
    if n < 2 or n > 9:
        raise Error("grpc: malformed grpc-timeout")
    var digits = value[byte = 0 : n - 1]
    var unit = value[byte = n - 1 : n]
    var amount = Int64(Int(digits))
    if amount < 0:
        raise Error("grpc: malformed grpc-timeout")
    if unit == "n":
        return amount
    elif unit == "u":
        return amount * 1000
    elif unit == "m":
        return amount * 1_000_000
    elif unit == "S":
        return amount * 1_000_000_000
    elif unit == "M":
        return amount * 60_000_000_000
    elif unit == "H":
        return amount * 3_600_000_000_000
    raise Error("grpc: unknown grpc-timeout unit")
