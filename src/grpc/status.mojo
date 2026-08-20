# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Status codes, the `Status` result type, and status-related coding.

Covers the canonical status codes
(https://grpc.io/docs/guides/status-codes/), the spec-mandated mappings from
HTTP status codes and HTTP/2 RST_STREAM error codes to gRPC codes, and the
percent-encoding used by the `grpc-message` trailer, all per
[PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).

`Status` also carries the raw `grpc-status-details-bin` payload (a serialized
`google.rpc.Status`) when a peer uses the rich error model.
"""


struct StatusCode:
    """The canonical gRPC status codes, as integer constants.

    Values match https://grpc.io/docs/guides/status-codes/ and every other
    gRPC implementation; they travel on the wire in the `grpc-status`
    trailer.
    """

    comptime OK = 0
    """The call completed successfully."""
    comptime CANCELLED = 1
    """The call was cancelled, typically by the caller."""
    comptime UNKNOWN = 2
    """Unknown error, e.g. an unhandled exception in a handler."""
    comptime INVALID_ARGUMENT = 3
    """The client specified an invalid argument."""
    comptime DEADLINE_EXCEEDED = 4
    """The deadline expired before the call completed."""
    comptime NOT_FOUND = 5
    """A requested entity was not found."""
    comptime ALREADY_EXISTS = 6
    """The entity a client tried to create already exists."""
    comptime PERMISSION_DENIED = 7
    """The caller lacks permission for the operation."""
    comptime RESOURCE_EXHAUSTED = 8
    """A resource (quota, rate limit, filesystem space) is exhausted."""
    comptime FAILED_PRECONDITION = 9
    """The system is not in a state required for the operation."""
    comptime ABORTED = 10
    """The operation was aborted, e.g. a concurrency conflict."""
    comptime OUT_OF_RANGE = 11
    """The operation was attempted past the valid range."""
    comptime UNIMPLEMENTED = 12
    """The method is not implemented or not registered on the server."""
    comptime INTERNAL = 13
    """An invariant expected by the underlying system was broken."""
    comptime UNAVAILABLE = 14
    """The service is currently unavailable; retrying may help."""
    comptime DATA_LOSS = 15
    """Unrecoverable data loss or corruption."""
    comptime UNAUTHENTICATED = 16
    """The request lacks valid authentication credentials."""


def status_code_name(code: Int) -> StaticString:
    """Returns the canonical upper-case name for a gRPC status code.

    Args:
        code: A `StatusCode` value.

    Returns:
        The name, e.g. `"DEADLINE_EXCEEDED"` for 4, or `"UNKNOWN_CODE"` for
        integers outside the canonical 0-16 range.
    """
    if code == 0:
        return "OK"
    elif code == 1:
        return "CANCELLED"
    elif code == 2:
        return "UNKNOWN"
    elif code == 3:
        return "INVALID_ARGUMENT"
    elif code == 4:
        return "DEADLINE_EXCEEDED"
    elif code == 5:
        return "NOT_FOUND"
    elif code == 6:
        return "ALREADY_EXISTS"
    elif code == 7:
        return "PERMISSION_DENIED"
    elif code == 8:
        return "RESOURCE_EXHAUSTED"
    elif code == 9:
        return "FAILED_PRECONDITION"
    elif code == 10:
        return "ABORTED"
    elif code == 11:
        return "OUT_OF_RANGE"
    elif code == 12:
        return "UNIMPLEMENTED"
    elif code == 13:
        return "INTERNAL"
    elif code == 14:
        return "UNAVAILABLE"
    elif code == 15:
        return "DATA_LOSS"
    elif code == 16:
        return "UNAUTHENTICATED"
    return "UNKNOWN_CODE"


struct Status(Copyable, Movable, Writable):
    """The outcome of an RPC: a status code, message, and optional details.

    On the wire the code travels as the `grpc-status` trailer, the message
    as the percent-encoded `grpc-message` trailer, and the details as the
    base64-coded `grpc-status-details-bin` trailer (the rich error model,
    carrying a serialized `google.rpc.Status`).
    """

    var code: Int
    """A `StatusCode` value; `StatusCode.OK` (0) means success."""
    var message: String
    """Human-readable error description; empty for OK statuses."""
    var details_bin: List[Byte]
    """Raw google.rpc.Status bytes from grpc-status-details-bin, if any."""

    def __init__(out self, *, code: Int, var message: String):
        """Constructs a status with empty `details_bin`.

        Args:
            code: A `StatusCode` value.
            message: Human-readable error description; may be empty.
        """
        self.code = code
        self.message = message^
        self.details_bin = List[Byte]()

    @staticmethod
    def ok() -> Status:
        """Returns an OK status with an empty message.

        Returns:
            A status with code `StatusCode.OK`.
        """
        return Status(code=StatusCode.OK, message=String())

    def is_ok(self) -> Bool:
        """Reports whether the status code is `StatusCode.OK`.

        Returns:
            True if the call succeeded.
        """
        return self.code == StatusCode.OK

    def write_to(self, mut writer: Some[Writer]):
        """Writes a human-readable rendering, e.g. `NOT_FOUND (5): no user`.

        Args:
            writer: The writer to render into.
        """
        writer.write(status_code_name(self.code), " (", self.code, ")")
        if self.message.byte_length() > 0:
            writer.write(": ", self.message)

    def to_error(self) -> Error:
        """Converts the status into a raisable `Error`.

        Returns:
            An error whose message is `grpc: ` followed by the rendered
            status.
        """
        return Error("grpc: ", self)


def http_status_to_grpc(http_status: Int) -> Int:
    """Synthesizes a gRPC status code from a non-200 HTTP status.

    Implements the spec's mapping table for responses that carry an HTTP
    error instead of a `grpc-status` trailer (typically from intermediary
    proxies): 400 becomes INTERNAL, 401 UNAUTHENTICATED, 403
    PERMISSION_DENIED, 404 UNIMPLEMENTED, and 429/502/503/504 UNAVAILABLE.

    Args:
        http_status: The `:status` pseudo-header value.

    Returns:
        The mapped `StatusCode` value; `StatusCode.UNKNOWN` for any HTTP
        status not in the spec table.
    """
    if http_status == 400:
        return StatusCode.INTERNAL
    elif http_status == 401:
        return StatusCode.UNAUTHENTICATED
    elif http_status == 403:
        return StatusCode.PERMISSION_DENIED
    elif http_status == 404:
        return StatusCode.UNIMPLEMENTED
    elif (
        http_status == 429
        or http_status == 502
        or http_status == 503
        or http_status == 504
    ):
        return StatusCode.UNAVAILABLE
    return StatusCode.UNKNOWN


def rst_code_to_grpc(rst: UInt32) -> Int:
    """Maps an HTTP/2 RST_STREAM error code to a gRPC status code.

    Implements the spec's mapping table for streams the peer reset before
    delivering trailers: REFUSED_STREAM becomes UNAVAILABLE, CANCEL becomes
    CANCELLED, ENHANCE_YOUR_CALM becomes RESOURCE_EXHAUSTED, and
    INADEQUATE_SECURITY becomes PERMISSION_DENIED.

    Args:
        rst: The HTTP/2 error code from the RST_STREAM frame.

    Returns:
        The mapped `StatusCode` value; `StatusCode.INTERNAL` for any other
        HTTP/2 error code.
    """
    if rst == 0x7:  # REFUSED_STREAM
        return StatusCode.UNAVAILABLE
    elif rst == 0x8:  # CANCEL
        return StatusCode.CANCELLED
    elif rst == 0xB:  # ENHANCE_YOUR_CALM
        return StatusCode.RESOURCE_EXHAUSTED
    elif rst == 0xC:  # INADEQUATE_SECURITY
        return StatusCode.PERMISSION_DENIED
    return StatusCode.INTERNAL


# --- grpc-message percent-encoding (spec: space/VCHAR except %) ---


def percent_encode_message(msg: StringSpan) -> String:
    """Percent-encodes a status message for the `grpc-message` trailer.

    The spec allows space and printable ASCII except `%` to pass through;
    every other byte (including UTF-8 continuation bytes) is emitted as
    `%XX` with upper-case hex digits.

    Args:
        msg: The status message to encode.

    Returns:
        The encoded header value.
    """
    comptime digits = "0123456789ABCDEF"
    var out = String()
    for b in msg.as_bytes():
        var c = Int(b)
        if c >= 0x20 and c <= 0x7E and c != 0x25:  # printable, not '%'
            out += chr(c)
        else:
            out += "%"
            out += digits[byte = (c >> 4) : (c >> 4) + 1]
            out += digits[byte = (c & 0xF) : (c & 0xF) + 1]
    return out^


def percent_decode_message(msg: StringSpan) -> String:
    """Decodes a percent-encoded `grpc-message` trailer value.

    Lenient by design: invalid %-sequences are passed through unchanged,
    per the spec's requirement never to fail on decoding, and any invalid
    UTF-8 in the result is replaced rather than raising.

    Args:
        msg: The header value to decode.

    Returns:
        The decoded status message.
    """
    var bytes = msg.as_bytes()
    var out = List[Byte]()

    def hex_val(c: Byte) -> Int:
        var ci = Int(c)
        if ci >= ord("0") and ci <= ord("9"):
            return ci - ord("0")
        if ci >= ord("a") and ci <= ord("f"):
            return ci - ord("a") + 10
        if ci >= ord("A") and ci <= ord("F"):
            return ci - ord("A") + 10
        return -1

    var i = 0
    while i < len(bytes):
        if Int(bytes[i]) == ord("%") and i + 2 < len(bytes):
            var hi = hex_val(bytes[i + 1])
            var lo = hex_val(bytes[i + 2])
            if hi >= 0 and lo >= 0:
                out.append(UInt8(hi * 16 + lo))
                i += 3
                continue
        out.append(bytes[i])
        i += 1
    return String(from_utf8_lossy=Span(out))
