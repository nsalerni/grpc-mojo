# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Length-Prefixed-Message framing for gRPC over HTTP/2.

Implements the message framing of
[PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
each message travels as a 1-byte Compressed-Flag, a 4-byte big-endian
Message-Length, then the message bytes. DATA frame boundaries carry no
meaning; messages are parsed from the stream's buffered bytes, and one
message may span many frames (or share a frame with its neighbors).

When the Compressed-Flag is 1, the message body is gzip (RFC 1952),
negotiated with `grpc-encoding` / `grpc-accept-encoding`.
"""

from h2 import Http2Connection, get_u32_be

from .gzip import gzip_compress, gzip_decompress
from .transport import GrpcTransport

comptime GRPC_MESSAGE_PREFIX_LEN = 5
"""Bytes in the message prefix: 1 compressed flag + 4 length."""

comptime DEFAULT_MAX_RECV_MESSAGE_SIZE = 4 * 1024 * 1024
"""4 MiB, matching the common gRPC default."""


def frame_message(
    payload: Span[Byte, _], *, compressed: Bool = False
) -> List[Byte]:
    """Wraps a serialized message in the gRPC length prefix.

    Args:
        payload: The serialized message bytes (already gzip'd when
            `compressed` is True).
        compressed: Value for the Compressed-Flag byte.

    Returns:
        The prefix followed by the payload, ready to send as DATA.
    """
    var out = List[Byte](capacity=GRPC_MESSAGE_PREFIX_LEN + len(payload))
    out.append(UInt8(1) if compressed else UInt8(0))
    var n = UInt32(len(payload))
    out.append(UInt8((n >> 24) & 0xFF))
    out.append(UInt8((n >> 16) & 0xFF))
    out.append(UInt8((n >> 8) & 0xFF))
    out.append(UInt8(n & 0xFF))
    out.extend(payload)
    return out^


def send_message(
    mut conn: Http2Connection[GrpcTransport],
    stream_id: UInt32,
    payload: Span[Byte, _],
    *,
    end_stream: Bool = False,
    compress: Bool = False,
) raises:
    """Frames one message and sends it as DATA on a stream.

    Args:
        conn: The HTTP/2 connection to send on.
        stream_id: The stream carrying the call.
        payload: The serialized message bytes (uncompressed).
        end_stream: Whether to set END_STREAM, half-closing the sender.
        compress: When True, gzip-compress `payload` and set the
            Compressed-Flag. The caller must advertise `grpc-encoding:
            gzip` on this call.

    Raises:
        On connection I/O, HTTP/2 protocol errors, or gzip failure.
    """
    var framed: List[Byte]
    if compress:
        var body = gzip_compress(payload)
        framed = frame_message(Span(body), compressed=True)
    else:
        framed = frame_message(payload)
    conn.send_data(stream_id, Span(framed), end_stream=end_stream)


def recv_message(
    mut conn: Http2Connection[GrpcTransport],
    stream_id: UInt32,
    *,
    max_size: Int = DEFAULT_MAX_RECV_MESSAGE_SIZE,
) raises -> Optional[List[Byte]]:
    """Reads one length-prefixed message from the stream.

    A Compressed-Flag of 1 is gunzip'd. See `recv_message_flag` to
    observe whether the wire flag was set.

    Args:
        conn: The HTTP/2 connection to read from.
        stream_id: The stream carrying the call.
        max_size: Reject messages whose declared (or uncompressed)
            length exceeds this (default `DEFAULT_MAX_RECV_MESSAGE_SIZE`).

    Returns:
        The uncompressed message bytes, or None on a clean end of the
        message stream.

    Raises:
        On a truncated prefix or body, an invalid compressed flag, a
        gzip failure, an oversized message, or connection errors.
    """
    var compressed = False
    return recv_message_flag(
        conn, stream_id, max_size=max_size, compressed=compressed
    )


def recv_message_flag(
    mut conn: Http2Connection[GrpcTransport],
    stream_id: UInt32,
    *,
    max_size: Int,
    mut compressed: Bool,
) raises -> Optional[List[Byte]]:
    """Reads one length-prefixed message and reports the Compressed-Flag.

    Args:
        conn: The HTTP/2 connection to read from.
        stream_id: The stream carrying the call.
        max_size: Reject messages whose declared (or uncompressed)
            length exceeds this.
        compressed: Set to True when the prefix Compressed-Flag was 1.

    Returns:
        The uncompressed message bytes, or None on a clean end of the
        message stream.

    Raises:
        On a truncated prefix or body, an invalid compressed flag, a
        gzip failure, an oversized message, or connection errors.
    """
    compressed = False
    if not conn.wait_data(stream_id, GRPC_MESSAGE_PREFIX_LEN):
        if conn.buffered_data_len(stream_id) == 0:
            return None
        raise Error("grpc: truncated message prefix")
    var prefix = conn.take_data(stream_id, GRPC_MESSAGE_PREFIX_LEN)
    var flag = prefix[0]
    if flag > 1:
        raise Error("grpc: invalid compressed flag")
    var length = Int(get_u32_be(Span(prefix), 1))
    if length > max_size:
        raise Error("grpc: message exceeds max size")
    # Drain incrementally: stream flow-control credit is granted on
    # consumption, so messages larger than the window must be consumed
    # as they arrive.
    var out = List[Byte](capacity=length)
    while len(out) < length:
        var avail = conn.buffered_data_len(stream_id)
        if avail > 0:
            var n = min(avail, length - len(out))
            out.extend(Span(conn.take_data(stream_id, n)))
            continue
        if not conn.wait_data(stream_id, 1):
            raise Error("grpc: truncated message body")
    if flag == 1:
        compressed = True
        return gzip_decompress(Span(out), max_size=max_size)
    return out^
