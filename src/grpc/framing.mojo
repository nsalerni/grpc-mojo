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
"""

from net import TCPStream
from h2 import Http2Connection, get_u32_be

comptime GRPC_MESSAGE_PREFIX_LEN = 5
"""Bytes in the message prefix: 1 compressed flag + 4 length."""

comptime DEFAULT_MAX_RECV_MESSAGE_SIZE = 4 * 1024 * 1024
"""4 MiB, matching the common gRPC default."""


def frame_message(
    payload: Span[Byte, _], *, compressed: Bool = False
) -> List[Byte]:
    """Wraps a serialized message in the gRPC length prefix.

    Args:
        payload: The serialized message bytes.
        compressed: Value for the Compressed-Flag byte. This
            implementation never compresses, so leave it False unless
            constructing test vectors.

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
    mut conn: Http2Connection[TCPStream],
    stream_id: UInt32,
    payload: Span[Byte, _],
    *,
    end_stream: Bool = False,
) raises:
    """Frames one message and sends it as DATA on a stream.

    Args:
        conn: The HTTP/2 connection to send on.
        stream_id: The stream carrying the call.
        payload: The serialized message bytes (uncompressed).
        end_stream: Whether to set END_STREAM, half-closing the sender.

    Raises:
        On connection I/O or HTTP/2 protocol errors.
    """
    var framed = frame_message(payload)
    conn.send_data(stream_id, Span(framed), end_stream=end_stream)


def recv_message(
    mut conn: Http2Connection[TCPStream],
    stream_id: UInt32,
    *,
    max_size: Int = DEFAULT_MAX_RECV_MESSAGE_SIZE,
) raises -> Optional[List[Byte]]:
    """Reads one length-prefixed message from the stream.

    Blocks on the connection until a full message (or end of stream)
    arrives. The body is drained incrementally as frames arrive because
    stream flow-control credit is granted on consumption, so messages
    larger than the flow-control window still make progress.

    Args:
        conn: The HTTP/2 connection to read from.
        stream_id: The stream carrying the call.
        max_size: Reject messages whose declared length exceeds this
            (default `DEFAULT_MAX_RECV_MESSAGE_SIZE`).

    Returns:
        The message bytes, or None on a clean end of the message stream
        (stream ended with no partial message buffered).

    Raises:
        On a truncated prefix or body, an invalid or set compressed flag
        (no codecs are implemented yet — docs/PRIMITIVES.md item 4), an
        oversized message, or connection errors.
    """
    if not conn.wait_data(stream_id, GRPC_MESSAGE_PREFIX_LEN):
        if conn.buffered_data_len(stream_id) == 0:
            return None
        raise Error("grpc: truncated message prefix")
    var prefix = conn.take_data(stream_id, GRPC_MESSAGE_PREFIX_LEN)
    var compressed = prefix[0]
    if compressed > 1:
        raise Error("grpc: invalid compressed flag")
    var length = Int(get_u32_be(Span(prefix), 1))
    if length > max_size:
        raise Error("grpc: message exceeds max size")
    if compressed == 1:
        # No codecs yet (docs/PRIMITIVES.md item 4); a compressed message
        # without negotiated encoding is a protocol error anyway.
        raise Error("grpc: compressed messages not supported")
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
    return out^
