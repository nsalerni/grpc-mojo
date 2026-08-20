# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""The byte-stream transport shared by h2c and TLS gRPC connections."""

from net import IOStream, TCPStream
from tls import TLSStream


@fieldwise_init
struct GrpcTransport(IOStream):
    """An `IOStream` containing either a TCP stream or a TLS stream.

    This keeps `GrpcChannel`, `ServerCall`, and generated service types
    transport-stable while `Http2Connection` continues to operate through
    its generic `IOStream` seam.
    """

    var _tcp: Optional[TCPStream]
    var _tls: Optional[TLSStream]

    @staticmethod
    def plaintext(var stream: TCPStream) -> GrpcTransport:
        """Wraps a connected TCP stream for h2c.

        Args:
            stream: The connected stream; ownership is taken.

        Returns:
            A transport carrying the TCP stream.
        """
        var out = GrpcTransport(_tcp=None, _tls=None)
        out._tcp = stream^
        return out^

    @staticmethod
    def secure(var stream: TLSStream) -> GrpcTransport:
        """Wraps a negotiated TLS stream.

        Args:
            stream: The TLS stream; ownership is taken.

        Returns:
            A transport carrying the TLS stream.
        """
        var out = GrpcTransport(_tcp=None, _tls=None)
        out._tls = stream^
        return out^

    def read_exact(self, n: Int) raises -> List[Byte]:
        """Reads exactly `n` bytes from the active stream.

        Args:
            n: Number of bytes to read.

        Returns:
            Exactly `n` bytes.

        Raises:
            On EOF, timeout, or transport errors.
        """
        if self._tls:
            return self._tls.value().read_exact(n)
        return self._tcp.value().read_exact(n)

    def write_all(self, data: Span[Byte, _]) raises:
        """Writes every byte to the active stream.

        Args:
            data: Bytes to write.

        Raises:
            On timeout or transport errors.
        """
        if self._tls:
            self._tls.value().write_all(data)
            return
        self._tcp.value().write_all(data)

    def set_read_timeout(self, nanos: Int64) raises:
        """Sets the active stream's read timeout.

        Args:
            nanos: Timeout in nanoseconds; 0 clears it.

        Raises:
            If the transport cannot apply the timeout.
        """
        if self._tls:
            self._tls.value().set_read_timeout(nanos)
            return
        self._tcp.value().set_read_timeout(nanos)

    def set_nodelay(self, enabled: Bool) raises:
        """Applies the no-delay latency hint to the active stream.

        Args:
            enabled: True to send small writes immediately.

        Raises:
            If the transport cannot apply the setting.
        """
        if self._tls:
            self._tls.value().set_nodelay(enabled)
            return
        self._tcp.value().set_nodelay(enabled)

    def close(mut self):
        """Closes the active stream; safe to call more than once."""
        if self._tls:
            self._tls.value().close()
            return
        if self._tcp:
            self._tcp.value().close()
