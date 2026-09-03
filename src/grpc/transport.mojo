# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""The byte-stream transport shared by TCP, TLS, and Unix gRPC connections."""

from std.ffi import c_int

from net import ReadinessStream, TCPStream, UnixStream
from tls import TLSStream


@fieldwise_init
struct GrpcTransport(ReadinessStream):
    """A readiness-capable TCP, TLS, or Unix stream for gRPC connections.

    This keeps `GrpcChannel`, `ServerCall`, and generated service types
    transport-stable while `Http2Connection` continues to operate through
    its generic stream seam. The wrapper owns exactly one concrete stream.

    After a partial operation reports would-block, callers must retry that
    same operation with the same unconsumed bytes. Plain TCP waits for the
    operation's natural direction. TLS callers inspect `wants_read()` and
    `wants_write()` because either operation can need either direction.
    """

    var _tcp: Optional[TCPStream]
    var _tls: Optional[TLSStream]
    var _unix: Optional[UnixStream]

    @staticmethod
    def plaintext(var stream: TCPStream) -> GrpcTransport:
        """Wraps a connected TCP stream for h2c.

        Args:
            stream: The connected stream; ownership is taken.

        Returns:
            A transport carrying the TCP stream.
        """
        var out = GrpcTransport(_tcp=None, _tls=None, _unix=None)
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
        var out = GrpcTransport(_tcp=None, _tls=None, _unix=None)
        out._tls = stream^
        return out^

    @staticmethod
    def local(var stream: UnixStream) -> GrpcTransport:
        """Wraps a connected Unix domain stream.

        Args:
            stream: The connected stream; ownership is taken.

        Returns:
            A transport carrying the Unix domain stream.
        """
        var out = GrpcTransport(_tcp=None, _tls=None, _unix=None)
        out._unix = stream^
        return out^

    def descriptor(self) -> c_int:
        """Returns the active stream's pollable descriptor.

        Returns:
            The descriptor owned by the wrapped TCP or TLS stream.
        """
        if self._tls:
            return self._tls.value().descriptor()
        if self._unix:
            return self._unix.value().descriptor()
        return self._tcp.value().descriptor()

    def _is_secure(self) -> Bool:
        return Bool(self._tls)

    def set_nonblocking(mut self, enabled: Bool) raises:
        """Switches the active stream's descriptor blocking mode.

        Args:
            enabled: True for non-blocking mode, False for blocking mode.

        Raises:
            If the descriptor flags cannot be updated.
        """
        if self._tls:
            self._tls.value().set_nonblocking(enabled)
            return
        if self._unix:
            self._unix.value().set_nonblocking(enabled)
            return
        self._tcp.value().set_nonblocking(enabled)

    def read(self, mut buf: List[Byte]) raises -> Int:
        """Performs one partial read through the active stream.

        Args:
            buf: Pre-sized buffer to fill and shrink to bytes read.

        Returns:
            Bytes read, or zero on orderly EOF.

        Raises:
            The typed would-block error when no progress is possible, or
            another transport error.
        """
        if self._tls:
            return self._tls.value().read(buf)
        if self._unix:
            return self._unix.value().read(buf)
        return self._tcp.value().read(buf)

    def write_some(self, data: Span[Byte, _]) raises -> Int:
        """Performs one partial write through the active stream.

        A would-block result accepts no bytes. Retry this operation with the
        same span after the required readiness direction is reported.

        Args:
            data: Bytes to offer without an internal retry loop.

        Returns:
            Bytes accepted, or zero when the span is empty.

        Raises:
            The typed would-block error when no progress is possible, or
            another transport error.
        """
        if self._tls:
            return self._tls.value().write_some(data)
        if self._unix:
            return self._unix.value().write_some(data)
        return self._tcp.value().write_some(data)

    def wants_read(self) raises -> Bool:
        """Reports TLS WANT_READ for the last blocked TLS operation.

        Plain TCP operations use their natural direction, so this returns
        False for h2c transports.

        Returns:
            True when the TLS operation must wait for readability.

        Raises:
            If the TLS readiness state cannot be queried.
        """
        if self._tls:
            return self._tls.value().wants_read()
        return False

    def wants_write(self) raises -> Bool:
        """Reports TLS WANT_WRITE for the last blocked TLS operation.

        Plain TCP operations use their natural direction, so this returns
        False for h2c transports.

        Returns:
            True when the TLS operation must wait for writability.

        Raises:
            If the TLS readiness state cannot be queried.
        """
        if self._tls:
            return self._tls.value().wants_write()
        return False

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
        if self._unix:
            return self._unix.value().read_exact(n)
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
        if self._unix:
            self._unix.value().write_all(data)
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
        if self._unix:
            self._unix.value().set_read_timeout(nanos)
            return
        self._tcp.value().set_read_timeout(nanos)

    def set_write_timeout(self, nanos: Int64) raises:
        """Sets the active stream's write timeout.

        Args:
            nanos: Timeout in nanoseconds; 0 clears it.

        Raises:
            If the transport cannot apply the timeout.
        """
        if self._tls:
            self._tls.value().set_write_timeout(nanos)
            return
        if self._unix:
            self._unix.value().set_write_timeout(nanos)
            return
        self._tcp.value().set_write_timeout(nanos)

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
        if self._unix:
            self._unix.value().set_nodelay(enabled)
            return
        self._tcp.value().set_nodelay(enabled)

    def close(mut self):
        """Closes the active stream; safe to call more than once."""
        if self._tls:
            self._tls.value().close()
            return
        if self._unix:
            self._unix.value().close()
            return
        if self._tcp:
            self._tcp.value().close()
