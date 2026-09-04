# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""`grpc.health.v1` Check: a status registry and handwritten messages.

`Watch` is not implemented. Status changes during a live stream would need
another thread or a parked async stream; the protocol allows UNIMPLEMENTED
on Watch, and clients fall back to Check.
"""

from proto import (
    WIRE_LEN,
    WIRE_VARINT,
    ProtoMessage,
    WireReader,
    WireWriter,
    decode,
    encode,
)

from .status import Status, StatusCode


comptime HEALTH_CHECK_PATH = "/grpc.health.v1.Health/Check"
"""Full method path for unary Check."""

comptime HEALTH_WATCH_PATH = "/grpc.health.v1.Health/Watch"
"""Full method path for Watch. Not registered; unknown methods are UNIMPLEMENTED."""


struct ServingStatus:
    """`HealthCheckResponse.ServingStatus` wire numbers."""

    comptime UNKNOWN = 0
    """The service's serving status is unknown."""
    comptime SERVING = 1
    """The service is serving."""
    comptime NOT_SERVING = 2
    """The service is not serving."""
    comptime SERVICE_UNKNOWN = 3
    """Used only by Watch for a service name that is not registered."""


struct HealthCheckRequest(Copyable, Defaultable, Movable, ProtoMessage):
    """Request for `grpc.health.v1.Health/Check`.

    An empty `service` name selects the server's overall status.
    """

    var service: String
    """Field `service` (number 1). Empty means overall status."""
    var _unknown: List[Byte]
    """Preserved unknown fields, re-emitted on encode."""

    def __init__(out self):
        """Initializes all fields to their proto3 defaults."""
        self.service = String()
        self._unknown = List[Byte]()

    def __init__(out self, var service: String):
        """Sets the service name.

        Args:
            service: Service name; empty selects overall status.
        """
        self.service = service^
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        """Appends the wire-format bytes to the writer.

        Fields set to their proto3 default are omitted; preserved
        unknown fields are re-emitted at the end.

        Args:
            writer: Destination wire-format writer.
        """
        if self.service.byte_length() != 0:
            writer.string_field(1, self.service)
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        """Merges fields decoded from the reader into this message.

        Later singular values overwrite earlier ones, and unknown
        fields are preserved.

        Args:
            reader: Source wire-format reader.

        Raises:
            If the input is not valid protobuf wire data.
        """
        while not reader.done():
            var tag = reader.read_tag()
            if tag[0] == 1:
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    self.service = reader.string_value()
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


struct HealthCheckResponse(Copyable, Defaultable, Movable, ProtoMessage):
    """Response for `grpc.health.v1.Health/Check`."""

    var status: Int
    """Field `status` (number 1), a `ServingStatus` wire number."""
    var _unknown: List[Byte]
    """Preserved unknown fields, re-emitted on encode."""

    def __init__(out self):
        """Initializes all fields to their proto3 defaults."""
        self.status = ServingStatus.UNKNOWN
        self._unknown = List[Byte]()

    def __init__(out self, status: Int):
        """Sets the serving status.

        Args:
            status: A `ServingStatus` wire number.
        """
        self.status = status
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        """Appends the wire-format bytes to the writer.

        Fields set to their proto3 default are omitted; preserved
        unknown fields are re-emitted at the end.

        Args:
            writer: Destination wire-format writer.
        """
        if self.status != 0:
            writer.int32(1, Int32(self.status))
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        """Merges fields decoded from the reader into this message.

        Later singular values overwrite earlier ones, and unknown
        fields are preserved.

        Args:
            reader: Source wire-format reader.

        Raises:
            If the input is not valid protobuf wire data.
        """
        while not reader.done():
            var tag = reader.read_tag()
            if tag[0] == 1:
                if tag[1] != WIRE_VARINT:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    self.status = Int(reader.int32_value())
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


struct HealthCheckOutcome(Copyable, Movable):
    """gRPC status plus serialized Check response bytes."""

    var grpc_status: Status
    """OK when `payload` is a Check response; NOT_FOUND for unknown names."""
    var payload: List[Byte]
    """Encoded `HealthCheckResponse`, empty on a non-OK status."""

    def __init__(out self, var grpc_status: Status, var payload: List[Byte]):
        """Stores a Check status and the encoded response.

        Args:
            grpc_status: OK when `payload` is a Check response.
            payload: Encoded `HealthCheckResponse`, or empty on error.
        """
        self.grpc_status = grpc_status^
        self.payload = payload^


struct Health(Movable):
    """Serving-status registry for `grpc.health.v1` Check.

    The empty service name is the overall server status and defaults to
    SERVING. A named service that was never `set_status`'d is unknown:
    Check returns NOT_FOUND, matching the official health protocol.
    `SERVICE_UNKNOWN` is a Watch-only response and is not used by Check.
    """

    var _status: Dict[String, Int]
    """Map from service name to `ServingStatus`. Empty key is overall."""

    def __init__(out self) raises:
        """Creates a registry whose overall status is SERVING.

        Raises:
            If the overall-status entry cannot be inserted.
        """
        self._status = Dict[String, Int]()
        self._status[String("")] = ServingStatus.SERVING

    def set_status(mut self, service: StringSpan, status: Int) raises:
        """Sets the serving status for a service name.

        Args:
            service: Service name; empty selects overall status.
            status: A `ServingStatus` wire number.
        """
        self._status[String(service)] = status

    def status(self, service: StringSpan) raises -> Optional[Int]:
        """Returns the registered status, or None if the name is unknown.

        Args:
            service: Service name; empty selects overall status.

        Returns:
            The `ServingStatus` wire number, or None when unset.
        """
        var key = String(service)
        if key in self._status:
            return self._status[key]
        return None

    def check(self, request: HealthCheckRequest) raises -> HealthCheckOutcome:
        """Evaluates Check for one request.

        Unknown service names return NOT_FOUND, not SERVICE_UNKNOWN.

        Args:
            request: The Check request, including an empty overall name.

        Returns:
            OK plus a serialized response, or NOT_FOUND with empty payload.
        """
        var found = self.status(request.service)
        if not found:
            return HealthCheckOutcome(
                Status(
                    code=StatusCode.NOT_FOUND,
                    message=String("unknown service"),
                ),
                List[Byte](),
            )
        var response = HealthCheckResponse(found.value())
        return HealthCheckOutcome(Status.ok(), encode(response))

    def check_bytes(self, request: Span[Byte, _]) raises -> HealthCheckOutcome:
        """Decodes a Check request and evaluates it.

        Args:
            request: Serialized `HealthCheckRequest` bytes.

        Returns:
            The Check outcome.

        Raises:
            If the request is not valid protobuf wire data.
        """
        return self.check(decode[HealthCheckRequest](request))
