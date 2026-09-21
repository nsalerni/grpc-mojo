# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""gRPC server reflection (v1 and v1alpha).

Handwritten messages matching `grpc.reflection.v1.ServerReflection`.
File descriptors are static bytes from codegen, not a Python-style
dynamic pool. Extension lookup is UNIMPLEMENTED (proto3 has no
extensions). Health Watch stays separate and UNIMPLEMENTED.
"""

from proto import (
    WIRE_LEN,
    WIRE_VARINT,
    ProtoMessage,
    WireReader,
    WireWriter,
)

from .status import StatusCode

comptime REFLECTION_V1_PATH = (
    "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
)
"""Full method path for v1 ServerReflectionInfo."""

comptime REFLECTION_V1ALPHA_PATH = (
    "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
)
"""Full method path for v1alpha ServerReflectionInfo."""

comptime _REQ_FILE_BY_FILENAME = 3
comptime _REQ_FILE_CONTAINING_SYMBOL = 4
comptime _REQ_FILE_CONTAINING_EXTENSION = 5
comptime _REQ_ALL_EXTENSION_NUMBERS = 6
comptime _REQ_LIST_SERVICES = 7

comptime _RESP_FILE_DESCRIPTOR = 4
comptime _RESP_LIST_SERVICES = 6
comptime _RESP_ERROR = 7


def is_reflection_path(path: StringSpan) -> Bool:
    """Reports whether `path` is v1 or v1alpha ServerReflectionInfo.

    Args:
        path: Request `:path`.

    Returns:
        True for either reflection method path.
    """
    return path == REFLECTION_V1_PATH or path == REFLECTION_V1ALPHA_PATH


def service_name_from_path(path: StringSpan) -> Optional[String]:
    """Extracts `package.Service` from `/package.Service/Method`.

    Args:
        path: Full method path.

    Returns:
        The service name, or None if the path is not `/service/method`.
    """
    var owned = String(path)
    var parts = owned.split("/")
    if (
        len(parts) != 3
        or parts[0].byte_length() != 0
        or parts[1].byte_length() == 0
        or parts[2].byte_length() == 0
    ):
        return None
    return String(parts[1])


struct ServerReflectionRequest(Copyable, Defaultable, Movable, ProtoMessage):
    """One `ServerReflectionInfo` request message."""

    var host: String
    """Field `host` (number 1)."""
    var kind: Int
    """Active `message_request` field number, or 0 if unset."""
    var file_by_filename: String
    """Field `file_by_filename` (number 3)."""
    var file_containing_symbol: String
    """Field `file_containing_symbol` (number 4)."""
    var _unknown: List[Byte]
    """Preserved unknown fields, re-emitted on encode."""

    def __init__(out self):
        """Initializes all fields to their proto3 defaults."""
        self.host = String()
        self.kind = 0
        self.file_by_filename = String()
        self.file_containing_symbol = String()
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        """Appends the wire-format bytes to the writer.

        Args:
            writer: Destination wire-format writer.
        """
        if self.host.byte_length() != 0:
            writer.string_field(1, self.host)
        if self.kind == _REQ_FILE_BY_FILENAME:
            writer.string_field(3, self.file_by_filename)
        elif self.kind == _REQ_FILE_CONTAINING_SYMBOL:
            writer.string_field(4, self.file_containing_symbol)
        elif self.kind == _REQ_LIST_SERVICES:
            writer.string_field(7, "")
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        """Merges fields decoded from the reader into this message.

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
                    self.host = reader.string_value()
            elif tag[0] == _REQ_FILE_BY_FILENAME:
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    self.kind = _REQ_FILE_BY_FILENAME
                    self.file_by_filename = reader.string_value()
            elif tag[0] == _REQ_FILE_CONTAINING_SYMBOL:
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    self.kind = _REQ_FILE_CONTAINING_SYMBOL
                    self.file_containing_symbol = reader.string_value()
            elif tag[0] == _REQ_FILE_CONTAINING_EXTENSION:
                self.kind = _REQ_FILE_CONTAINING_EXTENSION
                reader.capture_field(tag[0], tag[1], self._unknown)
            elif tag[0] == _REQ_ALL_EXTENSION_NUMBERS:
                self.kind = _REQ_ALL_EXTENSION_NUMBERS
                reader.capture_field(tag[0], tag[1], self._unknown)
            elif tag[0] == _REQ_LIST_SERVICES:
                self.kind = _REQ_LIST_SERVICES
                if tag[1] == WIRE_LEN:
                    _ = reader.string_value()
                else:
                    reader.capture_field(tag[0], tag[1], self._unknown)
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


struct ServerReflectionResponse(Copyable, Defaultable, Movable, ProtoMessage):
    """One `ServerReflectionInfo` response message."""

    var valid_host: String
    """Field `valid_host` (number 1)."""
    var kind: Int
    """Active `message_response` field number."""
    var file_descriptor_proto: List[List[Byte]]
    """Serialized `FileDescriptorProto` bytes (field 4)."""
    var service_names: List[String]
    """Service names for `list_services_response` (field 6)."""
    var error_code: Int
    """`ErrorResponse.error_code` (field 7 / 1)."""
    var error_message: String
    """`ErrorResponse.error_message` (field 7 / 2)."""
    var _unknown: List[Byte]
    """Preserved unknown fields, re-emitted on encode."""

    def __init__(out self):
        """Initializes all fields to their proto3 defaults."""
        self.valid_host = String()
        self.kind = 0
        self.file_descriptor_proto = List[List[Byte]]()
        self.service_names = List[String]()
        self.error_code = 0
        self.error_message = String()
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        """Appends the wire-format bytes to the writer.

        Args:
            writer: Destination wire-format writer.
        """
        if self.valid_host.byte_length() != 0:
            writer.string_field(1, self.valid_host)
        if self.kind == _RESP_FILE_DESCRIPTOR:
            var inner = WireWriter()
            for proto in self.file_descriptor_proto:
                inner.bytes_field(1, Span(proto))
            writer.len_prefixed(4, Span(inner.buf))
        elif self.kind == _RESP_LIST_SERVICES:
            var inner = WireWriter()
            for name in self.service_names:
                var svc = WireWriter()
                svc.string_field(1, name)
                inner.len_prefixed(1, Span(svc.buf))
            writer.len_prefixed(6, Span(inner.buf))
        elif self.kind == _RESP_ERROR:
            var inner = WireWriter()
            if self.error_code != 0:
                inner.int32(1, Int32(self.error_code))
            if self.error_message.byte_length() != 0:
                inner.string_field(2, self.error_message)
            writer.len_prefixed(7, Span(inner.buf))
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        """Merges fields decoded from the reader into this message.

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
                    self.valid_host = reader.string_value()
            elif tag[0] == _RESP_FILE_DESCRIPTOR:
                self.kind = _RESP_FILE_DESCRIPTOR
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    var sub = reader.sub_reader()
                    while not sub.done():
                        var inner = sub.read_tag()
                        if inner[0] == 1 and inner[1] == WIRE_LEN:
                            self.file_descriptor_proto.append(sub.bytes_value())
                        else:
                            sub.capture_field(inner[0], inner[1], self._unknown)
            elif tag[0] == _RESP_LIST_SERVICES:
                self.kind = _RESP_LIST_SERVICES
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    var sub = reader.sub_reader()
                    while not sub.done():
                        var inner = sub.read_tag()
                        if inner[0] == 1 and inner[1] == WIRE_LEN:
                            var svc = sub.sub_reader()
                            while not svc.done():
                                var st = svc.read_tag()
                                if st[0] == 1 and st[1] == WIRE_LEN:
                                    self.service_names.append(svc.string_value())
                                else:
                                    svc.capture_field(
                                        st[0], st[1], self._unknown
                                    )
                        else:
                            sub.capture_field(inner[0], inner[1], self._unknown)
            elif tag[0] == _RESP_ERROR:
                self.kind = _RESP_ERROR
                if tag[1] != WIRE_LEN:
                    reader.capture_field(tag[0], tag[1], self._unknown)
                else:
                    var sub = reader.sub_reader()
                    while not sub.done():
                        var inner = sub.read_tag()
                        if inner[0] == 1 and inner[1] == WIRE_VARINT:
                            self.error_code = Int(sub.int32_value())
                        elif inner[0] == 2 and inner[1] == WIRE_LEN:
                            self.error_message = sub.string_value()
                        else:
                            sub.capture_field(inner[0], inner[1], self._unknown)
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


struct ReflectionRegistry(Movable):
    """Static file-descriptor set for server reflection.

    Populate with codegen `*_file_descriptor_proto()` bytes, then pass
    to `Server.add_reflection` / `PollingServer.add_reflection`.
    """

    var _files: Dict[String, List[Byte]]
    """Map from proto filename to serialized `FileDescriptorProto`."""
    var _symbols: Dict[String, String]
    """Map from fully-qualified symbol to filename."""
    var _services: List[String]
    """Explicitly registered service names, without a leading slash."""

    def __init__(out self):
        """Creates an empty descriptor set."""
        self._files = Dict[String, List[Byte]]()
        self._symbols = Dict[String, String]()
        self._services = List[String]()

    def add_file(
        mut self,
        filename: String,
        var proto: List[Byte],
        symbols: List[String],
    ) raises:
        """Registers one `FileDescriptorProto` and its symbols.

        Args:
            filename: Proto filename as clients will request it, e.g.
                `echo.proto`.
            proto: Serialized `FileDescriptorProto` bytes.
            symbols: Fully-qualified names (`package.Message`,
                `package.Service`, `package.Service.Method`) without a
                leading dot.

        Raises:
            If the maps cannot store the entries.
        """
        self._files[filename.copy()] = proto^
        for symbol in symbols:
            self._symbols[symbol.copy()] = filename.copy()

    def add_service(mut self, var name: String) raises:
        """Adds a service name returned by `list_services`.

        Args:
            name: `package.Service` without a leading slash.

        Raises:
            If the name list cannot grow.
        """
        self._services.append(name^)

    def services(self) -> List[String]:
        """Returns a copy of the explicitly registered service names.

        Returns:
            The names passed to `add_service`.
        """
        var out = List[String]()
        for name in self._services:
            out.append(name.copy())
        return out^

    def respond(
        self, request: ServerReflectionRequest, extra_services: List[String]
    ) raises -> ServerReflectionResponse:
        """Builds a reflection response for one request.

        Args:
            request: Decoded `ServerReflectionRequest`.
            extra_services: Service names from the live routing table.

        Returns:
            The matching `ServerReflectionResponse`.

        Raises:
            If map lookup fails.
        """
        var resp = ServerReflectionResponse()
        resp.valid_host = request.host.copy()
        if request.kind == _REQ_LIST_SERVICES:
            resp.kind = _RESP_LIST_SERVICES
            var seen = Dict[String, Bool]()
            for name in self._services:
                if name not in seen:
                    seen[name.copy()] = True
                    resp.service_names.append(name.copy())
            for name in extra_services:
                if name not in seen:
                    seen[name.copy()] = True
                    resp.service_names.append(name.copy())
            return resp^
        if request.kind == _REQ_FILE_BY_FILENAME:
            return self._file_response(request.file_by_filename, resp^)
        if request.kind == _REQ_FILE_CONTAINING_SYMBOL:
            var symbol = request.file_containing_symbol.copy()
            if symbol.startswith("."):
                var stripped = String(
                    symbol[byte = 1 : symbol.byte_length()]
                )
                symbol = stripped^
            if symbol not in self._symbols:
                resp.kind = _RESP_ERROR
                resp.error_code = StatusCode.NOT_FOUND
                resp.error_message = String("unknown symbol ") + symbol
                return resp^
            return self._file_response(self._symbols[symbol], resp^)
        resp.kind = _RESP_ERROR
        if (
            request.kind == _REQ_FILE_CONTAINING_EXTENSION
            or request.kind == _REQ_ALL_EXTENSION_NUMBERS
        ):
            resp.error_code = StatusCode.UNIMPLEMENTED
            resp.error_message = String("extensions are not supported")
        else:
            resp.error_code = StatusCode.INVALID_ARGUMENT
            resp.error_message = String("unsupported reflection request")
        return resp^

    def _file_response(
        self, filename: String, var resp: ServerReflectionResponse
    ) raises -> ServerReflectionResponse:
        if filename not in self._files:
            resp.kind = _RESP_ERROR
            resp.error_code = StatusCode.NOT_FOUND
            resp.error_message = String("unknown file ") + filename
            return resp^
        resp.kind = _RESP_FILE_DESCRIPTOR
        resp.file_descriptor_proto.append(self._files[filename].copy())
        return resp^
