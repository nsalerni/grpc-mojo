# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Custom call metadata per the gRPC Custom-Metadata rules.

Implements the header-based metadata of
[PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
ASCII metadata uses names of `0-9 a-z _ - .` with values of space plus
printable ASCII, while binary metadata uses names ending in `-bin` with
values base64-coded on the wire. Decoding accepts both padded and unpadded
base64; encoding emits unpadded, as the spec asks implementations to do.

`Metadata` is the ordered container used for request metadata, response
headers, and trailers on both the client (`GrpcChannel`) and the server
(`ServerContext`). Keys beginning with `grpc-` are reserved for the protocol
and rejected by `Metadata.add`.
"""

from std.base64 import b64decode, b64encode

from hpack import HeaderField


def is_valid_metadata_key(key: StringSpan) -> Bool:
    """Reports whether a metadata key is well-formed.

    Valid keys are non-empty and contain only digits, lower-case ASCII
    letters, `_`, `-`, and `.`.

    Args:
        key: The metadata key to validate.

    Returns:
        True if every byte of `key` is in the allowed set.
    """
    if key.byte_length() == 0:
        return False
    for b in key.as_bytes():
        var c = Int(b)
        var ok = (
            (c >= ord("0") and c <= ord("9"))
            or (c >= ord("a") and c <= ord("z"))
            or c == ord("_")
            or c == ord("-")
            or c == ord(".")
        )
        if not ok:
            return False
    return True


def is_binary_key(key: StringSpan) -> Bool:
    """Reports whether a metadata key names a binary entry.

    Args:
        key: The metadata key to test.

    Returns:
        True if the key ends in `-bin`, marking a base64-coded value.
    """
    return key.endswith("-bin")


def encode_bin_value(data: Span[Byte, _]) -> String:
    """Base64-encodes a binary metadata value without padding.

    The spec asks implementations to emit unpadded base64 for `-bin`
    values; trailing `=` characters are stripped after encoding.

    Args:
        data: The raw bytes to encode.

    Returns:
        The unpadded base64 text for the wire.
    """
    var s = b64encode(data)
    while s.endswith("="):
        var trimmed = String(s[byte = 0 : s.byte_length() - 1])
        s = trimmed^
    return s^


def decode_bin_value(value: StringSpan) raises -> List[Byte]:
    """Decodes a base64 binary metadata value, padded or unpadded.

    Receivers must accept both forms, so padding is restored before
    decoding. Comma-joined lists are split by callers before reaching
    here (see `Metadata.get_binary`).

    Args:
        value: The base64 text from the wire.

    Returns:
        The decoded bytes.

    Raises:
        If the text is not valid base64.
    """
    var s = String(value)
    # std.base64.b64decode is lenient about invalid characters; validate
    # here so corrupt values raise as documented instead of silently
    # decoding to garbage bytes.
    var payload_end = s.byte_length()
    while payload_end > 0 and s[byte = payload_end - 1 : payload_end] == "=":
        payload_end -= 1
    var bytes = s.as_bytes()
    for i in range(payload_end):
        var c = Int(bytes[i])
        var ok = (
            (c >= ord("A") and c <= ord("Z"))
            or (c >= ord("a") and c <= ord("z"))
            or (c >= ord("0") and c <= ord("9"))
            or c == ord("+")
            or c == ord("/")
        )
        if not ok:
            raise Error("grpc: invalid base64 in binary metadata")
    while s.byte_length() % 4 != 0:
        s += "="
    return b64decode(s)


struct Metadata(Copyable, Defaultable, Movable, Sized, Writable):
    """Ordered key/value metadata attached to a call.

    Entries preserve insertion order and duplicate keys are allowed. The
    container stores wire form only: ASCII values as-is, binary (`-bin`)
    values base64-coded, decoded on access via `get_binary`.
    """

    var entries: List[HeaderField]
    """The metadata entries in insertion (wire) order, values in wire form."""

    def __init__(out self):
        """Constructs an empty metadata container."""
        self.entries = List[HeaderField]()

    def add(mut self, var key: String, var value: String) raises:
        """Appends an ASCII metadata entry.

        Args:
            key: The metadata key; must be valid per
                `is_valid_metadata_key` and must not start with `grpc-`
                (reserved for the protocol).
            value: The ASCII value, stored verbatim.

        Raises:
            If the key is invalid or reserved.
        """
        if not is_valid_metadata_key(key):
            raise Error("grpc: invalid metadata key: " + key)
        if key.startswith("grpc-"):
            raise Error("grpc: metadata keys may not start with grpc-")
        self.entries.append(HeaderField(name=key^, value=value^))

    def add_binary(mut self, var key: String, value: Span[Byte, _]) raises:
        """Appends a binary metadata entry, base64-coding the value.

        Args:
            key: The metadata key; must end in `-bin` and be valid per
                `is_valid_metadata_key`.
            value: The raw bytes; stored as unpadded base64.

        Raises:
            If the key does not end in `-bin` or is otherwise invalid.
        """
        if not is_binary_key(key):
            raise Error("grpc: binary metadata keys must end in -bin")
        if not is_valid_metadata_key(key):
            raise Error("grpc: invalid metadata key: " + key)
        self.entries.append(
            HeaderField(name=key^, value=encode_bin_value(value))
        )

    def get(self, key: StringSpan) -> Optional[String]:
        """Returns the first value stored under a key.

        Binary entries are returned in their base64 wire form; use
        `get_binary` to decode them.

        Args:
            key: The metadata key to look up.

        Returns:
            The first matching value, or None if the key is absent.
        """
        for e in self.entries:
            if e.name == String(key):
                return e.value.copy()
        return None

    def get_all(self, key: StringSpan) -> List[String]:
        """Returns every value stored under a key, in insertion order.

        Args:
            key: The metadata key to look up.

        Returns:
            All matching values; empty if the key is absent.
        """
        var out = List[String]()
        for e in self.entries:
            if e.name == String(key):
                out.append(e.value.copy())
        return out^

    def get_binary(self, key: StringSpan) raises -> Optional[List[Byte]]:
        """Returns the first binary value under a key, base64-decoded.

        Accepts padded and unpadded base64. Per the spec, a comma-joined
        header value is split on `,` and only the first element is decoded.

        Args:
            key: The `-bin` metadata key to look up.

        Returns:
            The decoded bytes, or None if the key is absent.

        Raises:
            If the stored value is not valid base64.
        """
        for e in self.entries:
            if e.name == String(key):
                # Spec: split on "," before base64-decoding.
                var first = e.value.split(",")[0]
                return decode_bin_value(first)
        return None

    def __len__(self) -> Int:
        """Returns the number of metadata entries.

        Returns:
            The entry count, counting duplicate keys separately.
        """
        return len(self.entries)

    def write_to(self, mut writer: Some[Writer]):
        """Writes a human-readable rendering of all entries.

        Args:
            writer: The writer to render into.
        """
        writer.write("Metadata(")
        for i in range(len(self.entries)):
            if i > 0:
                writer.write(", ")
            writer.write(self.entries[i])
        writer.write(")")

    @staticmethod
    def from_headers(
        headers: Span[HeaderField, _], *, skip_reserved: Bool = True
    ) -> Metadata:
        """Collects custom metadata from a decoded HTTP/2 header block.

        Pseudo-headers (names starting with `:`) are always dropped. With
        `skip_reserved` (the default), transport and protocol headers —
        anything starting with `grpc-`, plus `content-type`, `te`, and
        `user-agent` — are dropped too, leaving only application metadata.

        Args:
            headers: The decoded header fields, in wire order.
            skip_reserved: Whether to drop reserved transport/gRPC headers
                in addition to pseudo-headers.

        Returns:
            The surviving entries as metadata, order preserved.
        """
        var md = Metadata()
        for h in headers:
            if h.name.startswith(":"):
                continue
            if skip_reserved:
                if (
                    h.name.startswith("grpc-")
                    or h.name == "content-type"
                    or h.name == "te"
                    or h.name == "user-agent"
                ):
                    continue
            md.entries.append(h.copy())
        return md^
