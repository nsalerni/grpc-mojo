# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Gzip codec for gRPC payloads, via zlib in the stop-signal shim library.

gRPC uses RFC 1952 gzip (not zlib wrapping). A Compressed-Flag of 1 is
gzip only when `grpc-encoding` is `gzip`. Responses are gzipped only when
the request's `grpc-accept-encoding` lists `gzip`; a missing header means
identity only. This module does not wait on mojo-zlib; the C shim links
libz from conda-forge.
"""

from std.ffi import OwnedDLHandle, c_int
from std.os import getenv
from std.pathlib import Path
from std.sys import CompilationTarget

from hpack import HeaderField


def _shim_filename() -> String:
    """Returns `libgrpcstop.dylib` on macOS and `libgrpcstop.so` elsewhere.

    Returns:
        The platform-specific shared-library filename.
    """
    comptime if CompilationTarget.is_macos():
        return "libgrpcstop.dylib"
    else:
        return "libgrpcstop.so"


def _shim_path() raises -> String:
    """Resolves the gzip/stop-signal shim: env, `build/`, then conda prefix.

    Returns:
        An existing path to `libgrpcstop`.

    Raises:
        If no candidate path exists.
    """
    var name = _shim_filename()
    var candidates = List[String]()
    var env = getenv("GRPC_STOP_SHIM")
    if env != "":
        candidates.append(env)
    candidates.append(String("build/") + name)
    var prefix = getenv("CONDA_PREFIX")
    if prefix != "":
        candidates.append(prefix + "/lib/" + name)
    for c in candidates:
        if Path(c).exists():
            return c.copy()
    raise Error(
        "grpc: gzip shim not found; run bash tools/build_stop_signals.sh"
    )


def encoding_is_supported(value: StringSpan) -> Bool:
    """Reports whether a `grpc-encoding` token is identity or gzip.

    Args:
        value: Header value from `grpc-encoding`.

    Returns:
        True for `identity` and `gzip`; False for any other codec.
    """
    return value == "identity" or value == "gzip"


def _trim_ows(value: StringSpan) -> String:
    """Strips leading and trailing spaces and tabs from a header token.

    Args:
        value: One comma-separated `grpc-accept-encoding` token.

    Returns:
        The token with surrounding optional whitespace removed.
    """
    var n = value.byte_length()
    var start = 0
    var end = n
    var bytes = value.as_bytes()
    while start < end:
        var c = Int(bytes[start])
        if c != ord(" ") and c != ord("\t"):
            break
        start += 1
    while end > start:
        var c = Int(bytes[end - 1])
        if c != ord(" ") and c != ord("\t"):
            break
        end -= 1
    if start == 0 and end == n:
        return String(value)
    return String(value[byte=start:end])


def accept_encoding_allows_gzip(value: StringSpan) -> Bool:
    """Reports whether one `grpc-accept-encoding` value lists gzip.

    The value is a comma-separated list of content-codings. Each token is
    compared after stripping spaces and tabs. `gzip` matches; `gzip2` and
    `identity` do not.

    Args:
        value: One `grpc-accept-encoding` header value.

    Returns:
        True when a token is exactly `gzip`.
    """
    var parts = String(value).split(",")
    for part in parts:
        if _trim_ows(part) == "gzip":
            return True
    return False


def headers_accept_gzip(fields: Span[HeaderField, _]) -> Bool:
    """Reports whether request headers allow a gzip response.

    A missing `grpc-accept-encoding` header means identity only. When the
    header is repeated, gzip is allowed if any field lists it.

    Args:
        fields: The request header block.

    Returns:
        True when the client listed gzip.
    """
    for field in fields:
        if field.name == "grpc-accept-encoding":
            if accept_encoding_allows_gzip(field.value):
                return True
    return False


def gzip_compress(src: Span[Byte, _]) raises -> List[Byte]:
    """Compresses `src` as gzip (RFC 1952).

    Args:
        src: Uncompressed payload bytes.

    Returns:
        The gzip-wrapped compressed bytes.

    Raises:
        If the shim cannot be loaded or zlib deflate fails.
    """
    var lib = OwnedDLHandle(_shim_path())
    var bound = lib.get_function[c_int]("grpc_gzip_bound")(c_int(len(src)))
    if Int(bound) < 0:
        raise Error("grpc: gzip compress bound overflow")
    var dst = List[Byte]()
    dst.resize(Int(bound), 0)
    var n = lib.get_function[c_int]("grpc_gzip_compress")(
        src.unsafe_ptr(),
        c_int(len(src)),
        dst.unsafe_ptr(),
        bound,
    )
    if Int(n) < 0:
        raise Error("grpc: gzip compress failed")
    dst.shrink(Int(n))
    return dst^


def gzip_decompress(src: Span[Byte, _], *, max_size: Int) raises -> List[Byte]:
    """Decompresses a gzip payload, capping the uncompressed size.

    Args:
        src: Gzip-wrapped bytes.
        max_size: Maximum accepted uncompressed length.

    Returns:
        The uncompressed payload.

    Raises:
        If the shim cannot be loaded, the input is not gzip, or the
        uncompressed size would exceed `max_size`.
    """
    if max_size < 0:
        raise Error("grpc: gzip max_size must be non-negative")
    var lib = OwnedDLHandle(_shim_path())
    var cap = max_size
    if cap == 0:
        cap = 1
    var dst = List[Byte]()
    dst.resize(cap, 0)
    var n = lib.get_function[c_int]("grpc_gzip_decompress")(
        src.unsafe_ptr(),
        c_int(len(src)),
        dst.unsafe_ptr(),
        c_int(cap),
    )
    if Int(n) == -2 or Int(n) > max_size:
        raise Error("grpc: message exceeds max size")
    if Int(n) < 0:
        raise Error("grpc: gzip decompress failed")
    dst.shrink(Int(n))
    return dst^
