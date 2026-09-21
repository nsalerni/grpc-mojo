# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""gzip codec for gRPC payloads, via zlib in the stop-signal shim library.

gRPC uses RFC 1952 gzip (not zlib wrapping). Compressed messages are
negotiated with `grpc-encoding` / `grpc-accept-encoding`. This module
does not wait on mojo-zlib; the C shim links libz from conda-forge.
"""

from std.ffi import OwnedDLHandle, c_int
from std.os import getenv
from std.pathlib import Path
from std.sys import CompilationTarget


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


def gzip_decompress(
    src: Span[Byte, _], *, max_size: Int
) raises -> List[Byte]:
    """Decompresses a gzip payload, capping the uncompressed size.

    Args:
        src: gzip-wrapped bytes.
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
