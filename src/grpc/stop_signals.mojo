# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""C `sigaction` installer for `PollingServer` stop signals.

The handler only calls `write(2)` on the wakeup pipe, which is
async-signal-safe. This is not a cross-thread API.
"""

from std.ffi import OwnedDLHandle, c_int
from std.os import getenv
from std.pathlib import Path
from std.sys import CompilationTarget


def _shim_filename() -> String:
    comptime if CompilationTarget.is_macos():
        return "libgrpcstop.dylib"
    else:
        return "libgrpcstop.so"


def _shim_path() raises -> String:
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
        "grpc: stop-signal shim not found; run bash tools/build_stop_signals.sh"
    )


def install_wakeup_signal_handlers(write_fd: c_int) raises -> OwnedDLHandle:
    """Installs SIGTERM/SIGINT handlers that write `write_fd`.

    The returned handle must stay alive for as long as the handlers run;
    dropping it unmaps the handler and makes the next signal a crash.

    Args:
        write_fd: The `Wakeup.write_fd` to notify.

    Returns:
        The loaded shim library.

    Raises:
        If the shim cannot be loaded or `sigaction` fails.
    """
    var lib = OwnedDLHandle(_shim_path())
    var rc = lib.get_function[c_int]("grpc_install_stop_signals")(write_fd)
    if rc != 0:
        raise Error("grpc: sigaction for SIGTERM/SIGINT failed")
    return lib^
