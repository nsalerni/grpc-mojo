#!/bin/bash
# Builds build/libgrpcstop.{so,dylib} for PollingServer.install_stop_signals
# and the gzip codec (zlib, RFC 1952).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/shim/grpc_stop_signals.c"
GZIP_SRC="$ROOT/shim/grpc_gzip.c"
mkdir -p "$ROOT/build"

case "$(uname -s)" in
  Darwin)
    OUT="$ROOT/build/libgrpcstop.dylib"
    ;;
  *)
    OUT="$ROOT/build/libgrpcstop.so"
    ;;
esac

if [ -f "$OUT" ] && [ "$OUT" -nt "$SRC" ] && [ "$OUT" -nt "$GZIP_SRC" ]; then
  echo "stop-signal / gzip shim up to date: $OUT"
  exit 0
fi

CC_BIN="${CC:-cc}"
INCFLAGS=()
LIBFLAGS=(-lz)
if [ -n "${CONDA_PREFIX:-}" ]; then
  INCFLAGS+=("-I${CONDA_PREFIX}/include")
  LIBFLAGS+=("-L${CONDA_PREFIX}/lib")
fi

"$CC_BIN" -shared -fPIC -O2 -Wall -Werror \
  "${INCFLAGS[@]}" \
  -o "$OUT" "$SRC" "$GZIP_SRC" \
  "${LIBFLAGS[@]}"
echo "built $OUT"
