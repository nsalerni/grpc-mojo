#!/bin/bash
# Builds build/libgrpcstop.{so,dylib} for PollingServer.install_stop_signals.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/shim/grpc_stop_signals.c"
mkdir -p "$ROOT/build"

case "$(uname -s)" in
  Darwin)
    OUT="$ROOT/build/libgrpcstop.dylib"
    ;;
  *)
    OUT="$ROOT/build/libgrpcstop.so"
    ;;
esac

if [ -f "$OUT" ] && [ "$OUT" -nt "$SRC" ]; then
  echo "stop-signal shim up to date: $OUT"
  exit 0
fi

CC_BIN="${CC:-cc}"
"$CC_BIN" -shared -fPIC -O2 -Wall -Werror -o "$OUT" "$SRC"
echo "built $OUT"
