#!/usr/bin/env python3
"""Prove each package is independently extractable.

For every package, stage it into a scratch directory together with ONLY its
declared dependencies (docs/ARCHITECTURE.md), then compile and run a smoke
program against that staging area. A package that secretly reaches outside
its declared dependency set fails to build here — this is the mechanical
check behind the "standalone by design" claim.

Run directly or via the compliance suite (pixi run compliance).
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# package -> declared dependencies (must mirror docs/ARCHITECTURE.md)
PACKAGES: dict[str, list[str]] = {
    "net": [],
    "hpack": [],
    "proto": [],
    "tls": ["net"],
    "h2": ["hpack", "net", "tls"],
    "grpc": ["h2", "hpack", "net", "proto", "tls"],
}

# package -> source directory after fetch_deps.py
LOCATIONS: dict[str, str] = {
    "net": "packages/mojo-net/src/net",
    "hpack": "packages/mojo-http2/src/hpack",
    "h2": "packages/mojo-http2/src/h2",
    "proto": "packages/protomojo/src/proto",
    "tls": "packages/mojo-tls/src/tls",
    "grpc": "src/grpc",
}

SMOKE = {
    "net": """\
from net import SocketAddress, TCPListener, TCPStream, UDPSocket, resolve

def main() raises:
    var addr = SocketAddress.parse("127.0.0.1", 0)
    var listener = TCPListener("127.0.0.1", 0)
    var client = TCPStream.connect("127.0.0.1", listener.local_port)
    var server_side = listener.accept()
    client.write_all(String("x").as_bytes())
    var got = server_side.read_exact(1)
    if Int(got[0]) != ord("x"):
        raise Error("echo mismatch")
    client.close()
    server_side.close()
    listener.close()
    print("net-smoke-ok")
""",
    "hpack": """\
from hpack import Decoder, Encoder, HeaderField

def main() raises:
    var enc = Encoder()
    var dec = Decoder()
    var block = List[Byte]()
    var fields = [
        HeaderField(name=String(":method"), value=String("GET")),
        HeaderField(name=String("x-smoke"), value=String("ok")),
    ]
    enc.encode(Span(fields), block)
    var back = dec.decode(Span(block))
    if len(back) != 2 or back[1].value != "ok":
        raise Error("roundtrip mismatch")
    print("hpack-smoke-ok")
""",
    "proto": """\
from proto import WireReader, WireWriter

def main() raises:
    var w = WireWriter()
    w.int32(1, 150)
    var buf = w^.take()
    var r = WireReader(Span(buf))
    var tag = r.read_tag()
    if tag[0] != 1 or r.int32_value() != 150:
        raise Error("wire roundtrip mismatch")
    print("proto-smoke-ok")
""",
    "tls": """\
from tls import TLSContext, TLSStream

def main():
    print("tls-smoke-ok")
""",
    "h2": """\
from h2 import FrameHeader, Http2Connection, Settings
from net import TCPListener, TCPStream

def main() raises:
    var buf = List[Byte]()
    var h = FrameHeader(length=3, frame_type=0, flags=1, stream_id=7)
    h.serialize(buf)
    var parsed = FrameHeader.parse(Span(buf))
    if parsed.stream_id != 7:
        raise Error("frame header mismatch")
    # Live handshake over loopback proves connection wiring too.
    var listener = TCPListener("127.0.0.1", 0)
    var ct = TCPStream.connect("127.0.0.1", listener.local_port)
    var st = listener.accept()
    var client = Http2Connection(ct^, is_client=True)
    var server = Http2Connection(st^, is_client=False)
    # Client startup is queued until the transport caller flushes it.
    client.flush_output()
    server.process_next_frame()
    client.process_next_frame()
    client.close()
    server.close()
    listener.close()
    print("h2-smoke-ok")
""",
    "grpc": """\
from grpc import Metadata, Status, StatusCode, frame_message

def main() raises:
    var md = Metadata()
    md.add("x-smoke", "ok")
    var payload: List[Byte] = [1, 2, 3]
    var framed = frame_message(Span(payload))
    if len(framed) != 8:
        raise Error("framing mismatch")
    var st = Status(code=StatusCode.OK, message=String())
    if not st.is_ok():
        raise Error("status mismatch")
    print("grpc-smoke-ok")
""",
}


def check(pkg: str, verbose: bool = True) -> tuple[bool, str]:
    """Stage `pkg` + declared deps in isolation; compile and run a smoke."""
    with tempfile.TemporaryDirectory(prefix=f"extract_{pkg}_") as tmp_s:
        tmp = Path(tmp_s)
        stage = tmp / "src"
        stage.mkdir()
        for p in [pkg] + PACKAGES[pkg]:
            shutil.copytree(
                ROOT / LOCATIONS[p], stage / p,
                ignore=shutil.ignore_patterns("README.md"),
            )
        smoke = tmp / "smoke.mojo"
        smoke.write_text(SMOKE[pkg])
        r = subprocess.run(
            ["mojo", "run", "-I", str(stage), str(smoke)],
            capture_output=True, text=True, timeout=600,
        )
        if r.returncode != 0:
            return False, f"build or run failed: {r.stderr[-400:]}"
        if r.returncode != 0 or f"{pkg}-smoke-ok" not in r.stdout:
            return False, f"smoke run failed: {r.stdout!r} {r.stderr[-200:]!r}"
    return True, ""


def main() -> int:
    failed = 0
    for pkg, deps in PACKAGES.items():
        ok, detail = check(pkg)
        dep_str = " + ".join(deps) if deps else "stdlib only"
        print(f"{'PASS' if ok else 'FAIL'} {pkg} (staged with {dep_str})"
              + ("" if ok else f"  <- {detail}"))
        failed += 0 if ok else 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
