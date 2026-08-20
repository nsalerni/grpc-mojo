#!/usr/bin/env python3
"""grpc-mojo compliance suite (umbrella).

Every layer is differentially tested against an established reference
implementation:

  proto  vs  Python `protobuf` (the reference implementation)
  hpack  vs  python-hpack (the HPACK used by hyper-h2 / httpx)
  h2     vs  hyper-h2 + hyperframe (strict: raises ProtocolError on any
             protocol violation by our side)
  net    vs  CPython sockets (OS truth for TCP semantics)
  grpc   vs  grpcio (the reference gRPC implementation)

The proto, hpack, h2, and net sections are executed by each package's own
self-contained suite (packages/<pkg>/compliance/run_compliance.py) and
aggregated here; this script adds the gRPC sections, extraction isolation,
and the unit/interop suites, then writes the combined report.

Rerun with: pixi run compliance
Writes docs/COMPLIANCE.md and exits non-zero on any failure.
"""

import base64
import json
import platform
import re
import subprocess
import sys
import tempfile
import time
from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
BUILD = ROOT / "build"
TOOLS = ROOT / "test" / "compliance" / "tools"
REPORT = ROOT / "docs" / "COMPLIANCE.md"
HTML_REPORT = ROOT / "docs" / "COMPLIANCE.html"

RESULTS: dict[str, list[tuple[str, bool, str]]] = {}

# Package suites executed and aggregated by this umbrella (in report order).
PACKAGE_SUITES = ("protomojo", "mojo-http2", "mojo-net")


def record(section: str, name: str, ok: bool, detail: str = ""):
    RESULTS.setdefault(section, []).append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'} [{section}] {name}" + ("" if ok else f"  <- {detail}"))


def run_tool(binary: str, *args, timeout=60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(BUILD / binary), *map(str, args)],
        capture_output=True, text=True, timeout=timeout, cwd=ROOT,
    )


def build_tools():
    print("== building Mojo compliance tools ==")
    BUILD.mkdir(exist_ok=True)
    for src in sorted(TOOLS.glob("*.mojo")):
        out = BUILD / src.stem
        subprocess.run(
            ["mojo", "build", "-I", "packages/mojo-net/src", "-I", "packages/mojo-http2/src", "-I", "packages/protomojo/src", "-I", "src", "-I", "test", str(src), "-o", str(out)],
            check=True, cwd=ROOT,
        )
        print(f"  built {src.stem}")


# ------------------------------------------------------- package suites ---

def run_package_suites(tmp: Path):
    """Run each package's own compliance suite and aggregate its results.

    The package runners execute with this (root) interpreter — the root
    pixi env carries every reference dependency — and report their rows
    via --json under the same section keys the umbrella always used
    (proto, hpack, h2, net).
    """
    for pkg in PACKAGE_SUITES:
        pkg_dir = ROOT / "packages" / pkg
        json_path = tmp / f"{pkg}_compliance.json"
        print(f"== package suite: {pkg} ==", flush=True)
        r = subprocess.run(
            [sys.executable, "compliance/run_compliance.py", "--json", str(json_path)],
            cwd=pkg_dir,
        )
        if json_path.exists():
            data = json.loads(json_path.read_text())
            for section, rows in data["sections"].items():
                for name, ok, detail in rows:
                    RESULTS.setdefault(section, []).append((name, bool(ok), detail))
        else:
            record(pkg, f"{pkg} compliance suite produced results",
                   False, f"runner exited {r.returncode} without JSON output")


# ----------------------------------------------------------------- proto ---

def compile_test_protos(tmp: Path):
    subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc",
         f"-I{ROOT/'examples'}",
         f"--python_out={tmp}",
         str(ROOT / "examples" / "echo.proto")],
        check=True,
    )
    sys.path.insert(0, str(tmp))


def section_packaging(tmp: Path):
    """Each package must build and run staged with only its declared deps."""
    print("== packaging: extraction isolation ==")
    sys.path.insert(0, str(ROOT / "tools"))
    import check_extraction
    for pkg, deps in check_extraction.PACKAGES.items():
        ok, detail = check_extraction.check(pkg)
        dep_str = " + ".join(deps) if deps else "stdlib only"
        record("packaging",
               f"`{pkg}` builds and runs staged with {dep_str}", ok, detail)


# ----------------------------------------------------------------- grpc ---

def make_grpcio_probe_server(pb):
    import grpc
    code_by_num = {c.value[0]: c for c in grpc.StatusCode}

    def fail(req, ctx):
        num, _, details = req.message.partition("|")
        ctx.abort(code_by_num[int(num)], details)

    def meta_echo(req, ctx):
        pairs = []
        for k, v in ctx.invocation_metadata():
            if k.startswith("x-"):
                pairs.append(f"{k}={v.hex() if isinstance(v, bytes) else v}")
        ctx.send_initial_metadata((("x-initial", "from-python"),))
        ctx.set_trailing_metadata((("x-trailer", "python-trailer"),
                                   ("x-blob-bin", b"\xde\xad\xbe\xef")))
        return pb.EchoResponse(message=";".join(sorted(pairs)))

    def deadline(req, ctx):
        return pb.EchoResponse(message=str(int(ctx.time_remaining() * 1000)))

    def echo(req, ctx):
        return pb.EchoResponse(message=req.message)

    def sleep_2s(req, ctx):
        time.sleep(2.0)
        return pb.EchoResponse(message="finally")

    def fail_rich(req, ctx):
        from grpc_status import rpc_status
        from google.rpc import status_pb2, code_pb2
        st = status_pb2.Status(code=code_pb2.NOT_FOUND, message="rich error")
        ctx.abort_with_status(rpc_status.to_status(st))

    class Handler(grpc.GenericRpcHandler):
        def service(self, hcd):
            table = {"/probe.Probe/Fail": fail, "/probe.Probe/MetaEcho": meta_echo,
                     "/probe.Probe/Deadline": deadline, "/probe.Probe/Echo": echo,
                     "/probe.Probe/Sleep": sleep_2s,
                     "/probe.Probe/FailRich": fail_rich}
            fn = table.get(hcd.method)
            if fn is None:
                return None
            return grpc.unary_unary_rpc_method_handler(
                fn, request_deserializer=pb.EchoRequest.FromString,
                response_serializer=pb.EchoResponse.SerializeToString)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers((Handler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def section_grpc_client(tmp: Path):
    print("== grpc: mojo client vs grpcio server ==")
    import echo_pb2 as pb
    server, port = make_grpcio_probe_server(pb)
    try:
        # Every non-OK status code + unicode/percent details must map 1:1.
        all_ok, detail = True, ""
        for code in range(1, 17):
            details = f"détails for {code} 100%"
            r = run_tool("grpc_client_probe", port, "status", f"{code}|{details}")
            want = f"code={code} message={details}"
            if want not in r.stdout:
                all_ok, detail = False, f"code {code}: got {r.stdout.strip()!r} {r.stderr[:100]!r}"
                break
        record("grpc", "status-code mapping, all 16 codes + unicode/percent details", all_ok, detail)

        r = run_tool("grpc_client_probe", port, "meta")
        out = r.stdout
        checks = [
            ("request ascii metadata delivered", "x-ascii=hello meta" in out),
            ("request binary metadata (-bin) delivered", "x-payload-bin=0102ff00" in out),
            ("initial response metadata received", "initial=from-python" in out),
            ("trailing metadata received", "trailer=python-trailer" in out),
            ("trailing binary metadata decoded", "trailer-bin=deadbeef" in out),
        ]
        for name, ok in checks:
            record("grpc", name, ok, "" if ok else out.strip())

        r = run_tool("grpc_client_probe", port, "deadline", 2500)
        m = re.search(r"remaining_ms=(\d+) code=0", r.stdout)
        ok = bool(m) and 1500 <= int(m.group(1)) <= 2500
        record("grpc", "grpc-timeout propagated to grpcio deadline (2500ms sent)",
               ok, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "echo", 1_000_000, timeout=120)
        record("grpc", "1MB unary echo via grpcio server",
               "len=1000000 match=True code=0" in r.stdout, r.stdout.strip() + r.stderr[:150])

        r = run_tool("grpc_client_probe", port, "echo", 0)
        record("grpc", "empty message echo via grpcio server",
               "len=0 match=True code=0" in r.stdout, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "unicode")
        record("grpc", "unicode payload echo via grpcio server",
               "match=True code=0" in r.stdout, r.stdout.strip())

        r = run_tool("grpc_client_probe", port, "richstatus")
        from google.rpc import status_pb2
        m2 = re.search(r"code=5 details=([0-9a-f]+)", r.stdout)
        rich_ok = False
        if m2:
            st = status_pb2.Status.FromString(bytes.fromhex(m2.group(1)))
            rich_ok = st.code == 5 and st.message == "rich error"
        record("grpc", "rich error model: grpc-status-details-bin decoded from grpcio",
               rich_ok, r.stdout.strip())

        t0 = time.monotonic()
        r = run_tool("grpc_client_probe", port, "sleep", 300)
        took = time.monotonic() - t0
        record("grpc", "client deadline enforced vs sleeping server (300ms -> DEADLINE_EXCEEDED)",
               "code=4" in r.stdout and took < 1.5,
               f"out={r.stdout.strip()!r} took={took:.2f}s")
    finally:
        server.stop(0)


def section_grpc_server(tmp: Path):
    print("== grpc: grpcio client vs mojo server ==")
    import grpc
    import echo_pb2 as pb
    proc = subprocess.Popen([str(BUILD / "grpc_server_probe")], stdout=subprocess.PIPE, text=True, cwd=ROOT)
    line = proc.stdout.readline()
    port = int(line.strip().rsplit(":", 1)[-1])
    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        def method(name):
            return channel.unary_unary(
                f"/probe.Probe/{name}",
                request_serializer=pb.EchoRequest.SerializeToString,
                response_deserializer=pb.EchoResponse.FromString)

        big = "z" * 1_000_000
        resp = method("Echo")(pb.EchoRequest(message=big), timeout=60)
        record("grpc", "grpcio client: 1MB echo through mojo server", resp.message == big,
               f"len={len(resp.message)}")

        resp = method("Echo")(pb.EchoRequest(message=""), timeout=10)
        record("grpc", "grpcio client: empty message through mojo server", resp.message == "", "")

        resp = method("Timeout")(pb.EchoRequest(message="t"), timeout=3.0)
        ns = int(resp.message)
        # grpcio may round the wire timeout up slightly; allow 5% headroom.
        record("grpc", "grpcio deadline decoded by mojo server (3s sent)",
               2_000_000_000 <= ns <= 3_150_000_000, f"decoded {ns}ns")

        resp, call = method("MetaEcho").with_call(
            pb.EchoRequest(message="m"), timeout=10,
            metadata=(("x-py", "1"), ("x-raw-bin", b"\x00\xff")))
        seen = resp.message
        b64 = base64.b64encode(b"\x00\xff").decode().rstrip("=")
        meta_ok = "x-py=1" in seen and ("x-raw-bin=" + b64 in seen or "x-raw-bin=" + b64 + "=" in seen)
        record("grpc", "grpcio client metadata (ascii + -bin) visible in mojo handler", meta_ok, seen)
        imd = dict(call.initial_metadata())
        tmd = dict(call.trailing_metadata())
        record("grpc", "mojo server initial response metadata", imd.get("x-initial") == "from-mojo", str(imd))
        record("grpc", "mojo server trailing metadata", tmd.get("x-trailer") == "mojo-trailer", str(tmd))
        record("grpc", "mojo server binary trailing metadata (-bin, unpadded emit)",
               tmd.get("x-blob-bin") == b"\xde\xad\xbe\xef", str(tmd.get("x-blob-bin")))

        try:
            method("FailUnicode")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "mojo handler error -> grpcio UNKNOWN + unicode details", False, "no error raised")
        except grpc.RpcError as e:
            ok = (e.code() == grpc.StatusCode.UNKNOWN
                  and "falhou: résumé 100% 🔥" in e.details())
            record("grpc", "mojo handler error -> grpcio UNKNOWN + unicode details", ok,
                   f"{e.code()} {e.details()!r}")

        try:
            method("FailRich")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "mojo server rich error (grpc-status-details-bin)", False, "no error")
        except grpc.RpcError as e:
            from google.rpc import status_pb2
            tmd = dict(e.trailing_metadata())
            raw = tmd.get("grpc-status-details-bin")
            ok = False
            if raw:
                st = status_pb2.Status.FromString(raw)
                ok = st.code == 5 and st.message == "rich"
            record("grpc", "mojo server rich error (grpc-status-details-bin)", ok, str(tmd)[:150])

        try:
            method("Nope")(pb.EchoRequest(message="x"), timeout=10)
            record("grpc", "unknown method -> UNIMPLEMENTED trailers-only", False, "no error")
        except grpc.RpcError as e:
            record("grpc", "unknown method -> UNIMPLEMENTED trailers-only",
                   e.code() == grpc.StatusCode.UNIMPLEMENTED, str(e.code()))

        ok = True
        for i in range(10):
            r = method("Echo")(pb.EchoRequest(message=f"seq{i}"), timeout=10)
            ok = ok and r.message == f"seq{i}"
        record("grpc", "10 sequential calls on one connection", ok, "")
        channel.close()
    finally:
        proc.kill(); proc.wait()


# ---------------------------------------------------------------- units ---

def section_units():
    print("== unit suites (spec vectors + reference goldens) ==")
    r = subprocess.run([sys.executable, "tools/run_tests.py"], cwd=ROOT,
                       capture_output=True, text=True)
    m = re.search(r"(\d+)/(\d+) test files passed", r.stdout)
    ok = r.returncode == 0 and m and m.group(1) == m.group(2)
    record("units", f"unit test files ({m.group(0) if m else 'no summary'})", ok,
           r.stdout[-300:] if not ok else "")
    r = subprocess.run([sys.executable, "test/interop/run_interop.py"], cwd=ROOT,
                       capture_output=True, text=True)
    m = re.search(r"interop: (\d+) passed, (\d+) failed", r.stdout)
    ok = r.returncode == 0 and m and m.group(2) == "0"
    record("units", f"interop suite ({m.group(0) if m else 'no summary'})", ok,
           r.stdout[-300:] if not ok else "")


# --------------------------------------------------------------- report ---

def versions() -> dict[str, str]:
    import google.protobuf, grpc, hpack, h2, hyperframe
    mojo = subprocess.run(["mojo", "--version"], capture_output=True, text=True, cwd=ROOT).stdout.strip()
    return {
        "mojo": mojo,
        "python": platform.python_version(),
        "protobuf (reference for proto)": google.protobuf.__version__,
        "grpcio (reference for grpc)": grpc.__version__,
        "hpack (reference for hpack)": hpack.__version__,
        "h2/hyper-h2 (reference for h2)": h2.__version__,
        "hyperframe (reference for h2 frames)": hyperframe.__version__,
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
    }


def section_order() -> list[str]:
    """Canonical report order, plus any unexpected sections at the end."""
    known = ["proto", "hpack", "h2", "net", "grpc", "packaging", "units"]
    return known + [s for s in RESULTS if s not in known]


SECTION_TITLES = {
    "proto": ("`proto` vs Python `protobuf`",
              "Randomized differential testing: the reference implementation encodes seeded random messages; grpc-mojo decodes and re-encodes them; the reference parses the result and compares for semantic equality (byte equality where the encoding is deterministic). Malformed inputs must be accepted/rejected in agreement with the reference."),
    "hpack": ("`hpack` vs python-hpack",
              "Sequential header blocks encoded by one implementation and decoded by the other, in both directions, with dynamic-table state carried across blocks. Plus RFC 7541 Appendix C unit vectors (see `units`)."),
    "h2": ("`h2` vs hyper-h2 / hyperframe / h2spec",
           "Frame codec cross-checked byte-for-byte against hyperframe in both directions. Live connections run against hyper-h2, which raises ProtocolError on any protocol violation by the peer. h2spec (the standard RFC 9113/7541 conformance tool) runs its full suite against our server."),
    "net": ("`net` vs CPython sockets",
            "1 MiB echo in both directions between grpc-mojo TCP and CPython sockets, including half-close (shutdown) and clean-EOF semantics."),
    "grpc": ("`grpc` vs grpcio",
             "Behavioral compliance against the reference gRPC implementation in both directions: status-code mapping (all 16 codes), unicode/percent status details, ascii and binary (-bin) metadata in requests, initial response metadata and trailers, deadline (grpc-timeout) propagation, empty and 1 MB messages, sequential calls."),
    "packaging": ("Extraction isolation",
                  "Each package is staged into a scratch directory with only its declared dependencies (docs/ARCHITECTURE.md), then compiled and executed there. A package that reaches outside its dependency set fails this check — the mechanical proof behind independent open-sourcing."),
    "units": ("Spec-vector unit suites",
              "The repo's unit tests are themselves reference-anchored: protobuf goldens generated by Python protobuf, all RFC 7541 Appendix C vectors, and the grpcio interop suite."),
}




HTML_HEAD = """<!-- GENERATED by test/compliance/run_compliance.py - regenerate with: pixi run compliance -->
<title>grpc-mojo Compliance</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans+Condensed:wght@600&display=swap">
<style>
:root {
  --paper: #FAFAF8; --ink: #22262B; --muted: #6E6A62; --accent: #C2551F;
  --pass: #2E7D4F; --fail: #B3362B; --line: #E4E0D8; --panel: #F2F0EA;
  --mono: "IBM Plex Mono", ui-monospace, "SF Mono", Menlo, monospace;
  --sans: "IBM Plex Sans", -apple-system, "Segoe UI", sans-serif;
  --cond: "IBM Plex Sans Condensed", "Arial Narrow", var(--sans);
}
:root:not([data-theme="light"]) { }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --paper: #16181C; --ink: #E8E6E1; --muted: #98938A; --accent: #E0663A;
    --pass: #5EC08D; --fail: #E5776C; --line: #2C2F35; --panel: #1D2025;
  }
}
:root[data-theme="dark"] {
  --paper: #16181C; --ink: #E8E6E1; --muted: #98938A; --accent: #E0663A;
  --pass: #5EC08D; --fail: #E5776C; --line: #2C2F35; --panel: #1D2025;
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--paper); color: var(--ink);
  font: 16px/1.6 var(--sans); -webkit-font-smoothing: antialiased;
}
main { max-width: 76ch; margin: 0 auto; padding: 3.5rem 1.5rem 5rem; }
header { border-bottom: 2px solid var(--ink); padding-bottom: 1.75rem; margin-bottom: 2.5rem; }
.eyebrow {
  font: 500 0.72rem/1 var(--mono); letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--accent); margin: 0 0 0.9rem;
}
h1 {
  font: 600 clamp(1.9rem, 5vw, 2.6rem)/1.1 var(--cond);
  margin: 0 0 1.1rem; text-wrap: balance; letter-spacing: -0.01em;
}
.verdict { display: flex; align-items: baseline; gap: 0.75rem; flex-wrap: wrap; }
.verdict .score {
  font: 500 2rem/1 var(--mono); font-variant-numeric: tabular-nums;
  color: var(--pass);
}
.verdict .score.failing { color: var(--fail); }
.verdict .when { color: var(--muted); font-size: 0.85rem; }
.thesis { color: var(--muted); margin: 0.9rem 0 0; max-width: 62ch; }
.scorecard {
  display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 1.5rem; padding: 0; list-style: none;
}
.scorecard li {
  font: 400 0.78rem/1 var(--mono); padding: 0.45rem 0.7rem;
  border: 1px solid var(--line); border-radius: 3px; background: var(--panel);
  display: flex; gap: 0.55rem; align-items: center;
}
.scorecard .n { font-variant-numeric: tabular-nums; color: var(--pass); font-weight: 500; }
.scorecard .n.failing { color: var(--fail); }
section { margin: 2.75rem 0; }
h2 { font: 600 1.15rem/1.3 var(--sans); margin: 0 0 0.35rem; text-wrap: balance; }
h2 .pkg { font-family: var(--mono); font-weight: 500; color: var(--accent); }
.vs { color: var(--muted); font-weight: 400; }
.method { color: var(--muted); font-size: 0.88rem; margin: 0 0 1.1rem; max-width: 68ch; }
.tablewrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 0.88rem; }
th {
  text-align: left; font: 500 0.7rem/1 var(--mono); letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--muted); padding: 0 0.75rem 0.5rem 0;
  border-bottom: 1px solid var(--ink);
}
td { padding: 0.5rem 0.75rem 0.5rem 0; border-bottom: 1px solid var(--line); vertical-align: top; }
td.result { white-space: nowrap; font: 500 0.78rem/1.8 var(--mono); }
.pass { color: var(--pass); }
.fail { color: var(--fail); }
td .detail { display: block; color: var(--muted); font-size: 0.8rem; }
.envtable td:first-child { color: var(--muted); width: 40%; }
.envtable td { font-family: var(--mono); font-size: 0.8rem; }
.gaps { border-left: 3px solid var(--accent); background: var(--panel); padding: 1.1rem 1.4rem; }
.gaps h2 { margin-top: 0; }
.gaps ul { margin: 0.5rem 0 0; padding-left: 1.1rem; }
.gaps li { margin: 0.45rem 0; font-size: 0.9rem; }
.gaps strong { font-weight: 600; }
footer { margin-top: 3rem; color: var(--muted); font: 400 0.78rem/1.6 var(--mono); border-top: 1px solid var(--line); padding-top: 1rem; }
code { font-family: var(--mono); font-size: 0.92em; }
</style>
"""


def esc(t: str) -> str:
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def write_html_report():
    total = sum(len(v) for v in RESULTS.values())
    passed = sum(1 for v in RESULTS.values() for _, ok, _ in v if ok)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    all_ok = passed == total
    h = [HTML_HEAD, "<main>", "<header>"]
    h.append('<p class="eyebrow">grpc-mojo &middot; differential compliance run</p>')
    h.append("<h1>Every layer, checked against the reference implementation</h1>")
    h.append(
        f'<div class="verdict"><span class="score{"" if all_ok else " failing"}">'
        f"{passed}/{total}</span><span>checks passed</span>"
        f'<span class="when">{now}</span></div>'
    )
    h.append(
        '<p class="thesis">No self-grading: protobuf bytes are judged by Python '
        "<code>protobuf</code>, header compression by python-hpack, HTTP/2 by "
        "hyper-h2 (which rejects any protocol violation), sockets by CPython, "
        "and gRPC behavior by <code>grpcio</code> in both directions. The "
        "proto, hpack, h2, and net sections run in each package&rsquo;s own "
        "compliance suite and are aggregated here.</p>"
    )
    h.append('<ul class="scorecard">')
    for section in section_order():
        if section not in RESULTS:
            continue
        rows = RESULTS[section]
        p = sum(1 for _, ok, _ in rows if ok)
        cls = "" if p == len(rows) else " failing"
        h.append(
            f'<li>{esc(section)} <span class="n{cls}">{p}/{len(rows)}</span></li>'
        )
    h.append("</ul></header>")

    for section in section_order():
        if section not in RESULTS:
            continue
        title, blurb = SECTION_TITLES.get(section, (f"`{section}`", ""))
        pkg, _, ref = title.replace("`", "").partition(" vs ")
        rows = RESULTS[section]
        h.append("<section>")
        if ref:
            h.append(
                f'<h2><span class="pkg">{esc(pkg)}</span> '
                f'<span class="vs">vs</span> {esc(ref)}</h2>'
            )
        else:
            h.append(f"<h2>{esc(pkg)}</h2>")
        h.append(f'<p class="method">{esc(blurb)}</p>')
        h.append('<div class="tablewrap"><table>')
        h.append("<tr><th>Check</th><th>Result</th></tr>")
        for name, ok, detail in rows:
            cell = '<span class="pass">PASS</span>' if ok else '<span class="fail">FAIL</span>'
            extra = "" if ok else f'<span class="detail">{esc(detail[:200])}</span>'
            h.append(
                f"<tr><td>{esc(name)}</td><td class=\"result\">{cell}{extra}</td></tr>"
            )
        h.append("</table></div></section>")

    h.append("<section><h2>Environment</h2>")
    h.append('<div class="tablewrap"><table class="envtable">')
    for k, v in versions().items():
        h.append(f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>")
    h.append("</table></div></section>")

    h.append('<section class="gaps"><h2>Known gaps (tracked, not silent)</h2><ul>')
    gaps = [

        ("Compression", "grpc-encoding negotiation plumbing exists; codecs need a zlib binding (PRIMITIVES.md #4). Compressed messages are rejected, never mis-decoded."),
        ("TLS", "h2c plaintext only (PRIMITIVES.md #3)."),

        ("Concurrency", "connections served sequentially and bidi is receive-driven until Mojo exposes threads/async (PRIMITIVES.md #7)."),
        ("hpack value encoding", "header values are UTF-8 Strings; arbitrary octets are out of scope for now (gRPC uses base64 -bin metadata)."),
    ]
    for k, v in gaps:
        h.append(f"<li><strong>{esc(k)}</strong> &mdash; {esc(v)}</li>")
    h.append("</ul></section>")
    h.append(
        "<footer>Generated by test/compliance/run_compliance.py &middot; "
        "rerun with <code>pixi run compliance</code> &middot; canonical copy: "
        "docs/COMPLIANCE.md</footer>"
    )
    h.append("</main>")
    HTML_REPORT.write_text("\n".join(h))
    print(f"report: {HTML_REPORT.relative_to(ROOT)}")


def write_report():
    total = sum(len(v) for v in RESULTS.values())
    passed = sum(1 for v in RESULTS.values() for _, ok, _ in v if ok)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Compliance & Compatibility Report",
        "",
        "<!-- GENERATED by test/compliance/run_compliance.py — do not edit. -->",
        "<!-- Regenerate with: pixi run compliance -->",
        "",
        f"**Result: {passed}/{total} checks passed.** Generated {now}.",
        "",
        "Every check compares grpc-mojo against an established reference",
        "implementation — never against itself. The proto, hpack, h2, and net",
        "sections are executed by each package's own compliance suite",
        "(`packages/<pkg>/compliance/run_compliance.py`) and aggregated here;",
        "the umbrella adds the gRPC, packaging, and unit-suite sections.",
        "",
        "## Environment",
        "",
        "| Component | Version |",
        "|---|---|",
    ]
    for k, v in versions().items():
        lines.append(f"| {k} | {v} |")
    for section in section_order():
        if section not in RESULTS:
            continue
        title, blurb = SECTION_TITLES.get(section, (f"`{section}`", ""))
        rows = RESULTS[section]
        p = sum(1 for _, ok, _ in rows if ok)
        lines += ["", f"## {title} — {p}/{len(rows)}", "", blurb, "",
                  "| Check | Result |", "|---|---|"]
        for name, ok, detail in rows:
            status = "✅ pass" if ok else f"❌ **fail** — {detail[:160]}"
            lines.append(f"| {name} | {status} |")
    lines += [
        "",
        "## Known gaps (tracked, not silent)",
        "",

        "- **Compression**: `grpc-encoding` negotiation plumbing exists; codecs need a zlib binding (docs/PRIMITIVES.md item 4). Compressed messages are rejected, not mis-decoded.",
        "- **TLS**: h2c plaintext only (PRIMITIVES.md item 3).",

        "- **Concurrency**: connections served sequentially until Mojo exposes threads/async (PRIMITIVES.md item 7).",
        "",
    ]
    REPORT.write_text("\n".join(lines))
    print(f"\ncompliance: {passed}/{total} checks passed")
    print(f"report: {REPORT.relative_to(ROOT)}")
    write_html_report()
    return passed == total


def main() -> int:
    build_tools()
    with tempfile.TemporaryDirectory(prefix="grpc_mojo_compliance_") as tmp_s:
        tmp = Path(tmp_s)
        run_package_suites(tmp)
        compile_test_protos(tmp)
        section_packaging(tmp)
        section_grpc_client(tmp)
        section_grpc_server(tmp)
    section_units()
    return 0 if write_report() else 1


if __name__ == "__main__":
    sys.exit(main())
