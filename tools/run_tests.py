#!/usr/bin/env python3
"""Test runner: builds and runs every test_*.mojo with per-package includes.

Mojo 1.0 removed `mojo test`; tests here are ordinary executables with a
`def main()` that runs the file's checks and prints a pass line. Each
package's tests run against that package's include set only, matching the
sibling repositories checked out into packages/.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# suite root -> include directories (relative to repo root)
SUITES = {
    "packages/mojo-net/test": [
        "packages/mojo-net/src",
        "packages/mojo-net/test",
    ],
    "packages/protomojo/test": [
        "packages/protomojo/src",
        "packages/protomojo/test",
    ],
    # mojo-tls runs from its package root: the shim library and cert
    # fixtures live under its build/ directory.
    "packages/mojo-tls/test": [
        "packages/mojo-tls/src",
        "packages/mojo-net/src",  # declared dependency of tls
        "packages/mojo-tls/test",
    ],
    "packages/mojo-http2/test": [
        "packages/mojo-http2/src",
        "packages/mojo-net/src",  # declared dependency of h2
        "packages/mojo-tls/src",  # declared dependency of h2 TLS
        "packages/mojo-http2/test",
    ],
    "test": [  # umbrella (grpc + integration)
        "packages/mojo-net/src",
        "packages/mojo-http2/src",
        "packages/mojo-tls/src",
        "packages/protomojo/src",
        "src",
        "test",
    ],
}


# suite root -> working directory for the run (default: repo root)
SUITE_CWD = {
    "packages/mojo-tls/test": "packages/mojo-tls",
}

# suite root -> idempotent setup commands run once before its tests
SUITE_SETUP = {
    "packages/mojo-tls/test": [
        ["bash", "packages/mojo-tls/tools/build_shim.sh"],
        ["bash", "packages/mojo-tls/tools/gen_test_certs.sh"],
    ],
}

def suite_for(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    return max((s for s in SUITES if rel.startswith(s + "/")), key=len)


def includes_for(suite: str) -> list[str]:
    out: list[str] = []
    for inc in SUITES[suite]:
        out += ["-I", str(ROOT / inc)]
    return out


def main() -> int:
    tests = sorted(
        t
        for pattern in ("test/**/test_*.mojo", "packages/*/test/test_*.mojo")
        for t in ROOT.glob(pattern)
        if not t.name.endswith("_pb.mojo") and "gen" not in t.parts
    )
    if not tests:
        print("no tests found")
        return 1
    failed = []
    prepared: set[str] = set()
    for t in tests:
        rel = t.relative_to(ROOT)
        suite = suite_for(t)
        if suite not in prepared:
            for cmd in SUITE_SETUP.get(suite, []):
                subprocess.run(cmd, cwd=ROOT, check=True)
            prepared.add(suite)
        try:
            proc = subprocess.run(
                ["mojo", "run", *includes_for(suite), str(t)],
                cwd=ROOT / SUITE_CWD.get(suite, "."),
                capture_output=True,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired as e:
            failed.append(rel)
            print(f"FAIL {rel} (timeout after 600s)")
            if e.stdout:
                out = e.stdout if isinstance(e.stdout, str) else e.stdout.decode()
                print(out)
            continue
        if proc.returncode == 0:
            print(f"PASS {rel}")
            if proc.stdout.strip():
                print("     " + proc.stdout.strip().replace("\n", "\n     "))
        else:
            failed.append(rel)
            print(f"FAIL {rel}")
            print(proc.stdout)
            print(proc.stderr)
    print(f"\n{len(tests) - len(failed)}/{len(tests)} test files passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
