#!/usr/bin/env python3
"""Test runner: builds and runs every test_*.mojo with per-package includes.

Mojo 1.0 removed `mojo test`; tests here are ordinary executables with a
`def main()` that runs the file's checks and prints a pass line. Each
package's tests run against that package's include set only, mirroring the
future standalone repos.
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
    "packages/mojo-http2/test": [
        "packages/mojo-http2/src",
        "packages/mojo-net/src",  # declared dependency of h2
        "packages/mojo-http2/test",
    ],
    "test": [  # umbrella (grpc + integration)
        "packages/mojo-net/src",
        "packages/mojo-http2/src",
        "packages/protomojo/src",
        "src",
        "test",
    ],
}


def includes_for(path: Path) -> list[str]:
    rel = path.relative_to(ROOT).as_posix()
    best = max((s for s in SUITES if rel.startswith(s + "/")), key=len)
    out: list[str] = []
    for inc in SUITES[best]:
        out += ["-I", inc]
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
    for t in tests:
        rel = t.relative_to(ROOT)
        try:
            proc = subprocess.run(
                ["mojo", "run", *includes_for(t), str(t)],
                cwd=ROOT,
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
