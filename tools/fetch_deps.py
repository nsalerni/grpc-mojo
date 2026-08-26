#!/usr/bin/env python3
"""Fetch source dependencies for a standalone checkout.

The grpc-mojo family is five repositories. Until conda packages are the
default, dependents clone pinned tags from deps.json into a gitignored
directory (grpc-mojo uses packages/<name>; mojo-http2 and mojo-tls use
.deps/<name>) so include paths keep working.

Already-present directories are left untouched, so a local clone wins over
a fresh fetch. Use --update to fast-forward previously fetched clones to
their pinned ref.

URL selection: $GIT_URL_TEMPLATE (default
"https://github.com/nsalerni/{name}.git").
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TEMPLATE = "https://github.com/nsalerni/{name}.git"


def main() -> int:
    manifest = ROOT / "deps.json"
    if not manifest.exists():
        print("no deps.json: nothing to fetch")
        return 0
    spec = json.loads(manifest.read_text())
    template = os.environ.get("GIT_URL_TEMPLATE", DEFAULT_TEMPLATE)
    update = "--update" in sys.argv[1:]
    dest_root = ROOT / spec.get("dir", "packages")
    dest_root.mkdir(exist_ok=True)

    for name, dep in spec["deps"].items():
        dest = dest_root / name
        ref = dep.get("ref", "main")
        if dest.exists():
            if not update:
                print(f"  {name}: already present at {dest} (skipped)")
                continue
            subprocess.run(["git", "-C", str(dest), "fetch", "origin", ref], check=True)
            subprocess.run(["git", "-C", str(dest), "checkout", ref], check=True)
            subprocess.run(
                ["git", "-C", str(dest), "pull", "--ff-only", "origin", ref],
                check=False,
            )
            print(f"  {name}: updated to {ref}")
            continue
        url = dep.get("url") or template.format(name=name)
        print(f"  {name}: cloning {url} @ {ref}")
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, url, str(dest)],
            check=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
