#!/usr/bin/env python3
"""Build the mojo-tls shim and copy its test assets into the umbrella build."""

import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TLS_ROOT = ROOT / "packages" / "mojo-tls"


def main() -> int:
    if not (TLS_ROOT / "src").is_dir():
        sys.exit("dependency 'mojo-tls' not found; run `python3 tools/fetch_deps.py`")
    subprocess.run(
        ["bash", str(TLS_ROOT / "tools" / "build_shim.sh")], check=True
    )
    subprocess.run(
        ["bash", str(TLS_ROOT / "tools" / "gen_test_certs.sh")], check=True
    )

    build = ROOT / "build"
    certs = build / "certs"
    certs.mkdir(parents=True, exist_ok=True)
    shim = "libmojotls.dylib" if platform.system() == "Darwin" else "libmojotls.so"
    shutil.copy2(TLS_ROOT / "build" / shim, build / shim)
    for source in (TLS_ROOT / "build" / "certs").iterdir():
        if source.is_file():
            shutil.copy2(source, certs / source.name)
    print(f"prepared TLS shim and certificates in {build}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
