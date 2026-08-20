#!/usr/bin/env python3
"""Stage the standalone repository mirrors from this monorepo workspace.

Produces five self-contained package repo trees under --out (default
build/split), plus the umbrella:

  mojo-net/    <- packages/mojo-net
  protomojo/   <- packages/protomojo
  mojo-http2/  <- packages/mojo-http2 (+ source dependency tooling)
  mojo-tls/    <- packages/mojo-tls
  grpc-mojo/   <- repo root minus packages/ (fetch_deps repopulates
                  packages/ from the standalone repos, so every include
                  path keeps working unchanged)

Each tree gets its own CI workflow (tools/split/ci-<name>.yml), .gitignore,
and the shared community files. The script only stages files; committing
and pushing is done by the caller so authorship stays explicit.
"""

import argparse
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = ROOT / "tools" / "split"

EXCLUDE_NAMES = {".pixi", "build", ".deps", "__pycache__", ".DS_Store", ".git"}

PKG_GITIGNORE = """# pixi environments
.pixi/

# build outputs
build/
*.mojopkg
*.conda
*.bin

# fetched source dependencies
.deps/

# editor
.DS_Store
.vscode/
.idea/

"""

SHARED_FILES = ["CODE_OF_CONDUCT.md", "SECURITY.md"]

UMBRELLA_ITEMS = [
    "src",
    "test",
    "tools",
    "examples",
    "docs",
    "bench",
    "README.md",
    "LICENSE",
    "NOTICE",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    "CODE_OF_CONDUCT.md",
    "SECURITY.md",
    "pixi.toml",
    "pixi.lock",
    ".gitignore",
    ".gitattributes",
]


def copy_tree(src: Path, dst: Path):
    def ignore(_dir, names):
        return [n for n in names if n in EXCLUDE_NAMES]

    shutil.copytree(src, dst, ignore=ignore)


def write_workflow(repo_dir: Path, name: str):
    wf = TEMPLATES / f"ci-{name}.yml"
    dest = repo_dir / ".github" / "workflows" / "ci.yml"
    dest.parent.mkdir(parents=True)
    shutil.copy2(wf, dest)


def stage_package(stage: Path, name: str):
    dest = stage / name
    copy_tree(ROOT / "packages" / name, dest)
    (dest / ".gitignore").write_text(PKG_GITIGNORE)
    for f in SHARED_FILES:
        shutil.copy2(ROOT / f, dest / f)
    write_workflow(dest, name)
    if name == "mojo-http2":
        (dest / "tools").mkdir(exist_ok=True)
        shutil.copy2(ROOT / "tools" / "fetch_deps.py", dest / "tools" / "fetch_deps.py")
        (dest / "deps.json").write_text(
            json.dumps(
                {
                    "dir": ".deps",
                    "deps": {
                        "mojo-net": {"ref": "main"},
                        "mojo-tls": {"ref": "main"},
                    },
                },
                indent=2,
            )
            + "\n"
        )
    print(f"staged {name} -> {dest}")


def stage_umbrella(stage: Path):
    dest = stage / "grpc-mojo"
    dest.mkdir()
    for item in UMBRELLA_ITEMS:
        src = ROOT / item
        if not src.exists():
            raise SystemExit(f"missing expected root item: {item}")
        if src.is_dir():
            copy_tree(src, dest / item)
        else:
            shutil.copy2(src, dest / item)
    # The umbrella consumes its packages as fetched source deps; the
    # directory name stays `packages/` so include paths never change.
    gi = dest / ".gitignore"
    gi.write_text(gi.read_text() + "\n# fetched source dependencies\n/packages/\n")
    (dest / "deps.json").write_text(
        json.dumps(
            {
                "dir": "packages",
                "deps": {
                    "mojo-net": {"ref": "main"},
                    "protomojo": {"ref": "main"},
                    "mojo-http2": {"ref": "main"},
                    "mojo-tls": {"ref": "main"},
                },
            },
            indent=2,
        )
        + "\n"
    )
    write_workflow(dest, "grpc-mojo")
    print(f"staged grpc-mojo -> {dest}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "build" / "split"))
    args = ap.parse_args()
    stage = Path(args.out)
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    for name in ["mojo-net", "protomojo", "mojo-http2", "mojo-tls"]:
        stage_package(stage, name)
    stage_umbrella(stage)
    print("done")


if __name__ == "__main__":
    main()
