#!/usr/bin/env python3
"""Build grpc-mojo and test it in an isolated package environment."""

import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ROOT / "packages"
CHANNELS = ["https://conda.modular.com/max", "conda-forge"]
RATTLER_BUILD = "rattler-build>=0.30,<0.31"
RATTLER_INDEX = "rattler-index>=0.30,<0.31"
BUILD_TIMEOUT_SECONDS = 30 * 60
TEST_TIMEOUT_SECONDS = 5 * 60
DEPENDENCIES = [
    ("mojo-net", "0.2.4", PACKAGES / "mojo-net"),
    ("protomojo", "0.4.0", PACKAGES / "protomojo"),
    ("mojo-tls", "0.3.0", PACKAGES / "mojo-tls"),
    ("mojo-http2", "0.2.7", PACKAGES / "mojo-http2"),
]


def run(
    command: list[str],
    cwd: Path = ROOT,
    timeout_seconds: int = BUILD_TIMEOUT_SECONDS,
) -> None:
    environment = os.environ.copy()
    environment.pop("PIXI_PROJECT_MANIFEST", None)
    subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=True,
        timeout=timeout_seconds,
    )


def git_output(command: list[str], cwd: Path) -> str:
    result = subprocess.run(
        ["git", *command],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def verify_dependency(name: str, version: str, source: Path) -> None:
    tag = f"v{version}"
    try:
        expected = git_output(["rev-parse", f"{tag}^{{commit}}"], source)
        actual = git_output(["rev-parse", "HEAD"], source)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"dependency '{name}' is missing the {tag} tag"
        ) from error
    if actual != expected:
        raise RuntimeError(
            f"dependency '{name}' HEAD does not match the {tag} tag"
        )
    if git_output(["status", "--porcelain"], source):
        raise RuntimeError(f"dependency '{name}' checkout is not clean")


def platform_subdir() -> str:
    machine = platform.machine().lower()
    if platform.system() == "Darwin" and machine == "arm64":
        return "osx-arm64"
    if platform.system() == "Linux" and machine in {"x86_64", "amd64"}:
        return "linux-64"
    if platform.system() == "Linux" and machine in {"aarch64", "arm64"}:
        return "linux-aarch64"
    raise RuntimeError(f"unsupported package platform: {platform.system()} {machine}")


def add_channels(command: list[str], local_channel: Path) -> None:
    command.extend(["--channel", local_channel.as_uri()])
    for channel in CHANNELS:
        command.extend(["--channel", channel])


def index_channel(channel: Path) -> None:
    run(
        [
            "pixi",
            "exec",
            "--spec",
            RATTLER_INDEX,
            "rattler-index",
            "fs",
            str(channel),
        ]
    )


def build_package(
    name: str,
    version: str,
    source: Path,
    output: Path,
    local_channel: Path,
) -> Path:
    recipe = source / "recipe" / "recipe.yaml"
    if not recipe.is_file():
        raise RuntimeError(f"dependency '{name}' has no package recipe")
    command = [
        "pixi",
        "exec",
        "--spec",
        RATTLER_BUILD,
        "rattler-build",
        "build",
        "--recipe",
        str(recipe),
        "--output-dir",
        str(output),
        "--no-test",
    ]
    add_channels(command, local_channel)
    run(command)

    artifacts = sorted(output.rglob(f"{name}-{version}-*.conda"))
    if len(artifacts) != 1:
        raise RuntimeError(
            f"expected one {name} {version} package, found {len(artifacts)}"
        )
    return artifacts[0]


def main() -> None:
    for name, version, source in DEPENDENCIES:
        if not source.is_dir():
            raise RuntimeError(
                f"dependency '{name}' not found; run python3 tools/fetch_deps.py"
            )
        if not (source / "recipe" / "recipe.yaml").is_file():
            raise RuntimeError(f"dependency '{name}' has no package recipe")
        verify_dependency(name, version, source)

    with tempfile.TemporaryDirectory(prefix="grpc-mojo-package-") as temp:
        work = Path(temp)
        channel = work / "channel"
        channel_subdir = channel / platform_subdir()
        channel_subdir.mkdir(parents=True)
        (channel / "noarch").mkdir()
        index_channel(channel)

        for name, version, source in DEPENDENCIES:
            artifact = build_package(
                name, version, source, work / name, channel
            )
            shutil.copy2(artifact, channel_subdir / artifact.name)
            index_channel(channel)

        artifact = build_package(
            "grpc-mojo", "0.2.6", ROOT, work / "grpc-mojo", channel
        )
        test_command = [
            "pixi",
            "exec",
            "--spec",
            RATTLER_BUILD,
            "rattler-build",
            "test",
            "--package-file",
            str(artifact),
        ]
        add_channels(test_command, channel)
        run(test_command, timeout_seconds=TEST_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
