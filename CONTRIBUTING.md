# Contributing to grpc-mojo

Thanks for looking at the project. Protocol-visible behavior is checked
against reference implementations (`grpcio`, h2spec, protobuf conformance),
not against this repo agreeing with itself.

## Setup

```sh
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/nsalerni/grpc-mojo.git
cd grpc-mojo
pixi install
python3 tools/fetch_deps.py
pixi run test
```

Sibling packages ([mojo-net](https://github.com/nsalerni/mojo-net),
[mojo-http2](https://github.com/nsalerni/mojo-http2),
[mojo-tls](https://github.com/nsalerni/mojo-tls),
[protomojo](https://github.com/nsalerni/protomojo)) are separate repos.
`fetch_deps.py` clones the pinned tags into gitignored `packages/` so local
include paths work. Changes to those packages belong in their own
repositories.

## Checks

```sh
pixi run format
pixi run test
pixi run compliance          # if you change protocol behavior
pixi run interop-official    # if you change gRPC call semantics
```

Public APIs need docstrings in the
[Mojo docstring style](https://github.com/modular/modular/blob/main/mojo/stdlib/docs/docstring-style-guide.md).
This repo targets Mojo 1.0: `def` only, `comptime` not `alias`, `std.`-prefixed
imports. Hosts, method paths, metadata keys, and other borrowed text take
`StringSpan`. Filesystem paths are borrowed at the gRPC boundary and copied
to `String` at the TLS FFI boundary.

Generated files (`*_pb.mojo` and similar) should be regenerated, not edited:

```sh
pixi run gen-proto
```

## Pull requests

Fork, branch from `main`, and keep the change focused. Open an issue first
for large features; [docs/ROADMAP.md](docs/ROADMAP.md) lists planned work.

By contributing, you agree that your contributions are licensed under
[Apache License 2.0](LICENSE).
