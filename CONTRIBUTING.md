# Contributing to grpc-mojo

Thanks for your interest! This project aims to be a correct, verifiable gRPC
implementation for Mojo — and a source of reusable primitives for the Mojo
ecosystem. Contributions of all sizes are welcome.

## Getting set up

```sh
# toolchain (pixi manages Mojo and the Python reference implementations)
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/nsalerni/grpc-mojo && cd grpc-mojo
pixi install

# make sure everything is green before you start
pixi run test
```

## The one rule that matters most

**Correctness comes from reference implementations, never from our own code
agreeing with itself.** Protobuf behavior is pinned by golden bytes from
Python `protobuf`, HPACK by the RFC 7541 vectors, HTTP/2 by hyper-h2 and
h2spec, and gRPC semantics by `grpcio`. If you change protocol behavior, the
change must be validated by:

```sh
pixi run test              # unit suites (seconds)
pixi run compliance        # differential suite vs references (~2 min)
pixi run interop-official  # the 12 canonical gRPC interop cases (~1 min)
```

CI runs all three on macOS and Linux. A PR that only passes tests it wrote
for itself will be asked for reference-anchored coverage.

## Project layout and boundaries

Dependency edges point strictly down so every package stays extractable
(see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)):

| Package | May import |
|---|---|
| `packages/mojo-net`, `packages/protomojo`, `packages/mojo-http2/src/hpack` | standard library only |
| `packages/mojo-http2/src/h2` | `hpack`, `net` |
| `src/grpc` | everything above |

Each `packages/<repo>` folder is a self-contained image of a future
standalone repository — keep new files inside the right package, with its
tests in that package's `test/` directory.

Please don't add upward or sideways imports — extraction as standalone
packages ([docs/PRIMITIVES.md](docs/PRIMITIVES.md)) depends on it.

## Generated files

`packages/mojo-http2/src/hpack/tables.mojo`,
`packages/protomojo/test/proto_golden.mojo`, and every `*_pb.mojo` are
generated. Never edit them by hand — change the generator (in the owning
package's `tools/`) and run:

```sh
pixi run gen-hpack     # HPACK tables from RFC 7541
pixi run gen-vectors   # protobuf goldens via Python protobuf
pixi run gen-proto     # *_pb.mojo via tools/protoc-gen-mojo
```

`packages/protomojo/test/proto_messages.mojo` is the hand-written reference
for what the codegen emits; keep the two structurally in sync.

## Style

- Run `pixi run format` (mojo format) before committing.
- Public APIs need docstrings in the
  [Mojo docstring style](https://github.com/modular/modular/blob/main/mojo/stdlib/docs/docstring-style-guide.md).
  Check coverage per file with:
  `pixi run mojo doc --diagnose-missing-doc-strings -o /dev/null <file>`
- Comments state constraints the code can't express (spec section numbers,
  platform quirks, ownership rules) — not what the next line does.
- This repo targets Mojo 1.0: `def` only (no `fn`), `comptime` not `alias`,
  `std.`-prefixed imports, explicit `.copy()`/`^` moves, and tests are plain
  executables run by `tools/run_tests.py` (`mojo test` no longer exists).

## Submitting changes

1. Fork and branch from `main`.
2. Keep PRs focused; separate mechanical churn from behavior changes.
3. Include tests — reference-anchored where behavior is protocol-visible.
4. Make sure CI is green.

For large changes (new protocol features, new packages), open an issue first
so we can agree on the approach — [docs/ROADMAP.md](docs/ROADMAP.md) shows
where the project is headed and what's already planned.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
