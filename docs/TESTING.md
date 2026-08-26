# Testing strategy

Correctness is checked against reference implementations, not against this
repo agreeing with itself.

## Layers

| Layer | What it proves | Command |
|---|---|---|
| Unit tests | Each public API behaves per its docs, including error paths and edge values | `pixi run test` |
| Compliance (differential) | Byte-level and semantic agreement with Python `protobuf`, `python-hpack`, `hyper-h2`/`hyperframe`, CPython sockets, and `grpcio` | `pixi run compliance` |
| Official suites | h2spec (RFC 7540/7541, 146 checks), Google protobuf conformance (1476 proto3 binary and JSON tests), the 12 canonical gRPC interop cases in both directions over h2c, TLS, and Unix sockets | part of `pixi run compliance` / `pixi run interop-official` |
| Interop smoke | grpcio client ↔ mojo server and mojo client ↔ grpcio server round trips | `pixi run interop` |
| Benchmarks | Throughput/latency baselines; CI smoke-runs `std.benchmark` and the grpc-mojo / grpcio / tonic comparison | `pixi run bench` / `pixi run bench-compare-smoke` |

Each sibling repo runs its own unit tests and compliance slice. grpc-mojo
aggregates those results and adds gRPC-level checks.

## Coverage

Mojo 1.0 has no line-coverage tooling, so coverage is a feature matrix:

1. Every public symbol has at least one test, including documented error paths.
2. Protocol-visible behavior is also pinned by a reference (golden bytes, RFC
   vectors, h2spec, or a live `grpcio` peer).
3. Bugs get a regression test at the lowest layer that can express them.

When adding a public API: happy path, one test per documented `Raises:`, and a
differential check if the behavior is visible on the wire.

## Layout

```
src/grpc/                     gRPC implementation
test/integration/             gRPC end-to-end tests
test/compliance/              gRPC vs grpcio + aggregated sibling suites
test/interop/                 grpcio interop (quick + official cases)
packages/<repo>/              gitignored sibling checkouts from fetch_deps.py
```

`tools/run_tests.py` uses per-suite include paths so tests cannot reach
outside declared dependencies. `tools/check_extraction.py` compiles each
package from a staging directory that contains only those dependencies.

## Benchmarks

```sh
pixi run bench
```

CI runs `--smoke` so the benches keep compiling. Full runs are indicative,
not gating.
