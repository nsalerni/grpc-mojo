# Testing strategy

This project's correctness claim rests on **differential testing against
reference implementations**, not on our own tests agreeing with our own
code. This document maps out every verification layer, what it covers, and
how to run it.

## The layers

| Layer | What it proves | Command |
|---|---|---|
| Unit tests | Each public API behaves per its docs, including error paths and edge values | `pixi run test` |
| Compliance (differential) | Byte-level and semantic agreement with Python `protobuf`, `python-hpack`, `hyper-h2`/`hyperframe`, CPython sockets, and `grpcio` | `pixi run compliance` |
| Official suites | h2spec (RFC 9113/7541, 146 checks), Google protobuf conformance (698 binary-format tests), the 12 canonical gRPC interop cases × 2 directions | part of `pixi run compliance` / `pixi run interop-official` |
| Interop smoke | grpcio client ↔ mojo server and mojo client ↔ grpcio server round trips | `pixi run interop` |
| Benchmarks | Throughput/latency baselines; CI runs them in `--smoke` mode to prove they build and execute | `pixi run bench` |

Every package repo (`mojo-net`, `protomojo`, `mojo-http2`) carries its own
unit tests, its own slice of the compliance suite, and its own benchmarks,
so each remains independently verifiable after extraction. The umbrella
repo aggregates the package suites and adds the gRPC-level checks.

## Unit-test coverage philosophy

Mojo 1.0 has no line/branch coverage tooling yet, so coverage is tracked
**by feature matrix, not by line percentage**:

1. Every public symbol (struct, method, function) must be exercised by at
   least one test in its own package — including its documented error
   paths. The current matrix was built by auditing each public API against
   the test suites; the `test_*_edges.mojo` files exist specifically to
   close the gaps that audit found (error paths, boundary values,
   lifecycle, platform behaviors like SIGPIPE suppression and typed
   timeouts).
2. Anything protocol-visible must additionally be pinned by a reference:
   golden bytes from Python `protobuf`, RFC 7541 Appendix C vectors,
   hyperframe byte-differentials, h2spec, or a live grpcio peer. Never
   assert our encoder against our own decoder alone.
3. Bugs found by any layer get a regression test at the *lowest* layer
   that can express them (e.g. the sint32 cast-fold bug is pinned by both
   a unit test and the protobuf differential).

When adding a public API, add: a happy-path unit test, one test per
documented `Raises:` condition, and — if the behavior is visible on the
wire — a differential check in the package's compliance runner.

## Test layout

```
packages/<pkg>/test/          unit tests (test_*.mojo, executables)
packages/<pkg>/compliance/    differential suite vs references (+ tools/)
packages/<pkg>/bench/         benchmarks (--smoke for CI)
test/integration/             umbrella gRPC end-to-end tests
test/compliance/              umbrella suite: gRPC vs grpcio + aggregation
test/interop/                 grpcio interop (quick + 12 official cases)
```

`tools/run_tests.py` runs every `test_*.mojo` with per-suite include paths
so package tests cannot accidentally reach outside their declared
dependencies; `tools/check_extraction.py` proves each package still
compiles and runs from a bare staging directory with only its declared
dependencies present.

## Benchmarks

Benchmarks use `std.benchmark` and print `ns/op` plus derived throughput.
They are indicative, not gating — CI runs them with `--smoke` (a few
milliseconds per benchmark) purely to keep them compiling and running.
Full runs use ~0.5 s per measurement:

```sh
pixi run bench          # umbrella: proto, hpack/h2, net, end-to-end gRPC
```

The end-to-end gRPC benchmark forks the server into a child process
(Mojo 1.0 has no threads), so its numbers include both sides of the stack
plus loopback TCP.
