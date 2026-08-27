# Published gRPC benchmarks

Comparative loopback timings for **grpc-mojo**, **grpcio**, and **tonic**
on the same three shapes:

1. Unary echo, 11-byte message
2. Unary echo, 64 KiB message
3. Bidi ping-pong, 20 messages on one stream (reported per message)

Each implementation runs its own client against its own server on
`127.0.0.1`. grpc-mojo forks the server because Mojo 1.0 has no threads;
grpcio uses a thread pool; tonic uses tokio. That difference is part of
the stack, not a measurement error.

## How to run

```sh
pixi install
python3 tools/fetch_deps.py
pixi run bench-compare-smoke   # 5 iterations, CI
pixi run bench-compare         # 200 iterations
```

Tonic is included when `cargo` and `protoc` are on `PATH`. The sidecar
lives in `bench/tonic-echo/` and pins Rust 1.85 via `rust-toolchain.toml`.
If either tool is missing, the runner prints `tonic: skipped` and still
reports grpcio and grpc-mojo.

Do not commit nanosecond tables and do not `git diff` them. Numbers move
with CPU, thermal state, and iteration count. `--smoke` is only a build
and wiring check.

## What the numbers mean

Each shape records wall-clock nanoseconds for a complete RPC (or, for
bidi, one 20-message stream). The table prints **mean** and **p99** of
those samples. Bidi is divided by 20 so the column is per-message.

Warmup is two untimed unary calls. There is no keepalive, TLS, or
multi-channel load. This is a single-connection loopback comparison, not
a datacenter capacity number.

`pixi run bench` / `bench-smoke` still run the original
`std.benchmark` microbenches, including `bench/bench_grpc.mojo`.
