# Missing Mojo Primitives — Upstreaming Plan

> Sequencing and phase gates for executing this plan live in
> [ROADMAP.md](ROADMAP.md) (Tracks C and D).

Building gRPC exposed real gaps in Mojo 1.0's standard library. This document
tracks each gap, what we built instead, and the concrete path to contributing
it back — either to [modular/modular](https://github.com/modular/modular)'s
stdlib or as standalone community packages. Everything in this repo is layered
(see [ARCHITECTURE.md](ARCHITECTURE.md)) so extraction is a directory move,
not a rewrite.

## 1. Sockets / networking (`src/net`) — **stdlib candidate**

**Gap**: Mojo 1.0 has `std.io.FileDescriptor` but no `socket()`, no address
types, no TCP/UDP API. Community projects (e.g. lightbug_http) each carry
their own libc bindings.

**What we built**: POSIX socket bindings via `std.ffi.external_call`
(macOS/Linux, incl. the `sockaddr_in`/`addrinfo` layout differences), plus
`TCPListener`/`TCPStream` with a Go-`net`-shaped API — now including DNS
resolution (`getaddrinfo`), IPv4/IPv6 `SocketAddress`, `UDPSocket`, and
read/write timeouts with a typed timeout error. **Publish-ready.**

**Upstream path**:
1. Open a forum RFC thread ("std.net design") referencing this implementation
   as the working prototype; Modular's stdlib takes contributions via PRs with
   design discussion first for new modules.
2. Propose the minimal core: `SocketAddress` (v4/v6), `TCPListener`,
   `TCPStream`, error mapping from `errno`. Keep `setsockopt` surface small
   (`SO_REUSEADDR`, `TCP_NODELAY`).
3. Until accepted, publish as a standalone package `mojo-net` on the
   modular-community conda channel (prefix.dev) so other projects stop
   re-binding libc.

## 2. Async I/O reactor — **standalone package first**

**Gap**: `async fn` exists in the language and `std.runtime` has an internal
`TaskGroup`, but there is no public event loop, no async sockets, no
timers-as-futures. Everything async-adjacent is `_`-prefixed/private.

**What we built**: `mojo-net` provides a `Poller` over kqueue and epoll plus
non-blocking readiness streams. `grpc-mojo` uses it in the unary h2c
`PollingServer`, while the full server remains blocking.

**Plan**: keep the poll-based API usable without an async runtime, then add
`async fn read()/write()` adapters once Modular stabilizes the coroutine ABI.
This should not go into stdlib until Modular's own async design lands. Track
the forum's async discussions and align with that work.

## 3. TLS — **standalone package**

**What we built**: `mojo-tls`, a small C shim over system libssl with TLS
1.2/1.3, strict chain and hostname verification, SNI, and ALPN in both roles.
`TLSStream` conforms to `IOStream`. `mojo-http2` and grpc-mojo use that seam
for verified HTTP/2 and gRPC connections, and require the `h2` ALPN token.

**Verification**: CPython `ssl`, h2spec TLS mode, and grpcio all exercise the
implementation across client and server roles in CI.

## 4. Compression codecs (gzip/deflate): **integration pending**

**Available package**:
[`mojo-zlib`](https://github.com/gabrieldemarmiesse/mojo-zlib) provides Mojo
bindings to zlib. Mojo's stdlib still has no zlib/deflate API.

**Remaining work**: integrate a gzip codec with grpc-mojo's existing compressed
flag and `grpc-accept-encoding` negotiation, then verify it against grpcio. Use
that integration experience to inform a `std.compress` proposal upstream.

## 5. Big-endian byte order helpers — **small stdlib PR**

**Gap**: `std.bit` has `byte_swap` but no `to_be_bytes`/`from_be_bytes`-style
helpers on integers; every protocol library re-writes them.

**What we built**: `put_u32_be`/`get_u32_be` etc. in each package's
`bytes.mojo`.

**Upstream path**: small, uncontroversial PR to `std.bit` or `std.memory` —
`Int.to_bytes[endianness]()` / `from_bytes` (Rust-style). Good first
contribution; file the stdlib proposal issue with the API sketch and the three
call sites in this repo as motivation.

## 6. Base64 URL-safe + unpadded variants — **small stdlib PR**

**Gap**: `std.base64` covers standard alphabet; gRPC `-bin` metadata requires
accepting padded *and* unpadded input and emitting unpadded.

**What we built**: tolerant decode / unpadded encode in `src/grpc/metadata.mojo`.

**Upstream path**: PR adding `b64encode[padded=False]` and
lenient-decode option to `std.base64`.

## 7. Threads / structured concurrency — **blocked on Modular**

**Gap**: Mojo 1.0 has `async def` syntax but the entire task runtime went
private post-1.0 (`std.runtime._asyncrt`, explicitly unstable), there is no
`std.thread`, and `parallelize` lives in MAX, not the stdlib. A gRPC server
cannot serve connections concurrently without one of: threads, a public task
API, or an event loop (item 2).

**What we did**: the full `Server` keeps sequential blocking connections for
TLS and all four RPC kinds. The separate unary h2c `PollingServer` uses the
mojo-net reactor to overlap bounded connection I/O on one thread, while
handler calls remain serialized. `pthread_create` via FFI with
`abi("C")` callbacks is feasible today and is the pragmatic next step for a
community `mojo-threads` package — but thread-safety guarantees around Mojo's
ownership model need Modular's input before publishing one. Raise this in the
stdlib forum alongside the async RFC.

## Sequencing

| # | Item | Vehicle | Effort | When |
|---|------|---------|--------|------|
| 5 | BE/LE int↔bytes | modular/modular PR | S | now |
| 6 | base64 variants | modular/modular PR | S | now |
| 1 | `std.net` sockets | RFC thread + community package → stdlib PR | M | after v0 ships |
| 4 | gzip integration | existing `mojo-zlib` package, then `std.compress` proposal | M | next |
| 2 | reactor | community package (`mojo-reactor`) | L | after Modular async stabilizes |
| 7 | threads | forum RFC + `mojo-threads` package | M | next: unblocks concurrent server |
| 3 | TLS | community package (`mojo-tls`) | complete | shipped |
