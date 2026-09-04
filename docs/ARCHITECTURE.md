# grpc-mojo Architecture

grpc-mojo implements [gRPC over HTTP/2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
for Mojo 1.0. The stack is split across five repositories, the same way
`tonic` splits gRPC, HTTP/2, and codegen.

```text
┌─────────────────────────────────────────────────────┐
│  grpc        gRPC core: calls, status, metadata,     │
│              length-prefixed framing, client/server  │
├──────────────────────────┬──────────────────────────┤
│  h2                      │  proto                    │
│  HTTP/2 (RFC 9113)       │  Protobuf runtime         │
├──────────────┐           │                           │
│  hpack       │           │                           │
│  RFC 7541    │           │                           │
├──────────────┴───────────┴──────────────────────────┤
│  tls         TLS 1.2/1.3 over libssl                 │
│  net         TCP/UDP/Unix sockets, DNS, poller       │
└─────────────────────────────────────────────────────┘
```

## Repositories

Edges point strictly downward.

| Module | Repository | May import |
|---|---|---|
| `net` | [mojo-net](https://github.com/nsalerni/mojo-net) | standard library only |
| `hpack` | [mojo-http2](https://github.com/nsalerni/mojo-http2) | standard library only |
| `proto` | [protomojo](https://github.com/nsalerni/protomojo) | standard library only |
| `tls` | [mojo-tls](https://github.com/nsalerni/mojo-tls) | `net`, libssl |
| `h2` | [mojo-http2](https://github.com/nsalerni/mojo-http2) | `hpack`, `net`, `tls` |
| `grpc` | this repo (`src/grpc`) | `h2`, `proto`, `hpack`, `net`, `tls` |

Development checkouts clone the pinned sibling tags into gitignored
`packages/` (`python3 tools/fetch_deps.py`). See
[PACKAGING.md](PACKAGING.md).

## Layer notes

**net** binds POSIX sockets through `std.ffi.external_call` and exposes
`TCPListener` / `TCPStream`, UDP, Unix sockets, DNS, and a `Poller` over
kqueue/epoll.

**hpack** generates its static and Huffman tables from RFC 7541. **h2** is
the RFC 9113 connection state machine over `IOStream` (TCP, Unix, or TLS).

**proto** is the protobuf wire runtime plus `protoc-gen-mojo`. **grpc** adds
length-prefixed messages, status, metadata, deadlines, and client/server APIs
on top of HTTP/2.

## Concurrency

Mojo 1.0 has no public async I/O runtime. `Server` is blocking and sequential.
`PollingServer` uses kqueue or epoll to make bounded progress across many
unary h2c, TLS, or Unix connections on one thread; handlers still run
serially. Unix listeners are plaintext only. `request_stop()` drains via
GOAWAY on that same thread; there is no cross-thread stop.

## Out of scope for v0

Compression (`grpc-encoding: gzip`), retries, service config, load balancing,
channelz, and reflection. Health **Check** is implemented; **Watch** returns
UNIMPLEMENTED because status changes during a live stream need threads or
async. Tracked in [ROADMAP.md](ROADMAP.md).
