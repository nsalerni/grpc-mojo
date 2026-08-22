# grpc-mojo Architecture

grpc-mojo is a pure-Mojo implementation of [gRPC over HTTP/2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md),
targeting Mojo 1.0. It is deliberately structured as a stack of **independent,
extractable packages** — the same layering used by `tonic` (Rust), where the
codegen, HTTP/2, and gRPC layers are separate crates.

```
┌─────────────────────────────────────────────────────┐
│  grpc        gRPC core: calls, status, metadata,     │
│              length-prefixed framing, client/server  │
├──────────────────────────┬──────────────────────────┤
│  h2                      │  proto                    │
│  HTTP/2 (RFC 9113):      │  Protobuf runtime:        │
│  frames, streams,        │  wire format (varint,     │
│  connection state,       │  zigzag, all wire types), │
│  flow control            │  message traits, codegen  │
├──────────────┐           │  support                  │
│  hpack       │           │                           │
│  RFC 7541:   │           │                           │
│  static +    │           │                           │
│  dynamic     │           │                           │
│  tables,     │           │                           │
│  Huffman     │           │                           │
├──────────────┴───────────┴──────────────────────────┤
│  net         TCP sockets over libc FFI               │
│              (candidate for upstreaming to the       │
│              Mojo standard library)                  │
└─────────────────────────────────────────────────────┘
```

## Dependency rules

Edges point strictly downward. Each package may only import from the packages
listed here, so any of them can be extracted into a standalone repo unchanged:

| Package | Location | May import | Purpose |
|---------|----------|-----------|---------|
| `net`   | `packages/mojo-net` | stdlib only | Sockets, DNS, TCP/UDP |
| `hpack` | `packages/mojo-http2` | stdlib only | HPACK header compression |
| `proto` | `packages/protomojo` | stdlib only | Protobuf wire format + runtime |
| `tls`   | `packages/mojo-tls` | `net`, libssl | TLS 1.2/1.3, X.509 verification, ALPN |
| `h2`    | `packages/mojo-http2` | `hpack`, `net`, `tls` | HTTP/2 framing, connection, streams |
| `grpc`  | `src/grpc` | `h2`, `proto`, `net`, `tls` | gRPC protocol, client, server |

Each `packages/<repo>` directory is a self-contained image of its future
standalone repository: own manifest, tests, README, and license
(see [PACKAGING.md](PACKAGING.md)).

`proto` and `hpack` are pure functions over bytes — no I/O — which keeps them
trivially testable against the RFC/spec test vectors.

Extractability is enforced mechanically, not by convention:
`tools/check_extraction.py` (run in `pixi run compliance` and directly via
`pixi run check-extraction`) stages every package into a scratch directory
with only its declared dependencies and compiles + runs a smoke program
there.

## Layer notes

### `net` — sockets (see [PRIMITIVES.md](PRIMITIVES.md))

Mojo 1.0's standard library has **no socket API**. `net` binds the POSIX
socket calls (`socket`, `bind`, `listen`, `accept`, `connect`, `send`, `recv`,
`setsockopt`, `close`) through `std.ffi.external_call`, and exposes
`TCPListener` / `TCPStream` types shaped after Go's `net` and the draft APIs
discussed in the Mojo community. Blocking I/O first; the event-loop layer
(kqueue/epoll) is an explicit later stage, tracked in PRIMITIVES.md.

### `hpack` — RFC 7541

The static table (61 entries) and the Huffman code table (257 symbols) are
**generated** from the RFC text by `tools/gen_hpack_tables.py` — never
hand-transcribed. Decoder uses a flattened Huffman FSM; encoder uses the
canonical code table. Dynamic table with size updates and eviction per §4.

### `h2` — RFC 9113

Frame codec for all ten frame types, client/server connection preface,
SETTINGS negotiation, stream multiplexing with the state machine from §5.1,
and connection + stream flow control. Scope is deliberately
"the subset gRPC requires, implemented correctly": PRIORITY frames are parsed
and ignored (as RFC 9113 deprecates them), CONTINUATION is handled on receive.

### `proto` — protobuf runtime

Implements the [protobuf wire format](https://protobuf.dev/programming-guides/encoding/):
varint, zigzag (sint), fixed32/64, length-delimited, packed repeated fields,
field skipping for unknown fields, and nested messages. Messages implement a
`ProtoMessage` trait (`write_to` / `merge_from` / `byte_size`), so generated
code and hand-written messages share one runtime. Codegen from `.proto` files
is a `protoc` plugin (Python, in `tools/`) emitting Mojo structs — chosen over
a pure-Mojo `.proto` parser so we inherit protoc's battle-tested parsing and
descriptor semantics.

### `grpc` — the protocol

* Length-prefixed message framing (1-byte compressed flag + 4-byte BE length).
* Full `Status` code set with the RST_STREAM / HTTP-status mapping tables from
  the spec.
* Metadata with `-bin` base64 handling (accept padded, emit unpadded) and
  percent-encoding for `grpc-message`.
* `grpc-timeout` encoding/decoding (H/M/S/m/u/n units, ≤8 digits).
* Client: unary and streaming calls over one HTTP/2 connection.
* Server: service registry keyed by `/package.Service/Method` path, unary and
  streaming handlers, Trailers-Only for immediate errors, 415 for non-gRPC
  content types. `PollingServer` is a separate unary h2c or TLS path that uses
  queue-based HTTP/2 operations and readiness-driven partial socket I/O.
* Transport: h2c over `TCPStream`, or verified TLS over `TLSStream` with the
  `h2` ALPN token required in both roles.

## Concurrency model

Mojo 1.0 has `async fn` and an internal `TaskGroup` runtime, but no public,
stable async I/O story. The full `Server` therefore retains its blocking,
sequential connection model for all four RPC kinds. The opt-in unary
`PollingServer` uses kqueue or epoll to make bounded progress across many h2c
or TLS connections on one thread. Its handler calls are still serialized, so
a slow handler stalls the event loop. `GrpcTransport` keeps transport
mechanics below the gRPC protocol layer in both models.

`PollingServerConfig` bounds connections, accepted and pending TLS handshakes,
handshake work and time, inactivity, incomplete requests, socket bytes,
dispatched HTTP/2 frames, message bytes, and unsent wire bytes. One established
connection retains at most `max_message_size + 5` response bytes and
`max_pending_output_size` wire bytes. Across the server, those bounds scale
linearly with `max_connections`. OpenSSL's per-handshake and per-session
allocations sit outside the plaintext message and wire-output byte limits, so
pending handshakes have their own admission cap.

## Testing strategy

* **Spec vectors first**: every RFC 7541 Appendix C example, protobuf encoding
  examples from protobuf.dev, and golden bytes produced by Python's
  `grpcio`/`protobuf` are checked into `test/`.
* **Interop**: `test/interop/` runs the Mojo client against a `grpcio` Python
  server and vice versa over h2c and TLS. The definition of "compatible" is
  "talks to the reference implementation".
* Unit tests use `std.testing` and run with `pixi run test`.

## Out of scope for v0 (tracked in PRIMITIVES.md and issues)

Compression codecs (mojo-zlib exists, but gzip integration with the existing
flag/negotiation plumbing is pending), retries/service
config, load balancing, channelz/reflection/health services.
