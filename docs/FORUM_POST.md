# Forum post: networking and gRPC for Mojo 1.0

Copy for the Modular forum. Post after the conda packages are on
modular-community.

---

**Title:** mojo-net, mojo-tls, mojo-http2, protomojo, and grpc-mojo

Mojo 1.0 has no sockets in the standard library. We built a stacked
community answer and published it on modular-community.

```sh
pixi add grpc-mojo    # or mojo-net / mojo-tls / mojo-http2 / protomojo
```

Channel: `https://repo.prefix.dev/modular-community`.

| Package | What it is | Checks |
|---|---|---|
| [mojo-net](https://github.com/nsalerni/mojo-net) | TCP, UDP, DNS, Unix, poller | 15/15 vs CPython `socket` |
| [mojo-tls](https://github.com/nsalerni/mojo-tls) | TLS 1.2/1.3, SNI, ALPN, mTLS | 34/34 vs CPython `ssl` |
| [mojo-http2](https://github.com/nsalerni/mojo-http2) | HPACK + HTTP/2 | h2spec 146/146 |
| [protomojo](https://github.com/nsalerni/protomojo) | proto3 binary + JSON + `protoc-gen-mojo` | 1476/1476 conformance |
| [grpc-mojo](https://github.com/nsalerni/grpc-mojo) | unary + streaming gRPC | official interop 84/84 |

Getting started: https://nsalerni.github.io/grpc-mojo/

**std.net.** mojo-net is the working prototype for a minimal stdlib
socket API (`SocketAddress`, `TCPListener`/`TCPStream`, `resolve()`,
typed errno errors). RFC: https://github.com/nsalerni/mojo-net/blob/main/docs/STDNET_RFC.md
If you currently bind `socket()` yourself, please depend on `mojo-net`
instead. We would rather merge efforts than compete.

**Not in this stack (on purpose):** HTTP/1.1, HTTP/3, proto2, in-process
thread pools. `PollingServer` overlaps I/O on one thread; handlers stay
serial. Scale-out is processes.

Happy to take design notes on the `std.net` core. Gzip in grpc-mojo
already ships via zlib; we would still like a 1.0-compatible
`mojo-zlib` so the C shim can move into a shared codec package.
