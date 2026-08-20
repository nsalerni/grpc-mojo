# Changelog

## Unreleased

- Added verified gRPC over TLS for clients and servers through `mojo-tls`.
- Require the `h2` ALPN token on every secure gRPC connection.
- Expanded the official grpcio interop matrix to 48/48 cases across both
  roles and both h2c and TLS transports.

## 0.1.0 — 2026-08-19

Initial release.

- gRPC over HTTP/2 for Mojo 1.0: unary, server-streaming,
  client-streaming, and bidirectional calls, on both the client
  (`GrpcChannel`) and server (`Server`) side.
- Deadlines (`grpc-timeout`) enforced on both sides; cancellation;
  ASCII and binary (`-bin`) metadata; percent-coded status messages;
  the rich error model (`grpc-status-details-bin`).
- Verified against grpcio in both directions: the 12 canonical interop
  cases × 2 directions, plus a differential compliance suite spanning
  protobuf, HPACK, HTTP/2 (h2spec 146/146), sockets, and gRPC semantics.
- Built on [mojo-net](https://github.com/nsalerni/mojo-net),
  [protomojo](https://github.com/nsalerni/protomojo), and
  [mojo-http2](https://github.com/nsalerni/mojo-http2).
- Single-threaded by design until Mojo exposes threads; the server
  dispatch seam is ready for concurrency.
