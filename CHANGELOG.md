# Changelog

## Unreleased

- `PollingServer` poll timeouts use remaining time until the next keepalive
  PING instead of the full interval, stay gated on the HTTP/2 preface, and
  do not delay an expired idle or RPC deadline with a later PING timer.
  An unsent socket suffix or queued HTTP/2 output keeps writable interest
  instead of a zero timeout.
- `PollingServerConfig.keepalive_interval_ns` queues HTTP/2 keepalive PINGs
  from the Poller loop after an idle interval. 0 (the default) disables
  them. PINGs wait until the client connection preface is complete.
  Requires mojo-http2 v0.2.6 and mojo-net v0.2.4.
- Documented the blocking `Server` vs `PollingServer` contract: serial
  handlers, one thread, multi-process for load.
- Added `GrpcChannel.set_max_message_size` so clients cap serialized
  request and response payloads with the same 4 MiB default as `Server`
  and `PollingServer`. Oversized sends raise before the request is
  written; oversized responses raise from `recv_message`.
- Added `Server.set_max_message_size` so the blocking server admits
  request and response payloads with the same 4 MiB default as
  `PollingServer`. Oversized messages finish the call with
  `RESOURCE_EXHAUSTED`.
- Set the gRPC `user-agent` string to `grpc-mojo/0.2.2`.
- Updated the source and package dependencies to mojo-http2 v0.2.5 and
  mojo-tls v0.3.0. Authenticated handlers can inspect copied DNS, URI, email,
  and canonical IP subject alternative names through
  `ServerContext.peer_certificate`.
- Updated the source and package dependency to protomojo v0.3.0. The
  aggregated protobuf section now includes 1476 passing official proto3
  binary and JSON cases.
- Exposed each authenticated client's owned leaf certificate snapshot through
  `ServerContext` on blocking and polling servers.
- Added optional client CA verification to `PollingServer.tls` with bounded
  non-blocking handshakes and rejection before request dispatch.
- Added optional client CA verification to `Server.tls` for services that
  require a trusted client certificate before request dispatch.
- Added optional client certificate chain and private key file paths to
  `GrpcChannel.connect_tls` for services that require mutual TLS.
- Added blocking gRPC clients and servers over Unix domain sockets.
- Expanded the official grpcio interop matrix to cover all 12 cases in both
  roles over Unix sockets, alongside h2c and TLS.

## 0.2.2 - 2026-08-22

- Added an opt-in `PollingServer` for bounded concurrent unary h2c or TLS
  connection I/O over kqueue and epoll. TLS handshakes are non-blocking,
  strictly require `h2` ALPN, and have explicit admission, work, and time
  limits. Handlers remain serialized.
- Made `GrpcTransport` implement `ReadinessStream` for pollable partial TCP
  and TLS I/O, including exact TLS retry direction.

## 0.2.1 - 2026-08-22

- Published a generated official interoperability report and badge backed by
  48/48 grpcio cases across both roles and transports.
- Aligned source and installed-package checks with mojo-net v0.2.2,
  mojo-tls v0.2.1, and mojo-http2 v0.2.2.
- Kept isolated package extraction verification compatible with queued
  HTTP/2 client startup.

## 0.2.0 - 2026-08-20

- Added verified gRPC over TLS for clients and servers through `mojo-tls`.
- Require the `h2` ALPN token on every secure gRPC connection.
- Expanded the official grpcio interop matrix to 48/48 cases across both
  roles and both h2c and TLS transports.
- Added an installable compiled package with clean-prefix verification on
  macOS and Linux.

## 0.1.0 - 2026-08-19

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
