# grpc

gRPC over HTTP/2, with TCP, TLS, and Unix domain socket transports, per the
[gRPC HTTP/2 protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
`GrpcChannel` (client) and `Server` with all four RPC kinds, deadlines and
cancellation, ASCII and binary metadata, percent-encoded status messages,
and the `grpc-status-details-bin` rich error model.

Streaming model: server-, client-, and bidirectional streaming are fully
supported; bidi is receive-driven (ping-pong works; concurrent full-duplex
firehose waits on Mojo threads — [docs/PRIMITIVES.md](../../docs/PRIMITIVES.md)
item 7). Handlers are compile-time function parameters registered via
`register_unary` / `register_server_streaming` / `register_client_streaming`
/ `register_bidi`, or the generated `add_<service>_service` helper.

TLS clients use `GrpcChannel.connect_tls`; TLS servers use `Server.tls`.
Both require the `h2` ALPN token, and clients verify the certificate chain
and hostname by default. Clients can also load a PEM certificate chain and
private key from files when the server requires mutual TLS.
`Server.tls` and `PollingServer.tls` accept the path to a PEM CA bundle and
reject clients without a trusted certificate before request dispatch.
Handlers receive an owned snapshot in `ServerContext.peer_certificate`.
The field is `None` for h2c, Unix sockets, and TLS peers without a client
certificate. Authorization code must check the snapshot's `verified` field.

Blocking local services can use `GrpcChannel.connect_unix` with `Server.unix`
or `PollingServer.unix`. The client uses `localhost` as its default
`:authority`. Both servers refuse to replace an existing socket path unless
`remove_existing=True`. Unix listeners are plaintext only.

`PollingServer` is a separate opt-in server for unary h2c, TLS, or Unix
services. It uses `mojo-net` readiness polling to progress bounded I/O and
TLS handshakes across many connections on one thread. Its connection,
handshake, accept, inactivity, incomplete-request, read, frame, write,
message, and output limits are explicit. TLS requires the `h2` ALPN token
and can authenticate client certificates during its non-blocking handshake.
Handlers execute serially, and streaming methods remain on `Server`.

`Server.add_health_service` and `PollingServer.add_health_service` register
`grpc.health.v1` Check. `Health.set_status` / `status` keep the serving map;
the empty name is overall status and defaults to SERVING. Unknown names
return NOT_FOUND. Watch is not registered, so clients receive UNIMPLEMENTED
and fall back to Check.

Verification: the 12 canonical gRPC interop cases pass in both directions
over h2c, TLS, and Unix domain sockets against `grpcio`
(`pixi run interop-official`), plus
behavioral differential checks in `pixi run compliance` (all 16 status
codes, metadata, deadlines, 1 MB messages, rich errors, and TLS in both
roles).
