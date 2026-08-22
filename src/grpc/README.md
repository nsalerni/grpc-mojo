# grpc

gRPC over HTTP/2, with h2c and TLS transports, per the
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
and hostname by default.

`PollingServer` is a separate opt-in server for unary h2c or TLS services. It
uses `mojo-net` readiness polling to progress bounded I/O and TLS handshakes
across many connections on one thread. Its connection, handshake, accept,
inactivity, incomplete-request, read, frame, write, message, and output limits
are explicit. TLS requires the `h2` ALPN token. Handlers execute serially, and
streaming methods remain on `Server`.

Verification: the 12 canonical gRPC interop cases pass in both directions
over h2c and TLS against `grpcio` (`pixi run interop-official`), plus
behavioral differential checks in `pixi run compliance` (all 16 status
codes, metadata, deadlines, 1 MB messages, rich errors, and TLS in both
roles).
