# grpc

gRPC over HTTP/2 (h2c) per the
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

Verification: the 12 canonical gRPC interop cases pass in both directions
against `grpcio` (`pixi run interop-official`), plus behavioral differential
checks in `pixi run compliance` (all 16 status codes, metadata, deadlines,
1 MB messages, rich errors).
