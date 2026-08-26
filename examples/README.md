# Examples

| File | What it shows |
|---|---|
| [echo_server.mojo](echo_server.mojo) | Unary, server-streaming, client-streaming, and bidi handlers |
| [echo_client.mojo](echo_client.mojo) | Generated `EchoClient` plus an UNIMPLEMENTED check |
| [polling_tls_server.mojo](polling_tls_server.mojo) | `PollingServer.tls` with handshake and output limits |
| [echo.proto](echo.proto) | Schema used by the examples |

```sh
python3 tools/fetch_deps.py
pixi run example-server          # terminal 1; prints the bound port
pixi run example-client -- PORT  # terminal 2
```

Unix sockets (`Server.unix` / `GrpcChannel.connect_unix`) and client
certificates are documented in [src/grpc/README.md](../src/grpc/README.md),
not in these example files.
