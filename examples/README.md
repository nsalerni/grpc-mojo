# Examples

| File | What it shows |
|---|---|
| [echo_server.mojo](echo_server.mojo) | Unary, server-streaming, client-streaming, and bidi handlers |
| [echo_client.mojo](echo_client.mojo) | Generated `EchoClient` plus an UNIMPLEMENTED check |
| [polling_tls_server.mojo](polling_tls_server.mojo) | `PollingServer.tls` with handshake and output limits |
| [echo.proto](echo.proto) | Schema used by the examples |

```sh
python3 tools/fetch_deps.py
pixi run example-server   # terminal 1
pixi run example-client   # terminal 2
```

TLS, Unix sockets, and client certificates are covered in
[polling_tls_server.mojo](polling_tls_server.mojo) and in the module docs for
`Server`, `PollingServer`, and `GrpcChannel`.
