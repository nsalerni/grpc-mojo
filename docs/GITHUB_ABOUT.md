# GitHub About fields

Repository description, homepage, and topics are GitHub *settings*,
not files. The API token this stack's automation uses cannot `PATCH`
them (`403 Resource not accessible by integration`). Apply these in
each repo: **Settings → General → About**.

`main` is already protected against deletion and force-push.

Stack landing page (GitHub Pages from `grpc-mojo` `/`):
https://nsalerni.github.io/grpc-mojo/

| Repo | Description | Homepage | Topics |
|---|---|---|---|
| [mojo-net](https://github.com/nsalerni/mojo-net) | TCP, UDP, DNS, Unix sockets, and readiness polling for Mojo 1.0 | https://nsalerni.github.io/grpc-mojo/ | `mojo`, `networking`, `tcp`, `udp`, `sockets` |
| [mojo-tls](https://github.com/nsalerni/mojo-tls) | TLS 1.2/1.3 for Mojo 1.0 over libssl: SNI, ALPN, mTLS, session tickets | https://nsalerni.github.io/grpc-mojo/ | `mojo`, `tls`, `openssl`, `networking` |
| [mojo-http2](https://github.com/nsalerni/mojo-http2) | HPACK (RFC 7541) and HTTP/2 (RFC 9113) for Mojo 1.0 | https://nsalerni.github.io/grpc-mojo/ | `mojo`, `http2`, `hpack`, `networking` |
| [protomojo](https://github.com/nsalerni/protomojo) | Protocol Buffers for Mojo 1.0: proto3 binary, JSON, and protoc-gen-mojo | https://nsalerni.github.io/grpc-mojo/ | `mojo`, `protobuf`, `grpc`, `serialization` |
| [grpc-mojo](https://github.com/nsalerni/grpc-mojo) | gRPC client and server runtime for Mojo 1.0. Official interop 84/84 | https://nsalerni.github.io/grpc-mojo/ | `mojo`, `grpc`, `http2`, `protobuf`, `tls` |

`gh` equivalents once a token with `Administration: Read and write` is
available:

```sh
gh repo edit nsalerni/grpc-mojo \
  --description "gRPC client and server runtime for Mojo 1.0. Official interop 84/84" \
  --homepage "https://nsalerni.github.io/grpc-mojo/" \
  --add-topic mojo --add-topic grpc --add-topic http2 --add-topic protobuf --add-topic tls
```
