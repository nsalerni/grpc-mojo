# grpc-mojo

[![CI](https://github.com/nsalerni/grpc-mojo/actions/workflows/ci.yml/badge.svg)](https://github.com/nsalerni/grpc-mojo/actions/workflows/ci.yml)
[![Official gRPC interop](https://img.shields.io/endpoint?url=https%3A%2F%2Fnsalerni.github.io%2Fgrpc-mojo%2Fdocs%2Fofficial-interop-badge.json)](https://nsalerni.github.io/grpc-mojo/docs/COMPLIANCE.html)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Mojo 1.0](https://img.shields.io/badge/mojo-1.0-orange.svg)](https://www.modular.com/mojo)

gRPC for [Mojo](https://www.modular.com/mojo) 1.0: unary and streaming RPCs over
h2c, TLS, and Unix sockets.

**[Compliance report](https://nsalerni.github.io/grpc-mojo/docs/COMPLIANCE.html)**
([Markdown](docs/COMPLIANCE.md)) is regenerated on every CI run.

## Install

```sh
curl -fsSL https://pixi.sh/install.sh | sh
git clone https://github.com/nsalerni/grpc-mojo.git
cd grpc-mojo
pixi install
python3 tools/fetch_deps.py
```

`fetch_deps.py` clones the sibling packages into gitignored `packages/` so
local include paths work. A conda recipe lives in [`recipe/`](recipe/).

```sh
pixi run test
pixi run example-server          # terminal 1; prints the bound port
pixi run example-client -- PORT  # terminal 2
```

## Example

Generate Mojo from a `.proto` (plugin comes from [protomojo](https://github.com/nsalerni/protomojo)):

```sh
pixi run python -m grpc_tools.protoc -Iexamples \
  --plugin=protoc-gen-mojo=packages/protomojo/tools/protoc-gen-mojo \
  --mojo_out=examples examples/echo.proto
```

Server ([examples/echo_server.mojo](examples/echo_server.mojo) registers all
four RPC kinds):

```mojo
from echo_pb import ECHO_SAY_PATH, EchoRequest, EchoResponse
from grpc import Server, ServerContext

def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("echo: ") + req.message
    return resp^

def main() raises:
    var server = Server("127.0.0.1", 50051)
    server.register_unary[say](ECHO_SAY_PATH)
    server.serve()
```

Client ([examples/echo_client.mojo](examples/echo_client.mojo)):

```mojo
from echo_pb import EchoClient, EchoRequest

def main() raises:
    var client = EchoClient.connect("127.0.0.1", 50051)
    var req = EchoRequest()
    req.message = "hello"
    print(client.say(req, timeout_ns=10_000_000_000).message)
```

Runnable examples are listed in [examples/README.md](examples/README.md).
Unix sockets and mTLS are documented in [src/grpc/README.md](src/grpc/README.md).

## Features

- Unary, server-streaming, client-streaming, and bidirectional RPCs
- Deadlines, cancellation, ASCII and binary (`-bin`) metadata
- `protoc-gen-mojo` message types and client/server stubs
- h2c, TLS (`h2` ALPN), and Unix domain sockets
- Optional `PollingServer` for many unary h2c, TLS, or Unix connections
  on one thread, with `request_stop()` GOAWAY drain
- `grpc.health.v1` Check (`Watch` returns UNIMPLEMENTED)

## Current limits

- Blocking `Server` handles one connection at a time; `PollingServer` overlaps
  I/O but still runs handlers serially
- Streaming RPCs use the blocking server; bidi is receive-driven
- No compression codecs yet
- TLS uses one certificate chain; SNI-based selection is not supported

## Compliance

Official gRPC interop (12 cases in both directions over h2c, TLS, and Unix
sockets), h2spec, Google protobuf conformance, and differential checks against
`grpcio` and the sibling packages. See [docs/COMPLIANCE.md](docs/COMPLIANCE.md)
and [docs/TESTING.md](docs/TESTING.md).

## Related packages

| Package | Role |
|---|---|
| [mojo-net](https://github.com/nsalerni/mojo-net) | TCP, UDP, DNS, Unix sockets, poller |
| [mojo-tls](https://github.com/nsalerni/mojo-tls) | TLS 1.2/1.3 over libssl |
| [mojo-http2](https://github.com/nsalerni/mojo-http2) | HPACK and HTTP/2 |
| [protomojo](https://github.com/nsalerni/protomojo) | Protobuf runtime and `protoc-gen-mojo` |

## Documentation

- [src/grpc/README.md](src/grpc/README.md) — client, server, TLS, and Unix APIs
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — layering
- [docs/CODEGEN.md](docs/CODEGEN.md) — generated types and stubs
- [docs/PACKAGING.md](docs/PACKAGING.md) — repos, deps, and conda recipes
- [docs/ROADMAP.md](docs/ROADMAP.md) — remaining work
- [docs/PRIMITIVES.md](docs/PRIMITIVES.md) — Mojo stdlib gaps
- [docs/TESTING.md](docs/TESTING.md) — verification layers and benchmarks
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — grpc-mojo vs grpcio vs tonic
- [CONTRIBUTING.md](CONTRIBUTING.md)

## Trademarks

Independent community project. Not affiliated with or endorsed by
[gRPC](https://grpc.io) (Linux Foundation / CNCF) or
[Modular](https://www.modular.com). See [NOTICE](NOTICE).

## License

Apache-2.0.
