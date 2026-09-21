# Getting started

A unary gRPC call over TLS in Mojo 1.0, without cloning five repositories.

## 1. Create a project

```sh
pixi init hello-grpc && cd hello-grpc
```

In `pixi.toml`:

```toml
[workspace]
channels = [
    "https://conda.modular.com/max",
    "https://repo.prefix.dev/modular-community",
    "conda-forge",
]
platforms = ["osx-arm64", "linux-64", "linux-aarch64"]
```

```sh
pixi add grpc-mojo python
pixi add --pypi grpcio-tools protobuf
```

`pixi add grpc-mojo` installs the whole stack (`mojo-net`, `mojo-tls`,
`mojo-http2`, `protomojo`) and puts `protoc-gen-mojo` on `PATH`.

## 2. Write a `.proto`

`echo.proto`:

```proto
syntax = "proto3";
package echo;

service Echo {
  rpc Say(EchoRequest) returns (EchoResponse);
}

message EchoRequest { string message = 1; }
message EchoResponse { string message = 1; }
```

```sh
python3 -m grpc_tools.protoc -I. \
  --plugin=protoc-gen-mojo=$(command -v protoc-gen-mojo) \
  --mojo_out=. echo.proto
```

That emits `echo_pb.mojo`.

## 3. Server

```mojo
from echo_pb import ECHO_SAY_PATH, EchoRequest, EchoResponse, add_echo_service
from grpc import Server, ServerContext

def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("echo: ") + req.message
    return resp^

def main() raises:
    var server = Server("127.0.0.1", 50051)
    add_echo_service[say](server)
    server.serve()
```

For TLS, use `Server.tls(...)` with a certificate chain and key. See
[src/grpc/README.md](../src/grpc/README.md).

## 4. Client

```mojo
from echo_pb import EchoClient, EchoRequest

def main() raises:
    var client = EchoClient.connect("127.0.0.1", 50051)
    var req = EchoRequest()
    req.message = "hello"
    print(client.say(req, timeout_ns=10_000_000_000).message)
```

TLS client: `EchoClient.connect_tls("localhost", 50051, ca_file=...)`.

## Concurrency

`Server` is one connection at a time. `PollingServer` overlaps I/O on one
thread but **handlers stay serial**. A streaming handler on `PollingServer`
stalls every other connection until it returns. Scale-out is several
processes behind a load balancer, not an in-process thread pool.

## Next

- TCP / UDP / Unix without gRPC: [mojo-net](https://github.com/nsalerni/mojo-net)
- TLS only: [mojo-tls](https://github.com/nsalerni/mojo-tls)
- HTTP/2 only: [mojo-http2](https://github.com/nsalerni/mojo-http2)
- Protobuf without gRPC: [protomojo](https://github.com/nsalerni/protomojo)
- Verified numbers: [COMPLIANCE.md](COMPLIANCE.md)
