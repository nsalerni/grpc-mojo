# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Pure-Mojo gRPC over HTTP/2, with h2c and TLS transports.

Implements the gRPC wire protocol per
[PROTOCOL-HTTP2.md](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
length-prefixed message framing, `grpc-timeout` deadlines, custom metadata
(including base64-coded `-bin` binary metadata), percent-encoded status
messages, and the `grpc-status-details-bin` rich error model. All four RPC
kinds are supported: unary, server streaming, client streaming, and
bidirectional streaming. Conformance is verified against reference
implementations: the official gRPC interop suite passes 48/48 cases against
grpcio across both roles and both transports, the h2 layer passes h2spec
146/146, and the proto layer passes Google protobuf conformance 698/698.

The package depends on the `h2`, `proto`, `hpack`, `net`, and `tls` packages
(see docs/ARCHITECTURE.md); dependency edges point down only.

A minimal unary server (see examples/echo_server.mojo for all four kinds;
`EchoRequest`/`EchoResponse` are `tools/protoc-gen-mojo` output):

```mojo
from grpc import Server, ServerContext

def say(req: EchoRequest, mut ctx: ServerContext) raises -> EchoResponse:
    var resp = EchoResponse()
    resp.message = String("echo: ") + req.message
    return resp^

def main() raises:
    var server = Server("127.0.0.1", 50051)
    server.register_unary[say]("/echo.Echo/Say")
    server.serve()
```

And the matching client call (see examples/echo_client.mojo):

```mojo
from grpc import GrpcChannel

def main() raises:
    var channel = GrpcChannel.connect("127.0.0.1", 50051)
    var req = EchoRequest()
    req.message = "hello from mojo"
    var resp = channel.unary[EchoRequest, EchoResponse](
        "/echo.Echo/Say", req, timeout_ns=10_000_000_000
    )
    print("response:", resp.message)
    channel.close()
```

Streaming calls use `GrpcChannel.start_call`, `send_msg`, `recv_msg`,
`close_send`, and `finish` on the client, and the `register_*` methods plus
`ServerCall` on the server.
"""

from .client import CallResult, GrpcChannel, GRPC_MOJO_USER_AGENT
from .framing import (
    DEFAULT_MAX_RECV_MESSAGE_SIZE,
    GRPC_MESSAGE_PREFIX_LEN,
    frame_message,
    recv_message,
    send_message,
)
from .metadata import (
    Metadata,
    decode_bin_value,
    encode_bin_value,
    is_binary_key,
    is_valid_metadata_key,
)
from .server import (
    MethodKind,
    RawHandler,
    Server,
    ServerCall,
    ServerContext,
    UnaryBytesHandler,
)
from .status import (
    Status,
    StatusCode,
    http_status_to_grpc,
    percent_decode_message,
    percent_encode_message,
    rst_code_to_grpc,
    status_code_name,
)
from .timeout import decode_timeout, encode_timeout
from .transport import GrpcTransport
