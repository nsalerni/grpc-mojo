# protoc-gen-mojo — Code Generation Reference

`tools/protoc-gen-mojo` is a standard `protoc` plugin that turns `.proto`
files into Mojo message structs and gRPC stubs. It is validated by the same
gates as the runtime: the generated code passes Google's protobuf conformance
suite (1476/1476 proto3 binary and JSON tests) and drives the official gRPC
interop cases against `grpcio`.

## Invocation

```sh
pixi run python3 -m grpc_tools.protoc \
  -I path/to/protos \
  --plugin=protoc-gen-mojo=tools/protoc-gen-mojo \
  --mojo_out=OUT_DIR \
  your.proto
```

One Mojo module is emitted per `.proto` file in the request — including
transitively imported dependencies (well-known types included) — named after
the file's basename: `foo/bar/baz.proto` → `baz_pb.mojo`. Compile with the
output directory on the include path (`-I OUT_DIR`).

## Type mapping

| proto3 | Mojo | Wire notes |
|---|---|---|
| `int32` / `int64` | `Int32` / `Int64` | negative `int32` sign-extends to 10 bytes |
| `uint32` / `uint64` | `UInt32` / `UInt64` | |
| `sint32` / `sint64` | `Int32` / `Int64` | ZigZag varint |
| `fixed32` / `fixed64` | `UInt32` / `UInt64` | little-endian |
| `sfixed32` / `sfixed64` | `Int32` / `Int64` | little-endian |
| `float` / `double` | `Float32` / `Float64` | |
| `bool` | `Bool` | |
| `string` | `String` | UTF-8 validated on decode |
| `bytes` | `List[Byte]` | |
| `enum E` | `Int32` + a struct of `comptime` constants | open-enum semantics |
| message `M` (singular) | `Optional[M]` | presence tracked |
| `optional` scalar (proto3) | `Optional[T]` | explicit presence: set values encode even at default |
| `repeated` scalar | `List[T]` | packed by default; `[packed = false]` honored; decoder accepts both |
| `repeated` message/string/bytes | `List[T]` | |
| `map<K, V>` | `Dict[K, V]` | entry order not guaranteed (like every implementation) |
| `oneof o { ... }` | member fields + `var o_case: Int` | `0` = unset, else the set field number; message members merge on repeated occurrence |

## Semantics the generated code guarantees

- **Merge rules** (`merge_from`): later singular values overwrite, repeated
  fields append, submessages merge field-wise, oneof message members merge
  when the same member repeats.
- **Unknown fields are preserved**: captured on decode into `_unknown` and
  re-emitted after known fields on encode, per the proto3 spec.
- **Nesting depth** is limited to 100 (matching reference implementations);
  deeper input raises rather than exhausting the stack.
- **Recursive messages** work: a singular self-referential field is boxed as
  a 0-or-1 `List[T]` (an `Optional` would make the struct infinitely sized),
  and messages participating in any reference cycle get an explicit
  `__deinit__` and copy-initializer because Mojo 1.0's trait synthesis
  cannot close cycles. Presence check for a boxed field is
  `len(msg.field) != 0`.
- **Wire-type discipline**: a known field number carrying the wrong wire
  type is treated as an unknown field, like reference parsers.

## Services

For each service the plugin emits:

- `comptime <SERVICE>_<METHOD>_PATH` constants,
- a `<Service>Client` struct wrapping `GrpcChannel` — unary methods make the
  full call; streaming methods return the stream id and document the
  `send_msg` / `recv_msg` / `close_send` / `finish` flow,
- an `add_<service>_service[...](mut server)` helper that registers one
  compile-time handler per method with the right kind
  (`register_unary` / `register_server_streaming` /
  `register_client_streaming` / `register_bidi`).

Handler signatures:

| Method kind | Handler |
|---|---|
| unary | `def h(req: Req, mut ctx: ServerContext) raises -> Resp` |
| server-streaming | `def h(req: Req, mut ctx: ServerContext, mut call: ServerCall) raises` |
| client-streaming | `def h(mut ctx: ServerContext, mut call: ServerCall) raises -> Resp` |
| bidi | `def h(mut ctx: ServerContext, mut call: ServerCall) raises` |

## Current limitations

- proto3 only (`syntax = "proto3"`); proto2 and editions are rejected.
- Generated messages whose complete field graph supports the proto3 JSON
  mapping implement `ProtoJsonMessage`. Text format remains unsupported.
- Well-known types use their standard JSON mappings. Protoc requests that
  contain `google.protobuf.Any` also emit a static resolver for the generated
  message set.
- Field and message names colliding with Mojo keywords get a trailing
  underscore.
