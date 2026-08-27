# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""Fixed-iteration gRPC timings for comparison against grpcio and tonic.

Forks a child server (Mojo 1.0 has no threads) and reports mean and p99
nanoseconds for unary 11B, unary 64KiB, and 20-message bidi ping-pong.
Prints one JSON object on stdout. Pass `--smoke` for 5 iterations or
`--iters=N` for a custom count (`--smoke` wins).
"""

from std.ffi import c_int, external_call
from std.sys import argv
from std.time import monotonic

from echo_messages import EchoRequest, EchoResponse
from grpc import (
    GrpcChannel,
    GrpcTransport,
    Metadata,
    Server,
    ServerCall,
    ServerContext,
)
from h2 import Http2Connection
from net import TCPListener
from proto import encode


def iter_count() raises -> Int:
    var requested = 200
    var smoke = False
    for a in argv():
        if a == "--smoke":
            smoke = True
        elif a.startswith("--iters="):
            requested = Int(String(a[byte=8:]))
            if requested < 1:
                raise Error("bench: --iters must be positive")
    if smoke:
        return 5
    return requested


def percentile_ns(mut samples: List[Int64], p: Int) -> Int64:
    """Returns the ceiling percentile after sorting `samples` in place."""
    var n = len(samples)
    if n == 0:
        return 0
    for i in range(1, n):
        var key = samples[i]
        var j = i - 1
        while j >= 0 and samples[j] > key:
            samples[j + 1] = samples[j]
            j -= 1
        samples[j + 1] = key
    var rank = (p * n + 99) // 100
    if rank < 1:
        rank = 1
    if rank > n:
        rank = n
    return samples[rank - 1]


def mean_ns(samples: List[Int64]) -> Int64:
    var n = len(samples)
    if n == 0:
        return 0
    var total: Int64 = 0
    for sample in samples:
        total += sample
    return total // Int64(n)


def echo_handler(
    req: EchoRequest, mut ctx: ServerContext
) raises -> EchoResponse:
    return EchoResponse(message=req.message)


def chat_handler(mut ctx: ServerContext, mut call: ServerCall) raises:
    while True:
        var msg = call.recv[EchoRequest]()
        if not msg:
            return
        call.send(ctx, EchoResponse(message=msg.value().message))


def time_unary(
    mut channel: GrpcChannel, payload: List[Byte], iters: Int
) raises -> List[Int64]:
    var samples = List[Int64]()
    for _ in range(iters):
        var start = Int64(monotonic())
        var result = channel.unary_bytes(
            "/echo.Echo/Say", Span(payload), Metadata()
        )
        var elapsed = Int64(monotonic()) - start
        if not result.status.is_ok():
            raise Error("unary failed: " + String(result.status))
        samples.append(elapsed)
    return samples^


def time_bidi(mut channel: GrpcChannel, iters: Int) raises -> List[Int64]:
    var samples = List[Int64]()
    for _ in range(iters):
        var start = Int64(monotonic())
        var sid = channel.start_call("/echo.Echo/Chat", Metadata())
        for i in range(20):
            channel.send_msg(sid, EchoRequest(message=String(i)), last=False)
            var resp = channel.recv_msg[EchoResponse](sid)
            if not resp:
                raise Error("stream ended early")
        channel.close_send(sid)
        var result = channel.finish(sid)
        var elapsed = Int64(monotonic()) - start
        if not result.status.is_ok():
            raise Error("bidi failed: " + String(result.status))
        samples.append(elapsed)
    return samples^


def print_shape(name: StringSpan, mut samples: List[Int64], per_op_divisor: Int):
    var mean = mean_ns(samples)
    var p99 = percentile_ns(samples, 99)
    if per_op_divisor > 1:
        mean = mean // Int64(per_op_divisor)
        p99 = p99 // Int64(per_op_divisor)
    print(
        '"',
        name,
        '":{"mean_ns":',
        mean,
        ',"p99_ns":',
        p99,
        "}",
        sep="",
    )


def main() raises:
    var iters = iter_count()
    var listener = TCPListener("127.0.0.1", 0)
    var port = listener.local_port
    var pid = external_call["fork", c_int]()
    if pid == 0:
        var server = Server("127.0.0.1", 0)
        server.register_unary[echo_handler]("/echo.Echo/Say")
        server.register_bidi[chat_handler]("/echo.Echo/Chat")
        try:
            var tcp = listener.accept()
            var transport = GrpcTransport.plaintext(tcp^)
            var conn = Http2Connection(transport^, is_client=False)
            var handled = List[UInt32]()
            while True:
                conn.process_next_frame()
                _ = server.dispatch_ready(conn, handled)
        except:
            pass
        external_call["_exit", NoneType](c_int(0))

    listener.close()
    var channel = GrpcChannel.connect("127.0.0.1", port)
    var small = encode(EchoRequest(message="hello bench"))
    var big = encode(EchoRequest(message=String("x") * 65536))

    _ = time_unary(channel, small, 2)
    var unary_small = time_unary(channel, small, iters)
    var unary_big = time_unary(channel, big, iters)
    var bidi = time_bidi(channel, iters)

    print('{"impl":"grpc-mojo","iters":', iters, ",", sep="")
    print_shape("unary_11b", unary_small, 1)
    print(",")
    print_shape("unary_64kib", unary_big, 1)
    print(",")
    print_shape("bidi_x20", bidi, 20)
    print("}")

    channel.close()
    _ = external_call["kill", c_int](pid, c_int(9))
