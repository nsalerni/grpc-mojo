# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026 the grpc-mojo contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# ===----------------------------------------------------------------------=== #

"""End-to-end gRPC benchmarks: a real channel against a forked server.

The server runs in a forked child process (Mojo 1.0 has no threads —
PRIMITIVES.md item 7), so these numbers include the full stack on both
sides: proto coding, gRPC framing, HPACK, HTTP/2, and loopback TCP.
Run: `pixi run bench`. Pass `--smoke` for a milliseconds-long CI run.
"""

from std.benchmark import Unit, run
from std.ffi import c_int, external_call
from std.sys import argv

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
from proto import decode, encode


def is_smoke() -> Bool:
    for a in argv():
        if a == "--smoke":
            return True
    return False


def bench_time() -> Float64:
    return 0.005 if is_smoke() else 0.5


def run_capped[F: def() raises](f: F, secs: Float64) raises -> Float64:
    """Runs a benchmark bounded by `secs` and returns the mean in ns."""
    var report = run(f, min_runtime_secs=secs, max_runtime_secs=secs * 3)
    return report.mean(Unit.ns)


def report_line(name: StringSpan, ns_per_op: Float64, bytes_per_op: Int):
    var rps = 0.0
    var mib_s = 0.0
    if ns_per_op > 0:
        rps = 1e9 / ns_per_op
        mib_s = (Float64(bytes_per_op) / (1024 * 1024)) / (ns_per_op / 1e9)
    print(
        String(name),
        ": ",
        Int(ns_per_op),
        " ns/op",
        ", ",
        Int(rps),
        " ops/s",
        ", ",
        Int(mib_s),
        " MiB/s",
        sep="",
    )


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


def main() raises:
    var secs = bench_time()

    var listener = TCPListener("127.0.0.1", 0)
    var port = listener.local_port
    var pid = external_call["fork", c_int]()
    if pid == 0:
        # Child: serve the parent's connection until it closes.
        var server = Server("127.0.0.1", 0)
        server.register_unary[echo_handler]("/bench.Echo/Say")
        server.register_bidi[chat_handler]("/bench.Echo/Chat")
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

    # --- unary latency: 11-byte message ---
    var small = encode(EchoRequest(message="hello bench"))
    var small_size = len(small)

    def unary_small() raises {mut channel, small}:
        var r = channel.unary_bytes("/bench.Echo/Say", Span(small), Metadata())
        if not r.status.is_ok():
            raise Error("call failed: " + String(r.status))

    var r = run_capped(unary_small, secs)
    report_line("unary echo 11B", r, small_size * 2)

    # --- unary throughput: 64 KiB message each way ---
    var big_text = String("x") * 65536
    var big = encode(EchoRequest(message=big_text))
    var big_size = len(big)

    def unary_big() raises {mut channel, big}:
        var res = channel.unary_bytes("/bench.Echo/Say", Span(big), Metadata())
        if not res.status.is_ok():
            raise Error("call failed: " + String(res.status))

    r = run_capped(unary_big, secs)
    report_line("unary echo 64KiB", r, big_size * 2)

    # --- bidi streaming: 20-message ping-pong on one stream per op ---
    def bidi_pingpong() raises {mut channel}:
        var sid = channel.start_call("/bench.Echo/Chat", Metadata())
        for i in range(20):
            channel.send_msg(sid, EchoRequest(message=String(i)), last=False)
            var resp = channel.recv_msg[EchoResponse](sid)
            if not resp:
                raise Error("stream ended early")
        channel.close_send(sid)
        var result = channel.finish(sid)
        if not result.status.is_ok():
            raise Error("stream failed: " + String(result.status))

    r = run_capped(bidi_pingpong, secs)
    report_line("bidi ping-pong x20 (per message)", r / 20, small_size * 2)

    channel.close()
    _ = external_call["kill", c_int](pid, c_int(9))
    print("bench_grpc: done")
