# grpc-mojo Roadmap

The execution plan for closing the gaps in [COMPLIANCE.md](COMPLIANCE.md)'s
"known gaps" list and — the larger goal — turning the primitives this project
had to build into **standard functionality for the Mojo community**:
standalone packages on the community channel and contributions into
[modular/modular](https://github.com/modular/modular) itself.
[PRIMITIVES.md](PRIMITIVES.md) is the gap catalog; this document is the
sequenced plan.

Four tracks, interleaved across four phases. Track C/D items are the point,
not a side effect: grpc-mojo is the *proof-of-need and reference user* for
each primitive it upstreams.

---

## Track A — gRPC completeness

### A1. Streaming RPCs *(L)*
The largest missing protocol surface. The channel layer already frames
multiple messages per stream; what's missing is API + dispatch + codegen.

1. **Client API**: promote `start_call` / `send_request_bytes` /
   `recv_response_bytes` / `finish` into typed call objects —
   `ServerStreamingCall[Req, Resp]`, `ClientStreamingCall`, `BidiCall` —
   with `send(msg)`, `close_send()`, `recv() -> Optional[Resp]`.
2. **Server dispatch**: replace the "wait for END_STREAM, then invoke"
   model with handler kinds. Server-streaming and client-streaming work
   fine under blocking I/O (half-duplex message flow); full-duplex bidi is
   *sequenced* (recv-driven) until Track C6 lands threads — documented,
   not silent.
3. **Codegen**: `protoc-gen-mojo` emits stubs for all four method kinds
   (currently skips streaming with a comment).
4. **Verification**: the official gRPC interop cases `ping_pong`,
   `client_streaming`, `server_streaming` against grpcio (see B2).

*Depends on: nothing. Unblocks: B2 completion, real-world usefulness.*

### A2. Deadline enforcement + cancellation *(M)*
We propagate `grpc-timeout` correctly but nobody acts on it.

1. `net`: add `set_read_timeout(ns)` / `set_write_timeout(ns)` via
   `SO_RCVTIMEO`/`SO_SNDTIMEO`; map `EAGAIN` to a typed timeout error.
   (This lands in mojo-net → C1.)
2. Client: arm the socket timeout from the call deadline; on expiry send
   `RST_STREAM(CANCEL)` and surface `DEADLINE_EXCEEDED`. Add
   `cancel(call)` for user-initiated cancellation.
3. Server: check the decoded deadline before invoking and after returning;
   respond `DEADLINE_EXCEEDED` when blown; observe client RST_STREAM as
   cancellation.

### A3. Robustness against malformed and malicious peers *(M)*
- **Recursion-depth limit in the proto decoder** (default 100, like
  reference implementations) — the one item that is a bug today: deeply
  nested input can blow the stack. *Do first.*
- Bound per-stream buffering: stop auto-replenishing flow-control windows
  when the application isn't consuming (backpressure instead of unbounded
  memory).
- HTTP/2 abuse guards: rapid-reset (CVE-2023-44487) accounting,
  PING/SETTINGS flood limits, `MAX_CONCURRENT_STREAMS` enforcement,
  header-list-size limit (8 KiB default per spec) → `ENHANCE_YOUR_CALM` /
  GOAWAY.
- Unknown-field **preservation** (not just skipping): store raw unknown
  bytes on messages and re-emit on encode, per proto3 spec; required for
  any proxy-shaped use.

### A4. Codegen semantics *(M)*
- **Cross-file `.proto` imports** — one generated module per file,
  Mojo imports mirroring proto imports; unblocks well-known types
  (`Timestamp`, `Duration`, …), then ships helpers for them.
- proto3 `optional` → real presence via `Optional[T]` fields.
- Typed enums (struct wrapper with `Equatable`, name lookup) instead of
  bare `Int32`.
- Server-side registration codegen: `register_echo_service[handlers...]`
  companion to the client stub.

### A5. Protocol niceties *(S–M)*
`grpc-status-details-bin` (google.rpc.Status rich errors), graceful GOAWAY
drain on shutdown, keepalive PING management, configurable max message
sizes and initial window.

---

## Track B — Verification to the industry standard

The compliance suite is ours; these are the suites the rest of the world
recognizes. "100% compatible" is claimable when B1–B4 are green.

### B1. GitHub remote + CI *(S, do first)*
Push the repo; GitHub Actions matrix `{macos-14, ubuntu-24.04}` running
`pixi run test` and `pixi run compliance` with pixi caching. **Linux has
never executed** — the sockaddr/errno paths are written but unproven;
expect a short fix cycle. Badge the compliance score from the generated
report.

### B2. Official gRPC interop test suite *(M)*
Implement the canonical named cases (`empty_unary`, `large_unary`,
`custom_metadata`, `status_code_and_message`, `special_status_message`,
`unimplemented_method`, then the streaming set with A1) as
`interop_client.mojo` / `interop_server.mojo`, run against grpcio in CI:
`pixi run interop-official`.

### B3. h2spec *(S)*
Run [h2spec](https://github.com/summerwind/h2spec) (strict RFC 9113
conformance tool) against our server in CI. Will find strictness gaps our
happy-path hyper-h2 test can't (pseudo-header validation, stream-id
monotonicity, required error codes); feed fixes back into `h2`.

### B4. protobuf conformance runner *(M)*
Wire `proto` + codegen into Google's official
`conformance_test_runner` via a small Mojo conformance binary. The gold
standard for the wire format; supersedes our randomized differential as
the outer gate (keep ours — it runs in seconds).

### B5. Fuzzing + benchmarks *(M, ongoing)*
Structured differential fuzzers (Python-driven, seeded — extend the
existing compliance harness) for varint/frame/HPACK decoders; benchmark
suite vs grpcio and tonic (unary QPS, streaming throughput, p99) using
`std.benchmark`.

---

## Track C — Standalone community packages

Extraction strategy: **the monorepo stays the source of truth** while APIs
move fast; packages split out via `git subtree split` at first publish to
the **modular-community channel (prefix.dev)**. Repo topology, dependency
mechanics (conda ranges for releases, pixi-build source deps for
development), and the pre-split checklist are decided in
[PACKAGING.md](PACKAGING.md): three new repos (`mojo-net`, `protomojo`,
`mojo-http2` publishing both `mojo-hpack` and `mojo-h2`), with grpc-mojo
remaining the integration umbrella. Apache-2.0, semver from 0.x, each repo
carrying its extracted test subset.

| Package | Source | Before publishing | Community value |
|---|---|---|---|
| **protomojo** (+ `protoc-gen-mojo`) | `src/proto`, `tools/` | A3 depth limit, A4 imports | Protobuf for Mojo — useful far beyond gRPC. Flagship. |
| **mojo-hpack** | `src/hpack` | none — RFC-complete today | Any HTTP/2 work needs it |
| **mojo-net** | `src/net` | DNS (`getaddrinfo`), IPv6, UDP, timeouts (A2.1) | Ends per-project libc socket bindings (lightbug et al.) |
| **mojo-h2** | `src/h2` | A3 guards, B3 clean | HTTP/2 for Mojo servers/clients generally |
| **mojo-zlib** | [community package](https://github.com/gabrieldemarmiesse/mojo-zlib) | integrate its zlib bindings with grpc-mojo | Enables pending gRPC gzip support; general compression |
| **mojo-threads** | new | RFC first (see D4) | pthread_create/mutex/condvar via `abi("C")` trampolines; unblocks concurrent serving |
| **mojo-tls** | `packages/mojo-tls` | shipped with strict X.509, SNI, and ALPN in both roles | TLS for the whole ecosystem |

mojo-threads is deliberately RFC-before-code: thread-safety guarantees
interact with Mojo's ownership model (Send/Sync-like semantics), and a
package that gets this wrong will fragment the ecosystem. Prototype in a
branch, propose on the forum, publish after Modular engagement.

---

## Track D — Contributions into modular/modular

Ordered by leverage-per-effort:

1. **File the integer cast-fold bug** *(S, do first)* — composed casts
   (`UInt64(f(x))` where `f` returns `UInt32` derived from `Int32`)
   sign-extend where the direct conversion zero-extends. We have a
   minimal repro and a real-world consequence (10-byte protobuf varints)
   caught only by differential testing. Whether compiler bug or intended
   semantics, it deserves an issue + docs; possibly a stdlib lint.
2. **Small stdlib PRs** *(S each)*:
   - `to_be_bytes` / `from_be_bytes`-style int↔bytes helpers
     (PRIMITIVES #5) — motivated by three call sites in this repo.
   - `b64encode[padded=False]` + lenient decode (PRIMITIVES #6) —
     motivated by gRPC `-bin` metadata.
3. **`std.net` RFC** *(M)* — forum design thread with mojo-net as the
   working prototype; propose the minimal core (addresses incl. DNS,
   `TCPListener`/`TCPStream`, typed errno errors) and offer the PR.
4. **Threads/async RFC participation** *(M, ongoing)* — mojo-threads
   design thread; a kqueue/epoll `mojo-reactor` prototype as evidence for
   the async I/O discussion. Align with Modular's coroutine plans rather
   than forking; `std.runtime._asyncrt` is explicitly unstable.
5. **`std.compress` proposal** *(S)*: informed by mojo-zlib integration.
6. **Developer-experience feedback** *(S)* — write up what building a
   protocol stack on day-one Mojo 1.0 surfaced: `mojo test` removal and
   the executable-tests pattern, comptime-`Array` materialization
   ergonomics, `Dict` iteration requiring `Copyable` values, FFI socket
   cookbook for the docs.

---

## Phases

| Phase | Items | Exit criteria | Status |
|---|---|---|---|
| **1 — Foundation** | B1 CI+remote · A3 depth limit · D1 bug report · D2 stdlib PRs · C: extract protomojo + mojo-hpack | CI on macOS **and Linux**; Mojo issue filed; 2 stdlib PRs open; 2 packages on modular-community | CI+remote ✅ · depth limit ✅ · bug report / PRs / extraction pending |
| **2. Protocol completeness** | A1 streaming · A2 deadlines/cancellation · B2 official interop · B3 h2spec · A3 remaining guards | Official unary+streaming interop green vs grpcio; h2spec clean | ✅ streaming · ✅ deadlines/cancel · ✅ interop 48/48 across h2c and TLS · ✅ h2spec 146/146 · flood guards pending |
| **3: Ecosystem primitives** | C mojo-net (DNS/IPv6/timeouts) + publish; D3 std.net RFC; integrate mojo-zlib + gRPC compression; A4 codegen imports/presence; B4 conformance | `std.net` RFC posted; gzip interop; protobuf conformance green | ✅ net prereqs (DNS/IPv6/UDP/timeouts); ✅ A4 (imports, optional, unknown fields); ✅ conformance 698/698; mojo-zlib available; gzip integration + RFC pending |
| **4. Concurrency & TLS** | D4 threads RFC → C mojo-threads → concurrent server · C mojo-tls (ALPN h2) · A5 · B5 benchmarks | Concurrent connections; TLS interop; published benchmarks | TLS interop ✅ · bounded unary h2c polling ✅ · parallel handlers pending |

**Definition of "100% compatible", concretely:** official gRPC interop
suite green in both roles against grpcio, h2spec clean, protobuf
conformance green, on macOS and Linux in CI — not our own suite's word
for it.

## Risks

- **Mojo 1.x churn**: 1.0 just shipped; nightly already removes 1.0
  aliases. CI pins stable; a scheduled nightly job gives early warning.
- **Threads blocked on Modular**: if the safety RFC stalls, Phase 4
  degrades to "concurrent via multiple processes" and bidi stays
  sequenced — everything else proceeds.
- **Ecosystem duplication**: lightbug/community may have competing net
  APIs; the RFC thread is the coordination point — merge efforts rather
  than compete.
