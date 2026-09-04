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

## Remaining work

Shipped items stay in the tracks below as historical record. This section
is the live list.

### Blocked on Mojo 1.0

- **Blocking `Server` keepalive PINGs.** Arming `SO_RCVTIMEO` around
  `read_exact` would drop a partial HTTP/2 frame on timeout and desync
  the parser. Keepalive stays on the Poller path
  (`PollingServerConfig.keepalive_interval_ns`).
- **gRPC gzip.** Published `mojo-zlib` 0.1.7 depends on nightly Mojo, not
  `mojo >=1.0.0,<2`. Compressed messages are rejected, not mis-decoded,
  until a 1.0-compatible codec exists (or the community package retargets
  stable).
- **Concurrent handlers.** No `std.thread` and no public async I/O
  runtime. `PollingServer` overlaps connection I/O; handler calls stay
  serial. Full-duplex bidi firehose stays recv-driven ping-pong.
- **`std.net` RFC, `to_be_bytes`, unpadded base64, integer cast-fold.**
  These are Modular stdlib / compiler items (Track D), not local work.

### Sibling packages

- [mojo-tls](https://github.com/nsalerni/mojo-tls): TLS session resumption
- [protomojo](https://github.com/nsalerni/protomojo): proto2, editions, and
  text format remain out of scope unless a consumer needs them

### Process / access (not code)

- Publishing conda packages to modular-community needs tokens and a
  human publish step.
- Protecting `main` with a branch ruleset is a GitHub org setting.
- GitHub About fields (description, homepage, topics) are repository
  settings, not files in the tree.

---

## Track A — gRPC completeness

### A1. Streaming RPCs *(done)*
The channel layer frames multiple messages per stream. Server dispatch,
codegen for all four method kinds, and official streaming interop are
shipped. Typed client call objects (`ServerStreamingCall`,
`ClientStreamingCall`, `BidiStreamingCall`) wrap `start_call` /
`send_msg` / `recv_msg` / `finish`; generated stubs return those
handles. Full-duplex bidi remains recv-driven ping-pong until threads
exist — documented, not silent.

*Depends on: nothing. Unblocks: B2 completion, real-world usefulness.*

### A2. Deadline enforcement + cancellation *(done)*
Shipped. `net` maps socket timeouts to a typed error. Clients arm
`SO_RCVTIMEO` from `grpc-timeout`, send `RST_STREAM(CANCEL)` on expiry,
and surface `DEADLINE_EXCEEDED`. Servers check the decoded deadline
before and after the handler and treat a client RST_STREAM as
cancellation. `GrpcChannel.finish` and `cancel` clear the socket
timeout so a later call on the same channel is not bound by the
previous deadline.

### A3. Robustness against malformed and malicious peers *(done)*
Shipped. The proto decoder caps nesting at 100, matching reference
implementations. Unknown fields are stored and re-emitted on encode.
mojo-http2 applies rapid-reset, PING/SETTINGS flood, concurrent-stream,
and header-list-size limits with `ENHANCE_YOUR_CALM`. Stream receive
windows replenish when the application consumes bytes via `take_data`;
the connection window still replenishes when DATA arrives.

### A4. Codegen semantics *(done)*
Shipped. One generated module per `.proto` file, proto3 `optional` as
`Optional[T]`, typed enum wrappers (protomojo 0.4.0), and
`add_<service>_service[...]` server registration next to the client stub.

### A5. Protocol niceties *(S–M)*
Shipped: `grpc-status-details-bin` on client extract and server send; max
message size on `Server`, `PollingServer`, and `GrpcChannel`;
`PollingServer` keepalive PINGs; configurable HTTP/2 initial window on
`GrpcChannel`, `Server`, and `PollingServerConfig`.

Still open, and blocked as listed above: blocking-`Server` keepalive PING.

---

## Track B — Verification to the industry standard

The compliance suite is ours; these are the suites the rest of the world
recognizes. "100% compatible" is claimable when B1–B4 are green.

### B1. GitHub remote + CI *(done)*
CI runs `pixi run test` and `pixi run compliance` on `{macos-14,
ubuntu-24.04}` with pixi caching. The compliance report is generated and
badged from that run.

### B2. Official gRPC interop test suite *(done)*
The 12 canonical cases run in both roles over h2c, TLS, and Unix sockets
against grpcio (`pixi run interop-official`): 72/72.

### B3. h2spec *(done)*
[h2spec](https://github.com/summerwind/h2spec) is green: 146/146.

### B4. protobuf conformance runner *(done)*
Google's `conformance_test_runner` is green for proto3 binary and JSON:
1476/1476. proto2 cases are skipped on purpose.

### B5. Fuzzing + benchmarks *(done)*
Broader HTTP/2 framing and HPACK random cases live in mojo-http2.
Map / oneof / Any / proto3 JSON fuzz lives in protomojo. This repo
consumes both through `fetch_deps`. Comparative loopback benches vs
grpcio and tonic (unary 11B, unary 64KiB, bidi ping-pong, mean and p99)
are in `bench/compare.py` and [BENCHMARKS.md](BENCHMARKS.md). Tonic is
included when `cargo` and `protoc` are available. Do not git-diff
nanoseconds.

---

## Track C — Standalone community packages

**Done as GitHub repositories.** The four packages live in sibling repos;
grpc-mojo is the integration umbrella. `packages/` is a gitignored
`fetch_deps.py` checkout, not the source of truth. Repo topology and conda
mechanics are in [PACKAGING.md](PACKAGING.md). Remaining Track C work is
publishing conda packages to modular-community (tokens / a human step)
and a mojo-threads RFC before any thread package.

| Package | Repository | Remaining | Community value |
|---|---|---|---|
| **protomojo** (+ `protoc-gen-mojo`) | [protomojo](https://github.com/nsalerni/protomojo) | proto2, editions, and text format out of scope | Protobuf for Mojo — useful far beyond gRPC. Flagship. |
| **mojo-http2** (`hpack` + `h2`) | [mojo-http2](https://github.com/nsalerni/mojo-http2) | RFC 9218 stream priority if a consumer needs it | HPACK and HTTP/2 for Mojo servers/clients generally |
| **mojo-net** | [mojo-net](https://github.com/nsalerni/mojo-net) | none for the current socket/DNS/poller scope | Ends per-project libc socket bindings |
| **mojo-tls** | [mojo-tls](https://github.com/nsalerni/mojo-tls) | TLS session resumption | TLS 1.2/1.3 with strict X.509, SNI, and ALPN |
| **mojo-zlib** | [community package](https://github.com/gabrieldemarmiesse/mojo-zlib) | 1.0-compatible retarget, then gRPC gzip | Enables pending `grpc-encoding: gzip` |
| **mojo-threads** | not started | RFC first (see D4) | pthread_create/mutex/condvar via `abi("C")` trampolines; unblocks concurrent serving |

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
| **1 — Foundation** | B1 CI+remote · A3 depth limit · D1 bug report · D2 stdlib PRs · C: extract protomojo + mojo-hpack | CI on macOS **and Linux**; Mojo issue filed; 2 stdlib PRs open; 2 packages on modular-community | CI+remote ✅ · depth limit ✅ · packages extracted ✅ · bug report / stdlib PRs / conda publish pending |
| **2. Protocol completeness** | A1 streaming · A2 deadlines/cancellation · B2 official interop · B3 h2spec · A3 remaining guards | Official unary+streaming interop green vs grpcio; h2spec clean | ✅ streaming (including typed client call objects) · ✅ deadlines/cancel · ✅ interop 72/72 across h2c, TLS, and Unix sockets · ✅ h2spec 146/146 · ✅ flood guards, unknown-field preservation, and proto depth limit |
| **3: Ecosystem primitives** | C mojo-net (DNS/IPv6/timeouts) + publish; D3 std.net RFC; integrate mojo-zlib + gRPC compression; A4 codegen imports/presence; B4 conformance | `std.net` RFC posted; gzip interop; protobuf conformance green | ✅ net prereqs (DNS/IPv6/UDP/timeouts); ✅ A4 (imports, optional, unknown fields, typed enums, service registration); ✅ conformance 1476/1476 for proto3 binary and JSON; gzip blocked on a 1.0-compatible zlib package |
| **4. Concurrency & TLS** | D4 threads RFC → C mojo-threads → concurrent server · C mojo-tls (ALPN h2) · A5 · B5 benchmarks | Concurrent connections; TLS interop; published benchmarks | TLS interop ✅ · bounded h2c and TLS polling ✅ · A5 max-message + PollingServer keepalive ✅ · B5 published loopback benches vs grpcio/tonic ✅ · PollingServer GOAWAY drain ✅ · PollingServer streaming (blocking on the poll thread) ✅ · parallel handlers and blocking-server keepalive blocked on Mojo threads/async |

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
