# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub security advisories](https://github.com/nsalerni/grpc-mojo/security/advisories/new)
rather than public issues. You should receive a response within a week.

## Scope

grpc-mojo accepts untrusted HTTP/2 and protobuf input on h2c, TLS, and Unix
sockets. TLS clients verify the certificate chain and hostname by default.
The HTTP/2 layer applies flood and concurrency limits; the protobuf decoder
enforces the reference nesting-depth limit.

Certificate presence is not an authorization decision. Check
`PeerCertificate.verified`, then apply the service's identity policy.

The project has not had an external security review. Remaining work is in
[docs/ROADMAP.md](docs/ROADMAP.md).

## Residual risks

- HTTP/2 `SETTINGS` ACK timeout is not armed here; the connection is
  caller-driven. Apply an application timer if a peer that never ACKs
  SETTINGS should be torn down.
- Health Watch, gzip, and concurrent handlers stay unimplemented; they
  are blocked on Mojo threads or a 1.0-compatible zlib, not silent skips
  on the wire (Watch is UNIMPLEMENTED, compressed messages are rejected).
- Unix sockets are plaintext. Use TLS when the path is not already a
  trust boundary.
