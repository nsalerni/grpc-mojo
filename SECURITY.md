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
