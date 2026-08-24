# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub security advisories](https://github.com/nsalerni/grpc-mojo/security/advisories/new)
rather than public issues. You should receive a response within a week.

## Scope notes

grpc-mojo supports h2c, TLS, and Unix domain sockets. TLS clients verify the
certificate chain and hostname by default. TLS servers use one configured
certificate chain and require ALPN negotiation of the `h2` protocol.

The HTTP/2 layer limits rapid resets, PING and SETTINGS floods, concurrent
streams, header sizes, and buffered output. The protobuf decoder enforces the
reference nesting-depth limit. Client certificate authentication and
SNI-based certificate selection are not yet supported. The project has not
had an external security review. See [docs/ROADMAP.md](docs/ROADMAP.md) for
the remaining work.
