# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub security advisories](https://github.com/nsalerni/grpc-mojo/security/advisories/new)
rather than public issues. You should receive a response within a week.

## Scope notes

grpc-mojo supports h2c, TLS, and Unix domain sockets. TLS clients verify the
certificate chain and hostname by default. They can load a client certificate
chain and private key from files for a server that requires mutual TLS. TLS
servers use one configured certificate chain and require ALPN negotiation of
the `h2` protocol. Both server implementations can require a client
certificate that chains to a configured client CA before they accept HTTP/2
requests.

The HTTP/2 layer limits rapid resets, PING and SETTINGS floods, concurrent
streams, header sizes, and buffered output. The protobuf decoder enforces the
reference nesting-depth limit. Neither server selects certificates by SNI.
The project has not had an external security review. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the remaining work.
