# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub security advisories](https://github.com/nsalerni/grpc-mojo/security/advisories/new)
rather than public issues. You should receive a response within a week.

## Scope notes

grpc-mojo currently supports plaintext HTTP/2 (h2c) only — it is not yet
suitable for exposure to untrusted networks. The HTTP/2 layer implements the
standard abuse mitigations (rapid-reset accounting, PING/SETTINGS flood
limits, concurrency and header-size caps, flow-control backpressure), and
the protobuf decoder enforces the reference nesting-depth limit, but the
project has not had an external security review. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the TLS plan.
