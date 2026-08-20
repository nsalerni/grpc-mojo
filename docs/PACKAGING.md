# Packaging & Repo Topology

How the five packages become independently consumable, and how dependencies
work between them once they do. This refines Track C of
[ROADMAP.md](ROADMAP.md).

## Guiding fact: packages, not repos, are the unit of distribution

Mojo packages ship as conda packages on channels (the community standard is
the [modular-community channel](https://docs.modular.com/max/packages/) on
prefix.dev), built by the official
[`pixi-build-mojo`](https://pixi.prefix.dev/latest/build/backends/pixi-build-mojo/)
backend. One repo can publish several packages, so repo topology is a
maintenance decision while package granularity is an API decision. We keep
five packages regardless of how many repos host them.

## Decision: three new repos, five packages

| Repo | Publishes | Rationale |
|---|---|---|
| `mojo-net` | `mojo-net` | Zero deps; broadest audience; the working prototype behind the `std.net` RFC (PRIMITIVES.md #1) — a standalone repo strengthens the proposal |
| `protomojo` | `protomojo` + `protoc-gen-mojo` | Zero deps; flagship; audience (serialization) and release cadence independent of gRPC |
| `mojo-http2` | `mojo-hpack` **and** `mojo-h2` (two packages, one repo) | hpack's only realistic consumers are HTTP/2 stacks and the two version in lockstep; Python's hyper project split hpack/hyperframe/h2 into separate repos and the maintenance tax is well documented. Two packages from one repo still lets someone depend on `mojo-hpack` alone |
| `grpc-mojo` (this repo) | `grpc-mojo` | Product repo and integration umbrella: the gRPC compliance sections and official gRPC interop stay here; each package repo carries its own differential compliance suite (`packages/<pkg>/compliance/`, including h2spec and protobuf-conformance), which the umbrella runs and aggregates to test the repos together |

Five separate repos was considered and rejected: it quintuples CI/release
surface and fragments the differential-compliance harness for no consumer
benefit that package granularity doesn't already provide.

**Status**: the in-repo split is done — `packages/mojo-net`,
`packages/protomojo`, and `packages/mojo-http2` are self-contained
subfolders (own manifest, tests, README, LICENSE) that mirror the future
repos exactly; `git subtree split` of a `packages/<repo>` directory at
first publish preserves history. `tools/check_extraction.py` proves each
file set is self-sufficient on every compliance run.

## Dependency mechanics

### Released: conda version ranges (default)

Each package declares a `[package]` section and conda-style dependencies:

```toml
# mojo-http2/pixi.toml — the mojo-h2 package
[package]
name = "mojo-h2"
version = "0.1.0"

[package.build]
backend = { name = "pixi-build-mojo", version = "0.*" }

[package.run-dependencies]
mojo-hpack = ">=0.1.0,<0.2"
mojo-net = ">=0.1.0,<0.2"
```

Publishing is a PR adding `recipe.yaml` to the modular-community repo,
which builds and hosts the package. Consumers add the channel and a version
range; the conda solver resolves the graph and `pixi.lock` pins exact
versions. Conda allows only one version of a package per environment —
keep compatibility ranges honest and wide, and follow semver from `0.x`.

### Unreleased / development: pixi source dependencies

Pixi's `pixi-build` preview (`preview = ["pixi-build"]` in the workspace
manifest) supports `path` and `git` source entries, letting grpc-mojo's CI
build against the other repos' `main` before a release:

```toml
[workspace]
preview = ["pixi-build"]

[dependencies]
mojo-hpack = { path = "../mojo-http2" }   # or a git source entry
```

Use this for development and pre-release integration testing only; released
consumers should always see channel packages.

### Fallback: submodule + include path

`git submodule` plus `-I <dep>/src` works with zero infrastructure but has
no version solving. Acceptable for experiments; never for published
packages.

## Cross-repo version discipline

- Semver tags per repo; changelogs per package.
- grpc-mojo CI runs a compatibility matrix: released-channel versions of
  each dependency **and** their git `main`, gated by the compliance suite,
  so cross-repo breakage is caught before anyone tags a release.
- The umbrella harness (official interop + gRPC sections) together with
  the per-package differential suites it aggregates (h2spec, protobuf
  conformance, hpack/h2/net differentials) remain the release gate for
  every package: a dependency bump that fails them does not ship.

## Pre-split checklist (per package)

- [ ] API reviewed for the 0.1 surface; anything unstable made private.
- [ ] Extracted test subset green standalone.
- [ ] Package README (already at `packages/<repo>/README.md`) reviewed.
- [ ] `pixi-build-mojo` package build verified locally (`pixi build`).
- [ ] Known scope notes carried over (e.g. hpack's UTF-8-values note).
- [ ] modular-community recipe PR opened.
