# Packaging

How the five packages relate, and how to consume them.

## Repositories

Each package is its own GitHub repository. grpc-mojo is the gRPC integration
repo, not a monorepo.

| Repo | Publishes | Depends on |
|---|---|---|
| [mojo-net](https://github.com/nsalerni/mojo-net) | `mojo-net` | standard library |
| [protomojo](https://github.com/nsalerni/protomojo) | `protomojo` | standard library |
| [mojo-tls](https://github.com/nsalerni/mojo-tls) | `mojo-tls` | `mojo-net`, libssl |
| [mojo-http2](https://github.com/nsalerni/mojo-http2) | `mojo-http2` (`hpack` + `h2`) | `mojo-net`, `mojo-tls` |
| [grpc-mojo](https://github.com/nsalerni/grpc-mojo) | `grpc-mojo` | all of the above |

hpack and h2 version together in one repo so HTTP/2 consumers do not have to
solve two independent release cadences.

## Install from conda

Packages ship on the
[modular-community](https://github.com/modular/modular-community) channel
(`https://repo.prefix.dev/modular-community`).

```toml
[workspace]
channels = [
    "https://conda.modular.com/max",
    "https://repo.prefix.dev/modular-community",
    "conda-forge",
]
```

```sh
pixi add grpc-mojo
```

`protoc-gen-mojo` is on `PATH` from the `protomojo` package.

Publishing a new version is a PR to `modular/modular-community` that bumps
`context.version` and the git `rev` in `recipes/<name>/recipe.yaml`. The
channel builds from that recipe; there is no separate token upload from
this repo. Publish **bottom-up**: `mojo-net` and `protomojo`, then
`mojo-tls`, then `mojo-http2`, then `grpc-mojo`.

## Development checkouts

Until published packages are enough, grpc-mojo clones pinned sibling
tags into gitignored `packages/`:

```sh
python3 tools/fetch_deps.py
```

Pins live in [`deps.json`](../deps.json). Include paths in `pixi.toml` and CI
point at those checkouts. Do not commit `packages/`.

Dependents that need source checkouts (`mojo-http2`, `mojo-tls`) use `.deps/`
the same way. Leaf packages (`mojo-net`, `protomojo`) have no `deps.json`.

## Conda recipes

Each repo has a [`recipe/recipe.yaml`](../recipe/recipe.yaml) that precompiles
a `.mojoc` module. Version ranges for siblings are declared there. The
modular-community recipe copies that file and points `source` at a git
revision instead of `path: ..`.

Follow semver from `0.x`. Conda allows one version of a package per
environment, so keep compatibility ranges honest.

## Cross-repo bumps

1. Tag and release the lower package.
2. Update `deps.json` / `recipe.yaml` in dependents.
3. Run `pixi run test` and `pixi run compliance` (and
   `pixi run interop-official` in grpc-mojo).
4. Open a modular-community PR with the new `rev` and version.

A dependency bump that fails those suites does not ship.
