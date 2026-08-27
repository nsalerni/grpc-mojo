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

## Development checkouts

Until you depend on published conda packages, grpc-mojo clones pinned sibling
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
a `.mojoc` module. Version ranges for siblings are declared there. Publishing
to a conda channel (for example modular-community on prefix.dev) is a separate
step from tagging GitHub releases.

Follow semver from `0.x`. Conda allows one version of a package per
environment, so keep compatibility ranges honest.

## Cross-repo bumps

1. Tag and release the lower package.
2. Update `deps.json` / `recipe.yaml` in dependents.
3. Run `pixi run test` and `pixi run compliance` (and
   `pixi run interop-official` in grpc-mojo).

A dependency bump that fails those suites does not ship.
