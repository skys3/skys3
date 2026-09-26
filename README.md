# skys3

[![CI](https://github.com/skys3/skys3/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/skys3/skys3/actions/workflows/ci.yml?query=branch%3Amain)
[![Tests](https://github.com/skys3/skys3/raw/badges/main/tests.svg)](https://github.com/skys3/skys3/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://github.com/skys3/skys3/raw/badges/main/coverage.svg)](https://github.com/skys3/skys3/actions/workflows/ci.yml?query=branch%3Amain)

A minimal Rust crate bootstrap for the `skys3` repository.

## Design

See the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md)
for the proposed architecture: shard replication where every replica
acknowledges, automatic membership through a pluggable compare-and-swap control
store (S3, R2, or etcd), write-back buckets flushed to remote S3 targets, and
local buckets with per-object erasure coding. This is a design for review, not
implemented functionality. The
[task and PR plan](https://github.com/skys3/skys3/blob/main/docs/skys3-tasks-plan.md)
breaks its delivery into milestones and pull requests.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo coverage
```

`cargo coverage` is an alias (in `.cargo/config.toml`) for `cargo llvm-cov`
that fails if line coverage is below 85%. Change `--fail-under-lines` there to
move the threshold. It needs `cargo install cargo-llvm-cov` and
`rustup component add llvm-tools-preview`.

## CI

GitHub Actions runs the same formatting, lint, test, doc, and coverage checks
on pushes to `main` and on pull requests, plus a `cargo check` on the minimum
supported Rust version declared in `Cargo.toml`. Dependabot keeps actions and
crates up to date.

The tests and coverage badges above are generated on every push to `main`
and stored as SVG files on the `badges` branch
(`scripts/ci/publish-badges.sh`, rendered by `scripts/ci/badge.sh`), so they
need no external service and render in a private repository too. The job
that publishes them checks that GitHub serves them as images from the
rendered README (`scripts/ci/verify-badges-render.sh`).

## License

Copyright Sky Computing LLC. Licensed under the Functional Source License,
Version 1.1, Apache License 2.0 Future License (`FSL-1.1-ALv2`). See `LICENSE`
for the full text.
