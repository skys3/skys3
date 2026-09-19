# skys3

A minimal Rust crate bootstrap for the `skys3` repository.

## Design

See the [SkyS3 architecture proposal](docs/design.md) for replicated append-log
ingestion, automatic background erasure coding, S3/OIDC/STS compatibility,
external S3 caching, and pre-completion multi-region replication.
This is a design for review, not implemented functionality.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

## CI

GitHub Actions runs the same formatting, lint, and test checks on every push
and pull request using Node-24-safe action versions to avoid Node 20
deprecation warnings.

## License

Licensed under the Functional Source License, Version 1.1, Apache License 2.0
Future License (`FSL-1.1-ALv2`). See `/LICENSE` for the full text.
