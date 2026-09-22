# skys3

A minimal Rust crate bootstrap for the `skys3` repository.

## Design

See the [SkyS3 write-back S3 cache design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md)
for the proposed architecture: all-replica-acknowledged shard replication,
automatic membership through S3 conditional writes, and ordered write-back to
remote S3 targets. This is a design for review, not implemented functionality.

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