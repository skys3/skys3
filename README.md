# skys3

A minimal Rust crate bootstrap for the `skys3` repository.

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