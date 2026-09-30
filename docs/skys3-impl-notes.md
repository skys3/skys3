# SkyS3: Implementation Notes

**Implements:** the [task and PR plan](skys3-tasks-plan.md) for the [SkyS3 design](skys3-design.md).

This file records what implementing the plan turned up that the plan and the
design did not anticipate: surprises, wrong assumptions, tooling problems, and
how each one was solved. Design decisions themselves go in the design doc
(plan section 1.1); a note here links to them.

Each task has its own section, so PRs that run in parallel edit different parts
of this file. A task with nothing unexpected keeps "None."

## M0 Foundations

### M0-01 Workspace and CI baseline

- **The bootstrap crate's README include.** `src/lib.rs` used the repository
  README as its crate docs through `include_str!("../README.md")`. Moving the
  crate to `crates/skys3/` changes the relative path to `../../../README.md`,
  and the package's `readme` key to `../../README.md`; `license-file` is
  inherited from `[workspace.package]`, which Cargo rebases per crate.
- **`cargo-deny` and the project's own license.** The workspace crates are
  licensed through `LICENSE` (FSL-1.1-ALv2), which is not on the dependency
  allowlist. They are marked `publish = false` and `deny.toml` sets
  `[licenses.private] ignore = true`, so the allowlist applies only to
  dependencies. `allow-wildcard-paths = true` lets workspace crates depend on
  each other by path without a version.
- **Duplicate-version bans.** `multiple-versions = "deny"` is strict: large
  dependency trees (the AWS SDK, `hyper`) usually carry some duplicates. A PR
  that cannot avoid one adds a `skip` entry to `deny.toml` with the reason,
  rather than weakening the ban.

### M0-02 Core types

None.

### M0-03 Configuration

None.

### M0-04 Disk and clock abstractions

None.

### M0-05 Simulated S3 store

None.

### M0-06 Observability scaffolding

- **No crate for observability in the layout.** Plan section 4 had no home
  for `tracing` setup, metrics, and the admin listener, and none of the
  planned crates fits: the binary crate would make every library depend on
  it to register metrics. A small `skys3-obs` crate is added to the layout.
- **`syn` 2 and `syn` 3 in one build.** `tokio-macros` and
  `prometheus-client`'s derive macro have moved to `syn` 3, while
  `tracing-attributes` (the `#[instrument]` macro, on by default in
  `tracing`) is still on `syn` 2, which `multiple-versions = "deny"`
  rejects. The workspace pins `tracing` with `default-features = false,
  features = ["std"]`, so `#[instrument]` is unavailable. A later PR that
  wants it, or that pulls in another `syn` 2 user, adds a `skip` entry for
  `syn` to `deny.toml`.
- **A per-node registry instead of the `metrics` facade.** The `metrics`
  crate records into one process-global recorder, which would merge the
  metrics of the several nodes the simulation harness runs in one process.
  `prometheus-client` registries are plain values, so each node owns one.
  Its text output is OpenMetrics rather than the classic Prometheus format;
  Prometheus scrapes both.
- **Admin authentication without TLS.** The admin listener authenticates
  with a bearer token on non-loopback addresses (design section 12), but
  it serves plain HTTP until `rustls` arrives with the intra-cluster
  transport (M2-02), which should add admin TLS and client certificates.
- **Configuration keys for M0-03.** The listener takes plain structs
  (`AdminConfig`, `LogConfig`) until the configuration crate exists. The
  keys are in the design's section 14 example: `[admin] listen`,
  `[admin] token_file`, `[logging] filter`, and `[logging] format`.
  `AdminConfig::validate` holds the non-loopback-needs-a-token rule for
  configuration validation to call, and `AdminToken::from_file` loads the
  token file.
