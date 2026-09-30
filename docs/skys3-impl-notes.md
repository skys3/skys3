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

- **Bucket IDs cannot be S3 bucket names.** The plan has M0-03 validate
  "cluster and bucket ID lengths" in the configuration, which reads as if the
  bucket ID were the configured bucket name. S3 names run to 63 bytes, and
  the write identity leaves 49 bytes for the cluster and bucket IDs
  together. The bucket ID is therefore a separate ID, assigned at creation
  and never reused (design §4.1), at most 25 bytes; cluster IDs are at most
  24 (§7.2). Never reusing it also closes a hole: a bucket deleted and
  recreated over the same remote, with shard epochs starting again, could
  otherwise produce a write identity that an old remote object already
  carries, and the 412 check would take that object for its own write.
  M0-03 validates `cluster_id` with `ClusterId::new`; bucket IDs are
  validated where they are assigned and when registers are read.
- **The 96-byte reservation leaves out the metadata key.** S3 counts both
  keys and values against the 2 KiB user-metadata limit, so the identity
  needs 105 bytes: `skys3-wid` plus a 96-byte value. The design (§7.2) now
  says so, and `WriteIdentity::METADATA_RESERVED_BYTES` holds the figure.
  The plan's user-metadata item in M1 still says 96; it should use the
  constant.
- **Coordinator lease renewals must change the ETag.** A candidate takes
  over after seeing the same lease ETag for long enough (§6.7). S3 ETags are
  content hashes, so a renewal that rewrote identical bytes would look like
  no renewal. The fresh `proposal_id` in every write changes the bytes;
  §6.7 now states it.
- **Members may outnumber `replicas`.** A rebalance promotes the new member
  before removing the old one (§6.7), so `ShardConfig` validation does not
  bound the member count by `replicas`.
- **No fuzz target for write-identity parsing.** The plan asks for a
  `cargo-fuzz` target with every parser of untrusted input, but fuzzing
  infrastructure arrives with M1-01. The 412 check compares the remote value
  with the canonical text (`WriteIdentity::matches`) and never parses it,
  so no parser sits on an untrusted path yet. `FromStr` is covered by
  proptests for totality and canonical round trips; a caller that starts
  parsing remote values should add a fuzz target.

### M0-03 Configuration

None.

### M0-04 Disk and clock abstractions

None.

### M0-05 Simulated S3 store

None.

### M0-06 Observability scaffolding

None.
