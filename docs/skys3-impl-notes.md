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
  validated where they are assigned and when registers are read. The plan's
  M0-03 entry and section 14 row now say so.
- **The 96-byte reservation leaves out the metadata key.** S3 counts both
  keys and values against the 2 KiB user-metadata limit, so the identity
  needs 105 bytes: `skys3-wid` plus a 96-byte value. The design (§7.2) now
  says so, and `WriteIdentity::METADATA_RESERVED_BYTES` holds the figure.
  The plan's M1 user-metadata item said 96; it now names the constant.
- **Coordinator lease renewals must change the ETag.** A candidate takes
  over after seeing the same lease ETag for long enough (§6.7). S3 ETags are
  content hashes, so a renewal that rewrote identical bytes would look like
  no renewal. The fresh `proposal_id` in every write changes the bytes;
  §6.7 now states it.
- **Members may outnumber `replicas`.** A rebalance promotes the new member
  before removing the old one (§6.7), so `ShardConfig` validation does not
  bound the member count by `replicas`.
- **Fuzz targets before the fuzz CI job.** Rule 1.1 wants a `cargo-fuzz`
  target with every parser of untrusted input; write identities read back
  from remote metadata and quoted ETags from remote responses qualify. The
  targets `types_write_identity` and `types_etag` live in `fuzz/`, a crate
  with its own empty `[workspace]` table, so the stable workspace never
  builds it and needs no `exclude`. The targets keep
  `#![forbid(unsafe_code)]`: the `#[no_mangle]` export that `fuzz_target!`
  generates comes from an external macro, which the lint does not report
  (found in M0-06; the first version of this crate wrongly said otherwise).
  Both targets build with
  `cargo +nightly fuzz build` and ran 30 s each without findings; CI runs
  them once M1-01 adds the fuzz smoke job. The 412 check itself still
  compares bytes (`WriteIdentity::matches`) rather than parsing.

### M0-03 Configuration

- **The §14 example contradicted a rule.** Its `[buckets.archive-from-us]`
  table named its own cluster, `skys3-prod-a`, as `peer_source`: it showed
  the peer cluster's side in this cluster's file. A bucket cannot receive
  native replication from its own cluster, so loading rejects that. The
  example now shows `[buckets.archive-from-eu]` receiving from
  `skys3-prod-eu`, the cluster `archive` backs up to.
- **Keys the design names but §14 did not.** Wait-through versus fail-fast
  (§5.2) had no key, so the rule "in wait-through mode" could not be
  applied; §14 now has `replica_ack_timeout_mode`. The S3 control store's
  "endpoint and bucket" became the keys `endpoint` and `bucket`. §7.5 lets a
  bucket set `ack_policy` and §7.2 requires `discard_local` to be chosen per
  bucket, but §14 had both only in `[flush]`: they are now per-bucket
  overrides, and `discard_local` is refused in `[flush]`.
- **Unspecified constants.** §5.4's lease `margin` and §5.2's "one
  control-store round trip" had no values. They are fixed at 500 ms and 1 s
  (`ReplicationConfig::LEASE_MARGIN`, `CAS_ALLOWANCE`) and recorded in the
  design; the defaults satisfy both with room to spare.
- **All violations at once versus typed fields.** Serde stops at the first
  error, so fields whose checks are listed rules (`cluster_id`,
  `shards_per_bucket`, bucket names, target URLs, `peer_source`) are parsed
  as plain strings and numbers and converted during validation, which
  collects every violation. Type errors and unknown keys still stop parsing,
  with the key and line.
- **Inherited bucket settings.** A bad value in `[buckets.defaults]` would
  otherwise be reported again for every named bucket. A table is reported
  only for keys it sets, so each mistake appears once, at the table that
  made it.
- **Docs-only CI would skip the §14 test.** `scripts/ci/docs-only.sh`
  treated every Markdown file as documentation, so a pull request that
  changed only the design's §14 example would skip the test that loads it.
  The design doc and the configuration reference now count as code.
- **URL authorities reuse the node-address rules.** Review found that any
  non-empty authority was accepted (`https://:9000`, `https://host:bad`, an
  unclosed `[fd00::1`) and that IP literals were compared as text, so
  `[fd00::1]` and `[fd00:0:0:0:0:0:0:1]` escaped the correlated-store
  check. `skys3-types` exposes no parser for a host with an optional port,
  so the authority is parsed as a `NodeAddress`, with the scheme's default
  port appended when none is written. `NodeAddress` rejects uppercase DNS
  names, but host names are case-insensitive and operators paste them from
  provider consoles, so the authority is lowercased first and endpoints are
  stored in canonical form. Failure scopes compare parsed addresses, with
  IPv4-mapped IPv6 folded to IPv4.
- **Early returns hid violations.** An invalid `cluster_id` without an
  explicit `prefix` ended control-store validation early, and one invalid
  bucket table skipped the correlated-store check. Resolvers now check
  every key and return placeholders for values that failed, which are
  never used because a violation was reported.
- **No fuzz target.** The configuration file comes from the operator, not
  from untrusted clients, so the plan's parser rule does not apply; its
  parsing is `toml`'s.

### M0-04 Disk and clock abstractions

None.

### M0-05 Simulated S3 store

None.

### M0-06 Observability scaffolding

None.
