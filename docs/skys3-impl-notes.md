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

- **`syn` 2 and 3 in one tree.** `tokio-macros` 2.7.2 moved to `syn` 3, while
  `tracing-attributes` (through `turmoil`'s `tracing`) and `zerocopy-derive`
  (through `rand`) still use `syn` 2, which `multiple-versions = "deny"`
  rejects. `turmoil` enables Tokio's `macros` feature, so it cannot be turned
  off. Pinning `tokio-macros` 2.7.1 (the last release on `syn` 2) worked for
  this branch alone, but a trial merge with M0-02 and M0-06 showed it does
  not hold: `serde`, `thiserror`, and `prometheus-client`'s derive are on
  `syn` 3 too. `deny.toml` therefore skips `syn@2` with the reason, both
  versions being build-time only, and the lock takes `tokio-macros` 2.7.2.
  The skip goes once `tracing-attributes` and `zerocopy-derive` move to
  `syn` 3.
- **`rand` 0.9, not 0.10.** `turmoil` 0.7.2 depends on `rand` 0.9, so the
  workspace uses 0.9 as well to avoid a second copy of `rand` and
  `getrandom`. For the same reason `tempfile` is used without its default
  `getrandom` feature, which would pull in `getrandom` 0.4.
- **`turmoil`'s own file system is not used.** `turmoil` 0.7 has an
  `unstable-fs` feature: a `std::fs` shim with crash semantics. It is
  unstable, simulates at the `std::fs` level rather than behind a trait that
  also has a real implementation on a blocking pool, and its worker-thread
  context draws from `ThreadRng`, which a seed cannot replay. `SimDisk` in
  `skys3-io` implements the `Disk` trait instead.
- **Simulated time is per host.** `turmoil` runs each host on its own paused
  Tokio runtime, so `tokio::time::Instant` is that host's simulated time.
  `MonotonicClock` builds on it and adds drift and an origin, which means a
  node must create its clock inside its host software (`NodeClock::start`),
  not in the test driver.
- **One open file per segment on the real disk.** A first version kept each
  handle's length separately, so a second `open` of a file did not see
  appends made through the first. `RealDisk` now keeps a registry of open
  files by name and returns the same open file to every `open`.
- **Races in the open-file registry.** Review found that the registry's
  first version raced: between `remove`'s unlink and its registry update, a
  concurrent `create` of the same name could register the new file, whose
  entry `remove` then deleted, so a later `open` made a second open file with
  its own length and appends could overlap. A stress test reproduced it
  reliably. `create`, `open`, and `remove` now hold one async lock per disk
  across the system call and the registry update. `open` always opens the
  file system's file and shares a registered file only if its device and
  inode match, so a stale entry is never reused for a replaced file.
  Entries are weak, so closing the last handle closes the file. `SimDisk`
  already had these semantics: its operations run under one lock without
  awaiting.
- **Finding the replay command.** Cargo sets `CARGO_PKG_NAME` for test
  processes, the test binary is named `<target>-<hash>`, and the test harness
  names each test's thread after the test, including with
  `--test-threads=1`. The runner builds
  `SKYS3_SIM_SEED=<seed> cargo test -p <package> --test <target> -- <test> --exact`
  from these, and a test checks the exact output.

### M0-05 Simulated S3 store

- **The object-store trait needed a home before its client.** The flusher
  (M1-16), the remote client (M1-15), and the S3 control store (M2-04) must
  share one trait that the simulator implements without either depending on
  the other. `skys3-remote` is created now holding only the `ObjectStore`
  trait and its request, response, metadata, and error types (plan section
  4 updated); M1-15 adds the AWS SDK client and the probe to it. Its tests
  that use `SimS3` must be integration tests: `skys3-sim` depends on
  `skys3-remote`, so a unit test would see two copies of the trait.
- **`If-Match` on a missing key is 404, not 412.** AWS answers a
  conditional write whose `If-Match` finds no current object with `404
  NoSuchKey`, which the §7.2 recovery rule did not cover, and a retried
  `CompleteMultipartUpload` whose first attempt landed gets `404
  NoSuchUpload`. Design §7.2 now treats both like a 412. The AWS
  documentation was not reachable from the build environment, and older
  pages for conditional deletes described `204` for a missing key, so M1-26
  should confirm the behavior against AWS for each operation.
- **409 has no precise AWS definition.** AWS documents
  `ConditionalRequestConflict` only as a race with a concurrent write. The
  simulator makes a conditional write fail with it when another write to
  the key is applied during its request delay, so conflicts appear only with
  delays (random ones, or a scripted `Fault::Delay` in unit tests). Design
  §7.2 now says how the flusher handles it.
- **Lost `CreateMultipartUpload` responses orphan uploads.** The simulation
  scenario found that such an upload's ID never reaches the flusher, so the
  abort path of §7.3 cannot find it. Design §7.3 now names the lifecycle
  rule as its only backstop; a `ListMultipartUploads` scan would need a new
  trait method, left to M1-16b or M4-02.
- **Ignored versus rejected preconditions.** Providers without support may
  apply the write unconditionally or answer `501 NotImplemented`. The
  simulator models both per operation (`ConditionalSupport`), so the
  capability probe (M1-15) can be tested against each; design §7.2 records
  that both count as unsupported.
- **Nothing to fuzz.** Ranges are the typed `ByteRange`, and continuation
  tokens are keys into a table of issued tokens, so the simulator parses no
  strings. `ByteRange::resolve` has a proptest against a byte-by-byte model.
- **Listing edge cases the first version got wrong.** Review found two:
  a `ListParts` page with `max_parts = 0` was marked truncated without a
  marker, so a paginator looped forever, and a common prefix at or before
  `StartAfter` was returned when keys under it sorted after it (keys `b/1`,
  `b/3`, `StartAfter = b/2` returned `b/`, which S3 leaves out). Listing now
  compares each entry, key or common prefix, with the page's start position
  and the last entry returned, and a limit of zero returns an empty page
  that is not truncated for both operations, as S3 does for `max-keys=0`.
  Property tests page through both listings with page sizes from 0 up and
  compare them with a reference model. The first property test did not
  find the `StartAfter` bug in 256 cases: positions inside a common prefix
  were too rare, so the generator now biases toward them.
- **Simulation tests moved into a directory.** `tests/simulation.rs` became
  `tests/simulation/{main,disk,s3}.rs`, keeping the target name, so CI's
  `--test simulation` and the replay commands are unchanged.

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
- **Detached connection tasks outlived shutdown.** The first version
  spawned each admin connection with `tokio::spawn` and kept no handle, so
  a client stalled mid-request kept its socket, connection slot, and the
  listener's state alive after `serve` returned at the drain deadline.
  Connections now live in a `JoinSet`, which is reaped as the listener
  accepts and aborted and awaited at the deadline. A test holds a
  connection open with half-sent headers and checks it is gone when
  `serve` returns.
- **The bearer-token parser needed a fuzz target.** Plan rule 1.1 covers
  tokens, so the `Authorization: Bearer` parser has proptests and the
  `obs_bearer` cargo-fuzz target in `fuzz/`, the crate layout shared with
  M0-02. The parser is private, so the target reaches it through
  `skys3_obs::fuzzing`, a `#[doc(hidden)]` module that is not a stable API.
  `cargo +nightly fuzz run obs_bearer -- -max_total_time=30` ran about 9
  million inputs without a failure. No CI job runs it yet; M1-01 wires the
  fuzz smoke runs in (plan section 5).
- **`forbid(unsafe_code)` does work in a fuzz target.** The fuzz crate
  was first set up on the assumption that it could not forbid unsafe code,
  because `fuzz_target!` expands to a `#[no_mangle]` export. With
  `libfuzzer-sys` 0.4.13 the lint does not report code expanded from an
  external macro, so every target builds with `#![forbid(unsafe_code)]`,
  and a deliberate `unsafe {}` in one is still rejected. The
  `fuzz/Cargo.toml` header says so.
- **The fuzz crate is outside `cargo deny`.** `cargo deny check` runs on the
  main workspace, and the fuzz crate has its own workspace and lock file
  (`fuzz/Cargo.lock`, committed). Its dependencies never reach the shipped
  binary. `libfuzzer-sys` bundles libFuzzer under the NCSA license, which is
  not on the allowlist.

## M1 Single node

### M1-01 Log record format

- **A shard number alone does not name a shard.** The plan and §10.1 put
  a "shard id" in the record header. `ShardId` is a shard's number within
  its bucket (0 to 255), and segments are shared by every shard on a
  disk, across buckets. So the fixed header holds the bucket ID (up to 25
  bytes, in a zero-padded 32-byte field) and the shard number. The header
  is 80 bytes.
- **Record payloads needed bounds that configuration did not enforce.**
  `inline_max_bytes` and `extent_bytes` had no upper bound, and
  `extent_bytes` had no lower bound. Without one, a 5 GiB PUT could need
  any number of extent references. The format bounds payloads at 16 MiB
  and extent references per `PUT` at 81,920. Configuration loading now
  checks `inline_max_bytes` against the same bound and `extent_bytes`
  against 64 KiB to 16 MiB, using constants from `skys3-log`, so
  `skys3-config` depends on it. One existing test used
  `inline_max_bytes = 512 MiB` to break the segment-size rule, and now
  uses values within the new bounds.
- **Records the design lists fields for needed more.** `PUT` also needs
  `Last-Modified`, so every replica reports the same time; the
  inherited write-identity position (§7.2); and client checksums (§7.4).
  `ADOPT` needs the `seq` its read plan named (§9.2), the size, and
  `Last-Modified`. `EXTENT` records take their own `(epoch, seq)`, so a
  `PUT` references them by position. A `seq` can be reused in a new epoch
  after truncation, so a reference uses the full position. `CONFIG` and
  `TRUNCATE` take no sequence number. Their `seq` field holds the position
  they refer to, and `TRUNCATE` needs no body. These are recorded in §10.1.
- **A torn tail and a record from a newer build look alike.** Recovery
  (M1-02) cuts a torn tail back to the last record whose CRC verifies. A
  whole record with an unknown version or reserved kind is not damage,
  and cutting it would lose data. `DecodeError::class` separates
  incomplete or corrupt records from unsupported or invalid ones, so
  M1-02 can refuse to start instead of truncating.
- **Fuzzing past the CRC.** Random inputs almost never carry a valid
  CRC, so the `log_record` target also decodes a copy of each input with
  the CRC recomputed, which reaches the kind-specific parsers. It checks
  that every accepted input re-encodes to the same bytes, since the
  encoding is canonical. A 60-second run executed about 7.9 million
  inputs without a failure. The proptests do the same with valid records
  that are mutated and then resealed.
- **The fuzz smoke job.** The CI job finds targets with
  `cargo +nightly fuzz list` and runs each for 30 seconds. It installs
  cargo-fuzz with `taiki-e/install-action` and caches `fuzz/target`
  with `Swatinem/rust-cache`. It uploads `fuzz/artifacts/` when a target
  fails.
