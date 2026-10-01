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
  against 64 KiB to 16 MiB. The shared constants live in
  `skys3_types::limits`, which both `skys3-log` and `skys3-config` read.
  An earlier draft had `skys3-config` depend on `skys3-log`, but
  configuration is a lower layer: the segment and group-commit code
  (M1-02) will want configuration types, which would make a cycle. One
  existing test used
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
- **The prebuilt cargo-fuzz built for musl.** The first CI runs of the
  fuzz job failed before compiling anything: "sanitizer is incompatible
  with statically linked libc". `taiki-e/install-action` ships a
  cargo-fuzz built for `x86_64-unknown-linux-musl`. cargo-fuzz defaults
  `--target` to the triple it was built for, so it asked for a musl
  AddressSanitizer build, which Rust does not support. The job now passes
  `--target x86_64-unknown-linux-gnu` to `fuzz build` and `fuzz run`.
  A locally built cargo-fuzz never shows this, because its own triple is
  already the gnu host.

### M1-02 Segments, group commit, and recovery

- **A torn tail and damage look alike.** A disk may persist an unsynced
  range out of order, so whole records can follow a missing one after a
  crash, just as they follow a damaged record in the middle of a log. Two
  rules separate the cases. First, a class starts a new segment only
  between group commits, so only the last segment of each class can have
  a torn tail. Second, a crash leaves at most one group commit unsynced,
  so a record that verifies more than `group_commit_max_bytes` plus one
  maximum-size record past a bad record means damage. Recovery then
  refuses to start. Inside that window it cuts the tail. Lowering
  `group_commit_max_bytes` while a node is down narrows the window, which
  can make a torn tail look like damage, but never the reverse. Records
  of an unknown version or kind, or with a verified CRC but a broken
  header, also make recovery refuse to start. Design §10.1 records the
  rules.
- **A process restart after a failed sync can lose acknowledged records.**
  The turmoil scenario found this on its first 256 seeds. After a failed
  `fdatasync`, the page cache still showed the lost bytes. The restarted
  process recovered them as valid records, appended after them, and
  acknowledged. The next power loss zeroed the failed range, and recovery
  cut the segment at the hole, losing the later acknowledged records.
  Recovery cannot tell those bytes from durable ones. Design §10.4 now says
  a disk taken out of service is used again only after the host restarts,
  and the scenario models that. Nothing enforces it yet: startup (M1-13)
  should keep a disk out of service until the host restarts, for example
  by recording the boot ID with the failure.
- **Recovery syncs what it keeps.** A process crash keeps the page cache
  and directory entries that were never synced, so recovery sees records
  and segment files that are not durable. Recovery syncs the last segment
  of each class and the directory before the log appends anything. The
  process-crash crash-point test fails without this.
- **Tokio timers have millisecond resolution.** The default
  `group_commit_max_delay_us = 500` rounds up to the next 1 ms tick. A
  shorter wait would need a dedicated timer thread or spinning. The
  configuration reference and §10.4 say so. The performance suite
  (§16.3) should decide whether that matters.
- **Simulated disk operations never yield.** `SimMount` finishes each
  operation without yielding, so no record could queue behind a group
  commit in progress. A deliberate mutation that kept committing after a
  failed sync passed every test. The tests' audited disk now yields once per
  operation, and a unit test queues records behind a failing commit. Six
  deliberate mutations, including skipped data or directory syncs,
  skipped recovery syncs, and a missing damage check, each fail at least one test.
- **Any write-path I/O error takes the disk out of service**, not just a
  failed sync. A short write on a full disk leaves a partial record in the
  middle of a segment if appends continue, and truncating it is another
  write that can fail. Admission control (M1-17) is meant to keep disks
  from filling.
- **Recovery verifies headers, not bodies.** Recovery checks each
  record's fixed header and CRC, which is all a torn tail can break. A
  body that breaks the format under a valid CRC comes from a faulty writer.
  Replay (M1-03) decodes bodies and treats such a record as damage.
- **The benchmark.** `cargo run --release -p skys3-log --example
  group_commit_bench` runs concurrent appenders against a real disk and
  prints records per group commit, throughput, and latency. It is an
  example, not a criterion bench, to avoid a new dependency. On the
  development container's disk, 64 writers of 4 KiB inline records with
  the default 500 µs delay got about 60 records per group commit (214
  commits for 12,800 records, about 15,000 records per second). A single
  writer gets one record per commit.

### M1-03 Index and checkpoints

- **redb 4.3, not the 2.x API the design was written against.** The
  `StorageBackend` trait, `Durability::None` and `Durability::Immediate`
  remain. Checkpoint commits also enable quick repair, which commits in two
  phases and saves the allocator state. Without it, reopening after a crash
  walks the whole database to rebuild the allocator, which would be slow
  for a large namespace mirror.
- **redb cannot run on the `Disk` trait.** `Disk` is append-only and async,
  and redb needs synchronous writes at any offset. The simulated disk now
  also holds random-access block files (`SimBlockFile`), and
  `Index::open_sim` runs redb on one through a `StorageBackend`. One
  `SimDisk::crash` reverts the log and the index together. A crash keeps the
  synced image plus each unsynced write with the torn-write probability, as
  a random prefix. A failed sync loses its writes and takes the file out of
  service. Nodes use redb's own file backend (`Index::open`), which does
  not sync the directory of a new file, so `Index::open` does that itself.
- **Dropping a redb database makes it durable.** `Drop` commits the
  allocator state durably and writes a clean-shutdown header, which would
  turn a simulated crash into a checkpoint. The scenarios therefore crash
  the disk before they drop the index (power loss: its I/O then fails), or
  leak it with `mem::forget` (a killed process: the disk keeps every write).
- **Applied positions do not say where replay must start.** They name what
  each shard applied, not where its later records are. A record can also be
  acknowledged and not yet applied when a checkpoint runs. The log
  therefore keeps a summary of each segment: the highest position of each
  shard among the records it acknowledged there (`SegmentSummary`). Each
  checkpoint stores the summaries in its durable commit and releases a
  segment once every summarized position is behind it
  (`SegmentLog::release`). Replay reads a segment from where its summary
  ends, or from the start if the summary is not behind the checkpoint.
  Design §10.2 records the rules. Summaries cover acknowledged records only,
  so a crash cannot take back what they claim. The log's changes:
  requests carry their shard and position, and the log gains `summaries`,
  `release`, `released`, and `scan_range`. Removing released segments
  stays with M1-22.
- **A shard's extents and its other records are in different segments.**
  Replay reads segment by segment, so a `PUT` can come before the extents
  it references. Replay sorts each shard's records by position. A mutation
  that dropped the sort failed the simulation. For M1-04: a shard's applied
  position must mean that every earlier record of the shard was applied,
  `EXTENT` records included. The state machine cannot apply a small `PUT`
  while the extents of an earlier-sequenced upload are still unapplied.
- **A shard's records can span a node's disks.** Review found that replay
  ordered each shard's records within one disk at a time. A record on the
  first disk could then advance the applied position past an earlier
  record on a later disk, which replay skipped. Nothing in the design pins
  a shard replica to one disk, so replay now collects every disk's
  records before it orders each shard's. A unit test and the simulation,
  which now runs some seeds with two disks, fail without the fix. The
  location map names no disk yet; M1-13, which places replicas on disks,
  must add it or keep a replica's payload on one disk (design §10.2).
- **Entries name payload by position.** Payload named by node-local
  location would make entries differ between replicas. Entries name the
  record that holds the payload, and the location map resolves inline
  `PUT` records as well as extents (design §10.2).
- **Control state is not in the log,** so replay cannot restore it. Each
  change to the local copy commits durably at once (design §10.2).
- **The tests check themselves.** The seeded scenario
  (`crates/skys3-index/tests/simulation.rs`, run by CI's simulation job)
  checks that each restart reverts to the last durable commit, that replay
  reproduces the index exactly, that replay reads nothing of a released
  segment, and that released segments hold only records behind the
  checkpoint. Four deliberate mutations each fail it: dropping the replay
  sort, replaying from a summary that is not behind the checkpoint,
  releasing without comparing positions, and checkpointing without
  durability.
- **Left open.** A shard that stops applying, for example one dropped from
  the node, holds its segments forever; dropping a shard (M2, M3) must
  clear its summaries. The design asks checkpoints to keep each shard's
  latest `CONFIG` record reachable. The index does not store configurations
  yet, so reclaiming segments (M1-22) or the local control-state copies
  (M2-16) must keep it.

### M1-04 Shard state machine with one replica

- **A stale `FLUSHED` cannot simply be dropped.** The plan says `FLUSHED`
  cleans an entry only if its `seq` is still current. But when a newer
  version commits during a flush, §7.1 flushes it next "conditioned on the
  ETag the older flush produced", and dropping the older `FLUSHED` loses
  that ETag: the next flush would send the stale `If-Match`, get 412, find
  the older flush's write identity, and report a conflict. A `FLUSHED` of
  an older `seq` therefore records the remote ETag and version ID and
  leaves the newer version dirty. Design §4.2 records this and the other
  rules the state diagram leaves open (writes to a conflicted key, `TAGS`,
  duplicate `FLUSHED`, what `ADOPT` and `IMPORT` leave behind).
- **The applier reports nothing.** M1-03's `Applier` returns `()`, and a
  rejection is a no-op that still advances the applied position.
  `StateMachine::apply` returns an `Outcome`; the `Applier` impl drops it
  for replay, and `Recorder` keeps outcomes for the live path, so the
  index crate did not change.
- **A record the log refuses would leave a gap.** If a record failed to
  encode, or carried more inline payload than `inline_max_bytes`, after it
  got its position, the shard would have a hole in its log. `SegmentLog`
  gained `check`, and `LogRecord` gained `check`, which encodes only the
  headers, so a shard checks a record under its sequencer lock without
  copying payload. A record that fails after it is appended is an I/O
  error, which stops the shard.
- **The simulation never reorders acknowledgements.** The log acknowledges
  each group in queue order, and on a current-thread runtime the append
  tasks queue in position order. A deliberate mutation that applied
  records in acknowledgement order passed the seeded simulation and the
  single-threaded tests; it failed a test that runs writers on worker
  threads, which now runs 16 times. The pipeline's own unit tests cover
  reordering deterministically.
- **An abandoned seal leaked.** `seal` counts a seal before it waits for
  earlier writes; a gateway request cancelled during that wait would have
  left the shard sealed until restart. A guard lifts the seal unless
  `seal` returns it.
- **Holes in the durable log remain possible for unacknowledged records.**
  On worker threads, appends can queue out of position order, so a power
  loss can keep a later unacknowledged record without an earlier one.
  Replay applies what is there, which is correct with one replica. M2-07
  must queue a shard's records in order or rely on reconciliation (§6.6).
- **Removing a shard must not be cancellable halfway.** Review found
  that `ShardSet::remove` let the shard reopen while it still waited for
  the old pipeline, and the removal then deleted the new shard's index
  state. Holding a `Removing` slot fixes that only if the removal
  finishes: a dropped `remove` would leave the old pipeline applying
  records under a reopened shard. The removal therefore runs in its own
  task. Review also found that `open` ignored a newer epoch on an open
  shard; `Shard::reconfigure` now adopts it in order with writes, and a
  different configuration in the same epoch is refused.
- **Left open.** `Flushing` and `Conflict` have no record kind, so the
  flusher (M1-16) must keep those transitions out of the replayed index
  or make them replayable. Nothing flushes a `local` bucket in M1, so its
  tombstones stay; M1-09 can commit a `FLUSHED` right after a local
  `DELETE`. Conditional requests (M1-09) must check preconditions against
  writes sequenced but not yet applied. `ShardSet` uses one disk's log;
  choosing a disk per shard is M1-13's, as is reclaiming a removed shard
  that replay brings back (`Index::remove_shard` is not durable, and its
  segment summaries still hold the log, as M1-03 noted). The gateway's
  `Shards` takes a `BucketDocument`, and `ShardSet::open` a
  `ShardConfig`; M1-09 maps one to the other.

### M1-05 Control store interface and local backends

- **Which ID keys a bucket register.** The layout said `buckets/<bucket>.json`
  and `shards/<bucket>/<n>.json`, which could mean the S3 name or the bucket
  ID for either. Bucket registers are keyed by name, which requests use to
  find them, and shard registers by bucket ID, which is never reused, so a
  bucket recreated under the same name never inherits old shards. Design
  §6.1 now says so, with the key grammar every backend can store.
- **The lost-response rule cannot see a superseded write.** The simulation
  found seeds where no node reported creating `cluster.json`: the creator's
  answer was lost, and another node incremented the generation, replacing
  the creator's `proposal_id`, before the creator re-read. The rule then
  sees another writer's value and reports a lost race. That is safe, since
  the proposer acts on the current value as a loser would, but "exactly one
  node reports creating it" holds only without lost responses. Design §6.1
  now states the limit; the simulation checks that at most one node reports
  creating it and that all agree on the document, and the fault-free
  conformance suite checks exactly one.
- **Change semantics were open.** Design §6.2 now defines them:
  generation-driven, a snapshot first, then coalesced differences, at
  least once, and never `cluster.json` or `coordinator.lease`. They are the
  weakest a polling S3 backend can give, so every backend shares one
  implementation, `ChangeFeed`, built from `get` and `list` and woken by
  in-process notifications or a timer. M2-04 can use the polling feed as it
  is; the trait has no conditional `get`, so a feed that sends
  `If-None-Match` would be backend-specific. The increment comes after the
  writes it announces, and a lost increment race needs no retry, because
  the winning increment came after the loser's writes.
- **Reads need retries too.** The first simulation runs failed on a
  transient error in bootstrap's read of an existing `cluster.json`, which
  `propose` did not cover. `read_with_retries` retries reads under the same
  policy, and bootstrap and the generation increment use it.
- **No delete.** The plan's trait has none, but M1-06 (DeleteBucket) and
  M3-02 (forgetting nodes) will need one. Change reports already carry
  removals. Whoever adds `delete_if` must decide how a lost response is
  resolved, since a deleted register carries no `proposal_id`.
- **File I/O uses `std::fs`, not `skys3-io`'s `Disk`.** `Disk` models one
  flat directory of append-only segments, with no rename and no
  subdirectories, so it cannot do temp-file, `fsync`, rename, directory
  `fsync`. The file store runs `std::fs` on a caller-supplied
  `BlockingPool`, and a whole write, precondition check included, is one
  pool job, so a dropped future cannot leave the in-memory copy behind the
  files. One process owns the directory through `File::try_lock` (stable
  since Rust 1.89). Versions are content hashes, as S3 ETags are. Crashes
  cannot be simulated through `SimDisk`, so crash safety rests on the
  write protocol; a test checks that leftover temporary files are removed
  at open.
- **Faults as a wrapper, not a hook in the memory store.** `FaultyStore`
  wraps any backend, so the conformance suite can inject lost responses,
  late requests (applied after the error was returned, like a timed-out
  request in flight), conflicts, and outages into every backend, not only
  the in-memory one. Test support sits behind a `test-util` feature, which
  the crate's own tests enable through a dev-dependency on itself.
- **Where the simulation scenario lives.** It is in `skys3-control`'s own
  `simulation` test target, following the `skys3-sim` convention that
  scenarios live in the crate whose code they exercise; CI's
  `--workspace --test simulation` job runs it. Two deliberate mutations of
  the rule (treating a lost answer as success, and skipping the re-read)
  each fail the scenario on the first seeds.
- **No configuration key for the file backend yet.** §14's
  `[control_store] backend` offers `etcd` and `s3`. The file backend is not
  wired into the binary (rule 1.1); the task that wires the control store
  into the node (M1-13) adds `backend = "file"` and its directory key.

### M1-06 Gateway skeleton and bucket operations

- **The s3s dependency tree.** `s3s` 0.17 is on hyper 1 and http 1, like the
  workspace, but brings `crc-fast`, whose default features (which `s3s`
  turns on) implement `digest` 0.10's traits while everything else is on
  0.11, and `xxhash-rust`, licensed BSL-1.0. `deny.toml` skips
  `digest@0.10` and `crypto-common@0.1` with that reason, and allows
  BSL-1.0, a permissive license that asks nothing of binary distributions.
  `crc-fast` also gives M1-08 a CRC64NVME implementation.
- **s3s rejects signed requests without its own authenticator.** With no
  `S3Auth` set, `s3s` accepts unsigned requests, skips its access hook
  entirely, and answers any signed request with `501 NotImplemented`. The
  gateway's `Authenticator` stage returns the request to route, so SigV4
  (M1-07a), which SkyS3 verifies itself, must hand `s3s` a request it
  takes as anonymous: signature headers or query parameters removed and an
  `aws-chunked` body already decoded. Its access hook is unused; M1-07b
  authorizes in the pipeline.
- **Bounding XML before s3s parses it.** `s3s` reads a body only after
  routing, so the gateway decides from the method, path, and query whether
  a body is object data, which streams on, or XML, which it reads up to
  4 MiB and scans for depth and document type declarations with
  `quick-xml`, the version `s3s` already uses, so the tree gains nothing.
  `s3s`'s own XML deserializer follows each operation's schema and skips
  unknown elements without recursion, so depth was never a stack risk; the
  scan is defense in depth, and the size bound is what bounds memory. The
  classification assumes path-style requests. Virtual-hosted-style needs
  the service's domain, which no configuration key names yet.
- **How a request sets a bucket's mode and target.** S3 has no field for
  either, and configuration has no `target` key for `write_back` buckets
  (they are bound at attach time). CreateBucket takes them from
  `x-skys3-bucket-mode` and `x-skys3-bucket-target`, with the mode
  defaulting to the bucket's configured `mode`; design §11 records it. They
  are not `x-amz-` headers, so M1-07a must require them to be signed.
- **No delete in the control store.** `ControlStore` gained `delete_if`,
  conditional on a version, in both backends, the fault wrapper, and the
  conformance suite, with `propose_delete` for retries. A lost delete
  response is resolved by re-reading: an absent register counts as
  deleted. Tombstone values with a `proposal_id` were the alternative, but
  every reader and lister would have to skip them. Design §6.1 records the
  rule and that S3 backends delete with `DeleteObject` and `If-Match`; AWS
  supports it, Cloudflare R2 lists conditional headers only on `PutObject`,
  so the M2-04 probe must check conditional deletes too.
- **Detaching needs a fence in the shard.** Checking for dirty entries and
  then deleting the register races with writes in between, and two
  concurrent DeleteBucket requests could lift each other's fence. The shard
  interface has nesting `seal` and `unseal`, which the stub implements and
  the shard state machine (M1-04) must order with the shard's writes. A
  seal held across nodes, and lifted if its gateway fails, is left to the
  shard map (M2-08). Shards left behind by a creation or deletion whose
  outcome was unknown are for startup recovery (M1-13) to reclaim.
- **Bucket registers had no creation time.** ListBuckets reports
  `CreationDate`, which the AWS CLI's `s3 ls` reads unconditionally.
  `BucketDocument` gained `created_unix_ms`, without a format version bump
  since no register has been written by a release.
- **Bucket operations do touch the control store.** Design §6.1 said no
  client request reads or writes it. CreateBucket and DeleteBucket must;
  §6.1 now says so, and HeadBucket, ListBuckets, and GetBucketLocation are
  answered from the gateway's local copy, which `Gateway::reload_buckets`
  refreshes for changes other nodes make.
- **Configuration helpers.** Attaching a target must apply the control-store
  independence check, which was private to configuration loading.
  `skys3-config` now exports `parse_target` and
  `ControlStoreConfig::check_target_independence`.
- **ServerSideEncryptionConfigurationNotFoundError is a 400.** S3 (and
  `s3s`) answer it with 400, not 404 like the Object Lock equivalent.
- **Fuzzing found an unanswerable error.** The `gateway_request` target
  drives arbitrary requests, and arbitrary XML bodies for 14 operations,
  through the whole pipeline. Its first run found that the XML scan's
  error message quoted the parser's error, which quoted a NUL byte from the
  body; `s3s` cannot write that as XML, and the request got an empty
  `500`. Messages no longer quote request bytes, and an error whose message
  cannot be written is answered with its code alone. A later 60-second run
  executed about 250,000 inputs without a failure. The target asserts that
  no input gets a `500` and that responses stay under 64 KiB; memory is
  bounded with libFuzzer's `-rss_limit_mb`.
- **The checks must decode what s3s decodes (review).** The first version
  read raw query pairs and split the raw path, while `s3s` decodes the
  path before splitting off the bucket and decodes query names and
  values. `?part%4Eumber=10001` skipped the part-number check, `?a%63l`
  made an ACL body look like object data and skipped the XML checks, and
  `/b%2Fk` looked like a bucket request. `s3s`'s query parser
  (`OrderedQs`) is public only under `cfg(fuzzing)`, so the gateway parses
  queries with `serde_urlencoded` directly, as `s3s` does, and decodes the
  path the same way before splitting it.
- **Lost answers also lose the announcement (review).** A create or delete
  that landed with every answer lost returned `503` before incrementing
  the generation, and the retry, finding the work done, did not increment
  it either, so other gateways never learned of the change; a lost delete
  also left its sealed shards behind. Retries now announce what they find
  done, and a gateway remembers its deletions of unknown outcome (at most
  256) so a retry can finish them; design §4.1 and §6.1 record the rule.

### M1-07a SigV4 signing and aws-chunked bodies

- **The node clock cannot check request times.** The task asked for the
  `skys3-io` `Clock`, but it is monotonic with a per-node origin, and
  SigV4 times are Unix times. `skys3-sts` already had a `WallClock` for
  token lifetimes; it moved to `skys3-io` (`WallClock`, `SystemWallClock`,
  `ManualWallClock`), and `skys3-sts` re-exports it. Skew and presigned
  expiry tests set a `ManualWallClock`.
- **Canonical requests are built without decoding.** Decoding the path and
  re-encoding it, as many servers do, turns `%2F` into `/` and `%2B` into
  `+`, so a signature over one key would verify for another. The
  canonicalizer keeps `%XX` escapes (uppercasing their digits), keeps
  unreserved bytes, and encodes the rest; design §11 records the rule.
  The AWS Common Runtime suite agrees on every S3-relevant vector, except
  that its `post-sts-header-after` presigned form adds the session token
  after signing, which S3 does not do, so that form is skipped.
- **A raw `+` in the query was a signature bypass.** The canonicalizer
  first encoded a raw `+` as `%2B`, its byte, but `s3s` decodes the query
  as a form, where `+` is a space. A presigned URL for
  `prefix=private%2Badmin` still verified as `prefix=private+admin`, which
  `s3s` runs with a different prefix. A raw `+` in the query is now
  signed as `%20`, the space it means. That is also what the AWS SDKs'
  signer does (`aws-sigv4` form-decodes the query before encoding it), so
  refusing raw `+` would have refused correctly signed requests. An audit
  of the other encodings found them consistent: escapes in either case,
  invalid escapes (signed as `%25`), `;`, `=` inside values, empty names,
  non-UTF-8 escapes, and the path (`%2F`, and `+`, which paths do not
  decode). Repeated parameters were not: signing sorts them by value, so
  their order, which `s3s` keeps, was unsigned; a signed query may now
  not repeat a parameter. A proptest and the `gateway_sigv4_canonical`
  fuzz target check that a canonical query decodes, as `s3s` decodes it,
  to the same pairs as its query, which rules out any two queries that
  mean different things sharing a signature.
- **Parameter names are decoded as `s3s` decodes them.** M1-06's review
  fixes made the limit checks decode the query with `serde_urlencoded`, as
  `s3s` does. The authenticator first matched raw names, so
  `X-Amz-%53ignature` would have escaped presigned detection and
  stripping while `s3s` still saw a signature (and answered `501`). Every
  decision on a query parameter's name (presigned or SigV2, which values
  are the signing parameters, which pairs to strip, which pair the
  canonical query leaves out) now uses the pair decoded the same way;
  values decode as in a form too, so a raw `+` in a token is a space. The
  canonical request still uses the pairs as received. Headers need no
  such care: their names are not escaped, and every query parameter is
  in the canonical query, so all of them are signed.
- **The published examples could not be fetched.** The sandbox's proxy
  refuses `docs.aws.amazon.com`. The S3 documentation's signatures (GET
  and PUT Object, lifecycle, listing, the presigned URL, and both chunked
  uploads with their chunk and trailer signatures) are quoted by two
  independent implementations in the registry (`s3s-sigv4` and
  `aws-runtime` tests), which agree with each other and now with SkyS3.
  The CRT suite is vendored from the `aws-sigv4` crate, 31 of its 40
  vectors, with its Apache-2.0 license and a `NOTICE`.
- **Chunk data streams before its chunk is verified.** `s3s` buffers each
  chunk until its signature checks out, which costs a chunk of memory per
  upload and bounds chunk sizes. The decoder instead passes data through,
  checks a chunk's signature when its last byte arrives, and fails the read
  that carries it. A body is therefore authenticated only once read to its
  end without error; consumers must commit nothing before then, which they
  must do anyway for `x-amz-content-sha256` and trailing checksums. Memory
  is bounded by the 128-byte chunk line and 4 KiB trailer limits (design
  §12), never by a declared length.
- **Hashing runs on the reactor.** Payload SHA-256 and chunk hashing run in
  the body's `poll_frame`, one frame at a time. Design §15 puts hashing on
  blocking pools; when M1-08 moves checksum validation there, SigV4's
  hashing can join it.
- **s3s has no codes for two answers.** `XAmzContentSHA256Mismatch` and
  `MalformedTrailerError` are custom codes with status 400. Body failures
  are a `BodyError`, which the gateway maps for XML bodies (found through
  the error's source chain) and object operations (M1-09) must map too.
- **Anonymous requests pass through.** Rejecting them is authorization's
  job (M1-07b, `anonymous_access`). They get no `Authenticated` extension,
  may not carry `x-skys3-*` headers, and an unsigned-trailer `aws-chunked`
  body is still decoded for them.
- **Left out.** The `Date` header as the signing time (every SDK sends
  `x-amz-date`), SigV4a, and signed POST policy forms, which `s3s` still
  answers with `501` and which S3 operations in design §11 do not need.
- **Test signers.** `aws-sigv4`, `aws-credential-types`, and `aws-sdk-s3`
  were already in the lockfile through `skys3-remote`; as dev-dependencies
  they sign test requests independently of SkyS3's code, and the AWS SDK
  for Rust creates, lists, locates, presigns for, and deletes buckets over
  the `GatewayListener`. `http-body` became a direct dependency for the
  body wrappers; it was already in the tree through hyper.
- **Fuzzing.** `gateway_aws_chunked` ran about 39,000 inputs in 30 s (each
  signs and verifies many small chunks under the sanitizers) and
  `gateway_sigv4_canonical` about 440,000, with no failures.

### M1-07b Authorization and static credentials

- **s3s calls its access hook only with an auth provider.** M1-06 left
  `s3s` without one, so its `S3Access` hook never ran. The gateway now
  sets a provider that never finds a key (`authz::NoSignatures`): requests
  reach `s3s` with their signature removed, so it is never asked, and
  anything `s3s` still takes as signed, such as a signed POST form, is
  refused. The hook then authorizes every routed request by operation
  name, before `s3s` reads the operation's input.
- **The operation is known only after routing.** `s3s` resolves it, and
  the pipeline checks rejected-feature headers and reads XML bodies before
  that. Unsigned requests are therefore refused right after
  authentication, so an anonymous caller gets `AccessDenied` before
  anything else. A signed caller outside its policy may still see a
  rejected-feature answer, or have an XML body of at most 4 MiB read,
  first. Design §11 records the order.
- **The principal type became concrete.** M1-07a's `CredentialLookup` had
  an associated `Principal` type. Authorization finds the caller in the
  request's extensions by type, so a lookup with another type would have
  looked anonymous. `Authenticated` and `SigningCredential` now carry a
  `Principal` with `Permissions`: identity policies plus an optional
  session policy, which M1-24's sessions fill in.
- **Where the policy language lives.** Configuration must validate
  policies at load time, and STS (M1-24) must parse session policies and
  roles from `identity/`, so the parser and evaluator are
  `skys3_types::policy`, next to the other control-store documents;
  `Policy` also deserializes from a JSON object for registers.
  `Principal`, `Permissions`, and the operation-to-action table are in the
  gateway's `authz` module.
- **Conditions are refused, not ignored.** Ignoring a `Condition` would
  widen an `Allow`, so policies with `Condition`, `Principal`, policy
  variables, or ARNs other than S3's fail to parse. Duplicate keys are
  refused too (serde's derived visitors), so `{"Effect": "Allow",
  "Effect": "Deny"}` cannot mean different things to different readers.
- **`anonymous_access = true` needed a policy.** Allowing every action to
  anonymous callers would be the only reading without one, so the new key
  `[identity] anonymous_policy` is required when anonymous access is on
  and refused when it is off.
- **Static secrets live in files.** Each
  `[identity.static_credentials.<name>]` table names a
  `secret_access_key_file`, read at startup into a buffer that is zeroed
  after use, like `[admin] token_file`. The secret must be 32 to 128
  visible ASCII characters. `SecretAccessKey` became an
  `Arc<SecretBox<[u8]>>`, zeroed when its last clone is dropped. The
  signing key SigV4 derives from it per request is not yet zeroized.
- **Requests could arrive already authenticated (review).** The
  authenticator leaves an unsigned request's extensions alone, so an
  `Authenticated` extension set by an embedding or middleware passed the
  anonymous gate and was authorized as given. The gateway now removes
  `Authenticated` and `Trailers`, the extensions its own stages set and
  trust, from every request before authentication, whatever the
  `Authenticator`.
- **Policy limits held only for documents (review).** `Policy::parse`
  checked the document's byte length, but `Deserialize`, the path for
  policies embedded in registers, did not, and the two could disagree on
  whitespace. Both now apply the same limits after reading: at most
  10,240 bytes of text in the policy's string values, 1,280 per pattern,
  and 100 values per array, the last checked while the array is read.
  Proptests and the `types_policy` fuzz target check that both paths
  accept the same policies.
- **The test authenticator had to authenticate.** M1-06's
  `Unauthenticated` let anonymous requests through, which are now
  refused; it is renamed `TrustAll` and attaches a principal allowed
  everything. `MemoryCredentials` keys are allowed everything unless
  added `with_permissions`.
- **DeleteObjects has no action yet.** Its path names the bucket, but
  S3 checks `s3:DeleteObject` on each key, which needs the parsed body. Until
  M1-10 adds that check in the `delete_objects` access hook, it is denied
  to everyone. CopyObject's source check is left to M1-10 the same way.
- **Fuzzing.** `types_policy` ran about 2.5 million inputs in 30 s, and
  `gateway_request` and `gateway_sigv4_canonical`, which now go through
  the access hook and the new principal type, ran 30 s each, with no
  failures.

### M1-08 Checksums and ETags

- **`aws-lc-rs` has no MD5.** Its `digest` module offers SHA-1
  (`SHA1_FOR_LEGACY_USE_ONLY`) and SHA-256, which the gateway uses, but no
  MD5, legacy or not. MD5 comes from `md-5` 0.11, already a workspace
  dependency (M0-05) and in the tree through `s3s`. The CRCs come from
  `crc-fast`, which `s3s` and the AWS SDK's checksums already pull in, and
  which also combines the CRCs of consecutive ranges, as `FULL_OBJECT`
  multipart checksums need. It is declared with `default-features = false`;
  `s3s` turns on its `std` feature anyway, so the lock file gains no crate.
  `crc32c` stays for log records. Design §15 lists the choices.
- **The stored checksums could not hold a multipart checksum.** M1-01's
  `Checksums` mapped each algorithm to a digest, with no checksum type, so a
  `COMPOSITE` checksum (`<base64>-<parts>`) from an `ADOPT` of a remote
  multipart object, or from a completed upload, had no representation.
  `ChecksumAlgorithm` moved to `skys3_types::checksum` (`skys3-log`
  re-exports it), and the map's value is now a `Checksum`: the digest and,
  for a composite checksum, the part count. Log records and index values
  encode a `u16` part count after each digest, zero for `FULL_OBJECT`.
  The first version of this PR kept the formats at 1, reasoning that no
  release had written either; review pointed out that §10.1 requires a new
  version for any body change, and that the stack's PRs may land one by
  one, so version 1 data can exist. The log format is now version 2 and
  the index value format 2. Both readers still decode version 1, whose
  checksums have no part count and are `FULL_OBJECT`: the version reaches
  the body decoder in the log's `Reader` and the index's, a few lines
  each, and it keeps the rule meaningful. Versions 0 and 3 and up are
  rejected. A version 1 record re-encodes as version 2, so the fuzz
  targets compare bytes only for the current version and otherwise check
  that the re-encoding decodes to the same value.
  `ChecksumAlgorithm` is no longer `#[non_exhaustive]`: every algorithm
  needs a hasher, so a new one must break every `match`.
- **The S3 error documentation was unreachable.** The sandbox's proxy
  refuses `docs.aws.amazon.com`, so the error mapping comes from the S3 API
  reference text in the `aws-sdk-s3` crate (`BadDigest` when
  `x-amz-sdk-checksum-algorithm` names another algorithm), from the Ceph
  `s3-tests` suite (`InvalidDigest` and `BadDigest` for `Content-MD5`, and
  `BadDigest` for the checksum value `bad`), and from S3's message for a
  value of the wrong length (`InvalidRequest`, "Value for
  x-amz-checksum-crc32 header is invalid."). Telling a value that is not
  base64 (`BadDigest`) from base64 of the wrong length (`InvalidRequest`)
  reconciles the last two; the SDK matrix (M1-25) and the provider runs
  (M1-26) should confirm it against S3. `XAmzContentChecksumMismatch`,
  which the task suggested, is a code MinIO uses and none of these S3
  sources has; SkyS3 never sends it.
- **Known answers from `s3-tests`.** Besides the CRC catalogue's check
  values and the FIPS 180 and RFC 1321 examples, the tests use the suite's
  three 5 MiB parts: each part's checksum in all five algorithms, the
  composite SHA1 and SHA256 checksums, the combined CRC32, CRC32C, and
  CRC64NVME checksums, and the multipart ETag. All matched on the first run.
- **S3 adds a CRC64NVME checksum.** Since late 2024 S3 stores a CRC64NVME
  checksum with every object uploaded without one, and returns it with
  `x-amz-checksum-mode: ENABLED`; the validator does the same, at the cost
  of one CRC pass beside the MD5. Whether a flush forwards that checksum is
  the flusher's decision (M1-16), since some providers reject flexible
  checksums (M1-15).
- **Hashing on the pool is batched.** A pool job per body frame would cost
  a thread handoff every few KiB. `PooledHasher` hands the pool 256 KiB at
  a time, keeps one batch in flight while the caller reads on, and waits
  only when a second batch is full, so at most two batches are held. The
  `x-amz-content-sha256` hash moved onto it too, through
  `SigV4Authenticator::with_hashing_pool`; without a pool (tests, fuzzing)
  it hashes inline. The SHA-256 of each signed `aws-chunked` chunk still
  runs in the decoder, on the reactor: the decoder checks a chunk's
  signature before it yields the chunk's last byte, so moving that hash
  needs an asynchronous decoder. The SDKs' default upload form,
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER`, has no chunk signatures.
- **No object operation uses it yet.** `ChecksumValidator` takes the
  decoded body chunk by chunk and returns the MD5 ETag and the checksums
  to store, and `MultipartEtag` and `MultipartChecksum` fold part digests;
  PUT and GET (M1-09) and multipart uploads (M1-12) call them.
- **Fuzzing.** `gateway_checksums` reads checksum headers and stored
  checksum values; it ran about 500,000 inputs in 30 s with no failures.
  After the version bump, `log_record` (about 5.2 million inputs) and
  `index_codec` (about 500,000), which now also try every value behind a
  format 1 byte, ran 30 s each without failures, as did
  `gateway_checksums` again.

### M1-09 Object PUT, GET, HEAD, and DELETE

- **Conditional writes needed a protocol in the shard.** A precondition
  read from the index misses writes that are sequenced but not applied,
  and the index read is blocking I/O, which cannot run under the
  sequencer's lock. `Shard::commit_if` keeps, per key, the position of the
  latest record sequenced for it while it is unapplied, waits for such a
  record before reading, registers the read, and sequences the write only
  if no record of the key was sequenced since the read began. Records
  stay listed while an earlier read is in progress. The first version
  ended the read before checking for such a record, which let the list
  forget a write the read had missed; a multi-threaded race of
  `If-None-Match: *` creations found two winners on its first run. Design
  §5.1 records the rule. Disabling the wait for unapplied writes fails
  three of the new shard tests.
- **The shard stub became real shards.** The gateway's `Shards` trait
  gained `entry`, `payload`, `append_extent`, and `write` (with a
  `Precondition`), and `LocalShards` serves it from `ShardSet`, mapping a
  `BucketDocument` to a single-member `ShardConfig` in epoch 1 (the node
  ID is the caller's; M1-13 supplies it). Rather than reimplement objects
  in memory, `stub::MemoryShards` is now `LocalShards` over a `SimDisk`,
  with the old helpers (`put` in a state, `flush`, `is_sealed`,
  `set_unavailable`) built from real records, so they became `async`. The
  bucket tests now run against the real state machine, and
  `MemoryShards::open` replays a crashed disk, which the durability test
  uses. The gateway's own `ShardRef` stays a separate type from the log's
  (it carries the routing helpers), with a `From` conversion.
- **Durability is shown by crashing the simulated disk.** Acknowledged
  objects, inline and in extents, survive `SimDisk::crash` and a replay;
  a PUT whose sync fails answers `503`, as does every later one on the
  out-of-service disk.
- **s3s answers some things differently from S3.** It refuses a `Range`
  header it cannot parse as one range with `400`, where S3 serves the
  whole object; the pipeline drops such a header before `s3s` sees it
  (design §11), so no Range parser and no fuzz target were added. Its
  HeadObject is always `200`, so the pipeline turns a ranged HEAD's answer
  into `206`. It has no `InvalidPartNumber` code; the gateway builds one
  with status `416`.
- **Local tombstones.** M1-04 left tombstones of `local` buckets forever.
  A DELETE in a local bucket is followed by a `FLUSHED` of its tombstone
  (no remote ETag), which removes it unless a later write came first. It
  is not awaited; a crash can leave the tombstone, which hides nothing
  (design §4.2).
- **The write identity is refused on input.** The design only said it is
  stripped from responses. Accepting `x-amz-meta-skys3-wid` from a client
  would let it forge an identity the 412 recovery rule trusts, so a PUT
  carrying it gets `400 InvalidArgument` (design §7.2). User metadata is
  counted as S3 does, names without the `x-amz-meta-` prefix plus values.
- **Hashing without a pool.** `ChecksumValidator::new` requires a pool;
  `GatewayConfig` gained an optional `hashing_pool` (and
  `inline_max_bytes`, `extent_bytes` from `[storage]`). Without one, the
  gateway hashes on the request's task, which only tests do; the node
  (M1-13) must set it.
- **A conditional PUT is checked twice.** Once before its body is read,
  so a write bound to fail does not upload, say, 5 GiB of extents first,
  and again when the shard sequences it, which is the check that counts.
- **Fuzzing got slower.** `gateway_request` now reaches PutObject,
  GetObject, and DeleteObject over real shards. A fresh simulated disk,
  index, and pool per input cost about 1.5 ms natively, six times the
  rest of the pipeline, so the harness shares a disk among 256 inputs,
  gives each input fresh shards (a new bucket ID), and removes them
  afterwards. A 60-second run executed about 13,700 inputs (about 230 per
  second under the sanitizers) with no failure.
- **Left open.** Tags on upload (`x-amz-tagging`) answer `501` until
  M1-10. An entry without local bytes (an imported or evicted stub)
  answers GET with `503` until read-through fill (M1-20). The extents of
  a failed upload stay in bulk segments until compaction (M1-22) reclaims
  unreferenced ones. `x-amz-storage-class` is accepted and ignored.
- **Response header overrides.** `s3s` applies GetObject's `response-*`
  query parameters to the response itself, but parses HeadObject's and
  never applies them, and neither refuses them on anonymous requests. The
  gateway applies them to both outputs and refuses them, and values that
  are not header values, with `400 InvalidRequest` before reading the
  object.

### M1-10 DeleteObjects, tagging, and CopyObject

- **No format change was needed.** M1-01 had already defined `TAGS`, a
  `PUT` with tags, and a `PUT`'s copy source (bucket ID, key, `seq` and
  ETag of the version, optional `remote_etag`), and M1-04 applies `TAGS`.
  The log stays at version 2 and index values at format 2. The open choice
  was where tags given at upload go: they are part of the `PUT` record, so
  an object and its tags commit as one record under one write identity,
  rather than a `PUT` followed by a `TAGS` that a crash could separate.
  Design §10.1 records it. A `TAGS` is committed with an "object exists"
  precondition, so a tagging request racing a delete answers `404` instead
  of committing a record the state machine silently rejects.
- **The authorizer checked one resource per request.** DeleteObjects names
  a bucket in its path but needs `s3:DeleteObject` on each key of its body,
  and S3 answers a key the caller may not delete with an `AccessDenied`
  entry while deleting the others. `s3s` calls a per-operation access hook
  after parsing the input, so the request-level check lets DeleteObjects
  through (`authz::is_per_key`), and its hook stores a decision per key
  in a `KeyDecisions` request extension. The operation fails closed
  without one, and the gateway strips one that arrives with a request.
  CopyObject's `copy_object` hook checks `s3:GetObject` on the source
  (`authz::source_actions`). The authorization table gained a source
  column, a caller denied only the source, and, for key-by-key operations,
  a denied answer of `200` with an `AccessDenied` entry.
- **XML bodies were never checked against their digest.** `s3s` does not
  verify `Content-MD5`, and the gateway buffered XML bodies without
  checking them. It now checks every XML body that carries `Content-MD5`
  or an `x-amz-checksum-*` value before `s3s` parses it, with the same
  validator as object bodies, and DeleteObjects requires one, as S3 does
  (the AWS SDK for Rust sends `x-amz-checksum-crc32`). The answer to a
  DeleteObjects without one is `400 InvalidRequest` with S3's message;
  the S3 documentation was unreachable from the sandbox (as in M1-08), so
  the SDK matrix (M1-25) should confirm the code against S3.
- **A copy keeps its source's ETag rather than hashing its bytes again,
  unless the source is a multipart object.** Keeping the ETag is S3's
  result for an object stored by a single PUT. A copy of a multipart
  object (M1-12, merged into this branch) would otherwise carry a
  multipart ETag and composite checksums without part boundaries, which a
  flush by PutObject (M1-16) cannot reproduce, and keeping the source's
  parts would tie the copy to another key's upload. So a copy is always a
  single-part object: of a multipart source, its ETag is the MD5 of its
  bytes and its checksums are full-object checksums of the source's
  algorithms (CRC64NVME if it had none), all computed on the hashing pool
  while the bytes are copied. `partNumber=1` serves the whole copy, with no
  `x-amz-mp-parts-count`, and other part numbers answer `416`. S3 may keep
  a multipart source's ETag for some copies; the SDK matrix (M1-25) should
  compare. With `x-amz-checksum-algorithm`, the copy stores that
  algorithm's checksum, reused from the source only when it is a
  full-object one. Design §11 records it.
- **Writes could store and copy tags without the tagging actions.**
  Review found that PutObject and CreateMultipartUpload with
  `x-amz-tagging`, and CopyObject, needed only `s3:PutObject`, so a caller
  denied `s3:PutObjectTagging` could still set tags, and one denied
  `s3:GetObjectTagging` could copy a source's tags. As in S3, a write that
  gives tags now also needs `s3:PutObjectTagging`, checked in the
  operation's access hook, and a copy of a tagged source with the `COPY`
  tagging directive also needs `s3:GetObjectTagging` on the source and
  `s3:PutObjectTagging` on the copy. Whether the source has tags is known
  only once the operation reads it, so the hook leaves its decision in a
  `CopiedTags` extension, stripped from incoming requests like
  `KeyDecisions`; the copy fails closed without it. An empty
  `x-amz-tagging` still needs the action, and copies of untagged sources
  need neither. The S3 documentation was unreachable from the sandbox; the
  rule follows the AWS guidance that copying tagged objects needs both
  tagging actions, and the SDK matrix (M1-25) should confirm it. The
  authorization table gained rows for tagged writes and copies, and a test
  checks that callers denied the tagging actions still write untagged
  objects.
- **A self-copy that only set a storage class or website redirect was
  accepted and lost.** S3 counts either as a change, but SkyS3 stores
  neither (PutObject accepts and ignores them, M1-09), so such a copy
  answered success and changed nothing. It is now refused like any copy
  onto itself that does not replace the metadata.
- **Imported and adopted objects can carry a write identity.** Their
  metadata comes from the remote, so a `COPY` directive would have copied
  `x-amz-meta-skys3-wid` into the copy, naming another write. Copies drop
  it.
- **The fuzz harness could not reach DeleteObjects.** Its structured mode
  sends a fixed set of XML operations, which now include a working
  DeleteObjects; without a digest every input was refused before parsing.
  The harness adds the body's `Content-MD5` to every structured request.
- **CompleteMultipartUpload's checksum headers are not body digests.**
  Merging M1-12 showed that the XML digest check read its
  `x-amz-checksum-*` headers, which give the completed object's checksum,
  as digests of the XML body, and refused every completion that sent one.
  For CompleteMultipartUpload only `Content-MD5` is checked against the
  body.
- **Tags on CreateMultipartUpload.** M1-12 answered `501` to
  `x-amz-tagging` on create because tags did not exist yet. `MPU_CREATE`
  already carried tags, so create now parses them with PutObject's limits
  and the completed object takes them; no format change. Tagging a
  multipart object keeps its parts (GET with `partNumber` still serves
  them), and DeleteObjects of one releases its parts, as DeleteObject
  does; tests cover both.
  A new target, `gateway_tagging`, parses `x-amz-tagging` headers and
  checks that accepted sets keep S3's limits and round-trip through the
  header and PutObjectTagging forms. A 30-second run executed about
  670,000 inputs, and a 60-second run of `gateway_request` about 3,000,
  with no failure.
- **Left open.** How a `TAGS` flushes is M1-16's (plan section 14). A
  copy reads its source's payload by position after reading the entry, as
  a GET does; compaction (M1-22) must not reclaim payload a read in
  progress still uses after an overwrite. UploadPartCopy, with copy-source
  ranges, is M4-05's.

### M1-11 Listing

- **A page resumes after an item, not a key.** V1's `NextMarker` can be
  a common prefix, and so can the last item of a V2 page. Resuming after
  it as a key would list the prefix again, from its keys that sort after
  it. The shard page therefore compares items: a common prefix at or
  before `start_after` is skipped whole, even where some of its keys sort
  after it. The design said only that the token holds the last key;
  §9.4 now defines items and this rule.
- **The index paged by key.** `IndexReader::entries` reads every key,
  tombstones included, and a delimiter listing would have read all of a
  common prefix's keys to emit it once. `IndexReader::list` (with
  `ListQuery`, `ListPage`, `ListItem`) seeks past each common prefix once
  it has a live key, so a page costs about one redb range read per item.
  A common prefix whose keys are all tombstones is not listed. The page
  carries each object's `ObjectVersion`, boxed, which clippy asks for.
- **Asking every shard for a whole page would cost shards × `max-keys`.**
  The merge asks each shard for about its share (at least 16 items), and
  asks a shard for more only when its page is used up before the merged
  page is full; it never takes an item while such a shard is unasked, so
  the result is the same as asking for everything. The proptest runs
  batches down to one item so that refills happen; a mutation that refills
  only once every shard is used up fails it. The fan-out uses a
  `JoinSet`, since the workspace has no `futures` crate; a panicking shard
  task is resumed in the request.
- **Where the token key lives (design gap, §9.4).** `GatewayConfig` holds
  `ListTokenKeys`: a signing key and older keys that still verify.
  `GatewayConfig::new` generates a random key, which is what a single node
  (M1-13) uses: its tokens die with a restart, and the client gets
  `400 InvalidArgument` and starts over. With several nodes (M2), any
  gateway must accept another's tokens, so §9.4 records that the ring is
  then read from shared secret files named in configuration, with no
  configuration key added yet (no node serves the S3 API until M1-13).
  The key is not in the control store, which holds no secrets. Tokens are
  bound to the bucket ID, prefix, and delimiter, and carry a version byte
  for richer state later. The HMAC is `aws-lc-rs`'s, as SigV4 uses.
- **s3s echoes `encoding-type` but encodes nothing.** The gateway encodes
  keys, prefixes, delimiters, and markers itself, as form values with `/`
  kept, which is what S3 sends and what the SDKs decode.
- **Objects need an owner, and SkyS3 has no accounts.** V1 always returns
  `Owner`. Every object is owned by the bucket owner (bucket-owner-
  enforced), and the owner's ID is the cluster ID (§11).
- **Fuzzing.** `gateway_list_token` opens arbitrary tokens, and checks
  that each item round-trips through its own token and that a token
  changed in one character is refused. A 30-second run executed about
  850,000 inputs with no failure.
- **Left open.** During a `write_back` import the listing must merge the
  remote's listing (§9.1); the import task adds that. The configuration
  key for shared token keys comes with multi-node gateways.

### M1-12 Multipart upload, local

- **The upload ID is the `MPU_CREATE` position.** Formatted as 32 hex
  digits, it is unique within the shard, needs no generator, and is the
  write identity the completed object carries (`write_identity =
  Some(upload)`), so M1-16b can hand it to the remote
  `CreateMultipartUpload` unchanged. An ID that does not parse answers
  `404 NoSuchUpload`, never `400`. AbortMultipartUpload looks the upload up
  before committing, so an ID that never named an upload stays out of the
  log.
- **Versions moved in three places, not four.** Defining reserved record
  kinds needs no new log version (design §10.1), so the log stays at
  format 2. The index value format went to 3 (a `Parts` entry payload,
  and the upload and part values, which exist only in format 3); readers
  still take formats 1 and 2. The redb database got a format version bump
  of its own, 1 to 2, for the `uploads` and `parts` tables; opening a
  format 1 index creates them and rewrites the version, which
  `tests/uploads.rs` exercises.
- **Parts live beside the entry, not in it.** A completed object's entry
  holds only part numbers and sizes (`Payload::Parts`); each part's
  payload stays in the `parts` table under the upload. A GET reads the
  part rows it needs in one call before streaming, flattens them into the
  extent list M1-09's streaming already serves (an inline part becomes one
  "extent" covering its record), and answers `503` if the rows no longer
  match the entry. For a 10,000-part object the rows are read in one go;
  paging them is left for when it matters.
- **Releasing is removing location-map rows.** An aborted upload's parts,
  a replaced part, the parts a completion leaves out, and a part refused
  because its upload is gone all leave the location map when the
  orphaning record applies. Shard tests check that their positions no
  longer locate. Replaced object versions are not released, since a read
  of the old version may still be streaming; M1-22 must drop released
  records without a reference check and find the rest by reference
  (design §10.3).
- **The shard guards completion, not just the gateway.** The gateway
  checks the listed parts, then commits an `MPU_COMPLETE` that names each
  part's `MPU_PART` position. The state machine rejects it
  (`PartChanged`, `NoSuchUpload`) if a part was uploaded again or the
  upload ended meanwhile, and `LocalShards` maps those to `400
  InvalidPart` and `404 NoSuchUpload`. `MPU_CREATE`, `MPU_PART`, and
  `MPU_ABORT` do not take the key's conditional-write slot, so they never
  wait behind a conditional PUT; `MPU_COMPLETE` does.
- **`partNumber` counts kept parts.** On a multipart object `partNumber=N`
  serves the Nth part of the completed object, numbering from 1 in order,
  even if the parts were uploaded as 1, 3, and 7. This is what the stored
  form (numbers and sizes in order) gives directly; if S3 turns out to
  use the uploaded numbers, the entry already keeps them.
- **Checksums follow the upload.** Each part computes the upload's
  algorithm, or CRC64NVME, through `ChecksumValidator::requiring`, so
  completion can always fold a `MultipartChecksum`. A part that supplies a
  different algorithm than the upload's is refused with S3's "Checksum
  Type mismatch" `400 InvalidRequest`; with no upload algorithm any
  supplied value is checked and kept beside the CRC64NVME. Completion
  checks a part's checksum only when the request lists one, which the
  SDKs do for uploads created with an algorithm.
- **Known vectors.** The s3-tests three-part upload (5 MiB of `A`, `B`,
  and `C`) completes with S3's ETag `b2add96cc9702bbf4efb0ccdfc6b7747-3`
  and composite SHA256 `uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3`
  through the HTTP pipeline; the Rust SDK drives every operation over a
  real socket in `sigv4_http.rs`.
- **s3s details.** It serializes XML fields alphabetically, so
  `CommonPrefixes` precede the top-level `Prefix`, and it omits an empty
  `Prefix`. It does not escape quotes in ETags. `encoding-type` on
  ListMultipartUploads is ignored and not echoed.
- **Listings after M1-11.** Open uploads live outside the namespace
  table, so ListObjects never sees them, and a completed object lists
  with its multipart ETag and size like any other entry; a gateway test
  checks both. ListMultipartUploads keeps its own merge rather than M1-11's
  `listing::merge`, which is typed to `ListQuery` and `ListItem` and pages
  by item rather than by (key, upload).
- **Review fixes.** ListParts first checked that the upload was open
  and then read its parts in a second index read, so an abort between the
  two answered `200` with no parts, which no serialization point
  produces. `Shards::upload` now returns the upload with a page of its
  parts from one read transaction, and CompleteMultipartUpload uses the
  same read. Completion compared only the digest of a supplied
  whole-object checksum, so a composite value with the wrong `-N`, or
  none, passed; it now compares the parsed value, part count included,
  and so does the per-part check. `max-parts=0` and `max-uploads=0`
  answered a truncated page with no marker, which cannot be continued;
  they now answer an empty final page, as the simulator models S3, and
  ListParts names `NextPartNumberMarker` only on truncated pages.
- **Test helper limit.** The shared `answer` helper read at most 1 MiB of
  body; multipart objects are at least 5 MiB, so it now reads up to
  64 MiB.
- **Left open.** UploadPartCopy is M4-05, and cleanup of abandoned
  uploads M5-10. Eviction (M1-21) must drop part rows along with the
  entry it turns into a stub. `x-amz-if-match-initiated-time` on abort
  answers `501`.

### M1-13 Node binary and startup recovery

- **The configuration had nowhere to put a node.** Design §14 named no
  node ID, data directory, disks, S3 listen address, or TLS files. New
  `[node]` (`node_id`, `data_dir`, `disks`) and `[gateway]` (`listen`,
  `tls_cert_file`, `tls_key_file`) sections hold them, and
  `[control_store]` gained `backend = "file"` with `directory`, as M1-05
  asked. `node_id` is optional: a node generates one when it creates its
  data directory and keeps it in `node.json`, so a single-node setup needs
  no ID while a configured ID is still checked against the directory. The
  binary runs only the file backend; the etcd and S3 backends are refused
  until M2-04 and M2-05 land (plan rule 1.1).
- **Shards on several disks.** `ShardSet` used one disk's log, and the
  location map names no disk (M1-03 left the choice here). Adding the disk
  to the location map would have changed the index format; instead each
  shard replica keeps its records on the disk its hash picks among the
  disks in label order (`ShardSet::with_disks`, `disk_of`). That is stable
  only while the node keeps its disks, so `node.json` records each disk's
  label and path, each disk's `disk.json` names its cluster, node, and
  label, and a node refuses to start with a disk added, missing, swapped,
  or another node's. Reordering `disks` in the file is harmless. Design
  §10.2 records the rule.
- **Disks fenced until the host restarts.** M1-02 asked startup to keep a
  disk that failed a sync out of service until the host restarts. A task
  checks each log's failure every second and writes
  `out-of-service.json` with the boot ID; startup refuses the disk while
  the boot ID is unchanged and lifts the fence after a reboot. A process
  that dies within that second of the failure, before the fence is
  written, can still reuse the disk in the same boot; hooking the fence
  into the group committer would close that gap.
- **The kept control copy must not look fresh.** The node keeps
  `buckets/` and `identity/` in the index (design §6.2) and serves the
  gateway and STS from it while the control store does not answer
  (`NodeStore`). `IdentityCopy::sync` stamps the copy with the current
  time, so restoring it after a restart would have restarted the
  `identity_max_staleness` clock and let a stale copy issue sessions.
  `IdentityCopy::sync_as_of` and `StsEndpoint::sync_identity_as_of` take
  the kept sync time instead. The index gained `ControlWriter::clear` and
  `set_synced_at`, and `IndexReader::control_entries` and
  `control_synced_at`.
- **Polling left the copy behind bucket creation.** The first version
  refreshed the copy every `config_poll_interval`. A test that created a
  bucket, restarted at once, and found the control store unreadable got
  `NoSuchBucket`: the copy predated the bucket. The node now follows the
  store's change stream, which the file store wakes on every write, with
  the poll interval as a backstop, and refreshes the copy once more at a
  graceful shutdown. A crash right after a creation can still leave the
  copy behind; the next start syncs from the store if it answers.
- **Orphaned shards are reclaimed only from a live read.** Startup drops
  shards whose bucket ID no register names (M1-04, M1-06). Deciding that
  from the local copy could drop a bucket created after the copy was
  taken, so a node that starts from its copy reclaims nothing (design
  §4.1).
- **The file store did not know whose it was.** Review found three gaps.
  `FileControlStore::open` creates a missing directory, so a node whose
  control volume was not mounted bootstrapped an empty store, synced no
  buckets, and reclaimed every shard. The store refuses a second node only
  through `nodes/` registrations, which nothing writes yet, so a second
  node pointed at the directory loaded the first node's catalog over
  empty shards. And an error opening the store stopped startup before the
  kept copy was consulted. `control::open_file_store` now checks the
  directory before opening it and keeps an owner record, `.owner.json`
  (cluster, node ID, and a random `instance_id` that `node.json` gained),
  beside the store's lock file; the store's loader skips dot files. A node
  that has a copy treats a missing directory, a missing owner record, or
  a missing `cluster.json` as a reset store: it never bootstraps it, runs
  from the copy, and retries; a node without a copy claims only an empty
  store. Another owner stops startup (`StartError::ControlStore`). An
  open that fails otherwise runs from the copy too: `NodeStore` may hold
  no store, and the follow task reopens it every `config_poll_interval`.
  Orphans are reclaimed only when the start read a store it did not
  claim. The owner record lives in this crate rather than in
  `skys3-control`, because whether to claim depends on the node's copy.
- **The index outlived the node in one process.** Restarting a node
  in-process failed with redb's `DatabaseAlreadyOpen`: each shard's
  pipeline task holds the index until it notices, on another task, that
  its shard was dropped. Shutdown, and a failed start, now wait for the
  index's last handle before releasing the data directory's lock, so a
  second process never sees the directory free while the index is open.
- **Sessions moved to the system bucket.** M1-24 left sessions in memory.
  `SystemSessions` stores each as a `PUT` in `sys-sessions`, a one-shard
  `local` bucket with no register (generated IDs start with `b-`, so it
  cannot collide), with the expiry in the record's metadata so a sweep
  every five minutes finds expired sessions from the index alone. A record
  larger than `inline_max_bytes` goes in one extent, since that may be 0.
- **The admin listener had no hook for an API.** Its handlers were
  synchronous. `skys3_obs::AdminApi` is an asynchronous trait the node
  supplies (`AdminListener::with_api`); paths under `/v1/` go to it behind
  the token and are counted as `endpoint="api"`. Design §12 records the
  surface (plan section 14).
- **TLS on the gateway listener.** `GatewayListener::with_tls` takes a
  `rustls` server configuration built on `aws-lc-rs`; the handshake runs
  on the connection's task under the request-head timeout. The tests reuse
  the 100-year test CA and server certificate from `skys3-sts`, copied
  into `crates/skys3/tests/data/`.
- **Signals in tests without `unsafe`.** Sending `SIGTERM` needs
  `libc::kill`, which `forbid(unsafe_code)` rules out, so the end-to-end
  test runs `kill -TERM <pid>`; `Child::kill` sends `SIGKILL`.
- **Left open.** Nodes do not register under `nodes/` yet:
  `NodeRegistration` needs an intra-cluster address, which arrives with
  the transport (M2-02) and the node registry (M3-02). Bucket status scans
  each open shard's index entries per request, which is fine for M1 and
  should move to counters once the flusher (M1-16) keeps them. Reclaimed
  shards' segment summaries still hold their log segments until
  compaction (M1-22), as M1-03 noted. The admin listener still serves
  plain HTTP (M2-02).

### M1-14 Crash-consistency suite

- **The first sync boundaries found a node that could never start
  again.** redb creates a database in two synced steps, the header and
  then its magic number, and `Database::create` refuses a non-empty file
  without the magic number ("Not a redb database") forever. A power loss
  just after the index's first sync, or just before its second, left the
  simulated node unable to start: the scenario failed on its second run.
  The real node had the same gap through `Index::open`. Both now treat a
  file whose first bytes are each zero or the magic number's own byte, but
  not the whole magic number, as a creation cut short, truncate it, and
  create it again. Every later header write repeats the whole magic number,
  so a used index never looks like that; anything else is still refused.
  Design §10.2 records the rule, and `skys3-index` tests cover both
  backends.
- **Sync boundaries needed a power supply.** `SimDisk` could crash, but
  nothing numbered syncs or crashed at one, and crashing one disk while the
  node's other disk ran on would not be a power loss. `skys3-io` gained
  `SimPower`, which a node's disks share: it numbers every data, directory,
  and block-file sync of its disks in order, and `cut_at_sync` crashes all
  of them inside the planned sync, before or after it takes effect, while
  the syncing disk is still locked, so nothing reaches stable storage after
  the boundary. The sync then fails like any I/O on a crashed disk. The
  host itself is crashed by the driver at the next simulation step; until
  then its I/O fails on stale handles. A first run of a seed counts the
  node's syncs (`Report::syncs`), and the scenario replays the seed once
  per boundary and side (`Cluster::power_loss_at_sync`), relying on exact
  replay, which `a_seed_replays_exactly` now checks with a `write_back`
  bucket and remote faults too.
- **The checkers let a resurfaced write pass.** M2-03 keeps a write
  without an answer open forever, since a held link may deliver it late.
  With that rule a write cut off by a crash could resurface over a later
  acknowledged write, and both checkers explained it by placing the old
  write last: the plan's second rule (§5.2) was not enforced. The history
  now knows which server each operation reached (`History::call_to`), and
  `History::crashed` ends the unanswered ones as failed at the crash; an
  answer the dead process sent earlier still counts. That is only sound
  if no later life can receive the request, so the cluster workload now
  records an operation once the node has accepted its connection: a SYN
  held across a crash reaches the next life, but data on an accepted
  connection reaches only the life that accepted it. Requests that never
  connected are no longer recorded at all, since they reached no node.
  With that, `check_durable` enforces both rules unchanged; unit tests show
  the same resurfacing history failing with the crash and passing without.
- **"Flushed or still dirty".** `Survivors` gained `clean`: in a
  `write_back` bucket a copy whose entry is clean, or that has no entry,
  claims the remote holds its value, so only the remote counts for it. The
  harness reads `flushed` from the remote store. `check_durable` needed no
  other change.
- **The flusher in the harness.** Every simulated node runs a
  `FlushService` over its shards, as the binary does, against the
  harness's remote `SimS3`, with each bucket under its own prefix. The
  binary re-reads its buckets every second; the harness does it every
  200 ms, because the capability probe (about twenty requests) and a
  second pass must finish within a workload of a few seconds before any
  flush starts. With remote faults the probe often retried past the end of
  such a short run, so the sync-boundary scenario uses a fault-free remote
  and the random-fault scenario adds a `write_back` bucket with remote
  faults. The probe's scratch-key nonce comes from the system clock, which
  a seed cannot replay; it changes key names only, and replay stays exact.
- **The scenario has teeth.** Shards that acknowledge an unconditional PUT
  before its record is durable, and make every other request wait for such
  writes, pass a run without a crash and fail at some sync boundary, as a
  test checks.
- **Cost.** A single-node run takes about 0.1 s in a debug build, and a
  seed has about 40 syncs, so a seed costs about 80 runs and 15 s. The
  scenario declares a cost of 64: one seed in a plain `cargo test`, four in
  CI's simulation job, and sixteen at four times the length nightly.
- **`kill -9` loops.** `crates/skys3/tests/kill.rs` runs four AWS SDK
  clients (retries disabled, every wait bounded) against the binary,
  kills it with `SIGKILL` at a random moment, waits for every client to
  stop, and restarts it; the clients stop before the next life starts, so
  ending their pending operations at the kill is exact. CI runs 3 rounds
  with seed 0; the new nightly `kill-loops` job runs 200 in release mode
  with a random `SKYS3_KILL_SEED`. 25 rounds in a debug build took 31 s,
  with about 3,700 operations of which 32 writes were cut off by a kill.
  The process helpers of `binary.rs` moved to `tests/support/process.rs`.
- **Multipart objects are flushed now.** With M1-16b merged, the
  harness's flusher sends completed multipart uploads to the remote store
  instead of parking them, so the `write_back` checks also cover them: the
  remote ETag of a flushed upload is its multipart ETag, which is the value
  the history records. The harness needed no change for it.
- **Left open.** The kill loop has only a `local` bucket: no S3 server in
  the tests keeps `x-amz-meta-skys3-wid` and honors preconditions (M1-16's
  note), so the binary's flush path stays covered by the simulation only.
  Each sync-boundary run cuts the power once; a second cut during the
  recovery that follows is not enumerated, though recovery's own syncs are
  enumerated at first start and the random-fault scenario crashes nodes
  repeatedly. Replicated sync boundaries come with M2-07.

### M1-15 Remote target client and capability probe

- **The SDK's default features pull in two TLS stacks.** `aws-sdk-s3`'s
  defaults enable both the legacy `rustls` client (hyper 0.14, rustls 0.21,
  ring) and `default-https-client` (hyper 1, rustls 0.23, aws-lc-rs), plus
  `sigv4a`. Every AWS crate is used with `default-features = false`, and the
  HTTP client is built explicitly from `aws-smithy-http-client` with
  `rustls-aws-lc`, on the workspace's hyper 1. The first version used the
  `ring` provider to avoid aws-lc's C build, which needed `deny.toml` skips
  for `getrandom` 0.2 and `windows-sys` 0.52; it switched to aws-lc-rs so
  the binary has one crypto stack with the OIDC validation of M1-23, whose
  `getrandom@0.4` and `r-efi@6` skips it shares. The SDK adds 144 crates to
  the lock file; a clean debug build of `skys3-remote` takes about a minute.
- **Turning off aws-config's defaults drops `credential_process`.** The
  feature is `credentials-process`, and without it the default chain still
  builds; a profile that uses `credential_process` fails only when
  credentials are loaded, with an `InvalidConfiguration` error. The
  feature is on (it adds only tokio's `process` feature), and a unit test
  loads credentials from such a profile. `sso` and `credentials-login`
  stay off: they pull in the SSO and sign-in SDK clients.
- **`digest` 0.10 through the SDK's checksums.** `aws-smithy-checksums`
  uses `crc-fast`, whose default `std` feature needs `digest` 0.10, while
  everything else is on 0.11. It cannot be turned off from outside, so
  `deny.toml` skips `digest@0.10` and `crypto-common@0.1`.
- **SDK retries would break the store contract.** The SDK retries 5xx,
  throttling, and timeouts three times by default, so a caller could not
  see that a write may have been applied, nor control backoff. Retries are
  disabled; callers retry by write identity (design §15 now says so).
- **Flexible checksums break S3-compatible providers.** Recent SDKs send a
  CRC32 trailer with aws-chunked encoding on every `PutObject` and
  `UploadPart` by default, which several S3-compatible stores reject. The
  client asks for checksums only where S3 requires them. Bodies stay
  covered: SigV4 signs the payload's SHA-256, which a test checks.
- **Error responses lose information.** The SDK synthesizes an error code
  only for a `404` to `HEAD`; a `304` or `412` to `HEAD`, and a proxy's HTML
  `502`, arrive without one, so the client classifies them by status. M0-05's
  `S3Error` had only a kind, so a `403 AccessDenied` or `404 NoSuchBucket`
  became `Other`, which counts as possibly applied. `S3Error` now keeps the
  status and code, and `S3Error::is_transient` and `may_have_applied` use
  them (an `Other` with a 4xx status was not applied). A request the SDK
  could not build is the new kind `NotSent`. Connection failures, timeouts,
  and credential failures (which the SDK reports as dispatch failures) are
  `Timeout`, because the client cannot tell whether the request was sent.
- **SDK quirks.** `x-amz-copy-source` is sent as given, so the client
  percent-encodes the source key itself. Every request carries an
  `x-id=<Operation>` query parameter. `DefaultCredentialsChain` without an
  explicit region looks one up, from instance metadata if need be, and uses
  it over the configured one, so the region is passed explicitly.
- **The probe tests each header, not each operation.** AWS added
  `If-None-Match` on writes months before `If-Match`, so a provider can
  honor one and not the other. The probe tests both headers of `PutObject`
  and `CompleteMultipartUpload`, each with a failing and a holding
  precondition (a store that answers `412` to everything is "rejected", not
  "honored"), and an operation is protected only if all its headers are.
  Design §7.2 records the procedure and the scratch prefix.
- **Mock server instead of the SDK's replay client.** Request construction
  and error mapping are tested against a local hyper server rather than
  `StaticReplayClient`, whose `test-util` feature adds several crates and
  bypasses the real HTTP client, so timeouts and refused connections could
  not be tested. No test reaches the network.
- **Left for the attach path.** The constructor takes the region and an
  addressing override, but §14 has no key for a target's region, and S3-
  compatible providers need one (`auto` for R2). The PR that wires attach
  into the binary adds the keys. Scratch keys that a failed cleanup leaves
  behind are ordinary objects to a later import (M1-18), which may want to
  skip `.skys3-probe/`.

### M1-16 Flusher

- **"Never triggers an fsync of its own" needed a log primitive.** The
  group committer started a group with whatever arrived first, so a
  `FLUSHED` alone cost a sync. `SegmentLog::append_lazy` queues a record
  that waits for the next group another record starts, in queue order, and
  `Shard::commit_lazy` uses it. A disk with nothing else to write would
  keep such records forever, holding entries dirty and delaying the seal of
  a DeleteBucket, so a lazy record commits on its own after
  `LAZY_MAX_DELAY` (1 s): at most one extra sync per second on an idle
  disk, none on a busy one. Design §7.1 says so. The flusher does not wait
  for the record: its slot is free once the remote answered, and the next
  version of the key is conditioned on the ETag kept in memory until the
  record is applied. Waiting would have drained a backlog after an outage
  at one batch of `FLUSHED` records per second.
- **A lazy `FLUSHED` must not stall conditional writes.** `commit_if` waits
  for every unapplied record of its key, so a conditional PUT right after a
  flush would have waited up to a second. A `FLUSHED` changes the entry's
  state and remote fields but never its object version, which is all a
  conditional request checks, so `FLUSHED` records are no longer tracked
  for it.
- **Flushing and Conflict stay out of the index.** M1-04 asked whether the
  flusher would make them replayable. They have no record kind, so they
  are the primary flusher's in-memory view; the index shows such entries
  as dirty. A restarted flusher flushes them again, which is conditional:
  it finds a held conflict again (counted again in
  `skys3_flush_conflicts_total`) and an earlier success by its identity.
  Design §4.2 records it. Nothing resolves a held conflict yet except
  writing it out of band again; the `overwrite` and `discard_local`
  policies are plan M4, and the node logs a warning and holds when a bucket
  is configured with either.
- **The 412 rule as written turned crashes into false conflicts.** After a
  `FLUSHED` is lost (a crash or a flusher restart) while a newer version
  committed, the next flush is conditioned on stale knowledge, gets 412,
  and the HEAD finds an identity, but an older one of the same shard, not
  the one being flushed: "anything else" made it a conflict, and the hold
  policy would never flush the acknowledged newer write. The flusher now
  supersedes an object whose identity names an earlier position of the
  same shard, conditioned on its ETag; bucket IDs are never reused, so no
  other writer carries such an identity. Likewise a PUT whose HEAD finds no
  object (the object it was conditioned on was deleted, perhaps by its own
  delete whose answer was lost) creates the version with
  `If-None-Match: *`, since nothing can be overwritten, and a tombstone
  without a known remote ETag HEADs first, because an earlier flush may
  have landed without its record. The simulation found the first case at
  once when the rule was taken out (seed 4); design §7.2 records all
  three.
- **Tag-only changes re-PUT the object (plan section 14).**
  `PutObjectTagging` takes no precondition and would give the version no
  write identity, so a `TAGS` version is flushed like any version: a
  conditional `PutObject` of the bytes with the new tags and the `TAGS`
  record's identity. It needed no code of its own in the flusher, so M1-10
  merges without touching it. `PutObject` in `skys3-remote` gained `tags`
  (`x-amz-tagging`), the standard headers other than `Content-Type`, and
  `Content-MD5`; the simulator keeps tags (`SimS3::tags`) and checks
  `Content-MD5`.
- **Checksums are not forwarded; `Content-MD5` is (M1-08's question).**
  The stored `x-amz-checksum-*` values would break providers that reject
  flexible checksums (M1-15), so a single PUT carries `Content-MD5`, which
  its MD5 ETag already is and every provider verifies. A copy carries none,
  since its ETag need not be its bytes' MD5. Design §7.4 now says so.
- **Region and credentials for targets.** M1-15 left the region to the
  attach path. `[flush] target_region` (default `us-east-1`, `auto` for R2)
  is a node-wide key in §14 and the configuration reference; credentials
  come from the `aws-config` default chain. Each request has a 10-minute
  attempt timeout, so a hung connection cannot hold a flush slot forever.
- **Where the probe runs.** `FlushService` probes each `write_back`
  target when the node first sees the bucket, retrying with backoff, and
  starts the shard flushers only once it succeeds; the admin API shows
  `probe: running` with the last error meanwhile. The node follows its
  buckets and open shards every second rather than hooking bucket creation
  in the gateway.
- **Metrics and status.** `dirty_bytes`, `oldest_dirty_age_seconds`, and
  `flush_lag_seconds` are gauges by bucket name, set every second from the
  flushers' status; flush lag is the oldest dirty age without held
  conflicts, so it measures whether flushing keeps up, while the dirty age
  measures loss exposure. Conflicts are a gauge (`conflicted_keys`) and a
  counter. The admin API's bucket status gained a `flush` object that lists
  conflicts. The `objects` and `unflushed` counts still scan the index, as
  M1-13 left them: the flusher counts dirty keys, but objects need per-shard
  counters in the shard, which no task owns yet.
- **The binary's flush path is not exercised end to end.** No S3 server
  that honors preconditions and keeps `x-amz-meta-skys3-wid` runs in tests
  (SkyS3 itself refuses that header), so the node test checks only the
  wiring with an unreachable target. The flusher, service, and probe run
  against `SimS3`; M1-26 runs them against real providers.
- **The simulation checks itself.** `crates/skys3-flush/tests/simulation.rs`
  (300 seeds in about 40 s) fails within the first few seeds under each of
  three deliberate mutations: not recognizing the flush's own identity
  after a lost response, treating a foreign object as one to supersede,
  and not superseding the shard's own earlier writes. No fuzz target was
  added: the flusher parses nothing new (write identities go through the
  existing `WriteIdentity` parser).
- **Subscribing cannot miss a write being applied (PR review).** The
  apply pipeline used to take the shard's subscriber before applying a
  batch. A flusher that subscribed and scanned while the batch was applied
  then found the write neither in the index nor in its stream, and never
  flushed it. The pipeline now takes the subscriber after the index holds
  the batch, under the same lock that advances the applied position.
  - A subscriber that was there by then hears of the write.
  - A later subscriber's scan finds the write in the index.
  - A write may reach a subscriber both ways; the flusher ignores a
    version it already tracks.
  - The test `a_subscription_during_an_apply_hears_of_it` holds the
    index's write lock so that an apply stays in flight, then subscribes
    and scans. It fails without the fix.
- **Errors clear once their key gets past them (PR review).** The status
  kept the latest flush error forever, even after its key had flushed.
  The flusher now keeps each key's latest error. It drops that error when
  a later attempt flushes or settles the key, or finds it in conflict or
  awaiting multipart flush, and when the key is no longer tracked.
  `last_error` is the newest error still held, so one key's recovery does
  not hide another key that is still failing.
- **Multipart objects wait for M1-16b.** The flusher's branch merges M1-12.
  A completed multipart object (`Payload::Parts`) cannot be flushed as one
  `PutObject`: that would give it an MD5 ETag rather than the multipart
  ETag, and lose its part boundaries. Its attempt ends at once, without
  touching the remote, in a new `Phase::AwaitsMultipart`.
  - The key leaves the line and the flush lag. It stays in `dirty_bytes`
    and the oldest dirty age, because it is loss exposure.
  - It is not a conflict and is not retried. A newer version puts it back
    in line: an overwrite PUT or a DELETE flushes normally, conditioned on
    what the remote last had. A `TAGS` keeps the parts, so the key waits
    again.
  - The admin flush status lists such keys as `awaiting_multipart`, and
    the gauge `skys3_awaiting_multipart_flush_keys` counts them.
  - The shard now also reports `MPU_COMPLETE` to its subscriber. Without
    this, a running flusher would not see a multipart object written over a
    clean key until its next restart.
  - M1-16b replaces the early return in `Attempt::put` with a multipart
    flush.
- **Left open.** Multipart objects (M1-16b) and copies flushed as
  `CopyObject` (§7.2's copy row; a copy is flushed as a `PutObject` of its
  bytes, which is correct but transfers them) are not done. A version
  without local bytes (a `TAGS` on an imported or evicted stub) is retried
  with an error until read-through fill (M1-20) can supply them. The
  import (M1-18) plugs into `ImportProgress`, which today says every key is
  passed. Whole objects are read into memory for a PUT, bounded by
  `flush_max_inflight_bytes_per_target` per target; streaming arrives with
  M4-03. Concurrency is fixed at `flush_min_concurrency_per_shard` until
  M4-10. A conflict is not persisted, so after a restart it is
  re-detected rather than remembered.

### M1-16b Multipart flush after commit

- **The remote upload ID stays in memory.** Design §7.3 keeps remote
  upload IDs in the shard log, but that is for streaming (M4-02), where an
  upload outlives the request that opened it. A flush after commit opens,
  fills, and completes its upload within one attempt, so it logs nothing.
  A Complete whose answer is lost is resolved by aborting the upload:
  `404 NoSuchUpload` means it is gone, and a HEAD that finds the version's
  `MPU_CREATE` identity means the flush is done, without a retry that
  would upload every part again. When the abort fails too, the next
  attempt completes a new upload conditioned on the old ETag, gets `412`,
  and the HEAD finds the identity (§7.2); a test scripts both paths.
- **Abandoned remote uploads cannot be found by identity.** The plan
  suggested listing them. `ObjectStore` has no `ListMultipartUploads`, and
  S3's answer to it carries the key, upload ID, and initiation time but no
  user metadata, so it could not tell SkyS3's uploads from another
  writer's. The target instead keeps the uploads it knows are open: an
  abort that failed, and, through a drop guard, the upload of an attempt
  that was cancelled because its flusher stopped. The next multipart
  flush to the target aborts up to 16 of them first. What is lost with the
  node's memory, or was never known (a lost `CreateMultipartUpload`
  answer), is left to the abort-incomplete-uploads lifecycle rule of
  §7.3, as the design already says; M4-02's logged upload IDs cover the
  streaming case. M5-10 is cleanup for local buckets and does not apply.
- **The awaiting-multipart status was repurposed.** With multipart
  objects flushed, nothing waits. `Phase::AwaitsMultipart` and the
  admin field `awaiting_multipart` are gone. The admin field was added in
  M1-16 and never released, so removing it does not break the rule that
  admin fields are only added (§12). The gauge became
  `skys3_flush_orphaned_uploads`, the uploads the target holds to abort,
  and the admin flush status shows the same count as `orphaned_uploads`.
- **The remote model was missing fields.** `CreateMultipartUpload` had no
  standard headers or tags, and `UploadPart` no `Content-MD5`, so a
  multipart flush could not carry what a `PutObject` carries. Both gained
  them, in the AWS client and in `SimS3`, which also dropped the tags of
  a completed upload. Each part is sent with the `Content-MD5` its local
  part ETag already is. A `TAGS` change of a multipart object uploads its
  parts again as a multipart upload with the `TAGS` identity, so its ETag
  stays; §7.2's table says so.
- **The simulation rarely reached the 412 recovery.** The abort resolves
  most lost Complete answers at once, so treating a found
  `MPU_CREATE` identity as a conflict failed only at seed 295. The
  scenario now loses the answer to a third of its multipart completions,
  half of them with a failed abort as well, and that mutation fails at
  seed 9. To check aborts exactly, `SimS3::unanswered(operation)` counts
  successful requests whose answer never reached the caller (lost, or
  abandoned by a caller that stopped waiting); at the end, the open
  remote uploads must be exactly the unanswered `CreateMultipartUpload`
  requests. Not aborting after a failed part, and not handing a cancelled
  attempt's upload to the target, each fail at seed 0. 300 seeds take
  about 50 seconds.
- **Left open.** Parts are uploaded one at a time from the local log,
  each within the in-flight budget; concurrent parts and resuming a
  partly uploaded object belong to the streaming PRs (M4-02, M4-03). Some
  S3-compatible providers constrain parts beyond S3's 5 MiB minimum (R2
  documents that every part but the last must have the same size); a
  client upload that breaks such a rule cannot be reproduced there, and
  its flush is retried with the provider's error. The provider tests of
  M1-26 should cover it.

### M1-17 Dirty budget and admission control

- **Flushers waited for the capability probe.** M1-16's service started a
  bucket's shard flushers only once its target's probe succeeded. A node
  restarted during a remote outage then tracked no dirty bytes at all, so
  the budget would never have engaged, and `skys3_dirty_bytes` read 0. The
  flushers now start with their shards, scan and track at once, and begin
  flushing when the probe task publishes the target on a `watch` channel.
  A test writes during a failing probe and sees the bytes counted, then
  flushed once the probe succeeds.
- **Counting comes from the flushers, not the index.** The shard's own
  summary scans the whole index, too slow for every request. Each flusher
  already keeps its dirty bytes incrementally, so it moves a pair of atomic
  counters (its bucket's and the cluster's) with them, and gives its bytes
  back when it stops, since a new flusher of the shard scans and counts
  them again. Seeded scenarios with a faulty remote, outages, and flusher
  restarts check after every step that the budget equals what the flushers
  hold, and that a PUT is refused exactly when the budget says so.
- **Free space needs `statvfs`, which `std` lacks.** `libc` would need
  `unsafe`. `rustix` (already in the lock file through `tempfile`) has a
  safe `statvfs` behind its `fs` feature, so it became a direct dependency
  of `skys3-io` at the same version, adding no duplicate.
- **Two keys §14 did not name.** A per-bucket `max_dirty_bytes`
  (defaulting to `flush.max_dirty_bytes`) and `storage.disk_min_free_bytes`
  (default 1 GiB; 0 turns the check off) were added to §14 and the
  configuration reference. The free-space margin matters because, since
  M1-02, any write error takes a disk out of service until the host
  restarts.
- **Only `write_back` buckets have dirty bytes in M1.** A `local` bucket's
  writes are never flushed yet, so the budget does not limit them; they
  will count once flushing to a backup target (plan M5) exists.
- **Zero shares refused every write.** Review found that a `write_back`
  bucket with no open shard on the node got an account with a share of 0,
  and `dirty >= share` then refused all its writes, although nothing was
  dirty. Such a bucket now gets no account (its shards still count in the
  cluster's share), and a share that rounds down to 0 for a node that
  holds a shard is raised to one byte, so a write is admitted while
  nothing is dirty.
- **Rust 1.99 deprecated `fetch_update`.** It became stable on 2026-10-01
  while this PR was in review. CI tracks stable, so clippy with `-D
  warnings` failed on the budget's `fetch_update`. The fix was
  `try_update`, which MSRV 1.98.1 already has.

### M1-18 Namespace import

- **Writes made before the import reached their key ended in conflict.**
  M1-16's flusher HEADed a write of unknown remote state only while the
  import had not passed its key, deciding at flush time. A PUT committed
  before the import reached the key but flushed after the import passed it
  had no `remote_etag`, so it went out with `If-None-Match: *`, found the
  object the remote already held, and was held in conflict forever. The
  design had `IMPORT` dropped for every existing entry, so nothing ever
  told the entry what the remote held. An `IMPORT` now records the listed
  ETag on a dirty entry that has no `remote_etag` (design §4.2), and the
  flusher asks whether the import passed a key before it reads the entry.
  The new simulation scenario fails on its first seed with either change
  undone.
- **The import checkpoint lives in the index, not the log.** Plan M2-17
  says a new primary resumes import checkpoints "from the log", but §10.1
  has no record kind for them, and adding one changes the log format. A
  restarted import turned out to be safe from any position: every
  `IMPORT` is conditional, and the remote no longer holds what a flushed
  delete removed. So M1 keeps one checkpoint per bucket in a new `imports`
  table, synced after each page (index format 3), and M2-17 decides
  whether it moves into the log.
- **Lazily loaded metadata needed a marker, not a record kind.** An
  `ADOPT` naming the stub's `seq` already has the right semantics (dropped
  if a local write came first, adopts an out-of-band change). What it
  lacked was a way to tell a loaded entry from an unloaded one without a
  new entry field: every local write stores a checksum (the gateway
  computes CRC64NVME since M1-08), so "no checksums and no
  `Content-Type`" marks an unloaded stub, provided every `ADOPT` stores a
  `Content-Type`. The lazy load stores S3's default where the remote has
  none; M1-20's HEAD-based `ADOPT` must do the same once both are in the
  stack, or such objects reload on every read.
- **Tombstones waited up to 30 s after the import passed them.** The
  flusher retried a tombstone waiting for the import after the maximum
  backoff. It now backs off from the minimum, doubling, like any retry.
- **A retried capability probe misjudged the target.** The flush service
  built its probe, and so its scratch-key nonce, once and reused it for
  every retry. A run that failed under faults could leave its scratch
  keys behind, and the next run's `If-None-Match: *` that must hold then
  got `412`, so `PutObject` and `CompleteMultipartUpload` were reported
  unprotected and every flush went out unconditionally. The import
  scenario, whose remote is faulty from the start, found it on seed 0.
  Each run now takes a fresh nonce, and the scenario checks that the probe
  found every precondition honored.
- **The rate limit let single pages through at once.** Review found
  that the import waited only after a truncated page, so the first page
  committed at once and the last never waited: an import of one page
  ignored `import_max_keys_per_second`, and a rate of 1 still committed
  1,000 keys in a burst. A token bucket that starts empty and holds one
  second's keys now admits every page before it commits, and a page asks
  for at most a second's keys, so no stretch of `t` seconds commits more
  than `rate * (t + 1)` keys, even after slow requests.
- **A listing lost a common prefix the import was inside.** Review found
  that the remote side of a delimited listing sent the import's position
  as `StartAfter`, and S3 (and `SimS3`, since M0-05) leaves out a common
  prefix at or before it: with the import past `a/x`, a remote-only `a/y`
  did not list as `a/` unless a live local entry produced it. The merge
  now adds that prefix to the remote's items and checks it like any
  remote-only prefix. The gateway's test remote had modeled the opposite
  S3 behavior, which hid the bug; it now matches `SimS3`.
- **Index bytes per imported entry: 157** (§19 item 6). Measured by
  `index_bytes_per_imported_entry` in `skys3-flush`: 20,000 stubs with
  40-byte keys and a storage class, applied to a fresh index and made
  durable, grow the redb file by 157 bytes each, about 16 GB per member
  for 100 million objects. The log's `IMPORT` records come on top until
  their segments are released.

### M1-19 Parallel import

- **"Passed" stopped being a prefix of the key space.** M1-18's gateway
  took the import's position as "the index holds every key up to here",
  for both reads and listings. With ranges, a later range passes keys
  long before the first range gets there. The position the gateway gets
  is now the end of the gapless part (`ImportRanges::position`), which
  keeps the merge sound, and `RemoteReads::passed` (default: the
  position) answers per key. Both reads and listings ask it: had only
  reads used it, a key added out of band in a passed range would list
  but answer `404` to a HEAD.
- **Checkpoints are one value per bucket, rewritten per page.** Splitting
  the `imports` table's key per range would change its key layout; the
  ranges instead share the bucket's value (tag 2 in the codec), and one
  range still encodes exactly as M1-18 stored it, so existing checkpoints
  resume, and a stored single-stream checkpoint is split after its
  position. Since the whole value is rewritten after every page, streams
  that finish pages together share one sync, and `import_parallel_streams`
  gained an upper bound of 256 (it had none). A build from before this
  one rejects a multi-range value, so it cannot resume such an import.
- **The token bucket had to become a reservation.** M1-18's throttle was
  owned by the one stream and slept with `&mut self`. Shared by streams,
  each page now reserves its keys under a lock, the bucket going negative
  while pages wait, and sleeps outside it; the `rate * (t + 1)` bound
  holds over all streams.
- **Simulated durations depended on real disk speed.** Under a paused
  clock, Tokio advances time to the next timer whenever the runtime waits
  on a blocking-pool thread, so each commit cost a few polling intervals
  of simulated time, more on a slower machine. The scaling test runs the
  index on `BlockingPool::inline`, and the import rate is then a pure
  function of the remote's 100 ms round trip: 4.1 s with one stream for
  4,000 keys in pages of 100, 2.6 s, 1.3 s, and 0.8 s with two, four, and
  eight.
- **Shutdown left import streams running.** The flush service aborted the
  import task on drop without waiting, and with streams spawned in a
  `JoinSet` a stream could still store a checkpoint after `shutdown`
  returned, which made the restart test racy. The first fix, awaiting the
  aborted task, was not enough (review on the PR): a stream aborted while
  it awaited `set_import_ranges` had already queued the write on the
  index's `BlockingPool`, which runs a queued job whether or not anyone
  still waits for it. On a pool of several threads, a late write could
  then land after a restarted import's and move the stored progress back.
  Imports now stop cooperatively: a stop signal is checked between steps
  and interrupts listings, throttle waits, commits, and backoffs, but
  never a checkpoint write, and `shutdown` raises it and awaits the task.
  A bucket that stops being followed is told to stop the same way, and
  its next import waits for the old task before reading the checkpoint.
  `shutdown_waits_for_a_checkpoint_write_in_flight` holds the pool with a
  blocking job while a write is queued; with the abort it fails, as
  `shutdown` returns before the write lands. Its first version expected
  the hold to catch the second page and failed in CI on aarch64: under a
  paused clock, the runtime advances time while it waits for the pool's
  real thread, so a slow write lets the stream get pages further than
  planned. The test now waits until the holding job runs, which means
  every earlier write is done. It reads the checkpoint shown once the
  stream is stuck behind the hold, and expects exactly the next page to
  be stored when `shutdown` returns. It passed 200 runs alone and 200
  more alongside eight parallel copies and the full test binary on four
  cores.
- **The discovery budget first counted every node as sampled.** Charging
  a level its worst case, a listing and 96 probes per node, before
  sending anything stopped discovery from listing twelve small folders.
  A level is now charged its listings, then its probes, and abandoned
  only if what it actually needs exceeds the budget.

### M1-23 OIDC token validation

- **`jsonwebtoken` was not a good fit after all.** Design §15 named
  `openidconnect` or `jsonwebtoken`. `openidconnect` is a relying-party
  client for login flows. `jsonwebtoken` 11 has no `ring` backend, only
  `aws_lc_rs` and `rust_crypto`, picked by a process-wide `CryptoProvider`
  that panics at the first verification if feature unification ever enables
  both. Its `rust_crypto` backend uses the `rsa` crate, which carries an
  unpatched RustSec advisory (RUSTSEC-2023-0071) that `cargo deny` would
  reject. Once tokens and key
  sets were parsed by SkyS3 (to bound them, skip unusable keys, and fuzz
  them), `jsonwebtoken` contributed only the call into the crypto library.
  `skys3-sts` calls `aws-lc-rs`, the provider rustls and the AWS SDK use by
  default, directly. Design §11 and §15 record the decision.
- **`aws-lc-rs` default features duplicate `untrusted`.** Its `ring-io` and
  `ring-sig-verify` features pull `untrusted` 0.7, while `rustls-webpki`
  uses 0.9. `aws-lc-rs` is declared with `default-features = false`, which
  removes the duplicate. Without `ring-io`, a generated RSA key pair does not
  expose its modulus and exponent, so the test kit reads them from the
  PKCS#1 DER encoding.
- **Duplicates outside the target platforms.** `aws-lc-sys` builds with
  `cc`, whose `jobserver` uses `getrandom` 0.4 on Windows, which uses `r-efi`
  6 on UEFI; `rand` 0.9 uses `getrandom` 0.3 and `r-efi` 5. `cargo deny`
  checks every target, so `deny.toml` skips `getrandom@0.4` and `r-efi@6`
  with that reason.
- **The node clock cannot check token lifetimes.** `skys3_io::Clock` is
  monotonic with a per-node origin, so it cannot be compared with `exp`.
  `skys3-sts` has its own `WallClock` trait, with `SystemClock` and a
  `ManualClock` for tests. Key-cache ages use the same clock, and a reading
  that goes backwards counts as "long ago", so a stepped clock refreshes
  keys early rather than late.
- **The issuer allowlist is not node configuration.** The task suggested
  adding configuration keys for issuers and audiences, but design §6.1
  places OIDC providers in the control store under `identity/`, with roles
  and trust policies. `OidcProvider` is that record's schema (unknown fields
  rejected, like other control-store readers), and
  `OidcValidator::set_providers` replaces the allowlist when the node's copy
  changes. The only new key is `[identity] oidc_clock_skew_seconds`, because
  skew is a property of the node. Fetch limits and cache lifetimes are
  constants in `ValidatorSettings`.
- **Checked-in test certificates.** The TLS tests of `HttpsFetcher` use a
  test CA and a server certificate for `127.0.0.1` that are valid for 100
  years, generated once with `openssl`, under
  `crates/skys3-sts/tests/data/`. Generating them at test time would need
  `rcgen` and its dependencies.
- **The first fuzz build is slow.** `cargo +nightly fuzz build` compiles
  `aws-lc-sys` with the sanitizer flags, which took about 3.5 minutes.
  `sts_jwt` ran about 4.3 million inputs in 30 s and `sts_jwks` about 1.9
  million, with no failures.
- **`base64` 0.22 would duplicate the AWS SDK's 0.23.** `hyper-util`'s
  client features, which the AWS SDK's HTTP client enables (M1-15), pull in
  `base64` 0.23. Stacking M1-15 on this PR failed the duplicate-version ban,
  so the workspace uses `base64` 0.23; its API is unchanged for our use.

### M1-24 STS and session credentials

- **Session records sit behind a trait, in memory.** Object operations
  (M1-09) were being built in parallel, and writing sessions straight into
  a `ShardSet` would have meant reading inline payloads back out of the log,
  which no API offers yet. `SessionStore` has `insert`, `get`, and
  `remove_expired`, `MemorySessionStore` implements it, and `Session`
  serializes as JSON for the system bucket that will back the trait. Until
  then sessions do not survive a restart. Nothing serves the gateway yet
  (M1-13), so nothing is lost in practice; whoever wires the node must back
  the trait with the system bucket and call `remove_expired` periodically.
- **The secret cannot be derived from the token.** Deriving the secret
  from the session token would have kept no secret at rest, but every
  request carries the token, and a presigned URL puts it in the URL: anyone
  who saw one could sign anything for the session. The record instead holds
  the token's SHA-256 and the secret sealed with
  `HMAC-SHA256(token, "skys3 session secret")`, so a copy of the store
  cannot sign either. Expiry is checked only after the token opens the
  secret, so a caller without the token learns nothing about the session.
- **The identity copy is in memory.** Design §6.2 keeps local copies in the
  node index, and M2-16 owns persisting and propagating them. `IdentityCopy`
  is filled by `StsEndpoint::sync_identity`, which lists and reads
  `identity/` and then replaces the validator's allowlist. Its age runs from
  the start of the last complete sync. A register that does not parse is
  left out rather than failing the sync: failing would keep the register's
  old value, perhaps a looser trust policy, in force until the copy went
  stale. Two providers naming one issuer are both left out.
- **The provider record had no `proposal_id`.** M1-23's `OidcProvider`
  rejects unknown fields, so it could not be a register as it was.
  `ProviderDocument` repeats its fields plus `proposal_id`; flattening was
  not an option, since serde cannot combine `flatten` with
  `deny_unknown_fields`. `InvalidRegister::Document` carries the errors of
  documents defined outside `skys3-types`.
- **Policies are strings in registers.** `Policy` deserializes but has no
  serialized form, and a register must round-trip. `PolicyDocument<P>`
  keeps a policy with its JSON text and serializes as that string, as the
  configuration and IAM's API do. Role registers and session records use
  it, for identity, trust, and session policies alike.
- **Trust policies are a separate language.** Extending `Policy` with
  `Principal` and `Condition` would have let both appear in identity
  policies. `policy::trust::TrustPolicy` shares the parser's pieces and its
  limits: the base PR changed mid-task to count text instead of bytes and to
  bound arrays (`MAX_ARRAY_LEN`), and trust policies follow it, with
  condition objects bounded and duplicate keys refused by a small
  `UniqueMap`, since serde's maps keep the last duplicate silently. Only
  `StringEquals` and `StringLike` on `aud`, `sub`, and `azp` are accepted; a
  condition key must name an issuer that is a principal of its statement, so
  a typo is refused instead of making the condition never hold.
- **STS shares the S3 listener.** The AWS SDKs send STS query requests as
  `POST /`, which is not an S3 operation, so the gateway routes that shape to
  an `StsService` before authentication. No new listener or configuration
  key was needed, and `sts_web_identity` decides whether the node attaches
  one. `PolicyArns`, `ProviderId`, unknown, and repeated parameters are
  refused, because ignoring a narrowing parameter widens the session.
- **The SDK's provider cannot be pointed at an endpoint directly.**
  `ProviderConfig::with_env` is crate-private in `aws-config`, so a test
  could not hand the web-identity provider `AWS_ENDPOINT_URL_STS`. The test
  uses `aws_config::defaults(..).env(..)` instead, behind `aws-config`'s
  `test-util` feature (a dev-dependency only). That exercises the default
  credential chain configured purely by environment, as a workload is. It
  signs with the node's manual clock as the SDK's time source, so advancing
  the clock past expiry makes the SDK's identity cache refresh the session
  through the chain.
- **Review: the identity copy was read before a wait.** The endpoint took
  its snapshot before validating the token, which can wait seconds for an
  issuer's discovery document and keys. A sync during the wait that removed
  the provider or the role, or narrowed the trust policy, did not stop the
  session, and the copy could pass `identity_max_staleness` unnoticed. The
  endpoint now checks freshness before validation (so a stale node fetches
  nothing) and reads the copy again after it, deciding the provider
  (`OidcProvider::accepts`), the role, and the trust policy on that. A test
  holds the key fetch open with a gated fetcher and changes the
  configuration meanwhile. The session record is stored after these checks,
  so a sync between the checks and the insert can still race; that window
  holds no await on a remote service.
- **Test support moved behind a feature.** The token-minting `testkit`
  was `cfg(test)`; it is now behind `skys3-sts`'s `test-util` feature so
  the integration tests can mint tokens.
- **Per-role session limits were left out.** AWS roles carry a
  `MaxSessionDuration`; here `DurationSeconds` is bounded by
  `session_maximum_seconds` only, which keeps the role register small.

## M2 Replicated shards

### M2-01 Protocol model

- **TLA+ rather than a Rust model checker.** The model is
  `spec/ShardProtocol.tla`, checked by TLC. The design names TLA+ first,
  reviewers can read it beside the design, and it models the design rather
  than an implementation that does not exist yet; the simulation harness
  (M2-03) is where the real code gets checked. It stays outside the Cargo
  workspace and adds no crates. `spec/README.md` gives the full rationale.
- **The model found four gaps in the design**, each now fixed in
  `docs/skys3-design.md` and kept honest by a seeded bug that reintroduces
  it: a restarted node must count the restart as a lease grant before it
  may take over (section 5.4); a node must record a proposal durably and act
  as if its CAS succeeded until it learns the outcome, which keeps a
  restarted candidate from granting leases and a promoting primary from
  dropping the learner (section 6.3); a primary with a promotion
  outstanding needs the learner's lease, or the promoted learner can take
  over while the primary still serves reads on the old members' leases
  (sections 5.4 and 6.7); and reconciliation must compare records by
  `(epoch, seq)`, not truncate past the new primary's last `seq`
  (section 6.6). The step-down of a planned handoff must also be durable
  and count only for the epoch it names (section 5.4).
- **The first model was far too large.** With every feature in one
  specification, three nodes, three epochs, and one write, TLC was still
  growing past millions of states. Merging the R1 stop with the takeover
  proposal (a gap between them only removes acknowledgements), dropping
  members' commit watermarks (only primaries serve), learning
  configurations from the register or another node instead of from a
  history of every configuration, bounding restarts, and node symmetry
  brought it to about a million states. A profile is now a list of small
  models instead of one large one: two-node models reach four epochs and a
  restart in seconds, and three-node models cover a third member and a
  spare.
- **A seeded bug was first "caught" by a modeling error.**
  `truncate_by_seq_only` produced a violation that had nothing to do with
  truncation: the model let a primary drop the learner it was promoting
  and re-add it through the live-stream path, which credits the learner
  with records it never received. The design says the primary keeps
  waiting for that learner, so the model now forbids the drop, and the
  bug's real counterexample needs a re-admitted node. To keep that from
  recurring, each seeded bug is checked against only the invariant it is
  meant to break, and the traces of the subtler ones were read.
- **Integer ticks resolve the lease inequality coarsely.** Timers expire
  only at tick boundaries, so a grace slightly short of
  `Lease × hi / lo` can still pass: `Rates = {1, 3}` with `Lease = 2` and
  `Grace = 4` breaks the inequality and checks clean. The checks therefore
  use a grace with no drift allowance (`Grace = Lease`) and drift well past
  the bound (`Rates = {1, 4}`), both of which TLC must catch. The model shows
  the inequality is sufficient; it cannot measure a small shortfall.
- **TLA+ tooling.** The `v1.8.0` release of `tla2tools.jar` is a rolling
  prerelease whose jar is rebuilt nightly, so it cannot be pinned by
  checksum; `spec/check.sh` pins 1.7.4, the last stable release, by SHA-256.
  TLC 1.7.4 has no `-noGenerateSpecTE` (trace specs are opt-in there) and did
  not find a `-config` file outside the specification's directory, so the
  script copies the specification and its generated configuration into a
  temporary directory. TLC keeps its search queue on disk there.

### M2-02 Intra-cluster transport

- **No TLS settings existed.** M0-03's configuration had no certificate
  keys and no node ID. The PR adds `[transport]` (`listen`, default
  `0.0.0.0:7400`, and `tls_cert_file`, `tls_key_file`, `tls_ca_file`, set
  together or not at all) to §14 and the configuration reference. A node's
  ID is the one its certificate's SPIFFE ID names, so no `node_id` key is
  needed and a node cannot claim an ID its PKI did not issue.
- **Admin listener TLS was pointed at this PR.** §12 said TLS for the admin
  HTTP listener and client certificates in place of its token "are added
  with the node PKI (plan M2-02)". This PR defines the PKI and the `admin`
  certificate role, but serving the admin listener over TLS belongs with
  the binary's wiring, not the transport crate, so it is a new row of plan
  section 14 (proposed for M3-02) and §12 says it is not built yet.
- **rustls's own verifiers cannot check SPIFFE IDs.** `WebPkiServerVerifier`
  insists on a DNS name or IP address, and rustls keeps its mapping from
  `webpki` errors to TLS errors private. Both directions therefore use one
  custom verifier built on `rustls-webpki` (`verify_for_usage` and
  `valid_uri_names`, already in the tree through rustls), with a small
  error mapping of its own so alerts still say "unknown CA" or "expired".
  A numeric node ID is not a valid TLS server name, so the client checks
  that it reached the node it meant after the handshake, not through the
  server name.
- **TLS 1.3 reports a refused client late.** The server checks the
  client's certificate after the client has finished its half of the
  handshake, so a refused client's `connect` succeeds and the refusal
  (an alert) arrives on its first receive. Tests check the server's
  verdict, and `Transport::connect` documents it.
- **turmoil's reset outruns in-flight data.** Under random per-message
  latency, a segment arriving for a socket the peer has already dropped
  draws an RST that can overtake the peer's last data and close
  notification, so the receiver sees "connection reset" where real TCP
  would deliver the data. The simulation scenario closes its side without
  waiting for the node's close. turmoil's default 10 s simulation limit
  equals the handshake timeout, so the scenario raises it, and only every
  sixteenth seed waits out a handshake timeout, which costs about 10,000
  simulated ticks.
- **A test hung CI on smaller socket buffers.** A real-TCP test sent a
  3 MiB frame and only then read it, on a single-threaded runtime. Locally
  the loopback socket buffers held the whole frame, so it passed; on CI's
  runners they did not, and the send waited forever for a reader, which
  stalled every job running all tests. Capping `tcp_rmem` and `tcp_wmem`
  at 1 MiB reproduces it. The test now sends and receives together, over
  a test `Network` that sets 16 KiB `SO_SNDBUF` and `SO_RCVBUF`
  (`SmallBuffers`), so it exercises small buffers on every host; a new
  test has both peers send multi-MiB frames at once over split
  connections. Every network wait in the transport tests is bounded at
  30 s (`Bounded::bounded` in `tests/common`), so a regression fails with
  the waiting line instead of hanging; the simulation's waits are bounded
  by turmoil's simulated duration. The library itself does not deadlock
  when both peers send, as long as each connection's receiver runs on its
  own task; `Connection` documents that.
- **rcgen without its defaults.** rcgen's default features pull `ring`, a
  second crypto provider; the workspace entry enables only `aws_lc_rs`, and
  tests write PEM with the workspace's `base64` instead of rcgen's `pem`
  feature. `Cargo.lock` still lists rcgen's optional `x509-parser` chain
  and `untrusted` 0.7, which are never built; `cargo deny` checks the built
  graph and passes without new skips.

### M2-03 Cluster simulation harness

- **The harness is its own crate.** Plan section 4 put the cluster harness
  in `skys3-sim`, but the harness needs the node's crates (gateway, shards,
  index, control store, transport), and those use `skys3-sim` in their own
  tests, `skys3-control` even in unit tests, which a cycle would give two
  copies of every type. `skys3-sim` keeps the runner, the simulated S3
  store, and now the history recorder and checkers (`history`, `check`),
  which need nothing from the node; the harness is the new
  `skys3-cluster-sim`. Plan section 4 is updated.
- **The node could not run in a simulation.** `Node::start` is hard-wired to
  real disks, a file control store, OS sockets, and thread pools. Rather
  than a second, look-alike startup, the parts that matter moved into
  generic functions the node now calls too: `skys3::storage::recover` (log
  recovery, index replay, shards) and `skys3::control::open` and
  `refresh` (bootstrap, the copy of control state, falling back to the kept
  copy). `control::open` takes the copy the index keeps
  (`control::kept_copy`) and bootstraps only a node that never synced, as
  M1-13's review requires; opening the file store, its owner check, and a
  reset store stay in the node. The harness adds what differs: simulated disks, a drifting clock
  per life, a fault-injecting S3 control store, the gateway over `turmoil`
  TCP, and an authenticator that trusts every request. Sessions, STS, the
  admin listener, and disk fencing are not run.
- **Thread pools break replay.** Index work runs on `BlockingPool` threads,
  and a `turmoil` host whose task waits for a thread lets simulated time
  jump by however long the thread happens to take, so a seed did not
  replay. `BlockingPool::inline` runs each job on the caller when it is
  queued; the harness uses it, and a test runs one seed twice and compares
  the histories.
- **`SimDisk` had no process kill.** A crash without power loss must keep
  the page cache but invalidate the dead process's handles, or a
  destructor of the old life could still write. `SimDisk::kill` does
  that; the driver kills or crashes the disks before it crashes the host.
  A node with a failed sync is always restarted with a power loss, as
  M1-02 requires.
- **Heals must end their own fault.** Review found that the driver's
  heals were not scoped: the fallback power-loss restart of a failed sync
  (the fence) also hit the process that replaced the failed one after a
  crash or a supervisor restart, overlapping control-store windows that
  ended out of start order removed the oldest one's effect instead of
  their own, and the end of any message-loss window stopped all loss. The
  driver now counts each node's processes and a fence applies only to
  the process whose sync failed (moving to the next one if the node was
  down when the sync failed), and each loss or control-store window has
  an identity and ends on its own; what is in force is recomputed from
  the windows still open. Overlapping loss windows give the highest rate,
  as overlapping lost-response windows give the highest probability.
  `Report::fences` counts the fences a run forced.
- **A transient control-store error fails a node's start.**
  `Buckets::reload`, which `Gateway::new` calls, lists `buckets/` without
  retries, so one lost answer stops a starting node. The node binary has
  the same behavior. The harness treats a node that stops by itself as a
  supervisor would, restarting it after a power loss; the gateway should
  retry the listing like its reads (left open).
- **Where M1 keys live.** In M1 every node opens every bucket's shards as
  their only member. The harness writes a static placement to the
  `shards/` registers, as plan section 8 allows, and sends each key to its
  shard's primary, which is the only copy the durability check reads.
  Routing (M2-08) and replication (M2-07) replace that through the
  `NodeServices` hook, which returns the shards the gateway calls.
- **What an answer proves.** The checkers needed a rule for writes whose
  outcome is unknown. A write answered with a 5xx may take effect only
  before its answer (design §5.2), so its window closes then; a write
  without an answer stays open forever, since a held link can deliver it
  after the client gave up. Design §16.1 records the rule. Message loss in
  `turmoil` stalls a TCP stream instead of retransmitting, so lossy links
  surface as client timeouts, which the open window covers.
- **Unanswered writes made the search explode.** A Wing–Gong search tries
  every unanswered write at every point, so forty of them on one key, as
  a long partition produces, did not finish. Two reductions keep it
  exact and small: values no read returns and no `If-Match` names are
  merged into one, and of unanswered writes with the same effect only the
  earliest-called unplaced one is tried, since it can stand in for any
  later one. The search still gives up, with a violation, past
  `MAX_SEARCH_STATES`.
- **Seeds cost more.** A cluster seed takes about a second in a debug
  build, so 256 seeds per scenario would add tens of minutes to CI.
  `Runner::with_cost` divides `SKYS3_SIM_SEEDS` by a scenario's cost; the
  cluster scenarios run 8 or 16 seeds of CI's 256. The nightly job sets
  `SKYS3_SIM_FIRST_SEED` at random, `SKYS3_SIM_SEEDS=1024`, and
  `SKYS3_SIM_SCALE=4`, which lengthens the cluster workload and the fault
  window; the replay command repeats the scale.
- **Multipart uploads have one part.** S3 wants every part but the last
  to hold at least 5 MiB (`MIN_PART_BYTES`), more than a simulated cluster
  should move, so the workload's multipart uploads (6% of operations, half
  of them completed with `If-None-Match: *`) send a single part. The
  history records only the completion, as a `PUT` of the multipart ETag;
  an upload whose creation or part gets no answer is left open, which no
  read sees. A `GET` checks a multipart body against its ETag, the
  no-fault scenario checks that uploads complete and are read (over its
  seeds: a seed may overwrite each upload before a read), and the
  seeded-bug shards also drop unconditional completions. Uploads with
  several parts, aborts, and listing uploads are left to the gateway's
  own tests.
- **Not in the stack yet.** The flusher (M1-16) is not on this branch, so
  every bucket is `local`. M1-16 fills the checker's `flushed` state from
  the remote store the harness already provides.

### M2-04 S3 control-store backend

- **The trait has no conditional read.** `ChangeFeed` polls through `get`,
  so an `If-None-Match` poll could not go through it unchanged. The S3
  backend's change stream is the shared polling feed over a private view of
  the store that reads `cluster.json` with `If-None-Match` on the copy it
  last read and answers a `304` from that copy; the delivery rules stay
  shared with every backend.
- **One probe for every backend.** The plan describes the startup probe as
  part of the S3 backend. It is `ControlProbe`, generic over
  `ControlStore`, so the conformance suite (M2-06) can run it against
  etcd and the in-memory store too. Its writers go through `propose`, so
  the probe also tests the `409` retries and the lost-response rule that
  production writes use: mapping a lost S3 response to "nothing applied"
  makes it fail in the first seed of the simulation.
- **"Writers on different nodes".** A node probes alone at startup, so the
  probe races one writer per store handle the caller passes, at least two,
  as concurrent requests; per-run nonces keep probes of different nodes
  on different registers. The simulation scenario runs five nodes' probes
  concurrently against one faulty simulated bucket.
- **Conditional deletes: refuse rather than fall back.** M1-06 asked what
  `delete_if` does on a store without conditional `DeleteObject`. A
  tombstone fallback (conditional PUT of a tombstone, then unconditional
  DELETE) has an ABA race, since equal tombstones share an ETag, and
  permanent tombstones are what M1-06 already rejected. Such stores are
  refused; design §6.1 records why. R2 qualifies only if the probe finds
  it honors `If-Match` on `DeleteObject`.
- **Permanent errors had no `ControlError`.** `403 AccessDenied`, a
  missing bucket, or `501 NotImplemented` would have been `Unavailable`
  and retried for seconds. `ControlError::Rejected` is not retried; the
  gateway answers it with `500`. A `409` to a read or listing, which
  `skys3-remote` does not class as transient, is `Unavailable` there.
- **`SimS3` could not read stale.** `Fault::StaleRead` and
  `SimS3Faults::stale_read_probability` answer `GetObject` and
  `HeadObject` with the key's state from before its latest write. The
  probability is drawn only when it is non-zero, so seeds recorded for
  existing profiles replay unchanged. Listings are never stale.
- **Probe history on versioned control buckets.** The `ControlStore`
  interface has no delete by version ID, so on a versioned control bucket,
  which design §12 recommends for audit, each probe leaves its writes as
  noncurrent versions and delete markers, as every overwritten register
  does; a noncurrent-version lifecycle rule removes them.
- **Conditional deletes of missing keys on AWS.** The simulator answers
  `If-Match` on a key without a current object with `404`, which the
  backend maps to a failed precondition. If AWS answers `204`, as the
  older pages M0-05 found suggest, deleting an absent register reports
  success: harmless for `propose_delete`, which counts an absent register
  as deleted, but the conformance check that a second delete fails would
  not hold. M1-26 and the M2-06 nightly runs must confirm it.
- **The validator.** M0-03 already refused an S3 control store in a backup
  target's failure scope. Snapshot targets are now checked too (once when
  one defaults to the backup target), and a target in the control bucket
  whose prefix overlaps the control prefix is refused even with
  `allow_correlated_control_store`, the part of credential scoping SkyS3
  can enforce. Scoping the credential itself is the operator's; design
  §6.1 and the configuration reference give the policy.
- **No credential key yet.** §14 names no credential source for the
  control store, and nothing wires the S3 backend into the binary (rule
  1.1). The PR that does adds that key with the region and addressing
  keys M1-15 also left to the attach path.
- **No fuzz target.** The backend parses nothing new: listed keys go
  through the existing `RegisterKey` grammar, and values through the
  existing document parsing.
- **Listings need the same consistency as reads (review).** The first
  probe checked only reads, so a store with consistent `GetObject` but
  lagging `ListObjectsV2` passed, and its change streams would miss writes
  a generation announced: the feed lists once per generation, and a
  generation does not name the registers it announces, so the feed cannot
  tell a lagging listing from a complete one. The probe now lists the
  scratch registers after every round and every deletion, and refuses a
  store whose listing misses a register, shows an old version, or shows a
  deleted one; design §6.1 records why the feed does not relist instead.
  `SimS3` gained `stale_list_probability`, and a scripted
  `Fault::StaleRead` now also makes a listing stale. Stale reads are now
  drawn only for `GetObject` and `HeadObject`, which changed the draws of
  profiles with stale reads; they were new in this PR. That change exposed
  that the probe's cleanup stopped at a stale read showing a register as
  absent, so the cleanup now first deletes at the version the probe knows.
- **The prefix bound was the backend's alone (review).** The backend
  refuses prefixes over 512 bytes (S3's 1,024-byte key limit less the
  longest register key), which configuration accepted. Configuration now
  applies `ControlStoreConfig::MAX_PREFIX_LEN`, and a test in
  `skys3-control` checks that it equals the backend's bound, derived from
  `skys3-remote`, which `skys3-config` does not depend on.

### M2-05 etcd control-store backend

- **No `etcd-client`.** The plan's client crate, `etcd-client` 0.20 (tonic
  0.14, prost 0.14), compiles etcd's `.proto` files in its build script
  with `tonic-prost-build`, so every machine and CI job that builds the
  workspace would need `protoc`. It also adds about 25 crates (tonic's
  `axum` router, `prost-build`, `petgraph`, `pulldown-cmark`), and
  `cargo deny` fails on three new duplicates: `base64` 0.22 beside the
  workspace's 0.23, and `hashbrown` 0.15 and `foldhash` 0.1. The backend
  instead carries its own client for the three calls it uses
  (`KV.Range`, `KV.Txn`, `Watch.Watch`): hand-written `prost` messages, a
  field-for-field subset of etcd 3.6's `rpc.proto` and `kv.proto`, over
  `hyper`'s HTTP/2 client, with `rustls` and `aws-lc-rs` for `https://`
  endpoints. No new crate entered the tree. Tests decode responses
  captured from etcd 3.6.10 with `etcdctl -w protobuf`, so a wrong field
  number fails a test. Design §6.1 and §15 record the choice.
- **gRPC framing is untrusted input.** The 5-byte message prefix is
  checked against a 4 MiB limit before anything is buffered for the
  message, and a compressed message (never asked for) is refused. A
  proptest splits framed streams at random points, and the fuzz target
  `control_etcd_frame` feeds the decoder, the Protobuf responses, and the
  status trailers.
- **Which etcd errors may hide a write.** etcd answers "request timed
  out", "leader changed", and "no leader" with `UNAVAILABLE`, but a
  proposal that timed out can still commit, so a write answered with
  `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `CANCELLED`, `ABORTED`, `INTERNAL`,
  or `UNKNOWN`, or not answered, is `Indeterminate` and goes to the
  lost-response rule; for reads these are `Unavailable`.
  `RESOURCE_EXHAUSTED` ("too many requests", the space quota) applied
  nothing. A failed comparison is not a gRPC error (`succeeded = false`),
  so `FAILED_PRECONDITION`, like `PERMISSION_DENIED` and
  `UNAUTHENTICATED`, is `Rejected`. Only a request that never left (no
  connection to any endpoint) is `Unavailable` for a write. A call without
  an answer drops the connection and moves to the next endpoint, and so
  does a transient status (review): a member cut off from its quorum
  still answers over TCP, with `UNAVAILABLE`, so moving only on missing
  answers kept every retry, and the lost-response re-read, on that
  member. HTTP/2
  pings every 10 seconds while a call is open (etcd refuses pings more
  often than every 5) detect a dead watch connection; idle connections
  are not pinged, since etcd counts pings without streams against the
  client.
- **The watch only wakes the shared feed.** `changes()` opens a watch on
  `cluster.json`, returns once etcd confirms it, and hands the shared
  `ChangeFeed` a wake-up channel, so the delivery rules stay the ones
  every backend shares. Watches start at the current revision, so none
  is compacted at creation. When etcd ends, cancels, or compacts a
  watch, or the connection breaks, the task opens it again after a second
  and wakes the feed once; the feed reads the generation, which covers
  writes made while no watch was open. The task ends when the feed is
  dropped. Tests against an in-process fake etcd check each case with
  the feed already waiting, which only the watch can wake.
- **Listings are paged at one revision.** A listing reads 1,000 keys a
  page, every page after the first at the first one's revision, and
  starts over (three times at most) if that revision is compacted away
  between pages.
- **Tests with and without etcd.** `tests/etcd.rs` runs the conformance
  suite, the startup probe with writers on separate connections, a
  listing of 2,005 registers, endpoint failover, and watch delivery
  against the etcd in `SKYS3_ETCD_ENDPOINTS`, and returns at once, saying
  so, when it is unset. The new `etcd` CI job runs it against the pinned
  `gcr.io/etcd-development/etcd:v3.6.10` service container; the image has
  no shell, so the job polls `/health` in a bounded loop instead of a
  container health check. Error paths, TLS, and watch restarts run in
  every job against `etcd::fake`, an in-process stand-in on `hyper`'s
  HTTP/2 server, which also passes the conformance suite. Locally, all of
  it passed against an etcd 3.6.10 binary.
- **Not wired into the node.** M1-13 left the binary refusing etcd until
  this backend landed. It keeps refusing it: until replication (M2-07,
  M2-08) each M1 node serves every shard alone, and the file backend's
  owner record is what stops a second node from loading the same catalog
  over its own empty shards. A shared etcd store has no such guard, so
  two nodes would serve one bucket apart. The refusal now says so. The PR
  that wires it adds the TLS keys (CA, client certificate and key) to
  `[control_store]` and §14. etcd's user-and-password tokens are not
  supported; client certificates are.

### M2-06 Control-store conformance suite

- **Most of the suite already existed.** M1-05 started
  `skys3_control::conformance` and M2-04 and M2-05 ran it against their
  backends, each on one shared handle and with backend-specific extras
  (more racing writers, the probe over separate etcd connections). This
  PR makes it the harness the plan asks for: `Backend::connect` gives
  each racing writer, probing node, and change stream its own handle (a
  new etcd connection, a new S3 client), `Scale` sets writers,
  operations, and probe runs, and `run_at` runs every check with a fresh
  store and names each check as it starts. The extras folded into it:
  the etcd conformance and probe tests in `tests/etcd.rs`, two probe
  tests in `tests/probe.rs` (the AWS-like profile and faults per writer),
  and the extra racing calls in `tests/conformance.rs`.
- **A linearizability check without a search.** The cluster harness's
  checker (M2-03) searches over plain registers. Control-store values
  are unique and every successful `put_if` names the version it
  replaced, so the successful writes form one chain, which is the only
  order a linearization can give them; each read, write, and failed
  precondition then has a position (a lower bound for a failure), and
  real time must not contradict the positions. The check is a sweep over
  logical call and return ticks. An unanswered write counts once a read
  returns its value. Unit tests feed it forked, stale, and out-of-order
  histories, and `tests/conformance.rs` shows the suite failing on
  simulated stores that ignore preconditions, read stale, or list stale.
- **Change streams had to be `'static`.** Watchers run as tasks, but
  `ControlStore::Changes` had no `'static` bound, so a generic check
  could not move a stream into one. Every backend's stream already owns
  what it reads; the trait now says so.
- **The file store refuses a second node.** The watcher check first
  wrote one `nodes/` register per writer, which the file backend rejects
  by design (`SecondNode`); it writes `buckets/` registers instead.
- **No provider credentials here.** The nightly job could not be run.
  Its plumbing was checked end to end instead against SkyS3's own
  gateway, a local node with a file control store serving the control
  bucket over HTTP through the AWS SDK client (MinIO's download is
  blocked by the egress proxy): the whole suite passed at the default
  scale in 27 seconds and the cleanup left the bucket empty. The first
  nightly runs must confirm two open questions: whether R2 honors
  `If-Match` on `DeleteObject` (if not, the R2 job fails, which means R2
  is refused as a control store, design §6.1), and whether AWS answers
  `If-Match` on a missing key with `404` or `204` (M2-04's note;
  `conditional_deletes` expects a failed precondition).
- **Gating on secrets.** GitHub Actions cannot test secrets in a job's
  `if:`, so the nightly job maps each provider's `CONTROL_STORE_<P>_*`
  secrets into the environment and its first step skips the rest, with a
  notice, when any required one is empty. The test itself also skips
  when `SKYS3_CONTROL_S3_ENDPOINT` or `SKYS3_CONTROL_S3_BUCKET` is unset,
  like the etcd tests. The run is a task of its own so that the cleanup
  runs even when a check panics.
- **Run times.** At `Scale::LARGE` the suite takes about 29 seconds
  against a local etcd 3.6.10 (the CI job runs it after the M2-05
  tests), and a few seconds on the simulated stores with the paused
  clock. The file store and the fake etcd run on real time at
  `Scale::SMALL`.

### M2-07 Replication data path

- **The model's members never fell behind their primary; real ones do.**
  The M2-01 model makes an append durable as it is taken, so a primary
  that restarts in its own epoch always holds every record its members
  hold. With group commit, a member can sync a record before its primary
  does, and a primary that loses power comes back behind it. Truncating
  the member would lose nothing committed, but the primary cannot tell a
  record it sequenced from one it never did without a second numbering.
  Every record a member holds in the primary's epoch was sequenced by that
  primary, so the primary rolls such records forward instead: members send
  them in their `SyncAck` frames, the primary appends them, and it serves
  nothing until every member has synced and its whole log is committed.
  Design §5.1 ("Replicating") records the rule.
- **Segment classes left holes in a replica's log.** Hot and bulk records
  become durable independently (§10.1), so an `EXTENT` could be synced
  while an earlier hot record was not. On one replica that only cost an
  unacknowledged record (M1-04's note on holes); on a member, an
  acknowledgement must name a run without gaps. Each shard now has one
  appender task that queues records in position order (`SegmentLog::queue`
  splits queueing from waiting), and on a replicated shard it waits for
  every earlier record to be durable before it queues a record of the
  other class. This also settles M1-04's out-of-order queueing on worker
  threads. The cost: a lazy `FLUSHED` queued just before an extent holds
  the extent until the lazy record commits, up to `LAZY_MAX_DELAY` on an
  otherwise idle disk. M2-17's write path should check that this stays
  rare. Design §10.1 has the rule.
- **Appends of an earlier session.** After a link drops, frames of the
  old session can still sit in the member's queue. `Shard::begin_session`
  makes the member refuse appends of every earlier session, and
  `Shard::settle` waits until the records it already queued are durable or
  failed before it reports its last `seq`, so the primary resends from a
  point the member really holds.
- **Replay applies the uncommitted tail.** A replica replays its whole
  log on open. In the primary's epoch that is right, since the tail always
  commits by the roll-forward rule. After a primary change it is not:
  reconciliation (M2-12) truncates uncommitted records and must also undo
  what a member's replay applied. §5.1 says so.
- **The primary reads its own log for slow members.** Records committed on
  every member leave the primary's memory. A member that reconnects
  further back gets them from `Shard::read_tail`, which scans the
  segments on disk and fails unless the range is complete. Log compaction
  (M1-22) must keep the records a member may still need, or the link
  must fall back to a snapshot (M2-09).
- **Watch channels wake in random order.** `tokio::sync::watch` spreads
  its waiters over several `Notify`s chosen at random, so one channel
  shared by all of a primary's links woke them in a different order on
  each run, the links drew their network latencies in a different order,
  and a seed did not replay. The leader keeps one channel per member.
  Every `select!` on the replication path is `biased;` for the same
  reason. `a_replicated_seed_replays_exactly` passes 1,024 seeds.
- **How the simulation checks it.** `ReplicatedServices` runs the
  replication transport on each simulated node and records every replica.
  After every step, an audit checks that no primary has committed, or
  answered a write, beyond what every member holds durably.
  `ClusterConfig::every_member_durable` makes the recovery check read
  every acknowledged write from every member, not only from the primary.
  Two seeded bugs show the checks work: a primary that commits alone
  trips the audit, and M1 nodes that keep writes on one member trip the
  every-member check. Scenarios cover random crashes with message loss,
  and a power loss on each primary in turn, with a partition and a failed
  sync, while members hold its tail. All of them passed 1,024 seeds.
  `NodeServices::ready` lets the driver wait until every primary has
  reconciled before clients start, because a reconciling primary answers
  `503`.
- **Measured I/O per small PUT (§5.3).** `replicated_write_io` runs 24
  clients writing bodies of up to 256 bytes on three replicas, and counts
  group commits and records on every replica after startup. Over two
  seeds, 503 acknowledged writes took 2.66 group commits over all three
  replicas (0.89 per replica, each one sync of every file it wrote), and
  a group commit held 1.81 records on average (4.83 records per write
  over all replicas). With `SKYS3_SIM_SEEDS=256`, 1,943 writes took
  2.45 group commits each, at 1.94 records per group commit. A write
  costs at most three syncs, one per member,
  all in parallel, and group commit brings it under that. The record counts
  include the three records of each multipart completion, and writes that
  failed a condition or went unanswered, so they overstate records per
  acknowledged PUT. One serial durable round before success holds: the
  primary sends each record to its members while it syncs its own copy.
- **Left for later.**
  - **Wiring.** No configuration keys and no binary wiring: the node
    still serves shards alone, and `ReplicationConfig`'s defaults (100 ms
    beacons, 1 s link timeout, 200 ms reconnect delay) are fixed until
    M2-08 wires replication in.
  - **Static configurations only.** `Shard::reconfigure` refuses a
    replicated shard (M2-10, M2-11).
  - **No timeout yet.** There is no `replica_ack_timeout`. While a member
    is down, writes wait, and so does `Shard::close`, since its barrier
    waits for every earlier record to commit (M2-10).
  - **R2 refusals.** A member that refuses an append from an older epoch
    answers with its epoch. The primary only logs it and drops the link;
    deposing itself is M2-12's.
  - **Simulation follow-ups.** M1-14's power cuts at every sync boundary
    run on single-member shards only. Running them under
    `ReplicatedServices`, where a cut can land between a member's sync
    and its acknowledgement, would cover the commit rule at every
    boundary. The harness also starts a flusher on every `write_back`
    shard open on a node, members included, and a member refuses its
    `FLUSHED` records. The replication scenarios use only `local`
    buckets, so flushing replicated buckets from the primary alone
    waits for M2-08.
  - **One connection per shard and member.** This is simple, but a node
    with many shards opens many connections. Multiplexing shards over one
    connection per peer is a later optimisation.
  - **Size.** About 1,800 lines of non-test library code, a good part of
    it doc comments, plus about 350 in the simulation harness: over the
    plan's 1,500. Splitting would have left the data path without its
    checks, so it stays one task.

### M2-08 Shard map and routing

- **A forwarded record cannot travel at position `(0, 0)`.** A write's
  record crosses the wire in the log record format, but the encoder
  refuses a `PUT` or an `MPU_PART` that names an extent or an upload at or
  after its own position. Records travel at the last position,
  `(2⁶⁴−1, 2⁶⁴−1)`, and the replica sequences them as usual. The fault
  scenarios first passed with every such write failing, since a forwarded
  write that fails is recorded as unknown; the fault-free scenario, which
  asserts no unknown outcome, caught it.
- **Members served payloads and seals.** `Shard::payload` and `Shard::seal`
  never checked the replica's role; only reads and writes did, through
  `check_readable`. `LocalShards` now checks it for both, and maps the
  replica's `NotPrimary` to a new gateway `ShardError::NotPrimary`, which
  the forward server turns into a redirect hint. The replica's own check
  stays the only authority on who serves, so the server does not repeat
  it.
- **A forwarded write can outlive its gateway.** The harness ends an
  unanswered operation when the node it was sent to crashes, which is
  sound only if that node applies it. A gateway can forward a write and
  crash, or give up on it with `503`, while the primary still commits it.
  `Workload::any_gateway` records operations without a server and writes
  answered with a server error as unknown; the scenarios that send to the
  primary are unchanged.
- **Concurrent register reads failed requests.** Register reads were first
  rate-limited per shard, so every request that arrived while another was
  reading a shard's register failed. Reads of one node now wait for each
  other and look at the map again before reading, so concurrent requests
  share one read. A review then found that the interval was timed from
  the start of a read: a read slower than the interval, or one that
  failed, left every waiting request to read again in turn. The interval
  now runs from when a read finishes, and the requests within it take
  its result, its error included.
- **No new index format for the shard map.** The map is a new `shard_map`
  table, keyed like `shards`, holding each configuration's register JSON.
  It is a cache that is safe when stale, so it takes no index format of
  its own (the format is 3, from M1-18's `imports` table): an older build
  ignores the table, and opening an index that lacks it, of any supported
  format, adds it. It is kept apart from the control-state copy, which each sync
  replaces whole.
- **A pooled connection the peer dropped fails one write.** The gateway
  keeps idle connections per node (at most 8, for at most 60 s; the server
  drops a connection idle for 5 min). After a node restarts, the first
  write on each connection to it fails with `503`, because the gateway
  cannot tell whether the request arrived; a read moves to the next
  member. Probing a connection before reuse would cost a round trip on
  every request.
- **Primary-only flushing.** `FlushService::reconcile` starts flushers
  only on shards this node is the primary of, which M2-07 left open: a
  member refuses the `FLUSHED` records its primary does not send. The
  routing scenarios include a replicated `write_back` bucket.
- **Lazy `FLUSHED` records outlast short forwarding timeouts.** A write
  to a `write_back` shard can wait behind a lazy `FLUSHED` record on its
  primary for up to `LAZY_MAX_DELAY` (1 s), which M2-07 named as a risk. A
  routing scenario with a 1 s forwarding timeout failed writes that way;
  the scenarios now give forwarded requests 4 s, and the default is 30 s.
- **Leases and acknowledgement timeouts cross the wire as refusals.** A
  primary that lacks a lease from some member (M2-09), or a stopped one,
  refuses a forwarded read as unavailable, and the gateway answers `503`
  at once rather than asking other members, which would only redirect it
  back. A write that was not acknowledged in time (M2-10) travels as its
  own outcome, with the position it took, so the gateway answers it as
  the local path does.
- **Seals across nodes.** The design left them to this task. The gateway
  forwards seal and unseal to the primary, which holds a seal until it is
  lifted or it restarts. Lifting the seals of a crashed gateway, and seals
  across a primary change, go to M3-04, the first task that deletes
  buckets on several nodes; §4.1 says so.
- **How the simulation checks it.** `RoutedServices` puts a routing
  gateway on every node and starts each map stale, with the placement in
  epoch 3: a third of the shards known in epoch 2 with a member as
  primary (a redirect corrects it), a third in epoch 2 with only a node
  that does not exist (no member answers, so the gateway reads the
  register), and a third unknown. Clients send every request to a random
  node. An observer on every node's forward server records which replica
  served each request in which epoch, as it serves it, and an invariant
  fails the run if any was not the current primary in the current epoch.
  The observer first sat in the gateway, after the answer arrived, and so
  missed a request served by the wrong replica whose answer was lost; a
  review caught it. A seeded bug, members that
  answer reads from their own index, trips it; that scenario starts every
  map with a member as each shard's primary, since with the mixed maps a
  write's redirect could correct a map before any read reached the
  member, and one seed in 4,096 missed the bug. Scenarios cover no
  faults, random crashes, partitions, and message loss, and every node
  restarting in turn; each passed 4,096 seeds (`SKYS3_SIM_SEEDS=4096`).
- **A `GET` can fail when it races an overwrite.** A read that finds its
  object replaced between the entry and the payload answers a retryable
  `503`. Forwarding both calls widens that window, so the fault-free
  scenario allows a few failed reads; it still allows no failed write.
- **A replication seed failed before this task, and passes after the
  merge with M2-10, for no identified reason.**
  `replicated_writes_under_crashes_and_message_loss` with
  `SKYS3_SIM_SEED=86` found a history no order explains on
  `bucket-1/key-1`, on the M2-07 head (a4e1066) as well, so not through
  routing. It passes once M2-07's review fixes, M2-09, M1-18 and M2-10
  are merged in. It is not M2-10's relaxed rule for failed writes: the
  seed passes with a4e1066's checker too. The merged changes also change
  what the replicas do, and so the run that seed draws; the case the
  seed found may still exist, unexplained, under another seed.
- **Left for later.**
  - **Binary wiring.** Not done: the routing code alone is about 2,000
    lines of non-test library code, over the plan's 1,500, before any
    wiring. Running replicated shards in the binary also needs: a shared
    control store (the binary refuses anything but the file store, which
    a second node cannot open), peers' transport addresses (node
    registration is M3-02, so a static list in the configuration until
    then), shard registers for a static placement (no bootstrap command
    writes them yet; M3-04 makes CreateBucket write them), loading the
    `[transport]` credentials, opening replicas from the registers, the
    `serve_peers` listener, and configuration keys for the replication
    timing (`ReplicationConfig`) and `RoutingConfig`. A follow-up task
    (M2-08b) should take these; until then the binary serves every shard
    alone, as before.
  - **Multiplexing.** One request per connection at a time, with a small
    pool per node. Request IDs are on the wire, so a connection can carry
    several requests later.
  - **Coordinator pushes** of configurations (§6.2) come with M3-01.
  - **Size.** About 2,000 lines of non-test library code, most of it
    the forwarding messages and their checks, plus about 350 in the
    simulation harness.

### M2-09 Leases and strong reads

- **No takeover yet, so no read can really be stale.** Placements are
  static until M2-12, so the old primary is always the only one and the
  history checker cannot see a stale read. The simulation checks the
  property a takeover relies on instead: the lease audit of
  `ReplicatedServices` flags every read a primary serves once some
  member's `primary_grace` has passed, since that member could then have
  proposed itself and acknowledged writes the read misses. Within `ρ` no
  read is flagged, also with node 1 as slow and node 2 as fast as `ρ`
  allows; with node 1 40% slow and node 2 40% fast, reads during a
  partition between them are (about 1.33 s of lease against 1 s of grace),
  while the commit audit, linearizability, and the every-member
  durability check still pass. M2-12 can turn the drift-beyond-`ρ`
  scenario into an actual stale read in the history.
- **Clock readings cannot cross hosts, not even in the checker.** Each
  `turmoil` host runs its own paused Tokio runtime, whose `Instant`s have
  their own base, and outside any host `tokio::time::Instant::now` falls
  back to real time, so the driver cannot read a node's `MonotonicClock`.
  The audit converts each member's grace deadline to simulated time inside
  the member's host, as it changes (`Grace::subscribe`), and checks reads
  in the primary's host with `turmoil::sim_elapsed`. For the same reason,
  `ReplicatedServices::ready` asks only that every member granted each
  primary a lease, not that it is still valid.
- **Stalled writes hid the anomaly.** A partition stalls the writes of
  every shard it cuts a member from (there is no `replica_ack_timeout`
  before M2-10), and with the default 2 s client timeout every client was
  soon waiting on one, so no read landed in the 330 ms window. The lease
  scenarios give clients a 300 ms timeout.
- **Fixed drifts.** The harness drew every life's drift within the bound,
  so a scenario could not put two chosen clocks beyond `ρ`.
  `ClusterConfig::node_drifts` fixes the drift of the first nodes; the
  drift is still drawn, so fixing it changes no other draw of a seed.
- **Stamps rather than beacon numbers.** The primary stamps each append
  and beacon with its clock reading, and members echo the latest one, so
  the primary keeps no table of beacons in flight. A member grants a lease
  with every acknowledgement after its first stamp, also when it repeats
  an old stamp; that only restarts its grace later. Design §5.4 records
  the details, and that unconditional writes need no lease.
- **The renewal interval equalled the link timeout.** Both default to 1 s.
  On a busy link whose member's log stalls, the answers to renewal
  beacons were all the primary heard, one per timeout, so the link
  dropped and reconnected over and over, and a reconnect waits for the
  member's slow sync, so leases could lapse. Review caught it.
  `ReplicationConfig::beacon_every` now caps every beacon interval at a
  quarter of `link_timeout`; a test with a member that answers beacons
  and acknowledges no record keeps one session and its lease through
  three timeouts.
- **Left for later.** R1 needs a member to stop granting leases before it
  proposes (M2-12): `Grace` has no such switch yet, and the member's link
  echoes stamps as long as it follows the primary. The lease timings sit
  in `ReplicationConfig` with the design's defaults; the configuration
  keys exist (M0-03) but are wired in only with replication itself.

### M2-10 Acknowledgement timeout modes

- **The history checker assumed a failed write lands before its answer.**
  `Outcome::Failed` let a write take effect only between its call and its
  `503`, which held while failures came from crashes. With the timeout, a
  record commits after its writer was told it failed, so a read sent
  after the failure can miss it and a later read see it: allowed by §5.2,
  but flagged by the checker. A failed write's answer now holds back only
  writes that may have written: it is ordered after its call and before
  every such write sent after its answer, which is exactly "never
  resurfaces over a later PUT or DELETE". Writes refused for their
  condition count as reads, because the gateway refuses them on a plain
  read before it writes anything. Checker tests cover both sides.
- **Fail fast needed a rule.** §5.2 says requests fail "as soon as a member
  is late" without saying how a primary knows. Once a request times out,
  a fail-fast shard refuses new writes at once, unsequenced, until every
  record sequenced before the timeout is applied; wait-through shards take
  every write and let each wait its own timeout. Design §5.2 records it,
  together with what the timeout covers (conditional writes waiting for
  an earlier write of their key, and seals).
- **Closing a shard (decided, design §5.2).** `Shard::close` waits for its
  barrier at most `replica_ack_timeout`, then abandons the records still
  waiting: the pipeline stops, drops them unapplied and fails their
  waiters. They stay in the log, and the next opening rolls them forward
  in the primary's epoch (or M2-12's reconciliation discards them). A
  member closes the same way when its primary is gone; a shard alone keeps
  waiting for its disk. Every writer's own timeout starts before the
  close does, so in practice the writers have timed out already and the
  abandon only answers barriers. `ShardSet::close_all` first closed the
  shards one after another, so a node whose shards had all lost a member
  paid the timeout once per shard before its final checkpoint (40 s for
  eight shards at the default 5 s; review finding). It now closes them
  concurrently and returns once all are closed, in about one timeout.
- **Seeing that failed writes really apply.** A history alone rarely shows
  it: the next operation on the key is usually another write. So the
  `NotAcknowledged` error carries the position its record took, and
  `ReplicatedServices::late_writes` counts the failed writes a primary
  committed later. Over 10 seeds of the new `acks` scenarios (partitions
  between every pair of nodes), wait-through with 600 ms had 207 writes
  fail after getting a position and 205 commit later, and fail-fast with
  300 ms had 93 and 74; the rest were still waiting for a member when the
  run ended. The linearizability and durability checks pass on all of
  them. A seeded bug that resends each failed write a second later (as a
  gateway retrying on its own would) is caught in 10 of 10 seeds.
- **The lease scenarios use the default 2 s client timeout again.** They
  use fail-fast with 300 ms, so writes stalled by a partition fail at
  once and clients keep reading. Drift beyond `ρ` is still caught (230
  flagged reads over 10 seeds), and the within-`ρ` scenario still sees
  served and refused reads and no stale one.
- **The simulated wait-through timeout is below the design's minimum.**
  Configuration loading requires `replica_ack_timeout > member_suspect_after
  + 1 s` in wait-through mode, so that the removal (M2-11) fits inside it.
  Nothing removes members yet, so the scenarios use 600 ms to get answers
  before the clients' 2 s timeout.
- **Left for later.** `ReplicationConfig::ack_timeout` defaults to the
  design's 5 s wait-through; the `replica_ack_timeout_ms` and
  `replica_ack_timeout_mode` keys are mapped onto it when replication is
  wired into the binary. Until M2-11 removes a dead member, wait-through
  writes on its shards each wait the full timeout and their records pile
  up in the primary's memory and log until the member returns; fail-fast
  bounds that to the records sequenced before the first timeout.

### M2-11 Member removal

- **A replica cannot switch epochs at its own last `seq`.** M2-07 appended
  a `CONFIG` record at `(epoch, last seq)` of whichever replica adopted a
  configuration. A member behind its primary (say at seq 95 while the
  primary's `CONFIG` record follows seq 100) would then hold `(e+1, 95)`
  and next be sent `(e, 96)`: positions running backwards. Every replica
  now switches at the `seq` of the primary's `CONFIG` record. A `Sync`
  carries the epoch the primary sequences in, besides its configuration;
  a member that learned a newer configuration keeps sequencing in its old
  epoch, takes that epoch's tail, and appends its own `CONFIG` record just
  before the first record of the new epoch, or once it holds the
  primary's last record if all of those are older. This is the deliberate
  relaxation of "one epoch per record" M2-07 left for this task. A
  primary appends its `CONFIG` record only once every member of the new
  configuration has reported its log in this life, so no member holds an
  older-epoch record the primary lacks; until then the record waits, and
  the link of the last member to report appends it
  (`Shard::align`). Design §5.1 records the rules.
- **Restarting into a newer epoch had the same problem.** `Shard::open`
  appended the `CONFIG` record at once whenever the configuration was
  newer than the log. A replicated replica now opens in its log's epoch
  and aligns as above (`Shard::sequencing`). The node keeps no copy of
  the configuration of its log's epoch until M2-16, so a primary opened
  this way commits that epoch's tail under the newer configuration's
  rule: safe, since the register already holds it and a removal only
  shrinks the members.
- **When the commit rule changes.** The primary could drop the member as
  soon as its compare-and-swap landed, or once its `CONFIG` record is
  durable. It waits for the record (`Leader::adopt`, called by the
  pipeline when the record is durable), as §5.1 asks of every replica
  before it acts in a new epoch; it costs one group commit on the
  primary's own disk. The watchdog adopts without waiting for the record
  to *commit* (`Shard::begin_reconfigure`): a second late member would
  hold that up, and the watchdog would never get to remove it too.
- **A removal needs no durable proposal (decided, design §6.3).** §6.3
  makes every proposer record its proposal and act as if it succeeded.
  For a removal, acting as if it failed is the safe side: the primary
  keeps needing the member's acknowledgements and lease until it knows,
  and finds a lost answer in the register later, which it adopts if it
  keeps the primary and only removes members. A register naming another
  primary, or gone, stops the primary (`Shard::depose`); M2-12 refines
  what a deposed primary does.
- **"Unresponsive" was not defined (decided, design §6.4).** A member
  responds while its acknowledgements advance, or, once it holds what the
  primary had sequenced a moment earlier, while it answers anything. A
  member whose disk is stuck still answers beacons, so "heard from
  recently" was not enough. A member that never reports its log after the
  primary starts is suspected too, so a primary that restarts while a
  member is dead does not wait for it forever.
- **Pending writes and `min_write_replicas` (decided, design §6.4).** The
  design rejects new writes once fewer copies remain, but said nothing of
  the writes in flight that then commit with too few copies. They are
  applied, and their writers get `ShardError::UnderReplicated` (`503`):
  not acknowledged, as §5.2 allows. `FLUSHED`, `IMPORT`, and `ADOPT` are
  not client writes and go on, as under a seal.
- **The simulation assumed static placements in three places.** The
  every-member durability check read the members of each shard from the
  placement, the commit audit took them from it at acknowledgement time,
  and restarted nodes reopened every shard in epoch 1, which a replica
  past epoch 1 refuses. The harness now reads the registers straight from
  the control bucket, without requests or faults, as a stand-in for the
  shard map (M2-08) and the local copies (M2-16): nodes open a shard in
  its register's configuration (a removed member does not reopen it), the
  audit checks each acknowledgement against the register's members at
  that moment (so a primary dropping a member before its compare-and-swap
  landed is still caught), and the final check reads every member of the
  final configurations. The every-member check also split survivors by
  member index, which left shards with fewer members with no copy at all;
  their last member now stands in. The removal's compare-and-swap goes
  through each node's faulty control store, with lost answers and `409`s.
- **Measured.** Ten seeds of node 3 cut off from both other nodes
  for 8 s, with the defaults (3 s suspicion, 5 s wait-through timeout) and
  the control store losing, conflicting, or refusing 2 % of requests each:
  no write failed or went unanswered, and the slowest acknowledged write
  took 3.02 to 3.10 s, `member_suspect_after` plus the watchdog's check
  interval (100 ms), the compare-and-swap, and the `CONFIG` record's group
  commit. In fail-fast mode (2 s), every write that failed after taking a
  position, 2 to 6 per seed, later committed under the new epoch. The
  loopback test in `skys3-shard` sees the same with 300 ms.
- **The protocol model needed no change.** `RemoveMember` and the
  same-primary branch of `Adopt` in `spec/ShardProtocol.tla` already keep
  acknowledgements across the change and let a removed member grant
  leases only in its own epoch. The implementation refines them: between
  the compare-and-swap and the durable `CONFIG` record it needs the old,
  larger set of acknowledgements and leases, which only removes behaviors.
- **Metrics.** `under_replicated_bytes` and `oldest_under_replicated_age`
  have no labels: a node counts the shards it leads, so the sum over nodes
  counts each shard once (`Replication::exposure`,
  `ReplicationMetrics`). The bytes are an index scan per under-replicated
  shard and report; the age restarts with the node, since the register
  keeps no removal time. `docs/skys3-metrics.md` lists them as defined,
  not exported: the binary does not run replication yet.
- **With routing (M2-08).** `ShardError::UnderReplicated` reaches the
  gateway as `NotAcknowledged` without a position, so clients get
  `503 SlowDown` as §6.4 asks, and it crosses the forward wire as outcome
  7. A removal shows in the shard maps without further changes: the
  primary serves in epoch e+1 once it adopted it, so a gateway asking in
  epoch e learns it from the reply's hint, and the members that remain
  redirect with e+1 once they follow. The removed member stays a member
  of epoch e: it serves no routed request and redirects with epoch e,
  which a gateway that knows e+1 ignores. A primary deposed by its
  register refuses with `503` instead of redirecting; M2-12 decides what
  it answers. The routing scenarios now check served requests against the
  register, since a removal moves the epoch past the placement's.
- **Left for later.** Wiring (`member_suspect_after_ms` maps onto
  `ReplicationConfig::member_suspect_after`, and `with_removal` takes the
  node's control store) comes with replication in the binary. A removal
  does not bump the generation in `cluster.json`; nodes learn of it from
  sessions, and the removed member keeps its replica open in the old
  epoch until it restarts or rejoins as a learner (M2-15, M2-16).
  `skys3-shard` now depends on `skys3-control`, `skys3-obs`, and
  `prometheus-client`.

## M3 Coordinator

### M3-01 Coordinator lease and change propagation

- **The takeover wait alone does not keep tenures apart.** §6.7 gave only
  the candidate's rule, `coordinator_lease × (1+ρ)` after it first sees a
  lease version. A holder that acts for a whole `coordinator_lease` after
  each renewal can outlast that wait when its clock runs slow by `ρ` and
  the candidate's fast by `ρ`: by about `2ρ` of the lease. The holder now
  acts only for `coordinator_lease × (1−ρ)` after sending its renewal,
  counted from the first attempt of a write retried after a lost answer,
  and the candidate counts from when its read returned, which is after
  the write. Recorded in §6.7. Safety never rested on it (every change is
  a compare-and-swap), but it makes "two coordinators at once" a fault
  beyond `ρ` rather than routine, and the simulation checks it: no two
  tenures overlap in simulated time, and a seeded bug, one node with a
  quarter-length lease, is caught.
- **A lease write whose answer was lost spans calls.** `propose` applies
  the lost-response rule within one call; a renewal or takeover that ran
  out of attempts and is sent again in a later round would see its own
  landed write as a lost race. The elector keeps the pending lease write
  and resends the same value under the same precondition, and adopts it
  when a later read finds its `proposal_id`, with the tenure counted from
  the first attempt (so an old adoption is renewed before the node acts).
- **A holder whose tenure lapsed keeps renewing its own version.**
  Stepping down to a candidate would make it wait out its own lease
  (`coordinator_lease × (1+ρ)`) after a store outage longer than the
  tenure. It keeps the version it holds and renews by `If-Match`: if
  nobody took over, the renewal lands and it acts again at once; if
  someone did, the renewal is rejected and it follows the new holder.
- **Pushes have a port of their own in the simulation.** Replication's
  listener owns the transport port and serves only replication links, and
  the node binary does not wire the transport yet. The harness serves
  pushes (`ControlChanged`, acknowledged by `AdminReply`) on port 7401;
  serving both on one port needs a dispatch by message class when the
  transport is wired into the binary.
- **Pushes do not wake the node's sync yet.** `ControlHints::newer_than`
  is the hook beside the change stream in the node's sync loop, which
  M2-16 rewrites; the simulation records and checks delivery instead. A
  push is a hint, so a forged one from an admin certificate costs a read.
- **The push audit had to learn when nodes run.** Injected control-store
  faults stop a node during startup now and then, and the supervisor
  restarts it 500 ms later; invariants only run once clients start, and
  the harness restarts every node at the end. The check covers
  announcements from when the clients start until the final restart,
  skipping those within the bound before a node went down or a second
  after it came back, and announcements from different coordinators can
  arrive out of order, so a newer pushed generation counts.
- **An unanswered write was announced before it could land (review).**
  `apply` incremented the generation as soon as a write's retries ran out
  without an answer. A request still in flight (FaultyStore's
  `LateRequest`) could then land after the increment, and a node that
  had already listed the registers at that generation would miss it
  until some later change. Such a write is now kept as a `Pending` and
  settled before it is announced (§6.7): `settle` resends it under the
  same precondition until an answer comes, reading the register for its
  `proposal_id` after a failed precondition. After that answer no attempt
  can land, since versions are never reused. The coordinator plans
  nothing else until it has settled, and keeps settling after its tenure
  ends. Tests with a late request (a create and a delete) fail without
  the change.
- **A partly applied change was never pushed (review).** When a later
  write failed, `apply` returned only the error, which dropped the
  generation that announced the writes before it, so the coordinator
  never pushed them. `apply` now fails with `ChangeFailed`, which carries
  the `Applied` prefix and its generation. The coordinator pushes that
  generation and reports the prefix to the placement. A generation
  increment that fails after writes landed becomes a `Pending` too.
- **The placement stand-in.** Placement proper is M3-02 to M3-06, so the
  simulated coordinator moves shard registers of a bucket no gateway
  serves through their epochs, as membership changes will. The
  `Placement` trait is the extension point; `NoPlacement` changes
  nothing. The `ControlStore` trait is unchanged.
- **Numbers.** Over 32 seeds with a 1.2 s lease and `ρ` = 1%: placement
  paused at most 1.32 s when the coordinator lost the control store,
  against a bound of the takeover wait plus two read intervals and three
  placement intervals (2.31 s), and no client request failed; a node
  acting without the lease beside the holder made 8,712 changes with
  the holder, 325 of which lost a race and none of which wrote an epoch
  twice; the quarter-length-lease bug was caught in 20 of 32 seeds (in
  the rest the hasty node held the lease from the start).

### M3-02 Node registry and lifecycle

- **Nodes had no way to find the coordinator.** The lease names its
  holder, but nothing gave a node the holder's address. A node now reads
  `coordinator.lease` and the holder's registration, and only when it has
  no coordinator that answers as one, at most every `resolve_interval`;
  while the coordinator stays put, heartbeats cost the control store
  nothing. The answer says whether the receiver coordinates and whether
  its registry lists the sender, which registers again if not (a node
  forgotten during a long partition, for example).
- **A new coordinator has heard no heartbeats.** Heartbeats went to the
  previous holder, so silence measured from the last heartbeat this node
  happened to receive would make a node that regains the lease after a
  day forget everyone. `Placement` gained a `begin_tenure` hook with a
  default no-op (an additive change to the M3-01 trait), and silence is
  counted from the latest of the last heartbeat, the first listing of the
  node's current registration, and the start of the tenure. A node is not
  told it is unregistered before the tenure's first listing.
- **Forgetting needed a re-homing check before placement exists.** The
  `Rehoming` hook decides when a departing node can be forgotten; its
  default, `ShardScan`, lists `shards/` and keeps every node a shard
  register names as primary, member, or learner, rereading only registers
  whose version changed. A shard register that does not parse keeps every
  departing node. Re-homing itself is M3-03 and M3-05, which read the
  states from `NodeRegistry::entries`.
- **Scanning and then deleting raced with an overlapping coordinator.**
  Review found that the first version scanned the shards and deleted the
  registration in one round. A second coordinator (M3-01 allows brief
  overlaps, and a stale one keeps writing) could add the node to a shard
  between the scan and the delete. The two writes go to different
  registers, so both CASes succeeded, and a shard was left naming an
  unregistered node. Forgetting now takes two rounds. The first marks the
  registration `departing` by a CAS, and placement never assigns a marked
  node. The second, whose listing read the mark, scans the shards and
  deletes under `If-Match` on the marked version. So an assignment of the
  node comes from a planner that read it before the mark, and lands before
  the scan, which sees it. The exception is a planner that stalls across a
  whole round. No CAS can close that window without transactions across
  registers, which the design rules out. The result is then a shard member
  on a lost node, which member removal (§6.4) and replacement handle. A
  unit test, which fails when the two rounds are merged, runs a competing
  placement right after the scan. The mark is a new optional
  `NodeRegistration` field (`departing`, written only when set). It is
  added under format version 1, as M1-06 added `created_unix_ms`, because
  no release has shipped it. A marked registration no longer describes its
  node, so a node that returns clears the mark by registering again, and
  the heartbeat answer tells a marked node to do so. Silence is not reset
  by the coordinator's own mark. Recorded in design §6.7.
- **Registrations settle unanswered writes before announcing them.**
  M3-01's change path now hands back a write that may still land as a
  `Pending`, and announces it only once `settle` knows that it landed.
  `register` settles it at once. If settling fails too, the `Pending`
  goes back to the node's `Heartbeater` (`RegistrationError::Unsettled`),
  which settles it before it registers again, so a registration that lands
  late is still announced, and only once it has landed. A node that
  restarts in between loses the `Pending`. That leaves at most an
  unannounced registration, which the coordinator finds by listing and the
  next increment announces. The coordinator settles its own pending marks
  and deletes, as it settles every change.
- **A restart that changes nothing writes nothing.** The design said a
  restart updates the record under `If-Match`; rewriting an identical
  record would cost a CAS and a generation increment per restart. A node
  that crashed between its registration CAS and the increment leaves the
  write unannounced, which is harmless: the coordinator lists `nodes/` on
  every round rather than relying on change streams, and the next
  increment of any change announces it to the other nodes. Recorded in
  design §6.7, with heartbeat and registry traffic in the §6.1 table.
- **Pushes and heartbeats share one endpoint.** `ControlHints::serve`
  refused every frame but `ControlChanged`. `AdminEndpoint` now dispatches
  by kind, answering `NodeHeartbeat` only from node certificates (an
  `admin` tool certificate may send admin messages but has no node to
  record), and `ControlHints::serve` delegates to it unchanged. In the
  simulation both live on the push port, 7401, so the heartbeater and the
  pusher's peer sink move registered addresses to that port.
- **Zone and rack labels have no configuration keys yet.** The node binary
  does not wire the transport or the coordinator yet (M3-01 notes), so
  `NodeProfile` is built by the caller; `[node] zone` and `rack` keys
  belong with that wiring and §14. The simulation registers alternating
  rack labels and each node's disks, for which `NodeEnv` gained the disk
  labels.
- **The dirty-budget shares stand (§7.6).** Re-checked against the
  registry: load-following shares would put heartbeat delay and
  coordinator failover into admission control, and a partitioned node
  would still need the fixed share. Shares follow the shard registers'
  primaries, which already move with every takeover. No code changed.
- **Plan §14 also assigns admin-listener TLS to M3-02.** TLS for the admin
  HTTP listener and `admin` certificates as an alternative to its bearer
  token are unrelated to the registry and would push this PR past size M,
  so they are left open for a follow-up task.
- **A local heartbeat must check the node's own tenure.** The first
  simulation runs found a node judged suspect while it ran: it had looked
  up the coordinator while it held the lease itself, recorded its
  heartbeats in its own registry, and never looked again after another
  node took over. The local path now answers "coordinator" only while the
  node's own leadership says so, as the endpoint does for remote nodes.
- **Pushes to nodes that are down slow placement.** The coordinator awaits
  each change's push, and a push to a crashed node waits out its timeout
  (1 s in the simulation), so with two nodes down every change takes over
  a second, and re-homing the stand-in registers of a crashed coordinator
  took several seconds. The forgetting scenario allows for it; pushing
  in the background, or not waiting for suspect nodes, is left for when
  placement makes many changes in a row (M3-05, M3-06).
- **Numbers.** Over 64 seeds: four nodes joined with no peer list, every
  announcement after the clients started reached every node by push
  within 300 ms, and the last coordinator judged every node live; with
  `node_forget_after` at 2 s, the node that held no shard was forgotten
  after 2 s to 9 s of silence (longer when it had been coordinator and
  its stand-in registers had to be re-homed first), the node that held
  data shards never was, and the forgotten node registered again on its
  return.

### M3-03 Placement engine

- **Unlabeled nodes and reused rack names were undefined.** §6.7 names the
  levels but not what a node without a `rack` (or `zone`) label is at
  that level, nor whether rack names are scoped by zone. Treating an
  unlabeled node as a domain of its own could put two members in one
  physical rack, so at the `rack` and `zone` levels such a node never
  receives members and cluster health lists it under `unlabeled`. Rack
  labels are cluster-wide names: equal names in different zones count as
  one rack, which can only make placement more cautious. Recorded in
  §6.7.
- **Placing a bucket's shards one by one against the same loads would
  stack them.** With equal nodes, every shard of a new bucket would pick
  the same least-loaded nodes. `Topology::place_bucket` counts each
  placed shard into a private copy of the loads before placing the next,
  and ties between equal nodes break by a hash of the shard and the node
  (rendezvous), so 255 shards of 3 replicas over 9 equal nodes give every
  node exactly 85 members and 28 or 29 primaries. The hash is FNV-1a with
  a SplitMix64 finish, not `DefaultHasher`, so the result is the same on
  every build and coordinator.
- **The admin API cannot show placement health yet.** The node binary
  does not run the coordinator, and only the coordinator judges policy.
  `PolicyWatch` publishes a `PolicyReport` to a `PlacementHealth` handle
  before each placement round, and §12 now fixes the `placement` object
  of `GET /v1/health` that serves it; the binary wires it with the
  coordinator. Shard members in bucket status wait for the multi-node
  binary too (M3-04).
- **Shortfalls are judged by healthy domains.** A shard is reported short
  when its members not on departing nodes span fewer than `replicas`
  domains, so a lost rack shows up once its nodes depart, before
  replacement (M3-05) removes them. Only members whose separation can be
  shown count: on a registered node, not departing, with the label the
  level needs. The first version counted an unlabeled or unregistered
  member as a domain of its own, so two unlabeled members in a `rack`
  cluster could make a shard look whole, contradicting the rule that an
  unlabeled node cannot be shown to be apart from any other (found in
  review). An unregistered member is a node the coordinator forgot,
  which it does only after marking it departing, or one that never
  joined, so it counts for nothing either.
- **The `departing` mark arrived mid-task.** M3-02's review fix forgets a
  node in two rounds and marks its registration `departing` first. A
  `Candidate` built from a marked registration is `Departing` whether it
  comes from the registration alone (a gateway checking a new bucket) or
  from a registry entry, so the marked node receives no member, does not
  count towards satisfiability, and its members count as missing in the
  health report.

### M3-04 Automatic bucket and shard creation

- **The gateway writes the registers, not the coordinator.** §4.1, §6.1,
  and §11 already have CreateBucket write the bucket register from the
  gateway, and §6.1's lost-answer rule names CreateBucket as the
  proposer. Sending creations to the coordinator would have needed a new
  request message, a wait on its tenure (seconds while the lease moves),
  and a way to report the outcome back. The gateway instead reads the
  node registrations, places the shards with M3-03's `Topology`, and
  applies one `ChangeSet` through M3-01's change path: the bucket
  register, then each shard register, all `If-None-Match: *`, one
  generation increment. The cost: the gateway knows only registrations,
  so every node counts as live and empty, except one whose registration
  M3-02 marked `departing`, which gets no member. Within a bucket the shards
  still spread evenly (rendezvous ties); across buckets the load evens
  out only with rebalancing (M3-06). Recorded in §11.
- **The bucket register goes first, which reverses §11's single-node
  order.** On one node, shards are opened before the register so a
  bucket is never visible without them. Writing shard registers first on
  a cluster would leave orphans whenever the name is taken, and a
  creation cut short between the two would need a grace period before
  anyone could tell an orphan from a creation in progress. With the
  bucket register first, a lost name writes nothing, and every shard
  register has a bucket register written before it. The coordinator's
  `BucketShards` placement then finishes creations cut short (missing
  shard registers) and deletes shard registers whose bucket ID no bucket
  register names (left by DeleteBucket), acting only on what two
  consecutive scans showed. Because `ClusterScan` lists `buckets/` before
  `shards/`, the second scan's bucket listing comes after the first
  scan's shard listing, so no timing assumption is needed. A creation
  still in progress may look incomplete twice; the gateway and the
  coordinator then race on `If-None-Match: *`, and the gateway keeps a
  shard register another writer created and sends the rest. Recorded in
  §6.7 and §11.
- **A register that does not parse must stop deletions.** `ClusterScan`
  skipped bucket registers it could not parse, so a bucket register of a
  newer format would have made its shard registers look orphaned, and
  the coordinator would have deleted them. The scan now counts what it
  could not parse (`ClusterScan::unreadable`), and `BucketShards`
  deletes nothing while that count is not zero.
- **Settling a failed shard write can land it.** A test expected a shard
  register whose write failed with an error to stay absent. `Fault::Fail`
  counts as possibly applied, so `apply` keeps it as a `Pending`, and
  `create_bucket` settles it, which sends it again and writes it. The
  writes after it are never sent, and the coordinator completes the
  bucket. The test now expects this.
- **A creation reported done could leave its announcement owed
  (review).** `create_bucket` settled a `Pending` once. If that failed
  too, for example a generation increment that ran out of retries, it
  only logged the error and still returned `Created`. The gateway then
  answered `200` and dropped the only handle on the owed announcement.
  Nodes refresh only when the generation moves, so they could miss the
  bucket, or a late shard register, until some unrelated change. Such a
  creation is now `CreationError::Unsettled`, carrying the `Pending`.
  The gateway answers `503` and keeps settling it in a background task,
  pausing from the retry policy's longest backoff up to 30 s, until the
  generation is incremented. A retried CreateBucket finds the register
  and announces it as well. The same gap was in every other gateway
  announcement, DeleteBucket's included, and in the M1 path, whose
  failed increments were left for "the next change". All of them now
  retry in the background. What is owed lives in memory and ends with
  the process, as after a crash between a write and its increment.
  Making it durable would need a record of owed announcements. That is
  left open, as is the coordinator's own pending list (M3-01). The
  coordinator test fails without the fix, and so do the gateway tests,
  one for a creation and one for a deletion. Recorded in §11.
- **DeleteBucket is unchanged on the gateway; detaching drops shards in
  two places.** The gateway seals, refuses, and deletes the bucket
  register as before. The shard registers go when the coordinator deletes
  them. Replicas go on the deleting node at once; other members drop
  theirs only at their next start (startup recovery). Dropping them at
  runtime when a node's catalog loses the bucket was left out: a catalog
  read from the node's local copy can lag a bucket this gateway just
  created, and removing shards on that evidence could lose data.
- **Seals across nodes (§4.1 left them to this task).** A crashed
  gateway's seals stay in place: lifting a seal without knowing whether
  its delete applied could acknowledge a write into a deleted bucket. A
  retried DeleteBucket through any gateway seals again (seals nest) and
  finishes the deletion. Seals are not carried across a primary change.
  That gap belongs with takeover (M2-12), and §4.1 says so.
- **A policy the cluster cannot satisfy answers `400 InvalidRequest`.**
  The design said only "rejected". S3 has no code for this. `503` would
  have SDKs retry something that does not change until an operator adds
  nodes.
- **How the simulation shows it.** `ClusterConfig::create_buckets`
  writes no bucket or shard registers. Instead, a client creates each
  bucket through a different node's gateway (`ShardPlacement::Cluster`).
  It retries on `503`, and accepts `409 BucketAlreadyOwnedByYou` after a
  lost answer. It waits until every node answers HeadBucket. The workload
  then sends every request to a random node. The services are
  `CoordinatedServices<RoutedServices>`, with nodes that register
  themselves and a coordinator running `BucketShards` in place of
  M3-01's stand-in placement. The routing audit used to index the
  static placement and now falls back to epoch 1 for created shards.
  Without faults, no write fails, through a `local` and a `write_back`
  bucket on four nodes with three replicas. Under control-store faults,
  crashes, and message loss, the histories stay linearizable and
  durable on every member. The fault-free scenario passed 40 seeds
  and the faulty one 80 (`SKYS3_SIM_SEEDS` 256, and 1,024 from seed
  1,000).
- **Left for later.**
  - **Binary wiring.** The node binary still serves every shard alone
    (`ShardPlacement::Local`). It does not run the coordinator, register
    nodes, or wire the transport (M2-08 and M3-01 notes). Switching it to
    `ShardPlacement::Cluster` belongs with that wiring, and so does a
    `failure_domain` from `[cluster]` for the gateway.
  - **Announcing to other nodes.** A gateway's creation is announced only
    by the generation. Only the coordinator pushes. So on S3 control
    stores, other nodes learn of a new bucket within
    `config_poll_interval` (30 s by default), and until then they answer
    `404 NoSuchBucket` for it. The simulation polls every 500 ms. Pushing
    from the gateway, or reading a missing bucket's register before
    answering `404`, should come with the binary wiring.
  - **Load-aware placement at creation.** See the first point.
  - **Runtime dropping of a deleted bucket's replicas on other members**,
    and the seal gaps above.

## M6 Native peer transport

### M6-01 Peer protocol messages

- **`DATA` "at an offset" cannot address multipart uploads.** Design §7.8
  gives `DATA` an offset only. A multipart upload's parts arrive
  concurrently and in any order, and the final part boundaries are not
  known until `CompleteMultipartUpload`. A re-uploaded part must also
  replace the old one. Staged bytes are therefore addressed by *piece* and
  offset. A piece is the body of a single PUT, or one part, and the source
  gives it an ID that it never reuses. `COMMIT` lists the pieces of a
  multipart object with their part numbers, sizes, and MD5s. The decision
  is recorded in design §7.8.
- **The precondition needs a third case.** The design gives a `COMMIT`
  precondition as "the destination's expected current write identity, or
  absent". `flush_conflict_policy = "overwrite"` needs a write with no
  condition at all, so the precondition is one of `Absent`, `Matches`, and
  `Unconditional`.
- **`RESUME` had no trigger.** The table says that after a reconnect the
  destination returns its durable ranges, but no message asks for them.
  The destination now answers every `BEGIN` with a `RESUME`. For new
  staging the `RESUME` is empty, and a source can stream `DATA` without
  waiting for it.
- **`peer_frame_bytes` had no upper bound.** A destination stages each
  frame as one log record, so the configuration now limits it to the
  largest record payload (16 MiB), as `extent_bytes` is limited.
- **The intra-cluster frame was not reusable.** `skys3-net`'s `Frame`
  keys its header on `MessageKind`, a closed enum whose kinds the
  transport authorizes per node role. The peer protocol keeps the same
  layout, two length prefixes, a `prost` header, and a raw payload, but
  has its own envelope: a protobuf `oneof` of the nine messages, whose
  field numbers are never reused.
- **Size.** The plan sizes this task S (under 500 lines). The crate has
  about 1,300 lines of non-test code, not counting comments. Most of it
  is the `prost` wire structs and the checked conversions to typed
  messages.
- **Batch items could share a write identity (review).** `BATCH`
  validation refused two items of the same key, but not two items of
  different keys with the same write identity. Each item's `APPLIED`
  names it only by its identity, and a destination treats a known
  identity as a `COMMIT` replay. The second item could therefore get the
  first one's stored result and never be applied. Validation now
  refuses repeated identities too, on encode and decode. The proptest
  batch generator gives each item a distinct identity. A proptest that
  copies one item's identity onto another, and a rules test, both fail
  without the check.
