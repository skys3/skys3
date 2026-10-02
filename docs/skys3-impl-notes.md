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
