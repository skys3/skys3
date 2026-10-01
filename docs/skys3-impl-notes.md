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
- **Parameter names are decoded as `s3s` decodes them.** M1-06's review
  fixes made the limit checks decode the query with `serde_urlencoded`, as
  `s3s` does. The authenticator first matched raw names, so
  `X-Amz-%53ignature` would have escaped presigned detection and
  stripping while `s3s` still saw a signature (and answered `501`). Every
  decision on a query parameter's name (presigned or SigV2, which values
  are the signing parameters, which pairs to strip, which pair the
  canonical query leaves out) now uses the pair decoded the same way;
  values decode as in a form, so a raw `+` in a token is a space. The
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
