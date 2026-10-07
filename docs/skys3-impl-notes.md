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
- **A codec proptest failed on a location, not on an import.** CI on a
  later PR shrank `arbitrary_bytes_decode_canonically` to `[1, 0 × 20]`,
  re-encoded as `[3, 0 × 20]`, and the import codec looked like the cause.
  It was not: `decode_import` rejects those bytes (trailing bytes after
  `Running { after: None }`). They are a format 1 record location, which
  `decode_location` accepts and re-encodes in format 3, as the codec
  documents for every older value. The property dates from M1-03, when
  format 1 was the only format; once formats 2 and 3 existed, any 21
  random bytes starting with 1 or 2 failed it, about one run in two hundred.
  It now checks what the `index_codec` fuzz target always did: a value of
  the current format re-encodes byte for byte, and an older one re-encodes
  in the current format to the same value. A unit test keeps the case.

### M1-20 Read-through fill and ADOPT

- **Nothing evicted payload yet.** Imported and adopted stubs are evicted,
  but no clean entry could become one, and the design left open how the
  move between clean and evicted is recorded. Both directions are now
  node-local and made by no record (design §4.2): `Shard::fill` and
  `Shard::evict`, through a new `Index::update_local` that commits like an
  apply without moving the applied position. Each names the version it was
  decided for and is refused once the entry holds another, so a write that
  committed in between wins. `Shard::evict` is the transition only; the LRU
  policy and its budget are M1-21's. It refuses multipart objects: an
  evicted stub has no place for part boundaries, which `partNumber` reads
  and a later `TAGS` flush need, so M1-21 has to add one before it evicts
  them.
- **Where filled bytes go.** The design says only that they become clean
  cache. A fill commits them as `EXTENT` records of the shard, like a
  PUT's body, so the location map, compaction, and the gateway's extent
  streaming all apply unchanged. The cost is that fill extents take shard
  positions and are refused while the shard is sealed (a read then answers
  `503` until the DeleteBucket ends), and they are replicated to every
  member (see the replication bullet below).
- **The store reads whole bodies, and returned no `Last-Modified`.**
  `ObjectStore::get_object` returns one `Bytes`, so a fill reads ranges of
  at most 8 MiB (`FILL_CHUNK_BYTES`), each with the same `If-Match` and
  `versionId`, which also bounds its memory; the fills of a target hold at
  most `flush_max_inflight_bytes_per_target` at once, a budget beside the
  flushers' own. Ranges are
  served as soon as the extents covering them are applied, so a read far
  into a large object waits for the fill to reach it. `ObjectInfo` gained
  `last_modified_ms` for the `ADOPT` record; the AWS client fills it, and
  `SimS3`, which keeps no times, leaves it out, so the primary's clock
  stands in.
- **Review: extents larger than a chunk.** A read was a whole number of
  extents, at least one, so `extent_bytes` above 8 MiB (the configuration
  allows 16 MiB) made each GET one 16 MiB extent, past the chunk limit,
  and a GET larger than the in-flight budget took the whole budget and
  still held more. A fill's extents are now at most 8 MiB, and a read is
  the most whole extents that fit both the chunk and the budget. Extents
  are not cut below that to fit a smaller budget: the index holds at most
  81,920 extents per object, which 64 KiB extents of a 5 GiB object
  reach, so a budget smaller than one extent still admits one extent at a
  time. Regression tests fill two objects at once with 16 MiB extents,
  and with a budget of two and a half extents, and check every GET and
  the bytes all GETs ask for at once.
- **`versionId` pins the version.** The plan's "Done when" reads as if a
  fill always notices an out-of-band write, but with `versionId` a GET of
  the named version succeeds after one. A versioned remote therefore keeps
  serving the version SkyS3 knows, which matches its metadata, and adopts
  only once that version is gone (`NoSuchVersion`, for example after a
  lifecycle rule expired it). Design §9.2 says so; the simulation expires
  versions on versioned seeds to reach the `ADOPT`.
- **An out-of-band delete cannot be adopted.** `ADOPT` always makes an
  evicted stub, and no record removes a clean entry. When the HEAD after a
  failed fill finds no object, the conflict is counted, the read answers
  `503`, and the stub stays until a write replaces it (design §9.2).
  Adopting the delete as a tombstone would need a new record or a `DELETE`
  that SkyS3 itself would then flush; neither seemed right for M1.
- **How the gateway reaches the fill.** The gateway does not depend on
  `skys3-flush` or `skys3-remote`, so it defines a `Fills` trait (a boxed
  future and an `mpsc` body of `io::Result<Bytes>`), set in
  `GatewayConfig::fills`. The node implements it over each bucket's
  `Filler`, which `FlushService` keeps beside the bucket's flushers and
  creates before the probe finishes (reads need none), so the flush
  service is now built before the gateway. A GET resolves the key again
  after `FillError::Changed`, at most three times. Because the filler's
  counters are taken when the node starts following a bucket, the flush
  counters' series now appear then too, not after the probe; the metrics
  reference says so. `FlushSettings` gained `extent_bytes`, which the node
  sets from `[storage]`.
- **Fills and the dirty budget (M1-17).** A fill commits `EXTENT` records,
  but its bytes are clean cache: the flushers count only dirty versions,
  so fills never count against `max_dirty_bytes`, and the budget does not
  hold them back, since refusing a read would not drain it. They do take
  disk space, so the node does not fill a shard whose disk, or the data
  directory, is below `disk_min_free_bytes`: the read answers `503`, as a
  write would. Bounding the clean cache itself is M1-21's.
- **The simulation needed a deliberate race.** Random interleavings almost
  never commit a local write while a fill that will adopt is at the
  remote, so a mutation that applies `ADOPT` over a dirty entry passed 300
  seeds. The scenario now also stages that race (out-of-band write, a
  delayed fill, a local PUT while its GET is in flight) and requires the
  key to end at the local write. With it, that mutation fails at seed 0;
  a fill without its `If-Match` and `versionId`, and a fill that caches
  over a changed entry, fail at seed 7. 300 seeds take about 30 seconds.
- **Rebased onto replication (M2-07 to M2-17, M3, M6).** The fill was
  written for a shard alone; on the replicated stack it now runs only on
  the serving primary. The `Filler` reads the entry through
  `Shard::entry`, which admits reads only on a serving primary with its
  leases, so a member, a learner, a primary still reconciling, one that
  stepped down for a handoff, and a stopped replica refuse before any
  remote request (a new test covers the stopped case). The `EXTENT`
  records and the `ADOPT` go through `Shard::commit`, so the primary
  sequences them and they replicate like any record; members apply the
  `ADOPT` alike, its `expected_seq` check included. `Shard::fill` now
  checks the replica serves, as `commit` does, and not just that it runs:
  its payload must be records this replica sequenced as the primary it
  still is. `Shard::evict` still only needs a running replica, since it
  drops a payload of the replica's own. Learner snapshots already turn
  clean entries into evicted ones, which a later primary fills on demand.
  No index or log format changed; the index stays at format 6.
- **Open: fills on the members and across nodes.** The members hold the
  fill's `EXTENT` records in their logs, but their entries stay evicted,
  so the bytes are dead weight until compaction (M1-22) reclaims them;
  §9.2's "clean cache on up to `clean_copies` members" needs either a
  node-local fill path or members that make the entry clean when the
  primary does, which is §9.3's (M1-21). The node binary still serves
  every shard alone, so `NodeFills` fills the local replica only. On a
  routed node the gateway may resolve an evicted entry on another node's
  primary, and the local fill then answers `503`; forwarding fills to the
  primary needs a `Request` variant in the routing wire format.
- **Fills and the namespace import (M1-18).** An `ADOPT` from a fill
  stored a `Content-Type` only when the remote returned one, so an
  adopted object without one looked like an imported stub whose metadata
  was never loaded, and the next read would HEAD the remote and commit a
  second `ADOPT`. The fill now builds the metadata with the import's
  `loaded_metadata`, which defaults to `binary/octet-stream`, as design
  §9.1 requires of every `ADOPT`. In the gateway, a GET of a key only at
  the remote reads it there; any other key goes through the import's
  lookup, which loads a stub's metadata first, and then reads local bytes
  or fills. The lookup now also returns the entry's version, which the
  fill names.
- **Left open.** A copy whose source is evicted still answers `503`
  (§7.2's remote `CopyObject` row would avoid reading the bytes at all). A
  `TAGS` change of an evicted stub still cannot flush, since the flusher
  needs the bytes: reading them from the remote under `If-Match` would
  need its own conflict handling in the flusher. The node's `Fills` is
  wired in the binary but, like the flusher, not exercised end to end: no
  S3 server in the tests honors preconditions, so the `Filler` runs against
  `SimS3`, and M1-26 runs it against real providers.

### M1-21 Clean cache and eviction

- **Fills on members stay on the primary (M1-20's open item).** A member
  sees a fill only as `EXTENT` records of the key, which look exactly like
  the extents of a PUT body still on its way, so it cannot tell that they
  hold the version its entry names; linking them would need a record,
  which the node-local Clean ↔ Evicted rule forbids, or a log format
  change. Only the primary serves reads until read plans (M2-18), so a
  member's filled copy would serve nothing but a later takeover, where
  the new primary fills on demand anyway. A fill therefore keeps one copy,
  the primary's, whatever `clean_copies` says, and design §9.2 now says
  so. Copies on more members come from flushes: when a `FLUSHED` makes an
  entry clean, each replica keeps the payload only if its rank (primary 0,
  then the other members in configuration order; learners none) is below
  the bucket's `clean_copies`, and evicts it at once otherwise.
- **Multipart objects are evicted after all: index format 7.** M1-20
  refused them because a stub had nowhere to keep part boundaries. The
  entry's `Payload::Parts` already holds them, so an evicted multipart
  object keeps that payload and its part rows, with ETags and checksums,
  and only the rows' payloads become `Payload::None`. The codec refused a
  part without bytes, so it now accepts one, and the index format went
  from 6 to 7 because older builds would fail to decode such a part. A
  fill cuts its chunks at the part boundaries and gives each part its own
  extents back. The gateway used to decide "fill" from `Payload::None`; it
  now asks whether the entry is evicted, and a part without bytes answers
  `503` like any version whose bytes are not cached.
- **Eviction frees no disk space until compaction (M1-22).** The §9.3
  capacity model with `reserve_fraction` reads each disk's free space, but
  a byte evicted stays in the log until compaction reclaims it, so a disk
  over its reserve would have evicted its whole cache, one refresh after
  another, without gaining a byte. A disk's room for clean payload now
  counts what this node evicted from it since it started as free:
  available + cached + evicted − `reserve_fraction` × size. Evicting
  leaves the room unchanged, so the cache gives back exactly the
  shortfall. After a restart the evicted payload counts as used again,
  which is conservative; M1-22 should tell the cache what compaction
  reclaims. The free-space watcher of M1-17 now also feeds the cache, so
  it reads `statvfs` even when `disk_min_free_bytes` is 0;
  `skys3_io::disk::space` returns the size with the free space.
- **Reports to the cache had to be ordered.** The cache keeps in memory
  which entries are clean, from reports the replicas send after each
  change. A fill reported its Clean after its index commit, so a write
  applied in between could report Gone first and leave the cache counting
  a dirty entry as clean (and it then could not count it out). Every change
  the cache follows (applied records, fills, evictions, and the scan a
  replica gets when it opens) now runs under one lock per replica together
  with its report, so the cache sees them in index order. The eviction
  itself never trusts the cache: it names the version and refuses anything
  not clean at it.
- **The simulation needed deliberate races.** The seeded scenario first
  passed with an eviction that checked neither the version nor the state,
  because the evictor almost never interleaved with a write. It now also
  evicts at whatever version an entry holds, in any state, and stages
  "evict a version a write has just replaced"; it compares the index with
  a replay of the log up to which clean payloads were kept. With those, a
  check-free eviction and a state-blind one fail at seed 0. 256 seeds take
  about a minute in a debug build, so the scenario is declared at cost 4.
- **The cluster harness had no fills.** Evicted keys would have failed
  the final reads, so `ClusterConfig::clean_cache` turns on the cache and
  fills (`SimFills`, the node binary's without the free-space check). With
  a cache, the durability checker counts a dirty copy whose bytes its node
  does not locate as no copy, so `every_member_durable` checks that no
  member evicted dirty payload, also across crashes and power loss.
- **`TAGS` over an evicted member copy.** `TAGS` keeps the bytes of the
  version it changes; on a member that dropped its copy beyond
  `clean_copies`, the new dirty version has none. Nothing is lost (the
  bytes are the remote's), but it widens M1-20's open item: such a version
  cannot be flushed from that replica until the flusher fills before it
  flushes. Design §9.3 records it.
- **No copy is evicted before its bucket's policy is known.** Review found
  that a restarted node opened its replicas, and the cache scanned them,
  before the node first read the buckets' `clean_copies`: under the
  default of one copy, every member copy beyond rank 0 went, for good,
  since a higher setting brings none back. A bucket made at runtime had
  the same gap until the next round of the flush follower. The cache now
  keeps every copy of a bucket it was not told about, and evicts the extra
  ones when it is told, or when the setting drops; the node and the
  cluster harness also install the policies before the cache runs and
  before each round's reconcile awaits.
- **Left open.** A learner's snapshot still turns clean multipart entries
  into stubs without parts, as before. A new primary does not revisit
  copies already kept; LRU reclaims them. Only
  reads on the primary count as uses, since members serve none yet; read
  plans (M2-18) and the hot cache (M2-19) change that.

### M1-22 Segment compaction

- **Liveness needs the index, not just the location map.** Applying never
  removes a location for a replaced version (a read may still stream it),
  so the location map alone says nothing about what is live. A record is
  live when the map locates it in the segment *and* an entry, a completed
  object's parts, or a part of an open upload names its position;
  `IndexReader::holders` answers that per key. The index format stays at
  7: compaction adds no table, and its one durable commit per chunk only
  moves and removes locations. `Index::update_durable` commits with
  `Durability::Immediate`; a non-durable commit there lost data at seed 0
  of both simulations, because the segment file is removed right after.
- **Unnamed extents wait instead of being copied.** `EXTENT` records no
  entry names are a body still arriving, a failed upload, or a peer's
  staging (§7.8). Copying them would rewrite garbage forever, so their
  segment waits until it has been sealed for `peer_staging_ttl_seconds`
  and they are then dropped. The log tracks when it sealed each segment,
  in memory; a segment recovered at startup counts from startup, which
  only delays reclamation. A member's copy of a fill's extents, which
  nothing on the member names (M1-21), is dropped the same way.
- **Replicas hold what their members may lack.** Replay applies a
  replicated shard's durable tail past the commit watermark, so a
  segment's records can be applied and released while a member that is
  behind still needs them sent from this log. `Shard::replicated_through`
  is the highest commit watermark the replica learned since it opened
  (`None` for a replica alone); records after it keep their segment, as
  do records of a shard not open on the node. The unit test that opens a
  replicated shard whose member never reports catches dropping this hold;
  the cluster simulations do not, as the window is short there.
- **Dropping a `TRUNCATE` broke reading the tail.** Once compaction drops
  a `TRUNCATE`, the records it invalidated may still be in an older
  segment, so `read_tail` found two records for one `seq` and failed.
  It now keeps the newest epoch for each `seq` (a copied record, found
  twice, is alike). Design §10.3 records the rule.
- **The latest `CONFIG` is copied, though the index also keeps it.** The
  plan asks for it to stay reachable in the log; the record matching the
  index's `configs` row is copied and older ones are dropped. Both
  simulations check every node's log for each shard's latest `CONFIG`.
- **Hot clean payload is the most recently used half.** The cache ranks
  entries by last use per disk; compaction copies a clean payload only if
  it is among the most recently used entries holding half of the disk's
  cached bytes, and only while the cache is within its bounds. A node
  without a clean cache copies clean payload, since it never evicts.
- **The cache now hears what compaction reclaims (M1-21's open item).**
  `CleanCache::reclaimed` lowers the evicted bytes a disk's room counts as
  free by every payload byte compaction drops from that disk, evicted or
  replaced. That can count less free than there is, never more.
- **Retired segments stay readable for a minute.** A read that located a
  record just before compaction retired its segment keeps the file handle
  for `RETIRE_GRACE` (60 s), so streaming a GET is not cut off; the files
  are already unlinked and synced out of the directory. Review found two
  gaps: the segment left the listing before it joined the retired ones,
  so a read in between failed, and scans did not look at retired
  segments, so `read_tail`, which lists the segments and scans them one
  by one, could hit one compaction had just retired and abort a
  reconciliation; skipping it would lose records copied into a segment
  created after the listing. The move is now one step under the segments
  lock, scans see retired segments within the grace, and `read_tail`
  takes every scanner at once (`SegmentLog::scan_all`), each holding its
  file, so no time bound applies to it. One test catches the first gap
  with a clock that scans the segment each time `retire` reads the
  clock; another scans a listed segment after retiring it.
- **Teeth.** The shard simulation runs 2 to 4 process lives of writers
  over overwritten keys, multipart uploads, and `TAGS`, cuts the power at
  a random sync inside a compaction pass in 70% of seeds, and races a
  `TAGS`/`FLUSHED` writer against compaction in half of them; it compares
  the index with a replay of the model and checks every named payload's
  bytes. Copies without new locations, a non-durable index commit, a
  dropped `CONFIG`, and a relocation that ignored a racing write each
  fail it; the last only with the racer. Classifying dirty payload as
  droppable does not, because the relocation's re-check keeps the
  segment: that check is the safety net, not the classification. With
  `SKYS3_SIM_SEEDS=256` in a debug build: the shard scenario (cost 8, 32
  seeds) takes 33 s; the cluster scenarios take 29 s (crashes, message
  loss, and a partition; cost 32, 8 seeds) and 38 s (power loss at four
  syncs of a base run; cost 128, 2 seeds of five runs each).
- **Left open.** Every pass scans each released segment whole to measure
  its live bytes; per-segment live counters would avoid that, at the cost
  of keeping them across crashes. Segments of a shard dropped from the
  node are still never released (§10.2), so compaction never sees them.
  Compaction has no rate limit of its own beyond chunking (32 MiB or 4096
  records per durable commit). Fragment segments (M7) are not handled.

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

### M1-25 SDK matrix

- **The matrix needs HTTPS.** botocore sends `aws-chunked` bodies with
  trailing checksums only over TLS, and plain HTTP would have left the
  trailer forms untested. The node serves the test certificate from
  `crates/skys3/tests/data/`, and every client is told to trust its CA:
  `AWS_CA_BUNDLE` (Python, Go, CLI), `NODE_EXTRA_CA_CERTS`, a TLS context
  (Rust), and, for Java, a PKCS12 trust store the client writes at start
  and makes the JVM default, since the STS client inside the web-identity
  provider ignores any HTTP client the test configures. Every SDK client
  checks that at least one upload went as
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER`.
- **Getting an issuer and a role into the binary.** The node fetches OIDC
  documents over HTTPS from the system roots only, and nothing writes
  identity registers but the control store's own API. The test starts the
  binary with `SSL_CERT_FILE` naming the test CA (which
  `rustls-native-certs` honours), runs its own issuer with the test
  certificate (`tests/support/issuer.rs`), and writes the provider and
  role registers as files into the file control store while the node is
  down: it starts the node once to create the store, stops it, writes
  them, and starts it again, since the store reads its files when opened.
- **Sessions cannot be short, so each SDK is told to refresh early.**
  900 seconds is the STS minimum, too long to wait for in CI. Go's
  `ExpiryWindow`, Java's `prefetchTime` and `staleTime`, and a `memoize`
  around JavaScript's `fromTokenFile` (the default chain's own cache
  refreshes five minutes before expiry and cannot be told otherwise)
  refresh five seconds after issue. botocore (Python and the CLI)
  refreshes within 15 minutes of expiry, so with 900-second sessions every
  request refreshes. The Rust SDK's identity cache jitters its buffer by up
  to half, so no buffer gives a short fixed refresh age; a buffer of twice
  the session makes every request due, as in botocore, while the cache
  still loads one session at a time. Under load that is about one STS
  call per request: locally, Rust issued 475 to 1,700 sessions in 20
  seconds, each a `PUT` in `sys-sessions`, without a failed request.
- **Refresh must be proven per provider, and with expiring tokens.** A
  review found three ways the first version could pass without a
  refresh. JavaScript seeded its set of keys with the default chain's
  first key, so the tested provider's own first session made two. The
  CLI's workers have their own caches, so their first sessions made
  eight between them. And tokens lasted 60 seconds plus the node's 60
  seconds of skew, longer than the load, so a client that never reread
  the token file still worked. Now every client seeds its keys from the
  provider or cache under test (each CLI worker needs two of its own),
  and the node accepts a token for at most seven seconds
  (`oidc_clock_skew_seconds = 1`, tokens of six seconds) while the file
  gets a new one every two, written atomically since clients read it at
  any moment. The Rust client checks after the load that the token it
  read before it is refused with `ExpiredTokenException` and the current
  one accepted, so the clients that kept working reread the file.
- **SDK defaults differ.** botocore presigns with SigV2 unless told
  `signature_version="s3v4"`, which SkyS3 refuses as designed. The CLI's
  default checksum is CRC64NVME (it bundles the Common Runtime), the
  others' CRC32. Java needs `aws-crt` for CRC32C and CRC64NVME, and
  Python `botocore[crt]`. The SDKs' checksum enums now also list MD5,
  SHA512, and XXHASH algorithms, which SkyS3 answers with
  `501 NotImplemented` (design section 7.4); the clients test the five
  design section 11 names.
- **The CLI's session cache is not safe across processes.** Concurrent
  `aws` processes sharing a home failed with a `KeyError` on the cache
  file under `~/.aws/cli/cache` that another process was replacing. Each
  load worker of the CLI client has its own home.
- **s3-tests cleans up with versioning APIs.** Before and after every test
  it lists object versions, which SkyS3 rejects with `501`, so every test
  failed in teardown. A pytest plugin (`skys3_s3tests.py`) lists with
  ListObjectsV2 instead; nothing else about the tests changes. A full run
  of `test_s3.py` and `test_headers.py` at the pinned commit passed 236
  tests; the subset is the 209 of them that do not pass only by accident
  or slowly, and `subset.txt` lists why the rest are out. No failure was
  a gateway bug that this PR could fix in scope: most are rejected or
  unbuilt features, and the remaining differences (the encoding of a
  space as `+`, an echoed empty `Delimiter`, re-completing a completed
  upload, `BadDigest` for a malformed whole-object checksum, and others
  `subset.txt` names) are design decisions or need checking against AWS
  S3 first, which M7-01 owns.
- **Rust runs in the test, the rest in images.** The Rust client is part
  of `cargo test -p skys3 --test sdk`, so it runs in every `rust` and
  `coverage` job too; an image would rebuild `aws-sdk-s3` for versions
  `Cargo.lock` already pins. The other clients run in images built from
  digests of pinned tags, with versions pinned by `requirements.txt`,
  `go.sum`, `package-lock.json`, `pom.xml`, and the s3-tests commit.
- **Run locally without containers.** The docker daemon was not running in
  the sandbox, so the images were never built here: CI's `sdk` job is
  their first build. Every client ran on the host instead
  (`SKYS3_SDK_LOCAL=1`), with the same SDK versions but other runtimes:
  Python 3.11, Go 1.24.7, Node 22.22, Ubuntu's OpenJDK 21 with Maven
  3.9.16, and AWS CLI 2.37.8 from its installer. All seven passed at once
  in three minutes, the s3-tests subset being the longest at 2.5 minutes.
- **A full disk looks like a broken gateway.** The sandbox's shared disk
  filled up during a first full s3-tests run, and the node answered
  `503 SlowDown` (admission control's free-space reserve) to most of the
  later tests. The runs counted above were made with enough space.
- **Test ports can be taken before the binary binds them.** In one full
  workspace run the matrix's node exited at start; its log was lost, but
  the likely cause is the tests' `free_port`, which binds port 0 and
  drops the socket, so a test running in parallel can bind the port
  before the binary does (also while a node is down between restarts),
  and two calls could even return the same port. `Process::start` now
  recognises an exit whose log says `Address already in use`, rewrites
  the configuration's `[gateway]` and `[admin]` addresses with fresh
  ports, and starts again, five times at most; any other early exit still
  panics with the log. Callers read the addresses from the returned
  `Process`. Readiness waits for the node's own `serving` line before
  probing `/readyz`, with a deadline per probe, because until the node
  holds the admin port another process may answer there, or accept and
  never answer. `free_ports` holds its listeners until all are chosen, so
  a gateway and an admin port never coincide. A test holds a configured
  port and checks the node moves.

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

### M2-12 Primary takeover and reconciliation

- **R1 lives in the grace.** `Grace` gained a stopped state: once
  `primary_grace` has passed and the takeover delay with it, the candidate
  stops it (`Grace::stop_if_passed`), and from then on the member grants
  no lease and acknowledges no append of its epoch. A grant that comes in
  during the delay means the primary is back, and the member stands down.
  A lost proposal resumes it, and so does a session of an epoch newer
  than the one the candidate proposes over, which the grace holds under
  the same lock (`Grace::resume_for`, `Grace::propose_over`): the
  register has then moved past it. A resumed candidate proposes nothing
  until its grace passes again. The random-fault simulation found the
  first version, a plain flag, wrong: a candidate resumed by the old
  primary's session after that primary removed another member, then
  proposed over the removal while granting that primary leases. Takeover is
  opt-in (`Replication::with_takeover`, which needs `with_removal`), so the
  scenarios of earlier tasks run as before.
- **What the candidate proposes (decided, design §6.5).** Epoch `e+1`, itself
  as primary, the other members, no learners, and not the old primary,
  which comes back only as a learner (M2-15). The model allows keeping
  it; leaving it out means a dead primary does not stall the first writes
  until its removal. The delay is `takeover_delay` (400 ms) scaled down by
  the records the member holds past what it applied, plus a quarter of it
  as jitter hashed from the node, the shard, and the epoch.
- **The member becomes the primary in place.** `Shard::take_over` builds
  the `Leader` the replica lacked, behind a `OnceLock` so that the
  pipeline, links, and appender pick it up, switches the role, and
  appends the new epoch's `CONFIG` record at once, after its whole log.
  Unlike a removal (M2-11) it need not wait for the members' reports:
  its log is the one they reconcile to, so none keeps an older record
  past it. The primary serves once that record commits, that is once
  every member holds its whole log, tail included. The replica's watch
  then runs `start_primary`: leases, the removal watchdog, and links.
- **Losers adopt the winner's configuration.** A member that loses the
  compare-and-swap adopts what the register holds, even before the
  winner's session, so it refuses the old primary (R2) and, if the winner
  dies before reaching it, proposes over the winner's configuration a
  grace later. Without that, the simulation found a member proposing over
  epoch `e` forever after the winner crashed. A loser the register no
  longer names stays stopped.
- **A new primary's session restarts the grace.** A loser can learn of the
  winner from the winner's session rather than from its own refused
  compare-and-swap: the session arrives while it still waits out its
  takeover delay, before it stopped granting. It adopted the winner's
  configuration but granted nothing yet, so once the delay ended its
  grace, still counted from the old primary's last grant, had passed,
  and it proposed over the winner at once: a second takeover, epoch
  `e+2`, milliseconds after the first. Leases kept that safe (it had
  granted the winner nothing), but it deposed a live primary, against
  §6.5's "if the winner stays silent for `primary_grace` too". The
  loopback takeover test caught it under CPU load (about 4% of runs).
  `Grace::resume_for` now counts a session from a primary the member
  did not follow before as a grant, as a lost proposal already did
  (`Grace::resume`), under the same lock, and still refuses a session no
  newer than the epoch a stopped candidate proposes over. A session of
  the same primary in a newer epoch (its removal of other members) does
  not restart it, so a candidate still proposes over that at once.
- **The proposal is not recorded (decided, design §6.3).** §6.3 asks a
  candidate to record its proposal before the compare-and-swap. A lost
  answer is retried while the candidate stays stopped, and after a restart
  a node reopens each replica in the register's configuration, which
  tells the outcome. That holds for the simulation and the tests; M2-16
  must keep it, or record the proposal, if it opens replicas from the
  local copy while the register is unreachable.
- **Lineages, not last positions (decided, design §6.6).** The plan says to
  collect each member's last `(epoch, seq)` and truncate past the new
  primary's `seq`. That misses a member holding a different record at a
  `seq` the primary also holds. A `Sync` now carries the primary's
  lineage, the last `seq` it holds in each epoch since the position it
  opened at (`Lineage`, fields 4 and 5 of `Sync`), and the member keeps
  the prefix up to the largest `min(last)` over the epochs both logs
  share (`Lineage::reconcile`). The lineage lives in memory: a replica
  knows its epochs only from where it opened, which is enough as long as
  the match lies after both open points.
- **`TRUNCATE` semantics (decided, design §6.6, §10.1).** A `TRUNCATE` at
  `(E, s)` invalidates every record at `(e', s')` with `s' > s` and
  `e' < E` (`skys3_log::record::truncated_by`). `E` is the primary's
  epoch at `s + 1`, or the new configuration's if the primary's log ends
  at `s`, and every truncated record must be older. Replay
  (`Checkpointer`) and `Shard::read_tail` skip what a `TRUNCATE`
  invalidates, so records keep their place in the shared log and the
  records the primary sends next, at the same `seq`s, are not hidden. The
  index is not rolled back: a member applies only committed records, so
  nothing it truncates is applied, and one that has (after replaying a
  tail past the commit watermark on restart) is diverged. The `TRUNCATE`
  holds a place in the commit pipeline that nothing applies: no record
  queued after it is applied or reported durable until it is durable, and
  if its write or sync fails the member stops and `Shard::truncate`
  returns the error. Every other record, `CONFIG` included, has a writer
  slot whose failure stops the shard already.
- **A diverged member is removed.** A member that applied past the match,
  shares no epoch with the primary to compare by, or whose records past
  the match are not older than the primary's next, refuses the session.
  The new primary's watchdog removes it after `member_suspect_after`, and
  it rejoins as a learner (M2-15). That costs a member in rare cases
  (a member restarted with an unapplied tail, then a takeover by a
  member that lacked it) but never rewrites applied state.
- **Members accept records of the epochs in between.** The new primary's
  uncommitted tail can hold records of epochs newer than a member's but
  older than the new configuration's (rolled forward by earlier
  primaries). `Shard::receive` accepts them in order, and the member
  sequences in their epoch, so its `CONFIG` record still lands at the
  primary's.
- **What a deposed primary answers (decided, design §6.5).** A member's
  refusal now names its epoch; a primary refused for a newer epoch asks
  its watchdog to read the register (`Leader::rejected`). Once it knows
  the configuration that deposed it, it stops and answers
  `ShardError::NotPrimary` naming the new primary, which the gateway
  follows as a redirect, instead of `503`. A primary without a watchdog,
  or without the register, stops and answers `503`.
- **The simulation audits one committer per epoch.** `check_commits` now
  also fails when two nodes acknowledge writes in the same epoch of a
  shard. The commit audit compares each primary's watermark with the
  longest durable run of every member's lives, which a truncation only
  shortens for records that never committed. The routing audit lets a
  primary that a takeover deposed complete, in its own older epoch, a
  request it began before: a write whose members acknowledged it before
  they stopped commits after the compare-and-swap. The record is on the
  new primary too, and the model allows the same (`Commit` after
  `ProposeTakeover`); the commit and lease audits judge such requests.
- **Measured.** With the defaults (6 s grace, 400 ms delay), the shards of
  a crashed primary were served by new primaries 6.35 to 6.52 s after the
  crash over 24 seeds, under the 10 s of §13. The loopback test sees a
  takeover between `primary_grace` and `primary_grace` plus 1.5 s. The
  random-fault scenario (crashes, partitions, held links, control store
  outages, message loss, drift within `ρ`) ran 200 seeds clean after the
  fixes above, as did 24 to 48 seeds of each other scenario.
- **The protocol model needed no change.** `ProposeTakeover` already lets
  a candidate drop the old primary, holds it in `proposed` until it adopts
  the outcome, and `Sync` truncates to the `(epoch, value)` common prefix.
  The implementation refines them: it reconciles by lineage, which finds
  the same prefix, refuses to truncate applied records, and resumes
  granting only when a CAS was refused outright. Those only remove
  behaviors.
- **Left for later.** Wiring: `takeover_delay` has no configuration key
  yet, and the binary does not run replication (M2-16). Planned handoff
  (M2-13) reuses `take_over`. A node partitioned from its members but not
  from the control store, with a `primary_grace` shorter than
  `member_suspect_after` (not the defaults), can take over shards it
  cannot serve; the other members take them back a grace later. A
  pre-vote would avoid that. Seals (`Shard::seal`, as DeleteBucket
  places them) are a counter on the primary's replica only, so a new
  primary from a takeover starts unsealed (M3-04).

### M2-13 Planned handoff

- **A handoff is a takeover from the compare-and-swap on (decided, design
  §5.4, §6.5).** `Replication::hand_off` makes the primary step down; the
  candidate's `Grace` then stops without waiting for `primary_grace`
  (`Grace::step_down`, `Grace::stop_if_passed`), and the M2-12 candidate
  proposes and takes over as after silence. It proposes what a takeover
  proposes, so the old primary leaves the configuration and rejoins only
  as a learner. Keeping it as a member would turn a primary into a member
  in place, which needs the re-admission of M2-15. Until learners exist, a
  shard of `n` members can be handed off `n − 1` times; rebalancing
  (M3-06) adds the old primary back if it should stay.
- **The step-down is in the index (decided, design §5.4, §10.2).** The
  plan does not say where. A `step_downs` table, keyed by shard, holds the
  epoch with one durable commit, written before the message is sent.
  `Shard::open` opens a primary that stepped down in the configuration's
  epoch stopped, so after a restart it never serves in that epoch. The
  table is not a cache, so it bumps the index format to 4, and older
  builds refuse the index rather than ignore it.
- **The old primary drains first.** It stops admitting reads and writes,
  then waits for the reads it already admitted (a counter of reads in
  progress, `Shard::admit_read`), and for the writes it sequenced to commit
  within `replica_ack_timeout`. Without the read counter, a read that
  passed its lease check just before the step-down could read the index
  after the new primary acknowledged a write: the gap the 500 ms margin
  of `primary_grace` covers after silence, which a handoff does not wait
  for. Clients of the writes in flight get their answers. Review found
  that the first version released the sequencer's lock between a read's
  last serving check and its count, so a step-down on another thread
  could see no read in progress and let the candidate take over while
  that read was still to come. Both now happen under one hold of the
  lock. The cluster simulation could not have found it: each simulated
  node runs a current-thread runtime, and nothing awaits between the
  check and the count, so no step-down can run in between there; the
  lease audit would flag such a read if one were served.
- **The message rides the candidate's link (decided).** `StepDown(epoch,
  last)`, the `MessageKind::StepDown` frame of M2-02, goes after the last
  record on the replication link, so it arrives after every append. The
  other links end at once, so the members' grace runs out on its own if
  the message is lost; the old primary waits for the send at most
  `link_timeout`. The candidate proposes at once only once it holds every
  record up to `last` durably; otherwise it ignores the message and waits
  for its grace.
- **A step-down counts once.** A candidate whose proposal loses resumes
  granting and forgets the step-down; otherwise it would propose over the
  same epoch again in a tight loop.
- **No removal moves the register past a step-down.** The first
  message-loss seeds left a shard unavailable for good: the primary
  removed the candidate and every other member (a compare-and-swap in
  flight when the handoff began) and was left the only member of a
  configuration it had stepped down from in memory, so no one could take
  over. A handoff now takes a lock that member removals also take
  (`Leader::changing`), checks that the register holds the primary's
  configuration, and only then steps down; the watchdog removes no member
  after that. A removal whose answer was lost shows up in that check and
  refuses the handoff.
- **The old primary polls the register.** It is no longer a member of the
  new configuration, so no member's refusal tells it about the takeover,
  as it does after one from silence. It reads the shard's register every
  `lease_renew_interval` until it holds a newer configuration, then
  redirects to it; until then it answers `503`.
- **The lease audit checks step-downs.** The M2-09 audit flags a read
  served after some member's grace passed, which a handoff never waits
  for. It now also flags a read served after a member of the shard
  received the step-down of the primary of the read's epoch. A seeded bug
  (a stepped-down primary that still reads its index,
  `ReplicatedServices::stepped_down_serving_reads`) is caught by it.
- **A primary that hands off before the cluster is ready.** The first
  random-fault seeds failed with "the nodes did not start": a handoff
  before every primary served left a replica that never serves, and the
  harness waited for it. `ReplicatedServices::ready` now skips replicas
  that stepped down.
- **Measured.** In the loopback test the candidate serves before its
  600 ms grace could have passed, and the write in flight when the
  primary stepped down is acknowledged. The cluster scenarios hand off up
  to 31 shards per seed through gateways with stale maps. With the fixes
  above, 64 seeds of the stale-gateway scenario, 64 with message loss,
  and 128 with random crashes, partitions, message loss, and drift within
  `ρ` ran clean, and the seeded bug was caught in each of its 64 seeds.
- **The protocol model needed no change.** `StepDown` and
  `ProposeTakeover` already model the durable step-down and the
  candidate's proposal over the epoch it names. The implementation only
  removes behaviors: the drain, the `last` check, and ending the step-down
  when a candidate resumes.
- **Left for later.** The coordinator's `Handoff` admin message and when
  to hand off are M3-06's; the binary does not run replication yet
  (M2-16).

### M2-14 Learners: live stream and promotion

- **Learners are a role (decided, design §6.4, §6.7).** A node in a
  configuration's `learners` opens its replica as `Role::Learner`: it
  follows the primary's link as a member does, grants leases, and never
  serves or takes over. `Shard::reconfigure` accepts a configuration that
  adds learners, promotes a learner to member (`Learner` becomes `Member`,
  which then starts watching the primary's grace), or removes one; it
  refuses one that adds a member that was not a learner, or demotes a
  member. Adding a learner is the coordinator's compare-and-swap (M3-05);
  until then a test driver issues it.
- **The primary's peers are dynamic.** `Leader` now keeps the learners,
  the acknowledgement set, the backfilled learners, and an outstanding
  promotion, and publishes its peers on a watch channel; the primary
  starts a link for each new peer and stops it once the peer leaves the
  configuration. The commit rule waits for the members, the
  acknowledgement set, and the learner of an outstanding promotion;
  `min_write_replicas` counts the acknowledgement set's copies too.
- **What counts as "backfilled".** Backfill is the `Backfill` trait
  (`Replication::with_backfill`), the extension point for M2-15: the
  primary starts it once a learner has reported its log in the primary's
  life, and treats the learner as backfilled when it returns `Ok`. Without
  a hook, a learner counts as backfilled as soon as it has reported its
  log: the live stream sends it the primary's log from the record after
  its last (from `seq` 1 for an empty log), and no log segment is
  reclaimed before M1-22, so a learner that acknowledges `seq` `n` holds
  every record up to `n`. Promotion then needs it in the acknowledgement
  set, backfilled, and durable up to the larger of the commit watermark
  and the commit limit, the lowest `seq` every member acknowledged, all
  of which the primary may commit (`Leader::begin_promotion`). The check
  and the start of the wait happen under the leader's lock, so nothing
  commits in between. M2-15 replaces this with snapshot and
  payload copy, once segments can be reclaimed.
- **Joining and dropping (design §6.4).** The watchdog of M2-11 also tends
  learners: one that has reported its log, responds, and acknowledged
  every record sequenced a check ago joins the acknowledgement set; one
  in the set silent for `member_suspect_after` leaves it, without a
  compare-and-swap, and must meet the join rule again. The learner of an
  outstanding promotion is never dropped.
- **Learners receive only records the primary holds durably (found, design
  §6.4).** A restarted primary re-aligns its members before it reuses a
  `seq`, but not its learners. Had it streamed a record before its own
  sync and then lost it in a crash, a learner could hold a different
  record at the same `(epoch, seq)` as the one the primary writes next,
  and lineage reconciliation could not tell. The cost is one extra serial
  sync per write while a learner is in the acknowledgement set: the
  primary's, then the learner's. The protocol model treats the primary's
  log as durable at once, so it could not show this.
- **Promotion is durable and never on the write path (decided, design
  §6.3, §6.7).** The primary records the proposal in a new `promotions`
  index table (format 5) before the compare-and-swap, and from then on,
  across restarts too, waits for the learner and needs its lease. Commits
  never wait for the compare-and-swap. A request that fails is sent again,
  unchanged, on a later check; a proposal that loses follows the register,
  adopting the configuration there if it keeps the primary (which settles
  the promotion if its epoch is at or past the proposal's) and stopping
  otherwise. The primary stops waiting only on adopting a configuration
  at or past the proposal's epoch. A handoff is refused while a promotion
  is outstanding. `ShardRegisters::replace` and `Leader::changing` from
  M2-11 serve both removals and promotions; removal comes first when both
  are due.
- **A lone replica given learners opens again as primary.** A shard
  opened as its only member (`Role::Alone`) has no links. `ShardSet`
  closes it once its records are applied and opens it again as a primary
  when a newer configuration adds learners to the same members. A primary
  with no other member does not wait to align on open.
- **Lineage reconciliation keeps an empty log.** A learner with no records
  holds a prefix of any log, so `lineage::reconcile` keeps it instead of
  calling it diverged. A re-admitted learner with an old log still goes
  through reconciliation; one that is diverged refuses the session and
  stays out of the acknowledgement set. Discarding its records is M2-15's.
- **Simulation.** `ReplicatedServices::with_learners` runs a driver on
  each node that adds a spare node to each shard it leads by a
  compare-and-swap through the node's faulty control store, and has every
  node follow the registers that name it. `Audited` registers time each
  promotion's compare-and-swap, and `check_members_hold_commits` checks R3
  after every step: every member the register names holds every record any
  primary of the shard committed. Under a control store whose round trips
  take 0.5 to 2 s each way, promotions took 4.0 to 7.2 s over 32 seeds,
  while no acknowledged write in flight meanwhile took more than 220 ms:
  promotion adds no write stall. 64 seeds of random crashes, partitions,
  message loss, control-store faults, and lost compare-and-swap answers
  ran clean, and the seeded bug `promoting_early` (the driver promotes
  each learner as it adds it) was caught in each of its 32 seeds.
- **The protocol model now splits promotion (spec/ShardProtocol.tla).**
  `Promote` records the proposal; `SendPromotion` sends its
  compare-and-swap, again after a failure or a restart, and is disabled
  once the register moved past the primary's epoch, after which only
  `Adopt` ends the wait. The seeded bug `abandon_unsettled_promotion`
  (stop waiting without reading the register) violates
  `CommittedRecordsSurvive`. The `pr` profile passes with every seeded bug
  caught; the nightly profile's state counts in `spec/README.md` predate
  the split.
- **Review fixes.** Three findings, each with a test that fails without
  its fix:
  - **Commit limit when alone.** A primary with no other member and no
    learner to wait for set its commit limit to `Seq::MAX`. The
    pipeline's limit never comes down, so once a learner joined (the
    single survivor's repair), writes were still acknowledged on one
    copy, and the learner could never be promoted. The limit is now the
    last sequenced `seq`. The cluster scenario with single-member shards
    caught it as promotions that never came.
  - **Promotion precondition.** The promotion's compare-and-swap was sent
    over the configuration the replica held after recording it, so one
    adopted in between could be replaced by the stale proposal at its own
    epoch. It is now sent only while the replica holds the configuration
    the proposal extends. The model's `SendPromotion` already had this
    guard.
  - **Exposure on learners.** `Replication::exposure` skipped only
    members, so learners reported the shards they learn as exposure.
- **Left for later.** Snapshot and payload backfill and the re-admission
  of a node with a diverged log are M2-15's; choosing learners and when
  to add them is M3-05's; the binary does not run replication yet
  (M2-16).

### M2-15 Backfill and re-admission

- **A learner keeps its log or gets a snapshot (decided, design §6.7).**
  After its `SyncAck`, each session of a primary with a learner sends a
  verdict first: `Backfill { keep }`, after which the live stream goes on
  from the learner's last record, or a snapshot. The learner keeps its log
  if lineage reconciliation kept it, or, when reconciliation called it
  diverged (a re-admitted node whose lineage, or the primary's, is known
  only from where it last opened), if the primary durably holds a record
  of the same epoch at the learner's last `seq` (`SyncAck.unverified`
  carries that epoch): one primary per epoch assigns each `seq` once, so
  the logs agree up to there. A learner that holds nothing
  (`SyncAck.fresh`), is not verified, or ends before what the primary's
  log still holds (a failed `read_tail`) gets a snapshot, and its old
  records are discarded.
- **A snapshot needs no format bump (decided, design §10.2).** The
  primary reads the shard's rows of `namespace`, `uploads`, and `parts` in
  one read transaction (`IndexReader::shard_rows`), at an applied
  position that must be in the session's epoch, so that everything the
  stream sends next is of that epoch or later; clean entries go as
  evicted, with no payload. The learner stops its replica
  (`ShardSet::reopen_after`), appends `TRUNCATE` at `(epoch, 0)`, which
  invalidates every older record by the marker rule of M2-12, marks its
  applied position `(epoch, Seq::MAX)` with the shard's rows removed,
  installs the rows, sets the snapshot's position, and opens again. The
  marker and the final position are durable; the rows ride on the second.
  Replay skips everything at or before the marker, and a learner that
  opens with it drops the rows and holds nothing, so a cut install is
  redone. A build that predates the marker sees an exhausted `seq` and
  refuses to open, so the index format stays at 5.
- **Payload is filled by the learner's requests (decided, design §6.4).**
  The primary's watchdog opens a connection of its own to the learner,
  whose first frame is `Backfill { config }`. The learner checks it
  follows that primary in that configuration, scans its index in pages
  for entries that are not clean and open uploads whose payload it does
  not locate, and asks for those records in runs of at most 64
  (`BackfillAck { needs }`); the primary answers each with the encoded
  record from its log, which the learner queues at its position,
  unapplied, and locates durably (`Index::store_locations`). Once a
  scan finds nothing missing, it reports its applied position
  (`BackfillAck { complete }`), and `Leader::fill_complete` counts it as
  backfilled only if that is at or after the last snapshot the primary
  sent it. The new frames are decoded by the `shard_replication` fuzz
  target and the wire proptests.
- **A re-admitted node reopens its replica (found in review).** A member
  removed while it was up, or cut off, is not told, and keeps its replica
  open in its old role. When a later configuration adds it back as a
  learner, `ShardSet` stops that replica and opens the shard again as a
  learner from its log, since a configuration change never turns a member
  or primary into a learner; before, every open failed and the shard
  stayed under-replicated until the node restarted.
- **The `Backfill` hook stays as a test seam.** With a hook,
  `Replication::with_backfill` replaces the built-in fill, as M2-14's
  tests use it to hold a promotion back; without one, the watchdog runs
  the fill above.
- **Simulation.** `ReplicatedServices::replacing_lost_members` makes the
  learners driver add a learner to each shard left with fewer than
  `replicas` members: a node outside the shard if there is one, and the
  lost node itself, once it is back, otherwise. `sample_durability` opens
  a window when a shard has fewer than `replicas` members and records
  when new writes have `replicas` copies again (a learner in the
  acknowledgement set) and when all data does (backfill complete).
  `ReplicatedShards::track` follows the replica a learner opens again
  after a snapshot, so that the R3 audit keeps checking it. With four
  nodes and node 0 lost for good at 2 s, new writes regained three copies
  within 425 ms of each removal and all data within 437 ms (6 shards, 3
  seeds); with three nodes and node 2 down 6 s, so that only its
  re-admission can restore the copies, within 3.30 and 3.31 s, mostly the
  rest of its downtime. Four seeds of random crashes, partitions, message
  loss, and control-store faults with lost members replaced ran clean,
  with the R3 audit after every step.
- **The protocol model is unchanged.** `spec/ShardProtocol.tla` already
  models a learner's `Sync` against the common prefix, holes filled by
  `Backfill`, and the seeded bug `truncate_by_seq_only`; snapshot install
  and payload fill are implementation detail below it.
- **Left for later.** Coded objects (M8) will be skipped by the fill,
  since their fragments live outside the shard; segment reclaim (M1-22)
  will make the snapshot path common for any learner far behind; the
  coordinator chooses learners (M3-05).

### M2-16 Local control-state copies and propagation

- **The kept configuration lives in the index (decided, design §6.2,
  §10.2).** Index format 6 adds a `configs` table, keyed by shard, that
  the state machine writes when it applies a `CONFIG` record and that
  `Index::finish_install` sets to the configuration a snapshot installs
  in, so replay restores it like any applied state and it is never ahead
  of the log. Older builds refuse format 6. `ShardSet::kept_config` and
  `kept_configs` read it; removing a shard removes its row.
- **Resuming reads both and opens the newer (decided, design §6.2).**
  `Replication::resume` reads the register through the removal registers
  and the kept configuration, and opens the newer, so a stale read cannot
  move a replica back; a register that is gone or no longer names the
  node opens nothing. If the register cannot be read, the replica opens
  in the kept configuration, and a task reads the register every
  `link_timeout` until it can: a newer configuration that names the node
  is opened through `Replication::open` (in place, or, after the M2-15
  review fix, as a learner again for a node re-admitted as one), and
  otherwise the replica is deposed with the register's configuration as
  its redirect. Epochs fence a stale replica in between.
- **A register found gone stops the resumed replica (found in review).**
  The first version took a register that answered "absent" for a
  confirmation of the kept configuration and stopped reading it, so a
  replica whose bucket was deleted while the node was down, a lone
  primary among them, kept serving. The confirm loop now deposes the
  replica when the register is gone, or holds another configuration of
  its epoch, and reads again after a read older than its configuration,
  which is stale, instead of ending. `resume_kept`
  resumes every kept shard concurrently, so a slow store costs one wait,
  not one per shard.
- **A takeover proposal is recorded before its compare-and-swap (the
  M2-12 constraint; decided, design §6.3).** A candidate whose CAS landed
  unseen, and that restarted from its kept configuration, would otherwise
  follow the old primary again in the epoch it proposed over. It now
  stores the proposal durably (`Index::store_takeover`, a `takeovers`
  table) before sending it. On open, an outstanding proposal (newer than
  the configuration, made by this member) makes the member refuse
  sessions of older epochs and stop its grace (`Grace::stop_over`), and
  the takeover task sends the recorded proposal again, unchanged, until
  it learns the outcome. The record is forgotten when the register holds
  something else; adopting a newer configuration settles it. Step-downs
  (M2-13) and promotions (M2-14) were already in the index.
- **A sync refetches only what changed (decided, design §6.2).**
  `ControlCopy::fetch_since` lists the copied prefixes and reuses every
  kept value whose listed version is unchanged, so a sync after one
  change reads `cluster.json` and that register. `refresh` loads the kept
  copy first, so a restarted node does not read everything again.
- **STS staleness counts from the identity copy's last refresh
  (decided, design §6.2).** The copy's `synced_at` is the start of the
  last sync that listed every `identity/` register; a node now also syncs
  when half of `identity_max_staleness` has passed (`control::sync_due`),
  not only when the generation moves, so a quiet store does not make
  session issuance fail closed.
- **Simulation.** `ReplicatedServices::with_local_copies` opens replicas
  as a restarted node does, through `resume_kept` and `resume` over the
  faulty store; `replicas_now` gives each open replica's configuration,
  role, and serving state beside the register's. The scenario
  `restart::a_whole_cluster_restart_while_the_control_store_is_unreachable`
  takes node 3 of four down for 5 s, makes the store unreachable from
  3.5 s for 14 s, and crashes nodes 0 to 2 in turn from 4 s. In a 5 s
  window that opens 5 s after every node is back, still inside the
  outage, every shard is served by a primary in its register's
  configuration, no replica in another configuration serves, node 3
  holds stale configurations that stay fenced, and writes are
  acknowledged; the commit, lease, and routing audits run after every
  step. About twenty seeds ran clean. Simulated node IDs count from 1
  (`node-1`), while fault plans count positions from 0.
- **Clients of a stale gateway wait (found in testing).** Throughput
  through the outage is low: requests sent to node 3, whose shard map is
  stale, wait for its gateway's register reads, which retry under the
  outage while holding the node's register-read lock, and the clients
  wait with them. The same happens without local copies, so the scenario
  checks serving on the replicas rather than an acknowledgement on every
  shard. A gateway that redirects without the register is left for the
  routing work.
- **Left open.** The binary does not run replication yet (M2-08b), so
  it neither resumes replicas nor reads `with_takeover` or a
  `takeover_delay` from its configuration; the binary wiring here is the
  control copy's delta sync and the staleness sync. An index upgraded
  from format 5 keeps no configuration for a replica until it applies or
  replays a `CONFIG` record, so such a replica resumes only once its
  register can be read. About 900 lines of non-test code.

### M2-18 Read plans and holder fetch

- **A plan carries the version's layout (decided, design §9.2).**
  `Shard::plan` reads the entry and its layout in one index transaction.
  The layout is the version's bytes as log positions in body order, with
  a multipart object's parts flattened (`reads::layout`). Positions name
  the same immutable payload on every replica, so a holder can serve a
  version its own entry no longer names, as long as it still locates
  every position. The holders are every member, primary first, while the
  version is not clean. Once it is clean, they are the first
  `clean_copies` members (`CleanCache::clean_copies`), or every member
  while the cache has not been told the count. An evicted version has no
  holders.
- **Registration pins, then checks, in one exclusive section (decided,
  design §8.7, §10.3).** `Shard::register_read` serves the holder's own
  copy if its entry is at the planned version with bytes. Otherwise it
  serves the plan's layout if every position is still located, and
  otherwise it answers `None`, counted as `not_held`. The pin and the
  location check run inside `Reads::exclusive`, and so does compaction's
  relocating index commit. Either the registration pins a position
  first and compaction keeps it, or compaction removes the location first
  and the registration refuses.
- **`Reads` is node-wide and in memory.** One `Reads` per `ShardSet`
  holds registrations with their expiry, pin counts by shard and
  position, and counts of `registered`, `not_held`, and `lapsed`.
  `Shard::read_registered` refuses a fetch whose registration lapsed, as
  `Unavailable`. A restarted holder has no registrations, so a stream
  from it fails the same way. The binary configures it from
  `read_registration_ttl_seconds` and `fragment_release_delay_seconds`.
- **Compaction keeps what reads need (the coordinator's M1-22 hook;
  decided, design §10.3).** A pinned record is classified `Copy`, ahead
  of every other rule, so it is never evicted or dropped. `relocate`
  checks the pins again under `Reads::exclusive`. Payload that no entry
  names waits until compaction has found it unreferenced for the release
  delay. The `Compactor` remembers, per shard and position, when a pass
  first found it so, and a pass that does not find it so again forgets
  it. Compaction's own eviction of cold clean payload is not delayed: a
  plan from just before it finds the positions unlocated and asks the
  next holder or plans again. Shard tests and the shard simulation set a
  zero delay; `registered_reads_and_recently_unreferenced_payload_are_kept`
  covers the delay and a pin.
- **The gateway streams from one holder.** `HolderReads::open` orders
  the holders with its own node first, then by its own count of reads in
  progress from each. It registers, fetches the first piece before the
  response starts, and streams the rest through a channel of one piece.
  From registration on, a task renews every
  `read_registration_renew_interval_seconds`. Starting it only after the
  first fetch let a holder slower than the TTL lapse the read (found in
  review). It releases the registration at the end, and the renewal
  stops on every path that ends the read. A failed fetch ends the
  stream, so the client sees the body break off. If no holder holds the
  version, the GET resolves the key again, up to three rounds, then
  answers 503. HEAD reads the entry alone.
- **Five routed operations.** Plan, Register, Renew, Release, and Fetch
  are wire ops 14 to 18, and Register carries its layout as the frame
  payload. Holder operations (`Request::is_holder`) bypass the epoch and
  role checks: any replica with the payload may serve, and the version
  check is the holder's own.
- **Simulation.** `HolderFaults` delays registrations and fetches by a
  seeded random amount, and `HolderAudit` counts what holders did and
  flags fetches served under lapsed registrations. The scenarios are in
  `tests/simulation/reads.rs`: four nodes, three replicas,
  `clean_copies` 2, and a 4 KiB cache. Overwrites and evictions racing
  reads never return the wrong version, with and without crashes and
  message loss. Members serve reads, and some reads are of superseded
  versions. Registrations with a 100 ms TTL and a 1 s renewal fail GETs
  mid-stream (`Report::broken_reads`). The cluster-sim compaction
  scenario uses a 2 s release delay (`ReadRegistration::release_delay`).
  Every scenario declares `COST`. At `SKYS3_SIM_SEEDS=256` (8 seeds
  each, debug build) they take 25 s, 31 s, and 36 s, and the two
  seeded-bug scenarios take 6 s and 4 s. The compaction scenarios take
  28 s and 30 s.
- **Seeded bugs are caught at seed 0.** A holder that registers its
  current copy, whatever version the plan names (`ignore_version`), is
  caught by "a GET whose body does not match". That scenario uses
  prefix-only bodies, since a shorter new version only makes the gateway
  pass the holder over, and 500 ms registration delays. A holder that
  serves lapsed registrations (`ignore_lapse`) is caught by the holder
  audit. Both were caught on each of seeds 0 to 31 (`SKYS3_SIM_SEEDS=1024`).
- **Left open.** Fills still run only on the primary and are not
  forwarded, so a GET of an evicted version through another node
  answers 503. CopyObject still reads its source through the primary's
  `payload`. Load is what each gateway sees; no node reports its own.
  About 1,700 lines of non-test code, 400 of them in the simulation
  crate.

### M2-19 Hot cache

- **The cache is in memory, so the default dropped to 1 GiB (decided,
  design §9.2, §14).** The design gave `hot_cache_bytes_per_node` a
  64 GiB default without saying where the bytes live. A disk-backed cache
  would need the `Disk` trait in the gateway, which is generic over
  `Shards` and holds its collaborators as trait objects, and it would
  duplicate on the gateway's disk what the clean cache already keeps on
  the holders. The hot cache holds whole objects in memory instead, and
  64 GiB of memory is no default, so it is now 1 GiB; the configuration
  reference and the §14 example say so.
- **Keyed by bucket and key, one version per entry (decided).** The entry
  names its version by record position and ETag; a lookup of any other
  version misses and drops it. A fill of an earlier version that finishes
  after a later one was cached keeps the later one. Objects above an
  eighth of the cache are not kept, nor ranges, nor reads from the
  gateway's own replicas, nor reads that broke off: `HolderReads::open`
  collects the pieces it streams (`Keep`) and fills the cache only once
  the whole object streamed. A cached version is also served after the
  primary evicted it, without a fill.
- **Fills in progress count against the bound (decided, review of
  #70).** The first version gave each eligible miss a buffer of the
  object's full size, outside the cache's accounting, so a burst of
  misses could hold many times `hot_cache_bytes_per_node` at the 1 GiB
  default (up to 128 MiB per GET). Now a fill reserves the object's size
  when its stream starts (`HotCache::reserve`), evicting the least
  recently used objects to make room, so held bytes plus reservations
  never exceed the bound; one bound rather than a separate one for fills,
  so the setting stays the node's whole hot-cache memory. A reservation
  that does not fit beside the other fills, or a second fill of a version
  already filling (the coalescing: the first fill wins, later GETs just
  stream), is refused, and that GET keeps nothing. A fill keeps the
  response's own `Bytes` pieces, so the buffers grow as the bytes arrive
  and nothing is copied; entries are held as those pieces, and a range
  across pieces is copied on lookup. The reservation is a guard
  (`Fill`): finishing turns it into the entry, and dropping it, on a
  lapsed registration, a failed fetch, a dropped response body, or a
  short read, releases it. A burst of 36 GETs of 12 uncached objects
  against room for 8 peaks at exactly the bound and refuses 28 fills
  (`tests/hot_cache.rs`). New: `skys3_hot_cache_filling_bytes`, and
  `reserved`/`refused` in `HotCacheUsage`.
- **`GatewayConfig::hot_cache` is a shared handle.** Clones of a gateway
  configuration share one cache, which the cluster harness's nodes would
  have done, since every node's configuration is a clone of one. The
  harness now gives each node's every life a new cache, and the binary
  makes its own with metrics (`skys3_hot_cache_*`).
- **Counting who served each GET needed two sources.** A GET is served
  either by a holder, which the holder audit now counts per node
  (`ReplicatedServices::holder_reads`), or by a gateway's hot cache, which
  `Report::hot_cache_hits` counts per node over every life. The workload
  gained `get_percent`, `GET`s drawn ahead of the usual mix, since the
  mix's deletes made a hot key absent most of the time; with 0 it draws
  nothing extra, so other scenarios' seeds are unchanged.
- **Spread across gateways.** Five nodes, one replica of one key, 95% of
  operations `GET`s through any node: without hot caches the holder
  serves every `GET`; with them every node serves some, and the holder
  25% to 41% of them on seeds 0 to 7. The test runs both on each seed.
- **The seeded bug needed prefix-only bodies.** A cache lookup that
  ignores the version (`HotCaches::ignore_version`) first went unnoticed
  on seed 5: with bodies of random length, a new version is often longer
  than the cached one, and the cache then cannot hold the planned range,
  so it misses. With bodies no longer than their prefix and half the
  operations `GET`s, "a GET whose body does not match" catches it at
  seed 0, and on each of seeds 0 to 31 (`SKYS3_SIM_SEEDS=1024`).
- **Compaction had to be made to happen.** With 40 operations per client,
  the racing scenario compacted nothing on seeds 0 and 1, the two a run
  without `SKYS3_SIM_SEEDS` uses; 60 operations with shorter pauses
  compact on every seed but one of 0 to 7, and the test asserts that
  some seed compacted and some `GET` hit a hot cache.
- **Simulation cost.** At `SKYS3_SIM_SEEDS=256` (debug build) the
  racing scenario (overwrites, clean-cache evictions, compaction) takes
  30 s, the one with crashes and message loss 36 s, and the seeded bug
  5 s, at 8 seeds each. The spread scenario runs two clusters per seed,
  so it is declared at twice the cost: 4 seeds, about 22 s.
- **Merged the M2-18 fix** that starts renewing a registration at
  registration: the stream task no longer starts its own renewal, and
  the hot-cache collection sits in the same loop.

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

### M3-05 Replacement

- **The coordinator and the primary are equals (decided, design §6.7).**
  The coordinator adds a learner by a compare-and-swap of the shard
  register from epoch `e` to `e+1` over the version its scan read, under
  its lease, in a change announced by one generation increment. The
  primary's removals and promotions are compare-and-swaps over the same
  register, so the first to land wins and neither retries over the other.
  A primary whose change loses already adopted a configuration that keeps
  it primary and adds no member that was not a learner (M2-14's
  `can_adopt`), and proposes again over it; a coordinator change that
  loses is dropped, and the next round plans from the register. §6.4 said
  the primary adopts only a change that "only removes members", which is
  narrower than the code and than replacement needs; it now says what the
  code does.
- **Shards are counted the way cluster health counts them (decided).** A
  member or learner counts if it is on a registered node that is not
  departing, carries the label the level needs, and is in a domain no
  counted node of the shard is in (primary first, then members, then
  learners): M3-03's measure, with the exception below. Learners are added
  while the counted nodes are fewer than `replicas`, with
  `Topology::place` keeping the counted nodes and avoiding every node the
  shard names. Learners that do not count are dropped. Members that do not
  count (never the primary) are removed only once the counted members
  reach `replicas` without them, that is, after their replacements were
  promoted, so a removal never takes a shard below `replicas` members or
  below `replicas` counted ones. A departing primary is left to a handoff
  (M3-06), since only a new primary may write a configuration that names
  it (R1).
- **A member on an unregistered node is left alone (found).** The first
  version counted such a member for nothing, as cluster health does. In
  the simulation, whose harness writes the shard registers before the
  nodes register, a coordinator that listed the nodes before a slow node
  registered added learners to every shard that node held. Once they were
  promoted, those shards had four members, and the later node loss left
  them at three, so no window opened and the scenario failed (seed 104 of
  320). A node is forgotten only once no shard names it, so an
  unregistered member belongs to a node that has not registered yet, or
  to M3-02's rare stalled-planner race: nothing shows it lost. It now
  counts, in no domain, and is left to its primary, which removes it if
  it does not respond; the shard is then short and replaced. Recorded in
  §6.7.
- **Pace (decided).** `ReplacementConfig`: one change writes at most 32
  shard registers, the most urgent (fewest counted members) first; the
  registers may name at most 64 learners at once, which bounds concurrent
  backfills; member removals are at most one per 10 s across the cluster.
  Learner additions are not delayed, since they shorten the exposure
  window. These have no configuration keys yet: the binary does not run
  the coordinator, and the keys belong with that wiring (§14), as M3-02's
  labels do.
- **A learner on a dead node is swapped (decided).** A coordinator that
  has just taken over judges every node live for `suspect_after`, so when
  the lost node was coordinator, its successor can give a learner to the
  lost node itself; a learner's node can also die during backfill. Such a
  learner never joins, and the shard would stay short until
  `node_forget_after`. A learner on a suspect node is swapped, one per
  shard and round, for a live node when placement has one in a domain the
  shard does not otherwise use. With no live candidate (a three-node
  cluster), the learner stays, and the lost node rejoins when it returns.
- **A learner a takeover left out could not be added back (found).** The
  coordinator added a learner to a shard whose primary had died; the
  takeover dropped it (a candidate proposes no learners), and the
  coordinator added the same node again under the new primary. The node's
  replica, a learner of the old primary, refused the new configuration
  ("a new primary takes over by proposing itself"), every open failed, and
  the shard stayed short (seed 0 of the forgetting scenario). The base's
  fix in the same place (c40f33e) reopens a stale member or primary as a
  learner; the merge extends it to a learner whose primary changed, which
  stops and opens again as a learner of the new primary, as a restart
  would. A shard test fails without it. Recorded with re-admission in
  §6.7.
- **No plan before the registry has listed the nodes (decided).** An
  empty registry, as at the start of a tenure, makes every learner look
  unregistered and every node ineligible. `Replacement` plans nothing
  until `NodeRegistry::is_listed` says this tenure has listed `nodes/`.
- **The removal interval in review (found).** The timer started when a
  removal was planned, so a change whose earlier write lost its
  compare-and-swap, and never sent the removal, still held back every
  removal for the interval; it now starts once a removal took effect, or
  may have. And each coordinator kept its own timer, so a new tenure could
  remove a member right after its predecessor did; a tenure now waits a
  whole interval before its first removal, which keeps removals an
  interval apart across coordinators while tenures do not overlap.
  Recording the time in the control store would need a new register
  field and format version for a pacing bound. Recorded in §6.7; each
  fix has a test that fails without it.
- **The simulation's driver is gone.**
  `ReplicatedServices::replacing_lost_members` became
  `following_registers`: nodes follow the registers that name them (a
  100 ms poll, a stand-in for propagation to replicas, which the binary
  does not wire yet, M2-08b) and add nothing. `CoordinationConfig::replacement`
  runs `BucketShards` around `Replacement` in every node's coordinator.
  M2-15's backfill scenarios now run on it unchanged, and
  `DurabilityWindows` gained `restored`, the time until the register had
  `replicas` members again.
- **The replacement scenarios' cost (found).** One seed of either costs
  140 to 160 s of run time, against about 13 s for a typical seed, and at
  `with_cost(3, COST)` CI's fixed set of 256 ran eight of each: 2010 s and
  1773 s, the two slowest scenarios, which made the simulation job take
  half as long again. They now declare `8 * COST`, as `crash.rs` does for
  its heavy scenario, so the fixed set runs one seed of each (162 s and
  141 s) and larger seed sets more, and the clients write 500 operations,
  not 700, which still ends after the lost node is forgotten. No seed
  hung: all 59 scenarios finished at 256 seeds.
- **Numbers.** Over 10 seeds of each scenario (from seed 100), with the
  commit and R3 audits after every step: with five nodes and two buckets,
  losing the first node (often the coordinator) left four shards short
  about 3 s later (`member_suspect_after`); new writes had three copies
  again within 1.71 s of the removal, all data within 1.72 s, and every
  shard had three members within 1.84 s. With four nodes, losing a node
  for good and losing one that was then forgotten, every shard had three
  members within 2.91 s; the slowest are shards whose primary was lost,
  timed from the takeover, whose first learner the takeover dropped. With
  three nodes, the lost node rejoined as a learner, and its shards had
  three members 3.3 to 3.4 s after the removal, the rest of its 6 s
  downtime. With `node_forget_after` at 5 s, the lost node was forgotten
  10.3 to 11.6 s into the run. Twenty seeds of random crashes,
  partitions, message loss, and control-store faults ran clean. M2-15's
  driver, which ran on the primary, took about 0.4 s: the coordinator's
  rounds, the generation's push, and the nodes' register polls cost up to
  another second and a half.
- **Left for later.**
  - **Departing primaries and surplus members** are rebalancing's (M3-06):
    the coordinator never removes a primary, and removes only members that
    do not count.
  - **One change's writes are sequential.** `apply` sends a change's
    compare-and-swaps one after another, so 32 shards cost 32 round trips,
    and a node loss in a large cluster takes several changes. Sending them
    in parallel needs `apply` to report a set of outcomes, not a prefix.
  - **Each wrapped placement scans the shard registers itself.**
    `PolicyWatch`, `BucketShards`, and `Replacement` each list `shards/`
    every round (reading only changed registers); sharing one
    `ClusterScan` would save two listings per round.
  - **Pushes to a dead node still take their timeout** per change (M3-02
    notes); batching makes it a cost per change, not per shard.

### M3-06 Rebalancing

- **The coordinator asks the primary with a `Handoff` admin message
  (decided, design §6.7).** Only a primary can hand off (R1). M2-02 had
  already reserved `MessageKind::Handoff` in the admin class, and M6-01's
  peer protocol has nothing for it, so no new kind was needed. Its body
  names the bucket, the shard, the epoch the coordinator read, and the
  member; the answer (`HandoffAck`) says whether the handoff started. The
  node starts it only on its serving primary in that epoch, to another
  member, so a request from an older register or a former coordinator
  does nothing. `AdminEndpoint::with_handoffs` serves it through a
  `HandoffSink` (the extension point for the binary's replication), and
  `HandoffClient` sends it. A proptest and the `coord_handoff` fuzz target
  cover the decoder.
- **Shares (decided).** Among live eligible nodes, members follow disk
  capacity and primaries are split equally. A shard moves from `a` to `t`
  while `(s_a − 1) / c_a ≥ (s_t + 1) / c_t`, which stops with every node
  within one shard of the others at equal capacity and never moves a
  shard back; a primary moves alone only from a node that leads two more.
  A move whose source leads the shard moves the primary to the target
  when that helps primary balance, so a new node mostly gets its
  primaries without a primary being moved alone.
- **Composition with replacement (decided).** `Rebalancing` is the
  placement `Replacement` wraps, so it plans only in rounds where nothing
  needs repair. It starts moves only in a settled cluster (no learner, no
  surplus member, no outstanding handoff, no suspect node), one batch at a
  time, counting each move as done when it plans the next, so a batch
  never overshoots. The member that leaves is the one the move named, or
  after a coordinator failover the one on the node furthest over its
  share, so moves complete without state that must survive a failover.
- **Moves within a domain fought replacement (found).** At the `rack`
  level, a new node in a rack every shard already uses could only replace
  the member in its own rack. Replacement counted the member first, so it
  dropped the learner as co-located, and after a promotion it would have
  removed the new member. `Census` now counts the later of two non-primary
  nodes in one domain (a learner after the members), which is what a move
  appends; the primary keeps its domain, so rebalancing moves a primary
  only across domains. For a relabeled node this changes which of two
  co-located members replacement removes, not how many count. Recorded in
  §6.7.
- **A re-added learner could not follow its shard (found).** In the
  two-join scenario (seed 1), the second new node was suspect for a moment
  after joining, so replacement swapped its learners for live nodes; those
  were promoted, and rebalancing then added the new node again. Its
  replica, still a learner of the older epoch under the same primary,
  refused the new configuration ("a member joins only as a learner": a
  member it never knew had joined), so it never caught up and the shard
  kept a learner for the rest of the run. M3-05's fix reopened a learner
  only when its primary changed; `ShardSet` now also reopens a learner
  whose new configuration has members it never knew. A shard test covers
  it.
- **A departing primary, and primaries moved alone.** A primary that does
  not count is handed off once the other members that count reach
  `replicas`, which replacement brings about. A primary moved alone
  leaves the shard short one member until replacement adds a learner,
  since a handoff drops the old primary (M2-13); rebalancing moves one
  only once no shard needs to move.
- **Pace (decided).** `RebalanceConfig`: at most 4 moves per batch, a
  handoff every 2 s at most across the cluster (one shard's writes wait
  for a handoff at a time), and a handoff that did not land asked for
  again after 10 s. Named `rebalance_max_moves`,
  `rebalance_handoff_interval`, and `rebalance_handoff_retry` in §6.7;
  like M3-05's pace, they get configuration keys when the binary runs the
  coordinator.
- **The harness grew joining nodes and write timing.**
  `ClusterConfig::joining` holds the last nodes back, with empty disks
  and no shards, until a `Fault::Join`. `Report::writes` times every
  client write end to end, and `ReplicatedServices::handoff_times` times
  each handoff the coordinator asked for, from the step-down until a
  primary served the next epoch (`sample_serving`). The sim's handoff sink
  (`NodeServices::handoff_sink`) checks epoch, role, and member, and runs
  `Replication::hand_off`.
- **A write can fail fast while its primary is behind the register
  (found, left open).** In one seed a gateway learned a shard's new epoch
  from the register before the primary followed it (the sim's 100 ms
  register poll stands in for propagation, M2-08b). Every member answered
  with an older epoch, the gateway had read the register within
  `register_interval`, and it answered 503 after 20 ms. It is not a stall,
  happens after every coordinator change (replacement's too), and
  retrying an "is still in epoch" answer in the routing client, or pushes
  to replicas, would remove it.
- **Handoffs went to silent nodes (found in review).** `successor` took
  the move's target whenever it was still a member that counts, even
  once its node turned suspect, and its fallback only sorted live nodes
  first, so with none live it still chose a suspect one. A handoff to a
  node that does not answer stops a healthy primary and leaves the shard
  to another member's `primary_grace`. A handoff now goes only to a
  member on a live node, and none is asked for while no successor is
  live: the primary keeps serving (§6.7).
- **The latency check left out writes that never connected, which hid a
  harness flaw (found in review).** A `PUT`, multipart completion, or
  `DELETE` whose connection failed or timed out returned before it was
  timed. Timed now, as unanswered, the two-join scenario failed: a write
  waited 2.4 s, and others up to the client's whole 8 s timeout, outside
  the moves as well. Every one was sent to a node that had not joined
  yet: `Routes::nodes` listed every node, so any-gateway clients drew
  nodes still down, the connection hanging until the client gave up or
  the node started (and failed then). Nothing in rebalancing stalled.
  Clients and the bucket creator now address only the nodes present
  from the start, as `ClusterConfig::joining` already said; no write
  then failed to connect, and the bound held unchanged.
- **Writes on a write-back bucket wait up to 1 s per commit behind lazy
  `FLUSHED` records (found in CI, left open).** CI's seed set (8 seeds of
  each scenario) failed seed 3: a `PUT` on a shard no move touched (it
  stayed at epoch 1 all run) took 2.011 s. Traced with timing prints:
  its extent commit took 1.0 s, then its index record 1.0 s, with no
  register read over 200 ms and no slow round trip. A replicated shard's
  pipeline is ordered (M2-07): before it queues a record of the other
  segment class it drains every record in flight. The flusher commits
  `FLUSHED` (hot class) lazily (M1-16), and a lazy record becomes durable
  only once another record starts a group commit or `LAZY_MAX_DELAY`
  (1 s) passes. An extent (bulk class) queued after one therefore waits
  out the full delay: the drain holds back the very record that would
  have committed it. An index record queued behind such extents waits
  again. Setting `LAZY_MAX_DELAY` to 300 ms made the same seed's slowest
  write 388 ms, against 2.011 s, and the slowest write outside the moves
  317 ms, against 1.017 s. So the ~1 s write latency seen throughout
  these scenarios was this delay, not control-store faults on register
  reads, as the notes said before. It contradicts the log's promise
  that a lazy record delays others only while nothing else is written,
  and it costs every replicated write-back bucket up to 2 s per `PUT`.
  The fix belongs to the log and the shard pipeline, not to
  rebalancing: for instance, a way to commit the waiting lazy records
  now, called by the pipeline before a class-switch drain. Until then
  the scenarios' bound for a write that meets no handoff is derived from
  it: two `LAZY_MAX_DELAY` waits plus `ROUTING_SLACK` (2.8 s), checked to
  stay below `member_suspect_after` (3 s), the shortest wait a write
  held up by a membership change would see. Comparing with the run's
  own writes outside the moves was considered and rejected: two waits in
  one write are rare enough that a run's baseline often has none, as in
  seed 3 (1.017 s).
- **Numbers.** CI's seed set: 8 seeds of one node joining four, and 8
  of two nodes joining one after the other (`SKYS3_SIM_SEEDS=256` over a
  cost of 32), with two buckets of four shards and three members each,
  one of them `write_back`, four clients writing through any of the
  initial nodes' gateways, and the commit, R3, lease, and routing audits
  after every step. Each new node ended with its share: 4 or 5 of 24
  members and 1 or 2 of 8 primaries (4 members and 1 or 2 primaries
  each with six nodes). A join took 4 moves (8 to 14 promotions for two
  joins, a seed with replacement swapping learners as above);
  promotions' compare-and-swaps took at most 60 ms. One or two handoffs
  per run moved primaries, each serving again after 22 to 58 ms (about
  30 ms on average). No shard had fewer than `replicas` members after
  the review fixes; before them, once for 204 to 316 ms in one two-join
  seed. With every write timed, including any that never connected,
  writes in flight while shards moved and meeting no handoff took at
  most 2.011 s (seed 3, two lazy-record waits on a shard no move
  touched, above), otherwise at most 1.75 s, against up to 2.0 s before
  and after the moves: the same lazy-record waits, not moves. Up to
  three per run were unanswered, fast 503s (above). Writes meeting a
  handoff took at most 27 ms, at most 10 ms after the review fixes, most
  of them fast 503s. So the only stall moves cause is the handoff, tens
  of milliseconds. (Measured before and after merging M6-04 and the
  M3-05 removal-timer fixes, after the review fixes, and on CI's seed
  set; ranges cover all runs.)
- **Left for later.**
  - **Configuration keys** for the pace, with the coordinator's wiring.
  - **Rebalancing scans the shard registers itself**, a fourth listing per
    round (M3-05 notes).
  - **Primaries moved alone cost a short durability window.** Re-adding
    the old primary as the learner, rather than letting placement choose,
    would keep its verified log; the old primary is likely to be chosen
    anyway, since it lost a shard.
  - **A new node that is briefly suspect** gets its rebalancing learners
    swapped by replacement. Nothing breaks, but the moves take longer.

### M3-07 Control-store rebuild tool

- **What the binary can do (found).** The node binary runs only the file
  control store, every shard alone (M2-08b is not wired), so
  `skys3 control export` and `skys3 control rebuild` rebuild that store:
  from the one node's export, with the node stopped (the data directory's
  lock), and claimed for it with its `.owner.json`. Another backend is
  refused with a message. The plan and the write are backend-agnostic
  (`skys3_control::RebuildPlan`), and the simulation rebuilds a shared S3
  control store with them. A lone replica keeps a `CONFIG` record of its
  own, which is no register's, so the binary's export leaves shard
  configurations out; the simulation's exports carry them.
- **Exports are taken from stopped nodes, after log recovery (decided).**
  The index keeps the configuration of the latest `CONFIG` record a
  replica *applied*; a primary's removal makes its record durable, and
  acts on it, before the record commits. An export of a running node
  could therefore miss a configuration the node acts on, and rebuilding
  the older one would let a removed member take over over it: two
  configurations of one epoch. Replay applies every durable record, so
  an export after recovery holds the newest `CONFIG` record the node has.
  Stopping every node also leaves no proposal in memory that the rebuilt
  store could accept (an etcd store rebuilt from scratch reuses
  revisions). Recorded in design §6.2.
- **Registers are rebuilt byte for byte, not at fresh epochs (decided).**
  Each shard's newest configuration among the exports is written
  unchanged, `proposal_id` included. A replica compares its register with
  its whole configuration and is deposed by "another configuration of its
  epoch", so a rewritten `proposal_id` would have stopped every replica;
  at a fresh epoch every replica would have to adopt a configuration no
  one proposed. Fresh epochs buy nothing once every member of the newest
  configuration exported: the primary of any later configuration was a
  member of it (R1) and made the record durable first. So the plan
  refuses a configuration whose members did not all export, unless they
  are declared lost (`--lost`), and refuses two configurations of one
  epoch. Recorded takeovers and promotions need no handling: they are
  sent again over the rebuilt register and land or lose as usual.
- **Identity cached on some nodes only: the newest copy wins (decided).**
  Each node's copy is a whole listing at its generation, so the plan takes
  the copy with the highest generation, byte for byte. Merging copies was
  rejected: a role deleted after an older copy was read would come back,
  with its trust policy.
- **Copies of the newest generation that differ need the operator's
  choice (decided, review).** The first version broke ties by the sync's
  start time. Review found that unsafe: a register is written before the
  increment that announces it, nodes also sync on a timer, and a failed
  increment leaves a write unannounced, so two copies of one generation
  can differ. `synced_at_ms` comes from each node's own clock, and a sync
  that started later can still have listed a register earlier, so the
  tie-break could rebuild a deleted bucket or role, or drop a new one.
  No register carries a version that would order the copies. So the plan
  refuses differing copies of the newest generation
  (`RebuildError::DivergentCopies`), naming the nodes, grouped by equal
  copies, and the registers that differ. The operator then chooses one
  with `skys3 control rebuild --prefer <node>` (`RebuildOptions::prefer`),
  and each other copy is noted (`RebuildNote::CopyDiffers`). Only a copy
  of the newest generation can be chosen
  (`RebuildError::PreferredNotNewest`): a higher generation always wins.
  Equal copies need no choice, and the lowest node ID names them. The
  export keeps `synced_at_ms` for the operator; nothing orders by it.
  Recorded in design §6.2 and §6.9, with the limit: a write that only
  copies of older generations hold is lost with the store.
- **Shards of buckets the copy does not name (decided).** A bucket created
  shortly before the loss may be missing from every copy, and its nodes
  would drop its shards at their next start (§4.1). The export says which
  shards hold objects; such a shard stops the rebuild unless
  `--allow-unnamed`. An empty one is a deleted bucket's leftover and only
  reported.
- **A lost store stopped every replica that read its register (found).**
  M2-16 deposes a replica whose register is gone, and resumes nothing for
  one. With the whole store gone, a node restarted during the loss opened
  no replica, and a primary whose removal found its register "gone" would
  have stopped. `ControlRegisters` now answers `NotBootstrapped` for a
  register absent from a store that holds no `cluster.json` either, so the
  replica runs on in its kept configuration until the store is rebuilt.
  The drill fails without it (seed 0: the restarted node opened nothing).
  Recorded in design §6.2.
- **Nodes no longer follow a store backwards (decided).** A node with a
  copy synced from any store whose generation merely differed, so a store
  recreated at generation 1, by a node that never synced bootstrapping a
  lost shared store for instance, would have replaced its copy with
  nothing. `ControlCopy::fetch_since` now refuses a store behind the copy
  (`ControlError::GenerationBehind`), and the rebuilt `cluster.json` is
  at the generation after the newest copy's, so every exported node syncs.
- **Racing a store that is alive (decided).** The rebuild writes nothing
  into a store holding `cluster.json` or a bucket, shard, or identity
  register it does not write. Node registrations and a coordinator lease
  that a running coordinator writes into a lost store (M3-01, M3-02 write
  regardless of `cluster.json`) are left; nodes register again at start.
  `cluster.json` goes last with a `proposal_id` derived from the plan, so
  an interrupted rebuild, run again with the same exports, completes and
  then reports itself complete.
- **Simulation.** `Fault::LoseControlStore` deletes every register;
  `Fault::RebuildControlStore` stops every node, exports each from its
  disks through `skys3::rebuild::export` after `storage::recover`, and
  applies the plan through injected lost requests, lost answers,
  conflicts, and outages. `Report::rebuilds` and `Report::registers` (the
  final shard registers) carry the outcome. The drill
  (`rebuild::a_lost_control_store_is_rebuilt_and_membership_changes_resume`)
  loses the store at 1 s, restarts one node during the loss, rebuilds at
  4 s, and takes node 3 down at 13 s.
- **Numbers.** 32 seeds (0 to 31) ran clean, about 27 s each, 861 s in
  all. In each, writes were acknowledged on 6 or 7 of the 8 shards while
  the store was lost, the restarted node resumed its replicas, the plan
  held all 8 shard registers with no note, and `cluster.json` went from
  generation 2 to 3. Lost answers made 1 to 5 of the 11 registers turn up
  already written when sent again. After the restart every shard was
  served in its rebuilt configuration, and the first membership change on
  the rebuilt store landed 728 to 755 ms after node 3 went down
  (`member_suspect_after` is 700 ms); every shard node 3 belonged to
  ended without it. The scenario declares `4 * COST`, so CI's fixed set
  (`SKYS3_SIM_SEEDS=256`) runs 2 seeds, about 55 s.
- **Left open.**
  - **Rebuilding an etcd or S3 store from the binary**, with the binary's
    multi-node wiring (M2-08b).
  - **A full stop for the export.** An export from running replicas'
    in-memory configurations, with nodes frozen for membership, would
    avoid it.
  - **Writes into a lost store.** The coordinator and node registry still
    write into a store without `cluster.json`; a node that never synced
    would bootstrap one. The rebuild and the generation check make both
    safe, but they should wait for `cluster.json`.

### M3-08 Heal tests

- **No real-cluster heal test is possible yet (left open).** The node
  binary serves every shard alone (`LocalShards`), with the file control
  store only. It runs no coordinator and no replication, and does not
  wire the transport (M2-08, M3-04, and M3-07 notes). Several binaries are
  therefore several one-node clusters, and none of the four scenarios can
  be run against them. No plan task owns the binary's multi-node wiring,
  which M2-20's kill and partition tests need as well. The heal tests run
  the real node code under the cluster harness instead, and the
  real-binary versions wait for that wiring.
- **Nodes did not follow the registers of buckets created through a
  gateway (found, harness).** `ReplicatedServices::following_registers`
  looped over the static placement only, and the static placement is
  empty when the gateways create the buckets (`create_buckets`). So a
  primary never adopted the coordinator's learner additions for those
  buckets. In the rack scenario (seed 0) gateways answered 503 ("still in
  epoch 1") for 40 s, and a shard kept two members and a learner until
  the run ended. M3-04's scenarios do not follow registers, and M3-05's
  and M3-06's use the static placement, so the gap never showed. Nodes now
  follow every shard register in the store. The node code was not at
  fault.
- **Clients stalled on a lost node (found, harness).** Any-gateway clients
  draw from the initial nodes, and a connection to a crashed host waits
  out the whole request timeout (8 s). With one of five nodes lost, four
  clients managed about 1.5 operations a second, and the first run of the
  node scenario lasted 600 s of simulated time, until the lost node came
  back. `Workload::connect_timeout` (300 ms here) lets clients move on
  from a node that is down, as clients behind a load balancer would. The
  default is unchanged. M3-05's replacement scenarios have the same stall,
  which is likely why each of their seeds costs 140 to 160 s; giving
  them a connect timeout would cut that.
- **A coordinator lost in the middle of a change needed a fault the
  services trigger.** A fault plan names nodes by position at fixed times,
  but which node coordinates, and when its first move starts, depend on
  the seed. `NodeServices::triggered_faults` (default: none) hands the
  driver faults that fall at a protocol state. With
  `CoordinationConfig::lose_coordinator`, the first change that writes a
  shard register crashes its coordinator, either once the change is
  planned (`ChangeStage::Planned`: writes in flight, or landed but not
  announced) or once some of its writes landed (`ChangeStage::Applied`:
  learners added, with promotion and removal still to come).
  `CoordinatedServices::lost_coordinator` reports what was lost.
- **Rack labels and the gateways' level were fixed.** The harness labelled
  nodes with two alternating racks, and gateways created buckets at the
  `node` level. `CoordinationConfig::racks` and
  `ClusterConfig::failure_domain` make both configurable, with the old
  values as defaults. The rack scenario runs six nodes in four racks
  (`rack-0` and `rack-1` hold two nodes each) and loses `rack-0`, so three
  racks remain for three replicas.
- **Rebalancing raced the rack loss (seen, not a bug).** In seed 0 the
  coordinator planned two moves 80 ms after both `rack-0` nodes crashed,
  each onto one of them, because neither was suspect yet. Once they were,
  replacement swapped those learners (M3-05's rule), and the shards healed
  in 16.5 s.
- **Each scenario fails when a heal step is broken.** Seed 0 fails in each
  case, which is the seed CI runs:
  - Node loss and addition, with forgetting disabled
    (`node_forget_after` of an hour): the lost node stays suspect,
    rebalancing never starts, and the new node ends with no primary ("the
    load is uneven").
  - Rack loss, with the coordinator placing at the `node` level while
    gateways place at `rack`: a shard ends with two members in `rack-1`.
  - Rack loss, with the harness gap above put back: a shard ends with a
    learner that never joins.
  - Coordinator loss, with rebalancing's failover rule removed (only the
    member the coordinator's own move named may leave): a shard ends with
    four members. Before this run the mapping was the other way round
    (even seeds `Planned`), and seed 0 healed while seed 1 failed. So the
    even seeds now lose the coordinator after its writes landed, the case
    that catches this bug.
- **No product bug was found.** Every failure traced back to the harness
  or to the scenarios.
- **Numbers.** Seeds 0 to 15 of each scenario passed, with the commit and
  R3 audits after every step and the history checks at the end. Times are
  until the cluster was healed for good, from the fault:
  - Node lost for good, new node joining 2 s later: 11.6 to 13.1 s. The
    lost node was forgotten about 7 to 8 s after the loss.
  - Rack lost: 10.4 to 16.6 s.
  - Coordinator lost: 3.0 to 5.4 s when it came back after 2 s, and 11.4
    to 13.6 s when it was lost for good and forgotten.

  Every heal finished while the clients were still writing, as the check
  requires. A seed takes 30 to 40 s in a debug build. Each scenario
  declares `8 * COST`, so CI's fixed set (`SKYS3_SIM_SEEDS=256`) runs one
  seed of each: 31 s, 35 s, and 32 s, about 100 s added to the simulation
  job. At `SKYS3_SIM_SEEDS=1024`, four seeds of each passed, in 220 s with
  the three scenarios running side by side.

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

### M6-02 QUIC endpoint and connection pool

- **No trust-bundle keys existed.** §7.8 and §12 speak of "a configured
  trust bundle", but `[peering]` had no certificate keys. The PR adds
  `[peering.peers.<cluster-id>]`, with `ca_file` and `buckets`, to §14 and
  the configuration reference. Each peer has its own CA bundle, not one
  shared file: with a single bundle, a CA that one peer controls could
  issue a certificate in another peer's SPIFFE trust domain. A node
  presents its `[transport]` certificate to peers, so a configured peer
  requires the `[transport]` certificate files.
- **"Bucket pairs" had no shape.** No peer message names the source
  bucket. The write identity in `BEGIN`, `COMMIT`, `BATCH` items, and
  `ABORT` carries the source cluster and bucket ID, so a pair is the
  source bucket ID and the destination bucket name. The identity's cluster
  must also be the authenticated peer, or a peer could write in another
  cluster's name. How a refusal is answered (`ABORT` or `APPLIED`
  `refused`, with refused batch items dropped from the batch) is recorded
  in §7.8.
- **REST flush concurrency does not adapt yet.** The task said to mirror
  skys3-flush's adaptation, but skys3-flush still uses a fixed
  `flush_min_concurrency_per_shard`; adaptive concurrency is M4-10. The
  pool's `AdaptiveLimit` implements the §7.7 rule directly, and M4-10 can
  reuse it. A pure ratio test of round trip against base read loopback
  jitter (a base of tens of microseconds) as congestion and never grew,
  so rising latency also needs 5 ms of absolute growth.
- **quinn brings second versions of rand and ring's dependencies.**
  quinn-proto 0.11.19 is on rand 0.10, while proptest, turmoil, and the
  workspace are on 0.9, so `rand@0.10` and `rand_core@0.10` are skipped in
  deny.toml. quinn-proto also lists `ring`, but only for
  `wasm32-unknown-unknown`, so `getrandom@0.2` and `windows-sys@0.52`
  appear in the lockfile but are never compiled; they are skipped with
  that reason. With default features off and `rustls-aws-lc-rs`, quinn
  uses the workspace's `aws-lc-rs` provider, and `cargo tree -i ring`
  finds nothing for the host.
- **skys3-net kept the pieces private.** The peer verifier needs the
  node's certified key, the mapping from `webpki` errors to TLS alerts,
  and the SPIFFE parsing of certificate URIs. `Credentials::certified_key`,
  `pki_error`, and `PeerIdentity::from_uri_names` are now public, instead
  of a second copy in skys3-peer.
- **0-RTT cannot be attempted against the endpoint directly.** The
  endpoint issues no session tickets, so a client never has the key to
  send early data with. The test first gets a ticket under the
  destination's name from a server that issues them and accepts early
  data. It then sends a `HELLO` in 0-RTT to the real endpoint. The
  endpoint refuses the early data, and the connection is never
  established, because the early `HELLO` never arrives. A last check
  confirms that a full handshake with the endpoint leaves no ticket. With
  early data and tickets enabled in the server configuration, the test
  fails.
- **quinn sizes stream windows only at setup.** Connection send and
  receive windows can change at run time, but the per-stream receive
  window cannot. Stream windows are therefore the cap
  (`peer_max_inflight_bytes`), and the connection window, sized from the
  bandwidth-delay product every second, bounds the total. The sizing task
  holds only a weak handle to the connection. A strong handle would keep
  a connection that every user had dropped open forever, because
  keep-alives stop the idle timeout.
- **No simulation scenario yet.** Plan rule 1.1 runs peer-path code under
  the simulation harness, but quinn over turmoil needs an
  `AsyncUdpSocket` adapter for turmoil's UDP. That belongs to M6-08 (peer
  protocol simulation). This PR tests over loopback, with every network
  wait bounded at 30 s.
- **The pool's counters outlived cancelled calls (review).** A caller
  counted a connection being opened before awaiting the connect, and
  uncounted it only after the await. A cancelled caller left the slot
  taken forever, and with a limit of one the destination wedged. A stream
  also counted only once `open_bi` returned, so concurrent callers could
  all pick the same idle connection. Both reservations are now made under
  the pool's lock and held by guards that release them on drop: a
  connect slot that also wakes waiting callers, and a stream count taken
  before the stream is opened. Tests cancel a connect mid-handshake and
  check that a waiting caller takes over, and check that an open waiting
  for stream credit already counts. Each fails without its fix.
- **A pool test read the load of the test machine.** The test that grows
  the pool's limit adapted it from real loopback measurements. On a
  loaded machine, loopback's smoothed round trip reached 6 to 20 ms
  against a minimum under 1 ms, which the limit rightly reads as rising
  latency, so it held at one connection (49 of 200 runs under CPU load).
  The pool now takes its measurements from a `Meter`, by default the
  transport's statistics (`TransportMeter`). Tests that adapt the pool use
  a steady path, so they check the pool's rules and not the machine's
  scheduling. The rules themselves are unit-tested with samples.

### M6-03 Destination staging, DURABLE, and RESUME

- **The staging index lives on the accepting node, not the primary.**
  The plan has the primary stage frames and acknowledge durable ranges.
  The primary does append each frame, as an `EXTENT` record of the key
  through the shard's normal commit path, and answers only once every
  member holds it. But the index of what is staged (identity, pieces,
  durable ranges, extent positions) stays on the node that accepted the
  stream, which reports `DURABLE` and `RESUME`. Kept on the primary, it
  would be lost at every primary change (failure, planned handoff, and
  M3's rebalancing), and the relay would need new forwarding requests.
  On the accepting node it survives primary changes, because a reported
  extent is committed and a new primary keeps every committed record
  (§6.6); and the relay is M2-08's existing `AppendExtent` request
  (`PeerExtents` over the gateway's `Shards`). The cost: a source that
  reconnects to a different destination node, or outlives the node,
  restages the object. Recorded in §7.8.
- **Compaction would have dropped staged extents.** §10.3 drops
  `EXTENT` records that no entry references, and a staged extent is
  unreferenced until its `COMMIT`. §10.3 now keeps them until their
  segment is older than `peer_staging_ttl_seconds`. Compaction is not
  implemented yet, so nothing changes in code.
- **A resent frame can overlap what is held.** After a reconnect the
  source may frame the missing ranges differently, and frames still in
  flight from the old connection may land after their bytes are resent.
  Staged extents must not overlap, or a `COMMIT`'s `PUT` could not list
  them. The destination stages only the parts of a frame that are
  neither durable nor in flight, so a frame can become several extents,
  or none. A piece is capped at `MAX_EXTENTS` extents, the most a `PUT`
  references; past it the staging is refused.
- **An inbound stream could not answer while it received.**
  `InboundStream::recv` is not cancel-safe (a frame is read in several
  steps), so a loop cannot `select!` between it and finished appends,
  and `DURABLE`s would wait for the source's next frame. The sending
  half is now shared (`InboundSender`), so a reporter task sends
  `DURABLE`s while the stream's loop receives; `InboundStream::finish`
  became `async`, and two M6-02 tests changed with it.
- **Quota and TTL had no scope.** The quota counts bytes staged or in
  flight per source cluster on each destination node; a node cannot see
  the others' staging. The TTL runs from the staging's last `BEGIN` or
  `DATA`, not its creation, so a multipart upload that takes days keeps
  its staging while its parts arrive. Expiry is checked lazily, at most
  once a minute, as staging is used.
- **How it is tested.** Quinn under turmoil still needs M6-08's UDP
  adapter, so the reconnect scenario runs twice below the simulation
  harness. A proptest drives the staging table as a source with random
  frame sizes, failed appends, appends still in flight across a
  reconnect, and several reconnects. It checks that each `RESUME` reports
  exactly what is durable, that the source resends only the rest, that
  no byte is staged twice, and that the piece's extents assemble to its
  body. With trimming against in-flight ranges removed, it fails. A
  loopback QUIC test then drops a connection with two frames not
  acknowledged, and checks that the source resends exactly those two
  frames. `PeerExtents` is tested over real shards on a simulated disk.
  Every network wait is bounded at 30 s.
- **`COMMIT` and `BATCH` are answered `unavailable`.** Until M6-04 they
  get `APPLIED` with `unavailable`, so a source retries.
  `Staging::staged` and `StagedObject::extents` are the extension point:
  the extents a `PUT` references for a piece, or `None` if the piece is
  incomplete. Nothing is wired into the binary yet.
- **Staging without `DATA` cost no quota (review).** Every `BEGIN` of a
  new identity allocated an index entry charged nothing, so a source
  could grow the index without bound before the TTL swept it; tiny
  frames had the same effect per byte. Each staging is now charged
  64 KiB (`MIN_STAGING_CHARGE`, the smallest `extent_bytes`) and each
  staged part at least that much, instead of a separate count limit,
  so the one quota bounds both bytes and index memory. A `BEGIN` past
  it is answered `ABORT` (`quota exceeded`), and configuration now
  requires a quota of at least one staging and one frame. A test that
  sends only `BEGIN`s fails without the charge.
- **A late append could change replacement staging (review).** An
  append matched its staging by identity, piece, and range only. If the
  staging was discarded or expired and the identity begun again, an old
  failure removed the new in-flight range, and an old success attached
  its extent to the new staging. Each staging now has a generation that
  `admit` returns and `settle` checks. A test that settles an old
  append into a replacement fails without the check.

### M6-04 COMMIT, APPLIED, and ABORT

- **The stored result is the version itself.** The plan asks where a
  `COMMIT`'s result is kept so a replay returns it, across primary
  changes. A separate table of results would need a new record kind, an
  index table, and a rule for when to drop entries. Instead the `PUT`
  stores the commit's write identity as `x-amz-meta-skys3-wid`, which is
  where §7.2 puts it on any remote, and which the gateway already strips
  from responses and from copies. A `COMMIT` whose identity the current
  version carries is answered `committed` with that version's ETag, and
  nothing is written. The log and the index entry every member rebuilds
  from it hold the answer, so no new state is needed. The result lives as
  long as the version is current. A replay after a later write can only
  be a stale copy, because the source flushes one write of a key at a
  time. Such a copy fails its precondition, or finds its staging consumed
  and gets `incomplete`. Recorded in §7.8 ("Commits").
- **A precondition check alone does not stop duplicates.** An
  `Unconditional` `COMMIT` (the `overwrite` policy) holds whatever is
  current, so a second copy would apply again. The shard's condition
  therefore also fails when the current version already carries the
  commit's identity, and the gateway then reads the entry and answers
  `committed`. A test races two copies of an unconditional `COMMIT`. It
  fails without that clause: two records are written. The one remaining
  hole is a late copy of an unconditional delete, which needs no staging.
  §7.8 accepts it under `overwrite`.
- **"Current write identity" was undefined for local writes.** With
  `peer_local_writes`, a version can come from the destination's own
  clients and carry no `skys3-wid`. Reporting none would tell the source
  the key is absent. Such a version reports the identity of its local
  write instead: this cluster, the bucket, the shard, and the record's
  position. The forwarded condition therefore carries the destination's
  cluster ID. The intra-cluster `Condition` message got three fields and
  three kinds for peer conditions.
- **Deletes keep no identity.** A tombstone has no metadata, so a
  replayed delete cannot be recognized by identity. A delete of a key
  that has no current version writes nothing and is answered
  `committed`. This matches §7.2's 412 rule for deletes, and makes
  replayed deletes idempotent.
- **The destination does not hash staged bytes again.** The sequence
  diagram had the gateway "check checksums". Re-hashing a 5 GiB object
  at `COMMIT` would read every extent back. Each frame's CRC32C is checked
  on arrival and each extent's record checksum on disk, so the final
  checksums are stored as the source sent them. The diagram now reads
  "Check ranges and precondition". Nothing in this PR answers
  `checksum mismatch`. It stays for writes whose bytes the destination
  has whole, such as `BATCH` items.
- **The precondition is checked before the staged bytes are needed.** A
  `COMMIT` whose precondition already fails is answered with the current
  identity even with no staging, so the source learns of a conflict
  without restaging the object. The primary checks again when it
  sequences the record.
- **Staging is consumed only when the result is final.** `committed`
  discards the staging. So do `refused` and `checksum mismatch`, which
  make the source stage again. A precondition failure, `incomplete`, or
  `unavailable` keeps the staging until its TTL, so the source can commit
  it again, for example under the precondition the destination named.
  Before it applies a `COMMIT`, the stream waits for its own frames in
  flight. A source can therefore send `COMMIT` right after its last
  `DATA`.
- **`peer_source` stays in configuration.** It already existed as a
  `[buckets.<name>]` key (M0-03). The bucket pairs that authorize a peer
  are configuration too (M6-02). Moving the source into the bucket
  register would split one setting across two places. The new checks:
  `peer_source` must name a `[peering.peers]` table, and must not be set
  on a `read_only` bucket. The new key `peer_local_writes` (default
  `false`) is allowed only next to a `peer_source`. It is added to §14
  and the configuration reference.
- **Multipart `COMMIT`s are answered `unavailable`.** §7.8 says the
  destination keeps a multipart object's part boundaries. A `PUT` record
  has no parts. The multipart records cannot be reused either:
  `MPU_ABORT` and replaced parts release their extents' locations, which
  the staging still references. Publishing a multipart object in one
  record needs a `PUT` whose data lists each part's extents. That is a
  log and index format change, so it is left to a follow-up. A `COMMIT`
  that carries inline bytes is refused, since only `BATCH` items carry
  them. `BATCH` is still answered `unavailable` (M6-05).
- **A primary change is tested without the network.** Quinn under
  turmoil waits for M6-08. The replay test therefore crashes the
  destination's disk and recovers its log on another node
  (`MemoryShards::open_as`). That node opens the shard as primary in
  epoch 2, as a new primary holds every committed record. A fresh
  `PeerCommits` with no staging then answers the replay from the log.
  The loopback QUIC test streams an object and commits it without
  waiting for `DURABLE`. It replays the `COMMIT` on a new connection,
  checks a failing precondition, and checks that `ABORT` leaves nothing
  to publish. Every network wait is bounded at 30 s.
- **A local tag change kept the peer's identity (review).** A `TAGS`
  record makes a new version and clears its local write identity, but it
  kept the stored metadata. A version a peer had published therefore
  still carried the peer's `skys3-wid` after a local client
  (`peer_local_writes`) changed its tags. The source's next `COMMIT`,
  conditioned on that identity, then overwrote the tag change. The
  flusher treats `TAGS` as a write with its own identity (M1-16b), so
  `TAGS` now also drops the carried `x-amz-meta-skys3-wid` entry, and the
  version reports the `TAGS` record's local identity. The entry's name
  moved to `skys3-log` (`IDENTITY_METADATA`), next to the record limits.
  A test tags a published object and checks that the next `COMMIT` fails
  its precondition. Without the fix, it applies.
- **The identity did not always fit in the record (review).** A `PUT`
  record holds at most 8 KiB of metadata, and a `COMMIT` could carry all
  8 KiB. Adding the 116-byte identity entry then refused a valid object
  for good. §7.2 already reserves the identity's 105 bytes in the 2 KiB
  user-metadata limit, for remotes. The same is now done for the record
  limit (`IDENTITY_METADATA_RESERVED`). A client's stored metadata, and a
  `COMMIT`'s without an identity entry, stay 116 bytes below 8 KiB. A
  gateway test and a message-rules test each check the new bound, and
  both fail without it.

### M6-05 Small-object batches

- **No shard call wrote several records at once.** `Shards::write` commits
  one record, and each conditional write reads its key in its own index
  transaction. Concurrent writes share a group commit only if they reach
  the log within `group_commit_max_delay`. `Shard::commit_all_if` now
  reads the keys of a batch in one transaction and sequences every record
  whose check passes in one pass of the sequencer, so the records take
  consecutive positions and are queued for the log back to back. A test
  commits 30 records within `group_commit_max_bytes` and counts exactly
  one group commit in the log's statistics.
  `Shards::write_all` exposes it. By default it sends each write on its
  own, all at once, and `LocalShards` overrides it.
- **The forwarding protocol has no batch request.** A forwarded request
  carries its condition in a frame header, and `skys3-net` limits headers
  to 64 KiB. The peer conditions of 1,024 items take about 300 KiB. A
  batch request would need its conditions in the payload and a new answer
  with one result per item, so it is left out. `RoutedShards::write_all`
  sequences a batch in process when the map names this node as the
  primary, through `ForwardServer::write_all`, which observes it like any
  other request. Otherwise it forwards each write on its own, all at once.
  Recorded in §7.8 ("Batches").
- **A batch must look for races before it ends its read (found in
  testing).** The first version of `commit_all_if` ended its read and only
  then looked for records of its keys sequenced since the read began.
  Ending a read forgets the applied writes that no reader still needs, so
  a racing write could go unnoticed, and two writers each created the
  same key under `Absent`. A test that races batches against single
  conditional writes failed in 2 of 15 runs. The batch now looks for
  races first, as `commit_if` does, and the test passed 40 runs out of 40.
- **Batch items can be longer than `inline_max_bytes`.** By default
  `peer_frame_bytes` is 256 KiB and `inline_max_bytes` is 128 KiB, and a
  record in a hot segment carries at most `inline_max_bytes`. The
  destination therefore commits a longer body first as `extent_bytes`
  `EXTENT` records, as it does for a client upload. Such an item's `PUT`
  follows one group commit later.
- **Deletes read their key first.** A delete of a key with no current
  version must write nothing (M6-04), and a condition cannot express "skip
  without failing". Delete items therefore read their key before they are
  written, as a `COMMIT` does. Puts skip that read. A replayed put fails
  the condition's identity clause, and the entry read afterwards supplies
  its stored result.
- **Batch items are not hashed again either.** The `BATCH`'s CRC32C
  covers the bytes in transit, as each `DATA` frame's CRC32C does, so the
  final checksums are stored as the source sent them. Nothing answers
  `checksum mismatch` yet.
- **The round trips are counted, not timed.**
  `skys3-flush/tests/round_trips.rs` flushes 1,500 small objects over
  REST to the simulated store, which counts 1,500 requests (1.000 per
  object, not counting TCP and TLS handshakes). The same objects then go
  over loopback QUIC to a destination that applies them through
  `PeerCommits`. Connecting takes 2 round trips (the QUIC handshake and
  the `HELLO`s), and the two `BATCH`es take 1 each, for 4 in total (0.0027
  per object). `send_batch` sends the whole batch and finishes its stream
  before it reads anything, so one round trip per batch follows from its
  structure. The test asserts exactly 2 plus one per batch. `skys3-flush`
  gains `skys3-peer`, `skys3-net`, and `rcgen` as dev-dependencies.
- **Simulation and fuzzing.** Quinn under turmoil still waits for M6-08.
  Writers in the shard simulation now also commit batches of puts and
  deletes, some conditional, and the scenario's crash, replay, and
  in-order checks cover them. At 256 seeds its run time went from 47.8 s
  to 51.7 s, so no new scenario or cost was added. `BATCH` decoding
  already has M6-01's `peer_messages` fuzz target, and the builder parses
  no input, so this PR adds no fuzz target. A proptest packs random items
  (some with repeated keys or identities) and checks that every batch
  encodes, decodes, and that no item is lost.
- **A batch could outgrow one group commit (review).** A `BATCH` may hold
  16 MiB of inline bytes, but the log closes a group once it holds
  `group_commit_max_bytes` (4 MiB by default), so a large batch's records
  of one shard took several syncs. A test confirms it: 100 records of
  about 57 KiB with a 16 KiB cap take between 2 and 4 group commits.
  Letting one submission form a single larger group was rejected,
  because recovery's tear window (`LogConfig::tear_window`, §10.1) relies
  on no group passing the cap plus one record. The fix is on the source:
  by default `BatchBuilder` also closes a batch at 4 MiB of estimated
  records (`DEFAULT_BATCH_RECORD_BYTES`), and `with_record_bytes` sets
  another budget. Each item counts its bytes in the batch plus 256 bytes,
  which covers its record header and the destination's identity entry. A
  gateway test packs 1,024 objects of 8 KiB, some with the most metadata.
  The builder makes several batches, and each one writes at most 4 MiB of
  records at the destination. §7.8 now states the guarantee exactly. A shard's records of a
  batch share one group commit when they fit `group_commit_max_bytes` and
  reach the queue within `group_commit_max_delay`, and otherwise take one
  group commit per cap. A destination configured with a smaller cap
  splits default-size batches.

## M4 Large objects

### M4-01 Write identity for streamed single PUTs

- **The flusher needed no change, but the 412 rule had a trap.** The
  flusher already sends `write_identity.unwrap_or(version)`, written for
  `MPU_CREATE` (M1-12, M1-16b), so a `PUT` that inherits an
  `UPLOAD_BEGIN`'s position is created, replayed, and recognized after a
  412 by that one identity, as `tests/streamed.rs` checks. The trap is the
  "earlier write of this shard" clause: a write of the key that commits
  while a streamed body arrives has a *later* identity than the streamed
  PUT but is an *earlier* version. Comparing the remote identity with the
  inherited one instead of the version's position makes it foreign, and
  under `hold` the acknowledged streamed PUT is never flushed. The code
  already compared with the version; a unit test and the flush simulation
  (which now interleaves such writes) both fail under that mutation, the
  simulation at seed 11. Design §7.2 now says which position is compared.
- **When the record commits.** The gateway commits `UPLOAD_BEGIN` when the
  bytes received reach `streaming_flush_min_bytes`, not from
  `Content-Length`, and awaits it before taking another byte, so the
  identity is durable before M4-03 could stream anything. Only `write_back`
  buckets do it; copies and multipart parts never do. Its body is the key
  alone. Defining a reserved kind needs no new log version, and applying it
  changes no entry (`Effect::UploadBegun`), so the index format stays at 7.
  It is a client write (a sealed shard refuses it) but takes no
  conditional-write slot, and the forwarding wire accepts it as a write.
  `GatewayConfig::streaming_flush_min_bytes` (`None` turns it off) comes
  from `[flush]`; until M4-03 streams, a large PUT costs one more small
  record and is still flushed as one `PutObject`.
- **Compaction and a body still arriving (orchestrator's question).** The
  `UPLOAD_BEGIN` itself may go once applied: nothing reads it back, and the
  `PUT` holds its position. The body's extents are the problem, and not
  only for streamed PUTs: nothing names them until the `PUT`, and M1-22
  keeps unnamed extents only until their segment has been sealed for
  `peer_staging_ttl_seconds`. Nothing bounded how long a body could take,
  so a body slower than the TTL (it may be configured as low as a second)
  would commit a `PUT` naming dropped extents, on any replica. The shard
  checks only that the extents are *applied*. A hold per in-flight upload
  was rejected: members must know it too, so it would have to come from
  replicated state, and a failed PUT writes no record that could end it
  (a new record or index table would be a format change). Instead the
  gateway gives every body streamed as extents half of the TTL from its
  first extent (`GatewayConfig::max_body_duration`), then answers
  `400 RequestTimeout`. Design §10.3 and the configuration reference say
  so. `a_streamed_put_keeps_its_extents_and_identity_while_it_streams`
  compacts mid-stream, checks the `UPLOAD_BEGIN` is gone and the extents
  are kept, completes the PUT, compacts again, and crashes; with a zero TTL
  the extents go and the test fails.
- **Remote requests are counted.** The flush tests count requests to show
  that an inherited identity adds none: one
  `PutObject` for a streamed PUT, as for any single PUT; three (the lost
  `PutObject`, the refused retry, the HEAD) when the answer is lost; four
  when it supersedes a write whose `FLUSHED` was lost.
- **Simulation.** The flush scenario now also runs streamed PUTs: some
  with a write of the key in between, a quarter of them failed, and a check
  that no remote object or version carries a failed upload's identity. It
  took 51 s for 256 seeds alone, about as before. The cluster simulation's
  node settings set the streaming threshold to 1,024 bytes, below the
  workload's 2,048-byte bodies, so `UPLOAD_BEGIN` crosses forwarding,
  replication, crashes, takeovers, and compaction in every scenario
  without a new one. With `SKYS3_SIM_SEEDS=256` in a debug build, all 66
  cluster scenarios passed in 1,734 s (29 min). This machine has no
  baseline time from the same build.
- **Merging M2-18: compaction reclaimed nothing, and a 1 s stall.** With
  M2-18 merged, `replicated_compaction_survives_power_loss_at_its_syncs`
  reclaimed no segment at seeds 0 and 1 (5 of the first 16 seeds). It was
  not M2-18's rules alone, nor `UPLOAD_BEGIN` itself:
  - An overwritten body's unnamed `EXTENT` waits out the TTL from its
    segment's seal, and only then the release delay, from the first pass
    after it (M2-18). That order is needed: before the TTL, an extent may
    still be named and unnamed again between two passes, so an earlier
    sighting proves nothing. With 2 s each and a pass a second, a segment
    went about 4.3 s after its seal, and the workload's writes end about
    4 s in.
  - M2-18's runs lasted longer only because of a stall. A replica's
    `Appender` drains before a record of the other class, and a lazy
    `FLUSHED` in flight was then committed only after `LAZY_MAX_DELAY`,
    since the record held back was the one that would have started its
    group. So on a `write_back` shard, the first extent after a flush
    waited up to a second (about 0.9 s seen), as did every write behind
    it. An `UPLOAD_BEGIN`, a hot record, started the group first and hid
    the stall, so M4-01's runs were 1.5 to 2.5 s shorter.
  - The fix for the stall: `SegmentLog::commit_lazy_now` wakes the
    committer, and the appender calls it before any drain while a lazy
    record of its own is in flight. `a_lazy_record_does_not_hold_up_one_of_the_other_class`
    (shard) took 1.0 s before and passes now; the log test checks the
    call. With the stall gone, the scenario failed at 14 of 16 seeds.
  - The fix for the scenario: a 1 s TTL and a 1 s release delay, with the
    budget written in its doc comment. All 16 seeds pass, and both
    compaction scenarios pass at `SKYS3_SIM_SEEDS=2048`. The cluster
    nodes' gateways now get half the compaction TTL as their body deadline,
    as a real configuration has it; before, a 12 h deadline beside a 2 s
    TTL left bodies unbounded. The workload takes a `400 RequestTimeout`
    as a failed write, which faults now cause.
- **Left open.** M4-03 opens the remote multipart upload where the
  `UPLOAD_BEGIN` commits (`Upload::begin`). It must log the remote upload
  ID itself, because compaction drops the `UPLOAD_BEGIN`, and nothing finds
  an `UPLOAD_BEGIN` without a `PUT` after a crash. The same TTL hole
  applies to a peer's staging (M6-03): its staging lives on while `DATA`
  keeps arriving, but compaction counts the TTL from when each segment
  was sealed. A transfer that stages for longer than
  `peer_staging_ttl_seconds` can lose its first extents before its
  `COMMIT`.

### M4-02 Streaming multipart flush

- **Parts are sent from local extents, not teed from the body.** Design
  §7.3 said each remote part streams "from the incoming body and also from
  the local extents". A tee is not possible at the flusher: a part's body
  reaches the primary as `EXTENT` records, from its own gateway or
  forwarded by another node's, and an `EXTENT` names a key and an offset
  but not the upload or the part. Parts of one upload also arrive
  concurrently. Teeing in the gateway would need a remote connection for
  each part being uploaded, on every node, and would touch the gateway's
  part creation, which M4-05 is changing in parallel. So the primary's
  flusher sends a part once its `MPU_PART` is applied, reading the extents
  just written, which the page cache usually still holds, through the
  target's in-flight budget. The client gets its part ETag after the local
  commit, as before, and the transfer of part *n* overlaps the upload of
  the parts after it. The design's diagram and first bullet now say this,
  and a new "as built" list under §7.3 records the decisions. A part that
  fails local validation never commits, so it is never sent and never
  listed. A number uploaded again is sent again once the earlier send of
  that number ends, so the remote holds the later bytes.
- **`PART_FLUSHED` records each step; the log format is unchanged.**
  `PART_FLUSHED` was already a reserved kind of log version 2 (code 10), so
  defining it needs no new log version. Its body is the key, the local
  upload's position, the remote upload ID (1 to 1,024 bytes), and a step:
  *opened*; a *part* with its number, the `MPU_PART` position it sent, and
  the remote ETag; or *ended*. Each rides a lazy group commit
  (`commit_lazy`), like `FLUSHED`, from a detached task. The flusher awaits
  the *opened* record before it sends any part, so the ID is in the log
  before any data reaches the remote. It does not await part records.
  `ShardFlusher::stop` awaits every record still in flight, so a stopping
  flusher does not lose one. The state machine applies a step only to the
  remote upload the index holds with the same ID (`Rejection::NoRemoteUpload`
  otherwise), and an *opened* step replaces any earlier one. Applying it
  changes no entry, so compaction drops it once applied, like other
  metadata records.
- **Index format 8: two new tables.** This is a format change. The index
  `FORMAT_VERSION` goes from 7 to 8. Version 8 adds `remote_uploads`,
  keyed by `(shard, upload position)` (the same bytes as that upload's
  parts prefix), holding the key and the remote upload ID, and
  `remote_parts`, keyed by `(shard, upload position, part number)`, holding
  the `MPU_PART` position sent and the remote ETag. Rows outlive the local
  upload until an *ended* step, so the flusher can still complete or abort
  the remote upload after the local one is gone. Older builds refuse
  version 8, since they would forget those remote uploads. The value format
  stays 3; the new values decode only at format 3. Learner snapshots carry
  both tables (`ShardTable::RemoteUploads` = 4, `RemoteParts` = 5). Codec
  property tests, a table test, and the `index_codec` fuzz target cover
  them.
- **`Change` is now an enum.** `Shard::subscribe` reported only
  committed versions. The flusher now also needs uploads opened, parts
  stored, and uploads aborted, so `Change` has the variants `Stored`,
  `Opened`, `Part`, and `Aborted`. `Stored` carries the upload whose
  completion stored it. The flusher is the only subscriber.
- **Completion goes through the key's ordinary flush.** The version a
  local `MPU_COMPLETE` committed is flushed in the per-key order of §7.1.
  When the version's write identity is the upload, `put_parts` claims the
  stream. The claim waits for sends in flight. The flush then sends every
  kept part the remote does not hold from that same `MPU_PART`, lists
  exactly the kept parts, and completes with the §7.2 precondition.
  - A version without a stream takes the M1-16b after-commit path. That
    covers a version a `TAGS` record retagged, one whose remote upload is
    gone, and one whose flusher has streaming off.
  - A `404 NoSuchUpload` from the Complete aborts the stream. The flush is
    done if a HEAD then finds the version's identity (a lost answer);
    otherwise the next attempt sends the version after commit.
  - The remote multipart ETag equals the local one in every streaming
    test.
- **Aborts, and when a stream is reaped.** A remote upload is aborted when
  its local upload aborts, when the remote no longer has it, and when its
  key is clean or held in conflict without it. In those last two cases
  nothing will complete it. The conflict case was found in the
  simulation: a conflicted key's stream otherwise stayed open forever.
  Aborts are retried until `Ok` or `NoSuchUpload`, then recorded as
  *ended*. `FlushSettings::streaming` (default on) turns streaming off for
  new uploads. Remote uploads already recorded are still completed or
  aborted.
- **Load the streams before the entry scan.** The restart test raced: a
  stream loaded after the scan found its key untracked and was doomed
  wrongly. `run` now loads streams first, scans, and then reaps the
  streams of keys the scan left untracked.
- **Overlap metric.** `skys3_flush_streaming_overlap_ratio{bucket}` is a
  histogram, observed once per streamed completion: the fraction of the
  kept bytes the remote held when the local completion applied. Each part's
  send task timestamps the remote's answer, and the flusher timestamps the
  completion's `Stored` change when it reads it. The completion counts the
  parts acknowledged by then.
  Because Prometheus histograms have no `count()` or `sum()` outside
  test-util, the service test reads them from the encoded registry. The
  admin status gains `streamed_uploads` (the open streams). Both are in
  `docs/skys3-metrics.md` and §16.3.
- **Tests.** `tests/streaming.rs` has eight tests:
  - parts stream before completion, with one more request for the
    Complete and an overlap of 1.0;
  - a completion lists exactly its kept parts;
  - a part re-uploaded during a slow send follows it;
  - aborted and abandoned uploads are aborted at the remote;
  - a retagged object is sent after commit;
  - a restarted flusher resumes from the index;
  - a vanished remote upload is replaced after commit;
  - a lost Complete answer is recognized by the upload identity.

  The shard tests cover the new `Change` variants and `PART_FLUSHED`
  application. The log tests cover its layout and round trip.
- **Copied parts (after merging M4-05).** UploadPartCopy stores the copied
  bytes in the upload's shard and commits an ordinary `MPU_PART`, so the
  flusher streams it like an uploaded part, and no flush code changed.
  `tests/streamed_copies.rs` checks this end to end through the gateway,
  real shards, and the flush service: one part copies a whole 6 MiB
  source, and a second copies a 100-byte range. Both reach the remote
  upload before the client completes. The completion then sends only the
  Complete, and the remote object has the local ETag and the copied
  bytes. With streaming off, the test fails waiting for the remote
  upload.
- **Copied-part test runs in real time.** The flush tests' runtime pauses
  tokio time, and the paused clock auto-advances whenever every task
  waits. A holder's read of the 6 MiB source from the simulated disk is
  such a wait. The clock then jumped past the 30 s read-registration TTL,
  and the copy answered `503` "The object changed while it was read" in
  19 of 20 runs.
  - This is a test-harness trap, not a fault in M4-05: the copy works on
    a clock that runs normally.
  - The test now builds a real-time runtime and passed 20 of 20 runs.
  - Other tests that read large objects through holders on a paused
    clock would hit the same trap.
- **Simulation.** `streamed_uploads_reach_the_remote_only_after_their_local_commit`
  (`Runner::with_cost(8, 2)`) mixes the following on an AWS-like or
  R2-like `SimS3`:
  - concurrent uploads with random part sizes, re-uploaded numbers, and
    completions that keep a subset of the parts;
  - aborts, PUTs, `TAGS`, and DELETEs;
  - flusher restarts and injected faults.

  Before each local commit it checks that no remote object or version
  carries the upload's identity. Afterwards it checks that the remote
  object's ETag is the local one and that aborted identities never appear
  anywhere. It ends when no remote upload is open beyond the creates whose
  answers were lost, and no stream, orphan, or index row is left. The
  existing flush scenario now streams on half its seeds. Two fixes came
  from it:
  - `check_unpublished` retried a GET forever after a flush deleted the
    object, so a 404 now ends its wait.
  - M1-16b orphans left remote uploads open (seed 127), so the final
    check aborts them with `abort_orphaned_uploads` and requires none.
- **Seeded bugs the simulation catches.**
  - Completing the remote upload from `pump` as soon as the local
    upload's parts were sent, before the local commit, fails at
    **seed 0**: "upload c-test/b-flush/0/1.47 is at the remote before its
    local commit".
  - Listing every part the remote holds, instead of the kept parts
    (which lists a part the completion dropped or one replaced since),
    fails at **seed 1**: the flush never settles, with `400 InvalidPart`
    for part 2.
  - Both runs used `SKYS3_SIM_SEEDS=256`, and both mutations are reverted.
- **Timings.** At `SKYS3_SIM_SEEDS=256` in a debug build, the new scenario
  took 14.7 s alone (CI runs 128 of its seeds), and
  `flushes_reach_the_remote_through_faults` took 44.6 s on a loaded
  machine (28.1 s earlier in this task). All four flush scenarios
  together took 50.8 s. Streaming is on by default, so the cluster
  simulation's one-part multipart uploads now stream across forwarding,
  takeovers, crashes, and compaction. CI's whole simulation set
  (`cargo test --workspace --all-features --test simulation`,
  `SKYS3_SIM_SEEDS=256`) passed, with the 75 cluster scenarios taking
  2,337 s.
- **Review fixes.**
  - *A failed read could orphan an opened remote upload.* After
    `CreateMultipartUpload` and its *opened* record succeeded, the open
    read the local parts, and a failed read ended the open as if the local
    upload had closed. The stream was then dropped while the index still
    recorded the remote upload. Nothing completed or aborted it, and the
    completion opened a second one. The open now reads the parts in the
    read it already retries, before the Create, so nothing fallible
    follows the record. A part stored after that read reaches the stream
    by its change.
  - *Testing it.* `tests/stream_faults.rs` uses a `test-util` failpoint
    (`skys3_flush::test_hooks`) to fail one such read. Without the fix
    the parts never stream.
  - *Overlap depended on event order.* It counted the parts whose step
    the flusher had handled before the completion's change, and
    `select!` may take either first. It now compares each part's
    acknowledgement time with the completion's. A unit test handles the
    change before both steps, one part acknowledged before it and one
    after. Without the fix it counts neither part.
  - *Remaining skew.* A part acknowledged after the completion applied,
    but before the flusher read its change, still counts. The bound is
    the change's queueing delay in the flusher's loop.
- **Left open.**
  - Plan M4-04 adds the `ListParts` reconciliation. Until then, a part
    whose `PART_FLUSHED` was lost is sent again, which is correct but
    costs a transfer.
  - A remote upload whose `CreateMultipartUpload` answer was lost, or
    whose *opened* record a crash lost, is left to the bucket's
    abort-incomplete-uploads lifecycle rule (§7.3).
  - There is no tee of the incoming body (see the first bullet), so a
    part is read back from the local log once.

### M4-03 Streaming flush for large single PUTs

- **The gateway announces a streamed body; no record names it.** M4-01
  gave a streamed `PUT` an `UPLOAD_BEGIN` and an inherited write identity,
  but its `EXTENT` records name a key and an offset, not the upload, and a
  key may have several bodies arriving at once. The primary's flusher
  cannot tell which extents belong to which body until the `PUT` commits.
  So the gateway tells it: a `StreamedBody` announcement (key,
  `UPLOAD_BEGIN` position, metadata, tags, extents by offset) once at
  `UPLOAD_BEGIN`, and again each time at least `flush_part_bytes` more
  extents are committed. `Shards::announce` routes it to the primary,
  `Shard::announce` passes it to the flusher's subscription as
  `Change::Streamed`, and nothing is logged. The gateway sends
  announcements on spawned tasks, so the body's upload never waits for
  them, and awaits them all before the `PUT` commits, so the flusher sees
  them before the `PUT`'s `Stored`. Extents are keyed by offset, so their
  order does not matter. A failed announcement is logged at debug level
  and only costs overlap: the completion sends what the stream lacks.
- **Formats.** The log (version 2) and the index (format 8) do not
  change. The forwarding wire gains operation 19 (`Announcement`, its
  metadata and tags in the index's upload codec) and answer 15
  (`Announced`); the existing `gateway_forward` fuzz target decodes it.
- **Streams reuse M4-02's machinery.** A body stream is a `Stream` keyed
  by its `UPLOAD_BEGIN` position, opened with the announced metadata,
  tags, and identity, and recorded with `PART_FLUSHED` *opened* and
  *ended*. Part *n* is bytes `[(n−1)·P, n·P)`, `P` fixed from
  `FlushSettings::part_bytes` when the stream first pumps. A part goes out
  once announced extents cover it, read from the log, with a
  `Content-MD5` the flusher computes (inline up to 1 MiB, otherwise on the
  blocking pool). Part records are not written: their `MPU_PART` position
  does not exist, and a part only counts if it was sent from exactly the
  extents the `PUT` names in its range, which only memory knows. So a
  resumed body stream sends every part again.
- **Completion and ETags.** `Attempt::put` completes the stream when the
  version's `write_identity` names an open body stream and the version is
  streamable (bytes in the log, at most 10,000 parts). It shares the tail
  of the multipart completion (`complete_claimed`): the §7.2 precondition,
  the `404 NoSuchUpload` handling, and a `FLUSHED` with the remote
  multipart ETag as `remote_etag`, while `local_etag` stays the MD5 that
  clients see. The next flush of the key is conditioned on that
  `remote_etag`. A retagged or unstreamable version goes out as one
  `PutObject`, and its stream is aborted once the key is clean.
- **Bodies that never commit.** `FlushSettings::body_timeout` (the node
  uses `peer_staging_ttl_seconds`, twice the gateway's
  `max_body_duration`) dooms a body stream whose `PUT` has not committed
  that long after its last announcement; the flusher's loop sleeps until
  the next deadline, aborts the remote upload, and records *ended*. After
  a restart, an open remote upload without a local multipart upload is a
  body: completed by its key's flush if the `PUT` committed with its
  identity, otherwise kept for `body_timeout` and aborted. The crash test
  in `tests/streamed_puts.rs` finds the logged remote upload ID after a
  crash before the `PUT` and aborts it.
- **Surprises.**
  - *`claim` returned nothing while the stream was opening.* A `PUT` that
    commits right after `UPLOAD_BEGIN` found its stream in
    `Remote::Opening` and fell back to `PutObject`, aborting a stream that
    had just begun: in the flush simulation few streams completed. `claim`
    now waits for an open in flight, and an open whose `Create` failed
    after the `PUT` committed returns no stream.
  - *The paused clock times bodies out.* Flusher tests with
    `start_paused` and the index on the blocking pool let tokio jump the
    clock past `body_timeout` while a read waited for a pool thread. The
    new tests and the flush simulation open nodes with
    `Node::open_inline`, so the index runs on the caller's thread.
  - *Identical bytes, identical ETags.* The flush simulation copies a
    flushed object's bytes out of band to make conflicts. A streamed
    `PUT` flushed as `PutObject` has the same MD5 ETag as such a copy, so
    the flusher took the copy for its own write and deleted it. That is
    the inherent limit of ETag preconditions (§7.2); the simulation now
    copies a streamed version only when the remote holds it as its
    multipart object.
  - *A member took announcements.* `Shard::announce` first checked only
    that the shard ran, so after a primary change a gateway with a stale
    map had its announcement accepted by a member, whose flusher never
    sees the `PUT`; the cluster simulation's routing audit caught it
    (`routing_while_a_primary_restarts`, seed 0, intermittently). It now
    checks as a write does: a member answers `NotPrimary`, which
    redirects the gateway, and a primary that does not serve yet is
    unavailable.
  - *Crash tests need recovery.* The flusher test support reopened the
    index without replaying the log, which a crash test needs; `open_on`
    now replays with a `Checkpointer`.
- **Simulation.** The flush simulation (`tests/simulation.rs`) streams
  half of its large `PUT`s with 16-byte parts, two announcements, and a
  2 s body timeout, on half the seeds, and checks that each entry's
  `remote_etag` is the remote object's ETag. Seeded bugs: recording the
  `local_etag` as `remote_etag` after a streamed completion fails at seed
  18 (the `remote_etag` check), and with that check off at seed 122 (the
  flusher deletes another writer's object); completing the remote upload
  before the `PUT` commits fails at seed 2 (a failed `PUT`'s identity is
  published). No scenario was added, so no cost changes. At
  `SKYS3_SIM_SEEDS=256` in a debug build on four loaded cores,
  `flushes_reach_the_remote_through_faults` took 43.5 s alone (M4-02
  reported 44.6 s), the four flush scenarios 116 s one at a time, and
  CI's whole simulation set passed, its cluster simulation in 2,274.5 s.
- **Cluster simulation.** Gateways now announce bodies, so
  `skys3-cluster-sim` streams every `PUT` of 1,024 bytes or more in
  `write_back` buckets, with 512-byte parts, `min_part_size` 1 on the
  remote, and `body_timeout` twice `max_body_duration`. Such a `PUT` has
  at least two parts, so the durability check maps a remote ETag of two
  parts or more back to the MD5 of the remote bytes; the workload's own
  multipart uploads have one part.
- **Left open.**
  - Announcements are not durable. After a primary change or restart a
    body stream resumes only if its *opened* record survived, and then
    sends every part again; a body announced to a dead primary streams
    nothing and is sent after commit.
  - A body that is never completed after a restart waits for the full
    `body_timeout` (24 h by default) before its abort.
  - A version that falls back to `PutObject` is read whole into memory,
    as every single-PUT flush is.

### M4-04 Upload takeover after a primary change

- **Trusting `PART_FLUSHED` never built a wrong object; it could wedge
  one.** A `PART_FLUSHED` is written only after the remote acknowledged its
  part, it names the `MPU_PART` it was read from, and S3 checks every ETag
  a Complete lists. So M4-02's resumed streams could only cost transfers
  (a part whose record was lost was sent again), with one exception: a
  send of the deposed primary that its flusher took for failed, or that
  was still in flight when it died, can land after the new primary sent
  that part. The Complete then lists an ETag the remote no longer holds,
  gets `400 InvalidPart`, and M4-02 kept the stream as it was and retried
  forever. The simulation shows exactly that when the listing is removed
  (seeded bug below). The fix has two halves: a resumed stream lists the
  remote upload before it sends or completes, and an `InvalidPart` makes
  the stream list again before the retry (`Verdict::Relist`).
- **When a stream lists (decided, design §7.3).** A stream loaded from the
  index starts `Listing::Due`. An open one (local upload or body still
  arriving) lists at once from the flusher's pump, retrying with backoff,
  and sends nothing until then, so parts still to come are sent only if
  the remote lacks them. A closed one is listed by its completion. A claim
  never waits for a listing in flight: it would block a flush slot while
  the remote fails, without reporting an error. It drops the listing
  instead (listings are numbered, so a late answer is ignored) and the
  completion lists itself, which fails and backs off like any request.
- **What counts as held.** A recorded part counts only if the listing shows
  its recorded ETag. A part to send is not sent if the listing shows the
  local part's MD5 under its number: for a multipart part that is the
  index's ETag, so nothing is read; for a body part the bytes are read and
  hashed (`upload_span` now hashes before deciding). A multipart part
  found this way gets its `PART_FLUSHED`, so the index catches up. A
  number sent since the listing, or whose send failed (it may have
  landed), is dropped from the listing.
- **A gone remote upload is the completion's to judge.** The first version
  doomed any stream whose listing found `404 NoSuchUpload`. For a closed
  stream that is the "crash during complete" case: the old primary's
  Complete applied and only its `FLUSHED` was lost. Dooming made the key
  go the after-commit way, uploading every part again only to find its
  own object through the 412. Now only an open stream is ended that way;
  a closed one leaves it to its completion, whose `ListParts` failure goes
  through the same path as a Complete's (`claim_failed`): `NoSuchUpload`
  and a HEAD that finds the version's identity mean the flush is done.
  The test of that case sends three requests (`ListParts`, HEAD, abort).
- **Body streams are covered (decided, the orchestrator's question).**
  M4-03 sent every part of a resumed body again. With the listing, the
  completion keeps each part whose ETag is the MD5 of the `PUT`'s bytes
  in that part's range and sends only the others, so the resend is gone
  at the price of a local read. Announcements are still not durable: a
  body whose *opened* record never committed streams under a new remote
  upload if more announcements reach the new primary, and is otherwise
  sent after commit.
- **No format change.** The log stays at version 2, the index at format 8,
  and the forwarding wire is untouched. Listing results live in memory.
- **Simulation.** `primary_changes_at_every_step_of_a_streamed_upload_leave_no_partial_object`
  (`tests/takeover_simulation`, `Runner::with_cost(8, 4)`, so CI runs 64
  seeds) interleaves multipart uploads (re-uploaded numbers, partial
  completions, aborts), streamed PUTs (some never commit, some first
  announce extents their `PUT` will not name), and plain PUTs on an
  AWS-like or R2-like `SimS3` with faults. About one operation in six is
  a primary change aimed at a step: after open (the remote upload is
  recorded), mid-part (a moment after a part commits), after
  `PART_FLUSHED` (some parts recorded), or during complete (a moment
  after the local completion, or once the remote object appears). The
  flusher is dropped where it is, the node loses power (uncommitted
  `PART_FLUSHED` and `FLUSHED` records go, as with a dead primary), and a
  new flusher takes over from the log. At each change the deposed
  primary's sends of earlier part bodies may land up to 150 ms later.
  The checks: no identity at the remote before its local commit; every
  remote object and version with a committed write's identity holds
  exactly its bytes and ETag; aborted and failed writes never published;
  every key clean with its latest write and `remote_etag`; every remote
  upload the log ever recorded completed or aborted.
- **Seeded bugs it catches** (each run at `SKYS3_SIM_SEEDS=1024`, which is
  256 seeds of this scenario, and reverted):
  - Trusting `PART_FLUSHED` without `ListParts` (resumed streams start
    `Known`, and `InvalidPart` keeps the stream as M4-02 did): **seed 39**,
    the flush never settles, `400 InvalidPart: part 4`.
  - Reusing a listed part whose ETag differs from the local MD5, in both
    `upload_part` and `upload_span`: **seed 7**, the remote object of an
    upload holds an earlier body of its part 3.
  - The same for body parts only: **seed 9**, a streamed `PUT`'s remote
    object starts with the stale extents' bytes. Before the scenario
    announced stale extents, this bug passed all 256 seeds: the old
    primary sends a body's own bytes, so only extents the `PUT` does not
    name make a listed body part wrong.
  - Earlier in this task, the first version of the scenario caught both
    of the first two bugs at seed 0.
- **Timings.** At `SKYS3_SIM_SEEDS=1024` (256 seeds of the scenario) in a
  debug build, the scenario took 74.2 s alone; CI's 64 seeds take about a
  quarter of that. CI's whole simulation set (`cargo test --workspace
  --all-features --test simulation`, `SKYS3_SIM_SEEDS=256`) passed in 39.7 min of wall time, build
  included: the 78 cluster scenarios in 1,942 s, and the five flush
  scenarios, this one at 64 seeds, in 64.7 s.
- **Tests.** `tests/takeover.rs` plays the old primary's requests straight
  to the remote and counts the new primary's: a part sent without its
  record is not sent again (after a failed `ListParts`, too); a recorded
  part a late send replaced is sent again; a refused Complete lists again;
  a Complete the old primary sent is found by its identity; an open upload
  whose remote upload vanished is sent after commit; a resumed body sends
  only the parts whose MD5 the remote lacks; and a store that pages one
  part at a time, or whose pages stop advancing. Two existing restart
  tests now count the `ListParts`, and the resumed body test asserts it
  no longer sends its first part again.
- **Left open.**
  - The `InvalidPart` safety net relies on the target checking each part's
    ETag in `CompleteMultipartUpload`, as S3 and `SimS3` do. A target that
    assembled whatever its parts hold would let a late send through. The
    capability probe does not test it.
  - Remote uploads whose *opened* record died with the primary are still
    left to the lifecycle rule (§7.3).
  - The cluster simulation exercises takeovers of streamed uploads, but
    not aimed at steps; this scenario does that at the flusher, with a
    power loss standing in for the primary change (a new primary holds
    every committed record, and may lose the same uncommitted ones).

### M4-05 UploadPartCopy

- **The source is read as a GET reads it, not through the primary.**
  CopyObject (M1-10) reads its source's payload through the source
  shard's primary and answers `503` for an evicted source. The plan asks
  for a copy "from any source object in the cluster", so GET's loop
  (remote for a key the import has not reached, hot cache, holders on any
  node, fill of an evicted version, resolve again up to three times)
  moved out of `get` into `Objects::open`, which takes a selector of the
  bytes to serve; GET and UploadPartCopy both call it. An UploadPartCopy
  of an evicted source therefore fills just its range, and one on another
  node streams from a holder. CopyObject was left as it was (its whole-
  object `PUT` with a copy source is M4-07's to revisit). No record kind,
  wire operation, or index format changed: a copied part is an
  `MPU_PART` like any other, so completion, ListParts, `partNumber` reads,
  and the multipart flush (M1-16b) needed nothing.
- **ETag rule and known values.** A copied part's ETag is the MD5 of the
  bytes copied, which is S3's rule for objects without SSE-KMS or SSE-C,
  so the completed ETag is S3's. The tests take S3's answers from
  ceph/s3-tests at the pinned commit
  (`test_multipart_reupload_checksum_and_etag` and
  `test_multipart_use_cksum_helper_*`, which run against AWS S3 and are
  not marked as failing there): three ranged copies of one 15 MiB object
  of `A`, `B`, and `C` complete with `b2add96cc9702bbf4efb0ccdfc6b7747-3`
  and S3's per-part and whole-object checksums for SHA256, SHA1, CRC64NVME
  (also as the default), CRC32, and CRC32C. Every SDK client also checks
  that a copy at the source's own part boundaries has the source's ETag.
- **Checksums of a copied part.** UploadPart without checksum headers
  stores CRC64NVME beside the upload's algorithm (the validator always
  stores the request's algorithm or its default). A copy has no request
  checksum, so it computes only MD5 and the upload's algorithm, and the
  `CopyPartResult` carries only that one, as S3's does.
- **Which error for a range past the end.** s3-tests'
  `test_multipart_copy_invalid_range` accepts `400` or `416` but requires
  `InvalidRange`; S3-compatible servers that copy AWS's messages answer
  `400 InvalidArgument` there. The gateway answers `416 InvalidRange`, as
  a GET does, and every other form than `bytes=first-last` with both
  offsets `400 InvalidArgument`. The S3 documentation was unreachable from
  the sandbox, so M7-01 should confirm both against AWS S3. The parser
  has a proptest and a fuzz target, `gateway_copy_range`; a 30-second
  run executed about 5.3 million inputs with no failure.
- **Ranges need a source over 5 MiB (review).** The first version copied
  a range of any source. The UploadPartCopy API reference says a range
  may be copied only from a source larger than 5 MB, and AWS answers a
  smaller one with `400 InvalidRequest` ("The specified copy source is
  not supported as a byte-range copy source"). The check
  (`MIN_RANGED_SOURCE_BYTES`, 5 MiB) runs after the range is checked
  against the source, so a range past the end of a small source is still
  `416 InvalidRange`, which s3-tests' invalid-range test expects of a
  5-byte source; its improper-range test sends malformed ranges, which
  are refused before the source is read. No pinned s3-tests test, and no
  SDK client, makes a valid ranged copy of a source of 5 MiB or less. The
  gateway tests that did now use larger sources, and a test copies a
  range of 5 MiB + 1 and is refused one of exactly 5 MiB.
- **SDK copy helpers.** boto3's `copy` (s3transfer 0.19.2) and the AWS
  CLI's `aws s3 cp` between objects use UploadPartCopy above their
  multipart threshold, and s3transfer sends each part with
  `x-amz-copy-source-if-match` naming the ETag its HeadObject saw; the
  Python client asserts both. The Java transfer manager's copy needed the
  `s3-transfer-manager` artifact, over the multipart-enabled async client.
  The SDK for Go v2 has no copy helper (`feature/s3/manager` 1.23.11 and
  `feature/s3/transfermanager` 0.4.13 have none), nor has `lib-storage`
  for JavaScript, so those clients copy part by part with UploadPartCopy
  as the AWS documentation's examples do, as does the Rust client. Each
  also checks that a failed source condition answers `412`.
- **s3-tests.** Seven UploadPartCopy tests joined the subset (small,
  invalid and improper ranges, without a range, special key names,
  multiple sizes, and a percent-encoded key); the versioned, SSE, bucket
  policy, and logging variants stay out with their features. The subset
  now has 216 tests.
- **Local run.** As in M1-25 the docker daemon was not running, so every
  client ran on the host (`SKYS3_SDK_LOCAL=1`). This sandbox had no AWS
  CLI or boto3: the CLI 2.37.8 came from its installer and the Python
  packages from the pinned requirements, into scratch directories. All
  seven clients passed together in about four minutes.
- **No new simulation scenario.** The copy adds nothing to the
  replication, flush, encoding, or peer paths: its source read is M2-18's
  holder read, already simulated, and its write is an ordinary
  `MPU_PART`.
- **A slow source is not the client's fault.** A copy streams into the
  shard through the same `Upload` as a client body, with the same
  deadline (`max_body_duration`, half the compaction TTL, M4-01), which
  keeps compaction from dropping its first extents before the `MPU_PART`
  names them. Missing it answers a client `400 RequestTimeout`; for a copy
  the source was slow (a slow fill or holder), so the copy maps it to
  `503 ServiceUnavailable`, which SDKs retry.
- **Left open.** CopyObject still reads through the primary and still
  answers `503` for an evicted source. With M4-02, a copied part must also be
  streamed to the remote upload; it reaches the shard through the same
  `Upload` as an UploadPart body.

### M4-08 Write-through buckets

- **Which writes wait (decided, design §7.5).** The plan names PUTs only.
  Every write that makes a version of a key waits: PutObject, CopyObject,
  CompleteMultipartUpload, DeleteObject, each key of DeleteObjects, and
  the tagging writes. An acknowledged delete lost with the cluster would
  bring the object back, which is loss of an acknowledged write too.
  Parts, CreateMultipartUpload, and AbortMultipartUpload make no version
  and answer after their local commit. Only `write_back` buckets wait;
  `local` buckets have `backup_ack` (M4-09).
- **"The remote flush" needed a definition.** Coalescing (§7.1) means the
  version a write made may never be sent itself. A write is acknowledged
  once the remote holds its version or a later one of the key; a delete
  once `DeleteObject` succeeded, also while its tombstone waits for the
  import. The answer does not wait for the lazy `FLUSHED`: the remote is
  the durable copy, and waiting for the record would add up to a second
  (`LAZY_MAX_DELAY`) to every write on an idle disk.
- **The flusher had to be told in order.** The gateway cannot see the
  primary's flusher, which may be on another node, so it asks with a new
  `Shards::flushed`, forwarded as operation 20 (answer 16). The shard
  passes the wait to its flusher through the subscription
  (`Change::Awaited`), after the change that stored the version; then a
  key the flusher no longer tracks has had its latest version flushed,
  and needs no index read. That relied on an ordering the shard did not
  quite give: the apply loop advanced the applied position under the
  sequencer's lock but reported the changes after releasing it, so a wait
  queued in between could overtake the version's own `Stored`. The report
  now happens under the lock, and `Shard::await_flush` refuses a version
  not applied yet. `Change` lost nothing: `FlushWaiter` compares by key,
  version, and reply channel, so the enum keeps `Clone` and `Eq`.
- **What the client sees (decided, design §7.5).** A write is never
  undone: it is on every member and readable before its answer. A flush
  that does not finish within the new key `write_through_timeout_seconds`
  (default 30, added to §14 and the configuration reference) is answered
  `503 SlowDown`, and the flusher keeps sending it. A held conflict is
  answered `409 OperationAborted` at once, since a retry cannot succeed
  before the conflict is resolved. A primary change while a write waits
  ends the wait unanswered (the old flusher stops, or the replica answers
  as a member), and the gateway asks again every 100 ms; the new
  primary's flusher re-flushes the version under the same identity and
  answers. Each ask waits at most 5 s, and at most half the forwarding
  timeout, so a forwarded ask never outlives its request.
- **Tests.** `skys3-flush/tests/write_through.rs` runs the gateway, real
  shards, and the flush service against a remote whose requests take 5
  to 20 ms: each kind of write is answered only once the remote holds it
  (a streamed PUT's ETag is the remote multipart one), an outage gives
  `503 SlowDown` and the write lands once the remote is back, an
  out-of-band object gives `409 OperationAborted` (a later DELETE of the
  held key too), and a write whose flushers stop while it waits is
  answered by the next ones. With `ack_policy = "local"` all four fail.
  Unit tests cover the policy lookup, the wire round trip and refusals of
  operation 20, the forwarded wait, and the shard's ordering and errors.
- **Simulation.** `ClusterConfig::write_through` makes the cluster's
  `write_back` buckets write-through and audits them with the remote store
  as the only survivor, as if every node's disks were destroyed: each
  client checks the key right after it records an acknowledged write
  (the workload gained an acknowledgement hook), and the driver checks
  every key once the clients are done, before any fault heals.
  `write_through::losing_every_node_after_an_acknowledgement_loses_no_write`
  (`Runner::with_cost(4, COST)`, so CI runs 8 seeds) runs three
  replicated nodes with takeovers, any-gateway clients, random crashes,
  partitions, message loss, and control-store faults, and a remote that
  delays every request 2 to 15 ms each way and fails or loses 2% of
  them. With `SKYS3_SIM_SEEDS=8192` (256 seeds of each scenario) in a
  debug build, both scenarios passed in 711 s, run side by side: about
  2.8 s a seed, so CI's 8 seeds take about 22 s (21.6 s measured at
  `SKYS3_SIM_SEEDS=256`).
- **Seeded bug.** `WriteThrough::AckedLocally` keeps the audit but
  acknowledges after the local commit, as `ack_policy = "local"` does.
  `the_audit_catches_writes_acknowledged_after_their_local_commit`
  (no faults, CI's 8 seeds in 1.8 s) expects it caught, and it was at
  every one of 256 seeds, first at **seed 0**: `bucket-1/key-0`, a
  multipart completion acknowledged while the remote held nothing. The
  remote's 2 to 15 ms delays outlast a message's 1 to 5 ms, so the
  flush of a write acknowledged early never lands before its answer
  reaches the client.
- **Left open.** The native peer transport (§7.8) waits for `APPLIED`;
  its source side is M6. A destination cluster that receives a `COMMIT`
  for a write-through bucket of its own does not wait for its own remote.
  Write-through waits have no metric of their own; a timeout is logged as
  a warning.

### M4-09 Backup targets for local buckets

- **A backup is a flush target without a system of record.** The flush
  service now follows a `local` bucket whose `[buckets.<name>]` table
  names a `backup_target` (`FlushService::followed`, `Role::Backup`), with
  the same `ShardFlusher`s as a `write_back` bucket, so ordering,
  coalescing, conditional writes, write identity, and streaming are the
  ones M1-16 to M4-04 built. What a remote of record has and a backup
  does not is now optional (`SystemOfRecord`): no import (the target is
  `ImportDone`, so a key without a remote ETag is written with
  `If-None-Match: *` and a foreign object there is a conflict), no filler
  or `RemoteReader`, and no dirty budget, so a slow backup never refuses
  writes. `BucketStatus` gained `backup` and its `import` became an
  `Option`, which the admin flush status shows.
- **Clean, not evictable (decided, design §8.9, §9.3).** A backup's
  `FLUSHED` makes an entry clean, as in a `write_back` bucket, and every
  path that drops clean payload would then have dropped a `local` bucket's
  durable copy: the cache's LRU and its copies beyond `clean_copies`,
  compaction, and a learner's snapshot and backfill. Rather than make
  each of them ask the bucket's mode, the clean cache now evicts only the
  buckets it was told a `clean_copies` for (`CleanCache::evicts`), and the
  node tells it only of non-`local` ones. `Shard::evict` refuses the
  others (`CacheRefusal::Kept`), compaction copies their clean payload,
  and snapshots and backfill carry it (`Shard::evicts_clean`, also false
  with no cache). The same rule now covers a bucket whose policy the node
  has not read yet: M1-21 kept its copies but let LRU evict them, and now
  it is kept in full, and its replicas are scanned again once the cache
  is told. Read plans already named every member while the cache was not
  told, so a backed-up version is read from any member.
- **Deletes keep their tombstone (decided, design §4.2, §8.7).** In a
  `local` bucket without a backup the gateway commits the tombstone's
  `FLUSHED` itself (M1-09). With a backup it must not, or the backup
  flusher, finding no entry, would never delete the key there. The
  gateway's `Removal` and lifecycle expiration (`lifecycle::Tombstones`)
  both leave it to the backup's flush, so **lifecycle expirations reach
  the backup as ordinary deletes**, with no path of their own; the
  backup's own lifecycle rules are its business. DeleteBucket refuses a
  backed-up bucket with `409 BucketNotEmpty` while a tombstone remains,
  since detaching would stop its flusher and leave the object at the
  backup.
- **Relation to M4-08.** `backup_ack = "write_through"` is the M4-08 wait
  unchanged: `WriteThrough::wait_of` now decides by mode (`write_back` by
  `ack_policy`, `local` by a backup target and `backup_ack`), and the
  gateway then calls the same `reach_remote`, operation 20, and
  `Change::Awaited`, with `write_through_timeout_seconds`, `503 SlowDown`,
  and `409 OperationAborted`. The flusher answers the wait whichever role
  it has. The gateway also streams large single PUTs (`UPLOAD_BEGIN`,
  M4-03) to a backed-up `local` bucket, which a write-through wait on a
  large object needs.
- **Encoding waits for the backup (decided, design §8.4).** The flusher
  reads a version from the local replica, which encoding drops, so the
  encoder skips a version of a backed-up bucket that is not clean yet
  (`EncoderSettings::after_backup`, `Skip::NotBackedUp`). A later `TAGS`
  of a coded version cannot be flushed until the flusher reads coded
  objects; it stays dirty and visible in the flush metrics.
- **Base build fix.** The M5-10 base did not build the node binary
  without the `test-util` feature: the lifecycle module uses
  `tags_from_xml`, whose re-export was gated. It no longer is.
- **Tests.** `skys3-flush/tests/backup.rs` runs the gateway, real shards
  with a cache that has no room, and the flush service against a remote
  delaying requests 5 to 20 ms: every kind of write reaches the backup
  (a streamed PUT with its identity, a copy, tags), nothing is evicted, a
  delete keeps its tombstone through an outage, a bucket detaches only
  once its deletes reached the backup, and with `write_through` each
  write is answered once the backup holds it, `503` through an outage and
  `409` on a conflict. Unit and crate tests cover the cache, compaction,
  backfill, lifecycle, encoder, and gateway rules above.
- **Simulation.** `ClusterConfig::backup` gives the cluster's `local`
  buckets a backup target in the same `SimS3`, under `backup/<bucket>/`,
  and audits it. Once the clients are done and faults healed, a drain
  waits (at most 60 s of simulated time) until every live node's backup
  flushers are idle, and after the final power loss the backup must hold
  exactly what every member holds of each key, deletes included
  (`backup::audit`). Members are still checked to hold every
  acknowledged write's bytes, clean entries included. With
  `Backup::WriteThrough`, M4-08's `RemoteAudit`, now given a key prefix
  per bucket, checks each acknowledgement and every key against the
  backup alone, as if every node's disks were destroyed. Both scenarios
  in `tests/simulation/backup.rs` run three replicated nodes with
  takeovers, any-gateway clients, random crashes, partitions, message
  loss, and control-store faults, a remote delaying requests 2 to 15 ms
  each way and failing or losing 2% of them, a 2 KiB clean cache, and
  compaction. They are `Runner::with_cost(4, COST)`, so CI runs seeds 0
  to 7: 37 s for the asynchronous one and 40 s for the write-through one
  at `SKYS3_SIM_SEEDS=256` in a debug build. Both passed 64 seeds each
  (`SKYS3_SIM_SEEDS=2048`) in 580 s, run side by side. Their check that
  the backup holds some object, and M4-08's that the remote does, allow
  a run whose clients deleted every key last: M4-08's scenario does at
  seed 3 on this branch, where its imports run 32 streams (below).
- **A seed did not replay.** The simulated flush services now get the
  node's `BucketsConfig`, which backup targets need, and with it the
  default `import_parallel_streams` of 32 instead of `FlushSettings`'s 1.
  A parallel import first discovers split points by listing the remote,
  and that listing sees the capability probe's scratch keys, whose nonce
  `FlushService` drew from a randomly keyed hasher and the time: the
  split points, and so the import's checkpoint stores, differed between
  two runs of a seed, and `harness::a_seed_replays_exactly` failed at
  seed 3 with one more disk sync. `FlushService::with_probe_nonces` now
  gives probe runs consecutive nonces from a seed, which the simulation
  derives from each life's seed; nodes keep fresh ones. The shard
  flusher's open streams also moved from a `HashMap` to a `BTreeMap`,
  since the pump opened streams and handed out part sends in iteration
  order. The replay test passed seeds 0 to 127 afterwards.
- **Seeded bugs**, each without faults. The first two are
  `Runner::with_cost(2, COST)`, so CI runs seeds 0 to 7, in 19.4 s and
  2.6 s at `SKYS3_SIM_SEEDS=256`; the third runs twice the operations and
  is `Runner::with_cost(2, 2 * COST)`, seeds 0 to 3 in 16.3 s:
  - `Backup::EvictsLocal` tells the caches the `local` buckets'
    `clean_copies`, with a 1 MiB cache and no compaction, so only copies
    beyond it go and every read still succeeds. The member audit finds a
    lost copy ("is lost") at every one of seeds 0 to 63, first at
    **seed 0**.
  - `Backup::AckedLocally` acknowledges after the local commit while the
    audit expects `write_through`. The audit at acknowledgement finds a
    write the backup lacks ("is lost") at every one of seeds 0 to 63,
    first at **seed 0**.
  - `Backup::ForgetsDeletes` records each delete as flushed without
    deleting the key at the backup, through a `test-util` hook in
    `skys3-flush` (`test_hooks::forget_deletes`, thread-local as
    `skys3_shard::seed_bug` is). The final audit finds an object the
    members deleted ("the backup target holds") at every one of seeds 0
    to 127, first at **seed 0**. A lost delete shows only if it is a
    key's last change and the backup held the key, so this scenario runs
    16 keys and twice the operations; with the common workload seed 33
    missed it. Making the gateway remove the tombstone itself, the bug
    the design rules out, was tried first and went unnoticed even at seed
    0: the flusher nearly always reads the entry before it goes.
- **Hooks left.** M4-11 finds a bucket's backup with
  `FlushService::target_of`, for snapshots and the lost-key report. M6-06
  sees backups as ordinary `ShardFlusher`s, so a native transport for
  them comes with the one for `write_back` targets.
- **Left open.** `backup_target` is per-node configuration, as
  `ack_policy` is: a node whose table lacks it neither flushes the bucket
  nor keeps its tombstones, so tables must agree across nodes. The
  backup is assumed empty under its prefix; restoring from a backup, and
  a backup that already holds the bucket's objects, are left to M4-11.
  Backup lag has no metric of its own: the flush metrics carry it under
  the bucket's name.

## M5 Local erasure coding

### M5-01 EcCodec

- **The library's default rate is not a stable format.** `reed-solomon-simd`
  has two encodings, high rate and low rate, which produce different parity
  and cannot decode each other's. Its default encoder picks one per call
  from the fragment counts with a heuristic (high rate for every design
  geometry today, low rate for shapes like 2+3). A future release could
  change the heuristic and silently change fragments, so codec 1 uses the
  high-rate encoder and decoder explicitly; high rate supports every
  geometry up to the 255-fragment cap.
- **Fragment sizes.** Version 3 of the library accepts any even shard size,
  but its README promises identical output across versions only for
  multiples of 64. Codec 1 rounds the fragment length up to 64 bytes, at a
  cost of under `64·k` bytes per stripe.
- **Golden vectors checked independently.** The vectors were generated with
  `reed-solomon-simd` 3.1.0 and checked outside the repository against
  `reed-solomon-16` 0.1.0, the crate it was forked from; the high-rate
  parity matched byte for byte. A unit test also checks the run-time SIMD
  engine against the portable one, so a CI runner with a different CPU
  cannot pass with different bytes.
- **Decoding is slow unoptimized.** Every decode with a missing data
  fragment runs a transform over all of GF(2^16), whatever the fragment
  size. Unoptimized, the exhaustive erasure tests took over 30 seconds, and
  about 5 with the codec built at `opt-level = 3`, now set for
  `reed-solomon-simd` in the dev profile of the root `Cargo.toml`. The same
  cost keeps the fuzz target at about 800 runs per second.
- **`cpufeatures` 0.2.** `reed-solomon-simd` 3.1.0, the latest release,
  detects SIMD support with `cpufeatures` 0.2, while `sha1`, `sha2`, and
  `chacha20` are on 0.3. `deny.toml` skips `cpufeatures@0.2` until it
  moves.
- **Left open.** Configuration does not bound `ec.max_data_fragments +
  ec.parity_fragments` by the 255-fragment cap of `Geometry`; M5-03, which
  turns the configuration into geometries, should. A codec decodes whole
  stripes; decoding only the range a coded read needs (§8.5) is left to
  M5-06.

### M5-02 Fragment segments and fragment store

- **Fragments do not fit the log.** A fragment of a default 64 MiB stripe
  at 3+2 is about 21 MiB, past the log's 16 MiB payload limit, and a
  fragment has no shard position, so log records would distort segment
  summaries and replay. Fragment segments therefore hold their own record
  format (`skys3_ec::fragment`), in `frag-<id>.seg` files beside the log's,
  written by `FragmentStore` with the log's durability rules. The log's
  recovery already ignored files it does not know, so neither the log nor
  the index format changes. Compaction (M1-22's open point) only sees the
  segments `SegmentLog::segments` lists, so it leaves fragment segments
  alone; a test opens a log and a store on one disk and checks that each
  ignores the other's files.
- **The plan's fragment ID ignored disks.** A node has several disks, each
  with its own store, so an ID unique per disk is not unique per node. The
  ID is 128 bits: the disk's number (its position in label order), the
  segment, and the offset where the fragment was first written. That needs
  no persistent counter, never reuses an acknowledged ID (segment numbers
  only grow and the last segment stays), and survives moves by compaction.
  Recovery refuses a disk opened under another number.
- **`Geometry` and `CodecId` moved to `skys3-types`** (M5-01's open point),
  with the new `FragmentId` and `AttemptId`. `skys3-ec` now depends on
  `skys3-log` for the S3 field limits and error classes, so the log could
  not depend on `skys3-ec` for `EC_PUBLISH` (M5-04); `skys3-ec` re-exports
  the types, so its API keeps its names. `Geometry::new` now returns a
  `GeometryError`, which converts into `EcError::InvalidGeometry`.
- **Range reads need block checksums.** One CRC over a fragment would make
  a read of a range read the whole fragment to verify it. The header holds
  a CRC32C per 64 KiB block, and the header's own CRC covers them. A read
  returns the range with a CRC32C of its bytes, for the transfer to the
  gateway (M5-06).
- **Tags go stale in headers.** Tags change with `TAGS` records without a
  new version, so a header can only hold the tags of when its attempt read
  the object. Re-indexing takes those of the latest attempt; everything
  else in the object's metadata must agree across headers.
- **Re-indexing meets several attempts.** An abandoned encoding, a repair,
  and a move can each leave headers for the same object version, even with
  a different stripe count. `rebuild_layouts` keeps, per fragment index,
  the latest attempt's copy, and per stripe the latest attempt's layout
  that can be decoded, falling back to an older stripe count when the
  newest is incomplete. Design §8.4 records the rules.
- **The tear window depends on the largest fragment.** A crash can leave
  one group commit plus one record unsynced, and a record can be a 256 MiB
  fragment, so recovery's damage check only looks past that. The store's
  `max_fragment_bytes` (default 256 MiB) bounds both writes and the window;
  tests lower it to exercise the damage path. Configuration does not yet
  bound `ec_stripe_data_bytes / k` by it: M5-04 should, when it turns the
  configuration into stripes.
- **A seeded bug surfaced as a refusal, not a loss.** With recovery's syncs
  skipped, a process killed at its first data sync left a fragment only in
  the page cache; the next life kept it, wrote into a new segment, and the
  next power loss tore it inside a segment that was no longer the last, so
  recovery refused to start. The kill scenario counts that as a failure,
  as it should.
- **The crash simulation.** `crates/skys3-ec/tests/simulation.rs` draws
  one to four concurrent writers of one to four fragments each, of 64 B to
  about 195 KiB, on 256 KiB segments and 128 KiB group commits, with torn
  writes on half the crashes. It cuts the power before and after every
  sync of the workload (`SimPower`), and kills the process at every sync
  with a test-side mount that keeps the page cache; after each, every
  acknowledged fragment must read back whole with its header and a
  matching CRC32C, and the recovered store must take a fragment that
  survives the next power cut. Seeded bugs behind the new `test-util`
  feature (acknowledging before the sync, never syncing the directory,
  skipping recovery's syncs) each fail it within the first seeds. The
  scenarios declare a cost of 2: with `SKYS3_SIM_SEEDS=256` in a debug
  build they run 128 seeds each, in 18 s (power cuts) and 25 s (process
  crashes).
- **Fuzzing past the checksums.** The `ec_fragment_header` target decodes
  arbitrary bytes, and a copy with the payload's block checksums and the
  header's CRC recomputed, so the field parsers are reached; a decoded
  record must re-encode to the same bytes. 30 s ran 6.5 million inputs
  without a failure.
- **Left open.** The fragment map is rebuilt by reading one header per
  fragment at startup: a disk of ten million fragments would take ten
  million small reads, and if that proves too slow the map should move to
  an index table with checkpoints. Damage in a segment other than the last
  makes recovery refuse to start, as in the log, although repair (M5-08)
  could rebuild the fragments lost; skipping the damaged range is a
  candidate for M7. The store is not wired into the node: the fragment
  write and read RPCs come with M5-04 and M5-06.

### M5-03 Geometry and fragment placement

- **The §8.3 table follows no stated rule.** Taking the widest `k` that
  leaves one spare node gives 5+2 at 8 nodes and 7+2 at 10, where the
  table has 4+2 and 6+2, and 3+2 at 5 nodes has no spare at all. The rule
  that reproduces it: the narrowest geometry has `k = min_eligible_nodes −
  m` and needs no spare; wider ones step `k` by `m` up to
  `max_data_fragments` and need one spare node. Steps of `m` cost nothing
  at the `rack` and `zone` levels, since a `k` between two multiples of
  `m` needs as many domains as the next one. A `max_data_fragments` off
  the steps is still offered as the widest. Recorded in §8.3.
- **`⌈(k+m)/m⌉` domains are not always enough.** With uneven domains the
  cap of `m` per domain binds earlier: 11 nodes in racks of 9, 1, and 1
  span three racks but hold only four fragments of a stripe. Geometry is
  chosen from `Σ min(n_D, m)` over the eligible domains, which equals the
  design's domain count rule when every domain has at least `m` eligible
  nodes. A greedy placement under the caps then always completes, so the
  planner never returns a plan short of fragments.
- **Where the code lives.** Placement reuses `Topology`, `Candidate`, the
  domains, and the rendezvous hash of M3-03, so it is in `skys3-coord`
  (`fragments.rs`). The encoder (M5-04) runs on shard primaries and will
  depend on `skys3-coord` for it, as the gateway already does.
- **The record type had to leave `skys3-ec`.** `skys3-ec` depends on
  `skys3-log`, so `EC_PUBLISH` cannot name `skys3-ec`'s `StripeLayout`.
  `CodedStripe` and `FragmentLocation` are in `skys3-types`; `skys3-ec`
  re-exports `FragmentLocation` and converts a `CodedStripe` into its
  rebuilt `StripeLayout`, which keeps optional slots for re-indexing.
  `StripePlan::locate` joins a plan with the acknowledged fragment IDs.
- **Free space is not in the registrations.** A registration lists the
  bytes each disk offers, not what is used, so the planner ranks nodes by
  fragment bytes it is told they hold (`FragmentPlanner::hold`) plus
  those it planned. Durability preferences come first: a node in a rack
  with fewer of the stripe's fragments wins over an emptier node.
- **Configuration.** `[ec]` now rejects `max_data_fragments +
  parity_fragments` above 255. The other open bound, keeping a fragment
  of `ec_stripe_data_bytes` within the fragment store's 256 MiB, depends
  on the narrowest `k` (`GeometryPolicy::narrowest`) and spans `[ec]` and
  the bucket tables; it stays with M5-04, which turns the configuration
  into stripes, as M5-02 recorded.
- **Left open.** Nothing calls the planner yet: the encoder (M5-04)
  checks `FragmentPlanner::geometry` before an object qualifies, plans
  each stripe, and logs the `CodedStripe`s. Cluster health does not yet
  report a policy whose geometry the cluster cannot support. Repair
  (M5-08) and moves (M5-09) need placement around fragments a stripe
  keeps; `choose_nodes` is the place to add kept fragments to the
  per-domain counts.

### M5-04 Encoder and EC_PUBLISH

- **No new log version; index format 9.** `EC_PUBLISH` was reserved kind
  14, so defining its body (`skys3_log::record::EcPublish`) keeps log
  version 2: the key, the version's position and ETag, the attempt, the
  size, and per stripe its length, `k`, `m`, codec, and each fragment's
  node and ID. The index moves from format 8 to 9 for two tables,
  `fragments` (the per-shard index from node to fragments, keyed by
  shard, node, key, stripe, and index) and `attempts`, and its values
  from format 3 to 4 for the entry's coded layout (`ObjectVersion::coded`:
  the `EC_PUBLISH` position, the attempt, the stripes). Both recorded in
  §10.1 and §10.2.
- **What supersedes a publish.** Applying `EC_PUBLISH` codes the version
  only if the entry still has its position, ETag, and size and is not
  coded; otherwise it is rejected (`Rejection::Superseded`,
  `AlreadyCoded`, `NoEntry`, `Deleted`). `TAGS` moves `Entry::version`,
  so tags changed during an attempt supersede it, which is conservative;
  `TAGS` after a publish keeps the layout. `PUT`, `DELETE`, `ADOPT`, and
  `MPU_COMPLETE` replace the layout and remove its `fragments` rows.
- **Members drop replicas through compaction, gated on the commit.** A
  coded version's payload is `Holder::Coded` in the index. Compaction
  treats it as unreferenced only once the replica's commit watermark
  (`Shard::replicated_through`) covers the `EC_PUBLISH`, or the replica is
  alone, and then waits the release delay as for any unreferenced
  payload. The gate is needed because replay applies a log's uncommitted
  tail: a member that restarts holding an uncommitted `EC_PUBLISH` codes
  the entry, and a takeover could still truncate the record.
- **Fragment writes use the cluster transport, not the forwarding wire
  op.** Two message kinds in the replication class, `FragmentWrite` (7)
  and `FragmentWritten` (8), so mutual TLS names both ends and an admin
  certificate cannot write fragments. A write is the encoded
  `FragmentHeader` (`to_bytes`/`from_bytes`, new) and the fragment in
  frames of at most 8 MiB, under the transport's 32 MiB payload limit;
  the first frame's body carries both lengths and the fragment's CRC32C,
  and the receiver checks the lengths before it buffers anything. The
  receiver keeps the fragment in its store holding the fewest bytes and
  answers once it is durable. One connection per write, the answer read
  while frames go out so an early refusal never stalls both ends, and a
  node writes its own fragments without a connection.
- **Attempt numbers are reserved durably.** `Index::reserve_attempts`
  hands out blocks of 64 per shard in one durable commit and never goes
  back, so a primary that restarts in the same epoch never reuses a
  number. The encoder's `Attempts` tracks each attempt as writing, then
  publishing, until its record commits: `Attempts::abandon` succeeds only
  while it writes, which is the fence M5-05 needs, and an attempt whose
  commit was not confirmed stays publishing until the primary applies the
  record.
- **The encoder lives in `skys3-ec`** and now depends on `skys3-shard`,
  `skys3-coord`, `skys3-index`, and `skys3-net`. It reads the object from
  the primary's replica (`Shard::plan`, `Shard::payload`), plans each
  stripe with `FragmentPlanner`, writes the fragments in parallel through
  the `FragmentWriter` trait, re-plans a stripe without nodes whose writes
  failed (three times), and commits `EC_PUBLISH` on a task of its own so
  it is appended even if the encoder is dropped. An `EncodeObserver` sees
  each step; the simulation crashes nodes at them.
- **Stripe size bound.** Configuration validation now rejects an
  `ec_stripe_data_bytes` whose fragments in the narrowest geometry
  (`k = min_eligible_nodes − m`, clamped as `GeometryPolicy::narrowest`
  does) would exceed the fragment store's 256 MiB: at
  `[buckets.defaults]` and at each bucket whose table changes the stripe
  size. With the defaults the limit is 768 MiB. `skys3_config` exports
  `MAX_FRAGMENT_BYTES`, which the encoder asserts equals
  `MAX_FRAGMENT_LEN`.
- **The simulation has its own harness.** `Cluster::run` reads objects
  back through `GET` and checks survivors by replicated payload, neither
  of which understands a coded object until M5-06, so
  `skys3_cluster_sim::coding` runs six real nodes itself: one shard with
  members n0 to n2 under a static configuration, a fragment store and a
  fragment server on every node, checkpoints, and compaction with a
  300 ms release delay. Every 10 ms it checks that each object is
  readable from a member's replica or from `k` listed fragments of each
  stripe of a coded layout (nodes that are down count as holding what
  they held), and that no member dropped a coded version's bytes before
  some replica learned a watermark past its `EC_PUBLISH`. Once every
  object is coded and its replicas dropped, every node loses power and
  is recovered outside the simulation, and each member's entry must read
  back the last bytes written, from its log or decoded from fragments
  whose headers name that version, stripe, and index.
- **Crashes and seeded bugs.** The crash scenario covers, each seed,
  every step (a stripe's start, two fragments durable, a stripe's end,
  the last stripe's start, `EC_PUBLISH` sequenced, and committed) against
  every target (the primary, either member, one holder outside the shard,
  every holder of the stripe but the primary), with a kill, a power loss,
  or a power cut at the next sync drawn per run. A host whose disks lost
  power at a sync stops 3 ms later, so what it sent before the sync still
  leaves. Each seeded bug is first run clean and must pass: a fragment
  store that acknowledges before its sync (holders of the last stripe
  cut at their next sync) is caught by the read-back, since the lost
  fragments' IDs are reused for later fragments; members dropping
  replicas on an uncommitted `EC_PUBLISH` (`SeededBug::DropBeforeCommit`,
  one member down 3 s and the other restarted at once) is caught by the
  drop rule; and applying a superseded `EC_PUBLISH`
  (`SeededBug::PublishSuperseded`, the first object overwritten with as
  many bytes during its encoding) is caught by the read-back.
- **Seeds and cost.** CI's 256 seeds run seed 0 of the crash scenario
  (30 runs, 35 s in a debug build) and seeds 0 to 15 of each seeded-bug
  scenario (about 36 s, 40 s, and 55 s). All pass, every bug is caught
  on every one of them, and the crash scenario also passed seeds 1 to
  40 (1,200 runs, 25 minutes). The live check counts listed fragments
  without reading them, so a fragment lost under a reused ID shows only
  in the read-back.
- **Left open.** The node binary runs neither replication nor fragment
  stores yet, so nothing starts the encoder outside tests; the harness's
  accept loop (a first frame of kind `FragmentWrite` goes to the fragment
  server, anything else to replication) is how a node will dispatch.
  Cluster health does not report that no geometry is supportable;
  `ScanReport::paused` carries the `NoGeometry` for it. An object whose
  layout cannot fit the 2 MiB record header stays replicated. While an
  attempt's commit is unconfirmed the next scan may encode the object
  again, and the later `EC_PUBLISH` is rejected as `AlreadyCoded`. Orphan
  reclamation (M5-05) can use `Attempts::abandon` and
  `Attempts::in_progress`; coded reads (M5-06) have `ObjectVersion::coded`;
  repair (M5-08) has `IndexReader::fragments_on`.
- **Fuzzing.** A new target, `ec_transfer`, decodes the fragment header
  that starts a write and both message bodies; `log_record` and
  `index_codec` already reach the new record body and values.

### M5-05 Orphan fragment reclamation

- **The plan's fence had two holes.** Confirming only attempts not in
  progress, after reconciliation, is not enough. A primary deposed
  without knowing it does not track its successor's attempts, so it
  would judge their fragments orphans: the judge now treats an attempt of
  a later epoch than the one its replica sequences in as in progress
  (the seeded `JudgeBug::IgnoreLaterEpochs` is caught). And the
  encoder's shard handle outlives epochs, so an attempt started under one
  primary could publish under a later epoch of the same node, after
  another primary judged it abandoned: `EC_PUBLISH` now goes through the
  new `Shard::commit_in`, which appends only while the replica still
  sequences in the attempt's epoch. Recorded in §8.4.
- **`Attempts::abandon` was the wrong hook.** It abandons a writing
  attempt, but the plan counts writing attempts as in progress; using it
  would abandon every encoding that outlasts
  `fragment_orphan_after_seconds`, so a large object would never be
  coded. The judge uses a new `Attempts::orphaned(attempt, epoch)`: a
  writing attempt of the replica's epoch, or a publishing one, is in
  progress; one still writing from an earlier epoch cannot publish any
  more and is abandoned in the same step; an unknown one is not in
  progress. The encoder also forgets an attempt whose future is dropped
  before it publishes, which would otherwise stay "writing" forever.
- **The judge reads the index without leases.** `Shard::entry` admits a
  read only while the primary holds every member's lease, which a
  deposed primary loses within `primary_lease`. Reading through it would
  hide the epoch rule behind a clock bound, so the judge reads its
  replica's index directly on a blocking pool: the fence relies on
  epochs, not clocks.
- **Orphan queries are messages of their own.** `OrphanQuery` (9) and
  `OrphanVerdicts` (10), in the replication class, with the bodies in
  the frame's payload: a query of 256 fragments with long keys exceeds
  the 64 KiB frame header. The asking node is the TLS peer, so a node
  only learns verdicts on fragments it says it holds. A first version
  tried the nodes that may lead the shard in turn, each within the
  transport's 10 s handshake bound; in the takeover runs a node that was
  down then held every sweep that asked it for 10 s, past the whole
  window in which the before-reconciliation bug can strike (seed 25 of
  that scenario let it through even with 2 s per node). A query now goes
  to every such node at once, and each node gets 1 s, so an outage holds a
  sweep up about a second. Taking the first verdicts then failed wide
  runs (seed 7 of the primary-change scenario at 2048, the primary
  isolated as its `EC_PUBLISH` was appended): the deposed primary, back
  but still believing it led epoch 1, kept answering "in progress" for
  its own publishing attempt, at once since it asked itself, and its
  orphan was never reclaimed. "In progress" is safe but not final, so the
  first answer counts only if it decides every fragment; otherwise the
  answers are combined, a decided verdict winning over one in progress
  and referenced over orphan. A new `ec_orphans` fuzz target decodes and
  checks both bodies (1.1 million inputs in 30 s, no failure);
  proptests round-trip them.
- **Ages without timestamps.** The fragment map keeps no write time, and
  adding one would change the fragment format. The reclaimer counts a
  fragment's age from when its life first saw it, so a restart only
  delays reclamation, and it keeps the fragments a primary judged
  referenced out of further queries for the rest of the life.
- **Reclaiming without a format change.** A reclaimed fragment leaves the
  fragment map, and a segment whose records are all reclaimed is removed
  unless it is the last (recovery already tolerated gaps in segment
  numbers). Nothing is written: a restart finds the fragments of
  surviving segments again, and they are reclaimed again once asked
  about. Partly reclaimed segments wait for fragment-segment compaction
  (M5-07).
- **A member dropped the extents of a `PUT` it had not applied.** In the
  first takeover runs, the new primary could not encode an object: "no
  payload is located". The old primary had committed the object's `PUT`
  and died before its watermark reached the members; a member then held
  the `PUT` unapplied and, as compaction drops unnamed `EXTENT` records
  once their segment has been sealed for `unreferenced_ttl` (1 s in the
  harness), dropped the extents before the takeover applied the `PUT`.
  The production default is a day, so takeover runs use 15 s, but
  compaction could keep extents past the replica's applied position
  instead (an open point for M1-22's owner).
- **The harness grew rather than forked.** `skys3_cluster_sim::coding`
  takes `OrphanConfig` (reclaimers and judges on every node, with a
  seeded `JudgeBug`), `write_delay` (slow fragment writes, through a
  writer that also records each fragment's attempt), `takeover` (an
  in-memory shard register, removal and takeover, an encoder on every
  member that works while it leads), and `CrashKind::Isolate` (the
  primary cut off from the members and the register, still reachable by
  fragment holders). Nodes ask all of the first three members, so a
  stale primary gets asked too. The new check reads the
  layouts of every replica that leads and serves, since members can
  replay uncommitted records; the final check reads every member's. A
  run settles only once every fragment held is named by a member's
  layout or was published once (released fragments are M5-07's), which
  is the "orphans are reclaimed" half of Done when. The readability
  check still spans the first three members, because a new primary
  applies its predecessor's last commits only after reconciliation, and
  members are taken from the register everywhere else.
- **Scenarios, seeds, and cost.** At CI's 256 seeds in a debug build:
  reclamation racing publication (`orphan_after` 0 to 60 ms, a crash at
  a drawn step and target, then an overwrite that supersedes an attempt)
  runs 8 seeds of two runs in 34 s; encodings outlasting `orphan_after`
  (150 ms per fragment write against 100 ms) 8 seeds in 21 s; a primary
  change (the primary killed or isolated for 9 to 12 s at a drawn step)
  2 seeds of two runs in 43 s. Seeded bugs, each run clean first:
  confirming orphans without the tracker (`IgnoreAttempts`, caught with
  slow writes) 8 seeds in 26 s; answering before serving
  (`BeforeServing`: a member killed as the `EC_PUBLISH` is appended, the
  primary 200 ms later, the survivor taking over and rolling the record
  forward once it serves) 4 seeds in 36 s; a deposed primary judging a
  later epoch (`IgnoreLaterEpochs`, the primary isolated for 12 s) 2
  seeds in 38 s. Every bug is caught on every seed tried, with "which
  was reclaimed". Wider runs, at `SKYS3_SIM_SEEDS=2048` with the final
  client, all passed: 64 seeds each of racing, outlasting, and
  confirming without the tracker; 16 of the primary change; 32 of
  answering before serving; 16 of the deposed primary. The M5-04
  scenarios still pass at 256 seeds (44, 62, 46, and 42 s).
- **How repair and moves join the fence (M5-08, M5-09).** Register each
  attempt in the node's shared `Attempts` for the shard (`begin`,
  `publish`, `sequenced`, `finish`, crate-private in `skys3-ec`), append
  `EC_RELOCATE` with `Shard::commit_in(attempt.epoch, …)`, and have
  applying it update the entry's coded layout, which the judge reads.
  The fragment a move or repair replaces is then unreferenced, and an
  orphan unless M5-07 releases it first. The harness's `write_delay` and
  `takeover` give the "outlasts `fragment_orphan_after_seconds`" and
  primary-change scenarios.
- **Left open.** Nothing runs reclaimers or judges in the node binary
  yet (M5-04's open point); the harness's accept loop shows the
  dispatch, and a node will need the shard map (M2-08) as its source of
  primaries. A version overwritten after it was coded leaves fragments
  no layout names, which the judge calls orphans: once coded reads exist
  (M5-06), reclaiming those must also wait for the release delay and
  read registrations (M5-07), and the reclaimer is the place to check
  them. The reclaimer keeps one entry per fragment in memory, as the
  fragment map does. There are no metrics yet; `SweepReport` has the
  counts.

### M5-06 Coded reads

- **Ranged decoding needed two codec methods.** `EcCodec` could only
  decode whole fragments, so a degraded 1 MiB read of a 64 MiB stripe
  would have read and decoded all of it. reed-solomon-simd's high-rate
  code works on each 64-byte column on its own, so `EcCodec` gained
  `columns` (the 64-byte-aligned columns that hold a range of a
  fragment) and `decode_columns` (the data fragments' bytes of those
  columns from any `k` fragments' bytes of the same columns). Both are
  required trait methods; codec 1 implements them with the same checks
  as whole decoding, and a new `EcError::OutsideFragment` refuses
  columns past the fragment's length. Recorded in §8.5.
- **The reader is transport-free.** `skys3_ec::read::read_coded` takes a
  `FragmentSource` (one async `read` of a fragment range) and a
  `CodedRead` (the entry's coded layout, the object's size, the range)
  and returns a channel of body pieces, the first read before it
  returns. Pieces are at most 1 MiB within one data fragment. A failed
  fragment (not held, damaged, CRC32C mismatch, short) is skipped for
  the rest of its stripe and an unreachable node for the rest of the
  read; a degraded piece takes data fragments first, then parity, in
  index order. `FragmentReadClient` is the network source, with a local
  shortcut to the node's own `FragmentServer`.
- **Reads name what they expect.** A fragment ID alone is not enough: a
  fragment reclaimed since the plan (M5-05) or an ID reused on a
  replaced disk would serve another fragment's bytes, which pass their
  checksums. A `FragmentRead` names the shard, key, version, stripe,
  geometry, codec, and index, and the node answers "not held" unless
  the fragment's header matches. Recorded in §8.5.
- **Two message kinds, no format change.** `FragmentRead` (11) and
  `FragmentData` (12), replication class, request in the frame header
  (it is small), bytes in the payload with their CRC32C. The fragment
  listener dispatches on the first frame's kind, so writes and reads
  share it. The node computes the CRC32C from the bytes it verified
  against the fragment's block checksums, so corruption on the way is
  caught by the gateway. A new `ec_read` fuzz target decodes and checks
  both bodies; a proptest round-trips them. Nothing on disk changed.
- **Coded plans name no holders.** `Shard::plan` gave a coded version
  the layout of its dropped replica payload, so the gateway would ask
  each member and be refused before reading fragments. The plan of a
  coded version now has no layout and no holders, and the gateway reads
  the entry's coded layout instead (`GatewayConfig::fragments`; without
  a source a coded GET answers `503`). A stripe with fewer than `k`
  readable fragments re-resolves the key like a holder miss, up to three
  times, then `503`. Whole reads fill the hot cache. Recorded in §9.2.
- **No fragment read registrations (the M5-05 open point).** A
  registration only matters once something removes fragments a plan may
  still name. Today that is only the orphan reclaimer, for a version
  overwritten after it was coded; such a read fails cleanly (identity
  check, then re-resolution finds the new version). Release with
  `fragment_release_delay_seconds` and registrations, honoured by the
  reclaimer too, are left to M5-07, which owns release. Recorded in §8.5.
- **Flusher reads of coded objects stay open (M4-09).** The flusher
  reads a version's payload from the primary's log, which a coded
  version no longer has. The encoder codes a version of a bucket with a
  backup target only once the backup holds it (M5-04), so a regular
  flush never needs a coded version's bytes; one that must send a coded
  version again would fail and retry. Reading through `read_coded`
  there touches the flusher, which M4-11 is changing in parallel, so it
  is left for a follow-up.
- **The property test.** `tests/reads.rs` reads, for every geometry of
  the design's table, every set of up to `m` lost fragments, each
  missing or corrupt in flight, over three stripes, for the whole object
  and a range cutting through fragments; a proptest draws geometries
  1+1 to 10+4, object and stripe sizes, ranges, and up to `m` losses per
  stripe of any kind (not held, damaged, unreachable, corrupt). More
  than `m` losses fail with `Unreadable`. The seeded bugs
  `ReadBug::TrustCrc` (keep bytes that fail their CRC32C) and
  `ReadBug::WrongIndices` (decode with the fragments read packed into
  the first slots) read wrong bytes there.
- **The simulation.** `skys3_cluster_sim::coding` takes a `ReadConfig`:
  after the coding scenario's objects are coded and their replicas
  dropped, every node runs a gateway (an in-memory control store and
  shard map, routed shard client, hot cache) and three clients send 40
  whole or ranged GETs each through drawn nodes while, for four seconds,
  up to two *lossy* nodes of n1 to n5 (`m` is 2 on six nodes) crash,
  with or without power loss, lose their fragment disk for good, or
  corrupt the bytes they send. Every answer must be its version's bytes,
  by its ETag; `503` and broken-off bodies are allowed. The harness's
  fragment source counts parity reads, and the scenario requires some.
- **Crashes do not replay within one process.** Two runs of a seed in
  one process differed after a crash. A crashed host's runtime drops its
  tasks in an order tokio shards by task ID, and task IDs are global to
  the process, so its sockets close in another order on the second run
  and the latencies drawn after them differ. A fresh process replays a
  seed exactly. The replay scenario therefore runs coded reads with
  corruption only; the crashing scenarios still check every answer.
  (ECDSA signatures in the handshakes also vary in length, harmlessly:
  nothing depends on their size.)
- **Scenarios, seeds, and cost.** At CI's 256 seeds in a debug build:
  replay (cost 128) 2 seeds in REPLAY_TIME; degraded reads (lossy 2 on
  even seeds, else 1; cost 32) 8 seeds in 32 s; trusting bad CRCs
  (corruption only) 4 seeds in 30 s; wrong indices (crashes and lost
  disks) 4 seeds in 29 s. Both bugs are caught on every seed tried, with
  "not those of version". At `SKYS3_SIM_SEEDS=2048` all passed: 64
  seeds of degraded reads, 32 of each bug; replay passed 32 seeds at
  4096.
- **Hooks for M5-07, M5-08, M5-09.** Release should register fragment
  reads where the gateway resolves a coded plan (`objects/coded.rs`) and
  have `FragmentServer::read` check registrations, like holder reads.
  Repair (M5-08) can reuse `read_coded`'s column decoding through
  `EcCodec::decode_columns`, or read whole fragments through
  `FragmentReadClient`. A move (M5-09) changes the coded layout by
  `EC_RELOCATE`; readers holding the old plan fail on the identity check
  and re-resolve.
- **Left open.** The node binary does not serve fragment reads or set
  `GatewayConfig::fragments` yet (as for M5-04's encoder). CopyObject
  reads its source's payload from the source primary, so copying a coded
  object fails; it should read through `read_coded`, as GET and
  UploadPartCopy (which reads its source as a GET does) now do. There are no metrics for degraded reads yet.

### M5-10 Lifecycle expiration and multipart cleanup

- **The configuration lives in the bucket register.** An optional
  `lifecycle` field of `BucketDocument`, written only when set (as
  `departing` is on a node registration), so registers without one keep
  their bytes and no format bump was needed. It travels with the
  generation and the local copies, and a rebuild keeps it. The field had
  to be added to some two dozen `BucketDocument` literals across the crates.
  Validation (`skys3_types::lifecycle`) runs both in the gateway and in
  `BucketDocument::validate`, which also refuses a configuration on a
  bucket that is not `local`.
- **Only `local` buckets.** A `write_back` (or `write_through`) bucket
  answers PUT with `501 NotImplemented`: its durable home is the remote,
  and expiring the local copy alone would be undone by an import or
  diverge from the remote's own rules. Recorded in §8.7.
- **What is supported.** Prefix, tag, and size filters and `And`,
  `Expiration` by `Days` or `Date`, and `AbortIncompleteMultipartUpload`;
  transitions, noncurrent-version actions, and `ExpiredObjectDeleteMarker`
  set to true answer `501`. `ExpiredObjectDeleteMarker` set to false is
  accepted and ignored, as there are no delete markers, so a rule that has
  only it answers `400 InvalidRequest` for having no action. s3s does not
  require `Content-MD5` on this PUT, so the gateway does. Recorded in §11.
- **`Days` rounds to midnight UTC.** S3 counts a rule's days from
  `Last-Modified` and rounds up to the next midnight UTC, so an object
  written at 23:59 with `Days = 1` expires two minutes later at the
  earliest. The evaluator and the property test's reference evaluator do
  the same. `s3s` hands a `Date` as a `time` value, so the gateway gained
  the `time` crate, already in the tree, to convert it.
- **Passes keep no state.** Each expiration is a `DELETE` committed with
  `Shard::commit_all_if` only if the entry still holds the version the
  page read, followed by a `FLUSHED` that removes the tombstone, as for
  any delete in a `local` bucket. A pass resumes after a primary change
  simply because the new primary runs the next one; it skips a primary
  that is not serving yet, so it never judges an index that lacks what
  the earlier primaries committed.
- **The simulation needs its own clock.** The gateway stamps
  `Last-Modified` from the real wall clock, not simulated time, so the
  cluster simulation cannot age objects. Its passes read a lifecycle
  clock instead, which starts at a midnight in the 23rd century when the
  workload starts and runs a day per simulated second; `Date` rules fall
  at chosen moments of the run and `Days` rules at its first pass.
- **Abandoned deletes are not always truncated.** In the simulation, a
  deposed primary that the coordinator then replaced keeps the `DELETE`s
  it never committed in its log with no `TRUNCATE` after them. The audit
  therefore counts as abandoned every `DELETE` some node's log holds that
  the final primary's does not commit, and checks exactly-once on the
  final primary's log. Holding the primary's messages across a rule's
  date must last less than `member_suspect_after`, or the primary removes
  its members and nothing is taken over.
- **Simulation cost.** Both scenarios together take about 18 s in a
  debug build at CI's 256 seeds (8 seeds each, cost 32), and 75 s at 1,024.
- **Left open.** A pass over a large shard runs to its end in one interval
  with no rate limit; deletes of a page share group commits. Expired
  objects stay readable until the next pass, as S3 allows. There are no
  per-bucket lifecycle metrics, and bucket status does not report the
  last pass.
