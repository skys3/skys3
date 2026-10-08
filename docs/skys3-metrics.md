# SkyS3: Metrics Reference

**Implements:** the metrics rules of the [task and PR plan](skys3-tasks-plan.md) (section 1.1, M0-06) for the [SkyS3 design](skys3-design.md).

Every metric a SkyS3 node exports is listed here. A PR that adds a metric adds
its row in the same PR; a metric that the design names but no merged PR
exports yet is listed as planned, with the PR that owns it. Section 4 lists the
alerting rules, section 5 the dashboard, and section 6 which metric or alert
shows each failure of the design's failure matrix.

The tests of the `skys3` crate (`crates/skys3/src/metrics/reference.rs`) keep
this reference, the code, and the files under `deploy/` in step: they register
every component's metrics in one registry and fail if a registered metric has
no row here, if a row (other than a planned one) names a metric no code
registers, or if a type differs; a scan of the crates' sources fails if some
registration is missing from that registry. They also fail if an alerting rule
or a dashboard query reads a metric not listed here, if section 4 and the rules
differ, or if section 6 misses a row of design section 13.

## Contents

- [1. Scraping](#1-scraping)
- [2. Naming conventions](#2-naming-conventions)
- [3. Metrics](#3-metrics)
  - [3.1 Process, node, and admin listener](#31-process-node-and-admin-listener)
  - [3.2 Flush and loss exposure](#32-flush-and-loss-exposure)
  - [3.3 Replication](#33-replication)
  - [3.4 Read-through fill](#34-read-through-fill)
  - [3.5 Clean cache](#35-clean-cache)
  - [3.6 Segment compaction](#36-segment-compaction)
  - [3.7 Hot cache](#37-hot-cache)
  - [3.8 Lifecycle](#38-lifecycle)
  - [3.9 Repair](#39-repair)
  - [3.10 Coordinator and placement](#310-coordinator-and-placement)
- [4. Alerts](#4-alerts)
- [5. Dashboard](#5-dashboard)
- [6. Failure matrix coverage](#6-failure-matrix-coverage)

## 1. Scraping

Each node serves its metrics on the admin listener at `GET /metrics`, in the
OpenMetrics text format (`application/openmetrics-text; version=1.0.0`), which
Prometheus scrapes natively. The listener binds to `127.0.0.1:7490` by default.
On any other address it requires a bearer token, and a scraper sends
`Authorization: Bearer <token>` (Prometheus: `authorization.credentials_file`).
Design section 12 gives the rules, and section 14 the `[admin]` keys.

`/healthz` (liveness) and `/readyz` (readiness) are served on the same
listener without authentication, for load balancers and orchestrators.

Each node exports only its own state. Cluster-wide views are built by the
monitoring system from all nodes' series, never by one node on behalf of
others.

## 2. Naming conventions

The registry (`skys3_obs::MetricsRegistry`) enforces these rules when a metric
is registered.

- **Prefix.** Every exported name starts with `skys3_`. Code registers the base
  name, and the registry adds the prefix.
- **Base name.** Lowercase `snake_case` ASCII, starting with a letter, with no
  leading, trailing, or doubled `_`.
- **Units.** A metric with a unit declares it at registration, and the exporter
  appends it to the name. Durations are in seconds (`_seconds`), sizes in bytes
  (`_bytes`), and fractions are ratios from 0 to 1 (`_ratio`). Milliseconds,
  kibibytes, and percentages are never used.
- **Type suffixes.** The exporter appends `_total` to counters and `_info` to
  info metrics. A base name never ends with a unit or type suffix
  (`_bytes`, `_seconds`, `_ratio`, `_total`, `_info`, `_count`, `_sum`,
  `_bucket`, `_created`).
- **Design names.** The design names metrics without the prefix and sometimes
  without the unit. The exported name is `skys3_` plus the design name plus the
  unit suffix if the design name lacks it: `oldest_dirty_age` is exported as
  `skys3_oldest_dirty_age_seconds`, and `flush_lag_seconds` as
  `skys3_flush_lag_seconds`.
- **Labels.** Label names are `snake_case`. Label values come from a bounded
  set: node, bucket, shard, target, endpoint, or status code. Object keys,
  upload IDs, request IDs, and other unbounded or client-chosen values are
  never labels. A label that repeats the node's identity is left to the
  scraper's target labels.
- **Gauges versus counters.** A value that can go down (bytes dirty, age of the
  oldest entry) is a gauge. A count of events (requests, conflicts, retries) is
  a counter and never decreases while the process runs.

## 3. Metrics

Status is **exported** once a merged PR registers the metric, **defined** while
the metric exists but the node binary does not run the component that
registers it yet, and **planned** while the design names it and the owning PR
has not merged. The owning PR of a
planned metric decides its labels and records them here. **Design** names the
sections of the [design](skys3-design.md) the metric reports on.

### 3.1 Process, node, and admin listener

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_build_info` | info | `version` | §12 | exported (M0-06) | Always 1. `version` is the SkyS3 release of the running binary. |
| `skys3_admin_requests_total` | counter | `endpoint` (`healthz`, `readyz`, `metrics`, `api`, `other`), `code` (HTTP status) | §12 | exported (M0-06; `api` from M1-13) | Requests answered by the admin listener. `api` counts the admin API under `/v1/`. `code="401"` counts callers rejected for a missing or wrong token. |
| `skys3_disks_out_of_service` | gauge | none | §10.4 | exported (M1-13) | Disks an I/O error took out of service since the node started. Each stays out of service until the host restarts (design section 10.4). |
| `skys3_admission_refusals_total` | counter | `reason` (`bucket_budget`, `cluster_budget`, `disk_space`) | §7.6, §13 | exported (M1-17) | Writes answered `503 SlowDown` by admission control: a dirty-data budget was used up (design section 7.6), or a disk or the data directory had less than `disk_min_free_bytes` free (section 13). |
| `skys3_control_store_live` | gauge | none | §6.2 | exported (M1-13) | 1 once the control store has answered since the node started; 0 while the node serves its local copy of control state (design section 6.2). It does not return to 0 when the store stops answering later; `skys3_control_store_last_success_timestamp_seconds` shows that. |
| `skys3_control_store_last_success_timestamp_seconds` | gauge | none | §6.2, §6.10 | exported (M7-04) | When the node last read the control store successfully, as Unix time: the generation check it makes at least every `config_poll_interval_seconds`, or a sync of its copy. 0 if it has not since it started. `time()` minus this is how long the node has gone without the store, also for an outage that starts after startup. |
| `skys3_identity_synced_timestamp_seconds` | gauge | none | §6.2, §6.10 | exported (M7-04) | When the sync that produced the node's identity copy started, as Unix time; for the copy a node loads at startup, the sync that produced its stored copy. STS issues no new sessions once `time()` minus this exceeds `identity_max_staleness`. While the store answers, the node syncs at least every half of it. 0 before the first sync. |
| `skys3_identity_max_staleness_seconds` | gauge | none | §6.10 | exported (M7-04) | The configured `identity_max_staleness` (`[identity] identity_max_staleness_hours`), so that alerts compare the identity copy's age with the node's own setting. |

### 3.2 Flush and loss exposure

Design sections 7.1 and 7.6. Together these measure the loss exposure (RPO)
under `ack_policy = "local"`. A `local` bucket with a backup target has them
too (design section 8.9, M4-09): there the remote target is its backup, and
they measure what the backup lacks.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_dirty_bytes` | gauge | `bucket` | §7.6 | exported (M1-16) | Bytes of committed versions not yet at the remote target (`dirty_bytes`), from the flushers of the bucket's shards on this node: the size of each dirty key's latest version, tombstones counting 0, keys held in conflict included. |
| `skys3_dirty_budget_bytes` | gauge | `bucket` | §7.6 | exported (M1-17) | This node's share of the bucket's dirty-data budget (`max_dirty_bytes`, design section 7.6): the budget times the share of the bucket's shards whose primary is on this node. New writes to the bucket get `503 SlowDown` while `skys3_dirty_bytes` is at or above it, or while the node's share of the cluster's budget is used up. |
| `skys3_oldest_dirty_age_seconds` | gauge | `bucket` | §7.6 | exported (M1-16) | Age of the oldest committed change not yet at the remote (`oldest_dirty_age`), conflicts included: the loss exposure if every member of a shard were lost now. A key's age runs from the first change after its last flush; a key found dirty at startup counts from its `Last-Modified` (a tombstone from the start). 0 when nothing is dirty. |
| `skys3_flush_lag_seconds` | gauge | `bucket` | §7.6 | exported (M1-16) | Age of the oldest change the flushers are still working on (`flush_lag_seconds`): `oldest_dirty_age` without keys held in conflict, which never drain without an operator. A lag that keeps growing means flushing does not keep up with ingest or the remote is failing. |
| `skys3_conflicted_keys` | gauge | `bucket` | §7.2 | exported (M1-16) | Keys held in conflict under the `hold` policy: a flush found an out-of-band remote write (design section 7.2). The admin API lists them, and an operator resolves them through it. |
| `skys3_flush_orphaned_uploads` | gauge | `bucket` | §7.4 | exported (M1-16b) | Remote multipart uploads that flushes of multipart objects left open, because their abort failed or their flusher stopped mid-flight, and that later multipart flushes will abort (design section 7.4). Uploads lost with a node's memory are not counted; the remote bucket's abort-incomplete-uploads lifecycle rule removes them. |
| `skys3_flush_streaming_overlap_ratio` | histogram | `bucket` | §7.3, §16.3 | exported (M4-02) | For each multipart object whose upload streamed to the remote target (design section 7.3), the fraction of its bytes the remote already held when the client completed the upload: parts sent before the completion commits, counted only if they are the parts the completion kept. Observed when the remote upload is completed, by the flusher that saw the completion; one that restarted in between observes nothing. Buckets are tenths from 0.1 to 1. The streaming-flush overlap of design section 16.3. |
| `skys3_flush_conflicts_total` | counter | `bucket` | §7.2 | exported (M1-16) | Flushes that found an out-of-band remote write and put their key in conflict. A restarted flusher finds a held conflict again and counts it again. |
| `skys3_flush_conflicts_overwritten_total` | counter | `bucket` | §7.2 | exported (M4-06) | Conflicts resolved by flushing the local version unconditionally over the out-of-band write: by the `overwrite` policy, or by an operator's `overwrite` resolution (design section 7.2). |
| `skys3_flush_conflicts_discarded_total` | counter | `bucket` | §7.2 | exported (M4-06) | Conflicts resolved by adopting the out-of-band write and dropping the local version, an acknowledged write: by the `discard_local` policy, or by an operator's `discard_local` resolution (design section 7.2). |
| `skys3_flushes_total` | counter | `bucket` | §7.1 | exported (M1-16) | Versions flushed: the remote accepted them, or a retry found them there by their write identity. |
| `skys3_flush_copies_total` | counter | `bucket` | §11 | exported (M4-07) | Copies flushed as a remote server-side `CopyObject` of their clean source, without uploading their bytes (design section 11). A copy whose answer was lost and that a retry found by its write identity counts under `skys3_flushes_total` only. |
| `skys3_flush_copy_fallbacks_total` | counter | `bucket` | §11 | exported (M4-07) | Server-side copies given up for a regular upload: the source changed or was deleted at the remote since the copy committed, or the target refused the copy (design section 11). A steady rate where sources are not rewritten means the target lacks support the probe found. |
| `skys3_flush_retries_total` | counter | `bucket` | §7.7 | exported (M1-16) | Flush attempts that failed (`5xx`, `503 SlowDown`, a lost response, another error) and are retried after a backoff. |
| `skys3_flush_concurrency` | gauge | `bucket` | §7.7 | exported (M4-10) | The requests this node's flushers may have in flight to the bucket's target: the adaptive window of design section 7.7, between `flush_min_concurrency_per_shard` and `flush_max_concurrency_per_shard` times the bucket's shards flushed here. It approaches the bandwidth-delay product of the link in requests, and drops on throttles and rising latency. Set once the target's capability probe is done. |
| `skys3_flush_inflight_bytes` | gauge | `bucket` | §7.7 | exported (M4-10) | Bytes this node's flushers hold in memory for requests to the bucket's target, bounded by `flush_max_inflight_bytes_per_target`: single PUT bodies and multipart or streamed parts, from reading them until their answer. |
| `skys3_flush_base_round_trip_seconds` | gauge | `bucket` | §7.7 | exported (M4-10) | The target's base round trip, which the window compares latency with: the smallest mean latency of a round of requests among the last 1,024 rounds. 0 until a round has ended. |
| `skys3_flush_throttles_total` | counter | `bucket` | §7.7 | exported (M4-10) | Flush requests the target answered with a throttle (any `503`, `429`, or a throttling error code), each of which may shrink `skys3_flush_concurrency`. Their keys are retried and also counted in `skys3_flush_retries_total`. |
| `skys3_flush_transport` | gauge | `bucket`, `transport` | §7.8 | planned (M6-07) | 1 for the transport the flushers of a bucket whose target is a peer SkyS3 cluster use now: QUIC, or S3 REST once `target_transport = "auto"` fell back, for example because UDP is blocked (design section 13). M6-07 decides the name and labels; the target status of the admin API reports the same. |

The `bucket` label is the bucket's name. A node exports the gauges and the
counters for every `write_back` bucket it knows, counting the shards open on
it, also while the bucket's target has not passed its capability probe. A
bucket's gauges disappear when it is deleted.

### 3.3 Replication

Design section 6.4. These report data with fewer than `replicas` copies,
after a member was removed from a shard. A shard counts on the node that
leads it, so summing the series of every node counts each shard once; the
cluster's oldest age is the maximum over the nodes. The metrics are defined
by M2-11 (`skys3_shard::replication::ReplicationMetrics`) and exported once
replication runs in the node binary.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_under_replicated_bytes` | gauge | none | §6.4 | defined (M2-11) | Bytes of the object versions held by the shards this node leads whose configuration has fewer members than `replicas` (`under_replicated_bytes`): old data and new writes alike have fewer copies then. 0 when no shard is short of members. |
| `skys3_oldest_under_replicated_age_seconds` | gauge | none | §6.4 | defined (M2-11) | How long the longest of those shards has had fewer members than `replicas` (`oldest_under_replicated_age`), counted from when this node removed the member or opened the shard short of members: after a restart it starts over. 0 when no shard is short of members. |

### 3.4 Read-through fill

Design section 9.2. A fill reads an evicted version from a `write_back`
bucket's remote target into the clean cache.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_fills_total` | counter | `bucket` | §9.2 | exported (M1-20) | Evicted versions filled from the remote target and made clean cache. A fill whose version a write replaced meanwhile serves its reads but is not counted. |
| `skys3_fill_conflicts_total` | counter | `bucket` | §7.2, §9.2 | exported (M1-20) | Fills that found the remote changed out of band: their precondition failed, or the object or version was gone. Counted whether the remote's version was adopted, a local write came first and the `ADOPT` was dropped, or the remote object was deleted and nothing could be adopted. |

The `bucket` label is the bucket's name, as for the flush metrics.

### 3.5 Clean cache

Design section 9.3. Each node keeps the clean payload of its shard replicas
as cache, and evicts it least recently used first.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_clean_cache_bytes` | gauge | none | §9.3 | exported (M1-21) | Bytes of clean payload this node keeps as cache, over every shard replica on it. |
| `skys3_clean_cache_limit_bytes` | gauge | none | §9.3 | exported (M1-21) | The most clean payload this node keeps: `cache_max_bytes_per_node`, or the room its disks have under `reserve_fraction` if that is less and every disk's space has been read. |
| `skys3_clean_cache_evictions_total` | counter | none | §9.3 | exported (M1-21) | Clean payloads this node evicted: least recently used ones over a bound, and copies beyond the bucket's `clean_copies`. |

### 3.6 Segment compaction

Design section 10.3. Compaction reclaims released log segments whose live
ratio is below `compaction_live_threshold`, after every checkpoint interval.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_compaction_segments_total` | counter | none | §10.3 | exported (M1-22) | Log segments compaction reclaimed. |
| `skys3_compaction_reclaimed_bytes_total` | counter | none | §10.3 | exported (M1-22) | Bytes of the log segments compaction reclaimed: the size of their files. |
| `skys3_compaction_copied_bytes_total` | counter | none | §10.3 | exported (M1-22) | Bytes of records compaction copied out of the segments it reclaimed into the active segments: dirty payload, metadata records, each shard's latest `CONFIG`, and clean payload it kept as cache. |
| `skys3_compaction_evictions_total` | counter | none | §10.3 | exported (M1-22) | Clean payloads compaction evicted instead of copying them. They are not counted in `skys3_clean_cache_evictions_total`. |
| `skys3_compaction_write_amplification` | gauge | none | §10.3 | exported (M1-22) | Compaction's write amplification since the node started: the bytes its logs wrote, over those written for anything but compaction's copies. 1 while compaction has copied nothing. |

### 3.7 Hot cache

Design section 9.2. Each node's gateway keeps whole objects it read from
another node's holder in memory, and serves later GETs of the same version
from them. `hot_cache_bytes_per_node` bounds the objects held and the fills
in progress together.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_hot_cache_bytes` | gauge | none | §9.2 | exported (M2-19) | Bytes of objects this node's hot cache holds. |
| `skys3_hot_cache_filling_bytes` | gauge | none | §9.2 | exported (M2-19) | Bytes this node's hot cache reserves for fills in progress: the sizes of the objects GETs are streaming from other nodes to keep. With `skys3_hot_cache_bytes`, at most `hot_cache_bytes_per_node`. |
| `skys3_hot_cache_hits_total` | counter | none | §9.2 | exported (M2-19) | GETs this node's gateway served from its hot cache. |
| `skys3_hot_cache_misses_total` | counter | none | §9.2 | exported (M2-19) | GETs this node's gateway looked up in its hot cache without finding the version their read plan names. |
| `skys3_hot_cache_evictions_total` | counter | none | §9.2 | exported (M2-19) | Objects the hot cache dropped as least recently used. Versions a later one replaced are not counted. |

### 3.8 Lifecycle

Design section 8.7. Each node runs a lifecycle pass every
`lifecycle_interval_seconds` over the `local` bucket shards it leads, and
counts what the passes committed. A pass that stops early on a shard, for
example because the primary stepped down, takes up the rest at the next
interval.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_lifecycle_expired_total` | counter | none | §8.7 | exported (M5-10) | Object versions that lifecycle rules expired on this node's shard primaries: `DELETE`s its passes committed. |
| `skys3_lifecycle_aborted_uploads_total` | counter | none | §8.7 | exported (M5-10) | Incomplete multipart uploads that lifecycle rules aborted on this node's shard primaries. |

### 3.9 Repair

Design sections 8.3, 8.6, and 16.3. Each shard primary's repairer finds the
fragments its coded objects lost, rebuilds them on other nodes, and relocates
them with `EC_RELOCATE`; once nothing is lost, it moves fragments the same
way (M5-09). A node's repairers share its metrics, over every
shard it leads; the gauges change at each repair pass.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_repaired_fragments_total` | counter | none | §8.6 | defined (M5-08) | Fragments this node's shard primaries rebuilt on another node and relocated with a committed `EC_RELOCATE`. |
| `skys3_moved_fragments_total` | counter | none | §8.3 | defined (M5-09) | Fragments this node's shard primaries moved to another node, to drain a node, respect a failure domain's cap, or balance the nodes, with a committed `EC_RELOCATE` (design section 8.3). |
| `skys3_repair_bytes_total` | counter | none | §8.6 | defined (M5-08) | Bytes this node's repairs and fragment moves read and wrote: what `repair_bytes_per_second_per_node` caps. |
| `skys3_repair_duration_seconds` | histogram | none | §8.6, §16.3 | defined (M5-08) | The repair time: for each fragment repaired, the time from when its shard primary found it lost to when its `EC_RELOCATE` committed. Buckets from 1 s, doubling, to about 3 days. |
| `skys3_unrepaired_fragments` | gauge | none | §8.6 | defined (M5-08) | Fragments the shards this node leads know lost and have not repaired yet. |
| `skys3_oldest_unrepaired_age_seconds` | gauge | none | §8.6 | defined (M5-08) | How long the oldest of those fragments has been known lost; 0 when there is none. |

### 3.10 Coordinator and placement

Design section 6.7. The coordinator judges each bucket's placement policy
before every placement round. Only the node that is coordinator reports a
judgement; every other node reports 0, and a tenure that ends clears its
node's gauges, so the maximum over the nodes is the cluster's value. The
metrics are defined by M7-04 (`skys3_coord::CoordinatorMetrics`) and exported
once the coordinator runs in the node binary.

| Name | Type | Labels | Design | Status | Description |
|---|---|---|---|---|---|
| `skys3_coordinator` | gauge | none | §6.7 | defined (M7-04) | 1 while this node holds the coordinator lease and serves its tenure, otherwise 0. The sum over the nodes is 1 in a healthy cluster; it is 0 while no node holds the lease, and briefly more than 1 while a node that lost the lease has not noticed. |
| `skys3_placement_unsatisfied_buckets` | gauge | none | §6.7 | defined (M7-04) | Buckets whose placement policy the cluster does not satisfy now, as the coordinator last judged them: shards short of members in separate failure domains, members sharing a domain, or too few eligible domains for `replicas`. The admin API's placement health lists them. 0 on every node but the coordinator. |
| `skys3_placement_short_shards` | gauge | none | §6.7 | defined (M7-04) | Shards of those buckets with fewer members in separate failure domains than `replicas`, counting only members on registered nodes that are not departing and carry the label `failure_domain` needs. 0 on every node but the coordinator. |

## 4. Alerts

The Prometheus alerting rules are in
[`deploy/prometheus/skys3-alerts.yml`](../deploy/prometheus/skys3-alerts.yml),
which `rule_files` in `prometheus.yml` loads as is. They live under `deploy/`
with the dashboard, rather than in `docs/`, because they are configuration an
operator installs, not prose; this section documents them. The rules assume
that every node is scraped by a job named `skys3`.

- **Severity.** `critical` pages: data is at risk now, or writes or sessions
  are refused. `warning` opens a ticket: exposure is growing, or something
  needs an operator, but nothing is refused yet.
- **Thresholds.** They are starting points for the default configuration
  ([configuration reference](skys3-config.md)). The dirty-age thresholds in
  particular should follow the loss exposure (RPO) a deployment accepts.
- **Runbooks.** Every rule's `runbook_url` links a section of the
  [runbooks](skys3-runbooks.md) by the anchor in the Runbook column. The
  anchors are stable: the runbooks keep a section for each, and their tests
  fail for an anchor that has none.
- **External series.** Two rules read series no SkyS3 node exports: `up`, which
  Prometheus records for every scrape target, and `node_timex_sync_status`,
  which node_exporter's timex collector exports (1 while the kernel clock is
  synchronized). `SkyS3ClockUnsynchronized` needs node_exporter on every host.

| Alert | Severity | For | Runbook | Fires when |
|---|---|---|---|---|
| `SkyS3DirtyDataOld` | warning | 10m | `dirty-data-age` | A bucket's `skys3_oldest_dirty_age_seconds` (the maximum over the nodes) is over 15 minutes. |
| `SkyS3DirtyDataVeryOld` | critical | 10m | `dirty-data-age` | The same age is over an hour. |
| `SkyS3FlushStalled` | warning | 10m | `flush-stalled` | A bucket's `skys3_flush_lag_seconds` is over 10 minutes while its flushes are being retried: the remote target is unreachable, refusing, or throttling. |
| `SkyS3DirtyBudgetNearlyFull` | warning | 5m | `dirty-budget` | A node's `skys3_dirty_bytes` for a bucket is over 80% of its `skys3_dirty_budget_bytes`. |
| `SkyS3WritesRefusedOverBudget` | critical | 5m | `dirty-budget` | A node refuses writes because a dirty-data budget is used up (`skys3_admission_refusals_total` with reason `bucket_budget` or `cluster_budget`). |
| `SkyS3ConflictsHeld` | warning | 5m | `held-conflicts` | A bucket holds keys in conflict (`skys3_conflicted_keys`), which need an operator. |
| `SkyS3ConflictsDiscarded` | warning | 0m | `discarded-conflicts` | A bucket dropped acknowledged writes for out-of-band remote writes in the last hour (`skys3_flush_conflicts_discarded_total`). |
| `SkyS3UnderReplicated` | warning | 5m | `under-replication` | A shard has had fewer members than `replicas` for over 15 minutes (`skys3_oldest_under_replicated_age_seconds`). |
| `SkyS3UnderReplicatedLong` | critical | 5m | `under-replication` | The same for over an hour. |
| `SkyS3PlacementPolicyUnsatisfied` | warning | 15m | `placement-policy` | The coordinator finds a bucket whose placement policy the cluster does not satisfy (`skys3_placement_unsatisfied_buckets`). |
| `SkyS3CoordinatorMissing` | warning | 5m | `coordinator` | Not exactly one node is coordinator (`skys3_coordinator`). |
| `SkyS3ControlStoreUnreachable` | warning | 5m | `control-store-unreachable` | A node serves its local copy (`skys3_control_store_live` is 0), or has not read the control store for over 5 minutes (`skys3_control_store_last_success_timestamp_seconds`). |
| `SkyS3IdentityCopyAging` | warning | 5m | `identity-staleness` | A node's identity copy is older than three quarters of `identity_max_staleness`. |
| `SkyS3IdentityCopyStale` | critical | 1m | `identity-staleness` | It is older than `identity_max_staleness`: STS issues no new sessions. |
| `SkyS3NodeDown` | critical | 2m | `node-down` | A node does not answer scrapes (`up`). |
| `SkyS3DiskOutOfService` | critical | 0m | `disk-out-of-service` | A node took a disk out of service (`skys3_disks_out_of_service`). |
| `SkyS3DiskSpaceLow` | critical | 1m | `disk-space` | A node refuses writes because a disk is below `disk_min_free_bytes` (`skys3_admission_refusals_total` with reason `disk_space`). |
| `SkyS3ClockUnsynchronized` | warning | 10m | `clock-drift` | A host's clock is not synchronized (`node_timex_sync_status`). |
| `SkyS3FragmentsUnrepaired` | warning | 15m | `fragment-repair` | A lost fragment has waited over an hour for repair (`skys3_oldest_unrepaired_age_seconds`). |

Neither CI nor the tests run `promtool check rules`, since `promtool` is not
part of the toolchain. The tests parse the file, check each rule's fields
against this table, and check that each expression reads only metrics listed
here.

## 5. Dashboard

[`deploy/grafana/skys3-dashboard.json`](../deploy/grafana/skys3-dashboard.json)
is a Grafana dashboard to import as is. It asks for a Prometheus data source,
and filters by node (`instance`) and bucket. Its rows:

- **Overview:** nodes up, coordinators, disks out of service, and unsatisfied
  placement policies.
- **Flush backlog and dirty age:** oldest dirty age, flush lag, dirty bytes
  and each node's use of its budget share, flush and copy rates, retries and
  write refusals, orphaned uploads, and streaming overlap.
- **Replication and leases:** under-replicated bytes and age, scrape health,
  and the coordinator. Leases have no metric of their own: a primary that
  loses its leases shows as a node that does not answer, then as
  under-replication once its shards fail over.
- **Conflicts:** keys held, and conflicts found by flushes and fills and
  resolved by overwrite or discard.
- **Placement:** unsatisfied buckets and short shards, and fragment moves.
- **Control store and identity:** whether each node is live, the time since
  it last read the store, and its identity copy's age against
  `identity_max_staleness`.
- **Erasure-coding repair and reclamation:** unrepaired fragments, repairs,
  repair time, compaction, and lifecycle expiration.
- **Caches and reads:** the clean cache against its limit, evictions and
  fills, and the hot cache.
- **Remote and peer transport:** the adaptive flush window, bytes in flight,
  the target's base round trip, and throttles (design section 7.7). These
  cover a peer SkyS3 cluster reached over S3 REST; the native peer
  transport's metric is planned with M6-07 (`skys3_flush_transport`).
- **Admin listener:** requests by endpoint and status code.

## 6. Failure matrix coverage

Each row of the design's failure matrix (section 13), with the metrics and
alerts that show it. An alert of "None" means the failure is not one to page
on, or cannot be shown yet; the notes say why.

| Failure | Metrics | Alerts | Notes |
|---|---|---|---|
| Backup slow or dead | `skys3_under_replicated_bytes`, `skys3_oldest_under_replicated_age_seconds`, `up` | `SkyS3UnderReplicated`, `SkyS3UnderReplicatedLong`, `SkyS3NodeDown` | The primary removes the member after `member_suspect_after`, and the shard counts as under-replicated until a replacement is promoted. A slow backup on a node that still answers scrapes shows only by its removal. |
| Primary dead | `up`, `skys3_under_replicated_bytes`, `skys3_oldest_under_replicated_age_seconds` | `SkyS3NodeDown`, `SkyS3UnderReplicated` | The new primary runs without the dead member until it is replaced. The failover itself (under 10 s) shows to clients as `503` answers; the gateway exports no request metrics yet. |
| Two of three members dead | `up`, `skys3_under_replicated_bytes`, `skys3_oldest_under_replicated_age_seconds` | `SkyS3NodeDown`, `SkyS3UnderReplicated`, `SkyS3UnderReplicatedLong` | The shard refuses client writes until a learner catches up; no metric counts those refusals. |
| Whole cluster loses power | `up`, `skys3_control_store_live`, `skys3_control_store_last_success_timestamp_seconds` | `SkyS3NodeDown`, `SkyS3ControlStoreUnreachable` | Every node is down, then replays its logs. A node that restarts while the control store is unreachable serves its local copy and reports `skys3_control_store_live` 0. |
| Every member of a shard permanently lost | `up`, `skys3_oldest_dirty_age_seconds`, `skys3_dirty_bytes`, `skys3_oldest_under_replicated_age_seconds` | `SkyS3NodeDown`, `SkyS3DirtyDataOld`, `SkyS3UnderReplicatedLong` | What was lost is bounded by the dirty bytes and dirty age before the loss. The lost keys are reported from the latest index snapshot (design section 6.9), not by a metric. |
| Node holding EC fragments lost | `up`, `skys3_unrepaired_fragments`, `skys3_oldest_unrepaired_age_seconds`, `skys3_repaired_fragments_total`, `skys3_repair_duration_seconds` | `SkyS3NodeDown`, `SkyS3FragmentsUnrepaired` | Degraded reads go on; the repair metrics show the rebuild's progress. |
| Node fails while the control store is unreachable | `up`, `skys3_control_store_last_success_timestamp_seconds`, `skys3_control_store_live` | `SkyS3NodeDown`, `SkyS3ControlStoreUnreachable` | Both alerts at once mean that the failed node's shards take no writes until the store returns. |
| Remote target unreachable | `skys3_flush_lag_seconds`, `skys3_flush_retries_total`, `skys3_oldest_dirty_age_seconds`, `skys3_dirty_bytes`, `skys3_dirty_budget_bytes`, `skys3_admission_refusals_total` | `SkyS3FlushStalled`, `SkyS3DirtyDataOld`, `SkyS3DirtyBudgetNearlyFull`, `SkyS3WritesRefusedOverBudget` | Writes go on until the budget is used up. Reads of evicted data fail: `skys3_fills_total` stops growing, and the gateway exports no error rate yet. |
| Link to a peer SkyS3 cluster drops or flaps | `skys3_oldest_dirty_age_seconds`, `skys3_flush_lag_seconds`, `skys3_dirty_bytes` | `SkyS3DirtyDataOld` | Objects stay dirty at the source until `APPLIED` arrives (plan M6-06), so a link that stays down shows as dirty age. A flap that resumes shows nothing, as intended. |
| UDP blocked between peer clusters | `skys3_flush_transport` | None | Not shown by a metric yet: the fallback to S3 REST is plan M6-07's, and M6-07 adds the planned metric. The target status of the admin API reports the transport, and flushing over S3 REST keeps the flush metrics healthy. |
| Control store unreachable | `skys3_control_store_last_success_timestamp_seconds`, `skys3_control_store_live`, `skys3_identity_synced_timestamp_seconds`, `skys3_identity_max_staleness_seconds` | `SkyS3ControlStoreUnreachable`, `SkyS3IdentityCopyAging`, `SkyS3IdentityCopyStale` | The data path continues; STS stops issuing sessions once the identity copy is stale. |
| High-latency link to the control store | `skys3_oldest_under_replicated_age_seconds` | None | Not a fault to page on: client requests are unaffected. Its cost, slower failover and member removal, shows as longer under-replication after a member loss. The round trip itself is not measured: the node binary runs the file control store, whose latency is a local disk's. The PR that runs the S3 or etcd backend in the node binary adds a request-duration metric. |
| Coordinator dies | `skys3_coordinator`, `up` | `SkyS3CoordinatorMissing`, `SkyS3NodeDown` | Another node takes the lease after about `coordinator_lease`; the alert fires only if none does. |
| Primary partitioned from its backups | `skys3_under_replicated_bytes`, `skys3_oldest_under_replicated_age_seconds` | `SkyS3UnderReplicated` | A backup takes over and removes the old primary, whose node may still answer scrapes. |
| Out-of-band write at the remote | `skys3_flush_conflicts_total`, `skys3_conflicted_keys`, `skys3_flush_conflicts_overwritten_total`, `skys3_flush_conflicts_discarded_total`, `skys3_fill_conflicts_total` | `SkyS3ConflictsHeld`, `SkyS3ConflictsDiscarded` | Fills adopt the remote version without an alert; flushes follow the bucket's conflict policy. |
| Disk full or sync failure | `skys3_admission_refusals_total`, `skys3_disks_out_of_service` | `SkyS3DiskSpaceLow`, `SkyS3DiskOutOfService` | Refusals with reason `disk_space` count only the writes that arrive while a disk is low. The free space itself is node_exporter's `node_filesystem_avail_bytes`. |
| Clock rate drift beyond `ρ` | `node_timex_sync_status` | `SkyS3ClockUnsynchronized` | A node cannot measure its clock's rate without another clock, so SkyS3 exports nothing for it. An unsynchronized clock is the usual cause of drift beyond the bound, and node_exporter reports it. |
