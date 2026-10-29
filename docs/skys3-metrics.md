# SkyS3: Metrics Reference

**Implements:** the metrics rules of the [task and PR plan](skys3-tasks-plan.md) (section 1.1, M0-06) for the [SkyS3 design](skys3-design.md).

Every metric a SkyS3 node exports is listed here. A PR that adds a metric adds
its row in the same PR; a metric that the design names but no merged PR
exports yet is listed as planned, with the PR that owns it.

## Contents

- [1. Scraping](#1-scraping)
- [2. Naming conventions](#2-naming-conventions)
- [3. Metrics](#3-metrics)
  - [3.1 Process, node, and admin listener](#31-process-node-and-admin-listener)
  - [3.2 Flush and loss exposure](#32-flush-and-loss-exposure)
  - [3.3 Replication](#33-replication)
  - [3.4 Read-through fill](#34-read-through-fill)
  - [3.5 Clean cache](#35-clean-cache)

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
planned metric decides its labels and records them here.

### 3.1 Process, node, and admin listener

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_build_info` | info | `version` | exported (M0-06) | Always 1. `version` is the SkyS3 release of the running binary. |
| `skys3_admin_requests_total` | counter | `endpoint` (`healthz`, `readyz`, `metrics`, `api`, `other`), `code` (HTTP status) | exported (M0-06; `api` from M1-13) | Requests answered by the admin listener. `api` counts the admin API under `/v1/`. `code="401"` counts callers rejected for a missing or wrong token. |
| `skys3_disks_out_of_service` | gauge | none | exported (M1-13) | Disks an I/O error took out of service since the node started. Each stays out of service until the host restarts (design section 10.4). |
| `skys3_admission_refusals_total` | counter | `reason` (`bucket_budget`, `cluster_budget`, `disk_space`) | exported (M1-17) | Writes answered `503 SlowDown` by admission control: a dirty-data budget was used up (design section 7.6), or a disk or the data directory had less than `disk_min_free_bytes` free (section 13). |
| `skys3_control_store_live` | gauge | none | exported (M1-13) | 1 once the control store has answered since the node started; 0 while the node serves its local copy of control state (design section 6.2). |

### 3.2 Flush and loss exposure

Design sections 7.1 and 7.6. Together these measure the loss exposure (RPO)
under `ack_policy = "local"`.

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_dirty_bytes` | gauge | `bucket` | exported (M1-16) | Bytes of committed versions not yet at the remote target (`dirty_bytes`), from the flushers of the bucket's shards on this node: the size of each dirty key's latest version, tombstones counting 0, keys held in conflict included. |
| `skys3_dirty_budget_bytes` | gauge | `bucket` | exported (M1-17) | This node's share of the bucket's dirty-data budget (`max_dirty_bytes`, design section 7.6): the budget times the share of the bucket's shards whose primary is on this node. New writes to the bucket get `503 SlowDown` while `skys3_dirty_bytes` is at or above it, or while the node's share of the cluster's budget is used up. |
| `skys3_oldest_dirty_age_seconds` | gauge | `bucket` | exported (M1-16) | Age of the oldest committed change not yet at the remote (`oldest_dirty_age`), conflicts included: the loss exposure if every member of a shard were lost now. A key's age runs from the first change after its last flush; a key found dirty at startup counts from its `Last-Modified` (a tombstone from the start). 0 when nothing is dirty. |
| `skys3_flush_lag_seconds` | gauge | `bucket` | exported (M1-16) | Age of the oldest change the flushers are still working on (`flush_lag_seconds`): `oldest_dirty_age` without keys held in conflict, which never drain without an operator. A lag that keeps growing means flushing does not keep up with ingest or the remote is failing. |
| `skys3_conflicted_keys` | gauge | `bucket` | exported (M1-16) | Keys held in conflict under the `hold` policy: a flush found an out-of-band remote write (design section 7.2). The admin API lists them. |
| `skys3_flush_orphaned_uploads` | gauge | `bucket` | exported (M1-16b) | Remote multipart uploads that flushes of multipart objects left open, because their abort failed or their flusher stopped mid-flight, and that later multipart flushes will abort (design section 7.4). Uploads lost with a node's memory are not counted; the remote bucket's abort-incomplete-uploads lifecycle rule removes them. |
| `skys3_flush_conflicts_total` | counter | `bucket` | exported (M1-16) | Flushes that found an out-of-band remote write and put their key in conflict. A restarted flusher finds a held conflict again and counts it again. |
| `skys3_flushes_total` | counter | `bucket` | exported (M1-16) | Versions flushed: the remote accepted them, or a retry found them there by their write identity. |
| `skys3_flush_retries_total` | counter | `bucket` | exported (M1-16) | Flush attempts that failed (`5xx`, `503 SlowDown`, a lost response, another error) and are retried after a backoff. |

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

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_under_replicated_bytes` | gauge | none | defined (M2-11) | Bytes of the object versions held by the shards this node leads whose configuration has fewer members than `replicas` (`under_replicated_bytes`): old data and new writes alike have fewer copies then. 0 when no shard is short of members. |
| `skys3_oldest_under_replicated_age_seconds` | gauge | none | defined (M2-11) | How long the longest of those shards has had fewer members than `replicas` (`oldest_under_replicated_age`), counted from when this node removed the member or opened the shard short of members: after a restart it starts over. 0 when no shard is short of members. |

### 3.4 Read-through fill

Design section 9.2. A fill reads an evicted version from a `write_back`
bucket's remote target into the clean cache.

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_fills_total` | counter | `bucket` | exported (M1-20) | Evicted versions filled from the remote target and made clean cache. A fill whose version a write replaced meanwhile serves its reads but is not counted. |
| `skys3_fill_conflicts_total` | counter | `bucket` | exported (M1-20) | Fills that found the remote changed out of band: their precondition failed, or the object or version was gone. Counted whether the remote's version was adopted, a local write came first and the `ADOPT` was dropped, or the remote object was deleted and nothing could be adopted. |

The `bucket` label is the bucket's name, as for the flush metrics.

### 3.5 Clean cache

Design section 9.3. Each node keeps the clean payload of its shard replicas
as cache, and evicts it least recently used first.

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_clean_cache_bytes` | gauge | none | exported (M1-21) | Bytes of clean payload this node keeps as cache, over every shard replica on it. |
| `skys3_clean_cache_limit_bytes` | gauge | none | exported (M1-21) | The most clean payload this node keeps: `cache_max_bytes_per_node`, or the room its disks have under `reserve_fraction` if that is less and every disk's space has been read. |
| `skys3_clean_cache_evictions_total` | counter | none | exported (M1-21) | Clean payloads this node evicted: least recently used ones over a bound, and copies beyond the bucket's `clean_copies`. |
