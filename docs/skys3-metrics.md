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

Status is **exported** once a merged PR registers the metric, and **planned**
while the design names it and the owning PR has not merged. The owning PR of a
planned metric decides its labels and records them here.

### 3.1 Process, node, and admin listener

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_build_info` | info | `version` | exported (M0-06) | Always 1. `version` is the SkyS3 release of the running binary. |
| `skys3_admin_requests_total` | counter | `endpoint` (`healthz`, `readyz`, `metrics`, `api`, `other`), `code` (HTTP status) | exported (M0-06; `api` from M1-13) | Requests answered by the admin listener. `api` counts the admin API under `/v1/`. `code="401"` counts callers rejected for a missing or wrong token. |
| `skys3_disks_out_of_service` | gauge | none | exported (M1-13) | Disks an I/O error took out of service since the node started. Each stays out of service until the host restarts (design section 10.4). |
| `skys3_control_store_live` | gauge | none | exported (M1-13) | 1 once the control store has answered since the node started; 0 while the node serves its local copy of control state (design section 6.2). |

### 3.2 Flush and loss exposure

Design sections 7.1 and 7.6. Together these measure the loss exposure (RPO)
under `ack_policy = "local"`.

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_dirty_bytes` | gauge | decided in M1-16 | planned (M1-16) | Bytes of committed data not yet flushed to the remote target (`dirty_bytes`). |
| `skys3_oldest_dirty_age_seconds` | gauge | decided in M1-16 | planned (M1-16) | Age of the oldest dirty entry (`oldest_dirty_age`). |
| `skys3_flush_lag_seconds` | gauge | decided in M1-16 | planned (M1-16) | How far flushing trails ingest (`flush_lag_seconds`). |

### 3.3 Replication

Design section 6.4. These report data with fewer than `replicas` copies.

| Name | Type | Labels | Status | Description |
|---|---|---|---|---|
| `skys3_under_replicated_bytes` | gauge | decided in M2-11 | planned (M2-11) | Bytes held by fewer than `replicas` copies (`under_replicated_bytes`). |
| `skys3_oldest_under_replicated_age_seconds` | gauge | decided in M2-11 | planned (M2-11) | Age of the oldest under-replicated data (`oldest_under_replicated_age`). |
