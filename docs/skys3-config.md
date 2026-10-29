# SkyS3: Configuration Reference

**Implements:** the illustrative configuration of the [SkyS3 design](skys3-design.md), section 14.

Every SkyS3 node reads one TOML file. This reference lists every key with its type, default, and the rules checked when the file is loaded. The `skys3-config` crate implements it, and its tests load the design's section 14 example and check that every key there is listed here. A pull request that adds a key adds it here and to design section 14 (plan section 1.1).

## Contents

- [Loading and errors](#loading-and-errors)
- [`[cluster]`](#cluster)
- [`[control_store]`](#control_store)
- [`[replication]`](#replication)
- [`[storage]`](#storage)
- [`[cache]`](#cache)
- [`[flush]`](#flush)
- [`[ec]`](#ec)
- [`[buckets.defaults]` and `[buckets.<name>]`](#buckets)
- [`[peering]`](#peering)
- [`[identity]`](#identity)
- [`[admin]`](#admin)
- [`[logging]`](#logging)

## Loading and errors

Loading runs in two stages.

1. **Parsing.** The file must be TOML, every value must have its key's type, and every key must be known. A misspelled key or section is an error, never silently ignored. Parsing stops at the first problem and names its key and line.
2. **Validation.** Every rule in this reference is checked, and every broken rule is reported at once, each at its dotted key path, such as `replication.primary_grace_ms` or `buckets.archive.backup_target`.

Only `[cluster]` and its `cluster_id` are required. Every other key has a default, except the control-store endpoints, which the chosen backend requires.

**Conventions.**

- Durations are integers whose key names the unit: `_us`, `_ms`, `_seconds`, or `_hours`. Durations must be at least 1 unless a row says otherwise.
- Sizes are integers in bytes.
- An *endpoint URL* is `https://host[:port]` or `http://host[:port]`, without a path, query, or user information (`user:password@`). The host is a DNS name, an IPv4 address, or an IPv6 address in brackets, and the port a decimal number from 1 to 65535 without leading zeros, the rules of node addresses (`NodeAddress` in `skys3-types`). Host names are case-insensitive: they are lowercased, and IPv6 addresses are rewritten in RFC 5952 form, so `https://S3.Example` loads as `https://s3.example`. A *target URL* is path-style: `https://host[:port]/bucket`, or `https://host[:port]/bucket/prefix` to confine SkyS3 to a key prefix. Credentials never appear in the file; SkyS3 takes them from `aws-config` providers (design §11).
- Enumerations are lowercase strings, such as `"write_back"`.
- *Positive* means at least 1.

## `[cluster]`

| Key | Type | Default | Rules |
|---|---|---|---|
| `cluster_id` | string | required | 1 to 24 bytes of lowercase ASCII letters, digits, and `-`, starting and ending with a letter or digit. The limit keeps every write identity within 96 bytes (§7.2). |
| `failure_domain` | `"node"`, `"rack"`, or `"zone"` | `"node"` | The level at which shard members and fragments are kept apart (§6.7). |

## `[control_store]`

Where the cluster's registers live (§6.1).

| Key | Type | Default | Rules |
|---|---|---|---|
| `backend` | `"etcd"` or `"s3"` | `"etcd"` | `"s3"` covers AWS S3, R2, and other stores that pass the conditional-write probe. |
| `etcd_endpoints` | array of endpoint URLs | none | Required, and non-empty, for `backend = "etcd"`. Not allowed for `"s3"`. |
| `endpoint` | endpoint URL | none | Required for `backend = "s3"`. Not allowed for `"etcd"`. |
| `bucket` | string | none | The control bucket. Required for `backend = "s3"`: visible ASCII without `/`. Not allowed for `"etcd"`. |
| `prefix` | string | `"<cluster_id>/"` | The prefix of every register key. Visible ASCII, ending with `/` and not starting with it. |
| `allow_correlated_control_store` | boolean | `false` | Accept an S3 control store in the failure scope of a data target (see below). |
| `coordinator_lease_seconds` | integer | `10` | Positive. The holder renews every third of it (§6.7). |
| `config_poll_interval_seconds` | integer | `30` | Positive. How often a node polls `cluster.json` as a backstop to pushes (§6.2). |

**Independence from data targets.** With `backend = "s3"`, loading refuses a control-store `endpoint` in the same failure scope as a bucket's `backup_target`, unless `allow_correlated_control_store = true` (§6.1). Two endpoints share a scope when they have the same host name or IP address, whatever their ports, or are AWS S3 endpoints in the same region. IP addresses are compared as addresses: every spelling of an IPv6 address, and an IPv4 address and its IPv4-mapped IPv6 form, are the same. Write-back targets are bound when a bucket is attached, and the attach path applies the same check.

## `[replication]`

Timeouts, failure detection, and leases (§5.2, §5.4, §6.3–§6.5). `ρ` is `assumed_clock_drift`.

| Key | Type | Default | Rules |
|---|---|---|---|
| `replica_ack_timeout_ms` | integer | `5000` | Positive. In `wait_through` mode, greater than `member_suspect_after_ms` + 1000 (a fixed allowance for the removal CAS, §5.2). |
| `replica_ack_timeout_mode` | `"wait_through"` or `"fail_fast"` | `"wait_through"` | Whether requests wait through a failing member's removal or fail with 503 as soon as it is late (§5.2). |
| `member_suspect_after_ms` | integer | `3000` | Positive, and greater than `lease_renew_interval_ms`, so healthy members are not suspected between heartbeats. |
| `lease_renew_interval_ms` | integer | `1000` | Positive, and less than `primary_lease_ms`. |
| `primary_lease_ms` | integer | `4000` | Positive. |
| `primary_grace_ms` | integer | `6000` | At least `primary_lease_ms × (1+ρ)/(1−ρ) + 500`, rounded up (§5.4). The 500 ms margin covers the time between a lease check and the read it admits. With the defaults the bound is 4581. |
| `assumed_clock_drift` | float | `0.01` | `ρ`, the bound on clock rate drift between nodes. At least 0 and less than 1. |
| `node_forget_after_hours` | integer | `24` | Positive. When the coordinator forgets an unreachable node, after re-homing its shards (§6.7). |

## `[storage]`

The node storage engine (§10) and read registrations (§8.7).

| Key | Type | Default | Rules |
|---|---|---|---|
| `inline_max_bytes` | integer | `131072` (128 KiB) | The largest payload stored inline in its record (§5.1). From 0 to 16777216 (16 MiB), the largest log record payload (§10.1). |
| `extent_bytes` | integer | `1048576` (1 MiB) | From 65536 (64 KiB) to 16777216 (16 MiB). The extent records large bodies are streamed in. The minimum keeps a 5 GiB object within the extents one `PUT` record references (§10.1). |
| `segment_bytes` | integer | `268435456` (256 MiB) | Greater than both `extent_bytes` and `inline_max_bytes`. |
| `group_commit_max_delay_us` | integer | `500` | How long a group commit waits for more records. May be 0. |
| `group_commit_max_bytes` | integer | `4194304` (4 MiB) | Positive. |
| `index_checkpoint_interval_seconds` | integer | `10` | Positive (§10.2). |
| `compaction_live_threshold` | float | `0.5` | Greater than 0 and less than 1. Segments with a lower live ratio are reclaimed (§10.3). |
| `read_registration_ttl_seconds` | integer | `30` | Positive. |
| `read_registration_renew_interval_seconds` | integer | `10` | Positive, and less than `read_registration_ttl_seconds`. |

## `[cache]`

| Key | Type | Default | Rules |
|---|---|---|---|
| `hot_cache_bytes_per_node` | integer | `68719476736` (64 GiB) | The node-local hot cache (§9.2). May be 0. |
| `cache_max_bytes_per_node` | integer | `1099511627776` (1 TiB) | The bound on clean cache per node (§9.3). May be 0. |
| `reserve_fraction` | float | `0.10` | At least 0 and less than 1. The share of each disk held back from caching, for learner catch-up and filesystem overhead (§9.3). |

## `[flush]`

Write-back flushing (§7). `ack_policy` and `flush_conflict_policy` are the defaults of every bucket; a `[buckets.<name>]` table may override them.

| Key | Type | Default | Rules |
|---|---|---|---|
| `ack_policy` | `"local"` or `"write_through"` | `"local"` | §7.5. |
| `flush_min_concurrency_per_shard` | integer | `4` | Positive. |
| `flush_max_concurrency_per_shard` | integer | `64` | At least `flush_min_concurrency_per_shard`. |
| `flush_max_inflight_bytes_per_target` | integer | `1073741824` (1 GiB) | Positive. |
| `streaming_flush_min_bytes` | integer | `67108864` (64 MiB) | Positive. |
| `flush_part_bytes` | integer | `67108864` (64 MiB) | From 5 MiB to 5 GiB, the S3 part-size limits. |
| `flush_conflict_policy` | `"hold"` or `"overwrite"` | `"hold"` | `"discard_local"` loses acknowledged writes, so only a `[buckets.<name>]` table may choose it (§7.2). |
| `max_dirty_bytes` | integer | `2199023255552` (2 TiB) | Positive. The cluster's dirty-data budget (§7.6). |

## `[ec]`

Erasure coding of `local` buckets (§8).

| Key | Type | Default | Rules |
|---|---|---|---|
| `parity_fragments` | integer | `2` | Positive. `m` in every geometry. |
| `max_data_fragments` | integer | `8` | Positive. The widest `k`. |
| `min_eligible_nodes` | integer | `5` | Greater than `parity_fragments`, since a stripe puts at most one fragment on a node. |
| `fragment_release_delay_seconds` | integer | `60` | May be 0 (§8.7). |
| `fragment_orphan_after_seconds` | integer | `3600` | Positive (§8.4). |
| `repair_bytes_per_second_per_node` | integer | `104857600` (100 MiB/s) | Positive (§8.6). |

## `[buckets.defaults]` and `[buckets.<name>]` {#buckets}

`[buckets.defaults]` applies to every bucket. A `[buckets.<name>]` table overrides it for the bucket with that S3 name, which must be a valid S3 bucket name; quote names that contain `.`, as in `[buckets."logs.example"]`. No bucket named `defaults` can have a table of its own. A key a table does not set comes from `[buckets.defaults]`, then from the built-in default.

| Key | Type | Default | Rules |
|---|---|---|---|
| `mode` | `"write_back"`, `"local"`, or `"read_only"` | `"write_back"` | Fixed at creation (§4.1). |
| `shards_per_bucket` | integer | `8` | From 1 to 256. Fixed at creation. |
| `replicas` | integer | `3` | From 1 to 255. |
| `min_write_replicas` | integer | `2` | From 1 to `replicas` (§6.4). |
| `clean_copies` | integer | `1` | From 0 to `replicas` (§9.3). |
| `import_parallel_streams` | integer | `32` | Positive (§9.1). |
| `ec_min_object_bytes` | integer | `4194304` (4 MiB) | Positive (§8.2). |
| `ec_stripe_data_bytes` | integer | `67108864` (64 MiB) | Positive. |
| `ec_after_seconds` | integer | `600` | May be 0. |
| `backup_ack` | `"local"` or `"write_through"` | `"local"` | A named `local` bucket with `"write_through"` needs a `backup_target` (§8.9). |
| `index_snapshot_interval_seconds` | integer | `3600` | Positive (§8.9). |
| `target_transport` | `"auto"`, `"native"`, or `"s3"` | `"auto"` | §7.8. |

Keys allowed only in a `[buckets.<name>]` table:

| Key | Type | Default | Rules |
|---|---|---|---|
| `ack_policy` | `"local"` or `"write_through"` | `flush.ack_policy` | §7.5. |
| `flush_conflict_policy` | `"hold"`, `"overwrite"`, or `"discard_local"` | `flush.flush_conflict_policy` | §7.2. |
| `backup_target` | target URL | none | Only for `mode = "local"` (§8.9). |
| `snapshot_target` | target URL | `backup_target` | Where index snapshots go (§8.9). |
| `peer_source` | cluster ID | none | The cluster this bucket receives native replication from (§7.8). A valid cluster ID other than this cluster's. |

Rules are reported at the table that sets the offending key, so a bad value in `[buckets.defaults]` is reported once, not again for every bucket that inherits it.

## `[peering]`

The native QUIC transport between SkyS3 clusters (§7.8).

| Key | Type | Default | Rules |
|---|---|---|---|
| `quic_listen` | socket address | `"0.0.0.0:7443"` | |
| `congestion_control` | `"cubic"`, `"new_reno"`, or `"bbr"` | `"cubic"` | BBR is experimental in Quinn. |
| `peer_frame_bytes` | integer | `262144` (256 KiB) | Positive. |
| `peer_connect_timeout_ms` | integer | `3000` | Positive. |
| `peer_connections_per_shard` | integer | `64` | Positive. |
| `peer_max_inflight_bytes` | integer | `268435456` (256 MiB) | At least `peer_frame_bytes`. |
| `peer_staging_quota_bytes` | integer | `1099511627776` (1 TiB) | At least `peer_frame_bytes`. |
| `peer_staging_ttl_seconds` | integer | `86400` | Positive. |

## `[identity]`

Anonymous access and STS sessions (§11).

| Key | Type | Default | Rules |
|---|---|---|---|
| `anonymous_access` | boolean | `false` | |
| `sts_web_identity` | boolean | `true` | |
| `session_default_seconds` | integer | `3600` | From 900 to 43200 (the AWS STS limits), and at most `session_maximum_seconds`. |
| `session_maximum_seconds` | integer | `3600` | From 900 to 43200. |
| `identity_max_staleness_hours` | integer | `24` | Positive (§6.2). |

## `[admin]`

The admin HTTP listener: metrics, health, and the admin API.

| Key | Type | Default | Rules |
|---|---|---|---|
| `listen` | socket address | `"127.0.0.1:7490"` | |
| `token_file` | path | none | A file holding the bearer token callers must present. Required when `listen` is not a loopback address. Not empty. |

## `[logging]`

| Key | Type | Default | Rules |
|---|---|---|---|
| `filter` | string | `"info"` | `tracing` `EnvFilter` directives, such as `"info,skys3_shard=debug"`. The binary parses them. |
| `format` | `"text"` or `"json"` | `"text"` | |
