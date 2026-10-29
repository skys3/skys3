# SkyS3: Configuration Reference

**Implements:** the illustrative configuration of the [SkyS3 design](skys3-design.md), section 14.

Every SkyS3 node reads one TOML file. This reference lists every key with its type, default, and the rules checked when the file is loaded. The `skys3-config` crate implements it, and its tests load the design's section 14 example and check that every key there is listed here. A pull request that adds a key adds it here and to design section 14 (plan section 1.1).

## Contents

- [Loading and errors](#loading-and-errors)
- [`[cluster]`](#cluster)
- [`[node]`](#node)
- [`[gateway]`](#gateway)
- [`[control_store]`](#control_store)
- [`[transport]`](#transport)
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

## `[node]`

This node's identity and its directories (§10).

| Key | Type | Default | Rules |
|---|---|---|---|
| `node_id` | string | generated | A node ID: 1 to 63 bytes of lowercase ASCII letters, digits, and `-`, starting and ending with a letter or digit. When unset, the node generates one when it creates its data directory and keeps it. A node whose data directory holds another ID, or another cluster's data, refuses to start. |
| `data_dir` | path | `"/var/lib/skys3"` | Not empty. Holds the node's identity (`node.json`), its index (`index.redb`), and, by default, the file control store. One process uses it at a time. |
| `disks` | array of paths | `["<data_dir>/log"]` | 1 to 64 distinct, non-empty directories, one per disk, each holding that disk's log segments. A node keeps the disks it was created with: each shard replica's records stay on one disk, so starting with a disk added or missing is refused. |

## `[gateway]`

The S3 and STS listener (§3, §11).

| Key | Type | Default | Rules |
|---|---|---|---|
| `listen` | socket address | `"127.0.0.1:9000"` | |
| `tls_cert_file` | path | none | A PEM file with the certificate chain, leaf first. With `tls_key_file`, the gateway serves HTTPS (TLS 1.2 and 1.3, `rustls` with `aws-lc-rs`); without both, plain HTTP. Not empty, and set together with `tls_key_file`. |
| `tls_key_file` | path | none | A PEM file with the private key (PKCS#8, PKCS#1, or SEC1). Not empty, and set together with `tls_cert_file`. |

## `[control_store]`

Where the cluster's registers live (§6.1).

| Key | Type | Default | Rules |
|---|---|---|---|
| `backend` | `"etcd"`, `"s3"`, or `"file"` | `"etcd"` | `"s3"` covers AWS S3, R2, and other stores that pass the conditional-write probe. `"file"` keeps the registers in a local directory, for a single-node cluster; it refuses to serve a second node. The node binary runs only with `"file"` until replication (plan M2-07, M2-08): until then each node serves every shard alone, and a store several nodes can share would let two nodes serve one bucket apart. The etcd and S3 backends themselves exist (`skys3-control`). |
| `etcd_endpoints` | array of endpoint URLs | none | Required, and non-empty, for `backend = "etcd"`. Not allowed for the other backends. The client URLs of the etcd members, `http://` or `https://`, port 2379 if none is given; a request that goes unanswered moves to the next. The TLS and client-certificate keys for `https://` endpoints come with the node wiring. |
| `endpoint` | endpoint URL | none | Required for `backend = "s3"`. Not allowed for the other backends. |
| `bucket` | string | none | The control bucket. Required for `backend = "s3"`: visible ASCII without `/`. Not allowed for the other backends. |
| `directory` | path | `"<data_dir>/control"` | Where `backend = "file"` keeps the registers. Not empty. Not allowed for the other backends. The node claims an empty directory by writing `.owner.json`, and refuses one another node or data directory owns. Once a node has synced, it never creates or bootstraps this directory again: if it is missing (for example, an unmounted volume) or has lost its registers, the node runs from its local copy (design §6.2). |
| `prefix` | string | `"<cluster_id>/"` | The prefix of every register key. At most 512 bytes of visible ASCII, ending with `/` and not starting with it: S3's 1,024-byte key limit less the longest register key. |
| `allow_correlated_control_store` | boolean | `false` | Accept an S3 control store in the failure scope of a data target (see below). |
| `coordinator_lease_seconds` | integer | `10` | Positive. The holder renews every third of it (§6.7). |
| `config_poll_interval_seconds` | integer | `30` | Positive. How often a node polls `cluster.json` as a backstop to pushes (§6.2). |

**Independence from data targets.** With `backend = "s3"`, loading refuses a control-store `endpoint` in the same failure scope as a bucket's `backup_target` or `snapshot_target`, unless `allow_correlated_control_store = true` (§6.1). Two endpoints share a scope when they have the same host name or IP address, whatever their ports, or are AWS S3 endpoints in the same region. IP addresses are compared as addresses: every spelling of an IPv6 address, and an IPv4 address and its IPv4-mapped IPv6 form, are the same. Even with `allow_correlated_control_store = true`, a target in the control bucket whose prefix overlaps the control `prefix` (one starts with the other; no prefix covers the whole bucket) is refused, so that no target's credential reaches the registers. Write-back targets are bound when a bucket is attached, and the attach path applies the same checks.

**Credential scope.** The S3 control store addresses only keys under `prefix`. Its credential is the operator's to scope: `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on `arn:aws:s3:::<bucket>/<prefix>*`, and `s3:ListBucket` on the bucket with an `s3:prefix` condition starting with `prefix`, and nothing else (§6.1).

## `[transport]`

The intra-cluster transport: TCP with mutual TLS between the nodes of the cluster (design §12). Certificates come from the operator's PKI. The node certificate names the node with the URI subject alternative name `spiffe://<cluster_id>/node/<node-id>`, and the node's ID is the one its certificate names. The files are read when the node starts.

| Key | Type | Default | Rules |
|---|---|---|---|
| `listen` | socket address | `"0.0.0.0:7400"` | |
| `tls_cert_file` | path | none | The node's certificate chain in PEM, leaf first. Not empty. |
| `tls_key_file` | path | none | The leaf's private key in PEM (PKCS #8, PKCS #1, or SEC1). Not empty. |
| `tls_ca_file` | path | none | The CA certificates every peer's chain must lead to, in PEM. Not empty. |

The three files are set together or not at all. A node without them runs alone and does not open the transport.

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
| `group_commit_max_delay_us` | integer | `500` | How long a group commit waits for more records, from the first record's arrival. Records already queued join without a wait even after it. May be 0. Timers round it up to the next millisecond tick. |
| `group_commit_max_bytes` | integer | `4194304` (4 MiB) | Positive. |
| `index_checkpoint_interval_seconds` | integer | `10` | Positive (§10.2). |
| `compaction_live_threshold` | float | `0.5` | Greater than 0 and less than 1. Segments with a lower live ratio are reclaimed (§10.3). |
| `read_registration_ttl_seconds` | integer | `30` | Positive. |
| `read_registration_renew_interval_seconds` | integer | `10` | Positive, and less than `read_registration_ttl_seconds`. |
| `disk_min_free_bytes` | integer | `1073741824` (1 GiB) | May be 0, which turns the check off. Admission control (§13): while a log disk has less free space than this, writes that add data to the shards on it get `503 SlowDown`, and while the data directory's file system has less, every such write does. Deletes are still admitted. The margin keeps the disk from filling, since the first write error takes a disk out of service (§10.4). |

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
| `max_dirty_bytes` | integer | `2199023255552` (2 TiB) | Positive. The cluster's dirty-data budget (§7.6): once the dirty bytes of every `write_back` bucket together reach it, writes that add data get `503 SlowDown` until flushing drains them. Each node enforces a share of it (design §7.6). |
| `import_max_keys_per_second` | integer | `100000` | Positive. The most remote keys the namespace import of one bucket lists a second (§9.1), so an attach does not crowd out client writes on the shards' logs or exceed the target's request rate. |
| `target_region` | string | `"us-east-1"` | ASCII letters, numbers, and `-`. The region the flusher signs requests to `write_back` targets for (`"auto"` for Cloudflare R2). Credentials come from the `aws-config` default chain. |

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
| `import_parallel_streams` | integer | `32` | From 1 to 256. Key ranges a new namespace import lists in parallel, each with its own checkpoint; one rate limit, `import_max_keys_per_second`, covers them all (§9.1). |
| `ec_min_object_bytes` | integer | `4194304` (4 MiB) | Positive (§8.2). |
| `ec_stripe_data_bytes` | integer | `67108864` (64 MiB) | Positive. |
| `ec_after_seconds` | integer | `600` | May be 0. |
| `backup_ack` | `"local"` or `"write_through"` | `"local"` | A named `local` bucket with `"write_through"` needs a `backup_target` (§8.9). |
| `index_snapshot_interval_seconds` | integer | `3600` | Positive (§8.9). |
| `target_transport` | `"auto"`, `"native"`, or `"s3"` | `"auto"` | §7.8. |
| `max_dirty_bytes` | integer | `flush.max_dirty_bytes` | Positive. The bucket's dirty-data budget (§7.6). Only `write_back` buckets have dirty data. |

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
| `peer_frame_bytes` | integer | `262144` (256 KiB) | From 1 to 16777216 (16 MiB). The size of a `DATA` frame and the largest object in a `BATCH`; a destination stages each frame as one log record. |
| `peer_connect_timeout_ms` | integer | `3000` | Positive. |
| `peer_connections_per_shard` | integer | `64` | Positive. |
| `peer_max_inflight_bytes` | integer | `268435456` (256 MiB) | At least `peer_frame_bytes`. |
| `peer_staging_quota_bytes` | integer | `1099511627776` (1 TiB) | At least `peer_frame_bytes`. |
| `peer_staging_ttl_seconds` | integer | `86400` | Positive. |

### `[peering.peers.<cluster-id>]`

A peer cluster this node trusts, keyed by its cluster ID (§12). Peer connections present the node's own certificate, so a configured peer requires the `[transport]` certificate files. The table's cluster ID must not be this cluster's.

| Key | Type | Default | Rules |
|---|---|---|---|
| `ca_file` | path | required | Not empty. The peer's CA bundle in PEM; the peer's node certificates must lead to it and name the peer's cluster in their SPIFFE ID. |
| `buckets` | array of `{ source, destination }` tables | `[]` | The bucket pairs the peer may write as a source: `source` is the peer's bucket ID, which its write identities carry, and `destination` is this cluster's bucket name. No pair appears twice. Empty for a peer this node only sends to. |

## `[identity]`

Anonymous access, static credentials, STS sessions, and OIDC token validation (§11). Policies are JSON documents in a TOML string, in the policy language subset of design §11; a policy outside the subset is a parsing error.

| Key | Type | Default | Rules |
|---|---|---|---|
| `anonymous_access` | boolean | `false` | When false, unsigned requests are refused with `403 AccessDenied`. |
| `anonymous_policy` | string (JSON policy) | none | The policy that authorizes unsigned requests. Required when `anonymous_access` is true, and refused when it is false. |
| `sts_web_identity` | boolean | `true` | Whether the gateway serves STS `AssumeRoleWithWebIdentity` (a `POST` to `/` on its listener, §11). |
| `session_default_seconds` | integer | `3600` | From 900 to 43200 (the AWS STS limits), and at most `session_maximum_seconds`. |
| `session_maximum_seconds` | integer | `3600` | From 900 to 43200. |
| `identity_max_staleness_hours` | integer | `24` | Positive (§6.2). |
| `oidc_clock_skew_seconds` | integer | `60` | At most 300. The leeway for a token's `exp`, `nbf`, and `iat` (§11). |

Each `[identity.static_credentials.<name>]` table is one static access key, for bootstrap and service accounts. The name identifies the principal: 1 to 64 ASCII letters, digits, and `+=,.@_-`. Every key is required.

| Key | Type | Rules |
|---|---|---|
| `access_key_id` | string | 16 to 128 ASCII letters and digits, unique among static credentials. |
| `secret_access_key_file` | path | Not empty. A file holding the secret access key, read when the gateway starts: 32 to 128 visible ASCII characters, optionally followed by a line ending. |
| `policy` | string (JSON policy) | The policy that authorizes the credential's requests. |

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
