# SkyS3: Operator Runbooks

**Implements:** plan M7-05 for the [SkyS3 design](skys3-design.md), sections 6.9 and 13.

One runbook for each failure of the design's failure matrix (section 13) and
each item of what still needs a human (section 6.9). The alerting rules of
the [metrics reference](skys3-metrics.md) (section 4) link these runbooks by
their headings' anchors, which are stable. Section 7 records the drill that
exercised each runbook, and section 8 the commands and endpoints a runbook
needs that do not exist yet.

The tests of the `skys3` crate (`crates/skys3/src/metrics/runbooks.rs`) fail
if an alert links a runbook that is not here, if section 2 misses a row of
design section 13 or an item of section 6.9, if a runbook names a metric the
metrics reference does not list, or if a drill names a test that does not
exist.

## Contents

- [1. Using these runbooks](#1-using-these-runbooks)
- [2. Index](#2-index)
- [3. Flush and the remote target](#3-flush-and-the-remote-target)
  - [Dirty data age](#dirty-data-age)
  - [Flush stalled](#flush-stalled)
  - [Dirty budget](#dirty-budget)
  - [Held conflicts](#held-conflicts)
  - [Discarded conflicts](#discarded-conflicts)
  - [Peer link](#peer-link)
- [4. Replication and placement](#4-replication-and-placement)
  - [Under-replication](#under-replication)
  - [Network partition](#network-partition)
  - [Placement policy](#placement-policy)
  - [Coordinator](#coordinator)
  - [Shard lost](#shard-lost)
  - [Provisioning](#provisioning)
- [5. Nodes, disks, and clocks](#5-nodes-disks-and-clocks)
  - [Node down](#node-down)
  - [Cluster power loss](#cluster-power-loss)
  - [Disk out of service](#disk-out-of-service)
  - [Disk space](#disk-space)
  - [Clock drift](#clock-drift)
  - [Fragment repair](#fragment-repair)
- [6. Control store](#6-control-store)
  - [Control store unreachable](#control-store-unreachable)
  - [Control store latency](#control-store-latency)
  - [Identity staleness](#identity-staleness)
  - [Control store loss](#control-store-loss)
  - [etcd majority loss](#etcd-majority-loss)
- [7. Drills](#7-drills)
- [8. Open points](#8-open-points)

## 1. Using these runbooks

Each runbook has the same parts: **Symptoms** (the alerts and series that
show the failure), **Impact**, **Diagnosis**, **Remediation**,
**Verification**, and **Do not** (mistakes that make it worse, and when to
escalate). Escalating means opening an issue with the node's log, the
answers of `GET /v1/health` and `GET /v1/buckets` from every node, and the
alert's series over the incident.

**The admin API.** Each node serves it on its admin listener (`[admin]
listen`, `127.0.0.1:7490` by default). Where `[admin] token_file` is set,
every call but `/healthz` and `/readyz` needs the token:

```sh
ADMIN=http://127.0.0.1:7490
AUTH="Authorization: Bearer $(cat /etc/skys3/admin-token)"   # the file [admin] token_file names
curl -fsS -H "$AUTH" $ADMIN/v1/health
curl -fsS -H "$AUTH" $ADMIN/v1/buckets/<bucket>
curl -fsS -H "$AUTH" $ADMIN/v1/buckets/<bucket>/conflicts
curl -fsS -H "$AUTH" -X POST $ADMIN/v1/buckets/<bucket>/conflicts/<policy>/<key>
curl -fsS -H "$AUTH" $ADMIN/metrics
curl -fsS $ADMIN/readyz
```

Design section 12 describes the answers. Each node answers for itself only:
a question about the cluster is asked of every node.

**The command line.** `skys3 --config <file> --check-config` validates a
configuration. `skys3 control export` and `skys3 control rebuild` rebuild a
lost control store ([Control store loss](#control-store-loss)). Nothing else
is an operator command yet.

**The log.** The node logs to standard error, at the level `[logging]
filter` sets. The runbooks quote the messages to search for.

**What the node binary runs today.** Every node serves its buckets alone,
over the file control store (`[control_store] backend = "file"`; the
binary refuses `etcd` and `s3`). Replication, the coordinator, encoding and
fragment repair, and the peer transport are built and run in the cluster
simulation, but the binary does not run them yet. Their metrics are listed
as defined in the metrics reference, and their alerts cannot fire. The
runbooks for them describe what to do once the binary runs them, and say
which of their steps cannot be taken yet.

## 2. Index

### 2.1 Failure matrix

Each row of design section 13, in its order, with the runbooks to follow.

| Failure | Runbooks |
|---|---|
| Backup slow or dead | [Under-replication](#under-replication), [Node down](#node-down) |
| Primary dead | [Node down](#node-down), [Under-replication](#under-replication) |
| Two of three members dead | [Under-replication](#under-replication), [Node down](#node-down) |
| Whole cluster loses power | [Cluster power loss](#cluster-power-loss) |
| Every member of a shard permanently lost | [Shard lost](#shard-lost) |
| Node holding EC fragments lost | [Fragment repair](#fragment-repair), [Node down](#node-down) |
| Node fails while the control store is unreachable | [Control store unreachable](#control-store-unreachable), [Node down](#node-down) |
| Remote target unreachable | [Flush stalled](#flush-stalled), [Dirty budget](#dirty-budget), [Dirty data age](#dirty-data-age) |
| Link to a peer SkyS3 cluster drops or flaps | [Peer link](#peer-link) |
| UDP blocked between peer clusters | [Peer link](#peer-link) |
| Control store unreachable | [Control store unreachable](#control-store-unreachable), [Identity staleness](#identity-staleness) |
| High-latency link to the control store | [Control store latency](#control-store-latency) |
| Coordinator dies | [Coordinator](#coordinator) |
| Primary partitioned from its backups | [Network partition](#network-partition) |
| Out-of-band write at the remote | [Held conflicts](#held-conflicts), [Discarded conflicts](#discarded-conflicts) |
| Disk full or sync failure | [Disk space](#disk-space), [Disk out of service](#disk-out-of-service) |
| Clock rate drift beyond `ρ` | [Clock drift](#clock-drift) |

### 2.2 What still needs a human

Each item of design section 6.9, in its order.

| Item | Runbooks |
|---|---|
| Provisioning hardware | [Provisioning](#provisioning) |
| Loss of the control store | [Control store loss](#control-store-loss) |
| Flush conflicts under the `hold` policy | [Held conflicts](#held-conflicts) |
| Loss of a majority of voters | [etcd majority loss](#etcd-majority-loss) |
| Every member of a shard lost | [Shard lost](#shard-lost) |

### 2.3 Alerts

The metrics reference (section 4) lists each alert with its runbook; the
alerting rules link the same anchors.

## 3. Flush and the remote target

A `write_back` bucket's remote target holds its objects for good, and a
`local` bucket's backup target holds a copy. Data not yet there, *dirty*
data, exists only on the shard's members: it is what a loss of every member
would lose (design section 7.6).

### Dirty data age

**Symptoms.** `SkyS3DirtyDataOld` (warning) or `SkyS3DirtyDataVeryOld`
(critical): `max by (bucket) (skys3_oldest_dirty_age_seconds)` is over 15
minutes, or an hour.

**Impact.** The age is the bucket's loss exposure: a change that old would
be lost if every member of its shard were lost now. Nothing is refused yet.

**Diagnosis.**

1. Tell a stalled flush from held conflicts. Held keys age but are not flush
   lag:
   - `max by (bucket) (skys3_flush_lag_seconds)` close to the dirty age, with
     `rate(skys3_flush_retries_total[10m]) > 0`: flushes fail. Follow
     [Flush stalled](#flush-stalled).
   - `sum by (bucket) (skys3_conflicted_keys) > 0` and a small flush lag:
     keys are held in conflict. Follow [Held conflicts](#held-conflicts).
2. Neither: flushing keeps up but falls behind ingest. Compare
   `rate(skys3_flushes_total[10m])` with the write rate, and look at the
   window: `skys3_flush_concurrency`, `skys3_flush_inflight_bytes`,
   `skys3_flush_base_round_trip_seconds`, and
   `rate(skys3_flush_throttles_total[10m])`. A window held at its floor by
   throttles means the target limits the request rate.
3. `GET /v1/buckets/<bucket>` on each node: `flush.oldest_dirty_age_seconds`
   finds the node whose shards hold the old data, and `flush.errors` the
   latest error of each of its shards.

**Remediation.** Fix the cause the diagnosis found. For a target that
throttles, ask the provider for a higher request rate, or spread the bucket's
keys over more prefixes. For a link that is too slow, raise
`flush_max_concurrency_per_shard` or `flush_max_inflight_bytes_per_target`
(`[flush]`, then restart the node) only while the target is not throttling.
Lower the thresholds of the alerts to the loss exposure the deployment
accepts.

**Verification.** `skys3_oldest_dirty_age_seconds` falls back to seconds,
and the alerts resolve.

**Do not.** Do not restart nodes to "unstick" flushing: the flushers retry
by themselves, and a restart only adds a capability probe before the next
flush. After a restart, a key found dirty counts its age from its
`Last-Modified`, so the gauge can drop without anything having flushed. Do
not delete keys to bring the age down.

### Flush stalled

**Symptoms.** `SkyS3FlushStalled`: a bucket's `skys3_flush_lag_seconds` is
over 10 minutes while `skys3_flush_retries_total` grows. Usually also
`SkyS3DirtyDataOld`, then `SkyS3DirtyBudgetNearlyFull` and
`SkyS3WritesRefusedOverBudget`.

**Impact.** Writes go on until the bucket's dirty budget is used up, then
get `503 SlowDown` ([Dirty budget](#dirty-budget)). Reads of cached data go
on; reads of evicted data fail while the target is unreachable. The loss
exposure grows with the dirty age.

**Diagnosis.**

1. `GET /v1/buckets/<bucket>` on a node that alerts. In `flush`:
   - `probe`: `running` with a `probe_error` means the target never passed
     its capability probe since the node started: nothing is flushed. The
     error names the cause (connection refused, `403 AccessDenied`, an
     unknown bucket).
   - `errors`: each shard's latest flush error, `<key>: <error>`. A
     timeout or connection error means the target is unreachable from this
     node; `403` means the credentials (from the `aws-config` default chain
     on the node) lost access; `503 SlowDown` means throttling.
   - `dirty`, `flushing`, and `dirty_bytes`: how much waits.
2. Whether it is one node or all: the alert's series by `instance`. One node
   alone points at its network or credentials.
3. The target's own status page or console, for an outage at the provider.

**Remediation.** Restore the path to the target: network, DNS, credentials,
or the provider. The flushers retry each key with a backoff of up to 30
seconds and need no action once the target answers. A probe that failed is
run again after its backoff.

**Verification.** `flush.errors` empties, `skys3_flush_lag_seconds` and
`skys3_oldest_dirty_age_seconds` return to 0 or seconds, and
`rate(skys3_flush_retries_total[5m])` returns to 0.

**Do not.** Do not detach the bucket: DeleteBucket refuses a `write_back`
bucket with unflushed entries. Do not raise the dirty budget as a first
step ([Dirty budget](#dirty-budget)).

### Dirty budget

**Symptoms.** `SkyS3DirtyBudgetNearlyFull` (warning): a node's
`skys3_dirty_bytes / skys3_dirty_budget_bytes` for a bucket is over 0.8.
`SkyS3WritesRefusedOverBudget` (critical):
`rate(skys3_admission_refusals_total{reason=~"bucket_budget|cluster_budget"}[5m]) > 0`.

**Impact.** Once a node's share of the budget is used up, new writes to the
bucket's shards on that node get `503 SlowDown`. Reads, deletes, and
flushing go on. A delete shrinks the dirty bytes (a tombstone counts 0).

**Diagnosis.**

1. `GET /v1/buckets/<bucket>`: `flush.dirty_bytes` against
   `flush.dirty_budget_bytes`, this node's share of `max_dirty_bytes`
   (design section 7.6). `reason="cluster_budget"` means the cluster's
   `[flush] max_dirty_bytes` is used up across all `write_back` buckets.
2. Why the dirty data does not drain: almost always a stalled flush. Follow
   [Flush stalled](#flush-stalled) or [Held conflicts](#held-conflicts)
   first.

**Remediation.** Make flushing drain. If writes must be admitted before the
target returns, raise the budget for the outage: `max_dirty_bytes` in the
bucket's `[buckets.<name>]` table, or in `[flush]` for the cluster's, on
every node, then restart the nodes one at a time. Check first that the
disks hold the extra dirty data with room to spare above
`disk_min_free_bytes`, and lower the budget again once the outage is over.

**Verification.** `skys3_dirty_budget_bytes` shows the new share,
refusals stop, and the dirty bytes drain once the target answers.

**Do not.** A larger budget is a larger loss exposure: every byte admitted
over it is a byte only the shard's members hold. Do not raise it without
an end date.

### Held conflicts

**Symptoms.** `SkyS3ConflictsHeld`: `sum by (bucket) (skys3_conflicted_keys) > 0`
for 5 minutes. `skys3_flush_conflicts_total` counts the conflicts flushes
found. Held keys raise `skys3_oldest_dirty_age_seconds` but not
`skys3_flush_lag_seconds`.

**Impact.** Under the `hold` policy, a flush that finds the remote object
changed by another writer since SkyS3 last wrote or read it keeps the key
dirty until an operator decides (design section 7.2). Held keys never
drain, and count against the dirty budget. Clients read the local version.

**Diagnosis.**

1. On each node, `GET /v1/buckets/<bucket>/conflicts`: each held key with
   its `shard`, the local `seq`, and the remote object's `remote_etag` and
   `remote_identity`. A conflict is held by the primary of the key's shard,
   so ask every node. `conflict_policy` says what the bucket does with new
   conflicts.
2. Who wrote the remote object: a `remote_identity` names the SkyS3
   cluster, bucket, and shard whose write it is (`<cluster>/<bucket-id>/<shard>/<epoch>.<seq>`);
   `null` means a writer outside SkyS3. Read the remote object (`HeadObject`
   with the target's own tools) and find the writer, for example in the
   provider's access logs.
3. The node's log has `the remote changed out of band; the key is in
   conflict` with the key, shard, and `seq` of each.

**Remediation.** Stop the other writer first, or the next flush conflicts
again. Then choose, per key, with `POST
/v1/buckets/<bucket>/conflicts/<policy>/<key>` (the key percent-encoded):

- `overwrite`: the local version replaces the other writer's at the remote.
  `skys3_flush_conflicts_overwritten_total` counts it.
- `discard_local`: the other writer's version is adopted, and the local
  version, an acknowledged write, is dropped. Follow
  [Discarded conflicts](#discarded-conflicts) afterwards. A `local`
  bucket's backup target never discards (`409`).
- `hold`: flush again under the same precondition, for a key whose other
  writer's object has been removed at the remote.

The call answers `200` once the key is dirty again, and `404` if no flusher
of this node holds it. The resolution lives in the flusher's memory until
the key's next flush succeeds: if the shard's primary changes first, the
new primary finds the conflict again and holds it again.

**Verification.** The conflicts list empties, `skys3_conflicted_keys`
returns to 0, and the remote holds the chosen version.

**Do not.** Do not set `flush_conflict_policy = "overwrite"` or
`"discard_local"` to make an alert go away: each silently loses one writer's
data from now on. Escalate if conflicts keep appearing with no other writer
in sight: the remote may not honor the preconditions the probe found
(`flush.unprotected`).

### Discarded conflicts

**Symptoms.** `SkyS3ConflictsDiscarded`:
`sum by (bucket) (increase(skys3_flush_conflicts_discarded_total[1h])) > 0`.

**Impact.** Acknowledged writes were dropped for out-of-band remote writes,
by the bucket's `discard_local` policy or an operator's `discard_local`
resolution. Clients now read the other writer's version. SkyS3 keeps no
copy of the dropped version.

**Diagnosis.**

1. Which keys: the node's log has `an acknowledged write was dropped for an
   out-of-band write` with the key, shard, and `seq` of each.
2. Why: `GET /v1/buckets/<bucket>/conflicts` shows `conflict_policy`. Under
   `discard_local`, the bucket's table chose it; otherwise an operator
   resolved a held conflict that way.

**Remediation.** Tell the owners of the dropped writes, with the keys and
times. If the policy should not discard, set `flush_conflict_policy` in the
bucket's `[buckets.<name>]` table to `hold` on every node and restart them
one at a time. Stop the other writer.

**Verification.** The counter stops growing; the alert resolves an hour
after the last discard.

**Do not.** Do not try to restore a dropped version from the node's disks:
its log records are reclaimed by compaction, and no tool reads them.

### Peer link

**Symptoms.** For a bucket whose target is a peer SkyS3 cluster (design
section 7.8): `SkyS3DirtyDataOld` with a growing
`skys3_oldest_dirty_age_seconds` and `skys3_flush_lag_seconds` while the
link is down. A link that flaps and resumes shows nothing, by design.
`skys3_flush_transport`, which shows a fallback from QUIC to S3 REST, is
planned (plan M6-07).

**Impact.** Objects stay dirty at the source until the destination applies
them (`APPLIED`), and transfers resume from the last durable range. With
UDP blocked, `target_transport = "auto"` is meant to fall back to S3 REST.

**Diagnosis.** As in [Flush stalled](#flush-stalled): the bucket's
`flush.errors` name the transport's errors. Check UDP reachability of the
peer's `quic_listen` address, and the peer's trust bundle and bucket pairs
(`[peering.peers.<cluster-id>]`).

**Remediation.** Restore the path between the clusters. The flushers resume
by themselves.

**Verification.** The dirty age at the source returns to seconds.

**Do not.** Do not point a bucket at a SkyS3 cluster as a plain S3 target:
a SkyS3 gateway refuses the write-identity metadata every S3 REST flush
sends (`400 InvalidArgument`, section 8). The node binary does not run the
peer transport yet.

## 4. Replication and placement

Each shard has `replicas` members; every member acknowledges every commit
(design section 5). Failover and replacement are automatic. The node binary
does not run replication or the coordinator yet: these runbooks apply once
it does, and their metrics are defined but not exported until then.

### Under-replication

**Symptoms.** `SkyS3UnderReplicated` (warning) or
`SkyS3UnderReplicatedLong` (critical):
`max(skys3_oldest_under_replicated_age_seconds)` over 15 minutes, or an hour.
`skys3_under_replicated_bytes` is the data with fewer copies, summed over
the nodes. Usually after `SkyS3NodeDown`.

**Impact.** After a member is removed (a backup slow or dead, or a primary
that a member took over from), every record of the shard has one copy
fewer until a learner is added and backfilled; one more failure in that
window leaves one copy (design section 6.4). With fewer acknowledging
members than `min_write_replicas`, as after two of three members died, the
shard refuses client writes with `503 SlowDown` until a learner joins the
acknowledgement set, and serves reads.

**Diagnosis.**

1. Which node leads the short shards: the series of
   `skys3_under_replicated_bytes` by `instance`.
2. Whether replacement can happen: the coordinator adds learners only on
   eligible nodes in failure domains the shard does not use. On the
   coordinator, `GET /v1/health` reports `placement` (not served yet,
   section 8); `skys3_placement_short_shards` counts shards short of members
   in separate domains. A non-zero count for long means no eligible node:
   follow [Placement policy](#placement-policy).
3. Whether a coordinator runs: `sum(skys3_coordinator)` is 1. Otherwise
   follow [Coordinator](#coordinator).
4. Whether the control store answers: replacement and promotion are CASes
   on it. Otherwise follow
   [Control store unreachable](#control-store-unreachable).
5. The shard's register shows its members and learners: read
   `shards/<bucket-id>/<shard>.json` under the cluster's prefix with the
   store's own tools.

**Remediation.** None while replacement progresses: the coordinator adds a
learner, the learner backfills (under-replicated data first), and the
primary promotes it. If the cluster has no eligible node, add one
([Provisioning](#provisioning)) or bring the lost node back: a returning
node rejoins as a learner.

**Verification.** `skys3_under_replicated_bytes` and
`skys3_oldest_under_replicated_age_seconds` return to 0 on every node.

**Do not.** Do not restart or take down another member of a short shard
during the window. Do not lower `replicas` to make the alert go away.
Escalate a shard that stays short with an eligible node available.

### Network partition

**Symptoms.** A primary cut off from its backups: `SkyS3UnderReplicated`
once a backup takes over and the old primary is left out, while the old
primary's node may still answer scrapes. Clients of the old primary get
`503` answers until their gateways learn the new configuration.

**Impact.** The old primary loses its leases and stops serving; a backup
takes over by a CAS after `primary_grace` (design section 6.5). No
acknowledged write is lost. The old primary rejoins only as a learner.

**Diagnosis.** The node's log, and each side's reachability of the other's
transport address (`[transport] listen`). `SkyS3NodeDown` absent while
under-replication rises points at the network rather than a node.

**Remediation.** Repair the network. The cut-off node's shards re-admit it
as a learner; nothing else is needed.

**Verification.** As for [Under-replication](#under-replication).

**Do not.** Do not restart the old primary's node to "make it give up": it
already stopped serving, and a restart only adds a recovery.

### Placement policy

**Symptoms.** `SkyS3PlacementPolicyUnsatisfied`:
`max(skys3_placement_unsatisfied_buckets) > 0` for 15 minutes, with
`skys3_placement_short_shards` counting the shards short of members in
separate failure domains.

**Impact.** Shards of those buckets run with fewer members in separate
domains than `replicas`. The coordinator never co-locates members to make
up the difference (design section 6.7), so the shards stay exposed until
capacity returns.

**Diagnosis.** On the coordinator, `GET /v1/health` `placement`:
`failure_domain`, `eligible_nodes`, `domains`, `unlabeled` (nodes without
the label the level needs, which never receive members), and per bucket
`placeable`, `short`, and `co_located`. Not served yet (section 8): the
node binary does not run the coordinator. `placeable` false means the
cluster has fewer eligible domains than `replicas`.

**Remediation.** Add capacity in a missing domain, or label unlabeled
nodes ([Provisioning](#provisioning)). A shard with two members in one
domain, which relabeling a node can cause, is fixed by replacement once
another domain has an eligible node.

**Verification.** `skys3_placement_unsatisfied_buckets` returns to 0.

**Do not.** Do not lower `failure_domain` to make the alert go away; it
would let placement put members in one domain from then on.

### Coordinator

**Symptoms.** `SkyS3CoordinatorMissing`: `sum(skys3_coordinator) != 1` for 5
minutes.

**Impact.** None for clients. With no coordinator, placement work waits:
replacing lost members, rebalancing, shards of new buckets. With more than
one, all but one lose their CASes.

**Diagnosis.** 0 for longer than `coordinator_lease_seconds` means no node
takes the lease: check that the control store answers (every node's
`skys3_control_store_last_success_timestamp_seconds`). More than 1 for long
means a node believes it holds a lease it lost: its clock or process was
paused; find it by the series of `skys3_coordinator` by `instance`, and read
its log.

**Remediation.** Restore the control store. Restart a node that keeps
reporting itself coordinator after another has taken over.

**Verification.** `sum(skys3_coordinator)` is 1.

**Do not.** Do not edit `coordinator.lease` in the control store.

### Shard lost

**Symptoms.** Every member of a shard is gone for good: each member's node
fired `SkyS3NodeDown` and its disks are lost. Before the loss,
`skys3_dirty_bytes` and `skys3_oldest_dirty_age_seconds` of the shard's
bucket bounded what was at risk.

**Impact.** Data that had not reached its durable home is lost: dirty data
of a `write_back` bucket, data a `local` bucket's backup target lacked, and
every replicated object of a `local` bucket without a backup. The shard's
index is lost too. Clean data of a `write_back` bucket is refilled from the
remote. Coded objects survive in their fragments (design section 6.9).

**Diagnosis.**

1. Confirm the loss: the members' disks cannot be brought back. A node
   whose disks return is not lost; start it.
2. Whether the bucket has snapshots: only a bucket with a snapshot target
   writes them. That is its `[buckets.<name>] snapshot_target`, or, for a
   `local` bucket, its `backup_target` by default. A `write_back` bucket
   whose table names no `snapshot_target`, and a `local` bucket with
   neither key, write none (design section 8.9).
3. With a snapshot: the latest index snapshot of the shard, under
   `<prefix>.skys3-snapshots/<bucket ID>/<shard>/` in the snapshot target,
   gives the keys whose data only the members held, and the window after it
   in which other keys may have been lost. The lost-key report and the
   restore drill that build this exist as library functions
   (`skys3_flush::snapshot::{latest, lost_keys, drill}`) without a command
   (section 8). Listing the snapshot prefix with the target's own tools
   shows the snapshot's time, which starts the window.
4. Without a snapshot there is no list of keys and no start of the window
   from SkyS3:
   - a `write_back` bucket lost what was dirty: writes and deletes not yet
     at the remote. For writes, the last scraped
     `skys3_oldest_dirty_age_seconds` of the shard's primary before the
     loss bounds the window: comparing a listing of the remote target with
     the writers' own records of the writes in that window finds the keys.
     This holds while the primary did not restart after the writes; after a
     restart, a write found dirty is dated by its `Last-Modified`, which
     still bounds it. Deletes have no such bound: a tombstone counts 0 in
     `skys3_dirty_bytes`, and one found dirty at a restart is dated from the
     restart, since it has no `Last-Modified`. An acknowledged delete the
     remote never received leaves the remote's object looking current. So
     audit the writers' delete records back to the last time the shard was
     known fully flushed (`skys3_oldest_dirty_age_seconds` 0 on its
     primary), or to the bucket's creation if that is unknown, and delete
     at the remote what they deleted;
   - a `local` bucket without a backup lost every replicated object of the
     shard, whenever written. Only the writers' records name them. Its
     coded objects' fragments survive, and re-indexing them is the restore
     drill.

**Remediation.** Tell the bucket's owners which keys, or which window, were
lost. For a `write_back` bucket, the cluster serves what the remote holds
once the shard has members again. For a `local` bucket, the restore drill
re-indexes coded objects from their fragment headers; it has no command
yet. Give the bucket a `snapshot_target` (and a `local` bucket a
`backup_target`) so that a later loss can be reported.

**Verification.** The shard serves again, and the lost keys are accounted
for.

**Do not.** Do not start a node with a data directory copied from another
node: the data directory's instance ID names one node, and a copy can serve
records no other member knows.

### Provisioning

**Symptoms.** None: adding or retiring a machine is planned work.

**Impact.** Adding a node: rebalancing moves shards and primaries to it.
Retiring one: its shards are re-homed before it is forgotten.

**Procedure.** To add a node, give it a configuration with the cluster's
`cluster_id` and control store, and a node certificate from the cluster's
PKI (`[transport]`), and start it. It registers itself; every membership
decision after that is automatic (design section 6.7). To retire a node,
stop it: the coordinator marks it `departing` after `node_forget_after_hours`,
re-homes its shards, and forgets it. No command marks a node `departing`
sooner, and node labels (`zone`, `rack`) have no configuration key yet
(section 8). The node binary does not run several nodes yet.

**Verification.** For an added node, its share of shards; for a retired
one, `SkyS3UnderReplicated` resolves and no shard register names it.

**Do not.** Do not retire several nodes, or several failure domains, at
once: each takes a member from every shard it held. Retire one, and wait
until under-replication clears before the next.

## 5. Nodes, disks, and clocks

### Node down

**Symptoms.** `SkyS3NodeDown`: `up{job="skys3"} == 0` for 2 minutes; the
node's `/healthz` does not answer.

**Impact.** On a node alone, its buckets are unavailable. In a cluster, the
shards it led fail over within about `primary_grace` plus one CAS and
reconciliation (under 10 seconds by default), and the shards it was a
member of lose that member ([Under-replication](#under-replication)).
Fragments it held are rebuilt elsewhere ([Fragment repair](#fragment-repair)).

**Diagnosis.**

1. Whether the process runs, and its exit: the service manager's status
   and the node's log. A node that cannot start logs `the node cannot
   start` with the reason (a disk fenced, a data directory in use, a
   configuration error).
2. `skys3 --config <file> --check-config` for a configuration error.
3. Whether only the admin listener is unreachable: the gateway still
   answers S3 requests.

**Remediation.** Start the node. It recovers its logs, replays its index
from its checkpoints, and serves; a node that comes back while the control
store is unreachable serves from its copy ([Cluster power loss](#cluster-power-loss)).
For a node that cannot start, follow the runbook of the reason it logs.

**Verification.** `/readyz` answers `200`; `GET /v1/health` has `ready`
true, every disk `in_service`, and the expected `shards_open`;
`skys3_disks_out_of_service` is 0.

**Do not.** Do not wipe the data directory to make a node start: that loses
its copies of every shard it held, and on a node alone, every dirty write.

### Cluster power loss

**Symptoms.** `SkyS3NodeDown` for every node at once, then
`SkyS3ControlStoreUnreachable` from nodes that return before the control
store.

**Impact.** Nothing acknowledged is lost: every write was durable before it
was acknowledged. Nodes resume from each shard's latest `CONFIG` record even
if the control store is unreachable; shards whose membership changed while
a node was down stay fenced on it until it reads the current register.

**Diagnosis.** As in [Node down](#node-down), for each node. A node that
started without its control store has `control_store.live` false in
`GET /v1/health`, `skys3_control_store_live` 0, and logs `cannot open the
control store; running from the local copy`.

**Remediation.** Start every node; their order does not matter. Restore the
control store if it is down; each node opens it by itself once it can,
within `config_poll_interval_seconds`, and logs `opened the control
store`.

**Verification.** Every node ready, `skys3_control_store_live` 1 on each,
and bucket creation works.

**Do not.** Do not rebuild the control store because nodes started without
it ([Control store loss](#control-store-loss) is for a store that is lost,
not late).

### Disk out of service

**Symptoms.** `SkyS3DiskOutOfService`: `skys3_disks_out_of_service > 0`.
`/readyz` answers `503`, and `GET /v1/health` has `ready` false, `not_ready`
`["storage"]`, and the disk with `in_service` false and its `error`.

**Impact.** A write or sync on the disk failed. After a failed sync the
page cache can show bytes the disk lost, so the node stops using the disk
for the rest of the process (design section 10.4). Writes to the shards on
it fail; in a cluster their members on this node are removed and replaced.

**Diagnosis.**

1. Which disk: `GET /v1/health` `disks`, and the log's `a disk was taken
   out of service` with the disk's label and the error.
2. The device: the kernel log (`dmesg`), the device's SMART status, whether
   its file system is mounted read-only or missing.
3. Whether the fence was written: the node writes `out-of-service.json`
   into the disk's directory with the host's boot ID. If it could not
   (a missing directory, a dead device), it logs `cannot fence the disk; it
   stays out of service only until the node exits`.

**Remediation.**

1. Stop the node.
2. Fix or replace the device and bring its file system back at the same
   path. A node keeps the disks it was created with: a new, empty disk in
   its place is refused, so a replaced disk means a new node in a cluster
   (its old shards are replaced elsewhere), and lost data on a node alone
   ([Shard lost](#shard-lost)).
3. A fenced disk is used again after the host restarts. To use it without
   a restart, once the device is checked, remove `out-of-service.json` from
   the disk's directory. Until then the node refuses to start, naming the
   disk and the file.
4. Start the node.

**Verification.** `/readyz` `200`, every disk `in_service`,
`skys3_disks_out_of_service` 0, and the bucket's objects read back.

**Do not.** Do not remove the fence before checking the device: the fence
exists because the page cache may show data the disk does not hold. Do not
delete or move segment files.

### Disk space

**Symptoms.** `SkyS3DiskSpaceLow`:
`rate(skys3_admission_refusals_total{reason="disk_space"}[5m]) > 0`. The log
has `low on disk space; writes that add data get 503 SlowDown` with the
place (a disk's label, or the data directory) and its free bytes.

**Impact.** Writes that add data to shards on the low disk get `503
SlowDown`; all of them, if the data directory is low. Reads and deletes are
admitted. The margin, `[storage] disk_min_free_bytes`, keeps the disk from
filling, since a write error would take it out of service.

**Diagnosis.** The free space of each disk directory and of the data
directory (`GET /v1/health` `disks[].path`, and `[node] data_dir`), with
`df` or node_exporter's `node_filesystem_avail_bytes`. What fills it:
SkyS3's segments (dirty data that does not drain, see
[Dirty budget](#dirty-budget); clean cache, bounded by `[cache]`), or
something else on the same file system.

**Remediation.** Free space on the file system: remove what does not belong
to SkyS3, let dirty data drain, or lower `cache_max_bytes_per_node` and
restart. The node reads the free space every second and admits writes
again by itself, logging `disk space recovered; writes are admitted`.

**Verification.** The refusals stop, and a write succeeds.

**Do not.** Do not set `disk_min_free_bytes` to 0 to admit writes: a full
disk takes itself out of service. Do not delete segment files or the index.

### Clock drift

**Symptoms.** `SkyS3ClockUnsynchronized`: `node_timex_sync_status == 0` for
10 minutes on a host (node_exporter's timex collector).

**Impact.** A clock that is not synchronized may drift faster than
`assumed_clock_drift` (`ρ`), which the leases assume. Read linearizability
is then at risk: a primary could serve a read after another member took
over. Write safety does not depend on clocks (design sections 5.4 and 13).

**Diagnosis.** The host's time synchronization service (for example
`chronyc tracking` or `timedatectl`): its sources, and the measured
frequency error. SkyS3 cannot measure its own clock's rate.

**Remediation.** Restore synchronization. A host whose clock cannot be kept
within `ρ` should not run a node; raising `assumed_clock_drift` (with
`primary_grace_ms` at least `primary_lease_ms × (1+ρ)/(1−ρ) + 500`) is the
alternative, and lengthens failover.

**Verification.** `node_timex_sync_status` is 1.

**Do not.** Do not step a clock back by hand on a running node: the
identity copy's age counts a backward step as stale, and leases measure
time on monotonic clocks that a step does not fix.

### Fragment repair

**Symptoms.** `SkyS3FragmentsUnrepaired`:
`max(skys3_oldest_unrepaired_age_seconds) > 3600` for 15 minutes.
`skys3_unrepaired_fragments` counts the fragments known lost, and
`skys3_repaired_fragments_total` and `skys3_repair_duration_seconds` show
the rebuild's progress.

**Impact.** Coded objects stay readable: degraded reads decode from any `k`
fragments. Each further loss in a stripe brings it closer to unreadable.

**Diagnosis.** Which node is lost or silent: `SkyS3NodeDown`. A node that
does not answer fragment checks for `fragment_repair_after_seconds` has its
fragments rebuilt elsewhere. Repair is paced by
`repair_bytes_per_second_per_node`; `rate(skys3_repair_bytes_total[10m])`
near it means repair is busy, not stuck. Repair needs eligible nodes: at
most `m` fragments of a stripe per domain and one per node.

**Remediation.** Bring the node back if it can return; otherwise let repair
run. Raise `repair_bytes_per_second_per_node` (`[ec]`, then restart) if the
rebuild is too slow and the nodes have I/O to spare. Add nodes if too few
are eligible ([Provisioning](#provisioning)).

**Verification.** `skys3_unrepaired_fragments` returns to 0.

**Do not.** Do not take another fragment holder down while stripes are
short. The node binary does not run encoding or repair yet.

## 6. Control store

The control store holds the cluster's registers. No object request reads or
writes it; membership changes, bucket creation and deletion, and identity
updates do (design section 6.1).

### Control store unreachable

**Symptoms.** `SkyS3ControlStoreUnreachable`: a node has
`skys3_control_store_live == 0` (it started without the store and serves
its copy), or `time() - skys3_control_store_last_success_timestamp_seconds`
over 5 minutes (it has not read the store since). The log has `cannot read
the control store's generation` or `cannot open the control store; serving
the local copy`.

**Impact.** The data path goes on from the node's copy: reads, writes,
flushing. CreateBucket and DeleteBucket fail: `503` from a node that runs
from its copy, `500 InternalError` ("The cluster's control store failed")
from a node whose store failed a write. No membership change can happen:
a node that fails now takes the shards it belongs to out of service for
writes, and for reads once their leases lapse, until the store returns
(design section 6.10). STS stops issuing sessions once the identity copy
is older than `identity_max_staleness` ([Identity staleness](#identity-staleness)).

**Diagnosis.**

1. All nodes or one: the alert by `instance`. One node points at its
   network, credentials, or volume.
2. The store itself, with its own tools: for the file backend, its
   directory (`[control_store] directory`, `<data_dir>/control` by
   default); for etcd, the members' health; for S3, the bucket and the
   credentials.
3. Whether it is lost rather than unreachable: a store that answers
   without `cluster.json`, or with an older generation than the node's copy
   (`GET /v1/health` `control_store.generation`), is treated as reset, and
   the log says `the control store looks reset; running from the local
   copy`. Follow [Control store loss](#control-store-loss).

**Remediation.** Restore the store or the path to it. A node that could not
open it opens it within `config_poll_interval_seconds` and logs `opened the
control store`. A file store that failed a write refuses every later
request until it is opened again, which takes a restart of its node.

**Verification.** `skys3_control_store_last_success_timestamp_seconds`
advances on every node, `skys3_control_store_live` is 1, and a
CreateBucket succeeds.

**Do not.** Do not rebuild a store that is only unreachable: a rebuild
writes only into a store that holds no registers, and two stores for one
prefix would let two clusters claim it. Avoid restarting nodes or taking
any down for maintenance while the store is unreachable: no shard can
replace a member until it returns.

### Control store latency

**Symptoms.** None to page on: failover and member removal take one or two
extra round trips per CAS, which shows as longer under-replication after a
member loss. The round trip is not measured (section 8).

**Impact.** Client requests are unaffected: no object request touches the
control store. CreateBucket and DeleteBucket take a few round trips more.

**Diagnosis.** The round trip from the nodes to the store's endpoint, with
the network's own tools.

**Remediation.** Move the control store nearer the cluster, within the
placement rules of design section 6.1 (a different failure domain from
every data target).

**Verification.** Failover times return to their usual values.

**Do not.** Do not move the control store into a data target's region to
shorten the round trip: one regional outage would then stop flushing and
membership changes together.

### Identity staleness

**Symptoms.** `SkyS3IdentityCopyAging` (warning): `time() -
skys3_identity_synced_timestamp_seconds` is over three quarters of
`skys3_identity_max_staleness_seconds`. `SkyS3IdentityCopyStale`
(critical): over all of it.

**Impact.** Once stale, STS answers new `AssumeRoleWithWebIdentity` calls
with `503 ServiceUnavailable` ("The identity configuration is out of date;
retry later"), and the log has `refusing a new session`. Sessions already
issued stay valid until they expire; static credentials are unaffected.

**Diagnosis.** A node syncs its identity copy whenever the generation moves
and at least every half of `identity_max_staleness` while the store
answers. A stale copy therefore means the store has not answered for long:
follow [Control store unreachable](#control-store-unreachable). A node
whose clock stepped back before its last sync also counts its copy as
stale: compare the host's clock with the time synchronization service.

**Remediation.** Restore the control store. A node syncs once it reads the
store again; a restart of a node with the store answering syncs it at
once. To serve longer through an outage, raise `[identity]
identity_max_staleness_hours` on every node and restart them one at a
time, accepting that a revocation made meanwhile reaches the nodes later.

**Verification.** `skys3_identity_synced_timestamp_seconds` advances, and
an `AssumeRoleWithWebIdentity` call succeeds.

**Do not.** Do not raise `identity_max_staleness_hours` for good to quiet
the alert: it is how long a revoked role or provider stays usable on a cut-off
node.

### Control store loss

**Symptoms.** The store answers without its registers: the control bucket
was deleted, the credentials revoked for good, the etcd data lost, or the
file store's directory emptied. Nodes log `the control store looks reset;
running from the local copy` at their next start, and report
`control_store.live` false. While they run, `SkyS3ControlStoreUnreachable`.

**Impact.** As for an unreachable store, until it is rebuilt: the data path
runs from the nodes' copies, and no membership or bucket change can happen.
The nodes never bootstrap a lost store again by themselves.

**Diagnosis.**

1. Confirm the loss, not an outage: the store answers, and `cluster.json`
   under the cluster's prefix is gone (or holds an older generation than
   the nodes' copies). An unmounted volume or a wrong path looks the same:
   rule them out first.
2. Which nodes hold copies: every node's `GET /v1/health`
   `control_store.generation`.

**Remediation.** A rebuild is a deliberate, offline operation (design
section 6.2):

1. Stop every node. An export refuses a node that still runs (the data
   directory is locked).
2. On each node: `skys3 control export --config <file> --output
   <node>.json`. It reports the copy's generation and the shard
   configurations it holds.
3. Plan: `skys3 control rebuild --config <file> --from <node-1>.json
   --from <node-2>.json ... --dry-run`. Read the plan. It refuses:
   - a shard configuration whose members did not all export: export the
     missing nodes, or, only for a node whose disks are gone for good, add
     `--lost <node-id>`;
   - copies of the newest generation that differ: it names the nodes and
     registers; choose one with `--prefer <node-id>`, for example the copy
     that holds a bucket or role known to exist;
   - shards holding objects of a bucket the newest copy does not name:
     `--allow-unnamed` rebuilds without them.
4. Rebuild: the same command without `--dry-run`. It writes only into a
   store that holds no registers, `cluster.json` last, and running it again
   completes an interrupted rebuild. This build writes only the file
   control store, from its one node's export.
5. Start the nodes.

**Verification.** Every node's `GET /v1/health` has `control_store.live`
true and `generation` one past the newest copy's; `skys3_control_store_live`
is 1; the buckets and their objects are there; CreateBucket works.

**Do not.** Never rebuild while any node runs. Never name a node `--lost`
that may come back, and never start a node named `--lost` with its old data
directory: it may hold records of a configuration no other node knew.
Never rebuild into a store another cluster may use: the rebuild refuses a
store with registers, and a prefix shared by two clusters breaks both.
Writes that only older copies held are lost with the store (design
section 6.9).

### etcd majority loss

**Symptoms.** For an etcd control store: etcd answers `UNAVAILABLE` (no
leader) or nothing, so every node shows
[Control store unreachable](#control-store-unreachable). etcd's own
metrics and `etcdctl endpoint status` show fewer than a majority of voters.

**Impact.** As for an unreachable control store, for as long as the
majority is lost. etcd cannot recover a lost majority by itself.

**Diagnosis.** With etcd's tools: which members are lost for good, and
whether the members that are lost come back with their data intact.

**Remediation.** It depends on whether etcd can come back without losing a
write.

- **Every write kept.** The lost members return with their data (an outage,
  not a loss), and etcd regains its majority. Nothing more: the nodes
  follow the store again as after any
  [unreachable control store](#control-store-unreachable).
- **Any write possibly lost.** Recovering etcd from a snapshot, or forcing
  a new cluster from a surviving member, can lose writes the lost majority
  held. Treat that as a [control store loss](#control-store-loss), and
  rebuild from the nodes' exports:
  1. Stop every node before the recovered etcd answers them. A node that
     runs from its copy retries the store every
     `config_poll_interval_seconds`, and follows any store whose generation
     is not below its copy's.
  2. Recover etcd with its own disaster recovery, following the etcd
     documentation for its version.
  3. Rebuild into an empty prefix: export every node, and plan and write
     the rebuild as in [Control store loss](#control-store-loss), with the
     cluster's prefix cleared of what the recovery restored (the rebuild
     refuses a store that holds registers). This build's `skys3 control
     rebuild` plans for any backend but writes only the file control store
     (section 8).
  4. Start the nodes.

  Do not decide by comparing generations. A restored `cluster.json` at the
  same generation as the nodes' copies (`GET /v1/health`
  `control_store.generation`) does not prove that nothing was lost: a
  bucket or identity register is written before the increment that
  announces it, and nodes also sync on a timer, so a copy can hold a write
  that a snapshot taken at the same generation lacks. A node refuses only a
  store whose generation is below its copy's; at an equal generation it
  takes the restored registers, dropping the newer bucket and identity
  registers it held, and a shard register older than a configuration a
  replica acted on would let membership changes reuse that epoch (section
  8).

**Verification.** As for [Control store unreachable](#control-store-unreachable),
or for [Control store loss](#control-store-loss) after a rebuild.

**Do not.** Do not let the nodes reach a restored etcd before deciding.
Do not trust a restored store because its generation matches the nodes'.
The node binary does not run the etcd backend yet.

## 7. Drills

Each runbook was exercised once. Drills are named by test file and
function: `runbooks::` is `crates/skys3/tests/runbooks.rs`, which runs the
real binary unless it says otherwise; `simulation::<scenario>::` is a
scenario of the cluster simulation (`crates/skys3-cluster-sim/tests/simulation/`);
`conflicts::` and `sts::` are the integration tests of `skys3` and
`skys3-sts` of those names; a name that starts with a crate, such as
`skys3_coord::`, is a unit test in that crate's sources.

| Runbook | Drill | What it showed | Found wrong |
|---|---|---|---|
| [Dirty data age](#dirty-data-age) | `runbooks::drill_remote_outage` (in process: flush service, metrics, admin API) | During a remote outage `skys3_oldest_dirty_age_seconds` and `skys3_flush_lag_seconds` grow and `skys3_flush_retries_total` counts retries; `flush.errors` names the key and the error; once the remote returns, everything drains with no action, and the gauges return to 0. Held keys age without flush lag (`runbooks::drill_out_of_band_writes`). | Nothing. |
| [Flush stalled](#flush-stalled) | `runbooks::drill_remote_outage`; `runbooks::drill_dirty_budget` | As above. On the binary, a target unreachable from the start shows `probe` `running` with its `probe_error`. | A SkyS3 node cannot stand in for a remote target: its gateway refuses the write-identity metadata of every flush. The drills use a simulated remote; section 8. |
| [Dirty budget](#dirty-budget) | `runbooks::drill_dirty_budget` | The share fills, a write gets `503 SlowDown`, `skys3_admission_refusals_total{reason="bucket_budget"}` counts it, and `GET /v1/buckets/<bucket>` shows `dirty_bytes` at `dirty_budget_bytes`. A larger `max_dirty_bytes` and a restart admit writes, and the dirty data stays dirty. | Nothing. |
| [Held conflicts](#held-conflicts) | `runbooks::drill_out_of_band_writes` (in process); `conflicts::conflicts_are_listed_and_resolved_under_each_policy`; `simulation::conflicts::hold_keeps_conflicts_until_an_operator_resolves_them` | `skys3_conflicted_keys` and `skys3_flush_conflicts_total` count the held keys; the conflicts list shows each with `remote_identity` null for a writer outside SkyS3; `overwrite` puts the local version at the remote and counts it; `hold` after the other writer's object is removed flushes; a key not held answers `404`. | Nothing. |
| [Discarded conflicts](#discarded-conflicts) | `runbooks::drill_out_of_band_writes` (in process) | A `discard_local` resolution adopts the remote's version and counts it in `skys3_flush_conflicts_discarded_total`, the series the alert reads. | The log lines the runbook quotes were not checked: the in-process drill has no log. |
| [Peer link](#peer-link) | `simulation::peer::every_committed_change_reaches_the_peer_under_faults` | Under link faults, every committed change reaches the peer cluster, and stays dirty at the source until applied. | Not drilled on the binary, which does not run the peer transport. The fallback to S3 REST (UDP blocked) is not built yet (plan M6-07), and could not work against a SkyS3 gateway as it is (section 8). |
| [Under-replication](#under-replication) | `simulation::backfill::a_lost_member_is_replaced_and_its_shards_regain_their_copies`; `simulation::replacement::after_a_node_loss_every_affected_shard_returns_to_replicas_members`; `simulation::takeover::a_single_survivor_takes_over` | A lost member is removed, replaced by a learner, backfilled, and promoted with no operator action; a single survivor takes over and serves what was committed. | Not drilled on the binary: it does not run replication, so its metrics and alerts and the `placement` report cannot be read yet. |
| [Network partition](#network-partition) | `simulation::takeover::competing_candidates_across_a_partition` | Across a partition, one candidate's CAS wins and the shard serves on the majority side; no acknowledged write is lost. | Not drilled on the binary, as above. |
| [Placement policy](#placement-policy) | `skys3_coord::policy::tests::losing_a_rack_leaves_buckets_unsatisfied_without_co_location`; `skys3_coord::metrics::tests::the_gauges_follow_tenures_and_reports`; `simulation::heal::a_lost_rack_is_replaced_across_the_remaining_racks` | The report names short and co-located shards and unlabeled nodes without co-locating; the gauges follow the coordinator's report; a lost rack heals across the others. | Not drilled on the binary, which does not run the coordinator, so `GET /v1/health` has no `placement` yet. |
| [Coordinator](#coordinator) | `simulation::coordinator::coordinator_failover_delays_only_placement_work`; `simulation::heal::a_coordinator_lost_in_the_middle_of_a_change_is_succeeded` | Another node takes the lease; only placement work waits; a change cut short is completed by the successor. | Not drilled on the binary, as above. |
| [Shard lost](#shard-lost) | `simulation::drills::restore_drills_match_the_history`; `simulation::snapshots::the_lost_key_report_matches_the_history_with_backups` | The lost-key report and the restore drill match the history: the keys only the members held, the window after the snapshot, and coded objects re-indexed from their fragments. | The report and the drill have no command, so an operator cannot run them yet (section 8). |
| [Provisioning](#provisioning) | `simulation::registry::a_node_with_valid_credentials_joins_with_no_other_action`; `simulation::rebalancing::a_new_node_receives_its_share_of_shards_and_primaries`; `simulation::heal::a_lost_node_is_replaced_forgotten_and_succeeded_by_a_new_one` | A node with valid credentials joins and receives its share; a lost node is replaced, forgotten, and succeeded. | No command retires a node sooner than `node_forget_after_hours`, and node labels have no configuration key (section 8). |
| [Node down](#node-down) | `runbooks::drill_node_down`; `simulation::takeover::a_crashed_primary_fails_over_within_ten_seconds` | After `SIGKILL`, scrapes and `/healthz` fail; a restart recovers every acknowledged write and reports `ready` with every disk in service. A crashed primary's shards fail over within 10 seconds. | Nothing. |
| [Cluster power loss](#cluster-power-loss) | `runbooks::drill_node_down`; `simulation::restart::a_whole_cluster_restart_while_the_control_store_is_unreachable` | A node that restarts while its control store cannot be opened serves from its copy (`live` false), then opens the store by itself and logs it. In a cluster, unchanged shards serve again and changed ones stay fenced. | Nothing. |
| [Disk out of service](#disk-out-of-service) | `runbooks::drill_disk_out_of_service`; `simulation::harness::a_failed_sync_is_fenced_once` | A disk whose directory disappears fails its next segment: `skys3_disks_out_of_service` 1, `/readyz` `503`, the disk `in_service` false with its error, and `cannot fence the disk` logged. A fenced disk stops the next start, naming `out-of-service.json`; with the disk back and the fence removed, every acknowledged write reads back. | The drill cannot fail a sync on a real disk; it removes the disk's directory, and plants the fence a failed sync writes with the node's own function. |
| [Disk space](#disk-space) | `runbooks::drill_disk_space` | Below `disk_min_free_bytes`, writes get `503 SlowDown` and count as `disk_space` refusals, the log names the place, reads and deletes go on; freeing space admits writes within seconds without a restart. | Nothing. |
| [Clock drift](#clock-drift) | `simulation::leases::drift_beyond_rho_risks_stale_reads_but_writes_stay_safe` | Drift beyond `ρ` risks stale reads, and writes stay safe, as the runbook's impact says. | The diagnosis and remediation are the host's time service's; nothing in SkyS3 to drill. |
| [Fragment repair](#fragment-repair) | `simulation::repair::a_lost_holder_is_repaired_while_reads_go_on`; `simulation::fragment_moves::a_drained_node_is_emptied_while_reads_and_losses_go_on` | A lost holder's fragments are rebuilt elsewhere while degraded reads go on. | Not drilled on the binary, which does not run encoding or repair. |
| [Control store unreachable](#control-store-unreachable) | `runbooks::drill_control_store_outage`; `simulation::removal::without_the_control_store_writes_fail_until_it_returns` | With the store's volume gone under a running node, `skys3_control_store_last_success_timestamp_seconds` stops, `skys3_control_store_live` stays 1, the log says `cannot read the control store's generation`, the data path goes on, and CreateBucket fails. In a cluster, a member that fails while the store is down stalls its shards' writes until the store returns. | CreateBucket answered `500 InternalError`, not the `503` the design gives a node that runs from its copy: the file store failed a write. And the file store stayed refused after its volume returned, until the node restarted. The runbook now says both. |
| [Control store latency](#control-store-latency) | `simulation::workload::slow_and_lossy_control_store_with_partitions` | Client requests go on under control-store round trips of 100 ms and more. | The round trip is not measured (section 8). |
| [Identity staleness](#identity-staleness) | `runbooks::drill_control_store_outage`; `sts::a_stale_identity_copy_stops_new_sessions_only` | `skys3_identity_synced_timestamp_seconds` stops during the outage and advances after a restart with the store answering; a stale copy refuses new sessions and keeps issued ones. | The drill cannot age a copy past `identity_max_staleness` (an hour at least) on the binary; the STS test does it on a simulated clock. |
| [Control store loss](#control-store-loss) | `runbooks::drill_control_store_loss`; `simulation::rebuild::a_lost_control_store_is_rebuilt_and_membership_changes_resume` | A node without its registers serves from its copy and logs `the control store looks reset`; an export refuses a running node; the export, the dry run, and the rebuild write the store at the next generation, and buckets and objects are all there. In a cluster, membership changes resume on the rebuilt store. | Nothing. |
| [etcd majority loss](#etcd-majority-loss) | None | Not drilled: the node binary does not run the etcd backend, and CI's etcd job runs a single member, so a majority cannot be lost and recovered. The SkyS3 side is the [Control store unreachable](#control-store-unreachable) and [Control store loss](#control-store-loss) drills. | Section 8. |

## 8. Open points

What the runbooks need that does not exist yet. Each is a gap an operator
meets today, not a plan of this document.

- **A SkyS3 cluster as an S3 REST target.** A gateway refuses
  `x-amz-meta-skys3-wid` from clients (`400 InvalidArgument`), and every
  flush over S3 REST sends it, so a `write_back` or backup target that is a
  SkyS3 cluster reached over S3 REST fails every flush. The fallback of
  `target_transport = "auto"` to S3 REST (design sections 7.8 and 13, plan
  M6-07) needs a way through.
- **The file control store after a failed write.** It refuses every request
  until it is opened again, and a running node reopens only a store it
  could not open, so the node needs a restart once the store is back.
- **Bucket changes on a failed store** answer `500 InternalError`, while
  a node that runs from its copy answers `503`.
- **Lost-key report and restore drill commands.** `skys3_flush::snapshot`
  builds both, but no command or endpoint runs them.
- **Placement and membership status.** `GET /v1/health` `placement` and
  bucket status with each shard's members arrive with the coordinator and
  replication in the binary.
- **Retiring a node.** No command marks a node `departing` before
  `node_forget_after_hours`; node labels (`zone`, `rack`) have no
  configuration key.
- **Control-store round trips.** No metric measures them; the PR that runs
  the S3 or etcd backend in the binary adds one.
- **Gateway request metrics.** Failover and refused writes show to clients
  as `503` answers that no metric counts.
- **A control store rolled back to the same generation.** A node detects a
  store rolled back only by a generation below its copy's
  (`ControlCopy::fetch_since` in `crates/skys3/src/control.rs`, design
  section 6.2). A store restored to a state at the copy's generation, but
  lacking writes the copy listed, is followed: the node replaces its copy
  with the restored registers, losing newer buckets and roles, or bringing
  back deleted ones. A shard register older than the newest configuration
  a replica acted on is not followed by that replica, but a membership
  change written over it could take an epoch the replica already used.
  Register versions could tell (etcd's are ordered), but versions of other
  backends are not ordered, and nothing compares them now. Until then the
  [etcd majority loss](#etcd-majority-loss) runbook rebuilds after any
  recovery that may have lost a write.
- **The dirty age of deletes after a restart.** A tombstone found dirty at
  startup is dated from the startup (`dirty_since` in
  `crates/skys3-flush/src/shard.rs`), because a delete records no time, and
  it counts 0 in `skys3_dirty_bytes`. After a restart,
  `skys3_oldest_dirty_age_seconds` can therefore understate the loss
  exposure of unflushed deletes, and nothing bounds which deletes a lost
  shard took with it. Dating them would need the delete's commit time in
  its record or in the index.
- **etcd in the binary**, and a multi-member etcd harness to drill majority
  loss.
