//! The node's admin API: health and bucket status, served under `/v1/` by
//! the admin listener, behind its bearer token (design §12; plan section
//! 14 assigns the surface to M1-13).
//!
//! | Path | Answer |
//! |---|---|
//! | `GET /v1/health` | The node and cluster IDs, readiness, whether the control store answers or the node serves its local copy, each disk's state, and the node's cluster services (M8-04, below). |
//! | `GET /v1/buckets` | The status of every bucket. |
//! | `GET /v1/buckets/<name>` | The status of one bucket, or 404. |
//! | `GET /v1/buckets/<name>/conflicts` | The keys of the bucket that this node's flushers hold in conflict (§7.2). |
//! | `POST /v1/buckets/<name>/conflicts/<policy>/<key>` | Resolves the conflict `key` is held in under `policy` (M4-06, below). |
//! | `GET /v1/buckets/<name>/reconcile` | The bucket's reconciliation scans on this node (§9.1, M7-07, below). |
//! | `GET /v1/shards` | Every shard of every bucket: its configuration as this node knows it, and this node's role in it (M8-05b, below). |
//!
//! Answers are JSON objects; later fields are added, never renamed. A
//! bucket's status counts the objects and the unflushed entries (dirty,
//! flushing, in conflict, and tombstones) of the shards open on this node,
//! read from the index. A `write_back` bucket's status also has a `flush`
//! object from its flushers on this node (§7.1, §7.2):
//!
//! - `probe`: `running` until the target's capability probe succeeds, with
//!   `probe_error` from its last failed run, then `done`, with
//!   `unprotected` listing the operations sent unconditionally, and
//!   `server_side_copy`, whether copies of clean sources are sent as remote
//!   `CopyObject` requests rather than uploaded;
//! - `dirty`, `flushing`, and `dirty_bytes`: keys whose latest change is not
//!   at the remote (conflicts excluded), those being flushed, and the bytes
//!   of every version not at the remote;
//! - `dirty_budget_bytes`: this node's share of the bucket's dirty-data
//!   budget; new writes get `503 SlowDown` while `dirty_bytes` is at or
//!   above it (§7.6);
//! - `oldest_dirty_age_seconds` and `flush_lag_seconds`, as the metrics of
//!   the same names;
//! - `conflict_policy`: what a flush that finds an out-of-band write does
//!   (§7.2): `hold`, `overwrite`, or `discard_local`;
//! - `conflicts`: each key held in conflict, with its `shard`, the local
//!   `seq`, and the remote object's ETag and write identity;
//! - `orphaned_uploads`: remote multipart uploads that flushes left open
//!   and that wait to be aborted, as the metric of the same name;
//! - `concurrency`: the target's adaptive window (§7.7), `null` until the
//!   probe is done: `limit`, the requests the flushers may have in flight,
//!   between `floor` and `ceiling`; `in_flight`, those that are;
//!   `inflight_bytes`, the bytes held for them; and
//!   `base_round_trip_seconds`, the base round trip the window is sized
//!   against, `null` until measured;
//! - `streamed_uploads`: remote multipart uploads that stream open local
//!   uploads, or wait to be completed or aborted (§7.3);
//! - `errors`: the latest flush error of each shard that has one, until
//!   a later attempt gets its key past it;
//! - `import`: the namespace import (§9.1): `state` (`running` or `done`),
//!   `after`, the last key it has imported without a gap while it runs,
//!   `ranges` and `ranges_done`, the key ranges it lists in parallel and
//!   those done, `imported`, the `IMPORT` records committed since the node
//!   started, and `error`, its last error until it gets past it;
//! - `transport`: for a target that may be a SkyS3 peer (§7.8), what its
//!   discovery found, `null` for one flushed over S3 REST only:
//!   `configured`, the bucket's `target_transport`; `in_use`, `quic`, `s3`,
//!   or `null` while flushing waits for a transport; `peer`, whether the
//!   target served a peer descriptor that verified; `reason`, why, in
//!   words; and `switches`, how often the transport changed;
//! - `refused_keys`: the dirty keys a refusal holds up (§7.6), as the
//!   metric `skys3_flush_refused_keys`: those whose latest flush attempt
//!   the target refused, or every dirty key while it refuses the probe;
//! - `last_refusal`: the target's latest refusal since the node started,
//!   `null` before one: its `code` (`AccessDenied`, `InvalidAccessKeyId`,
//!   `SignatureDoesNotMatch`, `refused` for a SkyS3 peer's native refusal,
//!   or `other`), the `target` (`<endpoint>/<bucket>`), `error`, what was
//!   refused (`probe: ...`, `copy probe: ...`, or `<key>: ...`) and how,
//!   and `at_unix_ms`.
//!   Flushing retries refused keys with its usual backoff, so it resumes
//!   by itself once the target takes the writes again.
//!
//! **Cluster services** (plan M8-04). `health` has a `cluster` object:
//! `multi_node`, as `[cluster] multi_node` says, and with it true,
//!
//! - `transport`: `listen`, the bound address; `advertise`, the address
//!   the node registered; and `handshakes_in_flight` and
//!   `max_inflight_handshakes`, the listener's handshakes running now and
//!   its cap (design §12);
//! - `registration`: `registered`, whether the node has registered since
//!   it started; `registrations`, how often, an unchanged registration
//!   included; `error`, why the latest attempt failed, until one
//!   succeeds; and `claimed_by`, the instance ID of the data directory
//!   whose registration holds the node's ID, which stops the node
//!   (design §6.7);
//! - `heartbeats`: `answered`, the heartbeats the lease holder answered,
//!   `delivered`, those a coordinator acknowledged, and `coordinator`, the
//!   node that acknowledged the latest one (design §6.7);
//! - `pushed_generation`: the newest generation another node pushed, or
//!   `null` before any (design §6.2).
//!
//! Once the node runs the coordinator, `health` gains a `placement` object
//! from `skys3_coord::PlacementHealth` (design §12), and bucket status the
//! shards' members (M3-04).
//!
//! **Shards** (§5, §6.3, plan M8-05b). `GET /v1/shards` answers the
//! `node_id` and `shards`, one object per shard of every bucket the node
//! knows, in bucket order: `bucket`, `bucket_id`, and `shard`; `source`,
//! where the node knows the configuration from: `replica`, its own replica
//! of the shard, `map`, the gateway's shard map (a shard this node holds no
//! replica of, as last learned from a register or a redirect), or `null`
//! where it knows none yet; from the configuration, `epoch`, `primary`,
//! `members`, `learners`, `replicas`, and `min_write_replicas`; and
//! `role`, this node's: `primary`, `member`, `learner`, `stopped` (a
//! replica that stopped serving, as a deposed primary's or a removed
//! member's does until it is opened again), or `none`. A replica also
//! reports `serving`, whether it serves client requests (a primary whose
//! members hold its log and grant it leases), and `stepped_down`, whether
//! it handed the shard off. A single node reports each shard of its own
//! replica, as primary. With `[cluster] multi_node = true` the answer also
//! has `peers`, the transport address of each registered node, and
//! `routing`, the gateway's counts since the node started: `forwarded`
//! (calls sent to another node), `redirects` (redirect hints that updated
//! the shard map), and `register_reads`.
//!
//! **Conflicts** (§7.2, plan M4-06). A conflict is held by the flusher of
//! the key's shard on that shard's primary, so a node lists and resolves
//! only the conflicts its own flushers hold; an operator asks each node.
//! `GET /v1/buckets/<name>/conflicts` answers the bucket's `name`, its
//! `conflict_policy` (`null` where no flusher of it runs here), and its
//! `conflicts` as in the `flush` object. `POST
//! /v1/buckets/<name>/conflicts/<policy>/<key>`, where `key` is the rest of
//! the path, percent-decoded, returns a held key to dirty, and its next
//! flush carries out `policy`: `hold` retries the conditional flush, for
//! example once the operator has removed the other writer's object;
//! `overwrite` replaces the remote's write with the local version; and
//! `discard_local` adopts the remote's write and drops the local version,
//! an acknowledged write. The operator chooses per key, so a `discard_local`
//! resolution needs no opt-in in the bucket's table, unlike the policy;
//! a `local` bucket's backup target never discards. It answers `200`
//! with the `bucket`, `key`, `policy`, and `state` (`dirty`); `400` for an
//! unknown policy or a key that is not valid percent-encoded UTF-8; `404`
//! for an unknown bucket, a bucket not flushed here, or a key not held
//! here; `409` for `discard_local` on a backup target; and `503`
//! if the key's flusher stopped meanwhile. A resolution lives in the
//! flusher's memory until the key's next flush succeeds: if the primary
//! changes first, the new primary finds the conflict again and applies the
//! bucket's policy, holding it for the operator again under `hold`.
//!
//! **Reconciliation** (§9.1, plan M7-07). `GET /v1/buckets/<name>/reconcile`
//! answers the bucket's `name` and, where its `reconcile_interval_seconds`
//! is set and it is a `write_back` bucket this node follows, `enabled`
//! true with `interval_seconds`; `running_since_unix_ms` and `after`, the
//! last key compared, while a scan runs; `error`, the scan's last request
//! error until a request succeeds; and `last`, the last finished scan's
//! report, `null` before one: `started_unix_ms` and `finished_unix_ms`,
//! `shards` (those the node led and could read throughout), `listed`
//! (remote objects), `compared` (keys of those shards), `counts` by kind,
//! `unconfirmed`
//! (candidates past the report's 1,000 differences), and `differences`,
//! each with its `key`, `shard`, `kind` (`remote_only`, `index_only`,
//! `etag_mismatch`, or `size_mismatch`), `index` (`version`, `state`,
//! `size`, `remote_etag`, `remote_version_id`; `null` for `remote_only`),
//! and `remote` (`etag`, `size`, `version_id`, `write_identity`, and
//! `writer`: `outside`, `own`, `foreign`, or `unknown`; `null` for
//! `index_only`). Each node scans only the shards it leads, so an operator
//! asks every node.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use serde_json::{Value, json};
use skys3_config::{ConflictPolicy, TargetTransport};
use skys3_coord::{ControlHints, HeartbeatStatus};
use skys3_flush::{
    BucketStatus, Difference, FlushService, ProbeStatus, ReconcileStatus, Transport, Unresolved,
};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_index::ImportCheckpoint;
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::SegmentLog;
use skys3_obs::{AdminApi, ApiFuture, HandshakeLimit, Health};
use skys3_remote::ObjectStore;
use skys3_shard::{Role, ShardSet};
use skys3_types::{
    BucketDocument, BucketId, ClusterId, Generation, Label, NodeAddress, NodeId, ShardConfig,
};
use tokio::sync::watch;

use crate::datadir::DiskDir;

/// What the node knows about its control store, for `/v1/health`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlState {
    /// Whether the control store has answered since startup. Until it
    /// has, the node serves its local copy.
    pub live: bool,
    /// The generation of the node's copy.
    pub generation: Option<Generation>,
    /// When the sync that read the copy started, as time since the Unix
    /// epoch.
    pub synced_at: Option<Duration>,
}

/// What the cluster services of a node with `[cluster] multi_node = true`
/// report, for `/v1/health`.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// The address the transport listener is bound to.
    pub listen: std::net::SocketAddr,
    /// The address the node registers for other nodes.
    pub advertise: NodeAddress,
    /// The heartbeater's progress.
    pub heartbeats: watch::Receiver<HeartbeatStatus>,
    /// The generations other nodes pushed.
    pub hints: ControlHints,
    /// The transport listener's handshakes.
    pub handshakes: HandshakeLimit,
}

impl ClusterView {
    /// The `cluster` object of `/v1/health`.
    #[must_use]
    pub fn report(&self) -> Value {
        let status = self.heartbeats.borrow().clone();
        let pushed = self.hints.latest();
        json!({
            "multi_node": true,
            "transport": {
                "listen": self.listen.to_string(),
                "advertise": self.advertise.to_string(),
                "handshakes_in_flight": self.handshakes.in_flight(),
                "max_inflight_handshakes": self.handshakes.cap(),
            },
            "registration": {
                "registered": status.registrations > 0,
                "registrations": status.registrations,
                "error": status.registration_error,
                "claimed_by": status.claimed_by.as_ref().map(Label::as_str),
            },
            "heartbeats": {
                "answered": status.answered,
                "delivered": status.delivered,
                "coordinator": status.coordinator.as_ref().map(NodeId::as_str),
            },
            "pushed_generation": (pushed > Generation::ZERO).then(|| pushed.get()),
        })
    }
}

/// The source of the gateway's bucket list.
pub type BucketList = Arc<dyn Fn() -> Vec<BucketDocument> + Send + Sync>;

/// The future of [`Flushers::resolve`].
pub type ResolveFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Unresolved>> + Send + 'a>>;

/// What the admin API asks of the node's flushers: a
/// [`FlushService`] of any store.
pub trait Flushers: Send + Sync + 'static {
    /// The flush state of `bucket`, if it is flushed on this node.
    fn status(&self, bucket: &BucketId) -> Option<BucketStatus>;

    /// Resolves the conflict `key` of `bucket` is held in under `policy`
    /// ([`FlushService::resolve`]).
    fn resolve<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        policy: ConflictPolicy,
    ) -> ResolveFuture<'a>;

    /// The reconciliation scans of `bucket` on this node
    /// ([`FlushService::reconciliation`]).
    fn reconciliation(&self, bucket: &BucketId) -> Option<ReconcileStatus>;
}

impl<S: ObjectStore, D: Disk> Flushers for FlushService<S, D> {
    fn status(&self, bucket: &BucketId) -> Option<BucketStatus> {
        FlushService::status(self, bucket)
    }

    fn resolve<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        policy: ConflictPolicy,
    ) -> ResolveFuture<'a> {
        Box::pin(FlushService::resolve(self, bucket, key, policy))
    }

    fn reconciliation(&self, bucket: &BucketId) -> Option<ReconcileStatus> {
        FlushService::reconciliation(self, bucket)
    }
}

/// The future of [`ShardsReport::report`].
pub type ReportFuture<'a> = Pin<Box<dyn Future<Output = Value> + Send + 'a>>;

/// What `GET /v1/shards` reports on a node with `[cluster] multi_node =
/// true`, whose shards live on several nodes (plan M8-05b).
pub trait ShardsReport: Send + Sync + 'static {
    /// The answer for the shards of `buckets`.
    fn report<'a>(&'a self, buckets: &'a [BucketDocument]) -> ReportFuture<'a>;
}

/// The entry of `shard` of `bucket` in `GET /v1/shards`, for node `node`:
/// as its replica in `set` holds it, or else as `known` gives it.
pub(crate) async fn shard_entry<D: Disk>(
    bucket: &BucketDocument,
    shard: &ShardRef,
    set: &ShardSet<D>,
    known: Option<ShardConfig>,
    node: &NodeId,
) -> Value {
    let mut entry = json!({
        "bucket": bucket.name.as_str(),
        "bucket_id": bucket.bucket_id.as_str(),
        "shard": shard.shard.get(),
    });
    if let Some(replica) = set.get(&shard.into()).await {
        entry["source"] = json!("replica");
        describe(&mut entry, &replica.config());
        entry["role"] = json!(if replica.is_stopped() {
            "stopped"
        } else {
            role_name(replica.role())
        });
        entry["serving"] = json!(replica.is_serving());
        entry["stepped_down"] = json!(replica.stepped_down().is_some());
    } else if let Some(config) = known {
        entry["source"] = json!("map");
        describe(&mut entry, &config);
        entry["role"] = json!(role_in(&config, node));
    } else {
        entry["source"] = Value::Null;
        entry["role"] = json!("none");
    }
    entry
}

/// Adds the fields of `config` to a shard's entry in `/v1/shards`.
fn describe(entry: &mut Value, config: &ShardConfig) {
    fn names(nodes: &[NodeId]) -> Vec<&str> {
        nodes.iter().map(NodeId::as_str).collect()
    }
    entry["epoch"] = json!(config.epoch.get());
    entry["primary"] = json!(config.primary.as_str());
    entry["members"] = json!(names(&config.members));
    entry["learners"] = json!(names(&config.learners));
    entry["replicas"] = json!(config.replicas);
    entry["min_write_replicas"] = json!(config.min_write_replicas);
}

/// The name of a replica's role in `/v1/shards`.
fn role_name(role: Role) -> &'static str {
    match role {
        Role::Alone | Role::Primary => "primary",
        Role::Member => "member",
        Role::Learner => "learner",
        _ => "unknown",
    }
}

/// What `config` makes `node`, which holds no replica of its shard.
fn role_in(config: &ShardConfig, node: &NodeId) -> &'static str {
    if config.primary == *node {
        "primary"
    } else if config.is_member(node) {
        "member"
    } else if config.is_learner(node) {
        "learner"
    } else {
        "none"
    }
}

/// The node's admin API.
pub struct NodeAdmin<D: Disk> {
    /// The node's ID.
    pub node_id: NodeId,
    /// The cluster's ID.
    pub cluster_id: ClusterId,
    /// The gateway's buckets.
    pub buckets: BucketList,
    /// The node's shards.
    pub shards: LocalShards<D>,
    /// Each disk with its log.
    pub disks: Vec<(DiskDir, SegmentLog<D>)>,
    /// The control store's state.
    pub control: Arc<Mutex<ControlState>>,
    /// The node's readiness.
    pub health: Health,
    /// The node's flushers.
    pub flush: Arc<dyn Flushers>,
    /// The node's cluster services, with `[cluster] multi_node = true`.
    pub cluster: Option<ClusterView>,
    /// The shards replicated across nodes, with `[cluster] multi_node =
    /// true`; `None` for a single node, whose shards are in
    /// [`NodeAdmin::shards`].
    pub replicated: Option<Arc<dyn ShardsReport>>,
}

impl<D: Disk> fmt::Debug for NodeAdmin<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeAdmin")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

fn json_response(status: StatusCode, body: &Value) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl<D: Disk> NodeAdmin<D> {
    /// The answer to `GET /v1/health`.
    pub async fn health(&self) -> Value {
        let control = self
            .control
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let disks: Vec<_> = self
            .disks
            .iter()
            .map(|(disk, log)| {
                json!({
                    "label": disk.label.as_str(),
                    "path": disk.path.display().to_string(),
                    "in_service": log.is_in_service(),
                    "error": log.failure().map(|error| error.to_string()),
                })
            })
            .collect();
        let not_ready = self.health.not_ready();
        json!({
            "node_id": self.node_id.as_str(),
            "cluster_id": self.cluster_id.as_str(),
            "ready": not_ready.is_empty(),
            "not_ready": not_ready,
            "control_store": {
                "live": control.live,
                "generation": control.generation.map(Generation::get),
                "synced_at_unix_ms": control.synced_at.map(millis),
            },
            "disks": disks,
            "shards_open": self.shards.set().shards().await.len(),
            "cluster": self
                .cluster
                .as_ref()
                .map_or_else(|| json!({ "multi_node": false }), ClusterView::report),
        })
    }

    /// The status of `bucket`.
    pub async fn bucket(&self, bucket: &BucketDocument) -> Value {
        let (mut open, mut sealed, mut objects, mut unflushed) = (0_u32, 0_u32, 0_u64, 0_u64);
        let mut errors = Vec::new();
        for shard in ShardRef::all(bucket) {
            let Some(local) = self.shards.set().get(&(&shard).into()).await else {
                continue;
            };
            open += 1;
            sealed += u32::from(local.is_sealed());
            match local.summary().await {
                Ok(summary) => {
                    objects += summary.objects;
                    unflushed += summary.unflushed;
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        let target = bucket.target.as_ref().map(|target| {
            let prefix = target.prefix.as_deref().unwrap_or_default();
            format!("{}/{}/{prefix}", target.endpoint, target.bucket)
        });
        let mut status = json!({
            "name": bucket.name.as_str(),
            "bucket_id": bucket.bucket_id.as_str(),
            "mode": bucket.mode,
            "target": target,
            "created_unix_ms": bucket.created_unix_ms,
            "shards": bucket.shards.get(),
            "shards_open": open,
            "shards_sealed": sealed,
            "objects": objects,
            "unflushed": unflushed,
            "errors": errors,
        });
        if let Some(flush) = self.flush.status(&bucket.bucket_id) {
            status["flush"] = flush_status(&flush);
        }
        status
    }

    /// The answer to `GET /v1/shards`.
    pub async fn shards(&self) -> Value {
        let buckets = (self.buckets)();
        if let Some(replicated) = &self.replicated {
            return replicated.report(&buckets).await;
        }
        let mut shards = Vec::new();
        for bucket in &buckets {
            for shard in ShardRef::all(bucket) {
                let set = self.shards.set();
                shards.push(shard_entry(bucket, &shard, set, None, &self.node_id).await);
            }
        }
        json!({ "node_id": self.node_id.as_str(), "shards": shards })
    }

    /// The answer to `GET /v1/buckets/<name>/conflicts`.
    fn conflicts(&self, bucket: &BucketDocument) -> Value {
        let flush = self.flush.status(&bucket.bucket_id);
        json!({
            "bucket": bucket.name.as_str(),
            "conflict_policy": flush.as_ref().map(|flush| policy_name(flush.conflict_policy)),
            "conflicts": flush.as_ref().map_or_else(Vec::new, conflicts),
        })
    }

    /// The answer to `GET /v1/buckets/<name>/reconcile`.
    fn reconciliation(&self, bucket: &BucketDocument) -> Value {
        let status = self.flush.reconciliation(&bucket.bucket_id);
        let mut answer = json!({
            "bucket": bucket.name.as_str(),
            "enabled": status.is_some(),
        });
        if let Some(status) = status {
            answer["interval_seconds"] = json!(status.interval.as_secs());
            answer["running_since_unix_ms"] = json!(status.running_since_ms);
            answer["after"] = json!(status.after);
            answer["error"] = json!(status.error);
            answer["last"] = status.last.map_or(Value::Null, |report| {
                let counts: serde_json::Map<String, Value> = skys3_flush::DifferenceKind::ALL
                    .into_iter()
                    .map(|kind| (kind.as_str().to_owned(), json!(report.count(kind))))
                    .collect();
                json!({
                    "started_unix_ms": report.started_ms,
                    "finished_unix_ms": report.finished_ms,
                    "shards": report.shards.iter().map(|shard| shard.get()).collect::<Vec<_>>(),
                    "listed": report.listed,
                    "compared": report.compared,
                    "counts": counts,
                    "unconfirmed": report.unconfirmed,
                    "differences": report.differences.iter().map(difference).collect::<Vec<_>>(),
                })
            });
        }
        answer
    }

    /// The answer to `POST /v1/buckets/<name>/conflicts/<policy>/<key>`,
    /// with `rest` the path after `conflicts/`.
    async fn resolve(&self, bucket: &BucketDocument, rest: &str) -> Response<Full<Bytes>> {
        let error = |status, message: &str| json_response(status, &json!({ "error": message }));
        let Some((policy, key)) = rest.split_once('/') else {
            return error(StatusCode::NOT_FOUND, "not found");
        };
        let Some(policy) = parse_policy(policy) else {
            return error(
                StatusCode::BAD_REQUEST,
                "the policy is hold, overwrite, or discard_local",
            );
        };
        let Some(key) = percent_decode(key).filter(|key| !key.is_empty()) else {
            return error(
                StatusCode::BAD_REQUEST,
                "the key is not valid percent-encoded UTF-8",
            );
        };
        match self.flush.resolve(&bucket.bucket_id, &key, policy).await {
            Ok(()) => json_response(
                StatusCode::OK,
                &json!({
                    "bucket": bucket.name.as_str(),
                    "key": key,
                    "policy": policy_name(policy),
                    "state": "dirty",
                }),
            ),
            Err(refusal) => {
                let status = match refusal {
                    Unresolved::Backup => StatusCode::CONFLICT,
                    Unresolved::Stopped => StatusCode::SERVICE_UNAVAILABLE,
                    Unresolved::NotFlushed | Unresolved::NotHeld => StatusCode::NOT_FOUND,
                };
                error(status, &refusal.to_string())
            }
        }
    }

    async fn route(&self, method: &Method, path: &str) -> Option<Response<Full<Bytes>>> {
        let route = path.strip_prefix("/v1/")?;
        let known = route == "health"
            || route == "shards"
            || route == "buckets"
            || route.starts_with("buckets/");
        if !known {
            return None;
        }
        // `buckets/<name>/conflicts/<policy>/<key>` resolves; every other
        // route only reads.
        let conflict = route
            .strip_prefix("buckets/")
            .and_then(|rest| rest.split_once('/'))
            .and_then(|(name, rest)| Some((name, rest.strip_prefix("conflicts")?)));
        let resolving = conflict.and_then(|(name, rest)| Some((name, rest.strip_prefix('/')?)));
        let allowed = if resolving.is_some() {
            method == Method::POST
        } else {
            method == Method::GET || method == Method::HEAD
        };
        if !allowed {
            let mut response = json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &json!({"error": "method not allowed"}),
            );
            let allow = if resolving.is_some() {
                "POST"
            } else {
                "GET, HEAD"
            };
            response
                .headers_mut()
                .insert(header::ALLOW, http::HeaderValue::from_static(allow));
            return Some(response);
        }
        let find = |name: &str| {
            (self.buckets)()
                .into_iter()
                .find(|bucket| bucket.name.as_str() == name)
        };
        if let Some((name, rest)) = resolving {
            return Some(self.resolve(&find(name)?, rest).await);
        }
        if let Some((name, rest)) = conflict {
            if !rest.is_empty() {
                return None;
            }
            return Some(json_response(StatusCode::OK, &self.conflicts(&find(name)?)));
        }
        if let Some(name) = route
            .strip_prefix("buckets/")
            .and_then(|rest| rest.strip_suffix("/reconcile"))
            .filter(|name| !name.contains('/'))
        {
            let body = self.reconciliation(&find(name)?);
            return Some(json_response(StatusCode::OK, &body));
        }
        let body = match route {
            "health" => self.health().await,
            "shards" => self.shards().await,
            "buckets" => {
                let mut statuses = Vec::new();
                for bucket in (self.buckets)() {
                    statuses.push(self.bucket(&bucket).await);
                }
                json!({ "buckets": statuses })
            }
            _ => {
                let name = route.strip_prefix("buckets/")?;
                self.bucket(&find(name)?).await
            }
        };
        Some(json_response(StatusCode::OK, &body))
    }
}

/// The name of `policy` in configuration and in the admin API.
fn policy_name(policy: ConflictPolicy) -> &'static str {
    match policy {
        ConflictPolicy::Hold => "hold",
        ConflictPolicy::Overwrite => "overwrite",
        ConflictPolicy::DiscardLocal => "discard_local",
    }
}

/// The policy named `name`, as [`policy_name`] names it.
fn parse_policy(name: &str) -> Option<ConflictPolicy> {
    [
        ConflictPolicy::Hold,
        ConflictPolicy::Overwrite,
        ConflictPolicy::DiscardLocal,
    ]
    .into_iter()
    .find(|policy| policy_name(*policy) == name)
}

/// Decodes a percent-encoded path segment, or returns `None` if an escape
/// is not two hex digits or the bytes are not UTF-8.
fn percent_decode(encoded: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut rest = encoded.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        if byte == b'%' {
            let hex = after.get(..2)?;
            let text = std::str::from_utf8(hex).ok()?;
            if !text.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            bytes.push(u8::from_str_radix(text, 16).ok()?);
            rest = &after[2..];
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    String::from_utf8(bytes).ok()
}

/// Each key `status`'s flushers hold in conflict.
fn conflicts(status: &BucketStatus) -> Vec<Value> {
    status
        .shards
        .iter()
        .flat_map(|(shard, status)| status.conflicts.iter().map(move |c| (shard, c)))
        .map(|(shard, conflict)| {
            json!({
                "key": conflict.key,
                "shard": shard.get(),
                "seq": conflict.seq.get(),
                "remote_etag": conflict.remote_etag.as_ref().map(|etag| etag.as_str()),
                "remote_identity": conflict.remote_identity,
            })
        })
        .collect()
}

/// One difference of a reconciliation report.
fn difference(difference: &Difference) -> Value {
    json!({
        "key": difference.key,
        "shard": difference.shard.get(),
        "kind": difference.kind.as_str(),
        "index": difference.index.as_ref().map(|index| json!({
            "version": index.version.to_string(),
            "state": format!("{:?}", index.state).to_ascii_lowercase(),
            "size": index.size,
            "remote_etag": index.remote_etag.as_str(),
            "remote_version_id": index.remote_version_id,
        })),
        "remote": difference.remote.as_ref().map(|remote| json!({
            "etag": remote.etag.as_str(),
            "size": remote.size,
            "version_id": remote.version_id,
            "write_identity": remote.identity,
            "writer": remote.writer.as_str(),
        })),
    })
}

/// The `flush` object of a bucket's status.
fn flush_status(status: &BucketStatus) -> Value {
    let gauges = status.gauges(SystemWallClock.now());
    let shards = status.shards.iter().map(|(_, shard)| shard);
    let conflicts = conflicts(status);
    let errors: Vec<_> = shards
        .clone()
        .filter_map(|shard| shard.last_error.clone())
        .collect();
    let (probe, probe_error, unprotected, server_side_copy) = match &status.probe {
        ProbeStatus::Running { error, .. } => ("running", error.clone(), Vec::new(), false),
        ProbeStatus::Done {
            unprotected,
            server_side_copy,
        } => (
            "done",
            None,
            unprotected.iter().map(|op| op.as_str()).collect(),
            *server_side_copy,
        ),
    };
    json!({
        "probe": probe,
        "probe_error": probe_error,
        "unprotected": unprotected,
        "server_side_copy": server_side_copy,
        "dirty": shards.clone().map(|shard| shard.dirty).sum::<u64>(),
        "streamed_uploads": shards.clone().map(|shard| shard.streams).sum::<u64>(),
        "flushing": shards.map(|shard| shard.flushing).sum::<u64>(),
        "dirty_bytes": gauges.dirty_bytes,
        "dirty_budget_bytes": gauges.dirty_budget,
        "oldest_dirty_age_seconds": gauges.oldest_dirty_age,
        "flush_lag_seconds": gauges.flush_lag,
        "conflict_policy": policy_name(status.conflict_policy),
        "conflicts": conflicts,
        "orphaned_uploads": gauges.orphaned_uploads,
        "concurrency": status.concurrency.map(|window| json!({
            "limit": window.limit,
            "in_flight": window.in_flight,
            "floor": window.floor,
            "ceiling": window.ceiling,
            "inflight_bytes": window.inflight_bytes,
            "base_round_trip_seconds": window.base_round_trip.map(|base| base.as_secs_f64()),
        })),
        "errors": errors,
        "backup": status.backup,
        // A backup target imports nothing (§8.9).
        "import": status.import.as_ref().map(|import| json!({
            "state": match import.checkpoint {
                ImportCheckpoint::Running { .. } => "running",
                ImportCheckpoint::Done => "done",
            },
            "after": match &import.checkpoint {
                ImportCheckpoint::Running { after } => after.clone(),
                ImportCheckpoint::Done => None,
            },
            "ranges": import.ranges.ranges().len(),
            "ranges_done": import.ranges.done(),
            "imported": import.imported,
            "error": import.error,
        })),
        "transport": status.transport.as_ref().map(|transport| json!({
            "configured": match transport.configured {
                TargetTransport::Auto => "auto",
                TargetTransport::Native => "native",
                TargetTransport::S3 => "s3",
            },
            "in_use": transport.in_use.map(Transport::as_str),
            "peer": transport.peer,
            "reason": transport.reason,
            "switches": transport.switches,
        })),
        "refused_keys": gauges.refused_keys,
        "last_refusal": status.last_refusal.as_ref().map(|refusal| json!({
            "code": refusal.code.as_str(),
            "target": refusal.target,
            "error": refusal.error,
            "at_unix_ms": millis(refusal.at),
        })),
    })
}

impl<D: Disk> AdminApi for NodeAdmin<D> {
    fn call<'a>(&'a self, method: &'a Method, path: &'a str) -> ApiFuture<'a> {
        Box::pin(self.route(method, path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_round_trip_through_their_names() {
        for policy in [
            ConflictPolicy::Hold,
            ConflictPolicy::Overwrite,
            ConflictPolicy::DiscardLocal,
        ] {
            assert_eq!(parse_policy(policy_name(policy)), Some(policy));
        }
        assert_eq!(parse_policy("discard-local"), None);
        assert_eq!(parse_policy(""), None);
    }

    #[test]
    fn keys_are_percent_decoded() {
        assert_eq!(percent_decode("a/b c").as_deref(), Some("a/b c"));
        assert_eq!(
            percent_decode("b%20key%2F%c3%a9").as_deref(),
            Some("b key/\u{e9}")
        );
        assert_eq!(percent_decode("100%"), None);
        assert_eq!(percent_decode("%2"), None);
        assert_eq!(percent_decode("%zz"), None);
        assert_eq!(percent_decode("%+1"), None);
        assert_eq!(percent_decode("%ff"), None);
    }

    #[test]
    fn shard_roles_are_named_as_the_shards_route_reports_them() {
        let node = |name: &str| NodeId::new(name).unwrap();
        let config = ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: skys3_types::ShardId::new(0),
            epoch: skys3_types::Epoch::new(3),
            primary: node("a"),
            members: vec![node("a"), node("b")],
            learners: vec![node("c")],
            min_write_replicas: 2,
            replicas: 3,
            proposal_id: skys3_types::ProposalId::new("p").unwrap(),
        };
        assert_eq!(role_in(&config, &node("a")), "primary");
        assert_eq!(role_in(&config, &node("b")), "member");
        assert_eq!(role_in(&config, &node("c")), "learner");
        assert_eq!(role_in(&config, &node("d")), "none");
        for (role, name) in [
            (Role::Alone, "primary"),
            (Role::Primary, "primary"),
            (Role::Member, "member"),
            (Role::Learner, "learner"),
        ] {
            assert_eq!(role_name(role), name);
        }
        let mut entry = json!({});
        describe(&mut entry, &config);
        assert_eq!(
            entry,
            json!({
                "epoch": 3, "primary": "a", "members": ["a", "b"], "learners": ["c"],
                "replicas": 3, "min_write_replicas": 2,
            })
        );
    }
}
