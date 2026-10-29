//! The node's admin API: health and bucket status, served under `/v1/` by
//! the admin listener, behind its bearer token (design §12; plan section
//! 14 assigns the surface to M1-13).
//!
//! | Path | Answer |
//! |---|---|
//! | `GET /v1/health` | The node and cluster IDs, readiness, whether the control store answers or the node serves its local copy, and each disk's state. |
//! | `GET /v1/buckets` | The status of every bucket. |
//! | `GET /v1/buckets/<name>` | The status of one bucket, or 404. |
//!
//! Answers are JSON objects; later fields are added, never renamed. A
//! bucket's status counts the objects and the unflushed entries (dirty,
//! flushing, in conflict, and tombstones) of the shards open on this node,
//! read from the index. A `write_back` bucket's status also has a `flush`
//! object from its flushers on this node (§7.1, §7.2):
//!
//! - `probe`: `running` until the target's capability probe succeeds, with
//!   `probe_error` from its last failed run, then `done`, with
//!   `unprotected` listing the operations sent unconditionally;
//! - `dirty`, `flushing`, and `dirty_bytes`: keys whose latest change is not
//!   at the remote (conflicts excluded), those being flushed, and the bytes
//!   of every version not at the remote;
//! - `dirty_budget_bytes`: this node's share of the bucket's dirty-data
//!   budget; new writes get `503 SlowDown` while `dirty_bytes` is at or
//!   above it (§7.6);
//! - `oldest_dirty_age_seconds` and `flush_lag_seconds`, as the metrics of
//!   the same names;
//! - `conflicts`: each key held in conflict, with the local `seq` and the
//!   remote object's ETag and write identity;
//! - `orphaned_uploads`: remote multipart uploads that flushes left open
//!   and that wait to be aborted, as the metric of the same name;
//! - `errors`: the latest flush error of each shard that has one, until
//!   a later attempt gets its key past it.
//!
//! Placement (M3-03) adds shard members, and M4-06 its own fields.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use serde_json::{Value, json};
use skys3_flush::{BucketStatus, ProbeStatus};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::SegmentLog;
use skys3_obs::{AdminApi, ApiFuture, Health};
use skys3_types::{BucketDocument, BucketId, ClusterId, Generation, NodeId};

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

/// The source of the gateway's bucket list.
pub type BucketList = Arc<dyn Fn() -> Vec<BucketDocument> + Send + Sync>;

/// The source of a bucket's flush status, if it is flushed on this node.
pub type FlushStatus = Arc<dyn Fn(&BucketId) -> Option<BucketStatus> + Send + Sync>;

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
    /// The flushers' status by bucket.
    pub flush: FlushStatus,
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
        if let Some(flush) = (self.flush)(&bucket.bucket_id) {
            status["flush"] = flush_status(&flush);
        }
        status
    }

    async fn route(&self, method: &Method, path: &str) -> Option<Response<Full<Bytes>>> {
        let route = path.strip_prefix("/v1/")?;
        let known = route == "health" || route == "buckets" || route.starts_with("buckets/");
        if !known {
            return None;
        }
        if method != Method::GET && method != Method::HEAD {
            let mut response = json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &json!({"error": "method not allowed"}),
            );
            response
                .headers_mut()
                .insert(header::ALLOW, http::HeaderValue::from_static("GET, HEAD"));
            return Some(response);
        }
        let body = match route {
            "health" => self.health().await,
            "buckets" => {
                let mut statuses = Vec::new();
                for bucket in (self.buckets)() {
                    statuses.push(self.bucket(&bucket).await);
                }
                json!({ "buckets": statuses })
            }
            _ => {
                let name = route.strip_prefix("buckets/")?;
                let bucket = (self.buckets)()
                    .into_iter()
                    .find(|bucket| bucket.name.as_str() == name)?;
                self.bucket(&bucket).await
            }
        };
        Some(json_response(StatusCode::OK, &body))
    }
}

/// The `flush` object of a bucket's status.
fn flush_status(status: &BucketStatus) -> Value {
    let gauges = status.gauges(SystemWallClock.now());
    let shards = status.shards.iter().map(|(_, shard)| shard);
    let conflicts: Vec<_> = shards
        .clone()
        .flat_map(|shard| &shard.conflicts)
        .map(|conflict| {
            json!({
                "key": conflict.key,
                "seq": conflict.seq.get(),
                "remote_etag": conflict.remote_etag.as_ref().map(|etag| etag.as_str()),
                "remote_identity": conflict.remote_identity,
            })
        })
        .collect();
    let errors: Vec<_> = shards
        .clone()
        .filter_map(|shard| shard.last_error.clone())
        .collect();
    let (probe, probe_error, unprotected) = match &status.probe {
        ProbeStatus::Running { error } => ("running", error.clone(), Vec::new()),
        ProbeStatus::Done { unprotected } => (
            "done",
            None,
            unprotected.iter().map(|op| op.as_str()).collect(),
        ),
    };
    json!({
        "probe": probe,
        "probe_error": probe_error,
        "unprotected": unprotected,
        "dirty": shards.clone().map(|shard| shard.dirty).sum::<u64>(),
        "flushing": shards.map(|shard| shard.flushing).sum::<u64>(),
        "dirty_bytes": gauges.dirty_bytes,
        "dirty_budget_bytes": gauges.dirty_budget,
        "oldest_dirty_age_seconds": gauges.oldest_dirty_age,
        "flush_lag_seconds": gauges.flush_lag,
        "conflicts": conflicts,
        "orphaned_uploads": gauges.orphaned_uploads,
        "errors": errors,
    })
}

impl<D: Disk> AdminApi for NodeAdmin<D> {
    fn call<'a>(&'a self, method: &'a Method, path: &'a str) -> ApiFuture<'a> {
        Box::pin(self.route(method, path))
    }
}
