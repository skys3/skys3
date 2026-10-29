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
//! read from the index. The flusher (plan M1-16) adds flush lag and
//! conflicts, placement (M3-03) shard members, and M4-06 its own fields.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use serde_json::{Value, json};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_io::Disk;
use skys3_log::SegmentLog;
use skys3_obs::{AdminApi, ApiFuture, Health};
use skys3_types::{BucketDocument, ClusterId, Generation, NodeId};

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
        json!({
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
        })
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

impl<D: Disk> AdminApi for NodeAdmin<D> {
    fn call<'a>(&'a self, method: &'a Method, path: &'a str) -> ApiFuture<'a> {
        Box::pin(self.route(method, path))
    }
}
