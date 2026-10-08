//! The node's admin API: health and bucket status, served under `/v1/` by
//! the admin listener, behind its bearer token (design §12; plan section
//! 14 assigns the surface to M1-13).
//!
//! | Path | Answer |
//! |---|---|
//! | `GET /v1/health` | The node and cluster IDs, readiness, whether the control store answers or the node serves its local copy, and each disk's state. |
//! | `GET /v1/buckets` | The status of every bucket. |
//! | `GET /v1/buckets/<name>` | The status of one bucket, or 404. |
//! | `GET /v1/buckets/<name>/conflicts` | The keys of the bucket that this node's flushers hold in conflict (§7.2). |
//! | `POST /v1/buckets/<name>/conflicts/<policy>/<key>` | Resolves the conflict `key` is held in under `policy` (M4-06, below). |
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
//!   started, and `error`, its last error until it gets past it.
//!
//! Once the node runs the coordinator, `health` gains a `placement` object
//! from `skys3_coord::PlacementHealth` (design §12), and bucket status the
//! shards' members (M3-04).
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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use serde_json::{Value, json};
use skys3_config::ConflictPolicy;
use skys3_flush::{BucketStatus, FlushService, ProbeStatus, Unresolved};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_index::ImportCheckpoint;
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::SegmentLog;
use skys3_obs::{AdminApi, ApiFuture, Health};
use skys3_remote::ObjectStore;
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
        if let Some(flush) = self.flush.status(&bucket.bucket_id) {
            status["flush"] = flush_status(&flush);
        }
        status
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
        let known = route == "health" || route == "buckets" || route.starts_with("buckets/");
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
        ProbeStatus::Running { error } => ("running", error.clone(), Vec::new(), false),
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
}
