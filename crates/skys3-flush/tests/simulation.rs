//! Simulation scenarios for the flusher, run by CI's simulation job with a
//! larger seed set.
//!
//! Each seed runs one node whose shard takes a random sequence of PUTs,
//! streamed PUTs (an `UPLOAD_BEGIN`, the body's extents announced as the
//! gateway does, sometimes a write of the key while the body streams, then
//! the `PUT` that inherits the identity, or nothing if the upload fails),
//! completed multipart uploads, tag changes, and DELETEs on a few keys
//! while its flusher sends them to a simulated remote that delays
//! requests, loses requests and responses, and answers `500` and `503
//! SlowDown`. On most seeds the remote behaves like AWS S3 (every
//! precondition honored, versioned) and another writer sometimes writes
//! keys there out of band; on the rest it behaves like Cloudflare R2
//! (unconditional completions and deletes) with no other writer. The
//! flusher is sometimes stopped mid-flight and started again, which loses
//! what it kept in memory, as a restart does. Once the writes stop and the
//! remote heals, the checks are:
//!
//! - every key no other writer touched holds the latest acknowledged write
//!   at the remote, with that write's identity (an `MPU_CREATE`'s for a
//!   multipart object, an `UPLOAD_BEGIN`'s for a streamed PUT) and its
//!   local ETag (a multipart ETag for a multipart object), or for a
//!   streamed PUT sent through its stream the multipart ETag of its body in
//!   parts of `flush_part_bytes`, or is absent there after a delete, and is
//!   clean in the index, whose `remote_etag` is the remote object's ETag:
//!   lost responses never became conflicts, so they were recognized by the
//!   write identity;
//! - no remote object carries a streamed PUT's identity before its `PUT`
//!   commits;
//! - every key another writer touched is either flushed the same way or
//!   held in conflict, and in its remote history no write of the flusher
//!   follows another writer's: out-of-band writes are never overwritten,
//!   also those that upload a streamed PUT's very bytes, whose ETag is then
//!   its local MD5 one;
//! - no remote object, or version of one, carries the identity of a
//!   streamed PUT that failed;
//! - once the target has aborted the uploads flushes left to it, the only
//!   remote multipart uploads still open are those whose
//!   `CreateMultipartUpload` answer never reached the flusher: every other
//!   upload was completed or aborted.
//!
//! The read-through fill and `ADOPT` scenario is in `fill_simulation`, the
//! namespace import racing client writes in `import_simulation`,
//! streaming multipart flush in `stream_simulation`, and primary changes
//! at each step of a streamed upload in `takeover_simulation`.

mod concurrency_simulation;
mod fill_simulation;
mod import_simulation;
mod stream_simulation;
mod support;
mod takeover_simulation;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::{Phase, ShardFlusher};
use skys3_remote::{GetObject, ObjectStore, PutObject, UserMetadata};
use skys3_sim::s3::{Conditionals, Fault, Operation, SimS3Config, SimS3Faults};
use skys3_sim::{Runner, SimContext, SimS3};
use skys3_types::ETag;
use support::{
    Node, Patience, body_target, identity, md5_etag, runtime, streamed_etag, target_with, writes,
};

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(40),
    internal_error_probability: 0.05,
    slow_down_probability: 0.05,
    lost_request_probability: 0.04,
    lost_response_probability: 0.08,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

const KEYS: u32 = 6;

/// The part size a streamed PUT is sent in, small enough that a body has
/// several parts.
const PART_BYTES: u64 = 16;

/// How long a streamed body may go unannounced before its `PUT` is given
/// up on, which ends the streams of failed ones.
const BODY_TIMEOUT: Duration = Duration::from_secs(2);

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

#[test]
fn flushes_reach_the_remote_through_faults() {
    Runner::new().run(scenario);
}

#[test]
fn streamed_uploads_reach_the_remote_only_after_their_local_commit() {
    Runner::with_cost(8, 2).run(stream_simulation::scenario);
}

#[test]
fn primary_changes_at_every_step_of_a_streamed_upload_leave_no_partial_object() {
    Runner::with_cost(8, 4).run(takeover_simulation::scenario);
}

#[test]
fn flush_concurrency_approaches_the_bandwidth_delay_product() {
    Runner::with_cost(5, 8).run(concurrency_simulation::reaches_the_bandwidth_delay_product);
}

#[test]
fn flush_concurrency_backs_off_under_a_rate_limit_and_recovers() {
    Runner::with_cost(2, 8).run(concurrency_simulation::backs_off_under_a_rate_limit);
}

#[test]
fn flush_concurrency_scenarios_catch_seeded_bugs() {
    concurrency_simulation::catch_seeded_bugs();
}

#[test]
fn fills_adopt_out_of_band_writes_but_never_over_a_local_one() {
    Runner::new().run(fill_simulation::scenario);
}

#[test]
fn imports_never_resurrect_a_delete_or_overwrite_a_local_write() {
    Runner::new().run(import_simulation::scenario);
}

fn scenario(context: &mut SimContext) -> Outcome {
    let seed = context.fork_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let like_r2 = rng.random_ratio(1, 4);
    let config = SimS3Config {
        versioning: !like_r2,
        conditionals: if like_r2 {
            Conditionals::R2
        } else {
            Conditionals::AWS_S3
        },
        // Small parts keep multipart objects small.
        min_part_size: 1,
        ..SimS3Config::default()
    };
    let store = context.s3(config);
    store.set_faults(FAULTS);
    // Multipart uploads and large single PUTs stream to the remote on half
    // of the seeds, and are sent after they commit on the others (§7.3,
    // §7.4).
    let writes = writes(true, !like_r2, !like_r2);
    let target = if rng.random_ratio(1, 2) {
        body_target(&store, writes, PART_BYTES, BODY_TIMEOUT)
    } else {
        target_with(&store, writes)
    };
    runtime().block_on(async move {
        let node = Node::open_inline(seed).await;
        let mut flusher = node.flusher(&target);
        // The latest acknowledged state of each key, or `None` after a
        // delete.
        let mut latest: BTreeMap<String, Option<Version>> = BTreeMap::new();
        let mut touched = BTreeSet::new();
        // The identities of the streamed PUTs that failed.
        let mut failed = BTreeSet::new();
        for op in 0..rng.random_range(40..120) {
            let key = format!("key-{}", rng.random_range(0..KEYS));
            match rng.random_range(0..100) {
                0..=36 => {
                    let body = format!("{key} v{op}");
                    let seq = node.put(&key, &body).await;
                    latest.insert(key, Some(Version::put(body, seq, seq)));
                }
                // A streamed PUT. A write of the key that commits while its
                // body streams has a newer identity but an older version,
                // which a failed precondition's HEAD must tell apart (§7.2).
                // Its gateway announces the body's extents in two batches,
                // which stream to the remote, if the flusher streams, while
                // the body arrives (§7.3).
                37..=44 => {
                    let begun = node.begin(&key).await;
                    let filler = "x".repeat(rng.random_range(0..60));
                    let body = format!("{key} v{op} streamed {filler}");
                    let extents = node.extents(&key, &body, rng.random_range(4..=20)).await;
                    let split = rng.random_range(0..=extents.len());
                    node.announce(&key, begun, &extents[..split]);
                    if rng.random_ratio(1, 3) {
                        let body = format!("{key} v{op} meanwhile");
                        let seq = node.put(&key, &body).await;
                        latest.insert(key.clone(), Some(Version::put(body, seq, seq)));
                    }
                    tokio::time::sleep(Duration::from_millis(rng.random_range(0..30))).await;
                    node.announce(&key, begun, &extents[split..]);
                    tokio::time::sleep(Duration::from_millis(rng.random_range(0..30))).await;
                    unpublished_yet(&store, &key, begun)?;
                    if rng.random_ratio(1, 4) {
                        failed.insert(identity(begun));
                    } else {
                        let seq = node.complete_extents(&key, &body, begun, &extents).await;
                        latest.insert(key, Some(Version::streamed(body, seq, begun)));
                    }
                }
                45..=56 => {
                    let parts: Vec<String> = (0..rng.random_range(1..=3))
                        .map(|part| format!("{key} v{op} part {part}; "))
                        .collect();
                    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
                    // Lose the answer to a completion now and then, and
                    // fail the abort that would find the upload gone, so
                    // that the next attempt must recognize the object by
                    // its identity (§7.2).
                    if rng.random_ratio(1, 3) {
                        store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
                        if rng.random_bool(0.5) {
                            store.inject(Operation::AbortMultipartUpload, Fault::InternalError);
                        }
                    }
                    let object = node.multipart(&key, &parts).await;
                    let version = Version {
                        body: parts.concat(),
                        seq: object.complete,
                        identity: object.upload,
                        etag: object.etag,
                        streamed: None,
                    };
                    latest.insert(key, Some(version));
                }
                // A tag change of a live object: the same bytes, flushed
                // with the identity of the `TAGS` record.
                57..=61 => {
                    if let Some(Some(version)) = latest.get_mut(&key) {
                        version.seq = node.tag(&key, &[("op", &op.to_string())]).await;
                        version.identity = version.seq;
                        version.streamed = None;
                    }
                }
                62..=84 => {
                    node.delete(&key).await;
                    latest.insert(key, None);
                }
                // Another writer's object, sometimes with the very bytes of
                // the key's streamed PUT once the remote holds it from its
                // stream: its ETag is then that PUT's local one, which no
                // flush may take for the remote's. (A copy of an object sent
                // with `PutObject` has the same ETag as the object, so no
                // precondition can tell them apart.)
                85..=91 if !like_r2 => {
                    let at_remote = store.object(&key).map(|object| object.info.etag);
                    let copied = match latest.get(&key) {
                        Some(Some(version))
                            if version.streamed.is_some() && version.streamed == at_remote =>
                        {
                            rng.random_bool(0.5).then(|| version.body.clone())
                        }
                        _ => None,
                    };
                    let body = copied.unwrap_or_else(|| format!("theirs {op}"));
                    out_of_band(&store, &key, &body).await;
                    touched.insert(key);
                }
                92..=95 => {
                    flusher.stop().await;
                    flusher = node.flusher(&target);
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(rng.random_range(0..30))).await;
        }
        store.set_faults(SimS3Faults::NONE);
        node.settle(&flusher).await;
        check(&node, &flusher, &store, &latest, &touched).await?;
        check_unpublished(&store, &failed).await?;
        // Every upload the flusher knew of was completed or aborted.
        // Scripted faults may still fail a few aborts, and the streams of
        // uploads end on their own (§7.3).
        let patience = Patience::new();
        loop {
            target.abort_orphaned_uploads().await;
            let open = store.uploads().len() as u64;
            let unknown = store.unanswered(Operation::CreateMultipartUpload);
            let orphans = target.orphaned_uploads();
            let streams = flusher.status().streams;
            if open == unknown && orphans == 0 && streams == 0 {
                return Ok(());
            }
            if patience.is_exhausted() {
                return Err(format!(
                    "{open} remote uploads are open, {unknown} of them never known to the \
                     flusher, the target still holds {orphans}, and {streams} streams are open"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// A key's latest acknowledged version.
struct Version {
    /// Its bytes.
    body: String,
    /// Its `seq`.
    seq: u64,
    /// The `seq` its write identity names.
    identity: u64,
    /// Its local ETag.
    etag: ETag,
    /// For a streamed PUT, its remote ETag if it is sent through its
    /// stream: that of its body in parts of [`PART_BYTES`]. Sent after its
    /// commit instead, it has its local one.
    streamed: Option<ETag>,
}

impl Version {
    /// A single PUT of `body` at `seq`, whose identity names `identity`.
    fn put(body: String, seq: u64, identity: u64) -> Self {
        let etag = md5_etag(body.as_bytes());
        Self {
            body,
            seq,
            identity,
            etag,
            streamed: None,
        }
    }

    /// The streamed PUT of `body` at `seq` begun at `begun`.
    fn streamed(body: String, seq: u64, begun: u64) -> Self {
        let streamed = streamed_etag(&body, PART_BYTES as usize);
        Self {
            streamed: Some(streamed),
            ..Self::put(body, seq, begun)
        }
    }

    /// Whether `etag` is a remote ETag the version may have.
    fn has_remote_etag(&self, etag: &ETag) -> bool {
        *etag == self.etag || self.streamed.as_ref() == Some(etag)
    }
}

/// Checks that no remote object carries the identity of the streamed PUT
/// of `key` begun at `begun`, whose `PUT` has not committed (§7.3).
fn unpublished_yet(store: &SimS3, key: &str, begun: u64) -> Outcome {
    let wid = identity(begun);
    let published = store
        .keys()
        .iter()
        .filter_map(|key| store.object(key))
        .any(|object| object.info.metadata.write_identity() == Some(wid.as_str()));
    if published {
        return Err(format!("{key}: {wid} is at the remote before its PUT commits").into());
    }
    Ok(())
}

/// Writes `body` to `key` as another writer would, trying again while the
/// remote fails. Its retries may apply more than once, as any writer's.
async fn out_of_band(store: &SimS3, key: &str, body: &str) {
    let mut metadata = UserMetadata::new();
    metadata.insert("writer", "someone-else").unwrap();
    let request = PutObject::new(key, body.to_owned()).with_metadata(metadata);
    for _ in 0..20 {
        if store.put_object(request.clone()).await.is_ok() {
            return;
        }
    }
}

async fn check(
    node: &Node,
    flusher: &ShardFlusher,
    store: &SimS3,
    latest: &BTreeMap<String, Option<Version>>,
    touched: &BTreeSet<String>,
) -> Outcome {
    for (key, state) in latest {
        let conflicted = matches!(flusher.phase(key), Some(Phase::Conflict(_)));
        if conflicted {
            if !touched.contains(key) {
                return Err(format!("{key} is in conflict, but no one else wrote it").into());
            }
            continue;
        }
        let remote = store.object(key).map(|object| {
            let body = String::from_utf8(object.body.to_vec()).unwrap_or_default();
            (
                body,
                object.info.metadata.write_identity().map(str::to_owned),
                object.info.etag,
            )
        });
        // Another writer's object may follow the last flush; it is found
        // when the key is next flushed or filled.
        let foreign =
            touched.contains(key) && remote.as_ref().is_some_and(|(_, wid, _)| wid.is_none());
        // A streamed PUT's remote ETag is its local one if it was sent
        // after its commit, and a multipart ETag if it went through its
        // stream.
        let expected = state.as_ref().map(|version| {
            let etag = match &remote {
                Some((_, _, etag)) if version.has_remote_etag(etag) => etag.clone(),
                _ => version.etag.clone(),
            };
            (version.body.clone(), Some(identity(version.identity)), etag)
        });
        let entry = node.entry(key).await;
        let clean = match (&entry, state) {
            (None, None) => true,
            (Some(entry), Some(version)) => {
                entry.version.seq.get() == version.seq
                    && entry.state == skys3_index::EntryState::Clean
                    && entry
                        .remote_etag
                        .as_ref()
                        .is_some_and(|etag| version.has_remote_etag(etag))
                    // The index records the ETag the remote returned, which
                    // later flushes are conditioned on (§7.4).
                    && (foreign || entry.remote_etag == remote.as_ref().map(|r| r.2.clone()))
            }
            _ => false,
        };
        if !clean {
            return Err(format!(
                "{key}: the index holds {entry:?} after the flush, and the remote {remote:?}"
            )
            .into());
        }
        if remote != expected && !foreign {
            return Err(format!("{key}: the remote holds {remote:?}, not {expected:?}").into());
        }
    }
    if store.config().versioning {
        for key in touched {
            check_history(store, key).await?;
        }
    }
    Ok(())
}

/// Checks that no write of the flusher follows another writer's in `key`'s
/// version history.
async fn check_history(store: &SimS3, key: &str) -> Outcome {
    let mut foreign = false;
    for (_, version, delete_marker) in store.versions().into_iter().filter(|v| v.0 == key) {
        if delete_marker {
            if foreign {
                return Err(format!("{key}: the flusher deleted another writer's object").into());
            }
            continue;
        }
        let mut request = GetObject::new(key);
        if let Some(version) = version {
            request = request.with_version_id(version);
        }
        let object = store.get_object(request).await?;
        let ours = object.info.metadata.write_identity().is_some();
        if ours && foreign {
            return Err(format!("{key}: the flusher overwrote another writer's object").into());
        }
        foreign |= !ours;
    }
    Ok(())
}

/// Checks that no remote object, nor any version of one, carries one of
/// the `failed` identities: a streamed PUT that fails publishes none.
async fn check_unpublished(store: &SimS3, failed: &BTreeSet<String>) -> Outcome {
    for (key, version, delete_marker) in store.versions() {
        if delete_marker {
            continue;
        }
        let mut request = GetObject::new(&key);
        if let Some(version) = version {
            request = request.with_version_id(version);
        }
        let object = store.get_object(request).await?;
        if let Some(wid) = object.info.metadata.write_identity()
            && failed.contains(wid)
        {
            return Err(
                format!("{key}: a failed streamed PUT's identity {wid} was published").into(),
            );
        }
    }
    Ok(())
}
