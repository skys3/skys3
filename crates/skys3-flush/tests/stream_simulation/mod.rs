//! The simulation scenario of streaming multipart flush (§7.3).
//!
//! Each seed runs one node whose shard takes multipart uploads of a few
//! keys that interleave: uploads open, parts arrive one at a time, some
//! numbers are uploaded again, and each upload is completed with some of
//! its parts, all of them, or is aborted. PUTs, tag changes, and DELETEs of
//! the same keys come in between. The flusher streams the parts to a
//! simulated remote that delays requests, loses requests and responses,
//! and answers `500` and `503 SlowDown`, and like AWS S3 or Cloudflare R2;
//! it is sometimes stopped mid-flight and started again, which loses what
//! it kept in memory but not what its `PART_FLUSHED` records hold. The
//! checks are:
//!
//! - just before each local completion commits, no remote object, nor any
//!   version of one, carries the upload's write identity: no remote object
//!   appears before the local commit;
//! - once the writes stop and the flush settles, every key holds its
//!   latest write at the remote with that write's identity and local
//!   ETag: a multipart object's remote ETag is its local one, so its
//!   remote upload listed exactly the parts its completion kept;
//! - no remote object or version ever carries the identity of an aborted
//!   upload;
//! - every remote upload was completed or aborted, but those whose
//!   `CreateMultipartUpload` answer never reached the flusher, and the
//!   index keeps no remote upload.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use skys3_index::EntryState;
use skys3_remote::GetObject;
use skys3_remote::ObjectStore;
use skys3_sim::s3::{Conditionals, Operation, SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use skys3_types::{ETag, EpochSeq};

use crate::support::{Node, Patience, identity, md5_etag, runtime, streaming_target, writes};

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(30),
    internal_error_probability: 0.05,
    slow_down_probability: 0.03,
    lost_request_probability: 0.03,
    lost_response_probability: 0.06,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

const KEYS: u32 = 3;

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

/// A local upload that is open: each part number's latest position and
/// body.
struct Open {
    key: String,
    parts: BTreeMap<u16, (EpochSeq, String)>,
}

/// A key's latest write: its body, the `seq` its identity names, and its
/// local ETag; `None` after a delete.
type Latest = Option<(String, u64, ETag)>;

pub fn scenario(context: &mut SimContext) -> Outcome {
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
    let target = streaming_target(&store, writes(true, !like_r2, !like_r2));
    runtime().block_on(async move {
        let node = Node::open(seed).await;
        let mut flusher = node.flusher(&target);
        let mut latest: BTreeMap<String, Latest> = BTreeMap::new();
        let mut open: BTreeMap<EpochSeq, Open> = BTreeMap::new();
        let mut aborted = BTreeSet::new();
        for op in 0..rng.random_range(40..120) {
            let key = format!("key-{}", rng.random_range(0..KEYS));
            match rng.random_range(0..100) {
                0..=14 => {
                    let upload = node.create(&key).await;
                    let parts = BTreeMap::new();
                    open.insert(upload, Open { key, parts });
                }
                15..=49 => {
                    let Some((upload, state)) = open.iter_mut().choose(&mut rng) else {
                        continue;
                    };
                    let number = rng.random_range(1..=4);
                    let body = format!("{} part {number} op {op}; ", state.key);
                    let position = node.part(&state.key, *upload, number, &body).await;
                    state.parts.insert(number, (position, body));
                }
                50..=61 => {
                    let Some(&upload) = open.keys().choose(&mut rng) else {
                        continue;
                    };
                    let Some(Open { key, parts }) = open.remove(&upload) else {
                        continue;
                    };
                    // Some parts are left out, but never all.
                    let mut kept: Vec<_> = parts
                        .iter()
                        .filter(|_| rng.random_ratio(4, 5))
                        .map(|(n, (position, body))| (*n, *position, body.as_str()))
                        .collect();
                    if kept.is_empty() {
                        let Some((n, (position, body))) = parts.iter().next() else {
                            node.abort(&key, upload).await;
                            aborted.insert(identity(upload.seq.get()));
                            continue;
                        };
                        kept.push((*n, *position, body.as_str()));
                    }
                    check_unpublished(
                        &store,
                        &[identity(upload.seq.get())].into(),
                        "before its local commit",
                    )
                    .await?;
                    let object = node.finish(&key, upload, &kept).await;
                    let body: String = kept.iter().map(|(_, _, body)| *body).collect();
                    latest.insert(key, Some((body, object.upload, object.etag)));
                }
                62..=69 => {
                    let Some(&upload) = open.keys().choose(&mut rng) else {
                        continue;
                    };
                    if let Some(state) = open.remove(&upload) {
                        node.abort(&state.key, upload).await;
                        aborted.insert(identity(upload.seq.get()));
                    }
                }
                70..=79 => {
                    let body = format!("{key} put {op}");
                    let seq = node.put(&key, &body).await;
                    let etag = md5_etag(body.as_bytes());
                    latest.insert(key, Some((body, seq, etag)));
                }
                80..=84 => {
                    if let Some(Some((_, seq, _))) = latest.get_mut(&key) {
                        *seq = node.tag(&key, &[("op", &op.to_string())]).await;
                    }
                }
                85..=91 => {
                    node.delete(&key).await;
                    latest.insert(key, None);
                }
                92..=95 => {
                    flusher.stop().await;
                    flusher = node.flusher(&target);
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(rng.random_range(0..30))).await;
        }
        for (upload, state) in std::mem::take(&mut open) {
            node.abort(&state.key, upload).await;
            aborted.insert(identity(upload.seq.get()));
        }
        store.set_faults(SimS3Faults::NONE);
        node.settle(&flusher).await;
        check_latest(&node, &store, &latest).await?;
        check_unpublished(&store, &aborted, "though it was aborted").await?;
        // Every remote upload the flusher knew of is completed or aborted,
        // and the end of each stream recorded. Uploads of flushes after
        // commit that a stopped flusher left open wait for the target to
        // abort them.
        let patience = Patience::new();
        loop {
            target.abort_orphaned_uploads().await;
            let open = store.uploads().len() as u64;
            let unknown = store.unanswered(Operation::CreateMultipartUpload);
            let recorded = node.shard.remote_uploads().await?;
            let orphans = target.orphaned_uploads();
            let streams = flusher.status().streams;
            if open == unknown && orphans == 0 && streams == 0 && recorded.is_empty() {
                return Ok(());
            }
            if patience.is_exhausted() {
                return Err(format!(
                    "{open} remote uploads are open, {unknown} of them never known to the \
                     flusher, the target holds {orphans}, {streams} streams are open, and \
                     the index records {recorded:?}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// Checks that every key holds its latest write at the remote, or nothing
/// after a delete, and is clean in the index.
async fn check_latest(node: &Node, store: &SimS3, latest: &BTreeMap<String, Latest>) -> Outcome {
    for (key, state) in latest {
        let remote = store.object(key).map(|object| {
            let body = String::from_utf8(object.body.to_vec()).unwrap_or_default();
            let wid = object.info.metadata.write_identity().map(str::to_owned);
            (body, wid, object.info.etag)
        });
        let expected = state
            .as_ref()
            .map(|(body, seq, etag)| (body.clone(), Some(identity(*seq)), etag.clone()));
        if remote != expected {
            return Err(format!("{key}: the remote holds {remote:?}, not {expected:?}").into());
        }
        let entry = node.entry(key).await;
        if entry
            .as_ref()
            .is_some_and(|entry| entry.state != EntryState::Clean)
        {
            return Err(format!("{key}: the index holds {entry:?} after the flush").into());
        }
    }
    Ok(())
}

/// Checks that no remote object, nor any version of one, carries one of
/// the `identities`.
async fn check_unpublished(store: &SimS3, identities: &BTreeSet<String>, why: &str) -> Outcome {
    for (key, version, delete_marker) in store.versions() {
        if delete_marker {
            continue;
        }
        let mut request = GetObject::new(&key);
        if let Some(version) = version {
            request = request.with_version_id(version);
        }
        // Faults may be on: read until the store answers. A flush may
        // delete the object meanwhile.
        let object = loop {
            match store.get_object(request.clone()).await {
                Ok(object) => break Some(object),
                Err(error) if error.kind().status() == Some(404) => break None,
                Err(_) => {}
            }
        };
        if let Some(object) = object
            && let Some(wid) = object.info.metadata.write_identity()
            && identities.contains(wid)
        {
            return Err(format!("{key}: upload {wid} is at the remote {why}").into());
        }
    }
    Ok(())
}
