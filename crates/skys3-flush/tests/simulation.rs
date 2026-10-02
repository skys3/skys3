//! Simulation scenarios for the flusher, run by CI's simulation job with a
//! larger seed set.
//!
//! Each seed runs one node whose shard takes a random sequence of PUTs,
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
//!   multipart object) and its local ETag (a multipart ETag for a
//!   multipart object), or is absent there after a delete, and is clean in
//!   the index: lost responses never became conflicts, so they were
//!   recognized by the write identity;
//! - every key another writer touched is either flushed the same way or
//!   held in conflict, and in its remote history no write of the flusher
//!   follows another writer's: out-of-band writes are never overwritten;
//! - once the target has aborted the uploads flushes left to it, the only
//!   remote multipart uploads still open are those whose
//!   `CreateMultipartUpload` answer never reached the flusher: every other
//!   upload was completed or aborted.
//!
//! The read-through fill and `ADOPT` scenario is in `fill_simulation`, and
//! the namespace import racing client writes in `import_simulation`.

mod fill_simulation;
mod import_simulation;
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::{Phase, ShardFlusher};
use skys3_remote::{GetObject, ObjectStore, PutObject, UserMetadata};
use skys3_sim::s3::{Conditionals, Fault, Operation, SimS3Config, SimS3Faults};
use skys3_sim::{Runner, SimContext, SimS3};
use skys3_types::ETag;
use support::{Node, identity, md5_etag, runtime, target_with, writes};

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

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

#[test]
fn flushes_reach_the_remote_through_faults() {
    Runner::new().run(scenario);
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
    let target = target_with(&store, writes(true, !like_r2, !like_r2));
    runtime().block_on(async move {
        let node = Node::open(seed).await;
        let mut flusher = node.flusher(&target);
        // The latest acknowledged state of each key, or `None` after a
        // delete.
        let mut latest: BTreeMap<String, Option<Version>> = BTreeMap::new();
        let mut touched = BTreeSet::new();
        for op in 0..rng.random_range(40..120) {
            let key = format!("key-{}", rng.random_range(0..KEYS));
            match rng.random_range(0..100) {
                0..=44 => {
                    let body = format!("{key} v{op}");
                    let seq = node.put(&key, &body).await;
                    let etag = md5_etag(body.as_bytes());
                    let version = Version {
                        body,
                        seq,
                        identity: seq,
                        etag,
                    };
                    latest.insert(key, Some(version));
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
                    };
                    latest.insert(key, Some(version));
                }
                // A tag change of a live object: the same bytes, flushed
                // with the identity of the `TAGS` record.
                57..=61 => {
                    if let Some(Some(version)) = latest.get_mut(&key) {
                        version.seq = node.tag(&key, &[("op", &op.to_string())]).await;
                        version.identity = version.seq;
                    }
                }
                62..=84 => {
                    node.delete(&key).await;
                    latest.insert(key, None);
                }
                85..=91 if !like_r2 => {
                    out_of_band(&store, &key, &format!("theirs {op}")).await;
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
        // Scripted faults may still fail a few aborts.
        for _ in 0..10 {
            target.abort_orphaned_uploads().await;
        }
        // Every upload the flusher knew of was completed or aborted.
        let open = store.uploads().len() as u64;
        let unknown = store.unanswered(Operation::CreateMultipartUpload);
        if open != unknown || target.orphaned_uploads() != 0 {
            return Err(format!(
                "{open} remote uploads are open, {unknown} of them never known to the flusher, \
                 and the target still holds {}",
                target.orphaned_uploads()
            )
            .into());
        }
        Ok(())
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
        let expected = state.as_ref().map(|version| {
            (
                version.body.clone(),
                Some(identity(version.identity)),
                version.etag.clone(),
            )
        });
        let entry = node.entry(key).await;
        let clean = match (&entry, state) {
            (None, None) => true,
            (Some(entry), Some(version)) => {
                entry.version.seq.get() == version.seq
                    && entry.state == skys3_index::EntryState::Clean
                    && entry.remote_etag.as_ref() == Some(&version.etag)
            }
            _ => false,
        };
        if !clean {
            return Err(format!("{key}: the index holds {entry:?} after the flush").into());
        }
        // Another writer's object may follow the last flush; it is found
        // when the key is next flushed or filled.
        let foreign =
            touched.contains(key) && remote.as_ref().is_some_and(|(_, wid, _)| wid.is_none());
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
