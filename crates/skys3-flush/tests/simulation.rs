//! Simulation scenarios for the flusher, run by CI's simulation job with a
//! larger seed set.
//!
//! Each seed runs one node whose shard takes a random sequence of PUTs and
//! DELETEs on a few keys while its flusher sends them to a simulated
//! remote that delays requests, loses requests and responses, and answers
//! `500` and `503 SlowDown`. On most seeds the remote behaves like AWS S3
//! (every precondition honored, versioned) and another writer sometimes
//! writes keys there out of band; on the rest it behaves like Cloudflare R2
//! (unconditional deletes) with no other writer. The flusher is sometimes
//! stopped mid-flight and started again, which loses what it kept in
//! memory, as a restart does. Once the writes stop and the remote heals,
//! the checks are:
//!
//! - every key no other writer touched holds the latest acknowledged write
//!   at the remote, with that write's identity, or is absent there after a
//!   delete, and is clean in the index: lost responses never became
//!   conflicts, so they were recognized by the write identity;
//! - every key another writer touched is either flushed the same way or
//!   held in conflict, and in its remote history no write of the flusher
//!   follows another writer's: out-of-band writes are never overwritten.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::{Phase, ShardFlusher};
use skys3_remote::{GetObject, ObjectStore, PutObject, UserMetadata};
use skys3_sim::s3::{Conditionals, SimS3Config, SimS3Faults};
use skys3_sim::{Runner, SimContext, SimS3};
use support::{Node, identity, runtime, target_with, writes};

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(40),
    internal_error_probability: 0.05,
    slow_down_probability: 0.05,
    lost_request_probability: 0.04,
    lost_response_probability: 0.08,
    stale_read_probability: 0.0,
};

const KEYS: u32 = 6;

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

#[test]
fn flushes_reach_the_remote_through_faults() {
    Runner::new().run(scenario);
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
        ..SimS3Config::default()
    };
    let store = context.s3(config);
    store.set_faults(FAULTS);
    let target = target_with(&store, writes(true, !like_r2, !like_r2));
    runtime().block_on(async move {
        let node = Node::open(seed).await;
        let mut flusher = node.flusher(&target);
        // The latest acknowledged state of each key: its body and `seq`, or
        // `None` after a delete.
        let mut latest: BTreeMap<String, Option<(String, u64)>> = BTreeMap::new();
        let mut touched = BTreeSet::new();
        for op in 0..rng.random_range(40..120) {
            let key = format!("key-{}", rng.random_range(0..KEYS));
            match rng.random_range(0..100) {
                0..=59 => {
                    let body = format!("{key} v{op}");
                    let seq = node.put(&key, &body).await;
                    latest.insert(key, Some((body, seq)));
                }
                60..=84 => {
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
        check(&node, &flusher, &store, &latest, &touched).await
    })
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
    latest: &BTreeMap<String, Option<(String, u64)>>,
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
            )
        });
        let expected = state
            .as_ref()
            .map(|(body, seq)| (body.clone(), Some(identity(*seq))));
        let entry = node.entry(key).await;
        let clean = match (&entry, state) {
            (None, None) => true,
            (Some(entry), Some((_, seq))) => {
                entry.version.seq.get() == *seq && entry.state == skys3_index::EntryState::Clean
            }
            _ => false,
        };
        if !clean {
            return Err(format!("{key}: the index holds {entry:?} after the flush").into());
        }
        // Another writer's object may follow the last flush; it is found
        // when the key is next flushed or filled.
        let foreign =
            touched.contains(key) && remote.as_ref().is_some_and(|(_, wid)| wid.is_none());
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
