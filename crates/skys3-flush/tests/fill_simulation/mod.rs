//! The simulation scenario of read-through fill and `ADOPT` (§9.2).
//!
//! Each seed runs one node whose shard takes local PUTs of a few keys while
//! its flusher sends them to a simulated remote that delays requests,
//! loses requests and responses, and answers `500` and `503 SlowDown`.
//! Clean keys are evicted now and then, another writer overwrites keys at
//! the remote, and readers, some at once, read keys as the gateway does:
//! local bytes if the entry has them, otherwise through a fill, resolving
//! the key again after the fill adopted the remote's version. On a
//! versioned remote, the other writer also expires noncurrent versions, as
//! a lifecycle rule would, so that a fill finds the version it names gone.
//! The checks are:
//!
//! - every read returns the bytes of the version it resolved: their MD5 is
//!   that version's ETag;
//! - an `ADOPT` never replaced a local write that had not reached the
//!   remote: whenever a key's latest local write is not its version, the
//!   remote held that write, with its identity, at some point;
//! - a key held in conflict was written by the other writer, and still
//!   serves its local write;
//! - once the writes stop and the flush settles, the other writer
//!   overwrites every clean key once more and expires the version SkyS3
//!   wrote: after eviction, every read of it switches to the remote's
//!   version, through an `ADOPT`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use md5::{Digest, Md5};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::{Counters, FillError, Filler, FlushSettings, Phase, ShardFlusher};
use skys3_index::{Entry, EntryState, Payload};
use skys3_io::SystemWallClock;
use skys3_remote::{DeleteObject, GetObject, ObjectStore, PutObject, UserMetadata, VersionId};
use skys3_sim::s3::{Fault, Operation, SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use tokio::task::JoinSet;

use crate::support::{Node, identity, runtime, settings, target};

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(20),
    internal_error_probability: 0.03,
    slow_down_probability: 0.03,
    lost_request_probability: 0.02,
    lost_response_probability: 0.04,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

const KEYS: u32 = 4;

/// How many times a reader resolves a key again after a fill found it
/// changed, as the gateway does.
const ROUNDS: usize = 4;

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

pub fn scenario(context: &mut SimContext) -> Outcome {
    let seed = context.fork_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let versioning = rng.random_bool(0.5);
    let store = context.s3(SimS3Config {
        versioning,
        ..SimS3Config::default()
    });
    store.set_faults(FAULTS);
    let flush_target = target(&store);
    let filler = Filler::new(
        Arc::new(store.clone()),
        "",
        FlushSettings {
            extent_bytes: 8,
            ..settings()
        },
        Counters::default(),
        Arc::new(SystemWallClock),
    );
    runtime().block_on(async move {
        let node = Arc::new(Node::open(seed).await);
        let flusher = node.flusher(&flush_target);
        let mut world = World::default();
        let mut readers = JoinSet::new();
        for op in 0..rng.random_range(60..150) {
            let key = format!("key-{}", rng.random_range(0..KEYS));
            match rng.random_range(0..100) {
                0..=29 => {
                    let body = format!("{key} local {op}");
                    let seq = node.put(&key, &body).await;
                    world.local.insert(key, seq);
                }
                30..=44 => {
                    if let Some(entry) = node.entry(&key).await
                        && entry.state == EntryState::Clean
                    {
                        // A write may have come first; then nothing changes.
                        let _ = node.shard.evict(&key, entry.version).await;
                    }
                }
                45..=56 => {
                    out_of_band(&store, &key, &format!("{key} theirs {op}")).await;
                    world.theirs.insert(key);
                }
                57..=61 if versioning => {
                    world.remember(&store, &key).await;
                    expire(&store, &key).await;
                }
                62..=66 => race(&node, &filler, &store, &key, op, &mut world).await?,
                67..=79 => {
                    let (node, filler) = (Arc::clone(&node), filler.clone());
                    readers.spawn(async move { read(&node, &filler, &key).await });
                }
                _ => {
                    read(&node, &filler, &key).await?;
                }
            }
            tokio::time::sleep(Duration::from_millis(rng.random_range(0..20))).await;
        }
        while let Some(read) = readers.join_next().await {
            read.expect("a reader panicked")?;
        }
        store.set_faults(SimS3Faults::NONE);
        node.settle(&flusher).await;
        check(&node, &flusher, &store, &mut world).await?;
        switch(&node, &filler, &store, versioning).await?;
        flusher.stop().await;
        Ok(())
    })
}

/// What the scenario did, for its checks.
#[derive(Default)]
struct World {
    /// Each key's latest acknowledged local write, by `seq`.
    local: BTreeMap<String, u64>,
    /// Keys the other writer wrote.
    theirs: BTreeSet<String>,
    /// The write identities of versions the other writer expired.
    expired: BTreeSet<String>,
}

impl World {
    /// Remembers the identities of `key`'s versions before they expire.
    async fn remember(&mut self, store: &SimS3, key: &str) {
        for (version, _) in versions(store, key) {
            if let Some(identity) = identity_of(store, key, version).await {
                self.expired.insert(identity);
            }
        }
    }
}

/// Reads `key` as the gateway does, and checks that the bytes are those of
/// the version it resolved. Returns them with that version's entry, or
/// `None` if the key has no object or a fill failed under the faults.
async fn read(
    node: &Node,
    filler: &Filler<SimS3>,
    key: &str,
) -> Result<Option<(Entry, Vec<u8>)>, String> {
    for _ in 0..ROUNDS {
        let Some(entry) = node.entry(key).await else {
            return Ok(None);
        };
        let Some(object) = &entry.object else {
            return Ok(None);
        };
        let bytes = match &object.payload {
            Payload::Inline(position) => node
                .shard
                .payload(*position)
                .await
                .map_err(|e| e.to_string())?
                .to_vec(),
            Payload::Extents(extents) => {
                let mut bytes = Vec::new();
                for extent in extents {
                    let data = node.shard.payload(extent.position).await;
                    bytes.extend_from_slice(&data.map_err(|e| e.to_string())?);
                }
                bytes
            }
            Payload::None => {
                match filler
                    .read(&node.shard, key, entry.version, 0..object.size)
                    .await
                {
                    Ok(mut body) => {
                        let mut bytes = Vec::new();
                        while let Some(chunk) = body.recv().await {
                            match chunk {
                                Ok(chunk) => bytes.extend_from_slice(&chunk),
                                // The fill failed midway under the faults.
                                Err(_) => return Ok(None),
                            }
                        }
                        bytes
                    }
                    Err(FillError::Changed) => continue,
                    Err(FillError::Failed(_)) => return Ok(None),
                    Err(error) => return Err(format!("{key}: {error}")),
                }
            }
            Payload::Parts { .. } => return Err(format!("{key} is multipart")),
        };
        let digest = hex(&Md5::digest(&bytes));
        if digest != object.local_etag.as_str() {
            return Err(format!(
                "{key}: a read of {} returned {:?}, whose MD5 is {digest}",
                entry.version,
                String::from_utf8_lossy(&bytes)
            ));
        }
        return Ok(Some((entry, bytes)));
    }
    Ok(None)
}

/// Races a local write of `key` with a fill that finds the remote changed:
/// the other writer overwrites the key (and expires the version SkyS3
/// wrote, on a versioned remote), a reader starts filling it, and a client
/// writes it while the fill's GET is at the remote. Whichever commits
/// first, the `ADOPT` or the write, the key ends at the local write.
async fn race(
    node: &Arc<Node>,
    filler: &Filler<SimS3>,
    store: &SimS3,
    key: &str,
    op: u32,
    world: &mut World,
) -> Outcome {
    if let Some(entry) = node.entry(key).await
        && entry.state == EntryState::Clean
    {
        let _ = node.shard.evict(key, entry.version).await;
    }
    if node
        .entry(key)
        .await
        .is_none_or(|entry| entry.state != EntryState::Evicted)
    {
        return Ok(());
    }
    out_of_band(store, key, &format!("{key} theirs {op}")).await;
    world.theirs.insert(key.to_owned());
    if store.config().versioning {
        world.remember(store, key).await;
        expire(store, key).await;
    }
    store.inject(
        Operation::GetObject,
        Fault::Delay(Duration::from_millis(30)),
    );
    let requests = store.stats().requests;
    let reader = {
        let (node, filler, key) = (Arc::clone(node), filler.clone(), key.to_owned());
        tokio::spawn(async move { read(&node, &filler, &key).await })
    };
    while store.stats().requests == requests && !reader.is_finished() {
        tokio::task::yield_now().await;
    }
    let seq = node.put(key, &format!("{key} local {op}")).await;
    world.local.insert(key.to_owned(), seq);
    reader.await.expect("a reader panicked")?;
    let entry = node.entry(key).await.ok_or(format!("{key} has no entry"))?;
    if entry.version.seq.get() != seq {
        return Err(format!("{key}: {} replaced the local write {seq}", entry.version).into());
    }
    Ok(())
}

/// Checks the index once the flush settled.
async fn check(node: &Node, flusher: &ShardFlusher, store: &SimS3, world: &mut World) -> Outcome {
    for (key, &seq) in &world.local {
        let entry = node.entry(key).await.ok_or(format!("{key} has no entry"))?;
        if matches!(flusher.phase(key), Some(Phase::Conflict(_))) {
            if !world.theirs.contains(key) {
                return Err(format!("{key} is in conflict, but no one else wrote it").into());
            }
            if entry.version.seq.get() != seq {
                return Err(format!("{key}: a conflict lost the local write {seq}").into());
            }
            continue;
        }
        if entry.version.seq.get() == seq {
            continue;
        }
        // The local write was replaced by an adopted version: it must have
        // reached the remote first.
        if entry.version.seq.get() < seq {
            return Err(format!("{key}: the index is at {}, before {seq}", entry.version).into());
        }
        if store.config().versioning {
            for (version, _) in versions(store, key) {
                if let Some(identity) = identity_of(store, key, version).await {
                    world.expired.insert(identity);
                }
            }
            if !world.expired.contains(&identity(seq)) {
                return Err(format!(
                    "{key}: the local write {seq} was replaced by {} without reaching the remote",
                    entry.version
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Overwrites every clean key at the remote once more, expires the version
/// SkyS3 wrote, evicts the key, and checks that a read switches to the
/// remote's version.
async fn switch(node: &Node, filler: &Filler<SimS3>, store: &SimS3, versioning: bool) -> Outcome {
    for n in 0..KEYS {
        let key = format!("key-{n}");
        let Some(entry) = node.entry(&key).await else {
            continue;
        };
        if !matches!(entry.state, EntryState::Clean | EntryState::Evicted) {
            continue;
        }
        let body = format!("{key} theirs at the end");
        out_of_band(store, &key, &body).await;
        if versioning {
            expire(store, &key).await;
        }
        if entry.state == EntryState::Clean {
            let _ = node.shard.evict(&key, entry.version).await?;
        }
        let read = read(node, filler, &key).await?;
        let Some((read_entry, bytes)) = read else {
            return Err(format!("{key} could not be read without faults").into());
        };
        if bytes != body.as_bytes() || read_entry.version <= entry.version {
            return Err(format!(
                "{key}: after the remote changed, a read of {} returned {:?}",
                read_entry.version,
                String::from_utf8_lossy(&bytes)
            )
            .into());
        }
    }
    Ok(())
}

/// Writes `body` to `key` as another writer would, trying again while the
/// remote fails.
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

/// Deletes every noncurrent version of `key`, as a lifecycle rule would.
async fn expire(store: &SimS3, key: &str) {
    let all = versions(store, key);
    for (version, _) in all.iter().take(all.len().saturating_sub(1)) {
        let request = DeleteObject::new(key).with_version_id(version.clone());
        for _ in 0..20 {
            if store.delete_object(request.clone()).await.is_ok() {
                break;
            }
        }
    }
}

/// The versions of `key`, oldest first, delete markers included.
fn versions(store: &SimS3, key: &str) -> Vec<(VersionId, bool)> {
    store
        .versions()
        .into_iter()
        .filter(|(k, ..)| k == key)
        .filter_map(|(_, version, marker)| Some((version?, marker)))
        .collect()
}

/// The write identity of `version` of `key`, if it is an object SkyS3
/// wrote.
async fn identity_of(store: &SimS3, key: &str, version: VersionId) -> Option<String> {
    let request = GetObject::new(key).with_version_id(version);
    for _ in 0..20 {
        match store.get_object(request.clone()).await {
            Ok(object) => return object.info.metadata.write_identity().map(str::to_owned),
            Err(error) if !error.is_transient() => return None,
            Err(_) => {}
        }
    }
    None
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
