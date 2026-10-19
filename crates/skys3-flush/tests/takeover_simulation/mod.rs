//! The simulation scenario of upload takeover (§7.3): the primary changes
//! at every step of a streamed upload, and no partial or wrong object ever
//! reaches the remote.
//!
//! Each seed runs one node whose shard takes multipart uploads and streamed
//! single PUTs of a few keys, interleaved, with some part numbers uploaded
//! again and some uploads aborted or never committed, while its flusher
//! streams them to a simulated remote that delays requests, loses requests
//! and responses, and answers `500` and `503 SlowDown`, like AWS S3 or
//! Cloudflare R2. Between those writes the primary changes: the flusher
//! stops where it is, the node loses power, so the `PART_FLUSHED` and
//! `FLUSHED` records not yet committed are lost as they are when a primary
//! dies, and a new flusher takes over from what the log kept, as a new
//! primary's does. Each change is aimed at one step of one upload:
//!
//! - **after open**: once an upload's remote upload is recorded;
//! - **mid-part**: a moment after a part commits, while its send may be in
//!   flight, landed without its answer, or answered but not recorded;
//! - **after `PART_FLUSHED`**: once some of an upload's parts are recorded;
//! - **during complete**: a moment after the local completion commits,
//!   while the remote Complete may be in flight or applied without its
//!   `FLUSHED`.
//!
//! Some streamed bodies first announce extents that their `PUT` does not
//! name, so the remote upload holds parts of other bytes under the
//! numbers of the body's first parts.
//!
//! The deposed primary's sends may still land: at each change, an earlier
//! body of a part number uploaded more than once may reach the remote
//! upload a little later, replacing what a `PART_FLUSHED` recorded or what
//! the new primary sent. The checks are:
//!
//! - just before each local completion or streamed `PUT` commits, no
//!   remote object or version carries its write identity;
//! - every remote object and version carrying the identity of a completed
//!   upload or streamed `PUT` holds exactly its bytes, with its local ETag
//!   (a streamed `PUT` sent through its stream: the multipart ETag of its
//!   body in parts of `flush_part_bytes`), however often the primary
//!   changed: no partial or wrong object is ever visible;
//! - no remote object or version carries the identity of an upload that
//!   was aborted or a `PUT` that never committed;
//! - once the writes stop and the flush settles, every key holds its
//!   latest write at the remote and is clean in the index, whose
//!   `remote_etag` is the remote object's ETag;
//! - every remote upload whose ID the log recorded was completed or
//!   aborted, and the index keeps none. Those whose ID never reached the
//!   log (a lost `CreateMultipartUpload` answer, or an *opened* record lost
//!   with the primary) are left to the remote bucket's lifecycle rule.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use skys3_flush::ShardFlusher;
use skys3_index::EntryState;
use skys3_log::record::ExtentRef;
use skys3_remote::{GetObject, ObjectStore, UploadId, UploadPart};
use skys3_sim::s3::{Conditionals, SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use skys3_types::{ETag, EpochSeq};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::support::{
    Node, Patience, body_target, identity, md5_etag, runtime, streamed_etag, writes,
};

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(30),
    internal_error_probability: 0.04,
    slow_down_probability: 0.02,
    lost_request_probability: 0.03,
    lost_response_probability: 0.05,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

const KEYS: u32 = 3;

/// The part size streamed PUTs are sent in.
const PART: u64 = 16;

/// How long a streamed body may go unannounced before its `PUT` is given
/// up on.
const BODY_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest a primary change waits for the step it aims at.
const STEP_WAIT: Duration = Duration::from_millis(500);

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

/// A multipart upload that is open: every body each part number had, the
/// latest last, with the latest one's position.
struct Upload {
    key: String,
    parts: BTreeMap<u16, (EpochSeq, Vec<String>)>,
}

/// A streamed single PUT whose body is arriving: its bytes, their extents,
/// and how many of those were announced.
struct Body {
    key: String,
    body: String,
    extents: Vec<(u64, ExtentRef)>,
    announced: usize,
}

/// What a write that committed must look like wherever the remote holds
/// it: its bytes, and the ETags it may have.
#[derive(Clone, Debug)]
struct Expected {
    body: String,
    etags: Vec<ETag>,
}

/// The step of an upload a primary change is aimed at.
#[derive(Clone, Copy, Debug)]
enum Step {
    AfterOpen,
    MidPart,
    AfterRecords,
    DuringComplete,
}

/// The scenario's state between operations.
struct World {
    rng: SmallRng,
    store: SimS3,
    /// Open multipart uploads, by `MPU_CREATE` position.
    uploads: BTreeMap<EpochSeq, Upload>,
    /// Streamed PUTs whose body is arriving, by `UPLOAD_BEGIN` position.
    bodies: BTreeMap<EpochSeq, Body>,
    /// Each key's latest write, with its identity.
    latest: BTreeMap<String, (String, Expected)>,
    /// Every committed write's identity, and what it must look like.
    committed: BTreeMap<String, Expected>,
    /// The identities of aborted uploads and of PUTs that never commit.
    unpublished: BTreeSet<String>,
    /// Every remote upload ID the log recorded.
    recorded: BTreeSet<String>,
    /// The deposed primaries' sends still on their way.
    late: JoinSet<()>,
}

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
        min_part_size: 1,
        ..SimS3Config::default()
    };
    let store = context.s3(config);
    store.set_faults(FAULTS);
    let target = body_target(&store, writes(true, !like_r2, !like_r2), PART, BODY_TIMEOUT);
    runtime().block_on(async move {
        let mut node = Node::open_inline(seed).await;
        let mut flusher = node.flusher(&target);
        let mut world = World {
            rng,
            store: store.clone(),
            uploads: BTreeMap::new(),
            bodies: BTreeMap::new(),
            latest: BTreeMap::new(),
            committed: BTreeMap::new(),
            unpublished: BTreeSet::new(),
            recorded: BTreeSet::new(),
            late: JoinSet::new(),
        };
        for op in 0..world.rng.random_range(30..80) {
            let key = format!("key-{}", world.rng.random_range(0..KEYS));
            match world.rng.random_range(0..100) {
                0..=11 => world.create(&node, key).await,
                12..=36 => drop(world.part(&node, op).await),
                37..=46 => drop(world.complete(&node).await?),
                47..=51 => world.abort(&node).await,
                52..=59 => drop(world.begin(&node, key, op).await),
                60..=71 => world.announce(&node),
                72..=79 => drop(world.commit_body(&node).await?),
                80..=83 => world.put(&node, key, op).await,
                _ => {
                    let step = match world.rng.random_range(0..4) {
                        0 => Step::AfterOpen,
                        1 => Step::MidPart,
                        2 => Step::AfterRecords,
                        _ => Step::DuringComplete,
                    };
                    world.aim(&node, step, op).await?;
                    // The primary changes.
                    world.record_uploads(&node).await;
                    world.send_late(&node).await;
                    drop(flusher);
                    node = node.crash().await;
                    flusher = node.flusher(&target);
                }
            }
            let pause = world.rng.random_range(0..20);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        // Uploads still open are aborted, and bodies still arriving never
        // commit.
        for (upload, state) in std::mem::take(&mut world.uploads) {
            node.abort(&state.key, upload).await;
            world.unpublished.insert(identity(upload.seq.get()));
        }
        for begun in std::mem::take(&mut world.bodies).into_keys() {
            world.unpublished.insert(identity(begun.seq.get()));
        }
        while world.late.join_next().await.is_some() {}
        world.record_uploads(&node).await;
        store.set_faults(SimS3Faults::NONE);
        node.settle(&flusher).await;
        world.check_versions().await?;
        world.check_latest(&node).await?;
        world.check_uploads(&node, &flusher, &target).await
    })
}

impl World {
    /// Opens a multipart upload of `key`.
    async fn create(&mut self, node: &Node, key: String) {
        let upload = node.create(&key).await;
        let parts = BTreeMap::new();
        self.uploads.insert(upload, Upload { key, parts });
    }

    /// Stores a part of an open upload, perhaps a number stored before.
    async fn part(&mut self, node: &Node, op: u32) -> Option<EpochSeq> {
        let (&upload, state) = self.uploads.iter_mut().choose(&mut self.rng)?;
        let number = self.rng.random_range(1..=4);
        let body = format!("{} part {number} op {op}; ", state.key);
        let position = node.part(&state.key, upload, number, &body).await;
        let (latest, bodies) = state
            .parts
            .entry(number)
            .or_insert_with(|| (position, Vec::new()));
        *latest = position;
        bodies.push(body);
        Some(upload)
    }

    /// Completes an open upload with some of its parts, never none, or
    /// aborts it if it has none. Returns the upload completed.
    async fn complete(
        &mut self,
        node: &Node,
    ) -> Result<Option<EpochSeq>, Box<dyn std::error::Error>> {
        let Some(&upload) = self.uploads.keys().choose(&mut self.rng) else {
            return Ok(None);
        };
        let Some(Upload { key, parts }) = self.uploads.remove(&upload) else {
            return Ok(None);
        };
        let mut kept: Vec<(u16, EpochSeq, &str)> = parts
            .iter()
            .filter(|_| self.rng.random_ratio(4, 5))
            .map(|(n, (position, bodies))| (*n, *position, latest_body(bodies)))
            .collect();
        if kept.is_empty() {
            let Some((n, (position, bodies))) = parts.iter().next() else {
                node.abort(&key, upload).await;
                self.unpublished.insert(identity(upload.seq.get()));
                return Ok(None);
            };
            kept.push((*n, *position, latest_body(bodies)));
        }
        let wid = identity(upload.seq.get());
        check_unpublished(&self.store, &wid, "before its local commit").await?;
        let object = node.finish(&key, upload, &kept).await;
        let expected = Expected {
            body: kept.iter().map(|(_, _, body)| *body).collect(),
            etags: vec![object.etag],
        };
        self.commit(key, wid, expected);
        Ok(Some(upload))
    }

    /// Aborts an open upload.
    async fn abort(&mut self, node: &Node) {
        let Some(&upload) = self.uploads.keys().choose(&mut self.rng) else {
            return;
        };
        if let Some(state) = self.uploads.remove(&upload) {
            node.abort(&state.key, upload).await;
            self.unpublished.insert(identity(upload.seq.get()));
        }
    }

    /// Begins a streamed PUT of `key` whose body has a few parts, stores
    /// its extents, and announces the first of them.
    async fn begin(&mut self, node: &Node, key: String, op: u32) -> EpochSeq {
        let begun = node.begin(&key).await;
        let len = self.rng.random_range(PART * 2..PART * 5) as usize;
        let body: String = format!("{key} body op {op} ")
            .chars()
            .cycle()
            .take(len)
            .collect();
        // Some bodies first have extents announced that their `PUT` will
        // not name: the parts sent from them are in no remote object.
        if self.rng.random_ratio(1, 3) {
            let stale: String = "stale ".chars().cycle().take(PART as usize * 2).collect();
            let stale = node.extents(&key, &stale, 8).await;
            node.announce(&key, begun, &stale);
        }
        let extents = node.extents(&key, &body, 8).await;
        let mut state = Body {
            key,
            body,
            extents,
            announced: 0,
        };
        announce_some(node, &mut self.rng, at(begun), &mut state);
        self.bodies.insert(at(begun), state);
        at(begun)
    }

    /// Announces more of a body, as its gateway does after a primary
    /// change too: the new primary's flusher gets the announcements.
    fn announce(&mut self, node: &Node) {
        if let Some((&begun, state)) = self.bodies.iter_mut().choose(&mut self.rng) {
            announce_some(node, &mut self.rng, begun, state);
        }
    }

    /// Commits the `PUT` of a body after announcing the rest of it, or
    /// gives the body up: its `PUT` never commits. Returns the body's
    /// `UPLOAD_BEGIN` if its `PUT` committed.
    async fn commit_body(
        &mut self,
        node: &Node,
    ) -> Result<Option<EpochSeq>, Box<dyn std::error::Error>> {
        let Some(&begun) = self.bodies.keys().choose(&mut self.rng) else {
            return Ok(None);
        };
        let Some(mut state) = self.bodies.remove(&begun) else {
            return Ok(None);
        };
        let wid = identity(begun.seq.get());
        if self.rng.random_ratio(1, 5) {
            self.unpublished.insert(wid);
            return Ok(None);
        }
        state.announced = state.extents.len();
        node.announce(&state.key, begun.seq.get(), &state.extents);
        check_unpublished(&self.store, &wid, "before its PUT commits").await?;
        let seq = begun.seq.get();
        node.complete_extents(&state.key, &state.body, seq, &state.extents)
            .await;
        let expected = Expected {
            etags: vec![
                streamed_etag(&state.body, PART as usize),
                md5_etag(state.body.as_bytes()),
            ],
            body: state.body,
        };
        self.commit(state.key, wid, expected);
        Ok(Some(begun))
    }

    /// Commits a plain PUT of `key`.
    async fn put(&mut self, node: &Node, key: String, op: u32) {
        let body = format!("{key} put {op}");
        let seq = node.put(&key, &body).await;
        let expected = Expected {
            etags: vec![md5_etag(body.as_bytes())],
            body,
        };
        self.commit(key, identity(seq), expected);
    }

    fn commit(&mut self, key: String, wid: String, expected: Expected) {
        self.committed.insert(wid.clone(), expected.clone());
        self.latest.insert(key, (wid, expected));
    }

    /// Brings an upload to `step`, so that the primary changes there.
    async fn aim(&mut self, node: &Node, step: Step, op: u32) -> Outcome {
        let store = self.store.clone();
        match step {
            Step::AfterOpen => {
                let key = format!("key-{}", self.rng.random_range(0..KEYS));
                let upload = if self.rng.random_ratio(1, 2) {
                    self.create(node, key).await;
                    self.uploads.keys().next_back().copied()
                } else {
                    Some(self.begin(node, key, op).await)
                };
                if let Some(upload) = upload {
                    wait_for(async || recorded_parts(node, upload).await.is_some()).await;
                }
            }
            Step::MidPart => {
                self.part(node, op).await;
                self.pause(30).await;
            }
            Step::AfterRecords => {
                if let Some(upload) = self.part(node, op).await {
                    wait_for(async || recorded_parts(node, upload).await.unwrap_or(0) > 0).await;
                }
            }
            Step::DuringComplete => {
                let completed = if self.rng.random_ratio(2, 3) {
                    self.complete(node).await?
                } else {
                    self.commit_body(node).await?
                };
                if let Some(upload) = completed {
                    // Until the remote object appears, or a moment.
                    let wid = identity(upload.seq.get());
                    let deadline =
                        Instant::now() + Duration::from_millis(self.rng.random_range(0..60));
                    wait_for(async || {
                        Instant::now() >= deadline
                            || store.object(&self.latest_key_of(&wid)).is_some_and(|o| {
                                o.info.metadata.write_identity() == Some(wid.as_str())
                            })
                    })
                    .await;
                }
            }
        }
        Ok(())
    }

    /// The key whose latest write has the identity `wid`.
    fn latest_key_of(&self, wid: &str) -> String {
        self.latest
            .iter()
            .find(|(_, (latest, _))| latest == wid)
            .map(|(key, _)| key.clone())
            .unwrap_or_default()
    }

    async fn pause(&mut self, most_ms: u64) {
        let pause = self.rng.random_range(0..=most_ms);
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }

    /// Notes every remote upload ID the log records now: each must be
    /// completed or aborted in the end.
    async fn record_uploads(&mut self, node: &Node) {
        for (_, remote) in node.shard.remote_uploads().await.unwrap() {
            self.recorded.insert(remote.id);
        }
    }

    /// Sends, a little later, an earlier body of some part numbers that
    /// were uploaded more than once to the remote upload the log records
    /// for them, as the deposed primary's sends still in flight may land.
    async fn send_late(&mut self, node: &Node) {
        let remote = node.shard.remote_uploads().await.unwrap();
        for (upload, remote) in remote {
            let Some(state) = self.uploads.get(&upload) else {
                continue;
            };
            for (&number, (_, bodies)) in &state.parts {
                if bodies.len() < 2 || !self.rng.random_ratio(1, 2) {
                    continue;
                }
                let earlier = bodies[self.rng.random_range(0..bodies.len() - 1)].clone();
                let delay = Duration::from_millis(self.rng.random_range(0..150));
                let (store, key) = (self.store.clone(), remote.key.clone());
                let id = UploadId(remote.id.clone());
                self.late.spawn(async move {
                    tokio::time::sleep(delay).await;
                    let body = Bytes::from(earlier.into_bytes());
                    let part = UploadPart::new(key, id, u32::from(number), body);
                    let _ = store.upload_part(part).await;
                });
            }
        }
    }

    /// Checks every remote object and version: one carrying a committed
    /// write's identity holds exactly its bytes and one of its ETags, and
    /// none carries the identity of an aborted upload or a `PUT` that never
    /// committed.
    async fn check_versions(&self) -> Outcome {
        for (key, version, delete_marker) in self.store.versions() {
            if delete_marker {
                continue;
            }
            let mut request = GetObject::new(&key);
            if let Some(version) = version {
                request = request.with_version_id(version);
            }
            let object = self.store.get_object(request).await?;
            let Some(wid) = object.info.metadata.write_identity() else {
                continue;
            };
            if self.unpublished.contains(wid) {
                return Err(format!("{key}: {wid} is at the remote but never committed").into());
            }
            let Some(expected) = self.committed.get(wid) else {
                return Err(format!("{key}: {wid} is at the remote but is no write").into());
            };
            if object.body != expected.body.as_bytes()
                || !expected.etags.contains(&object.info.etag)
            {
                return Err(format!(
                    "{key}: {wid} is at the remote as {:?} with ETag {}, not {:?}",
                    String::from_utf8_lossy(&object.body),
                    object.info.etag,
                    expected,
                )
                .into());
            }
        }
        Ok(())
    }

    /// Checks that every key holds its latest write at the remote and is
    /// clean in the index, with the remote object's ETag.
    async fn check_latest(&self, node: &Node) -> Outcome {
        for (key, (wid, expected)) in &self.latest {
            let Some(remote) = self.store.object(key) else {
                return Err(format!("{key}: the remote holds nothing, not {wid}").into());
            };
            let held = remote.info.metadata.write_identity();
            if held != Some(wid.as_str())
                || remote.body != expected.body.as_bytes()
                || !expected.etags.contains(&remote.info.etag)
            {
                return Err(format!(
                    "{key}: the remote holds {held:?} with ETag {}, not {wid}: {expected:?}",
                    remote.info.etag
                )
                .into());
            }
            let entry = node.entry(key).await;
            let clean = entry.as_ref().is_some_and(|entry| {
                entry.state == EntryState::Clean
                    && entry.remote_etag.as_ref() == Some(&remote.info.etag)
            });
            if !clean {
                return Err(format!("{key}: the index holds {entry:?} after the flush").into());
            }
        }
        Ok(())
    }

    /// Checks that every remote upload the log recorded was completed or
    /// aborted, and that the index and the flusher keep none.
    async fn check_uploads(
        &self,
        node: &Node,
        flusher: &ShardFlusher,
        target: &skys3_flush::Target<SimS3>,
    ) -> Outcome {
        let patience = Patience::new();
        loop {
            target.abort_orphaned_uploads().await;
            let open: Vec<String> = self
                .store
                .uploads()
                .into_iter()
                .map(|(id, _)| id.0)
                .filter(|id| self.recorded.contains(id))
                .collect();
            let recorded = node.shard.remote_uploads().await?;
            let orphans = target.orphaned_uploads();
            let streams = flusher.status().streams;
            if open.is_empty() && orphans == 0 && streams == 0 && recorded.is_empty() {
                return Ok(());
            }
            if patience.is_exhausted() {
                return Err(format!(
                    "the remote uploads {open:?} the log recorded are open, the target holds \
                     {orphans}, {streams} streams are open, and the index records {recorded:?}"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The latest of a part number's bodies.
fn latest_body(bodies: &[String]) -> &str {
    bodies.last().map_or("", String::as_str)
}

/// The position of the record at `seq` of the test shard.
fn at(seq: u64) -> EpochSeq {
    crate::support::at(seq)
}

/// Announces the next few extents of `body`, begun at `begun`.
fn announce_some(node: &Node, rng: &mut SmallRng, begun: EpochSeq, body: &mut Body) {
    let more = rng.random_range(1..=4);
    let end = (body.announced + more).min(body.extents.len());
    node.announce(&body.key, begun.seq.get(), &body.extents[..end]);
    body.announced = end;
}

/// How many parts the index records of the remote upload of the upload at
/// `upload`, or `None` if it records no remote upload.
async fn recorded_parts(node: &Node, upload: EpochSeq) -> Option<usize> {
    let (_, parts) = node.shard.remote_upload(upload).await.ok()??;
    Some(parts.len())
}

/// Waits until `done` holds, or [`STEP_WAIT`] has passed.
async fn wait_for(done: impl AsyncFn() -> bool) {
    let deadline = Instant::now() + STEP_WAIT;
    while !done().await && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Checks that no remote object, nor any version of one, carries `wid`.
async fn check_unpublished(store: &SimS3, wid: &str, why: &str) -> Outcome {
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
            && object.info.metadata.write_identity() == Some(wid)
        {
            return Err(format!("{key}: {wid} is at the remote {why}").into());
        }
    }
    Ok(())
}
