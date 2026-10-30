//! Remote server-side copies (§7.2, §11, plan M4-07): a copier client
//! copies clean objects of the `write_back` buckets through any node's
//! gateway while the workload, the faults, and the remote's failures run,
//! and an audit checks what every copy's flush left at the remote.
//!
//! With [`ClusterConfig::copies`](crate::ClusterConfig::copies), the
//! copier first writes a few *source* keys in every `write_back` bucket.
//! Then it copies a source to a *destination* key, in its own bucket or
//! another one (all of them flush to one remote bucket), each copy with
//! its own user metadata and, every other copy, its own tags (`REPLACE`),
//! else the source's (`COPY`). Before most copies it waits until the
//! remote store holds the source's last write and its `FLUSHED` record
//! is due, so that the source is clean and the copy can flush as a remote
//! `CopyObject`. Before some copies it makes the remote store fail its
//! next `CopyObject` ([`Copies::stalls`]), and right after some it writes
//! the source again ([`Copies::rewrites`]), so that the source may change
//! at the remote before the copy flushes: the copy's
//! `x-amz-copy-source-if-match` then fails, and the copy must be uploaded
//! instead, never held as a conflict. Copies and source writes whose
//! answer is lost are retried by nobody: their keys simply end with
//! whatever the cluster committed.
//!
//! Once every fault healed, a drain waits until the flushers have nothing
//! left to send, and records the keys they hold in conflict. No other
//! writer touches the remote store, so the audit requires none. After the
//! final power loss it reads every copier key on every member of its
//! shard and checks, for each, that the members agree and that the remote
//! store holds exactly their version: its bytes (by MD5), user metadata,
//! content type, tags, and the write identity of the version's record.
//! It counts the copies the remote store applied as `CopyObject`, which a
//! scenario requires some of.
//!
//! [`CopyBug`] seeds bugs the audit must catch: copies flushed without
//! the copy's write identity, and a source's failed precondition taken
//! for a conflict at the destination.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::Full;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::test_hooks::CopyBug;
use skys3_gateway::ShardRef;
use skys3_index::{Entry, EntryState};
use skys3_remote::probe::ConditionalProbe;
use skys3_sim::SimS3;
use skys3_sim::check::Violation;
use skys3_sim::s3::{Fault, Operation};
use skys3_types::{BucketDocument, ClusterId, WriteIdentity};

use crate::backup::Flushers;
use crate::cluster::remote_prefix;
use crate::conflicts::{drain, write_back_buckets};
use crate::s3;
use crate::workload::{Routes, Workload, etag_of};

/// Source keys per `write_back` bucket.
const SOURCES: usize = 2;
/// Destination keys per `write_back` bucket.
const DESTINATIONS: usize = 3;
/// The share of copies whose source the copier first waits to be clean.
const CLEAN_SOURCES: f64 = 0.75;
/// How long the copier waits for a source's last write to reach the
/// remote store.
const CLEAN_LIMIT: Duration = Duration::from_secs(10);
/// How long the copier waits, once the remote store holds a source's
/// write, for the `FLUSHED` record that makes it clean: it rides a later
/// group commit, at most a second later (§7.1).
const FLUSHED_DELAY: Duration = Duration::from_millis(1500);
/// Attempts of each first write of a source.
const SOURCE_ATTEMPTS: usize = 20;

/// The copier's shape and the seeded bug, if any.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Copies {
    /// How many copies the copier sends.
    pub copies: usize,
    /// The probability that the copier writes a copy's source again right
    /// after the copy is answered.
    pub rewrites: f64,
    /// The probability that the copier makes the remote store fail the
    /// next `CopyObject` it gets, before it sends a copy: the copy's flush
    /// then waits for a retry, while a new write of its source may land.
    pub stalls: f64,
    /// A seeded bug the flushers run.
    pub bug: CopyBug,
}

impl Copies {
    /// `copies` copies, a third of them followed by a new write of their
    /// source, a quarter stalled, and no bug.
    #[must_use]
    pub fn new(copies: usize) -> Self {
        Self {
            copies,
            rewrites: 1.0 / 3.0,
            stalls: 0.25,
            bug: CopyBug::None,
        }
    }

    /// The same, writing each copy's source again with probability
    /// `rewrites`, and stalling its flush with probability `stalls`.
    #[must_use]
    pub fn with_rewrites(mut self, rewrites: f64, stalls: f64) -> Self {
        self.rewrites = rewrites;
        self.stalls = stalls;
        self
    }

    /// The same, running the seeded `bug`.
    #[must_use]
    pub fn with_bug(mut self, bug: CopyBug) -> Self {
        self.bug = bug;
        self
    }
}

/// What the copier did and the audit found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopyAudit {
    /// Copies the gateways acknowledged.
    pub acknowledged: usize,
    /// Copies of the copier's keys the remote store applied as
    /// `CopyObject`, answered or not: copies flushed server-side.
    pub server_side: usize,
    /// Copies the remote store applied as uploads (`PutObject`): of a
    /// source that was not clean, or whose remote object changed first.
    pub uploaded: usize,
    /// Sources the copier wrote again right after copying them.
    pub rewritten: usize,
    /// Copier keys whose remote object the audit checked against the
    /// members' version.
    pub audited: usize,
}

/// The copier's record, shared with the drain and the audit.
#[derive(Debug, Default)]
pub(crate) struct CopyLog {
    acknowledged: usize,
    rewritten: usize,
    /// The keys held in conflict once the flushers drained, as the
    /// history would name them.
    held: Option<BTreeSet<String>>,
    /// Why the copier or the drain failed, if it did.
    failure: Option<String>,
}

/// The copier's record.
pub(crate) type Shared = Arc<Mutex<CopyLog>>;

/// Each member's entry of a key as recovery found it, by bucket name and
/// key.
pub(crate) type Entries = BTreeMap<(String, String), Vec<Option<Entry>>>;

fn lock(log: &Shared) -> std::sync::MutexGuard<'_, CopyLog> {
    log.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Every key the copier writes: its bucket and object key, sources first.
pub(crate) fn keys(routes: &Routes) -> Vec<(BucketDocument, String)> {
    let buckets = write_back_buckets(routes);
    let mut keys = Vec::new();
    for bucket in &buckets {
        for n in 0..SOURCES {
            keys.push((bucket.clone(), format!("copy-source-{n}")));
        }
    }
    for bucket in &buckets {
        for n in 0..DESTINATIONS {
            keys.push((bucket.clone(), format!("copy-target-{n}")));
        }
    }
    keys
}

/// What the copier works with.
pub(crate) struct Copier {
    pub copies: Copies,
    pub routes: Routes,
    pub workload: Workload,
    /// The remote store, which the copier watches for its sources'
    /// flushes.
    pub store: SimS3,
    pub seed: u64,
    pub log: Shared,
}

/// Runs the copier (see the module documentation), recording what it did,
/// or why it failed, in its log.
pub(crate) async fn copier(copier: Copier) -> turmoil::Result {
    let log = Arc::clone(&copier.log);
    if let Err(error) = copier.run().await {
        lock(&log).failure = Some(error);
    }
    Ok(())
}

/// A source key: its bucket and key, and the ETag of its last
/// acknowledged write.
struct Source<'a> {
    bucket: &'a BucketDocument,
    key: &'a String,
    etag: Option<String>,
}

impl Copier {
    async fn run(self) -> Result<(), String> {
        let mut rng = SmallRng::seed_from_u64(self.seed);
        let keys = keys(&self.routes);
        let mut sources: Vec<Source<'_>> = keys
            .iter()
            .filter(|(_, key)| key.contains("source"))
            .map(|(bucket, key)| Source {
                bucket,
                key,
                etag: None,
            })
            .collect();
        let targets: Vec<_> = keys
            .iter()
            .filter(|(_, key)| key.contains("target"))
            .collect();
        if sources.is_empty() {
            return Err("the cluster has no write_back bucket to copy in".to_owned());
        }
        let mut writes = 0;
        for source in &mut sources {
            for _ in 0..SOURCE_ATTEMPTS {
                writes += 1;
                source.etag = self
                    .put(&mut rng, source.bucket, source.key, writes)
                    .await?;
                if source.etag.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if source.etag.is_none() {
                let (bucket, key) = (&source.bucket.name, source.key);
                return Err(format!("no write of {bucket}/{key} was acknowledged"));
            }
        }
        for n in 0..self.copies.copies {
            let pause = rng.random_range(Duration::ZERO..=self.workload.think_time);
            tokio::time::sleep(pause).await;
            let drawn = rng.random_range(0..sources.len());
            let source = &mut sources[drawn];
            // Most copies wait for a clean source, which a remote copy
            // needs; the others may copy one still dirty, and be uploaded.
            if rng.random_bool(CLEAN_SOURCES) {
                self.await_clean(source).await;
            }
            let (bucket, target) = targets[rng.random_range(0..targets.len())];
            if rng.random_bool(self.copies.stalls) {
                self.store
                    .inject(Operation::CopyObject, Fault::InternalError);
            }
            let host = self.host(&mut rng);
            let request = copy_request(n, (source.bucket, source.key), (bucket, target))?;
            if write(&host, request, self.workload.timeout).await? {
                lock(&self.log).acknowledged += 1;
            }
            if rng.random_bool(self.copies.rewrites) {
                writes += 1;
                let written = self
                    .put(&mut rng, source.bucket, source.key, writes)
                    .await?;
                // A write without an answer may still land: wait for none.
                source.etag = written;
                lock(&self.log).rewritten += 1;
            }
        }
        Ok(())
    }

    /// Waits until the remote store holds `source`'s last acknowledged
    /// write, and then for its lazy `FLUSHED` (§7.1) to make it clean, up
    /// to [`CLEAN_LIMIT`].
    async fn await_clean(&self, source: &Source<'_>) {
        let Some(etag) = &source.etag else {
            return;
        };
        let remote_key = format!("{}{}", remote_prefix(source.bucket), source.key);
        let deadline = tokio::time::Instant::now() + CLEAN_LIMIT;
        while tokio::time::Instant::now() < deadline {
            if self
                .store
                .object(&remote_key)
                .is_some_and(|object| object.info.etag.as_str() == etag)
            {
                tokio::time::sleep(FLUSHED_DELAY).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// A node drawn at random, whose gateway routes the request.
    fn host(&self, rng: &mut SmallRng) -> String {
        let nodes = &self.routes.nodes;
        nodes[rng.random_range(0..nodes.len())].to_string()
    }

    /// Writes a new body to `bucket`'s `key`, with user metadata and a
    /// tag of its own, through any node. Returns its ETag if it was
    /// acknowledged.
    async fn put(
        &self,
        rng: &mut SmallRng,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
    ) -> Result<Option<String>, String> {
        let mut body = format!("copier write {n} of {}/{key}:", bucket.name).into_bytes();
        let len = rng.random_range(body.len()..=self.workload.max_body.max(body.len()));
        body.resize(len, b'a' + (n % 26) as u8);
        let etag = etag_of(&body);
        let request = Request::put(format!("/{}/{key}", bucket.name))
            .header("x-amz-meta-source", n.to_string())
            .header("x-amz-tagging", format!("written={n}"))
            .body(Full::new(Bytes::from(body)))
            .map_err(|error| error.to_string())?;
        let acknowledged = write(&self.host(rng), request, self.workload.timeout).await?;
        Ok(acknowledged.then_some(etag))
    }
}

/// The CopyObject of copy `n` from `source` to `target`: with its own
/// metadata, and its own tags for an even `n`, the source's for an odd
/// one.
fn copy_request(
    n: usize,
    (source_bucket, source): (&BucketDocument, &String),
    (bucket, target): (&BucketDocument, &String),
) -> Result<Request<Full<Bytes>>, String> {
    let mut request = Request::put(format!("/{}/{target}", bucket.name))
        .header(
            "x-amz-copy-source",
            format!("{}/{source}", source_bucket.name),
        )
        .header("x-amz-metadata-directive", "REPLACE")
        .header("x-amz-meta-copy", n.to_string())
        .header("content-type", format!("application/copy-{}", n % 3));
    if n.is_multiple_of(2) {
        request = request
            .header("x-amz-tagging-directive", "REPLACE")
            .header("x-amz-tagging", format!("copy={n}"));
    }
    request
        .body(Full::new(Bytes::new()))
        .map_err(|error| error.to_string())
}

/// Sends the write `request` to `host`: whether it was acknowledged. A
/// server error or no answer is not, and an unexpected answer is an
/// error.
async fn write(
    host: &str,
    request: Request<Full<Bytes>>,
    timeout: Duration,
) -> Result<bool, String> {
    let answer = match s3::connect(host, timeout).await {
        Ok(connection) => connection.send(request, timeout).await,
        Err(error) => Err(error),
    };
    match answer {
        Ok(response) if response.status() == StatusCode::OK => Ok(true),
        Ok(response) if response.status().is_server_error() => Ok(false),
        // A gateway that gave up on its request answers a client error
        // that names the timeout.
        Ok(response) if String::from_utf8_lossy(response.body()).contains("Timeout") => Ok(false),
        Ok(response) => Err(format!(
            "a copier write answered {}: {}",
            response.status(),
            String::from_utf8_lossy(response.body())
        )),
        Err(_) => Ok(false),
    }
}

/// Waits, once every fault healed, until the flushers have nothing left
/// to send, and records the keys held in conflict.
pub(crate) async fn finish(
    flushers: Arc<Flushers>,
    routes: Routes,
    log: Shared,
) -> turmoil::Result {
    let buckets = write_back_buckets(&routes);
    match drain(&flushers, &buckets).await {
        Ok(held) => lock(&log).held = Some(held),
        Err(error) => lock(&log).failure = Some(error),
    }
    Ok(())
}

/// Checks every copier key, with each member's entry as recovery found it
/// (`entries`, by bucket name and key), against the remote store, and
/// that no key is held in conflict. Returns what it found.
///
/// # Errors
///
/// The copier's or the drain's failure, a conflict held, or the first key
/// whose members disagree or whose remote object is not their version.
pub(crate) fn audit(
    log: &Shared,
    store: &SimS3,
    cluster: &ClusterId,
    routes: &Routes,
    entries: &Entries,
) -> Result<CopyAudit, Violation> {
    let log = lock(log);
    let violation = |key: &str, reason: String| Violation {
        operations: Vec::new(),
        key: key.to_owned(),
        reason,
    };
    if let Some(failure) = &log.failure {
        return Err(violation("", format!("the copier failed: {failure}")));
    }
    let held = log
        .held
        .as_ref()
        .ok_or_else(|| violation("", "the flushers never drained".to_owned()))?;
    if let Some(key) = held.iter().next() {
        return Err(violation(
            key,
            format!("a conflict was reported for the cluster's own write: {held:?}"),
        ));
    }
    let mut audited = 0;
    for (bucket, key) in keys(routes) {
        let name = format!("{}/{key}", bucket.name);
        let found = entries
            .get(&(bucket.name.as_str().to_owned(), key.clone()))
            .map_or(&[][..], Vec::as_slice);
        // The members hold the same version, but not always in the same
        // state: the final power loss may cut a `FLUSHED` record on some
        // of them and not on others (see `check`), so each is checked.
        let versions: Vec<_> = found
            .iter()
            .map(|entry| entry.as_ref().map(|entry| entry.version))
            .collect();
        if versions.windows(2).any(|pair| pair[0] != pair[1]) {
            let states: Vec<_> = found
                .iter()
                .map(|entry| entry.as_ref().map(|entry| (entry.version, entry.state)))
                .collect();
            return Err(violation(
                &name,
                format!("the members disagree: {states:?}"),
            ));
        }
        let members: Vec<_> = found.iter().flatten().collect();
        if members.is_empty() {
            continue;
        }
        for entry in members {
            check(store, cluster, &bucket, &key, entry)
                .map_err(|reason| violation(&name, reason))?;
        }
        audited += 1;
    }
    let to_targets = |operation| {
        store
            .applied(operation)
            .iter()
            .filter(|key| {
                !key.contains(ConditionalProbe::SCRATCH_DIR) && key.contains("copy-target-")
            })
            .count()
    };
    Ok(CopyAudit {
        acknowledged: log.acknowledged,
        server_side: to_targets(Operation::CopyObject),
        uploaded: to_targets(Operation::PutObject),
        rewritten: log.rewritten,
        audited,
    })
}

/// Checks that the remote store holds `entry`'s version of `bucket`'s
/// `key`: clean, with its bytes, metadata, tags, and write identity.
fn check(
    store: &SimS3,
    cluster: &ClusterId,
    bucket: &BucketDocument,
    key: &str,
    entry: &Entry,
) -> Result<(), String> {
    let remote = store.object(&format!("{}{key}", remote_prefix(bucket)));
    let (object, remote) = match (&entry.object, remote) {
        (None, None) => return Ok(()),
        (Some(object), Some(remote)) => (object, remote),
        (object, remote) => {
            return Err(format!(
                "the members hold {:?}, the remote store {:?}",
                object.as_ref().map(|o| &o.local_etag),
                remote.map(|r| r.info.etag)
            ));
        }
    };
    // The last flushes' `FLUSHED` records ride later group commits (§7.1),
    // which the final power loss may cut: such an entry is still dirty,
    // and the remote must hold its version all the same.
    if entry.state == EntryState::Clean && entry.remote_etag.as_ref() != Some(&remote.info.etag) {
        return Err(format!(
            "the entry records the remote ETag {:?}, the remote store holds {}",
            entry.remote_etag, remote.info.etag
        ));
    }
    // Every version the copier writes is a single PUT or a copy of one,
    // whose ETag is the MD5 of its bytes.
    let digest = etag_of(&remote.body);
    if digest != object.local_etag.as_str() {
        return Err(format!(
            "the remote bytes have the MD5 {digest}, the version {}",
            object.local_etag
        ));
    }
    let shard = ShardRef::for_key(bucket, key);
    let position = object.write_identity.unwrap_or(entry.version);
    let expected = WriteIdentity::new(
        cluster.clone(),
        bucket.bucket_id.clone(),
        shard.shard,
        position,
    );
    let mut metadata = remote.info.metadata.clone();
    let identity = metadata.remove(WriteIdentity::METADATA_KEY);
    if identity.as_deref() != Some(expected.to_string().as_str()) {
        return Err(format!(
            "the remote object has the write identity {identity:?}, not {expected}"
        ));
    }
    let local: BTreeMap<&str, &str> = object
        .metadata
        .iter()
        .filter_map(|(name, value)| Some((name.strip_prefix("x-amz-meta-")?, value.as_str())))
        .collect();
    let at_remote: BTreeMap<&str, &str> = metadata.iter().collect();
    if local != at_remote {
        return Err(format!(
            "the remote object has the user metadata {at_remote:?}, the version {local:?}"
        ));
    }
    let content_type = object.metadata.get("content-type");
    if remote.info.content_type.as_ref() != content_type {
        return Err(format!(
            "the remote object has the content type {:?}, the version {content_type:?}",
            remote.info.content_type
        ));
    }
    let tags = store.tags(&format!("{}{key}", remote_prefix(bucket)));
    if tags.as_ref() != Some(&object.tags) {
        return Err(format!(
            "the remote object has the tags {tags:?}, the version {:?}",
            object.tags
        ));
    }
    Ok(())
}
