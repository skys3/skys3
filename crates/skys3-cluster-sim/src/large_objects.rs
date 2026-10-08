//! Large objects under faults (§7.3, §7.4, §7.5, §8.9, plan M4-13): no
//! partial remote object is ever visible, and ETags match.
//!
//! With [`ClusterConfig::large_objects`](crate::ClusterConfig::large_objects),
//! clients send the large-object mix (streamed single `PUT`s, multipart
//! uploads of several parts with UploadPartCopy, re-uploaded and dropped
//! parts, aborts), every multipart step and `PutObject` at the remote store
//! fails or loses its answer more often ([`LargeObjects::step_faults`]),
//! a node may crash right after the remote applied one of its steps
//! ([`LargeObjects::step_crash_probability`]), and the run audits the
//! remote store:
//!
//! - **At every write the store applies**, through its observer: the
//!   object now at an audited key is a complete version a client sent and
//!   the cluster may have committed, with exactly that version's bytes
//!   and size, its metadata, a write identity of the key's shard that
//!   names no other version, and the ETag the version must have there (the
//!   MD5 of a single `PUT` sent whole, the multipart ETag of 512-byte parts
//!   of one that streamed, or the client's multipart ETag of an upload).
//!   A body that failed its checks, or an upload never completed, must
//!   never appear.
//! - **At the end**, once clients aborted every upload any node lists and
//!   every flusher drained (`drain`: no stream left open), and every node
//!   lost power, against the final logs: every write identity the remote
//!   ever held
//!   names a committed version of its key with the local ETag the client
//!   sent; every `FLUSHED` record holds the remote ETag its version's
//!   object had at the remote; and every remote upload whose ID the final
//!   primary's log holds is completed or aborted, unless its local upload
//!   is still open. An upload whose ID was never committed (a lost
//!   `CreateMultipartUpload` answer, or an *opened* record lost in a
//!   crash, perhaps still in a deposed primary's log) is left to the
//!   remote's abort-incomplete-uploads rule (§7.3) and counted.
//!
//! The history checkers judge that no acknowledged write is lost, as in
//! every run, with write-through and backup audits as those configurations
//! add them. Seeded bugs of the flusher ([`LargeObjectBug`]) show that the
//! audit catches what it should.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::test_hooks::LargeObjectBug;
use skys3_gateway::ShardRef;
use skys3_log::RecordBody;
use skys3_log::record::RemoteStep;
use skys3_remote::GetOutput;
use skys3_sim::SimS3;
use skys3_sim::check::Violation;
use skys3_sim::history::History;
use skys3_sim::s3::{Applied, Operation, SimS3Faults};
use skys3_types::{BucketDocument, ClusterId, EpochSeq, Seq, WriteIdentity};

use crate::backup::Flushers;
use crate::faults::Fault;
use crate::lifecycle::ShardLogs;

/// The large-object mix, its faults, and its audit (plan M4-13).
#[derive(Clone, Debug, PartialEq)]
pub struct LargeObjects {
    /// The random faults of each multipart step (`CreateMultipartUpload`,
    /// `UploadPart`, `ListParts`, `CompleteMultipartUpload`,
    /// `AbortMultipartUpload`) or `PutObject` named here at the remote
    /// store, in place of
    /// [`ClusterConfig::remote_faults`](crate::ClusterConfig::remote_faults).
    pub step_faults: Vec<(Operation, SimS3Faults)>,
    /// The probability that a node crashes, with or without power loss,
    /// right after the remote store applied one of its multipart steps or
    /// `PutObject`s: before it learns the answer, or before it records
    /// what the answer said.
    pub step_crash_probability: f64,
    /// The most crashes aimed at steps in one run.
    pub step_crashes: usize,
    /// Whether flushers stream uploads as they arrive (§7.3). Without,
    /// every multipart version is sent after its commit, as the flusher's
    /// own multipart upload (§7.4).
    pub streaming: bool,
    /// A bug seeded into every flusher.
    pub bug: LargeObjectBug,
}

impl LargeObjects {
    /// The multipart steps and `PutObject`.
    pub const STEPS: [Operation; 6] = [
        Operation::CreateMultipartUpload,
        Operation::UploadPart,
        Operation::ListParts,
        Operation::CompleteMultipartUpload,
        Operation::AbortMultipartUpload,
        Operation::PutObject,
    ];

    /// `faults` for every one of [`LargeObjects::STEPS`].
    #[must_use]
    pub fn every_step(faults: &SimS3Faults) -> Vec<(Operation, SimS3Faults)> {
        Self::STEPS
            .iter()
            .map(|step| (*step, faults.clone()))
            .collect()
    }
}

impl Default for LargeObjects {
    /// Each step takes 2 to 15 ms each way, fails 2% of the time with a
    /// `500` and 2% with `503 SlowDown`, and loses 1% of its requests and
    /// 4% of its answers; a node crashes after one step in 40, at most four
    /// times a run.
    fn default() -> Self {
        Self {
            step_faults: Self::every_step(&SimS3Faults {
                min_delay: Duration::from_millis(2),
                max_delay: Duration::from_millis(15),
                internal_error_probability: 0.02,
                slow_down_probability: 0.02,
                lost_request_probability: 0.01,
                lost_response_probability: 0.04,
                ..SimS3Faults::NONE
            }),
            step_crash_probability: 0.025,
            step_crashes: 4,
            streaming: true,
            bug: LargeObjectBug::None,
        }
    }
}

/// How long a node aimed at a step stays down.
const STEP_DOWNTIME: std::ops::RangeInclusive<Duration> =
    Duration::from_millis(200)..=Duration::from_millis(1500);

/// What the audit of large objects found ([`Report::large_objects`]).
///
/// [`Report::large_objects`]: crate::Report::large_objects
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LargeObjectAudit {
    /// Objects of single `PUT`s sent whole that the remote store applied.
    pub single: u64,
    /// Objects of single `PUT`s that streamed as remote multipart uploads.
    pub streamed: u64,
    /// Objects of the clients' multipart uploads.
    pub multipart: u64,
    /// Remote uploads aborted.
    pub aborted: u64,
    /// `FLUSHED` records checked against the remote ETag of their version.
    pub flushed: u64,
    /// Crashes aimed at a step, by the step's operation.
    pub step_crashes: BTreeMap<String, u64>,
    /// Remote uploads still open at the end whose IDs were never committed:
    /// the remote's lifecycle rule must abort them (§7.3).
    pub unlogged: u64,
}

/// A version a client may make: registered before the request that would
/// make it is sent.
#[derive(Clone, Debug)]
pub(crate) struct Expected {
    /// The key, as the history names it.
    pub name: String,
    /// Every byte of the object.
    pub body: Bytes,
    /// The ETag clients see.
    pub local_etag: String,
    /// The ETags the remote store may give it.
    pub remote_etags: Vec<String>,
    /// Whether the cluster may commit it: false for a body that fails its
    /// checks.
    pub committable: bool,
}

/// The versions clients may make, and the audit of every object the
/// remote store holds. Clones share them.
#[derive(Clone)]
pub(crate) struct Versions {
    watch: Arc<Mutex<Watch>>,
    history: History,
}

struct Watch {
    cluster: ClusterId,
    /// Each audited remote key: the history's name of the key, and its
    /// shard.
    keys: BTreeMap<String, (String, ShardRef)>,
    expected: BTreeMap<String, Expected>,
    /// Each write identity the remote held: the operation that wrote it,
    /// and every ETag it had.
    identities: BTreeMap<String, (String, BTreeSet<String>)>,
    /// The position of each node, by host.
    nodes: BTreeMap<String, usize>,
    rng: SmallRng,
    probability: f64,
    crashes_left: usize,
    /// Crashes aimed at steps, for the driver to start.
    crashes: Vec<Fault>,
    audit: LargeObjectAudit,
    /// The first object found wrong: its key's name, and why.
    violation: Option<(String, String)>,
}

impl Versions {
    /// The audit of `keys` (each with the history's name for it, its
    /// bucket, and its key), each under the remote prefix `prefix` gives
    /// it, on a cluster of `nodes` hosts, drawing its crashes from `seed`.
    pub(crate) fn new(
        config: &LargeObjects,
        cluster: ClusterId,
        keys: Vec<(String, BucketDocument, String)>,
        prefix: impl Fn(&BucketDocument) -> Option<String>,
        nodes: &[String],
        history: History,
        seed: u64,
    ) -> Self {
        let keys = keys
            .into_iter()
            .filter_map(|(name, bucket, key)| {
                let remote = format!("{}{key}", prefix(&bucket)?);
                Some((remote, (name, ShardRef::for_key(&bucket, &key))))
            })
            .collect();
        let watch = Watch {
            cluster,
            keys,
            expected: BTreeMap::new(),
            identities: BTreeMap::new(),
            nodes: nodes
                .iter()
                .enumerate()
                .map(|(index, host)| (host.clone(), index))
                .collect(),
            rng: SmallRng::seed_from_u64(seed),
            probability: config.step_crash_probability,
            crashes_left: config.step_crashes,
            crashes: Vec::new(),
            audit: LargeObjectAudit::default(),
            violation: None,
        };
        Self {
            watch: Arc::new(Mutex::new(watch)),
            history,
        }
    }

    fn watch(&self) -> std::sync::MutexGuard<'_, Watch> {
        self.watch.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes `remote` fail and lose the answers of the steps as `config`
    /// says, and audit every request it applies.
    pub(crate) fn watch_store(&self, config: &LargeObjects, remote: &SimS3) {
        for (step, faults) in &config.step_faults {
            remote.set_operation_faults(*step, Some(faults.clone()));
        }
        let versions = self.clone();
        remote.observe(Some(Arc::new(move |applied: &Applied<'_>| {
            versions.applied(applied);
        })));
    }

    /// Registers the version operation `op` may make.
    pub(crate) fn expect(&self, op: String, expected: Expected) {
        self.watch().expected.insert(op, expected);
    }

    /// Whether `body` with `etag` is the version operation `op` made, as
    /// a client reads it.
    pub(crate) fn matches(&self, op: &str, body: &[u8], etag: &str) -> bool {
        self.watch()
            .expected
            .get(op)
            .is_some_and(|expected| expected.body == body && expected.local_etag == etag)
    }

    /// Audits a request the remote store applied, and may aim a crash at
    /// the node that sent it.
    fn applied(&self, applied: &Applied<'_>) {
        let mut watch = self.watch();
        if LargeObjects::STEPS.contains(&applied.operation)
            && applied.operation != Operation::ListParts
            && let Some(&node) = applied.source.and_then(|host| watch.nodes.get(host))
        {
            watch.aim(node, applied.operation);
        }
        if applied.operation == Operation::AbortMultipartUpload {
            watch.audit.aborted += 1;
        }
        if let (Some(key), Some(object)) = (applied.key, applied.current)
            && watch.violation.is_none()
            && let Some((name, shard)) = watch.keys.get(key).cloned()
            && let Err(reason) = watch.check(&shard, object)
        {
            watch.violation = Some((name, reason));
        }
    }

    /// The crashes aimed at steps since the last call.
    pub(crate) fn take_crashes(&self) -> Vec<Fault> {
        std::mem::take(&mut self.watch().crashes)
    }

    /// The first object found wrong, with its key's history.
    pub(crate) fn violation(&self) -> Option<Violation> {
        let (key, reason) = self.watch().violation.clone()?;
        Some(self.violation_of(key, reason))
    }

    fn violation_of(&self, key: String, reason: String) -> Violation {
        let operations = self
            .history
            .operations()
            .into_iter()
            .filter(|operation| operation.key == key)
            .map(|operation| operation.to_string())
            .collect();
        Violation {
            key,
            reason,
            operations,
        }
    }

    /// Audits the final logs and the remote store's open uploads, and
    /// returns what the whole run found.
    ///
    /// # Errors
    ///
    /// The first version, record, or upload found wrong.
    pub(crate) fn audit(
        &self,
        logs: &BTreeMap<ShardRef, ShardLogs>,
        remote: &SimS3,
    ) -> Result<LargeObjectAudit, Violation> {
        if let Some(violation) = self.violation() {
            return Err(violation);
        }
        let watch = self.watch();
        let fail = |name: &str, reason: String| -> Result<LargeObjectAudit, Violation> {
            Err(self.violation_of(name.to_owned(), reason))
        };
        let names: BTreeMap<(ShardRef, &str), &str> = watch
            .keys
            .iter()
            .map(|(remote, (name, shard))| {
                let key = name.split_once('/').map_or(remote.as_str(), |(_, key)| key);
                ((shard.clone(), key), name.as_str())
            })
            .collect();
        let finals = Finals::read(&watch.cluster, logs);
        let mut audit = watch.audit.clone();

        // Every identity the remote held names a committed version.
        for (identity, (op, _)) in &watch.identities {
            let expected = &watch.expected[op];
            match finals.committed.get(identity) {
                Some((_, etag)) if *etag == expected.local_etag => {}
                found => {
                    return fail(
                        &expected.name,
                        format!(
                            "the remote held {identity}, the write of {op}, but the final logs \
                             commit {found:?} under that identity"
                        ),
                    );
                }
            }
        }
        // Every `FLUSHED` names the remote ETag its version had.
        for (shard, key, seq, etag) in &finals.flushed {
            let Some(name) = names.get(&(shard.clone(), key.as_str())) else {
                continue;
            };
            audit.flushed += 1;
            let identities = finals
                .versions
                .get(&(shard.clone(), key.clone(), *seq))
                .cloned()
                .unwrap_or_default();
            let held: Vec<&BTreeSet<String>> = identities
                .iter()
                .filter_map(|identity| watch.identities.get(identity).map(|(_, etags)| etags))
                .collect();
            if !held.iter().any(|etags| etags.contains(etag)) {
                return fail(
                    name,
                    format!(
                        "FLUSHED of seq {seq} records the remote ETag {etag}, but the remote \
                         held that version ({identities:?}) with {held:?}"
                    ),
                );
            }
        }
        // Every remote upload a log recorded ended, unless its local upload
        // is still open.
        for (id, key) in remote.uploads() {
            match finals.opened.get(&id.0) {
                None => audit.unlogged += 1,
                Some(upload) if finals.open_locally(upload) => {}
                Some(_) => {
                    let name = watch.keys.get(&key).map_or(key.as_str(), |(name, _)| name);
                    return fail(
                        name,
                        format!(
                            "the remote upload {} of {key}, committed in its log, was never \
                             completed or aborted",
                            id.0
                        ),
                    );
                }
            }
        }
        Ok(audit)
    }
}

impl Watch {
    /// Aims a crash at `node` right after the remote applied its
    /// `operation`, with the configured probability, while crashes are
    /// left.
    fn aim(&mut self, node: usize, operation: Operation) {
        if self.crashes_left == 0 || !self.rng.random_bool(self.probability) {
            return;
        }
        self.crashes_left -= 1;
        self.crashes.push(Fault::Crash {
            node,
            power_loss: self.rng.random_bool(0.5),
            downtime: self.rng.random_range(STEP_DOWNTIME),
        });
        *self
            .audit
            .step_crashes
            .entry(format!("{operation:?}"))
            .or_default() += 1;
    }

    /// Checks the object the remote store now holds at a key of `shard`:
    /// a complete version a client registered, with its bytes, metadata,
    /// ETag, and a write identity of the shard that no other version has.
    fn check(&mut self, shard: &ShardRef, object: &GetOutput) -> Result<(), String> {
        let metadata = &object.info.metadata;
        let etag = object.info.etag.to_string();
        let Some(op) = metadata.get("op") else {
            return Err(format!(
                "the remote holds an object no client wrote ({etag})"
            ));
        };
        let Some(expected) = self.expected.get(op) else {
            return Err(format!("the remote holds {op}, which no client completed"));
        };
        let Some(identity) = metadata.write_identity() else {
            return Err(format!("the remote holds {op} without a write identity"));
        };
        let wid = identity.parse::<WriteIdentity>().ok();
        if !wid.is_some_and(|wid| {
            wid.cluster == self.cluster && wid.bucket == shard.bucket && wid.shard == shard.shard
        }) {
            return Err(format!(
                "the remote holds {op} with {identity}, not an identity of its shard"
            ));
        }
        if !expected.committable {
            return Err(format!(
                "the remote holds {op}, a body that failed its checks"
            ));
        }
        if object.body != expected.body || object.info.size != expected.body.len() as u64 {
            return Err(format!(
                "the remote holds a partial object of {op}: {} bytes of {}, whose first {} \
                 match",
                object.body.len(),
                expected.body.len(),
                object
                    .body
                    .iter()
                    .zip(&expected.body)
                    .take_while(|(a, b)| a == b)
                    .count()
            ));
        }
        if !expected.remote_etags.contains(&etag) {
            return Err(format!(
                "the remote holds {op} with the ETag {etag}, not one of {:?}",
                expected.remote_etags
            ));
        }
        let (written, etags) = self
            .identities
            .entry(identity.to_owned())
            .or_insert_with(|| (op.to_owned(), BTreeSet::new()));
        if written != op {
            return Err(format!(
                "the remote holds {op} with {identity}, the identity of {written}"
            ));
        }
        etags.insert(etag.clone());
        let counter = match (etag.contains('-'), expected.local_etag.contains('-')) {
            (true, true) => &mut self.audit.multipart,
            (true, false) => &mut self.audit.streamed,
            _ => &mut self.audit.single,
        };
        *counter += 1;
        Ok(())
    }
}

/// What the final logs of every shard hold, for the audit.
#[derive(Default)]
struct Finals {
    /// Each committed version's write identity: its key and local ETag.
    committed: BTreeMap<String, (String, String)>,
    /// The write identities of the versions at each position of a key.
    versions: BTreeMap<(ShardRef, String, Seq), BTreeSet<String>>,
    /// Every `FLUSHED` with a remote ETag: its shard, key, version, and
    /// ETag.
    flushed: Vec<(ShardRef, String, Seq, String)>,
    /// The local upload each committed remote upload ID streamed.
    opened: BTreeMap<String, EpochSeq>,
    /// Local multipart uploads created, and those completed or aborted, as
    /// the final primary's log holds them.
    created: BTreeSet<EpochSeq>,
    closed: BTreeSet<EpochSeq>,
}

impl Finals {
    fn read(cluster: &ClusterId, logs: &BTreeMap<ShardRef, ShardLogs>) -> Self {
        let mut finals = Finals::default();
        for (shard, held) in logs {
            let identity = |position| {
                WriteIdentity::new(cluster.clone(), shard.bucket.clone(), shard.shard, position)
                    .to_string()
            };
            // The uploads' state is the final primary's: a record only
            // another member holds was never committed, as when a primary
            // crashed before its lazy `PART_FLUSHED` reached the others.
            let primary = held.primary.iter().map(|record| (true, record));
            let others = held.others.iter().flatten().map(|record| (false, record));
            for (committed, record) in primary.chain(others) {
                let position = record.position;
                let version = match &record.body {
                    RecordBody::Put(put) => Some((
                        &put.key,
                        put.inherited_identity.unwrap_or(position),
                        &put.etag,
                    )),
                    RecordBody::MpuComplete(complete) => {
                        if committed {
                            finals.closed.insert(complete.upload);
                        }
                        Some((&complete.key, complete.upload, &complete.etag))
                    }
                    RecordBody::MpuCreate(_) if committed => {
                        finals.created.insert(position);
                        None
                    }
                    RecordBody::MpuAbort(abort) if committed => {
                        finals.closed.insert(abort.upload);
                        None
                    }
                    RecordBody::Flushed(flushed) => {
                        if let Some(etag) = &flushed.remote_etag {
                            finals.flushed.push((
                                shard.clone(),
                                flushed.key.clone(),
                                flushed.seq,
                                etag.to_string(),
                            ));
                        }
                        None
                    }
                    RecordBody::PartFlushed(part) => {
                        if committed && part.step == RemoteStep::Opened {
                            finals
                                .opened
                                .insert(part.remote_upload_id.clone(), part.upload);
                        }
                        None
                    }
                    _ => None,
                };
                if let Some((key, written, etag)) = version {
                    let identity = identity(written);
                    finals
                        .committed
                        .insert(identity.clone(), (key.clone(), etag.to_string()));
                    finals
                        .versions
                        .entry((shard.clone(), key.clone(), position.seq))
                        .or_default()
                        .insert(identity);
                }
            }
        }
        // Each log is applied in order, but replicas hold the same records:
        // flushes are checked once each.
        finals.flushed.sort();
        finals.flushed.dedup();
        finals
    }

    /// Whether the local multipart upload opened at `upload` is still
    /// open: created, and neither completed nor aborted.
    fn open_locally(&self, upload: &EpochSeq) -> bool {
        self.created.contains(upload) && !self.closed.contains(upload)
    }
}

/// How often the drain looks at the flushers.
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Waits, for at most `limit`, until the live flushers of every shard of
/// `buckets` have nothing left to do: no key dirty, being flushed, or
/// held in conflict, no stream open, and no remote upload left to abort.
///
/// # Errors
///
/// What was still to do at the deadline.
pub(crate) async fn drain(
    flushers: Arc<Flushers>,
    buckets: Vec<BucketDocument>,
    limit: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let left = busy(&flushers, &buckets);
        if left.is_none() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(left.unwrap_or_default());
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
}

/// What the live flushers of `buckets` still have to do, if anything.
fn busy(flushers: &Flushers, buckets: &[BucketDocument]) -> Option<String> {
    let live: Vec<_> = flushers
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    for bucket in buckets {
        let mut idle = BTreeSet::new();
        for service in &live {
            let Some(status) = service.status(&bucket.bucket_id) else {
                continue;
            };
            if status.orphaned_uploads > 0 {
                let orphans = status.orphaned_uploads;
                return Some(format!("{} has {orphans} uploads to abort", bucket.name));
            }
            for (shard, flusher) in &status.shards {
                let quiet = !flusher.stopped
                    && flusher.dirty == 0
                    && flusher.flushing == 0
                    && flusher.streams == 0
                    && flusher.conflicts.is_empty();
                if !quiet {
                    return Some(format!(
                        "{}/{shard}: {flusher:?}, with the probe {:?} and the window {:?}",
                        bucket.name, status.probe, status.concurrency
                    ));
                }
                idle.insert(*shard);
            }
        }
        if let Some(shard) = bucket.shards.shards().find(|shard| !idle.contains(shard)) {
            return Some(format!("{}/{shard} has no flusher", bucket.name));
        }
    }
    None
}
