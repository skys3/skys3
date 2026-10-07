//! The encoder of a shard's primary (design §8.2, §8.4): it turns
//! replicated objects into erasure-coded stripes, publish before retire.
//!
//! For each object that qualifies, an [`Encoder`]:
//!
//! 1. starts an attempt, whose ID ([`AttemptId`]) is the primary's epoch
//!    and a number drawn from blocks the shard's index reserves durably
//!    ([`Index::reserve_attempts`](skys3_index::Index::reserve_attempts)),
//!    so no number is used twice in an epoch, across restarts too;
//! 2. reads the object from its local replica, stripe by stripe, up to
//!    `ec_stripe_data_bytes` each, and encodes each stripe with the
//!    current codec ([`current_codec`]);
//! 3. plans each stripe's geometry and nodes ([`FragmentPlanner`]) and
//!    writes its fragments in parallel through a [`FragmentWriter`], which
//!    returns an ID only once a fragment is durable. A failed write
//!    re-plans the stripe without the nodes that failed, a few times,
//!    before the attempt is abandoned;
//! 4. once every fragment of every stripe is durable, commits an
//!    `EC_PUBLISH` record ([`EcPublish`]) naming the version it read. It
//!    commits on every member like any write, and is rejected when
//!    applied if the version is no longer the key's current one.
//!
//! Members then drop their replicated copy through compaction, once they
//! know the record committed (`skys3_shard::compaction`). A crash before
//! step 4 commits leaves the replicas authoritative and some orphan
//! fragments; one after leaves extra replicas that compaction drops.
//!
//! The [`Attempts`] of the encoder are the fence orphan reclamation
//! ([`crate::orphans`]) needs: an attempt is in progress from step 1 until
//! its record commits or the encoder abandons it before appending the
//! record. The record is appended only while the shard still sequences in
//! the attempt's epoch ([`Shard::commit_in`]), so a later primary never
//! publishes it.

mod attempts;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_config::BucketSettings;
use skys3_coord::{FragmentPlanner, NoGeometry, StripeRequest};
use skys3_index::{Entry, EntryState, IndexError, ObjectVersion, Payload};
use skys3_io::{Disk, WallClock};
use skys3_log::record::{EcPublish, ExtentRef, LogRecord, MAX_HEADER_LEN, MAX_STRIPES, RecordBody};
use skys3_shard::{Effect, Outcome, Rejection, Role, Shard, ShardError};
use skys3_types::{AttemptId, CodedStripe, Epoch, EpochSeq, NodeId, Seq};
use tokio::task::JoinSet;

pub(crate) use attempts::{AttemptNumbers, Publication, Writing, publish};
pub use attempts::{AttemptState, Attempts};

use crate::fragment::{FragmentHeader, MAX_FRAGMENT_LEN, ObjectMeta, PartSize, StripeInfo};
use crate::transfer::{FragmentWriter, TransferError};
use crate::{EcError, current_codec};

// The configuration bounds stripes by the fragment format's limit.
const _: () = assert!(MAX_FRAGMENT_LEN == skys3_config::MAX_FRAGMENT_BYTES);

/// How many entries a scan reads at a time.
const SCAN_PAGE: usize = 256;

/// What decides which objects an encoder codes and how (§8.2): the
/// bucket's `ec_*` settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncoderSettings {
    /// `ec_min_object_bytes`: smaller objects stay replicated.
    pub min_object_bytes: u64,
    /// `ec_stripe_data_bytes`: the most data bytes of one stripe.
    pub stripe_data_bytes: u64,
    /// `ec_after_seconds`: how long after its `Last-Modified` an object
    /// is first encoded.
    pub after: Duration,
    /// How many times a stripe whose fragment writes failed is planned
    /// again, without the nodes that failed, before the attempt is
    /// abandoned.
    pub replans: usize,
    /// Whether the bucket has a backup target (§8.9): a version is then
    /// encoded only once the backup holds it, its entry clean, since the
    /// flusher sends it from the local replica that encoding drops.
    pub after_backup: bool,
}

impl EncoderSettings {
    /// The settings of a bucket.
    #[must_use]
    pub fn from_bucket(settings: &BucketSettings) -> Self {
        Self {
            min_object_bytes: settings.ec_min_object_bytes,
            stripe_data_bytes: settings.ec_stripe_data_bytes,
            after: settings.ec_after(),
            replans: 3,
            after_backup: settings.backup_target.is_some(),
        }
    }
}

/// Why an object was not encoded, though nothing failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Skip {
    /// The key has no current object: absent, or deleted.
    Absent,
    /// The version is coded already.
    Coded,
    /// This replica holds no bytes of the version.
    NoLocalBytes,
    /// The object is smaller than `ec_min_object_bytes`.
    TooSmall,
    /// The object was written less than `ec_after_seconds` ago.
    TooRecent,
    /// The bucket's backup target does not hold the version yet (§8.9).
    NotBackedUp,
    /// The object needs more stripes than one `EC_PUBLISH` record can
    /// name; it stays replicated.
    TooLarge,
    /// The cluster supports no geometry: encoding pauses (§8.3).
    Paused(NoGeometry),
}

/// What [`Encoder::encode`] did with an object.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Encoded {
    /// Its `EC_PUBLISH` committed and was applied, at this position.
    Published(EpochSeq),
    /// Its `EC_PUBLISH` committed and was rejected when applied: the
    /// version was superseded while it was encoded. Its fragments are
    /// orphans.
    Rejected(Rejection),
    /// It was not encoded.
    Skipped(Skip),
}

/// Why an attempt failed. Unless the error is [`EncodeError::Publish`],
/// nothing was appended, the replicas stay authoritative, and any fragment
/// written is an orphan.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EncodeError {
    /// The shard refused a read.
    #[error(transparent)]
    Shard(#[from] ShardError),
    /// The index failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// The nodes left after failed writes support no geometry.
    #[error(transparent)]
    NoGeometry(#[from] NoGeometry),
    /// The codec failed.
    #[error(transparent)]
    Codec(#[from] EcError),
    /// Some fragment of a stripe could not be written, after every
    /// re-plan.
    #[error("stripe {stripe}'s fragments could not all be written: {source}")]
    Fragments {
        /// The stripe.
        stripe: u32,
        /// The last write's failure.
        source: TransferError,
    },
    /// The object's bytes are not all on this replica, or do not add up.
    #[error("the object's bytes cannot be read: {0}")]
    Bytes(String),
    /// The stripes do not make a valid `EC_PUBLISH` record.
    #[error("the stripes cannot be published: {0}")]
    Layout(String),
    /// The attempt was abandoned, by orphan reclamation's fence, before it
    /// appended its record.
    #[error("attempt {0} was abandoned")]
    Abandoned(AttemptId),
    /// The `EC_PUBLISH` record may be appended, but its commit was not
    /// confirmed. The attempt stays in progress until it commits.
    #[error("the EC_PUBLISH record was not confirmed: {0}")]
    Publish(ShardError),
}

/// One step of an attempt, as an observer sees it: what a simulation
/// crashes nodes at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeEvent {
    /// The object's key.
    pub key: String,
    /// The attempt.
    pub attempt: AttemptId,
    /// The step.
    pub step: EncodeStep,
}

/// The steps of an attempt, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeStep {
    /// The attempt started; the object has `stripes` stripes.
    Started {
        /// The object's stripe count.
        stripes: u32,
    },
    /// A stripe was planned and encoded, and its fragment writes start.
    StripeStarted {
        /// The stripe's number.
        stripe: u32,
        /// The nodes of its fragments, by index.
        nodes: Vec<NodeId>,
    },
    /// A fragment is durable on `node`; `written` of the stripe's are.
    FragmentWritten {
        /// The stripe's number.
        stripe: u32,
        /// The fragment's index.
        index: u8,
        /// Its node.
        node: NodeId,
        /// How many of the stripe's fragments are durable now.
        written: usize,
    },
    /// Every fragment of the stripe is durable.
    StripeWritten {
        /// The stripe's number.
        stripe: u32,
    },
    /// The `EC_PUBLISH` record is sequenced, and not yet known to commit.
    Appended,
    /// The record committed: `published` if it was applied, false if the
    /// version was superseded.
    Committed {
        /// The record's position.
        position: EpochSeq,
        /// Whether the version is coded now.
        published: bool,
    },
    /// The attempt failed before it appended its record.
    Abandoned,
}

/// What a scan of the shard did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanReport {
    /// Objects whose `EC_PUBLISH` committed and applied.
    pub published: usize,
    /// Objects whose `EC_PUBLISH` was rejected as superseded.
    pub rejected: usize,
    /// Objects whose attempt failed.
    pub failed: usize,
    /// Why encoding is paused, if it is: the cluster supports no geometry.
    pub paused: Option<NoGeometry>,
}

/// The cluster's current fragment planner: the eligible nodes and the
/// `[ec]` policy, as the node knows them when it is asked.
pub type PlannerSource = Arc<dyn Fn() -> FragmentPlanner + Send + Sync>;

/// Sees every step of every attempt.
pub type EncodeObserver = Arc<dyn Fn(&EncodeEvent) + Send + Sync>;

/// A shard primary's encoder (§8.2, §8.4): it codes the shard's objects
/// that qualify, stripe by stripe, and publishes each with `EC_PUBLISH`
/// once every fragment is durable. The crate documentation lists the
/// steps.
pub struct Encoder<D: Disk, W: FragmentWriter> {
    shard: Shard<D>,
    writer: Arc<W>,
    planner: PlannerSource,
    clock: Arc<dyn WallClock>,
    settings: EncoderSettings,
    attempts: Attempts,
    numbers: AttemptNumbers,
    observer: Option<EncodeObserver>,
}

impl<D: Disk, W: FragmentWriter> Encoder<D, W> {
    /// The encoder of `shard`, which writes fragments through `writer`,
    /// places stripes with the planner `planner` returns, and reads the
    /// time of day from `clock`.
    pub fn new(
        shard: Shard<D>,
        writer: Arc<W>,
        planner: PlannerSource,
        clock: Arc<dyn WallClock>,
        settings: EncoderSettings,
    ) -> Self {
        Self {
            shard,
            writer,
            planner,
            clock,
            settings,
            attempts: Attempts::default(),
            numbers: AttemptNumbers::default(),
            observer: None,
        }
    }

    /// Reports every step of every attempt to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: EncodeObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Tracks this encoder's attempts in `attempts`, which the shard's
    /// [`OrphanJudge`](crate::OrphanJudge) on this node shares: one tracker
    /// per shard and node, for every kind of fragment-writing attempt.
    #[must_use]
    pub fn with_attempts(mut self, attempts: Attempts) -> Self {
        self.attempts = attempts;
        self
    }

    /// The attempts in progress.
    #[must_use]
    pub fn attempts(&self) -> &Attempts {
        &self.attempts
    }

    /// Encodes every object of the shard that qualifies, one at a time,
    /// while this replica leads the shard; does nothing while the cluster
    /// supports no geometry.
    pub async fn scan(&self) -> ScanReport {
        let mut report = ScanReport::default();
        self.attempts.settle(self.shard.applied());
        if let Err(paused) = (self.planner)().geometry() {
            report.paused = Some(paused);
            return report;
        }
        let mut after = None;
        loop {
            if !self.leads() {
                return report;
            }
            let page = match self.shard.entries(after.clone(), SCAN_PAGE).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::debug!(shard = %self.shard.shard(), %error, "an encoder scan stopped");
                    return report;
                }
            };
            let Some((last, _)) = page.last() else {
                return report;
            };
            after = Some(last.clone());
            for (key, entry) in page {
                if self.qualifies(&entry).is_err() {
                    continue;
                }
                match self.encode(&key).await {
                    Ok(Encoded::Published(_)) => report.published += 1,
                    Ok(Encoded::Rejected(_)) => report.rejected += 1,
                    Ok(Encoded::Skipped(Skip::Paused(paused))) => {
                        report.paused = Some(paused);
                        return report;
                    }
                    Ok(Encoded::Skipped(_)) => {}
                    Err(error) => {
                        report.failed += 1;
                        tracing::info!(shard = %self.shard.shard(), key, %error, "an object was not encoded");
                    }
                }
            }
        }
    }

    /// Scans the shard every `interval` while this replica leads it, until
    /// the returned future is dropped.
    pub async fn run(self, interval: Duration) {
        loop {
            if self.leads() {
                let report = self.scan().await;
                if let Some(paused) = &report.paused {
                    tracing::debug!(shard = %self.shard.shard(), %paused, "encoding is paused");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Encodes `key`'s current version if it qualifies, and publishes it.
    ///
    /// # Errors
    ///
    /// [`EncodeError`] if the attempt failed.
    pub async fn encode(&self, key: &str) -> Result<Encoded, EncodeError> {
        let plan = self.shard.plan(key).await?;
        let Some(entry) = plan.entry else {
            return Ok(Encoded::Skipped(Skip::Absent));
        };
        if let Err(skip) = self.qualifies(&entry) {
            return Ok(Encoded::Skipped(skip));
        }
        let object = entry
            .object
            .as_ref()
            .expect("a qualifying entry has an object");
        if plan.layout.is_empty() {
            return Ok(Encoded::Skipped(Skip::NoLocalBytes));
        }
        let planner = (self.planner)();
        if let Err(paused) = planner.geometry() {
            return Ok(Encoded::Skipped(Skip::Paused(paused)));
        }
        let stripes = object.size.div_ceil(self.settings.stripe_data_bytes.max(1));
        let Some(stripes) = u32::try_from(stripes)
            .ok()
            .filter(|count| u64::from(*count) <= MAX_STRIPES as u64)
            .filter(|count| fits(key, &entry, *count, &planner))
        else {
            return Ok(Encoded::Skipped(Skip::TooLarge));
        };

        let attempt = self.numbers.next(&self.shard)?;
        self.attempts.begin(attempt);
        // Forgets the attempt if this future is dropped before it appends.
        let _writing = Writing {
            attempts: &self.attempts,
            attempt,
        };
        self.emit(key, attempt, EncodeStep::Started { stripes });
        let request = Attempt {
            key,
            attempt,
            entry: &entry,
            object,
            stripes,
        };
        let publish = match self.write(&request, planner, &plan.layout).await {
            Ok(publish) => publish,
            Err(error) => {
                self.attempts.finish(attempt);
                self.emit(key, attempt, EncodeStep::Abandoned);
                return Err(error);
            }
        };
        self.publish(key, attempt, publish).await
    }

    /// Whether `entry` qualifies for encoding now, but for the cluster's
    /// geometry and the bytes on this replica.
    fn qualifies(&self, entry: &Entry) -> Result<(), Skip> {
        let Some(object) = &entry.object else {
            return Err(Skip::Absent);
        };
        if object.coded.is_some() {
            return Err(Skip::Coded);
        }
        if matches!(object.payload, Payload::None) {
            return Err(Skip::NoLocalBytes);
        }
        if object.size < self.settings.min_object_bytes.max(1) {
            return Err(Skip::TooSmall);
        }
        let now = u64::try_from(self.clock.now().as_millis()).unwrap_or(u64::MAX);
        let after = u64::try_from(self.settings.after.as_millis()).unwrap_or(u64::MAX);
        if now.saturating_sub(object.last_modified_ms) < after {
            return Err(Skip::TooRecent);
        }
        if self.settings.after_backup && entry.state != EntryState::Clean {
            return Err(Skip::NotBackedUp);
        }
        Ok(())
    }

    /// Whether this replica leads the shard.
    fn leads(&self) -> bool {
        leads(&self.shard)
    }

    /// Writes every stripe's fragments and returns the record that
    /// publishes them.
    async fn write(
        &self,
        request: &Attempt<'_>,
        mut planner: FragmentPlanner,
        layout: &[ExtentRef],
    ) -> Result<EcPublish, EncodeError> {
        let size = request.object.size;
        let mut reader = StripeReader::new(&self.shard, layout, size)?;
        let mut stripes = Vec::with_capacity(request.stripes as usize);
        for number in 0..request.stripes {
            if self.attempts.state(request.attempt) != Some(AttemptState::Writing) {
                return Err(EncodeError::Abandoned(request.attempt));
            }
            let offset = u64::from(number) * self.settings.stripe_data_bytes;
            let len = (size - offset).min(self.settings.stripe_data_bytes);
            let data = reader.read(offset, len).await?;
            let stripe = self
                .write_stripe(request, &mut planner, number, offset, data)
                .await?;
            self.emit(
                request.key,
                request.attempt,
                EncodeStep::StripeWritten { stripe: number },
            );
            stripes.push(stripe);
        }
        let publish = EcPublish {
            key: request.key.to_owned(),
            version: request.entry.version,
            etag: request.object.local_etag.clone(),
            attempt: request.attempt,
            size,
            stripes,
        };
        let record = LogRecord {
            shard: self.shard.shard().clone(),
            position: EpochSeq::new(Epoch::new(u64::MAX), Seq::new(u64::MAX)),
            body: RecordBody::EcPublish(publish),
        };
        record
            .check()
            .map_err(|error| EncodeError::Layout(error.to_string()))?;
        let RecordBody::EcPublish(publish) = record.body else {
            unreachable!("the record holds the EC_PUBLISH it was built with");
        };
        Ok(publish)
    }

    /// Encodes one stripe and writes its fragments, planning it again
    /// without the nodes whose writes failed.
    async fn write_stripe(
        &self,
        request: &Attempt<'_>,
        planner: &mut FragmentPlanner,
        number: u32,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<CodedStripe, EncodeError> {
        let shard = self.shard.shard();
        let codec = current_codec();
        let len = data.len() as u64;
        let mut avoid: Vec<NodeId> = Vec::new();
        let mut last = None;
        for _ in 0..=self.settings.replans {
            let plan = planner.plan(
                &StripeRequest::new(&shard.bucket, shard.shard, request.key, number, len)
                    .avoid(&avoid),
            )?;
            let fragments = codec.encode(plan.geometry, &data)?;
            self.emit(
                request.key,
                request.attempt,
                EncodeStep::StripeStarted {
                    stripe: number,
                    nodes: plan.nodes.clone(),
                },
            );
            let info = StripeInfo {
                number,
                count: request.stripes,
                offset,
                data_len: len,
                geometry: plan.geometry,
                codec: codec.id(),
            };
            let mut writes = JoinSet::new();
            for (index, (node, fragment)) in plan.nodes.iter().zip(fragments).enumerate() {
                let header = request.header(shard, info, index);
                let (writer, node) = (Arc::clone(&self.writer), node.clone());
                writes.spawn(async move {
                    let result = writer.write(&node, &header, Bytes::from(fragment)).await;
                    (index, node, result)
                });
            }
            let mut ids = vec![None; plan.nodes.len()];
            let mut written = 0;
            while let Some(joined) = writes.join_next().await {
                let (index, node, result) = joined.map_err(|error| EncodeError::Fragments {
                    stripe: number,
                    source: TransferError::Invalid(error.to_string()),
                })?;
                match result {
                    Ok(id) => {
                        ids[index] = Some(id);
                        written += 1;
                        self.emit(
                            request.key,
                            request.attempt,
                            EncodeStep::FragmentWritten {
                                stripe: number,
                                index: index as u8,
                                node,
                                written,
                            },
                        );
                    }
                    Err(error) => {
                        tracing::debug!(%node, %error, "a fragment write failed");
                        avoid.push(node);
                        last = Some(error);
                    }
                }
            }
            if let Some(ids) = ids.into_iter().collect::<Option<Vec<_>>>() {
                return plan
                    .locate(number, offset, len, codec.id(), &ids)
                    .map_err(|error| EncodeError::Layout(error.to_string()));
            }
        }
        Err(EncodeError::Fragments {
            stripe: number,
            source: last.expect("a failed stripe had a failed write"),
        })
    }

    /// Commits the `EC_PUBLISH` record of an attempt whose fragments are
    /// all durable.
    async fn publish(
        &self,
        key: &str,
        attempt: AttemptId,
        publish_record: EcPublish,
    ) -> Result<Encoded, EncodeError> {
        let body = RecordBody::EcPublish(publish_record);
        let appended = || self.emit(key, attempt, EncodeStep::Appended);
        let committed = match publish(&self.shard, &self.attempts, attempt, body, appended).await {
            Publication::Committed(committed) => committed,
            Publication::Abandoned => {
                self.emit(key, attempt, EncodeStep::Abandoned);
                return Err(EncodeError::Abandoned(attempt));
            }
            Publication::NotAppended(error) => {
                self.emit(key, attempt, EncodeStep::Abandoned);
                return Err(EncodeError::Publish(error));
            }
            Publication::Unconfirmed(error) => return Err(EncodeError::Publish(error)),
        };
        let published = committed.outcome == Outcome::Applied(Effect::Published);
        self.emit(
            key,
            attempt,
            EncodeStep::Committed {
                position: committed.position,
                published,
            },
        );
        Ok(match committed.outcome {
            Outcome::Rejected(rejection) => Encoded::Rejected(rejection),
            _ => Encoded::Published(committed.position),
        })
    }

    fn emit(&self, key: &str, attempt: AttemptId, step: EncodeStep) {
        if let Some(observer) = &self.observer {
            observer(&EncodeEvent {
                key: key.to_owned(),
                attempt,
                step,
            });
        }
    }
}

/// What one attempt encodes.
struct Attempt<'a> {
    key: &'a str,
    attempt: AttemptId,
    entry: &'a Entry,
    object: &'a ObjectVersion,
    stripes: u32,
}

impl Attempt<'_> {
    /// The header of fragment `index` of the stripe `stripe`.
    fn header(
        &self,
        shard: &skys3_log::ShardRef,
        stripe: StripeInfo,
        index: usize,
    ) -> FragmentHeader {
        // A geometry has at most 255 fragments.
        let index = index as u8;
        fragment_header(shard, self.key, self.entry, self.attempt, stripe, index)
    }
}

/// Whether `shard`'s replica on this node leads the shard and serves: what
/// every fragment-writing attempt waits for.
pub(crate) fn leads<D: Disk>(shard: &Shard<D>) -> bool {
    matches!(shard.role(), Role::Primary | Role::Alone) && shard.is_serving()
}

/// The header of fragment `index` of stripe `stripe` of `key`'s version
/// `entry`, written by `attempt` (§8.4).
///
/// # Panics
///
/// If `entry` has no object: only an object's fragments are written.
pub(crate) fn fragment_header(
    shard: &skys3_log::ShardRef,
    key: &str,
    entry: &Entry,
    attempt: AttemptId,
    stripe: StripeInfo,
    index: u8,
) -> FragmentHeader {
    let object = entry.object.as_ref().expect("fragments are an object's");
    let parts = match &object.payload {
        Payload::Parts { parts, .. } => parts
            .iter()
            .map(|part| PartSize {
                number: part.number,
                size: part.size,
            })
            .collect(),
        _ => Vec::new(),
    };
    FragmentHeader {
        shard: shard.clone(),
        key: key.to_owned(),
        version: entry.version,
        attempt,
        stripe,
        index,
        object: ObjectMeta {
            size: object.size,
            last_modified_ms: object.last_modified_ms,
            etag: object.local_etag.clone(),
            identity: object.write_identity.unwrap_or(entry.version),
            metadata: object.metadata.clone(),
            tags: object.tags.clone(),
            checksums: object.checksums.clone(),
            parts,
        },
    }
}

/// Whether an `EC_PUBLISH` of `stripes` stripes of `key` fits a record's
/// header, at the widest geometry and the longest node name the planner
/// knows. The record is checked exactly before it is appended; this only
/// avoids writing fragments that could never be published.
fn fits(key: &str, entry: &Entry, stripes: u32, planner: &FragmentPlanner) -> bool {
    let etag = entry
        .object
        .as_ref()
        .map_or(0, |o| o.local_etag.as_str().len());
    let node = planner
        .topology()
        .candidates()
        .map(|c| c.node.as_str().len())
        .max()
        .unwrap_or(0) as u64;
    let fragments = planner.policy().widest().total_fragments() as u64;
    let fixed = 1024 + 2 * key.len() as u64 + etag as u64;
    let stripe = 12 + fragments * (1 + node + 16);
    fixed + u64::from(stripes) * stripe <= u64::from(MAX_HEADER_LEN)
}

/// Reads an object's bytes from this replica's log, stripe by stripe.
struct StripeReader<'a, D: Disk> {
    shard: &'a Shard<D>,
    /// Each extent with its offset in the object.
    extents: Vec<(u64, ExtentRef)>,
    /// The extent read last, by its index, and its bytes.
    cached: Option<(usize, Bytes)>,
}

impl<'a, D: Disk> StripeReader<'a, D> {
    fn new(shard: &'a Shard<D>, layout: &[ExtentRef], size: u64) -> Result<Self, EncodeError> {
        let mut extents = Vec::with_capacity(layout.len());
        let mut offset = 0u64;
        for extent in layout {
            extents.push((offset, *extent));
            offset += u64::from(extent.len);
        }
        if offset != size {
            return Err(EncodeError::Bytes(format!(
                "the layout holds {offset} bytes of a {size}-byte object"
            )));
        }
        Ok(Self {
            shard,
            extents,
            cached: None,
        })
    }

    /// Bytes `offset..offset + len` of the object.
    async fn read(&mut self, offset: u64, len: u64) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::with_capacity(len as usize);
        let end = offset + len;
        let first = self
            .extents
            .partition_point(|(start, extent)| start + u64::from(extent.len) <= offset);
        for index in first..self.extents.len() {
            let (start, extent) = self.extents[index];
            if start >= end {
                break;
            }
            let bytes = match &self.cached {
                Some((cached, bytes)) if *cached == index => bytes.clone(),
                _ => {
                    let bytes = self.shard.payload(extent.position).await?;
                    if bytes.len() as u64 != u64::from(extent.len) {
                        return Err(EncodeError::Bytes(format!(
                            "the payload at {} has {} bytes, not {}",
                            extent.position,
                            bytes.len(),
                            extent.len
                        )));
                    }
                    self.cached = Some((index, bytes.clone()));
                    bytes
                }
            };
            let from = offset.saturating_sub(start) as usize;
            let to = (end - start).min(u64::from(extent.len)) as usize;
            out.extend_from_slice(&bytes[from..to]);
        }
        Ok(out)
    }
}
