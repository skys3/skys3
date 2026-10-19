//! Lifecycle expiration and cleanup of abandoned multipart uploads on a
//! shard's primary (design §8.7).
//!
//! A `local` bucket's lifecycle configuration
//! ([`LifecycleConfiguration`]) lives in its register. Each shard primary
//! evaluates it over its own index, in passes that the node runs
//! periodically ([`run_pass`]):
//!
//! - **Expiration.** A pass reads the shard's entries in key order, a page
//!   at a time, and finds the object versions whose expiry
//!   ([`LifecycleConfiguration::expires_at`]) has passed. Each commits as
//!   an ordinary `DELETE`, conditional on the key still holding the
//!   version that matched: the check runs when the delete is sequenced,
//!   after every earlier write of the key is applied
//!   ([`Shard::commit_if`]), so a version overwritten, retagged, or deleted
//!   since the page was read is left alone, and is judged again by the
//!   next pass. The deletes of a page are sequenced together and share
//!   group commits. As for any delete in a `local` bucket, a `FLUSHED`
//!   then removes the tombstone ([`Tombstones::Remove`]); in a bucket with
//!   a backup target, the backup's flusher does, once it has deleted the
//!   key there ([`Tombstones::Keep`], §8.9).
//! - **Abandoned uploads.** A pass reads the shard's open uploads and
//!   aborts, with an `MPU_ABORT`, each one whose abort time
//!   ([`LifecycleConfiguration::aborts_at`]) has passed. An abort of an
//!   upload that completed or was aborted meanwhile is rejected when it
//!   applies, and changes nothing.
//!
//! **Across primary changes.** A pass keeps no state: what it decided is in
//! the log, and nothing else is. A new primary's next pass starts from its
//! index, which holds every record the old primary committed and every one
//! it rolled forward (design §6.6). An expiration the old primary appended
//! but did not commit is either rolled forward, and the key's version is
//! gone, or truncated, and the new primary expires the version again. Since
//! each delete is conditional on the version, a version is expired by
//! exactly one committed `DELETE`, however often the primary changes.

use std::sync::Arc;

use prometheus_client::metrics::counter::Counter;
use skys3_index::Entry;
use skys3_io::Disk;
use skys3_log::record::{Delete, Flushed, MpuAbort};
use skys3_log::{RecordBody, ShardRef};
use skys3_obs::MetricsRegistry;
use skys3_types::lifecycle::LifecycleConfiguration;
use skys3_types::{BucketDocument, BucketMode, EpochSeq};

use crate::error::ShardError;
use crate::machine::{Effect, Outcome};
use crate::set::ShardSet;
use crate::shard::Shard;

/// How many entries or uploads a pass reads per index transaction, and so
/// the most expirations it sequences together.
const PAGE: usize = 256;

/// What becomes of the tombstone an expiration leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tombstones {
    /// A `FLUSHED` removes it at once: the bucket is never flushed.
    Remove,
    /// It stays until the flusher of the bucket's backup target has
    /// deleted the key there and commits the `FLUSHED` itself (§8.9).
    Keep,
}

/// What a pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    /// Object versions expired: `DELETE`s committed.
    pub expired: u64,
    /// Open uploads aborted.
    pub aborted: u64,
    /// Shards whose pass stopped early, for example because the replica
    /// stopped being the primary.
    pub failed: u64,
}

impl LifecycleReport {
    fn add(&mut self, other: Self) {
        self.expired += other.expired;
        self.aborted += other.aborted;
        self.failed += other.failed;
    }
}

/// The lifecycle metrics.
#[derive(Debug, Clone, Default)]
pub struct LifecycleMetrics {
    expired: Counter,
    aborted: Counter,
}

impl LifecycleMetrics {
    /// Registers the lifecycle metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "lifecycle_expired",
            "Object versions that lifecycle rules expired on this node's shard primaries.",
            metrics.expired.clone(),
        );
        registry.register(
            "lifecycle_aborted_uploads",
            "Incomplete multipart uploads that lifecycle rules aborted on this node's shard \
             primaries.",
            metrics.aborted.clone(),
        );
        metrics
    }

    fn record(&self, report: LifecycleReport) {
        self.expired.inc_by(report.expired);
        self.aborted.inc_by(report.aborted);
    }
}

/// Runs one lifecycle pass over every shard of `buckets` whose replica on
/// this node is the primary: the shards of each `local` bucket with a
/// lifecycle configuration, at the wall-clock time `now_ms` (milliseconds
/// since the Unix epoch), leaving each bucket's tombstones as
/// `tombstones` says. A shard whose pass fails is reported in
/// [`LifecycleReport::failed`] and tried again by the next pass.
pub async fn run_pass<D: Disk>(
    set: &ShardSet<D>,
    buckets: &[BucketDocument],
    now_ms: u64,
    metrics: &LifecycleMetrics,
    tombstones: impl Fn(&BucketDocument) -> Tombstones,
) -> LifecycleReport {
    let mut total = LifecycleReport::default();
    for bucket in buckets {
        let (BucketMode::Local, Some(config)) = (bucket.mode, &bucket.lifecycle) else {
            continue;
        };
        for shard in bucket.shards.shards() {
            let shard = ShardRef::new(bucket.bucket_id.clone(), shard);
            let Some(replica) = set.get(&shard).await else {
                continue;
            };
            // A primary serves once its members hold its whole log and it
            // applied every record of it (§6.5): a pass reads what the
            // earlier primaries committed.
            if replica.is_stopped() || replica.role().follows() || !replica.is_serving() {
                continue;
            }
            let report = match expire(&replica, config, now_ms, tombstones(bucket)).await {
                Ok(report) => report,
                Err((report, error)) => {
                    tracing::debug!(%shard, %error, "a lifecycle pass stopped early");
                    LifecycleReport {
                        failed: 1,
                        ..report
                    }
                }
            };
            metrics.record(report);
            total.add(report);
        }
    }
    total
}

/// Runs one lifecycle pass of `config` over `shard`, at `now_ms`, leaving
/// the tombstones of expired keys as `tombstones` says.
///
/// # Errors
///
/// What the pass did before it stopped, and the error that stopped it: a
/// replica that is no longer the primary, or a sealed shard, refuses the
/// pass's writes.
pub async fn expire<D: Disk>(
    shard: &Shard<D>,
    config: &LifecycleConfiguration,
    now_ms: u64,
    tombstones: Tombstones,
) -> Result<LifecycleReport, (LifecycleReport, ShardError)> {
    let mut report = LifecycleReport::default();
    if config.expires_objects() {
        let mut after = None;
        loop {
            let page = shard.entries(after.clone(), PAGE).await;
            let page = page.map_err(|error| (report, error))?;
            let full = page.len() == PAGE;
            after = page.last().map(|(key, _)| key.clone());
            let due: Vec<_> = page
                .into_iter()
                .filter(|(key, entry)| is_due(config, key, entry, now_ms))
                .map(|(key, entry)| (key, entry.version))
                .collect();
            let (expired, failure) = expire_versions(shard, due, tombstones).await;
            report.expired += expired;
            if let Some(error) = failure {
                return Err((report, error));
            }
            if !full {
                break;
            }
        }
    }
    if config.aborts_uploads() {
        let mut after = None;
        loop {
            let page = shard.uploads("", after.clone(), PAGE).await;
            let page = page.map_err(|error| (report, error))?;
            let full = page.len() == PAGE;
            after = page
                .last()
                .map(|(key, upload, _)| (key.clone(), Some(*upload)));
            for (key, upload, state) in page {
                if config
                    .aborts_at(&key, state.initiated_ms)
                    .is_none_or(|at| at > now_ms)
                {
                    continue;
                }
                let abort = RecordBody::MpuAbort(MpuAbort { key, upload });
                let committed = shard.commit(abort).await;
                let committed = committed.map_err(|error| (report, error))?;
                if let Outcome::Applied(Effect::Aborted { .. }) = committed.outcome {
                    report.aborted += 1;
                }
            }
            if !full {
                break;
            }
        }
    }
    Ok(report)
}

/// Whether `entry`'s object has expired under `config` at `now_ms`.
fn is_due(config: &LifecycleConfiguration, key: &str, entry: &Entry, now_ms: u64) -> bool {
    entry.object.as_ref().is_some_and(|object| {
        config
            .expires_at(key, object.size, &object.tags, object.last_modified_ms)
            .is_some_and(|at| at <= now_ms)
    })
}

/// Commits a `DELETE` of each key in `due` that still holds the version
/// given with it, sequenced together, and then, with
/// [`Tombstones::Remove`], the `FLUSHED` records that remove their
/// tombstones. Returns how many deletes committed, and the first error of
/// those that did not.
async fn expire_versions<D: Disk>(
    shard: &Shard<D>,
    due: Vec<(String, EpochSeq)>,
    tombstones: Tombstones,
) -> (u64, Option<ShardError>) {
    if due.is_empty() {
        return (0, None);
    }
    let versions: Arc<[EpochSeq]> = due.iter().map(|(_, version)| *version).collect();
    let (keys, deletes): (Vec<String>, Vec<RecordBody>) = due
        .into_iter()
        .map(|(key, _)| (key.clone(), RecordBody::Delete(Delete { key })))
        .unzip();
    let results = shard
        .commit_all_if(deletes, |at, entry| {
            let unchanged =
                entry.is_some_and(|entry| entry.version == versions[at] && entry.object.is_some());
            if unchanged { Ok(()) } else { Err(()) }
        })
        .await;
    let mut removals = Vec::new();
    let mut failure = None;
    for (key, result) in keys.into_iter().zip(results) {
        match result {
            Ok(Ok(committed)) => removals.push(RecordBody::Flushed(Flushed {
                key,
                seq: committed.position.seq,
                remote_etag: None,
                remote_version_id: None,
            })),
            // The key changed since the page was read.
            Ok(Err(())) => {}
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    let expired = removals.len() as u64;
    if tombstones == Tombstones::Remove && !removals.is_empty() {
        // A local bucket without a backup target is never flushed, so a
        // FLUSHED removes each tombstone unless the key was written again.
        // One that is lost only takes space.
        let removed = shard.commit_all_if(removals, |_, _| Ok::<(), ()>(())).await;
        if let Some(error) = removed.into_iter().find_map(Result::err) {
            tracing::debug!(shard = %shard.shard(), %error, "an expired key's tombstone was not removed");
        }
    }
    (expired, failure)
}
