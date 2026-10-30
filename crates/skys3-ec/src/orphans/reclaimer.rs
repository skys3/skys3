//! A fragment node's side of orphan reclamation: it asks about fragments
//! that have been on it for `fragment_orphan_after_seconds`, and reclaims
//! those their shard's primary judges orphans.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_io::{Clock, Disk, MonoTime};
use skys3_log::ShardRef;
use skys3_types::{FragmentId, NodeId};

use super::wire::MAX_SUSPECTS;
use super::{OrphanConfirmer, Suspect, Verdict};
use crate::FragmentStore;

/// A fragment the reclaimer reclaimed, as an observer sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reclaimed {
    /// The node that held it.
    pub node: NodeId,
    /// Its shard.
    pub shard: ShardRef,
    /// What its header said of it.
    pub suspect: Suspect,
}

/// Sees every fragment reclaimed.
pub type ReclaimObserver = Arc<dyn Fn(&Reclaimed) + Send + Sync>;

/// What one sweep did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Fragments asked about.
    pub asked: usize,
    /// Fragments reclaimed as orphans.
    pub reclaimed: usize,
    /// Fragments a committed layout references, kept for this life.
    pub referenced: usize,
    /// Fragments whose attempt is in progress, asked about again later.
    pub in_progress: usize,
    /// Fragments no primary gave a verdict on, asked about again at the
    /// next sweep.
    pub unanswered: usize,
}

/// What the reclaimer knows of a fragment in this life.
#[derive(Debug, Clone, Copy)]
enum Seen {
    /// Not judged referenced; it may be asked about once it has waited
    /// `fragment_orphan_after_seconds` since then.
    Since(MonoTime),
    /// A committed layout references it; it is not asked about again in
    /// this life. Releasing it is fragment release's job (§8.7).
    Referenced,
}

/// A fragment node's orphan reclamation (§8.4): it sweeps the node's
/// fragment stores, asks the primaries of the fragments' shards about
/// those it has held for `orphan_after`, and reclaims the orphans.
///
/// A fragment's age counts from when this life first saw it, since the
/// fragment map keeps no time: a restart only delays reclamation. A
/// fragment whose header cannot be read is left alone.
pub struct OrphanReclaimer<D: Disk, C: OrphanConfirmer> {
    node: NodeId,
    stores: Vec<FragmentStore<D>>,
    confirmer: C,
    clock: Arc<dyn Clock>,
    orphan_after: Duration,
    seen: Mutex<HashMap<FragmentId, Seen>>,
    observer: Option<ReclaimObserver>,
}

impl<D: Disk, C: OrphanConfirmer> OrphanReclaimer<D, C> {
    /// The reclaimer of node `node`, whose fragment stores are `stores`,
    /// asking through `confirmer` about fragments held for `orphan_after`
    /// (`fragment_orphan_after_seconds`) as `clock` measures it.
    #[must_use]
    pub fn new(
        node: NodeId,
        stores: Vec<FragmentStore<D>>,
        confirmer: C,
        clock: Arc<dyn Clock>,
        orphan_after: Duration,
    ) -> Self {
        Self {
            node,
            stores,
            confirmer,
            clock,
            orphan_after,
            seen: Mutex::default(),
            observer: None,
        }
    }

    /// Reports every fragment reclaimed to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: ReclaimObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Sweeps every `interval` until the returned future is dropped.
    pub async fn run(self, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let report = self.sweep().await;
            if report.reclaimed > 0 || report.unanswered > 0 {
                tracing::debug!(node = %self.node, ?report, "an orphan sweep ended");
            }
        }
    }

    /// Asks about every fragment held for `orphan_after` and not known to
    /// be referenced, and reclaims the orphans.
    pub async fn sweep(&self) -> SweepReport {
        let mut report = SweepReport::default();
        let mut due: BTreeMap<ShardRef, Vec<(usize, Suspect)>> = BTreeMap::new();
        for (store, id) in self.due() {
            // A header that cannot be read names no shard to ask.
            let Ok(header) = self.stores[store].header(id).await else {
                continue;
            };
            let suspect = Suspect {
                id,
                key: header.key,
                attempt: header.attempt,
            };
            due.entry(header.shard).or_default().push((store, suspect));
        }
        for (shard, fragments) in due {
            for batch in fragments.chunks(MAX_SUSPECTS) {
                self.ask(&shard, batch, &mut report).await;
            }
        }
        report
    }

    /// The fragments, by store, that have waited `orphan_after` since this
    /// life first saw them. Forgets fragments no store holds any more.
    fn due(&self) -> Vec<(usize, FragmentId)> {
        let now = self.clock.now();
        let mut seen = self.seen();
        let mut held = HashMap::with_capacity(seen.len());
        let mut due = Vec::new();
        for (store, fragments) in self.stores.iter().enumerate() {
            for id in fragments.ids() {
                let state = *seen.entry(id).or_insert(Seen::Since(now));
                if let Seen::Since(since) = state
                    && now.saturating_duration_since(since) >= self.orphan_after
                {
                    due.push((store, id));
                }
                held.insert(id, state);
            }
        }
        *seen = held;
        due
    }

    /// Asks about one batch of `shard`'s fragments and acts on the
    /// verdicts.
    async fn ask(&self, shard: &ShardRef, batch: &[(usize, Suspect)], report: &mut SweepReport) {
        let suspects: Vec<Suspect> = batch.iter().map(|(_, s)| s.clone()).collect();
        report.asked += suspects.len();
        let verdicts = match self.confirmer.confirm(shard, &suspects).await {
            Ok(verdicts) if verdicts.len() == suspects.len() => verdicts,
            Ok(_) | Err(_) => {
                report.unanswered += suspects.len();
                return;
            }
        };
        let now = self.clock.now();
        for ((store, suspect), verdict) in batch.iter().zip(verdicts) {
            match verdict {
                Verdict::Referenced => {
                    report.referenced += 1;
                    self.seen().insert(suspect.id, Seen::Referenced);
                }
                Verdict::InProgress => {
                    report.in_progress += 1;
                    self.seen().insert(suspect.id, Seen::Since(now));
                }
                Verdict::Orphan => {
                    // An error leaves a removable segment behind, and the
                    // fragment reclaimed all the same.
                    if let Err(error) = self.stores[*store].reclaim(suspect.id).await {
                        tracing::warn!(id = %suspect.id, %error, "a reclaimed segment stays");
                    }
                    report.reclaimed += 1;
                    self.seen().remove(&suspect.id);
                    if let Some(observer) = &self.observer {
                        observer(&Reclaimed {
                            node: self.node.clone(),
                            shard: shard.clone(),
                            suspect: suspect.clone(),
                        });
                    }
                }
            }
        }
    }

    fn seen(&self) -> MutexGuard<'_, HashMap<FragmentId, Seen>> {
        // Plain inserts and lookups, which a panic cannot leave half done.
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
