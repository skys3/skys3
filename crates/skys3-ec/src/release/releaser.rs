//! A shard primary's side of fragment release: it tells fragment nodes
//! what the shard released, and records with `EC_RELEASE` that they know.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_index::{Index, IndexError, Released};
use skys3_io::{BlockingPool, Disk};
use skys3_log::RecordBody;
use skys3_log::record::{EcRelease, MAX_RELEASED};
use skys3_shard::{Effect, Outcome, Shard};
use skys3_types::{FragmentLocation, NodeId};
use tokio::task::JoinSet;

use super::{ReleaseSink, ReleasedFragment};
use crate::PlannerSource;
use crate::encoder::leads;

/// How a shard primary releases fragments (§8.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseSettings {
    /// How often a pass runs while the replica leads.
    pub interval: Duration,
    /// The most released fragments a pass takes up.
    pub batch: usize,
}

impl ReleaseSettings {
    /// How often a pass runs by default: well within
    /// `fragment_release_delay_seconds`, which starts only once a node
    /// learns of a release.
    pub const INTERVAL: Duration = Duration::from_secs(10);

    /// The most fragments a pass takes up by default.
    pub const BATCH: usize = 4096;
}

impl Default for ReleaseSettings {
    fn default() -> Self {
        Self {
            interval: Self::INTERVAL,
            batch: Self::BATCH,
        }
    }
}

/// What one release pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReleaseReport {
    /// Released fragments the pass took up.
    pub pending: usize,
    /// Fragments whose nodes noted the release.
    pub told: usize,
    /// Fragments on nodes the cluster's topology no longer lists, which
    /// nobody can tell.
    pub forgotten: usize,
    /// Fragments whose nodes did not answer; the next pass tells them
    /// again.
    pub unanswered: usize,
    /// `EC_RELEASE` records committed.
    pub records: usize,
    /// Fragments those records removed from the shard's index of released
    /// fragments.
    pub removed: usize,
}

/// Releases the fragments a shard's index of released fragments holds
/// (see [the module](crate::release)): on every replica, while it leads
/// the shard and serves, so a new primary releases only after
/// reconciliation (§6.6) committed what its predecessors applied.
pub struct Releaser<D: Disk, S: ReleaseSink> {
    shard: Shard<D>,
    sink: Arc<S>,
    planner: PlannerSource,
    pool: BlockingPool,
    settings: ReleaseSettings,
    /// Where the next pass starts reading the index of released
    /// fragments: after the last row the previous pass read, or at the
    /// start once a pass reached the end. Rows a node does not answer for
    /// stay pending without holding back the rows after them.
    cursor: Mutex<Option<FragmentLocation>>,
}

impl<D: Disk, S: ReleaseSink> Releaser<D, S> {
    /// The releaser of `shard`, which tells nodes through `sink`, learns
    /// which nodes the cluster still lists from the topology of the
    /// planner `planner` returns, and reads the index on `pool`.
    #[must_use]
    pub fn new(
        shard: Shard<D>,
        sink: Arc<S>,
        planner: PlannerSource,
        pool: BlockingPool,
        settings: ReleaseSettings,
    ) -> Self {
        Self {
            shard,
            sink,
            planner,
            pool,
            settings,
            cursor: Mutex::new(None),
        }
    }

    /// Runs a pass every [`ReleaseSettings::interval`] while this replica
    /// leads the shard, until the returned future is dropped.
    pub async fn run(self) {
        loop {
            if leads(&self.shard) {
                let report = self.pass().await;
                if report.unanswered > 0 {
                    tracing::debug!(shard = %self.shard.shard(), ?report, "a release pass ended");
                }
            }
            tokio::time::sleep(self.settings.interval).await;
        }
    }

    /// Tells the nodes of up to [`ReleaseSettings::batch`] released
    /// fragments, and commits an `EC_RELEASE` for each key whose fragments
    /// their nodes noted, while this replica leads the shard.
    pub async fn pass(&self) -> ReleaseReport {
        let mut report = ReleaseReport::default();
        if !leads(&self.shard) {
            return report;
        }
        let rows = match self.pending().await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(shard = %self.shard.shard(), %error, "release cannot read the index");
                return report;
            }
        };
        report.pending = rows.len();
        let mut by_node: BTreeMap<NodeId, Vec<ReleasedFragment>> = BTreeMap::new();
        let mut keys = BTreeMap::new();
        for (location, released) in rows {
            let Released {
                key,
                version,
                stripe,
                index,
            } = released;
            keys.insert(location.clone(), key.clone());
            by_node
                .entry(location.node)
                .or_default()
                .push(ReleasedFragment {
                    fragment: location.fragment,
                    key,
                    version,
                    stripe,
                    index,
                });
        }
        let planner = (self.planner)();
        let topology = planner.topology();
        // A topology that lists no node says nothing of any: the node does
        // not know the cluster yet.
        let known = topology.candidates().next().is_some();
        let mut done: Vec<FragmentLocation> = Vec::new();
        let mut tells = JoinSet::new();
        for (node, fragments) in by_node {
            if known && topology.get(&node).is_none() {
                report.forgotten += fragments.len();
                done.extend(located(&node, &fragments));
                continue;
            }
            let (sink, shard) = (Arc::clone(&self.sink), self.shard.shard().clone());
            tells.spawn(async move {
                let told = sink.release(&node, &shard, &fragments).await;
                (node, fragments, told)
            });
        }
        while let Some(joined) = tells.join_next().await {
            // A tell that panicked is tried again at the next pass.
            let Ok((node, fragments, told)) = joined else {
                continue;
            };
            match told {
                Ok(()) => {
                    report.told += fragments.len();
                    done.extend(located(&node, &fragments));
                }
                Err(reason) => {
                    tracing::debug!(%node, reason, "a node was not told of released fragments");
                    report.unanswered += fragments.len();
                }
            }
        }
        let mut by_key: BTreeMap<String, BTreeSet<FragmentLocation>> = BTreeMap::new();
        for location in done {
            if let Some(key) = keys.get(&location) {
                by_key.entry(key.clone()).or_default().insert(location);
            }
        }
        self.record(by_key, &mut report).await;
        report
    }

    /// Commits one `EC_RELEASE` for each key's fragments, while the
    /// replica leads; the rest wait for the next pass.
    async fn record(
        &self,
        by_key: BTreeMap<String, BTreeSet<FragmentLocation>>,
        report: &mut ReleaseReport,
    ) {
        for (key, locations) in by_key {
            let locations: Vec<FragmentLocation> = locations.into_iter().collect();
            for fragments in locations.chunks(MAX_RELEASED) {
                if !leads(&self.shard) {
                    return;
                }
                let body = RecordBody::EcRelease(EcRelease {
                    key: key.clone(),
                    fragments: fragments.to_vec(),
                });
                match self.shard.commit(body).await {
                    Ok(committed) => {
                        report.records += 1;
                        if let Outcome::Applied(Effect::Released { fragments }) = committed.outcome
                        {
                            report.removed += fragments;
                        }
                    }
                    Err(error) => {
                        tracing::info!(shard = %self.shard.shard(), key, %error, "an EC_RELEASE did not commit");
                        return;
                    }
                }
            }
        }
    }

    /// Up to a batch of the shard's released fragments, from one index
    /// transaction, after where the previous pass stopped; the pass after
    /// one that reached the end starts again at the start.
    async fn pending(&self) -> Result<Vec<(FragmentLocation, Released)>, IndexError> {
        let index: Arc<Index> = Arc::clone(self.shard.index());
        let shard = self.shard.shard().clone();
        let batch = self.settings.batch;
        let after = self.cursor().clone();
        let rows = self
            .pool
            .run(move || index.read()?.released(&shard, after.as_ref(), batch))
            .await??;
        *self.cursor() = if rows.len() < batch {
            None
        } else {
            rows.last().map(|(location, _)| location.clone())
        };
        Ok(rows)
    }

    fn cursor(&self) -> MutexGuard<'_, Option<FragmentLocation>> {
        // A plain assignment, which a panic cannot leave half done.
        self.cursor.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The locations of `fragments` on `node`.
fn located<'a>(
    node: &'a NodeId,
    fragments: &'a [ReleasedFragment],
) -> impl Iterator<Item = FragmentLocation> + 'a {
    fragments.iter().map(move |fragment| FragmentLocation {
        node: node.clone(),
        fragment: fragment.fragment,
    })
}
