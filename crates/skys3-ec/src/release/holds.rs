//! A fragment node's released fragments and the reads that hold them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::{EcConfig, StorageConfig};
use skys3_io::{Clock, Disk, MonoTime};
use skys3_types::{FragmentId, NodeId};

use super::ReleaseMetrics;
use crate::{CompactionReport, FragmentStore};

/// How long a fragment node keeps what it was told to release (§8.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoldSettings {
    /// `fragment_release_delay_seconds`: how long a released fragment
    /// stays after its node learns of the release, for a read plan issued
    /// before the release whose gateway has not registered it yet.
    pub release_delay: Duration,
    /// `read_registration_ttl_seconds`: how long a read's hold lasts
    /// unless its gateway renews it.
    pub registration_ttl: Duration,
}

impl HoldSettings {
    /// The settings `[ec]` and `[storage]` give.
    #[must_use]
    pub fn from_config(ec: &EcConfig, storage: &StorageConfig) -> Self {
        Self {
            release_delay: ec.fragment_release_delay(),
            registration_ttl: storage.read_registration_ttl(),
        }
    }
}

/// What one sweep of a node's released fragments did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Released fragments reclaimed: dropped from their store's map.
    pub reclaimed: usize,
    /// Their bytes.
    pub reclaimed_bytes: u64,
    /// Released fragments kept, for the delay or because a read holds
    /// them.
    pub kept: usize,
    /// What compacting the node's stores did, summed over them.
    pub compaction: CompactionReport,
    /// Stores whose compaction failed; they are compacted again at the
    /// next sweep.
    pub failed: usize,
}

/// A released fragment the node still keeps.
#[derive(Debug, Clone, Copy)]
struct Releasing {
    since: MonoTime,
    bytes: u64,
}

/// One read's hold: the fragments it may read, until its gateway renews
/// it or `until` passes.
#[derive(Debug, Clone)]
struct Hold {
    until: MonoTime,
    fragments: BTreeSet<FragmentId>,
}

#[derive(Debug, Default)]
struct State {
    released: BTreeMap<FragmentId, Releasing>,
    /// Holds by the reading node, as mutual TLS names it, and its
    /// registration number.
    holds: BTreeMap<(NodeId, u64), Hold>,
}

/// The fragments a node was told to release, and the reads that hold
/// fragments on it (§8.7). Clones share them.
///
/// A released fragment stays readable for
/// [`HoldSettings::release_delay`] after the node learns of the release,
/// and then for as long as a read's hold names it, before
/// [`FragmentHolds::sweep`] reclaims it and compacts the node's stores.
/// A gateway registers a read's hold before its first fetch and renews it
/// while it streams; a hold its gateway stopped renewing lapses after
/// [`HoldSettings::registration_ttl`].
///
/// Nothing here is kept on disk. After a restart, a node holds no
/// registration, and a released fragment whose segment survived is found
/// again; orphan reclamation releases it again (§8.4).
#[derive(Clone)]
pub struct FragmentHolds {
    inner: Arc<Inner>,
}

struct Inner {
    clock: Arc<dyn Clock>,
    settings: HoldSettings,
    metrics: ReleaseMetrics,
    state: Mutex<State>,
    #[cfg(feature = "test-util")]
    bug: Option<HoldsBug>,
}

/// A bug to seed in a node's holds, for tests that show the simulation
/// catches it.
#[cfg(feature = "test-util")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldsBug {
    /// Reclaims released fragments once the delay passed, whatever reads
    /// hold them.
    IgnoreHolds,
    /// Reclaims released fragments at once, without the delay.
    NoDelay,
}

impl std::fmt::Debug for FragmentHolds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state();
        f.debug_struct("FragmentHolds")
            .field("released", &state.released.len())
            .field("holds", &state.holds.len())
            .finish_non_exhaustive()
    }
}

impl FragmentHolds {
    /// The holds of a node, timed by `clock`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, settings: HoldSettings) -> Self {
        Self {
            inner: Arc::new(Inner {
                clock,
                settings,
                metrics: ReleaseMetrics::default(),
                state: Mutex::default(),
                #[cfg(feature = "test-util")]
                bug: None,
            }),
        }
    }

    /// The same holds with `bug` seeded.
    ///
    /// # Panics
    ///
    /// Panics if a clone of the holds exists already.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_bug(mut self, bug: Option<HoldsBug>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("the holds are not shared yet")
            .bug = bug;
        self
    }

    #[cfg(feature = "test-util")]
    fn ignores_holds(&self) -> bool {
        self.inner.bug == Some(HoldsBug::IgnoreHolds)
    }

    #[cfg(not(feature = "test-util"))]
    fn ignores_holds(&self) -> bool {
        false
    }

    #[cfg(feature = "test-util")]
    fn ignores_delay(&self) -> bool {
        self.inner.bug == Some(HoldsBug::NoDelay)
    }

    #[cfg(not(feature = "test-util"))]
    fn ignores_delay(&self) -> bool {
        false
    }

    /// Counts in `metrics`.
    ///
    /// # Panics
    ///
    /// Panics if a clone of the holds exists already.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ReleaseMetrics) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("the holds are not shared yet")
            .metrics = metrics;
        self
    }

    /// The settings.
    #[must_use]
    pub fn settings(&self) -> HoldSettings {
        self.inner.settings
    }

    /// Notes that fragment `id`, of `bytes` bytes, is released, from now
    /// on unless it was already. Returns whether it was not.
    pub fn release(&self, id: FragmentId, bytes: u64) -> bool {
        let since = self.inner.clock.now();
        let mut state = self.state();
        if state.released.contains_key(&id) {
            return false;
        }
        state.released.insert(id, Releasing { since, bytes });
        self.inner.metrics.release(bytes);
        true
    }

    /// Whether fragment `id` is released and not reclaimed yet.
    #[must_use]
    pub fn is_released(&self, id: FragmentId) -> bool {
        self.state().released.contains_key(&id)
    }

    /// Registers or renews the hold of read `registration` of `reader` on
    /// `fragments`, for [`HoldSettings::registration_ttl`] from now. A
    /// renewal adds `fragments` to those the hold names, so a read may
    /// name its fragments over several messages.
    pub fn hold(
        &self,
        reader: &NodeId,
        registration: u64,
        fragments: impl IntoIterator<Item = FragmentId>,
    ) {
        let now = self.inner.clock.now();
        let until = now.saturating_add(self.inner.settings.registration_ttl);
        let mut state = self.state();
        let hold = state
            .holds
            .entry((reader.clone(), registration))
            .or_insert_with(|| Hold {
                until,
                fragments: BTreeSet::new(),
            });
        if hold.until <= now {
            // A hold that lapsed protected nothing since; it starts over.
            hold.fragments.clear();
        }
        hold.until = until;
        hold.fragments.extend(fragments);
    }

    /// Ends the hold of read `registration` of `reader`, if any.
    pub fn end(&self, reader: &NodeId, registration: u64) {
        self.state().holds.remove(&(reader.clone(), registration));
    }

    /// Whether a hold that has not lapsed names fragment `id`.
    #[must_use]
    pub fn is_held(&self, id: FragmentId) -> bool {
        let now = self.inner.clock.now();
        self.state()
            .holds
            .values()
            .any(|hold| hold.until > now && hold.fragments.contains(&id))
    }

    /// Reclaims the released fragments that have waited the release delay
    /// and that no read holds, from whichever of `stores` holds each, then
    /// compacts every store in service whose live fragments take less than
    /// `live_threshold` of a segment (§10.3).
    pub async fn sweep<D: Disk>(
        &self,
        stores: &[FragmentStore<D>],
        live_threshold: f64,
    ) -> SweepReport {
        let (due, kept) = self.due();
        let mut report = SweepReport {
            kept,
            ..SweepReport::default()
        };
        for (id, bytes) in due {
            let store = stores.iter().find(|store| store.len(id).is_some());
            let reclaimed = match store {
                Some(store) => match store.reclaim(id).await {
                    Ok(reclaimed) => reclaimed,
                    Err(error) => {
                        // The fragment is reclaimed all the same.
                        tracing::warn!(%id, %error, "a reclaimed segment stays");
                        true
                    }
                },
                None => false,
            };
            self.inner.metrics.reclaim(bytes, reclaimed);
            if reclaimed {
                report.reclaimed += 1;
                report.reclaimed_bytes += bytes;
            }
        }
        for store in stores.iter().filter(|store| store.is_in_service()) {
            match store.compact(live_threshold).await {
                Ok(compacted) => {
                    self.inner.metrics.compacted(&compacted);
                    let total = &mut report.compaction;
                    total.segments += compacted.segments;
                    total.copied += compacted.copied;
                    total.copied_bytes += compacted.copied_bytes;
                    total.damaged += compacted.damaged;
                    total.removed_bytes += compacted.removed_bytes;
                }
                Err(error) => {
                    tracing::warn!(%error, "fragment-segment compaction failed");
                    report.failed += 1;
                }
            }
        }
        report
    }

    /// Sweeps `stores` every `interval` until the returned future is
    /// dropped.
    pub async fn run<D: Disk>(
        self,
        stores: Vec<FragmentStore<D>>,
        interval: Duration,
        live_threshold: f64,
    ) {
        loop {
            tokio::time::sleep(interval).await;
            let report = self.sweep(&stores, live_threshold).await;
            if report.reclaimed > 0 || report.compaction.segments > 0 {
                tracing::debug!(?report, "a fragment release sweep ended");
            }
        }
    }

    /// The released fragments to reclaim now, with their bytes, and how
    /// many are kept. Forgets the holds that lapsed.
    fn due(&self) -> (Vec<(FragmentId, u64)>, usize) {
        let now = self.inner.clock.now();
        let delay = if self.ignores_delay() {
            Duration::ZERO
        } else {
            self.inner.settings.release_delay
        };
        let mut state = self.state();
        state.holds.retain(|_, hold| hold.until > now);
        let held: BTreeSet<FragmentId> = if self.ignores_holds() {
            BTreeSet::new()
        } else {
            state
                .holds
                .values()
                .flat_map(|hold| hold.fragments.iter().copied())
                .collect()
        };
        let mut due = Vec::new();
        state.released.retain(|id, releasing| {
            let waited = now.saturating_duration_since(releasing.since) >= delay;
            if waited && !held.contains(id) {
                due.push((*id, releasing.bytes));
                false
            } else {
                true
            }
        });
        (due, state.released.len())
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Plain inserts and removals, which a panic cannot leave half done.
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
