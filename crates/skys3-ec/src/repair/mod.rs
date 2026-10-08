//! Repair (design §8.6): a shard primary rebuilds the fragments its coded
//! objects lost, on other nodes, and relocates them with `EC_RELOCATE`.
//!
//! A [`Repairer`] runs on every replica of a shard and works while its
//! replica leads the shard and serves. Each pass:
//!
//! 1. **Finds lost fragments.** It lists the nodes that hold the shard's
//!    fragments and each node's fragments from the shard's index from node
//!    to fragments ([`IndexReader::fragments_on`]). Every fragment of a
//!    node that the cluster's topology no longer lists as eligible
//!    (departing, or forgotten) is lost. Other nodes are checked: each pass
//!    reads one byte of a few of a node's fragments in turn
//!    ([`RepairSettings::checks_per_node`]), which a node serves only if it
//!    holds the fragment intact under the header the layout expects. A
//!    fragment the node answers it does not hold, or holds damaged, is
//!    lost, and the node's other fragments are then all checked, as after
//!    a lost disk. A node that has answered none of the checks for
//!    `fragment_repair_after_seconds` has lost them all. Checks, reads,
//!    and rebuilt fragments name the version the layout's fragments were
//!    written for (`Coded::version`) and the object's ETag, not the
//!    entry's version, which a retag moves past it.
//! 2. **Orders the stripes.** Stripes with more fragments lost go first,
//!    then key and stripe order. A stripe that lost more than `m` cannot be
//!    rebuilt and is reported.
//! 3. **Rebuilds each stripe** as a fragment-writing attempt, fenced like
//!    an encoding (§8.4): its ID comes from the numbers the index reserves,
//!    it is tracked in the shard's [`Attempts`] from its start, and its
//!    record is appended only while the replica sequences in its epoch. It
//!    reads `k` surviving fragments whole, in index order (data fragments
//!    first, nodes that answered checks before silent ones), rebuilds the
//!    lost ones ([`EcCodec::reconstruct`]), places them around the
//!    fragments the stripe keeps ([`FragmentPlanner::replace`]), writes
//!    them, and once every one is durable commits an `EC_RELOCATE` that
//!    moves them. A write that fails places that fragment again without
//!    its node, a few times.
//!
//! Every byte a repair reads or writes first takes its share of the node's
//! [`RepairBandwidth`] (`repair_bytes_per_second_per_node`).
//!
//! Nothing of a repair is kept: a new primary, or this one after a
//! restart, finds the lost fragments again from its index and its own
//! checks, once it serves. The attempts of an earlier primary or life are
//! not in its tracker, so their fragments are orphans (§8.4), and an
//! `EC_RELOCATE` they appended commits through reconciliation or is cut.
//!
//! [`IndexReader::fragments_on`]: skys3_index::IndexReader::fragments_on
//! [`EcCodec::reconstruct`]: crate::EcCodec::reconstruct

mod bandwidth;
mod metrics;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_config::EcConfig;
use skys3_coord::{FragmentPlanner, NodeState, ReplaceRequest};
use skys3_index::{Entry, Index, IndexError};
use skys3_io::{BlockingPool, Disk};
use skys3_log::record::{EcRelocate, FragmentMove};
use skys3_shard::{Effect, Outcome, Shard, ShardError};
use skys3_types::{AttemptId, CodedStripe, EpochSeq, FragmentId, FragmentLocation, NodeId};
use tokio::task::JoinSet;
use tokio::time::Instant;

pub use bandwidth::RepairBandwidth;
pub use metrics::RepairMetrics;

use crate::encoder::{
    AttemptNumbers, Attempts, PlannerSource, Publication, Writing, fragment_header, leads, publish,
};
use crate::fragment::StripeInfo;
use crate::read::MAX_READ_LEN;
use crate::read::{FragmentIdentity, FragmentReadError, FragmentRequest, FragmentSource};
use crate::transfer::{FragmentWriter, TransferError};
use crate::{EcError, codec};

/// How a shard primary repairs (§8.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairSettings {
    /// How often a pass runs while the replica leads.
    pub interval: Duration,
    /// `fragment_repair_after_seconds`: how long a node may answer none of
    /// the checks before its fragments count as lost.
    pub lost_after: Duration,
    /// How many of a node's fragments a pass checks, in turn.
    pub checks_per_node: usize,
    /// How many times a fragment whose write failed is placed again before
    /// the stripe's repair is given up until the next pass.
    pub replans: usize,
    /// `repair_bytes_per_second_per_node`, for the cap
    /// [`Repairer::new`] makes when none is shared.
    pub bytes_per_second: u64,
}

impl RepairSettings {
    /// How often a pass runs by default.
    pub const INTERVAL: Duration = Duration::from_secs(30);

    /// How many of a node's fragments a pass checks by default.
    pub const CHECKS_PER_NODE: usize = 16;

    /// The settings `[ec]` gives.
    #[must_use]
    pub fn from_config(config: &EcConfig) -> Self {
        Self {
            interval: Self::INTERVAL,
            lost_after: config.fragment_repair_after(),
            checks_per_node: Self::CHECKS_PER_NODE,
            replans: 3,
            bytes_per_second: config.repair_bytes_per_second_per_node,
        }
    }
}

/// One step of a stripe's repair, as an observer sees it: what a
/// simulation crashes nodes at, and checks the order and pace of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairEvent {
    /// The object's key.
    pub key: String,
    /// The stripe's number.
    pub stripe: u32,
    /// The repair's attempt.
    pub attempt: AttemptId,
    /// The step.
    pub step: RepairStep,
}

/// The steps of a stripe's repair, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairStep {
    /// The repair started, in pass `pass`, for the fragments at `lost`.
    Started {
        /// The pass, counted from 1 in this repairer's life.
        pass: u64,
        /// The indices of the fragments the pass found lost.
        lost: Vec<u8>,
    },
    /// A surviving fragment was read whole.
    Read {
        /// Its index.
        index: u8,
        /// Its node.
        node: NodeId,
    },
    /// The rebuilt fragments were placed: each index and its new node.
    Placed {
        /// The indices and nodes.
        nodes: Vec<(u8, NodeId)>,
    },
    /// A rebuilt fragment is durable on its new node.
    Written {
        /// Its index.
        index: u8,
        /// Its node.
        node: NodeId,
    },
    /// The `EC_RELOCATE` record is sequenced, and not yet known to commit.
    Appended,
    /// The record committed: `relocated` if it was applied, false if the
    /// layout had moved on.
    Committed {
        /// The record's position.
        position: EpochSeq,
        /// Whether the layout names the rebuilt fragments now.
        relocated: bool,
    },
    /// The repair failed before it appended its record.
    Abandoned,
}

/// Sees every step of every repair.
pub type RepairObserver = Arc<dyn Fn(&RepairEvent) + Send + Sync>;

/// What a pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairReport {
    /// The nodes that hold the shard's fragments.
    pub nodes: usize,
    /// Fragments found lost.
    pub lost: usize,
    /// Stripes whose `EC_RELOCATE` committed and was applied.
    pub repaired: usize,
    /// Stripes whose `EC_RELOCATE` committed and was rejected: the version
    /// or its layout changed meanwhile.
    pub rejected: usize,
    /// Stripes whose repair failed; the next pass tries again.
    pub failed: usize,
    /// Stripes that lost more fragments than they can rebuild.
    pub unrecoverable: usize,
}

/// Why a stripe's repair failed. Unless the error is
/// [`RepairError::Publish`], nothing was appended and any fragment written
/// is an orphan.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepairError {
    /// The index failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// The index's blocking pool is shut down.
    #[error("the index's blocking pool is shut down")]
    PoolClosed,
    /// The codec failed.
    #[error(transparent)]
    Codec(#[from] EcError),
    /// Fewer than `k` fragments of the stripe could be read.
    #[error("only {read} of the {needed} fragments needed could be read")]
    Unreadable {
        /// The fragments read.
        read: usize,
        /// `k`.
        needed: usize,
    },
    /// The eligible nodes have no room for the rebuilt fragments under the
    /// placement rules: the repair waits for a node (§8.3).
    #[error("no eligible node has room for the rebuilt fragments")]
    NoRoom,
    /// A rebuilt fragment could not be written, after every re-plan.
    #[error("a rebuilt fragment could not be written: {0}")]
    Fragments(TransferError),
    /// The attempt was abandoned, by orphan reclamation's fence, before it
    /// appended its record.
    #[error("attempt {0} was abandoned")]
    Abandoned(AttemptId),
    /// The `EC_RELOCATE` record may be appended, but its commit was not
    /// confirmed, or nothing was appended.
    #[error("the EC_RELOCATE record was not committed: {0}")]
    Publish(ShardError),
}

/// A bug to seed in a repairer, for tests that show the cluster
/// simulation catches it.
#[cfg(feature = "test-util")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairBug {
    /// Registers an attempt with the tracker only once its fragments are
    /// written, so orphan reclamation's fence does not see it writing.
    Unfenced,
    /// Starts the stripes that lost the fewest fragments first.
    LeastLostFirst,
    /// Transfers without taking the bandwidth cap.
    Unthrottled,
}

/// A shard primary's repairer (§8.6). The module documentation describes a
/// pass.
pub struct Repairer<D: Disk, W: FragmentWriter> {
    shard: Shard<D>,
    source: Arc<dyn FragmentSource>,
    writer: Arc<W>,
    planner: PlannerSource,
    pool: BlockingPool,
    settings: RepairSettings,
    bandwidth: RepairBandwidth,
    attempts: Attempts,
    numbers: AttemptNumbers,
    metrics: RepairMetrics,
    observer: Option<RepairObserver>,
    state: Mutex<State>,
    passes: AtomicU64,
    #[cfg(feature = "test-util")]
    bug: Option<RepairBug>,
}

/// What a repairer remembers between passes.
#[derive(Default)]
struct State {
    /// Per node: since when it has answered no check, and where its next
    /// checks start.
    nodes: BTreeMap<NodeId, Health>,
    /// When each fragment known lost was first found lost.
    lost: BTreeMap<(NodeId, FragmentId), Instant>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Health {
    silent_since: Option<Instant>,
    cursor: usize,
}

/// A fragment the shard's index places on a node, with what its header
/// must say.
#[derive(Debug, Clone)]
struct Held {
    key: String,
    stripe: u32,
    index: u8,
    fragment: FragmentId,
}

/// The shard's fragments as one index transaction saw them.
struct Inventory {
    /// Each node and the fragments it holds that the current layouts name.
    nodes: Vec<(NodeId, Vec<Held>)>,
    /// The entries of their keys.
    entries: BTreeMap<String, Entry>,
}

/// One stripe to rebuild.
#[derive(Debug, Clone)]
struct Damaged {
    key: String,
    stripe: u32,
    lost: BTreeSet<u8>,
}

impl<D: Disk, W: FragmentWriter> Repairer<D, W> {
    /// The repairer of `shard`, which reads surviving fragments from
    /// `source`, writes rebuilt ones through `writer`, places them with the
    /// planner `planner` returns, and reads the index on `pool`.
    pub fn new(
        shard: Shard<D>,
        source: Arc<dyn FragmentSource>,
        writer: Arc<W>,
        planner: PlannerSource,
        pool: BlockingPool,
        settings: RepairSettings,
    ) -> Self {
        Self {
            shard,
            source,
            writer,
            planner,
            pool,
            bandwidth: RepairBandwidth::new(settings.bytes_per_second),
            settings,
            attempts: Attempts::default(),
            numbers: AttemptNumbers::default(),
            metrics: RepairMetrics::default(),
            observer: None,
            state: Mutex::default(),
            passes: AtomicU64::new(0),
            #[cfg(feature = "test-util")]
            bug: None,
        }
    }

    /// Tracks this repairer's attempts in `attempts`, the tracker every
    /// fragment-writing attempt of the shard on this node shares with its
    /// [`OrphanJudge`](crate::OrphanJudge).
    #[must_use]
    pub fn with_attempts(mut self, attempts: Attempts) -> Self {
        self.attempts = attempts;
        self
    }

    /// Takes repair bandwidth from `bandwidth`, the cap every repairer on
    /// this node shares.
    #[must_use]
    pub fn with_bandwidth(mut self, bandwidth: RepairBandwidth) -> Self {
        self.bandwidth = bandwidth;
        self
    }

    /// Counts in `metrics`, which every repairer on this node shares.
    #[must_use]
    pub fn with_metrics(mut self, metrics: RepairMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Reports every step of every repair to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: RepairObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// The same repairer with `bug` seeded.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_bug(mut self, bug: Option<RepairBug>) -> Self {
        self.bug = bug;
        self
    }

    /// Runs a pass every [`RepairSettings::interval`] while this replica
    /// leads the shard, until the returned future is dropped.
    pub async fn run(self) {
        loop {
            if leads(&self.shard) {
                let report = self.pass().await;
                if report.lost > 0 {
                    tracing::info!(shard = %self.shard.shard(), ?report, "a repair pass ended");
                }
            }
            tokio::time::sleep(self.settings.interval).await;
        }
    }

    /// Finds the shard's lost fragments and repairs the stripes that lost
    /// them, most damaged first, while this replica leads the shard.
    pub async fn pass(&self) -> RepairReport {
        let mut report = RepairReport::default();
        if !leads(&self.shard) {
            // What a replica that no longer leads knew lost is its
            // successor's to find and report.
            self.state().lost.clear();
            self.metrics.backlog(self.shard.shard(), 0, None);
            return report;
        }
        let pass = self.passes.fetch_add(1, Ordering::Relaxed) + 1;
        self.attempts.settle(self.shard.applied());
        let inventory = match self.inventory().await {
            Ok(inventory) => inventory,
            Err(error) => {
                tracing::warn!(shard = %self.shard.shard(), %error, "repair cannot read the index");
                return report;
            }
        };
        report.nodes = inventory.nodes.len();
        let planner = (self.planner)();
        let (lost, silent) = self.find_lost(&inventory, &planner).await;
        report.lost = lost.len();
        let mut damaged = self.damaged(&inventory, &lost);
        self.order(&mut damaged);
        for stripe in damaged {
            if !leads(&self.shard) {
                break;
            }
            let Some(entry) = inventory.entries.get(&stripe.key) else {
                continue;
            };
            let Some(coded) = coded_stripe(entry, stripe.stripe) else {
                continue;
            };
            if stripe.lost.len() > coded.geometry().parity_fragments() {
                report.unrecoverable += 1;
                tracing::error!(
                    shard = %self.shard.shard(),
                    key = stripe.key,
                    stripe = stripe.stripe,
                    lost = stripe.lost.len(),
                    "a stripe lost more fragments than it can rebuild"
                );
                continue;
            }
            match self
                .repair(pass, entry, &stripe, &silent, planner.clone())
                .await
            {
                Ok(true) => report.repaired += 1,
                Ok(false) => report.rejected += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::info!(
                        shard = %self.shard.shard(),
                        key = stripe.key,
                        stripe = stripe.stripe,
                        %error,
                        "a stripe was not repaired"
                    );
                }
            }
        }
        report
    }

    /// The shard's fragments and their keys' entries, from one index
    /// transaction.
    async fn inventory(&self) -> Result<Inventory, RepairError> {
        let index: Arc<Index> = Arc::clone(self.shard.index());
        let shard = self.shard.shard().clone();
        self.pool
            .run(move || {
                let reader = index.read()?;
                let mut nodes = Vec::new();
                let mut entries = BTreeMap::new();
                for node in reader.fragment_nodes(&shard)? {
                    let mut held = Vec::new();
                    for (row, fragment) in reader.fragments_on(&shard, &node)? {
                        if !entries.contains_key(&row.key)
                            && let Some(entry) = reader.entry(&shard, &row.key)?
                        {
                            entries.insert(row.key.clone(), entry);
                        }
                        let location = FragmentLocation {
                            node: node.clone(),
                            fragment,
                        };
                        let named = entries
                            .get(&row.key)
                            .and_then(|entry| coded_stripe(entry, row.stripe))
                            .and_then(|stripe| stripe.fragments().get(usize::from(row.index)))
                            == Some(&location);
                        if named {
                            held.push(Held {
                                key: row.key,
                                stripe: row.stripe,
                                index: row.index,
                                fragment,
                            });
                        }
                    }
                    nodes.push((node, held));
                }
                Ok::<_, IndexError>(Inventory { nodes, entries })
            })
            .await
            .map_err(|_| RepairError::PoolClosed)?
            .map_err(RepairError::from)
    }

    /// The fragments lost, by node and ID, and the nodes that answered no
    /// check this pass.
    async fn find_lost(
        &self,
        inventory: &Inventory,
        planner: &FragmentPlanner,
    ) -> (BTreeMap<(NodeId, FragmentId), Instant>, BTreeSet<NodeId>) {
        let now = Instant::now();
        let mut checks = JoinSet::new();
        let mut lost = BTreeSet::new();
        // A topology that lists no node says nothing of any: the node does
        // not know the cluster yet.
        let known = planner.topology().candidates().next().is_some();
        {
            let mut state = self.state();
            for (node, held) in &inventory.nodes {
                let departed = known
                    && planner
                        .topology()
                        .get(node)
                        .is_none_or(|candidate| candidate.state == NodeState::Departing);
                if departed {
                    lost.extend(held.iter().map(|held| (node.clone(), held.fragment)));
                    continue;
                }
                if held.is_empty() {
                    continue;
                }
                let health = state.nodes.entry(node.clone()).or_default();
                let start = health.cursor % held.len();
                let window = self.settings.checks_per_node.clamp(1, held.len());
                health.cursor = start + window;
                let requests: Vec<FragmentRequest> = held[start..]
                    .iter()
                    .chain(&held[..start])
                    .filter_map(|held| self.check(inventory, node, held))
                    .collect();
                let source = Arc::clone(&self.source);
                let node = node.clone();
                checks.spawn(async move {
                    let outcome = check_node(source.as_ref(), requests, window).await;
                    (node, outcome)
                });
            }
        }
        let mut outcomes = BTreeMap::new();
        while let Some(joined) = checks.join_next().await {
            if let Ok((node, outcome)) = joined {
                outcomes.insert(node, outcome);
            }
        }
        let mut silent = BTreeSet::new();
        let mut state = self.state();
        for (node, held) in &inventory.nodes {
            let Some(outcome) = outcomes.get(node) else {
                continue;
            };
            let health = state.nodes.entry(node.clone()).or_default();
            if outcome.answered {
                health.silent_since = None;
            } else {
                silent.insert(node.clone());
                let since = *health.silent_since.get_or_insert(now);
                if now.duration_since(since) >= self.settings.lost_after {
                    lost.extend(held.iter().map(|held| (node.clone(), held.fragment)));
                }
            }
            lost.extend(
                outcome
                    .missing
                    .iter()
                    .map(|fragment| (node.clone(), *fragment)),
            );
        }
        // Each fragment keeps the time it was first found lost, for the
        // repair time (§16.3); fragments no longer lost are forgotten.
        let known = std::mem::take(&mut state.lost);
        state.lost = lost
            .into_iter()
            .map(|fragment| {
                let since = known.get(&fragment).copied().unwrap_or(now);
                (fragment, since)
            })
            .collect();
        let oldest = state.lost.values().min().copied();
        self.metrics
            .backlog(self.shard.shard(), state.lost.len() as u64, oldest);
        (state.lost.clone(), silent)
    }

    /// The request that checks `held` on `node`: a read of its first byte.
    fn check(&self, inventory: &Inventory, node: &NodeId, held: &Held) -> Option<FragmentRequest> {
        let entry = inventory.entries.get(&held.key)?;
        Some(FragmentRequest {
            node: node.clone(),
            fragment: held.fragment,
            identity: self.identity(&held.key, entry, held.stripe, held.index)?,
            range: 0..1,
        })
    }

    /// What the header of fragment `index` of stripe `stripe` of `key`'s
    /// version `entry` says, if the version is coded with such a stripe:
    /// the version the layout's fragments were written for, which a retag
    /// leaves behind the entry's, and the object's ETag.
    fn identity(
        &self,
        key: &str,
        entry: &Entry,
        stripe: u32,
        index: u8,
    ) -> Option<FragmentIdentity> {
        let object = entry.object.as_ref()?;
        let coded = object.coded.as_ref()?;
        let at = coded.stripes.get(usize::try_from(stripe).ok()?)?;
        Some(FragmentIdentity {
            shard: self.shard.shard().clone(),
            key: key.to_owned(),
            version: coded.version,
            etag: object.local_etag.clone(),
            stripe: stripe_info(at, coded.stripes.len()),
            index,
        })
    }

    /// The stripes that lost the fragments `lost`.
    fn damaged(
        &self,
        inventory: &Inventory,
        lost: &BTreeMap<(NodeId, FragmentId), Instant>,
    ) -> Vec<Damaged> {
        let mut stripes: BTreeMap<(String, u32), BTreeSet<u8>> = BTreeMap::new();
        for (node, held) in &inventory.nodes {
            for held in held {
                if lost.contains_key(&(node.clone(), held.fragment)) {
                    stripes
                        .entry((held.key.clone(), held.stripe))
                        .or_default()
                        .insert(held.index);
                }
            }
        }
        stripes
            .into_iter()
            .map(|((key, stripe), lost)| Damaged { key, stripe, lost })
            .collect()
    }

    /// Orders `damaged` most lost fragments first, then by key and stripe.
    fn order(&self, damaged: &mut [Damaged]) {
        if self.has_bug(RepairBug::LeastLostFirst) {
            damaged.sort_by_key(|stripe| stripe.lost.len());
            return;
        }
        damaged.sort_by_key(|stripe| std::cmp::Reverse(stripe.lost.len()));
    }

    /// Repairs one stripe as a fragment-writing attempt: whether its
    /// `EC_RELOCATE` was applied.
    async fn repair(
        &self,
        pass: u64,
        entry: &Entry,
        damaged: &Damaged,
        silent: &BTreeSet<NodeId>,
        mut planner: FragmentPlanner,
    ) -> Result<bool, RepairError> {
        let attempt = self.numbers.next(&self.shard)?;
        let unfenced = self.has_bug(RepairBug::Unfenced);
        if !unfenced {
            self.attempts.begin(attempt);
        }
        // Forgets the attempt if this future is dropped before it appends.
        let _writing = Writing {
            attempts: &self.attempts,
            attempt,
        };
        let key = damaged.key.as_str();
        let emit = |step| self.emit(key, damaged.stripe, attempt, step);
        emit(RepairStep::Started {
            pass,
            lost: damaged.lost.iter().copied().collect(),
        });
        let rebuilt = self
            .rebuild(entry, damaged, silent, attempt, &mut planner)
            .await;
        let moves = match rebuilt {
            Ok(moves) => moves,
            Err(error) => {
                self.attempts.finish(attempt);
                emit(RepairStep::Abandoned);
                return Err(error);
            }
        };
        if unfenced {
            self.attempts.begin(attempt);
        }
        // The entry's version, not the layout's: a retag since this attempt
        // read the entry rejects the record, and the next pass rebuilds the
        // fragments with the new tags.
        let body = skys3_log::RecordBody::EcRelocate(EcRelocate {
            key: key.to_owned(),
            version: entry.version,
            etag: entry
                .object
                .as_ref()
                .map(|object| object.local_etag.clone())
                .expect("a coded entry has an object"),
            attempt,
            moves: moves.clone(),
        });
        let appended = || emit(RepairStep::Appended);
        let committed = match publish(&self.shard, &self.attempts, attempt, body, appended).await {
            Publication::Committed(committed) => committed,
            Publication::Abandoned => {
                emit(RepairStep::Abandoned);
                return Err(RepairError::Abandoned(attempt));
            }
            Publication::NotAppended(error) => {
                emit(RepairStep::Abandoned);
                return Err(RepairError::Publish(error));
            }
            Publication::Unconfirmed(error) => return Err(RepairError::Publish(error)),
        };
        let relocated = committed.outcome == Outcome::Applied(Effect::Relocated);
        emit(RepairStep::Committed {
            position: committed.position,
            relocated,
        });
        if relocated {
            let now = Instant::now();
            let mut state = self.state();
            for moved in &moves {
                let since = state
                    .lost
                    .remove(&(moved.from.node.clone(), moved.from.fragment))
                    .unwrap_or(now);
                self.metrics.repaired_after(now.duration_since(since));
            }
            let oldest = state.lost.values().min().copied();
            self.metrics
                .backlog(self.shard.shard(), state.lost.len() as u64, oldest);
        }
        Ok(relocated)
    }

    /// Reads `k` surviving fragments of the stripe, rebuilds the lost ones,
    /// and writes them to new nodes: the moves that relocate them.
    async fn rebuild(
        &self,
        entry: &Entry,
        damaged: &Damaged,
        silent: &BTreeSet<NodeId>,
        attempt: AttemptId,
        planner: &mut FragmentPlanner,
    ) -> Result<Vec<FragmentMove>, RepairError> {
        let key = damaged.key.as_str();
        let stripe = coded_stripe(entry, damaged.stripe)
            .ok_or(RepairError::Unreadable { read: 0, needed: 1 })?;
        // The rebuilt fragments name the version the others were written
        // for, which a retag leaves behind the entry's.
        let version = coded_version(entry).ok_or(RepairError::Unreadable { read: 0, needed: 1 })?;
        let geometry = stripe.geometry();
        let codec = codec(stripe.codec())?;
        let len = codec.fragment_len(geometry, stripe.data_len())?;
        let mut lost = damaged.lost.clone();

        // Read k survivors whole: data fragments first, and nodes that
        // answered this pass's checks before silent ones.
        let mut order: Vec<u8> = (0..geometry.total_fragments())
            .map(|index| index as u8)
            .filter(|index| !lost.contains(index))
            .collect();
        order.sort_by_key(|index| silent.contains(&stripe.fragments()[usize::from(*index)].node));
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; geometry.total_fragments()];
        let mut read = 0;
        for index in order {
            if read == geometry.data_fragments() {
                break;
            }
            self.check_writing(attempt)?;
            let location = &stripe.fragments()[usize::from(index)];
            let identity = self
                .identity(key, entry, damaged.stripe, index)
                .ok_or(RepairError::Unreadable { read, needed: 1 })?;
            match self.read_whole(location, identity, len).await {
                Ok(bytes) => {
                    slots[usize::from(index)] = Some(bytes);
                    read += 1;
                    let node = location.node.clone();
                    self.emit(
                        key,
                        damaged.stripe,
                        attempt,
                        RepairStep::Read { index, node },
                    );
                }
                Err(FragmentReadError::NotHeld { .. } | FragmentReadError::Damaged { .. }) => {
                    // Lost too: rebuilt in the same attempt.
                    lost.insert(index);
                }
                Err(_) => {}
            }
        }
        if read < geometry.data_fragments() {
            return Err(RepairError::Unreadable {
                read,
                needed: geometry.data_fragments(),
            });
        }
        let present: Vec<Option<&[u8]>> = slots.iter().map(Option::as_deref).collect();
        let mut fragments = codec.reconstruct(geometry, stripe.data_len(), &present)?;
        drop(slots);

        // Place and write the rebuilt fragments, again without a node
        // whose write failed.
        let info = stripe_info(stripe, coded_len(entry));
        let mut keep: Vec<NodeId> = (0..geometry.total_fragments())
            .filter(|index| !lost.contains(&(*index as u8)))
            .map(|index| stripe.fragments()[index].node.clone())
            .collect();
        let mut avoid: Vec<NodeId> = silent.iter().cloned().collect();
        let mut remaining: Vec<u8> = lost.iter().copied().collect();
        let mut written: BTreeMap<u8, FragmentLocation> = BTreeMap::new();
        let mut last = None;
        for _ in 0..=self.settings.replans {
            self.check_writing(attempt)?;
            let request = ReplaceRequest {
                bucket: &self.shard.shard().bucket,
                shard: self.shard.shard().shard,
                key,
                stripe: damaged.stripe,
                geometry,
                data_len: stripe.data_len(),
                keep: &keep,
                count: remaining.len(),
                avoid: &avoid,
            };
            let nodes = planner.replace(&request).ok_or(RepairError::NoRoom)?;
            let placed: Vec<(u8, NodeId)> = remaining.iter().copied().zip(nodes).collect();
            self.emit(
                key,
                damaged.stripe,
                attempt,
                RepairStep::Placed {
                    nodes: placed.clone(),
                },
            );
            let mut writes = JoinSet::new();
            for (index, node) in placed {
                let header = fragment_header(
                    self.shard.shard(),
                    key,
                    entry,
                    version,
                    attempt,
                    info,
                    index,
                );
                let data = Bytes::from(std::mem::take(&mut fragments[usize::from(index)]));
                let writer = Arc::clone(&self.writer);
                let bandwidth = self.throttle().cloned();
                writes.spawn(async move {
                    if let Some(bandwidth) = bandwidth {
                        bandwidth.take(data.len() as u64).await;
                    }
                    let result = writer.write(&node, &header, data.clone()).await;
                    (index, node, data, result)
                });
            }
            remaining.clear();
            while let Some(joined) = writes.join_next().await {
                let (index, node, data, result) = joined.map_err(|error| {
                    RepairError::Fragments(TransferError::Invalid(error.to_string()))
                })?;
                self.metrics.transferred(data.len() as u64);
                match result {
                    Ok(fragment) => {
                        let step = RepairStep::Written {
                            index,
                            node: node.clone(),
                        };
                        self.emit(key, damaged.stripe, attempt, step);
                        keep.push(node.clone());
                        written.insert(index, FragmentLocation { node, fragment });
                    }
                    Err(error) => {
                        tracing::debug!(%node, %error, "a rebuilt fragment's write failed");
                        avoid.push(node);
                        fragments[usize::from(index)] = data.to_vec();
                        remaining.push(index);
                        last = Some(error);
                    }
                }
            }
            if remaining.is_empty() {
                return Ok(written
                    .into_iter()
                    .map(|(index, to)| FragmentMove {
                        stripe: damaged.stripe,
                        index,
                        from: stripe.fragments()[usize::from(index)].clone(),
                        to,
                    })
                    .collect());
            }
            remaining.sort_unstable();
        }
        Err(RepairError::Fragments(
            last.expect("a failed placement had a failed write"),
        ))
    }

    /// Reads fragment `location` whole, `len` bytes, in reads of at most
    /// [`MAX_READ_LEN`], each checked against its CRC32C.
    async fn read_whole(
        &self,
        location: &FragmentLocation,
        identity: FragmentIdentity,
        len: u64,
    ) -> Result<Vec<u8>, FragmentReadError> {
        let mut data = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
        let mut start = 0;
        while start < len {
            let end = (start + MAX_READ_LEN).min(len);
            if let Some(bandwidth) = self.throttle() {
                bandwidth.take(end - start).await;
            }
            let request = FragmentRequest {
                node: location.node.clone(),
                fragment: location.fragment,
                identity: identity.clone(),
                range: start..end,
            };
            let bytes = self.source.read(request).await?;
            self.metrics.transferred(end - start);
            if bytes.data.len() as u64 != end - start || crc32c::crc32c(&bytes.data) != bytes.crc32c
            {
                return Err(FragmentReadError::Unreachable {
                    node: location.node.clone(),
                    reason: "the bytes failed their checks on the way".to_owned(),
                });
            }
            data.extend_from_slice(&bytes.data);
            start = end;
        }
        Ok(data)
    }

    /// Fails if `attempt` was abandoned by orphan reclamation's fence.
    fn check_writing(&self, attempt: AttemptId) -> Result<(), RepairError> {
        let unfenced = self.has_bug(RepairBug::Unfenced);
        if unfenced || self.attempts.state(attempt) == Some(crate::AttemptState::Writing) {
            Ok(())
        } else {
            Err(RepairError::Abandoned(attempt))
        }
    }

    /// The bandwidth cap transfers take, unless a seeded bug skips it.
    fn throttle(&self) -> Option<&RepairBandwidth> {
        (!self.has_bug(RepairBug::Unthrottled)).then_some(&self.bandwidth)
    }

    fn emit(&self, key: &str, stripe: u32, attempt: AttemptId, step: RepairStep) {
        if let Some(observer) = &self.observer {
            observer(&RepairEvent {
                key: key.to_owned(),
                stripe,
                attempt,
                step,
            });
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(feature = "test-util")]
    fn has_bug(&self, bug: RepairBug) -> bool {
        self.bug == Some(bug)
    }

    #[cfg(not(feature = "test-util"))]
    fn has_bug(&self, _bug: RepairBug) -> bool {
        false
    }
}

/// The bugs a repairer can be seeded with: none without `test-util`.
#[cfg(not(feature = "test-util"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairBug {
    Unfenced,
    LeastLostFirst,
    Unthrottled,
}

/// What a node's checks found.
struct NodeCheck {
    /// Whether the node answered any check.
    answered: bool,
    /// The fragments it answered it does not hold, or holds damaged.
    missing: Vec<FragmentId>,
}

/// Checks the first `window` of `requests` on their node, and every other
/// one too if one of those is missing, until the node stops answering.
async fn check_node(
    source: &dyn FragmentSource,
    requests: Vec<FragmentRequest>,
    window: usize,
) -> NodeCheck {
    let mut outcome = NodeCheck {
        answered: false,
        missing: Vec::new(),
    };
    for (at, request) in requests.into_iter().enumerate() {
        if at >= window && outcome.missing.is_empty() {
            break;
        }
        let fragment = request.fragment;
        match source.read(request).await {
            Ok(_) => outcome.answered = true,
            Err(FragmentReadError::NotHeld { .. } | FragmentReadError::Damaged { .. }) => {
                outcome.answered = true;
                outcome.missing.push(fragment);
            }
            // The node does not answer: the rest would wait as long.
            Err(_) => break,
        }
    }
    outcome
}

/// The version `entry`'s coded layout's fragments were written for, if it
/// has a coded layout.
fn coded_version(entry: &Entry) -> Option<EpochSeq> {
    let coded = entry.object.as_ref()?.coded.as_ref()?;
    Some(coded.version)
}

/// Stripe `stripe` of `entry`'s coded layout, if it has one.
fn coded_stripe(entry: &Entry, stripe: u32) -> Option<&CodedStripe> {
    let coded = entry.object.as_ref()?.coded.as_ref()?;
    coded.stripes.get(usize::try_from(stripe).ok()?)
}

/// How many stripes `entry`'s coded layout has.
fn coded_len(entry: &Entry) -> usize {
    entry
        .object
        .as_ref()
        .and_then(|object| object.coded.as_ref())
        .map_or(0, |coded| coded.stripes.len())
}

/// What every fragment header of `stripe`, of an object of `count`
/// stripes, says of the stripe.
fn stripe_info(stripe: &CodedStripe, count: usize) -> StripeInfo {
    StripeInfo {
        number: stripe.number(),
        // A layout has at most `MAX_STRIPES` stripes, which fits a u32.
        count: count as u32,
        offset: stripe.offset(),
        data_len: stripe.data_len(),
        geometry: stripe.geometry(),
        codec: stripe.codec(),
    }
}
