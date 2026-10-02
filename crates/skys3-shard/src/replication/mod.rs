//! The replication data path between a shard's primary and its members
//! (§5.1, §6.6), over the intra-cluster transport.
//!
//! [`Replication`] runs on every node. It opens the node's shard replicas
//! in the roles their configurations give it, and:
//!
//! - **As a primary**, it keeps one link per member and shard. A link
//!   connects, opens a session with a `Sync` (which also rolls forward the
//!   records the member holds beyond the primary's log), and then sends the
//!   member every record the primary holds after the member's last, as the
//!   primary appends it, with the commit watermark. While there is nothing
//!   to send it sends beacons that carry the watermark. Appends and
//!   beacons carry lease stamps, and a beacon goes out at least every
//!   [`ReplicationConfig::lease_renew_interval`] even while records flow.
//!   The member's acknowledgements feed the commit rule and the primary's
//!   leases ([`Leader`]). A link that fails,
//!   or hears nothing for [`ReplicationConfig::link_timeout`], reconnects,
//!   and resumes from what the member then holds.
//! - **As a member**, it accepts links from the shard's primary only. A
//!   `Sync` opens a new session, which refuses appends of every earlier one
//!   (such as appends of a primary's earlier life still in the network),
//!   waits until the records already queued are durable, and reports the
//!   last one. Appends must come from the shard's epoch (rule R2 refuses
//!   older ones) and follow the member's last record. The member
//!   acknowledges the run of records it holds durably as it grows, and
//!   applies records up to the commit watermark. Each acknowledgement
//!   echoes the latest lease stamp, which grants the primary a lease, and
//!   restarts the member's [`Grace`] for the shard
//!   ([`Replication::grace`], §5.4).
//! - **Planned handoff** (§5.4): [`Replication::hand_off`] makes a primary
//!   step down to one of its members, which then takes over without
//!   waiting for its grace ([`Replication::with_takeover`]).
//! - **Removing members** (§6.4), once [`Replication::with_removal`] gives
//!   the node the shard registers: a primary removes a member that stays
//!   unresponsive for [`ReplicationConfig::member_suspect_after`], by a
//!   compare-and-swap of the shard's register to the next epoch without
//!   it. The records the member held back commit under the new epoch once
//!   the primary's `CONFIG` record is durable, and a shard left with fewer
//!   members than its `min_write_replicas` stays readable and refuses
//!   writes. [`Replication::exposure`] reports what then has fewer copies
//!   than `replicas`.
//! - **Learners** (§6.4, §6.7): a primary links to the learners of its
//!   configuration as to its members, and takes a learner into the
//!   acknowledgement set once it holds what the primary had sequenced a
//!   moment ago; it drops a learner that misses `member_suspect_after`
//!   from that set without changing the configuration. Once a learner's
//!   [`Backfill`] is complete and it is durable up to the commit
//!   watermark, the primary promotes it by a compare-and-swap of the
//!   shard's register, without pausing commits.
//!
//! - **Resuming** (§6.2): a node that starts opens each replica in its
//!   shard's register, or, while the register cannot be read, in the
//!   configuration of the latest `CONFIG` record it applied
//!   ([`Replication::resume`]). Epochs fence a replica whose configuration
//!   is stale, until it reads the register.
//!
//! A configuration change switches epochs at the same `seq` on every
//! replica (§5.1): a primary's session carries its newest configuration
//! and the epoch it sequences in, and a member in an earlier epoch takes
//! that epoch's tail before it appends its own `CONFIG` record where the
//! primary's is. A link whose primary changes configuration opens a new
//! session; one to a member the configuration leaves out ends.

mod backfill;
mod exposure;
mod learners;
mod member;
mod primary;
mod removal;
mod resume;
mod takeover;
#[cfg(test)]
mod tests;
pub mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use skys3_control::ProposalIds;
use skys3_io::{Clock, Disk, MonoTime};
use skys3_log::ShardRef;
use skys3_net::{Frame, Listener, Network, Receiver, Sender, Transport, TransportError};
use skys3_types::{Epoch, NodeAddress, NodeId, Seq, ShardConfig};

pub use self::exposure::{Exposure, ReplicationMetrics};
pub use self::learners::Backfill;
pub use self::removal::{BoxFuture, ControlRegisters, Replaced, ShardRegisters};
use crate::ack::AckTimeout;
use crate::error::ShardError;
use crate::leader::Leader;
use crate::lease::Grace;
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

/// Timing of the replication links. Configuration keys for them come with
/// the PR that runs replication in the node binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// How often a primary sends a beacon with the commit watermark while
    /// it has no record to send a member.
    pub beacon_interval: Duration,
    /// How long either end of a link waits for the next frame before it
    /// drops the link. Members answer every beacon, and a primary sends one
    /// at least every quarter of this timeout whatever the other intervals
    /// say ([`ReplicationConfig::beacon_every`]), so a live link hears
    /// something well within it, even while a member's log is slow to
    /// sync and acknowledges no record.
    pub link_timeout: Duration,
    /// How long a primary waits before it reconnects a link that dropped.
    pub reconnect_delay: Duration,
    /// `lease_renew_interval`: the longest a primary goes without sending a
    /// member a beacon, even while it sends records (§5.4). At least
    /// `beacon_interval`.
    pub lease_renew_interval: Duration,
    /// `primary_lease`: how long a member's acknowledgement lets the
    /// primary serve reads, from the stamp it echoes, on the primary's
    /// clock (§5.4).
    pub primary_lease: Duration,
    /// `primary_grace`: how long after it last granted a lease a member
    /// waits, on its own clock, before it may propose a new primary
    /// (§5.4). Configuration loading checks it against `primary_lease`
    /// and the drift bound `ρ`.
    pub primary_grace: Duration,
    /// `replica_ack_timeout` and its mode: how long a replicated shard's
    /// requests wait for its members (§5.2, [`Shard::set_ack_timeout`]).
    pub ack_timeout: AckTimeout,
    /// `member_suspect_after`: how long a member may stay unresponsive
    /// before its primary removes it (§6.4), once the node removes members
    /// at all ([`Replication::with_removal`]).
    pub member_suspect_after: Duration,
    /// The longest a member waits, once `primary_grace` has passed, before
    /// it proposes itself as primary (§6.5): the delay shrinks to an
    /// eighth of this as the member holds more records past what it
    /// applied, so the member with the longest log usually proposes first,
    /// and up to a quarter more spreads members with equal logs apart.
    pub takeover_delay: Duration,
}

impl Default for ReplicationConfig {
    /// The design's defaults, with beacons every 100 ms.
    fn default() -> Self {
        Self {
            beacon_interval: Duration::from_millis(100),
            link_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_millis(200),
            lease_renew_interval: Duration::from_secs(1),
            primary_lease: Duration::from_secs(4),
            primary_grace: Duration::from_secs(6),
            ack_timeout: AckTimeout::DEFAULT,
            member_suspect_after: Duration::from_secs(3),
            takeover_delay: Duration::from_millis(400),
        }
    }
}

impl ReplicationConfig {
    /// The longest a primary goes without sending a member a beacon:
    /// `beacon_interval` while it has nothing to send, else
    /// `lease_renew_interval`, and never more than a quarter of
    /// `link_timeout`. A member answers every beacon at once, so its answer
    /// reaches the primary well before either end gives up on a link that
    /// is merely busy (§5.4). Beacons more frequent than
    /// `lease_renew_interval` only renew leases sooner.
    #[must_use]
    pub fn beacon_every(&self, idle: bool) -> Duration {
        let interval = if idle {
            self.beacon_interval
        } else {
            self.lease_renew_interval
        };
        interval.min(self.link_timeout / 4)
    }
}

/// The replication service of one node: see the [module](self) docs.
pub struct Replication<N: Network, D: Disk> {
    inner: Arc<Inner<N, D>>,
}

struct Inner<N: Network, D: Disk> {
    node: NodeId,
    set: ShardSet<D>,
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    clock: Arc<dyn Clock>,
    config: ReplicationConfig,
    /// The grace of each shard this node is a member of, in this life.
    graces: Mutex<BTreeMap<ShardRef, Arc<Grace>>>,
    /// What primaries remove members through, and members take over
    /// through, if they do.
    removal: OnceLock<Arc<removal::Removal>>,
    /// What backfills the learners of this node's primaries, if anything
    /// beyond the live stream does.
    backfill: OnceLock<Arc<dyn Backfill>>,
    /// Whether members take over from a silent primary.
    takeover: AtomicBool,
    /// The shards whose members watch their primary in this life.
    candidates: Mutex<BTreeSet<ShardRef>>,
    /// Since when each under-replicated shard this node leads has been so,
    /// as far as this life saw.
    under: Arc<Mutex<BTreeMap<ShardRef, MonoTime>>>,
}

impl<N: Network, D: Disk> Inner<N, D> {
    /// The grace of `shard`, started now if the shard has none yet.
    fn grace_of(&self, shard: &ShardRef) -> Arc<Grace> {
        let mut graces = self.graces.lock().unwrap_or_else(PoisonError::into_inner);
        let grace = graces.entry(shard.clone()).or_insert_with(|| {
            Arc::new(Grace::new(
                Arc::clone(&self.clock),
                self.config.primary_grace,
            ))
        });
        Arc::clone(grace)
    }
}

impl<N: Network, D: Disk> Clone for Replication<N, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<N: Network, D: Disk> fmt::Debug for Replication<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replication")
            .field("node", &self.inner.node)
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> Replication<N, D> {
    /// The replication service of node `node`, whose replicas are in `set`,
    /// reaching the other nodes at `peers` through `transport`, and timing
    /// leases and grace on `clock`.
    #[must_use]
    pub fn new(
        node: NodeId,
        set: ShardSet<D>,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        clock: Arc<dyn Clock>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node,
                set,
                transport,
                peers,
                clock,
                config,
                graces: Mutex::default(),
                removal: OnceLock::new(),
                backfill: OnceLock::new(),
                takeover: AtomicBool::new(false),
                candidates: Mutex::default(),
                under: Arc::default(),
            }),
        }
    }

    /// Lets this node's primaries remove members that stay unresponsive
    /// for [`ReplicationConfig::member_suspect_after`] (§6.4), by replacing
    /// their shard's register in `registers`, under proposal IDs from
    /// `ids`. Primaries opened from then on remove members; without it,
    /// configurations change only when the node opens a newer one. A second
    /// call changes nothing.
    #[must_use]
    pub fn with_removal(self, registers: impl ShardRegisters, ids: ProposalIds) -> Self {
        let removal = removal::Removal {
            registers: Box::new(registers),
            ids: Mutex::new(ids),
        };
        let _ = self.inner.removal.set(Arc::new(removal));
        self
    }

    /// Backfills the learners of this node's primaries through `backfill`
    /// (§6.7) instead of the built-in backfill of their payload: the hook
    /// reports when a learner holds the shard's history, and a primary
    /// promotes a learner only then. Tests hold promotions back with it. A
    /// second call changes nothing.
    #[must_use]
    pub fn with_backfill(self, backfill: impl Backfill) -> Self {
        let _ = self.inner.backfill.set(Arc::new(backfill));
        self
    }

    /// Lets this node's members take over from a primary that stays silent
    /// for `primary_grace` (§5.4, §6.5), by replacing their shard's
    /// register through the registers [`Replication::with_removal`] gave:
    /// without them, it changes nothing. Members opened from then on watch
    /// their primary.
    #[must_use]
    pub fn with_takeover(self) -> Self {
        self.inner.takeover.store(true, Ordering::SeqCst);
        self
    }

    /// The node's shard replicas.
    #[must_use]
    pub fn set(&self) -> &ShardSet<D> {
        &self.inner.set
    }

    /// The grace of this node's replica of `shard`, if it is a member of
    /// it: the time since it last granted the primary a lease (§5.4).
    #[must_use]
    pub fn grace(&self, shard: &ShardRef) -> Option<Arc<Grace>> {
        let graces = self.inner.graces.lock();
        graces
            .unwrap_or_else(PoisonError::into_inner)
            .get(shard)
            .cloned()
    }

    /// Opens this node's replica in the shard of `config`, or returns it if
    /// it is open, adopting `config` if it is newer
    /// ([`ShardSet::open_replica`]). As the primary of a replicated shard,
    /// it starts its links to the members and learners on the current
    /// Tokio runtime, which run until the shard stops, its leases, and, if
    /// the node removes members, the watch over them, which also tends its
    /// learners. As a member, it starts the shard's grace, unless it is
    /// running: opening counts as granting a lease, since this life does
    /// not know when the node last granted one. A learner waits for its
    /// primary's sessions. Either way its requests wait for the members at
    /// most [`ReplicationConfig::ack_timeout`].
    ///
    /// The primary of a shard learns of a learner added to its register
    /// when the node opens the newer configuration, as the coordinator's
    /// change propagation will have it do (plan M3-05), or reads the
    /// register after a member refused it or a compare-and-swap failed.
    ///
    /// A node that starts opens its replicas with
    /// [`Replication::resume`] instead, which finds the configuration in
    /// the register or, while the register cannot be read, in the node's
    /// local copy (§6.2).
    ///
    /// # Errors
    ///
    /// As [`ShardSet::open_replica`].
    pub async fn open(&self, config: &ShardConfig) -> Result<Shard<D>, ShardError> {
        let inner = &self.inner;
        let shard = inner.set.open_replica(config, &inner.node).await?;
        shard.set_ack_timeout(inner.config.ack_timeout);
        if shard.role() == Role::Member {
            let grace = inner.grace_of(shard.shard());
            self.watch_primary(&shard, grace);
        }
        self.start_primary(&shard);
        if shard.role() == Role::Alone && is_under_replicated(&shard.config()) {
            self.on_adopted()(&shard.config());
        }
        Ok(shard)
    }

    /// Starts a primary's links to its members, its leases, and, if the
    /// node removes members, the watch over them, unless they run already.
    /// A primary that stepped down in an earlier life starts none of them:
    /// it watches the register for its successor instead (§5.4).
    fn start_primary(&self, shard: &Shard<D>) {
        let inner = &self.inner;
        if let Some(epoch) = shard.stepped_down() {
            if shard.leader().is_some_and(|leader| leader.claim_links()) {
                self.await_successor(shard, epoch);
            }
            return;
        }
        let Some(leader) = shard.leader().filter(|leader| leader.claim_links()) else {
            return;
        };
        leader.start_leases(Arc::clone(&inner.clock), inner.config.primary_lease);
        let removal = inner.removal.get();
        if let Some(removal) = removal {
            let watchdog = removal::Watchdog {
                shard: shard.clone(),
                leader: Arc::clone(leader),
                node: inner.node.clone(),
                removal: Arc::clone(removal),
                config: inner.config,
                adopted: self.on_adopted(),
                backfill: inner.backfill.get().cloned(),
                fill: self.fill(shard, leader),
            };
            tokio::spawn(watchdog.run());
        }
        let mut links = BTreeMap::new();
        self.link_peers(shard, leader, &mut links);
        let linking = (self.clone(), shard.clone(), Arc::clone(leader));
        tokio::spawn(async move {
            // Links to the learners a later configuration adds (§6.7), and
            // again to a peer whose link ended, as a learner's does when it
            // is removed and added back.
            let (replication, shard, leader) = linking;
            let mut peers = leader.peers();
            let interval = replication.inner.config.link_timeout;
            while !shard.is_stopped() {
                match tokio::time::timeout(interval, peers.changed()).await {
                    Ok(Ok(())) => replication.link_peers(&shard, &leader, &mut links),
                    Ok(Err(_)) => return,
                    Err(_) => {}
                }
            }
        });
        if is_under_replicated(&shard.config()) {
            self.on_adopted()(&shard.config());
        }
    }

    /// The built-in backfill of the payload of the learners of `shard`,
    /// which `leader` leads (§6.7).
    fn fill(&self, shard: &Shard<D>, leader: &Arc<Leader>) -> removal::Fill {
        let (replication, shard, leader) = (self.clone(), shard.clone(), Arc::clone(leader));
        Arc::new(move |learner| {
            let (replication, shard, leader) =
                (replication.clone(), shard.clone(), Arc::clone(&leader));
            Box::pin(async move {
                backfill::fill(&replication, &shard, &leader, &learner)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
    }

    /// Starts a link to each member and learner of `leader` that has none
    /// running in `links`, in the order the leader lists them.
    fn link_peers(
        &self,
        shard: &Shard<D>,
        leader: &Arc<Leader>,
        links: &mut BTreeMap<NodeId, tokio::task::JoinHandle<()>>,
    ) {
        let inner = &self.inner;
        let peers = leader.peers().borrow().clone();
        for peer in peers {
            if links.get(&peer).is_some_and(|link| !link.is_finished()) {
                continue;
            }
            let Some(address) = inner.peers.get(&peer) else {
                tracing::warn!(shard = %leader.shard(), %peer, "no address for a member or learner");
                continue;
            };
            let link = primary::Link {
                shard: shard.clone(),
                leader: Arc::clone(leader),
                member: peer.clone(),
                address: address.clone(),
                transport: inner.transport.clone(),
                config: inner.config,
                watched: inner.removal.get().is_some(),
            };
            links.insert(peer, tokio::spawn(link.run()));
        }
    }

    /// Hands the shard off to its member `to` (§5.4): this node's replica,
    /// its primary, checks that the shard's register holds its
    /// configuration, if the node has the registers, and removes no member
    /// from then on; it stops serving reads and writes and renewing its leases,
    /// lets the reads and writes in progress finish, records the step-down
    /// durably ([`Shard::stepped_down`]), and sends `to` a step-down message
    /// with the last `seq` it sequenced. `to` then proposes itself as
    /// primary at once, as in a takeover (§6.5); the old primary is left
    /// out of the next configuration and rejoins only as a learner.
    ///
    /// It waits for the message to be sent at most
    /// [`ReplicationConfig::link_timeout`]. If it is not, or is lost on the
    /// way, `to` and the other members take over once their grace passes.
    /// Either way the replica serves nothing more: it answers requests as
    /// unavailable, and with a redirect to the new primary once it reads
    /// the new configuration in the shard's register, if the node has the
    /// registers ([`Replication::with_removal`]).
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] if the shard is not open on this node;
    /// [`ShardError::Configuration`] if the replica is not a primary that
    /// leads the shard, or `to` is not another member of it;
    /// [`ShardError::Unavailable`] if the shard stopped, its register cannot
    /// be read or holds another configuration (as after a removal whose
    /// answer was lost), or the step-down could not be recorded.
    pub async fn hand_off(&self, shard: &ShardRef, to: &NodeId) -> Result<HandedOff, ShardError> {
        let replica = self
            .inner
            .set
            .get(shard)
            .await
            .ok_or_else(|| ShardError::NotFound(shard.clone()))?;
        let leader = replica.leader().ok_or_else(|| {
            ShardError::configuration(shard, "the replica does not lead the shard")
        })?;
        // No removal moves the register on from here; one that landed
        // unseen, or is in flight, would leave this primary leading a
        // configuration it did not step down in.
        let _changing = leader.changing().lock().await;
        // A promotion that may have landed unseen made the learner a member
        // the candidate must hold every record for.
        if leader.promoting().is_some() {
            return Err(ShardError::unavailable(shard, "a promotion is outstanding"));
        }
        if let Some(removal) = self.inner.removal.get() {
            let held = removal
                .registers
                .read(shard)
                .await
                .map_err(|error| ShardError::unavailable(shard, error))?;
            if held.as_ref() != Some(&replica.config()) {
                return Err(ShardError::unavailable(
                    shard,
                    "the shard's register holds another configuration",
                ));
            }
        }
        tracing::info!(%shard, %to, "handing the shard off");
        let (epoch, last) = replica.step_down(to).await?;
        let sent = leader.await_step_down(self.inner.config.link_timeout).await;
        replica.depose("the primary stepped down for a planned handoff", None);
        self.await_successor(&replica, epoch);
        Ok(HandedOff { epoch, last, sent })
    }

    /// Reads the register of `shard`, whose primary stepped down in
    /// `epoch`, every `lease_renew_interval` until it names a newer
    /// configuration, or none: the replica then redirects to it (§6.5).
    fn await_successor(&self, shard: &Shard<D>, epoch: Epoch) {
        let Some(removal) = self.inner.removal.get().cloned() else {
            return;
        };
        let (shard, interval) = (shard.clone(), self.inner.config.lease_renew_interval);
        tokio::spawn(async move {
            // Until the register names a newer configuration, or another
            // path deposed the replica with one.
            while shard.config().epoch <= epoch {
                tokio::time::sleep(interval).await;
                match removal.registers.read(shard.shard()).await {
                    Ok(held) if held.as_ref().is_none_or(|held| held.epoch > epoch) => {
                        shard.depose("another primary took the shard over", held.as_ref());
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::debug!(shard = %shard.shard(), %error, "reading the shard's register failed");
                    }
                }
            }
        });
    }

    /// Starts a member's watch over its primary, to take over once the
    /// primary goes silent (§6.5), if the node's members take over and the
    /// watch does not run already.
    fn watch_primary(&self, shard: &Shard<D>, grace: Arc<Grace>) {
        let inner = &self.inner;
        let Some(removal) = inner.removal.get() else {
            return;
        };
        if !inner.takeover.load(Ordering::SeqCst) {
            return;
        }
        let key = shard.shard().clone();
        if !lock(&inner.candidates).insert(key.clone()) {
            return;
        }
        let candidate = takeover::Candidate {
            replication: self.clone(),
            shard: shard.clone(),
            grace,
            removal: Arc::clone(removal),
        };
        let candidates = self.clone();
        tokio::spawn(async move {
            candidate.run().await;
            lock(&candidates.inner.candidates).remove(&key);
        });
    }

    /// What the watchdog of a primary calls with each configuration it
    /// adopts: notes since when the shard is under-replicated.
    fn on_adopted(&self) -> Box<dyn Fn(&ShardConfig) + Send + Sync> {
        let (under, clock) = (Arc::clone(&self.inner.under), Arc::clone(&self.inner.clock));
        Box::new(move |config| {
            let shard = ShardRef::new(config.bucket_id.clone(), config.shard);
            let mut under = under.lock().unwrap_or_else(PoisonError::into_inner);
            if is_under_replicated(config) {
                under.entry(shard).or_insert_with(|| clock.now());
            } else {
                under.remove(&shard);
            }
        })
    }

    /// What the shards this node leads hold with fewer copies than their
    /// `replicas`, and since when (§6.4): the metrics
    /// `under_replicated_bytes` and `oldest_under_replicated_age`. A shard
    /// counts while its configuration has fewer members than `replicas`.
    /// It reads the index of each such shard, so it is for a periodic
    /// report, not for every request.
    pub async fn exposure(&self) -> Exposure {
        let set = &self.inner.set;
        let mut counted = Vec::new();
        let mut exposure = Exposure::default();
        for shard in set.shards().await {
            let Some(replica) = set.get(&shard).await else {
                continue;
            };
            let config = replica.config();
            if replica.role().follows() || replica.is_stopped() || !is_under_replicated(&config) {
                continue;
            }
            match replica.stored_bytes().await {
                Ok(bytes) => exposure.bytes = exposure.bytes.saturating_add(bytes),
                Err(error) => tracing::debug!(%shard, %error, "a shard's bytes are not known"),
            }
            exposure.shards += 1;
            counted.push(shard);
        }
        let now = self.inner.clock.now();
        let mut under = self
            .inner
            .under
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        under.retain(|shard, _| counted.contains(shard));
        for shard in counted {
            let since = *under.entry(shard).or_insert(now);
            exposure.oldest = exposure.oldest.max(now.saturating_duration_since(since));
        }
        exposure
    }

    /// Sets `metrics` to [`Replication::exposure`] every `interval`, until
    /// the returned future is dropped.
    pub async fn report_exposure(&self, metrics: &ReplicationMetrics, interval: Duration) {
        loop {
            metrics.set(self.exposure().await);
            tokio::time::sleep(interval).await;
        }
    }

    /// Accepts links from primaries on `listener` until it fails, serving
    /// each on a task of its own.
    pub async fn serve(&self, listener: Listener<N>) {
        loop {
            let incoming = match listener.accept().await {
                Ok(incoming) => incoming,
                Err(error) => {
                    tracing::warn!(%error, "accepting a replication link failed");
                    tokio::time::sleep(self.inner.config.reconnect_delay).await;
                    continue;
                }
            };
            let replication = self.clone();
            tokio::spawn(async move {
                let connection = match incoming.handshake().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        tracing::debug!(%error, "a replication link failed its handshake");
                        return;
                    }
                };
                let (mut receiver, sender) = connection.into_split();
                let timeout = replication.inner.config.link_timeout;
                match recv(&mut receiver, timeout).await {
                    Ok(first) => replication.follow((receiver, sender), first).await,
                    Err(error) => tracing::debug!(%error, "a replication link sent nothing"),
                }
            });
        }
    }

    /// Serves a link from a primary, whose first frame `first` arrived on
    /// `link`, as a member, until the link fails or a later session
    /// replaces it. A node whose transport also carries other messages
    /// accepts connections itself and hands replication links here.
    pub async fn follow<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let served = if first.header.kind == skys3_net::MessageKind::Backfill {
            backfill::serve(link, &first, self).await
        } else {
            member::serve(link, first, self).await
        };
        if let Err(error) = served {
            tracing::debug!(%error, "a replication link to a primary ended");
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update leaves the value consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `config` has fewer members than its `replicas` (§6.4).
fn is_under_replicated(config: &ShardConfig) -> bool {
    config.members.len() < usize::from(config.replicas)
}

/// What a primary that handed its shard off told the candidate (§5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandedOff {
    /// The epoch the primary stepped down in.
    pub epoch: Epoch,
    /// The last `seq` it sequenced, which the candidate holds before it
    /// proposes itself.
    pub last: Seq,
    /// Whether the step-down message was sent. It may still be lost on
    /// the way; if it is, the members take over once their grace passes.
    pub sent: bool,
}

/// Why a link ended.
#[derive(Debug, thiserror::Error)]
enum LinkError {
    #[error("the primary changed its configuration")]
    Reconfigured,

    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("no frame came within {0:?}")]
    Timeout(Duration),
    #[error("the peer closed the link")]
    Closed,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Shard(#[from] ShardError),
}

/// Receives the next frame within `timeout`.
async fn recv<S>(receiver: &mut Receiver<S>, timeout: Duration) -> Result<Frame, LinkError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, receiver.recv()).await {
        Err(_) => Err(LinkError::Timeout(timeout)),
        Ok(Ok(Some(frame))) => Ok(frame),
        Ok(Ok(None)) => Err(LinkError::Closed),
        Ok(Err(error)) => Err(error.into()),
    }
}
