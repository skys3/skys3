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
//!   leases ([`Leader`](crate::Leader)). A link that fails,
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
//! - **Removing members** (§6.4), once [`Replication::with_removal`] gives
//!   the node the shard registers: a primary removes a member that stays
//!   unresponsive for [`ReplicationConfig::member_suspect_after`], by a
//!   compare-and-swap of the shard's register to the next epoch without
//!   it. The records the member held back commit under the new epoch once
//!   the primary's `CONFIG` record is durable, and a shard left with fewer
//!   members than its `min_write_replicas` stays readable and refuses
//!   writes. [`Replication::exposure`] reports what then has fewer copies
//!   than `replicas`.
//!
//! A configuration change switches epochs at the same `seq` on every
//! replica (§5.1): a primary's session carries its newest configuration
//! and the epoch it sequences in, and a member in an earlier epoch takes
//! that epoch's tail before it appends its own `CONFIG` record where the
//! primary's is. A link whose primary changes configuration opens a new
//! session; one to a member the configuration leaves out ends.

mod exposure;
mod member;
mod primary;
mod removal;
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
use skys3_types::{NodeAddress, NodeId, ShardConfig};

pub use self::exposure::{Exposure, ReplicationMetrics};
pub use self::removal::{BoxFuture, ControlRegisters, Replaced, ShardRegisters};
use crate::ack::AckTimeout;
use crate::error::ShardError;
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
    /// it starts its links to the members on the current Tokio runtime,
    /// which run until the shard stops, its leases, and, if the node removes
    /// members, the watch over them. As a member, it starts the shard's
    /// grace, unless it is running: opening counts as granting a lease,
    /// since this life does not know when the node last granted one. Either
    /// way its requests wait for the members at most
    /// [`ReplicationConfig::ack_timeout`].
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
    fn start_primary(&self, shard: &Shard<D>) {
        let inner = &self.inner;
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
            };
            tokio::spawn(watchdog.run());
        }
        for member in leader.members() {
            let Some(address) = inner.peers.get(&member) else {
                tracing::warn!(shard = %leader.shard(), %member, "no address for a member");
                continue;
            };
            let link = primary::Link {
                shard: shard.clone(),
                leader: Arc::clone(leader),
                member,
                address: address.clone(),
                transport: inner.transport.clone(),
                config: inner.config,
                watched: removal.is_some(),
            };
            tokio::spawn(link.run());
        }
        if is_under_replicated(&shard.config()) {
            self.on_adopted()(&shard.config());
        }
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
            if replica.role() == Role::Member
                || replica.is_stopped()
                || !is_under_replicated(&config)
            {
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
        if let Err(error) = member::serve(link, first, &self.inner).await {
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
