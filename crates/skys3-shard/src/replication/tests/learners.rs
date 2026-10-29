//! Learners over real TCP (§6.4, §6.7): a primary and a member, and a new
//! node that the test driver adds as a learner by a compare-and-swap of
//! the shard's register, as the coordinator does (plan M3-05). Every link
//! goes through a proxy the test can cut.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_control::{
    ControlError, Expected, MemoryControlStore, ProposalIds, RetryPolicy, TypedKey,
    propose_document,
};
use skys3_io::SimMount;
use skys3_log::ShardRef;
use skys3_net::TokioNetwork;
use skys3_types::{Epoch, EpochSeq, NodeId, ProposalId, Seq, ShardConfig};
use tokio::sync::watch;
use tokio::time::Instant;

use super::removal::{Proxy, initial, ms, put, register, timing, until};
use super::takeover::run;
use super::{Pki, node, shard, shard_set};
use crate::error::ShardError;
use crate::replication::{
    Backfill, BoxFuture, ControlRegisters, Exposure, Replaced, Replication, ShardRegisters,
};
use crate::shard::{Role, Shard};

/// Shard `b-1/0` in epoch 1 on nodes 1 and 2, node 1 its primary, which
/// aims for three members.
pub(super) fn two_members() -> ShardConfig {
    ShardConfig {
        members: vec![node(1), node(2)],
        ..initial()
    }
}

/// `config`'s next configuration, with `learner` added as a learner: the
/// test driver's compare-and-swap.
pub(super) fn with_learner(config: &ShardConfig, learner: u8) -> ShardConfig {
    ShardConfig {
        epoch: Epoch::new(config.epoch.get() + 1),
        learners: vec![node(learner)],
        proposal_id: ProposalId::new(format!("add-{learner}")).unwrap(),
        ..config.clone()
    }
}

/// Whether `next` promotes a learner of `current`.
fn promotes(current: &ShardConfig, next: &ShardConfig) -> bool {
    next.members.iter().any(|m| current.is_learner(m))
}

/// The shard registers of a store, with the promotions held up for a
/// while, as by a slow control store, and answered first by `scripted`
/// answers, if any.
pub(super) struct Promotions {
    pub(super) registers: ControlRegisters<MemoryControlStore>,
    pub(super) delay: Duration,
    pub(super) scripted: Mutex<VecDeque<Result<Replaced, ControlError>>>,
    /// How many promotions were proposed.
    pub(super) proposed: Arc<Mutex<usize>>,
    /// The epochs of each promotion's compare-and-swap: the one it
    /// expects, and the one it writes.
    pub(super) epochs: Arc<Mutex<Vec<(Epoch, Epoch)>>>,
}

impl ShardRegisters for Promotions {
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>> {
        Box::pin(async move {
            if !promotes(current, next) {
                return self.registers.replace(current, next).await;
            }
            *lock(&self.proposed) += 1;
            lock(&self.epochs).push((current.epoch, next.epoch));
            tokio::time::sleep(self.delay).await;
            let scripted = lock(&self.scripted).pop_front();
            match scripted {
                Some(answer) => answer,
                None => self.registers.replace(current, next).await,
            }
        })
    }

    fn read<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        self.registers.read(shard)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A backfill that completes once the test opens its gate.
struct Gate(watch::Receiver<bool>);

impl Backfill for Gate {
    fn backfill<'a>(&'a self, _: &'a ShardRef, _: &'a NodeId) -> BoxFuture<'a, Result<(), String>> {
        let mut open = self.0.clone();
        Box::pin(async move {
            let _ = open.wait_for(|open| *open).await;
            Ok(())
        })
    }
}

/// Nodes 1 to 3, none with the shard open, each reaching the others
/// through proxies of its own, with the register in `store` holding
/// `config`. Node 1's promotions go through `promotions`, and its
/// backfills through `gate`, if there is one.
pub(super) struct Nodes {
    pub(super) replications: Vec<Replication<TokioNetwork, SimMount>>,
    /// The proxies of node 1's links, by the node they reach.
    proxies: BTreeMap<NodeId, Proxy>,
}

pub(super) async fn nodes(
    pki: &Pki,
    store: &MemoryControlStore,
    config: &ShardConfig,
    promotions: Promotions,
    gate: Option<watch::Receiver<bool>>,
) -> Nodes {
    let policy = RetryPolicy::default();
    let key = TypedKey::shard(&config.bucket_id, config.shard);
    let created = propose_document(store, &key, Expected::Absent, config, &policy).await;
    assert!(created.is_ok());
    let mut listeners = Vec::new();
    for n in 1..=3 {
        let listener = pki
            .transport(&node(n))
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        listeners.push(listener);
    }
    let addresses: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let mut proxies = BTreeMap::new();
    let mut peers = BTreeMap::new();
    for (to, address) in (2..=3).zip(&addresses[1..]) {
        let proxy = Proxy::to(*address).await;
        peers.insert(node(to), proxy.address.to_string().parse().unwrap());
        proxies.insert(node(to), proxy);
    }
    let mut replications = Vec::new();
    let mut promotions = Some(promotions);
    for (n, listener) in (1..=3).zip(listeners) {
        let (set, clock) = shard_set(70 + u64::from(n)).await;
        let peers = if n == 1 {
            peers.clone()
        } else {
            BTreeMap::new()
        };
        let replication = Replication::new(
            node(n),
            set,
            pki.transport(&node(n)),
            peers,
            clock,
            timing(),
        );
        let ids = ProposalIds::seeded(u64::from(n));
        let replication = match (promotions.take(), &gate) {
            (Some(promotions), Some(gate)) => replication
                .with_removal(promotions, ids)
                .with_backfill(Gate(gate.clone())),
            (Some(promotions), None) => replication.with_removal(promotions, ids),
            (None, _) => {
                replication.with_removal(ControlRegisters::new(store.clone(), policy), ids)
            }
        };
        let serving = replication.clone();
        tokio::spawn(async move { serving.serve(listener).await });
        replications.push(replication);
    }
    Nodes {
        replications,
        proxies,
    }
}

/// Commits a `PUT` of `key` and returns how long it took.
pub(super) async fn timed_put(primary: &Shard<SimMount>, key: &str) -> Duration {
    let started = Instant::now();
    primary.commit(put(key, 10)).await.unwrap();
    started.elapsed()
}

/// The replica of the shard `replication` has open now: a learner opens
/// its replica again once it installed a snapshot.
pub(super) async fn current(replication: &Replication<TokioNetwork, SimMount>) -> Shard<SimMount> {
    let deadline = Instant::now() + super::WAIT;
    loop {
        if let Some(replica) = replication.set().get(&shard()).await {
            return replica;
        }
        assert!(Instant::now() < deadline, "the shard did not open again");
        tokio::time::sleep(ms(5)).await;
    }
}

/// The test driver adds node 3 to `current` as a learner, and tells the
/// learner and the primary, as the coordinator's change propagation will.
pub(super) async fn add_learner(
    store: &MemoryControlStore,
    nodes: &Nodes,
    current: &ShardConfig,
) -> (ShardConfig, Shard<SimMount>) {
    let next = with_learner(current, 3);
    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    assert_eq!(
        registers.replace(current, &next).await.unwrap(),
        Replaced::Accepted
    );
    let learner = nodes.replications[2].open(&next).await.unwrap();
    assert_eq!(learner.role(), Role::Learner);
    nodes.replications[0].open(&next).await.unwrap();
    (next, learner)
}

#[test]
fn a_learner_catches_up_and_is_promoted_without_pausing_commits() {
    run(promotion());
}

async fn promotion() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let (opened, gate) = watch::channel(false);
    let proposed = Arc::default();
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: ms(600),
        scripted: Mutex::default(),
        proposed: Arc::clone(&proposed),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, Some(gate)).await;
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving() && leader.holds_leases()).await;
    for key in ["a", "b", "c"] {
        timed_put(&primary, key).await;
    }

    // The new learner takes the log from its first record, and joins the
    // acknowledgement set once it has caught up.
    let (added, _) = add_learner(&store, &nodes, &two_members()).await;
    until(|| leader.acking() == [node(3)]).await;
    assert_eq!(leader.learners(), [node(3)]);
    // Only the primary reports the shard's exposure, not its learner.
    assert_eq!(nodes.replications[0].exposure().await.shards, 1);
    assert_eq!(nodes.replications[2].exposure().await, Exposure::default());
    timed_put(&primary, "d").await;
    let committed = leader.commit();
    let learner = current(&nodes.replications[2]).await;
    assert!(*learner.durable().borrow() >= committed);
    // It installed a snapshot of the primary's index, taken once the
    // primary had applied its CONFIG record of epoch 2, and holds none of
    // the records before it.
    assert!(learner.applied() >= EpochSeq::new(Epoch::new(2), Seq::new(3)));
    assert_eq!(learner.lineage().epoch_at(Seq::new(1)), None);
    assert!(learner.check_readable().is_err());

    // Its backfill is not complete: no promotion yet.
    tokio::time::sleep(ms(300)).await;
    assert_eq!(*lock(&proposed), 0);
    assert_eq!(register(&store).await, added);
    assert!(leader.promoting().is_none());

    // Once it is, the primary proposes the promotion, and recorded it
    // first. The control store takes 600 ms to answer; commits do not
    // wait for it, only for the learner, as before.
    opened.send_replace(true);
    let recorded = || {
        let index = primary.index().read().unwrap();
        index.promotion(primary.shard()).unwrap()
    };
    until(|| recorded().is_some()).await;
    assert_eq!(recorded(), leader.promoting());
    until(|| *lock(&proposed) == 1).await;
    let mut slowest = Duration::ZERO;
    while leader.promoting().is_some() {
        slowest = slowest.max(timed_put(&primary, "e").await);
    }
    assert!(
        slowest < ms(300),
        "a write stalled {slowest:?} during the promotion"
    );

    // The register made node 3 a member; the primary and the learner
    // follow, and commits wait for it as a member.
    let promoted = register(&store).await;
    assert_eq!(promoted.epoch, Epoch::new(3));
    assert_eq!(promoted.members, [node(1), node(2), node(3)]);
    assert!(promoted.learners.is_empty());
    until(|| leader.members() == [node(2), node(3)] && leader.learners().is_empty()).await;
    until(|| learner.role() == Role::Member && learner.config() == promoted).await;
    timed_put(&primary, "f").await;
    assert!(*learner.durable().borrow() >= leader.commit());
    until(|| learner.sequencing() == Epoch::new(3)).await;
}

#[test]
fn a_silent_learner_leaves_the_acknowledgement_set_without_a_cas() {
    run(silence());
}

async fn silence() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    // The backfill never completes, so the learner stays one.
    let (_closed, gate) = watch::channel(false);
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: Duration::ZERO,
        scripted: Mutex::default(),
        proposed: Arc::default(),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, Some(gate)).await;
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving()).await;
    timed_put(&primary, "a").await;
    let (added, _) = add_learner(&store, &nodes, &two_members()).await;
    until(|| leader.acking() == [node(3)]).await;

    // Cut off, the learner holds up a write for member_suspect_after, then
    // leaves the set; the register does not change.
    nodes.proxies[&node(3)].cut();
    let stalled = timed_put(&primary, "b").await;
    assert!(stalled >= ms(200), "the write stalled only {stalled:?}");
    assert!(leader.acking().is_empty());
    assert_eq!(register(&store).await, added);
    assert!(timed_put(&primary, "c").await < ms(200));

    // Back, it catches up before it rejoins.
    nodes.proxies[&node(3)].heal();
    until(|| leader.acking() == [node(3)]).await;
    let learner = current(&nodes.replications[2]).await;
    assert!(*learner.durable().borrow() >= leader.commit());
    assert_eq!(learner.role(), Role::Learner);
}

#[test]
fn a_promotion_that_loses_follows_the_register() {
    run(lost());
}

async fn lost() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let (_open, gate) = watch::channel(true);
    let added = with_learner(&two_members(), 3);
    // The coordinator removed the learner meanwhile.
    let removed = ShardConfig {
        epoch: Epoch::new(3),
        learners: Vec::new(),
        proposal_id: ProposalId::new("removed").unwrap(),
        ..added.clone()
    };
    let scripted = VecDeque::from([
        Err(ControlError::Unavailable("down".to_owned())),
        Ok(Replaced::Holds(Some(removed.clone()))),
    ]);
    let proposed = Arc::default();
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: ms(50),
        scripted: Mutex::new(scripted),
        proposed: Arc::clone(&proposed),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, Some(gate)).await;
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving()).await;
    timed_put(&primary, "a").await;
    add_learner(&store, &nodes, &two_members()).await;

    // The first attempt fails and is sent again; the second finds the
    // learner removed: the primary adopts that, and stops waiting.
    until(|| primary.config() == removed).await;
    assert_eq!(*lock(&proposed), 2);
    until(|| leader.promoting().is_none() && leader.learners().is_empty()).await;
    assert!(leader.acking().is_empty());
    assert!(timed_put(&primary, "b").await < ms(200));
    assert!(!primary.is_stopped());
}

#[test]
fn a_primary_hands_off_only_without_a_promotion_outstanding() {
    run(handoff());
}

async fn handoff() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let (_open, gate) = watch::channel(true);
    // The control store does not answer the promotion.
    let down = || Err(ControlError::Unavailable("down".to_owned()));
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: ms(20),
        scripted: Mutex::new((0..1000).map(|_| down()).collect()),
        proposed: Arc::default(),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, Some(gate)).await;
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving()).await;
    add_learner(&store, &nodes, &two_members()).await;
    until(|| leader.promoting().is_some()).await;
    let refused = nodes.replications[0].hand_off(&shard(), &node(2)).await;
    assert!(
        matches!(&refused, Err(ShardError::Unavailable { reason, .. }) if reason.contains("promotion")),
        "{refused:?}"
    );
}

#[test]
fn a_promotion_expects_the_configuration_it_was_built_from() {
    run(overtaken());
}

/// The register moves on while the primary records its promotion: the
/// proposal, built over epoch 2, must not be written over epoch 3.
async fn overtaken() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let (opened, gate) = watch::channel(false);
    let epochs = Arc::default();
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: Duration::ZERO,
        scripted: Mutex::default(),
        proposed: Arc::default(),
        epochs: Arc::clone(&epochs),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, Some(gate)).await;
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving()).await;
    timed_put(&primary, "a").await;
    let (added, _learner) = add_learner(&store, &nodes, &two_members()).await;
    until(|| leader.acking() == [node(3)]).await;

    // Hold the primary's index, so that recording the promotion waits.
    let (release, held) = std::sync::mpsc::channel::<()>();
    let (holding, hold) = std::sync::mpsc::channel();
    let index = Arc::clone(primary.index());
    let holder = std::thread::spawn(move || {
        index
            .update_control(|_| {
                holding.send(()).unwrap();
                let _ = held.recv();
                Ok(())
            })
            .unwrap();
    });
    hold.recv().unwrap();
    opened.send_replace(true);
    until(|| leader.promoting().is_some()).await;

    // Meanwhile the coordinator removes member 2, and the primary adopts
    // that.
    let removed = ShardConfig {
        epoch: Epoch::new(3),
        members: vec![node(1)],
        proposal_id: ProposalId::new("removed").unwrap(),
        ..added.clone()
    };
    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    assert_eq!(
        registers.replace(&added, &removed).await.unwrap(),
        Replaced::Accepted
    );
    let adopting = {
        let (primary, removed) = (primary.clone(), removed.clone());
        tokio::spawn(async move { primary.reconfigure(&removed).await })
    };
    until(|| primary.config() == removed).await;
    release.send(()).unwrap();
    holder.join().unwrap();
    adopting.await.unwrap().unwrap();

    // The stale promotion is never written over the newer register, which
    // would bring node 2 back; a new one, over epoch 3, may be.
    let promoted = || async {
        let now = register(&store).await;
        now.epoch > Epoch::new(3) && now.is_member(&node(3))
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !promoted().await {
        assert!(Instant::now() < deadline, "no promotion over epoch 3");
        tokio::time::sleep(ms(10)).await;
    }
    let now = register(&store).await;
    assert_eq!(now.epoch, Epoch::new(4));
    assert_eq!(now.members, [node(1), node(3)]);
    let epochs = lock(&epochs).clone();
    assert!(
        epochs.iter().all(|(over, to)| over.get() + 1 == to.get()),
        "{epochs:?}"
    );
}
