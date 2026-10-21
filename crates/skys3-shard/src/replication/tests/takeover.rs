//! Primary takeover over real TCP (§6.5, §6.6): three nodes whose links
//! each go through a proxy the test can cut, with the shard's register in
//! an in-memory control store.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use skys3_control::{
    Expected, MemoryControlStore, ProposalIds, RetryPolicy, TypedKey, propose_document,
};
use skys3_io::SimMount;
use skys3_net::TokioNetwork;
use skys3_types::{Epoch, NodeId, ProposalId, ShardConfig};
use tokio::time::Instant;

use super::removal::{Proxy, initial, ms, put, register, timing, until};
use super::{Pki, WAIT, node, shard_set};
use crate::error::ShardError;
use crate::replication::{
    ControlRegisters, Replaced, Replication, ReplicationConfig, ShardRegisters as _,
};
use crate::shard::{Role, Shard};

/// One node of the test cluster, and the proxies its links to each other
/// node go through.
pub(super) struct Node {
    pub(super) replication: Replication<TokioNetwork, SimMount>,
    pub(super) shard: Shard<SimMount>,
    proxies: BTreeMap<NodeId, Proxy>,
}

impl Node {
    /// Cuts the links from this node to `to`.
    pub(super) fn cut(&self, to: u8) {
        self.proxies[&node(to)].cut();
    }

    pub(super) fn heal(&self, to: u8) {
        self.proxies[&node(to)].heal();
    }
}

/// Starts nodes 1 to `count` with the shard open in `config`, each
/// reaching the others through proxies of its own, with the register in
/// `store` and its timings from `timing`, which may differ per node.
pub(super) async fn cluster(
    pki: &Pki,
    count: u8,
    config: &ShardConfig,
    store: &MemoryControlStore,
    timing: impl Fn(u8) -> ReplicationConfig,
) -> Vec<Node> {
    let key = TypedKey::shard(&config.bucket_id, config.shard);
    let policy = RetryPolicy::default();
    let created = propose_document(store, &key, Expected::Absent, config, &policy).await;
    assert!(created.is_ok());
    let mut listeners = BTreeMap::new();
    for n in 1..=count {
        let listener = pki
            .transport(&node(n))
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        listeners.insert(n, listener);
    }
    let addresses: BTreeMap<u8, SocketAddr> = listeners
        .iter()
        .map(|(n, listener)| (*n, listener.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::new();
    for (n, listener) in listeners {
        let mut proxies = BTreeMap::new();
        let mut peers = BTreeMap::new();
        for (&to, &address) in &addresses {
            if to != n {
                let proxy = Proxy::to(address).await;
                peers.insert(node(to), proxy.address.to_string().parse().unwrap());
                proxies.insert(node(to), proxy);
            }
        }
        let (set, clock) = shard_set(50 + u64::from(n)).await;
        let replication = Replication::new(
            node(n),
            set,
            pki.transport(&node(n)),
            peers,
            clock,
            timing(n),
        )
        .with_removal(
            ControlRegisters::new(store.clone(), policy),
            ProposalIds::seeded(u64::from(n)),
        )
        .with_takeover();
        let shard = replication.open(config).await.unwrap();
        let serving = replication.clone();
        tokio::spawn(async move { serving.serve(listener).await });
        nodes.push(Node {
            replication,
            shard,
            proxies,
        });
    }
    nodes
}

/// The timings of node `n`: node 1, the first primary, suspects its
/// members only long after their grace passes, so that it does not remove
/// them before they take over: a primary cut off from its members but not
/// from the control store removes them first (§6.4).
pub(super) fn patient(n: u8) -> ReplicationConfig {
    let timing = timing();
    if n == 1 {
        ReplicationConfig {
            member_suspect_after: timing.primary_grace * 30,
            ..timing
        }
    } else {
        timing
    }
}

pub(super) fn run(scenario: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, scenario)
            .await
            .expect("the test finished in time");
    });
}

/// The node whose replica serves as primary in an epoch after the first,
/// once there is one.
pub(super) async fn new_primary(nodes: &[Node]) -> usize {
    loop {
        let serving = nodes.iter().position(|n| {
            n.shard.role() == Role::Primary
                && n.shard.config().epoch > Epoch::new(1)
                && n.shard.is_serving()
        });
        if let Some(found) = serving {
            return found;
        }
        tokio::time::sleep(ms(5)).await;
    }
}

#[test]
fn a_member_takes_over_from_a_silent_primary_which_it_then_deposes() {
    run(silent_primary());
}

async fn silent_primary() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let nodes = cluster(&pki, 3, &initial(), &store, patient).await;
    let old = &nodes[0].shard;
    until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
    old.commit(put("a", 10)).await.unwrap();

    // The primary can reach neither member: both members' grace passes,
    // and they compete for the shard; the register picks one.
    let cut = Instant::now();
    nodes[0].cut(2);
    nodes[0].cut(3);
    let winner = new_primary(&nodes).await;
    let took = cut.elapsed();
    let grace = timing().primary_grace;
    assert!(took >= grace, "took over after {took:?}");
    assert!(took < grace + ms(1500), "took over after {took:?}");
    let register = register(&store).await;
    let primary = &nodes[winner].shard;
    assert_eq!(register, primary.config());
    assert_eq!(register.epoch, Epoch::new(2));
    assert_eq!(register.primary, node(winner as u8 + 1));
    // The old primary is no longer a member.
    assert_eq!(register.members.len(), 2);
    assert!(!register.is_member(&node(1)));

    // The new primary serves what the old one committed, and writes with
    // the other member.
    until(|| primary.leader().unwrap().holds_leases()).await;
    assert!(primary.entry("a").await.unwrap().is_some());
    primary.commit(put("b", 20)).await.unwrap();
    let loser = &nodes[if winner == 1 { 2 } else { 1 }].shard;
    until(|| loser.config() == register && *loser.durable().borrow() >= primary.last_sequenced())
        .await;
    assert_eq!(loser.role(), Role::Member);
    // The old primary serves no read: its leases lapsed with the grace.
    assert!(old.check_readable().is_err());

    // Once it reaches the members again, they refuse it for the newer
    // epoch, and it learns from the register that it is deposed: it then
    // redirects to the new primary.
    nodes[0].heal(2);
    nodes[0].heal(3);
    until(|| old.is_deposed()).await;
    match old.check_readable() {
        Err(ShardError::NotPrimary { primary, epoch, .. }) => {
            assert_eq!((primary, epoch), (register.primary.clone(), register.epoch));
        }
        other => panic!("the deposed primary answered {other:?}"),
    }
    assert_eq!(old.config(), register);
    assert!(matches!(
        old.commit(put("c", 1)).await,
        Err(ShardError::NotPrimary { .. })
    ));
    assert!(nodes[0].replication.exposure().await.shards == 0);
}

#[test]
fn a_new_primary_truncates_what_a_member_holds_beyond_its_log() {
    run(truncation());
}

async fn truncation() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    // Node 3 waits so long before proposing that node 2 takes over.
    let nodes = cluster(&pki, 3, &initial(), &store, |n| ReplicationConfig {
        takeover_delay: if n == 3 { ms(20_000) } else { ms(40) },
        ..patient(n)
    })
    .await;
    let old = &nodes[0].shard;
    until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
    old.commit(put("a", 10)).await.unwrap();

    // Node 2 stops receiving: a write reaches node 3 alone, and does not
    // commit. Then node 3 is cut off too.
    nodes[0].cut(2);
    let before = old.last_sequenced();
    let pending = old.clone();
    let write = tokio::spawn(async move { pending.commit(put("x", 30)).await });
    until(|| old.last_sequenced() > before).await;
    let member3 = &nodes[2].shard;
    let held = old.last_sequenced();
    until(|| *member3.durable().borrow() >= held).await;
    nodes[0].cut(3);

    let winner = new_primary(&nodes).await;
    assert_eq!(winner, 1, "node 2 took over");
    let primary = &nodes[1].shard;
    // Node 3 truncated the write the new primary lacks, then took the new
    // primary's records in its place.
    primary.commit(put("y", 40)).await.unwrap();
    let last = primary.last_sequenced();
    until(|| *member3.durable().borrow() >= last && member3.applied().seq >= last).await;
    assert!(primary.entry("x").await.unwrap().is_none());
    assert!(primary.entry("y").await.unwrap().is_some());
    let index = member3.index().read().unwrap();
    assert!(index.entry(member3.shard(), "x").unwrap().is_none());
    assert!(index.entry(member3.shard(), "y").unwrap().is_some());
    // The write the old primary still waits on never commits.
    nodes[0].heal(2);
    nodes[0].heal(3);
    let refused = write.await.unwrap();
    assert!(refused.is_err(), "{refused:?}");
    // Its replica of node 3 reads its log without the truncated record.
    let before = skys3_types::Seq::new(held.get() - 1);
    let tail = member3.read_tail(before, last).await.unwrap();
    assert!(
        tail.iter()
            .all(|(position, _)| position.epoch == Epoch::new(2)),
        "{tail:?}"
    );
}

#[test]
fn a_single_survivor_takes_over() {
    run(single_survivor());
}

async fn single_survivor() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let pair = ShardConfig {
        members: vec![node(1), node(2)],
        min_write_replicas: 1,
        ..initial()
    };
    let nodes = cluster(&pki, 2, &pair, &store, |_| timing()).await;
    let old = &nodes[0].shard;
    until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
    old.commit(put("a", 10)).await.unwrap();
    // The primary stops for good.
    old.close().await.unwrap();
    let winner = new_primary(&nodes).await;
    assert_eq!(winner, 1);
    let survivor = &nodes[1].shard;
    assert_eq!(survivor.config().members, [node(2)]);
    assert!(survivor.entry("a").await.unwrap().is_some());
    survivor.commit(put("b", 10)).await.unwrap();
}

#[test]
fn a_member_removed_while_up_stops_once_its_grace_passes() {
    run(removed_while_up());
}

/// Node 3 is removed while it is up, as a coordinator's move removes a
/// member, and is not told: the primary stops sending to it, its grace
/// passes, and its takeover finds the register without it. Its replica
/// then stops in that configuration rather than stay open as a member of
/// the old one.
async fn removed_while_up() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let nodes = cluster(&pki, 3, &initial(), &store, patient).await;
    let primary = &nodes[0].shard;
    until(|| primary.is_serving() && primary.leader().unwrap().holds_leases()).await;
    primary.commit(put("a", 10)).await.unwrap();

    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    let removed = ShardConfig {
        epoch: Epoch::new(2),
        members: vec![node(1), node(2)],
        proposal_id: ProposalId::new("remove-3").unwrap(),
        ..initial()
    };
    assert_eq!(
        registers.replace(&initial(), &removed).await.unwrap(),
        Replaced::Accepted
    );
    nodes[0].replication.open(&removed).await.unwrap();
    primary.commit(put("b", 10)).await.unwrap();
    let stale = &nodes[2].shard;
    until(|| stale.is_stopped()).await;
    assert_eq!(stale.config(), removed);
    assert_eq!(register(&store).await, removed);
    // The others go on.
    primary.commit(put("c", 10)).await.unwrap();
    assert_eq!(nodes[1].shard.role(), Role::Member);
}

#[test]
fn a_member_readmitted_and_removed_again_stops_once_its_grace_passes() {
    run(readmitted_and_removed_again());
}

/// Node 3 is removed and added back as a learner in one change while it
/// is up, as a coordinator's rebalancing does, before its grace passes:
/// its replica stops and opens again as a learner, which the primary
/// promotes. Removed again while up, the new replica stops once its grace
/// passes, as the first would have: the watch over the primary follows
/// the replica the node holds now, not the one it watched before, whose
/// grace the new replica kept renewing (found by the kill, partition, and
/// peering suite, plan M8-08).
async fn readmitted_and_removed_again() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    // Node 3's grace outlasts its readmission by far.
    let grace = ms(2000);
    let timing = |n: u8| {
        let timing = patient(n);
        if n == 3 {
            ReplicationConfig {
                primary_grace: grace,
                ..timing
            }
        } else {
            timing
        }
    };
    let nodes = cluster(&pki, 3, &initial(), &store, timing).await;
    let primary = &nodes[0].shard;
    until(|| primary.is_serving() && primary.leader().unwrap().holds_leases()).await;
    primary.commit(put("a", 10)).await.unwrap();

    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    let readmitted = ShardConfig {
        epoch: Epoch::new(2),
        members: vec![node(1), node(2)],
        learners: vec![node(3)],
        proposal_id: ProposalId::new("readmit-3").unwrap(),
        ..initial()
    };
    assert_eq!(
        registers.replace(&initial(), &readmitted).await.unwrap(),
        Replaced::Accepted
    );
    let learner = nodes[2].replication.open(&readmitted).await.unwrap();
    assert_eq!(learner.role(), Role::Learner);
    assert!(nodes[2].shard.is_stopped());
    nodes[0].replication.open(&readmitted).await.unwrap();
    // The primary promotes the learner.
    until(|| learner.role() == Role::Member && learner.config().members.len() == 3).await;
    let promoted = learner.config();
    assert_eq!(register(&store).await, promoted);

    let removed = ShardConfig {
        epoch: promoted.epoch.checked_next().unwrap(),
        members: vec![node(1), node(2)],
        proposal_id: ProposalId::new("remove-3").unwrap(),
        ..promoted.clone()
    };
    assert_eq!(
        registers.replace(&promoted, &removed).await.unwrap(),
        Replaced::Accepted
    );
    nodes[0].replication.open(&removed).await.unwrap();
    primary.commit(put("b", 10)).await.unwrap();
    let started = Instant::now();
    until(|| learner.is_stopped()).await;
    assert!(started.elapsed() < grace * 3, "{:?}", started.elapsed());
    assert_eq!(learner.config(), removed);
    primary.commit(put("c", 10)).await.unwrap();
}
