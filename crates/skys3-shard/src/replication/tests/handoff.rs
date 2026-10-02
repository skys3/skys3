//! Planned handoff over real TCP (§5.4): the primary steps down to a
//! member, which takes over without waiting for its grace, or, when the
//! step-down message is lost, once its grace passes.

use std::num::NonZeroUsize;
use std::sync::Arc;

use skys3_control::{MemoryControlStore, RetryPolicy};
use skys3_io::BlockingPool;
use skys3_log::ShardRef;
use skys3_types::{Epoch, ShardConfig, ShardId};
use tokio::time::Instant;

use super::removal::{initial, put, register, timing, until};
use super::takeover::{Node, cluster, new_primary, patient, run};
use super::{Pki, node, shard};
use crate::error::ShardError;
use crate::replication::{ControlRegisters, Replaced, ShardRegisters};
use crate::shard::{Role, Shard};

/// Three nodes in the initial configuration, node 1 serving as primary
/// with every lease, and key `a` committed.
async fn serving(pki: &Pki, store: &MemoryControlStore) -> Vec<Node> {
    let nodes = cluster(pki, 3, &initial(), store, patient).await;
    let old = &nodes[0].shard;
    until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
    old.commit(put("a", 10)).await.unwrap();
    nodes
}

#[test]
fn a_handoff_does_not_wait_for_the_grace() {
    run(handoff());
}

async fn handoff() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let nodes = serving(&pki, &store).await;
    let old = &nodes[0].shard;

    // A write in flight when the primary steps down still commits.
    let before = old.last_sequenced();
    let writing = old.clone();
    let write = tokio::spawn(async move { writing.commit(put("b", 20)).await });
    until(|| old.last_sequenced() > before).await;
    let started = Instant::now();
    let handed = nodes[0]
        .replication
        .hand_off(&shard(), &node(2))
        .await
        .unwrap();
    assert!(handed.sent);
    assert_eq!(handed.epoch, Epoch::new(1));
    assert_eq!(handed.last, old.last_sequenced());
    assert!(write.await.unwrap().is_ok());

    let winner = new_primary(&nodes).await;
    let took = started.elapsed();
    assert_eq!(winner, 1, "the candidate took over");
    assert!(took < timing().primary_grace, "took over after {took:?}");
    let register = register(&store).await;
    assert_eq!(register.epoch, Epoch::new(2));
    assert_eq!(register.primary, node(2));
    assert_eq!(register.members, [node(2), node(3)]);
    let primary = &nodes[1].shard;
    until(|| primary.leader().unwrap().holds_leases()).await;
    assert!(primary.entry("a").await.unwrap().is_some());
    assert!(primary.entry("b").await.unwrap().is_some());
    primary.commit(put("c", 30)).await.unwrap();

    // The old primary stepped down durably, serves nothing, and redirects
    // to the new primary once it reads the register.
    assert_eq!(old.stepped_down(), Some(Epoch::new(1)));
    let index = old.index().read().unwrap();
    assert_eq!(index.step_down(old.shard()).unwrap(), Some(Epoch::new(1)));
    until(|| old.config() == register).await;
    match old.check_readable() {
        Err(ShardError::NotPrimary { primary, epoch, .. }) => {
            assert_eq!((primary, epoch), (node(2), Epoch::new(2)));
        }
        other => panic!("the old primary answered {other:?}"),
    }
    let again = nodes[0].replication.hand_off(&shard(), &node(3)).await;
    assert!(
        matches!(again, Err(ShardError::Unavailable { .. })),
        "{again:?}"
    );
}

#[test]
fn a_lost_step_down_falls_back_to_the_grace() {
    run(lost_step_down());
}

async fn lost_step_down() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let nodes = serving(&pki, &store).await;
    let old = &nodes[0].shard;

    // The candidate cannot be reached: the message never arrives, whether
    // or not the primary wrote it before it saw the link drop, and the
    // members take over once their grace passes.
    nodes[0].cut(2);
    let started = Instant::now();
    nodes[0]
        .replication
        .hand_off(&shard(), &node(2))
        .await
        .unwrap();
    assert!(old.check_readable().is_err());
    new_primary(&nodes).await;
    let took = started.elapsed();
    assert!(took >= timing().primary_grace, "took over after {took:?}");
    let candidate = nodes[1].replication.grace(&shard()).unwrap();
    assert_eq!(candidate.stepped_down(), None);
    let register = register(&store).await;
    assert!(!register.is_member(&node(1)));
    until(|| old.config() == register).await;
    assert!(matches!(
        old.check_readable(),
        Err(ShardError::NotPrimary { .. })
    ));
}

#[test]
fn a_primary_that_stepped_down_never_serves_in_that_epoch_again() {
    run(restart_after_step_down());
}

async fn restart_after_step_down() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let nodes = serving(&pki, &store).await;
    let old = &nodes[0].shard;

    // Handing off needs a primary, and another member to hand off to.
    let replication = &nodes[0].replication;
    let refused = replication.hand_off(&shard(), &node(1)).await;
    assert!(
        matches!(refused, Err(ShardError::Configuration { .. })),
        "{refused:?}"
    );
    let refused = replication.hand_off(&shard(), &node(9)).await;
    assert!(
        matches!(refused, Err(ShardError::Configuration { .. })),
        "{refused:?}"
    );
    let refused = nodes[1].replication.hand_off(&shard(), &node(3)).await;
    assert!(
        matches!(refused, Err(ShardError::Configuration { .. })),
        "{refused:?}"
    );
    let other = ShardRef::new(shard().bucket, ShardId::new(5));
    let refused = replication.hand_off(&other, &node(2)).await;
    assert!(
        matches!(refused, Err(ShardError::NotFound(_))),
        "{refused:?}"
    );
    // Nor while the register holds another configuration than the
    // primary's, as after a removal whose answer was lost.
    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    let moved = ShardConfig {
        epoch: Epoch::new(2),
        ..initial()
    };
    let replaced = registers.replace(&initial(), &moved).await.unwrap();
    assert_eq!(replaced, Replaced::Accepted);
    let refused = replication.hand_off(&shard(), &node(2)).await;
    assert!(
        matches!(refused, Err(ShardError::Unavailable { .. })),
        "{refused:?}"
    );
    let replaced = registers.replace(&moved, &initial()).await.unwrap();
    assert_eq!(replaced, Replaced::Accepted);
    assert!(old.is_serving());

    replication.hand_off(&shard(), &node(3)).await.unwrap();
    new_primary(&nodes).await;

    // The node restarts while its register still names it, as if the
    // candidate's proposal had not landed yet: its replica opens stopped,
    // and serves nothing in the epoch it stepped down in.
    let (_, log) = replication.set().logs().next().unwrap();
    let pool = BlockingPool::new("restarted", NonZeroUsize::MIN).unwrap();
    let reopened = Shard::open_replica(
        &initial(),
        &node(1),
        log.clone(),
        Arc::clone(old.index()),
        pool,
    )
    .await
    .unwrap();
    assert_eq!(reopened.role(), Role::Primary);
    assert_eq!(reopened.stepped_down(), Some(Epoch::new(1)));
    assert!(reopened.is_stopped());
    assert!(!reopened.is_serving());
    assert!(matches!(
        reopened.check_readable(),
        Err(ShardError::Unavailable { .. })
    ));
    assert!(reopened.commit(put("z", 1)).await.is_err());
    // A later epoch in which it leads again is not affected.
    let later = ShardConfig {
        epoch: Epoch::new(7),
        ..initial()
    };
    let (_, log) = replication.set().logs().next().unwrap();
    let pool = BlockingPool::new("later", NonZeroUsize::MIN).unwrap();
    let later = Shard::open_replica(&later, &node(1), log.clone(), Arc::clone(old.index()), pool)
        .await
        .unwrap();
    assert_eq!(later.stepped_down(), None);
    assert!(!later.is_stopped());
}
