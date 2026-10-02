//! Resuming replicas after a restart (§6.2): from the register, or from
//! the configuration of the latest `CONFIG` record while the register
//! cannot be read, and the takeover a member recorded before it went down
//! (§6.3).

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use skys3_control::{ControlError, ProposalIds};
use skys3_io::{BlockingPool, SimMount};
use skys3_log::ShardRef;
use skys3_net::TokioNetwork;
use skys3_types::{Epoch, ProposalId, ShardConfig};

use super::removal::{timing, until};
use super::{Pki, WAIT, config, connect, node, refusal, shard, shard_set, sync};
use crate::replication::{BoxFuture, Replaced, Replication, ShardRegisters};
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

/// A shard register the test sets: `None` while it cannot be read.
#[derive(Clone, Default)]
struct Register(Arc<Mutex<Option<Option<ShardConfig>>>>);

impl Register {
    fn holding(config: Option<ShardConfig>) -> Self {
        let register = Self::default();
        register.set(Some(config));
        register
    }

    fn set(&self, held: Option<Option<ShardConfig>>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = held;
    }

    fn get(&self) -> Result<Option<ShardConfig>, ControlError> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| ControlError::Unavailable("the register is unreachable".to_owned()))
    }
}

impl ShardRegisters for Register {
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>> {
        Box::pin(async move {
            let held = self.get()?;
            if held.as_ref() == Some(next) {
                return Ok(Replaced::Accepted);
            }
            if held.as_ref() != Some(current) {
                return Ok(Replaced::Holds(held));
            }
            self.set(Some(Some(next.clone())));
            Ok(Replaced::Accepted)
        })
    }

    fn read<'a>(
        &'a self,
        _: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        Box::pin(async move { self.get() })
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn run(test: impl Future<Output = ()>) {
    runtime().block_on(async {
        tokio::time::timeout(WAIT, test)
            .await
            .expect("the test finished in time");
    });
}

/// Node `n`'s replication over `set`, without peers, with the shard
/// registers `register`.
fn replication(
    n: u8,
    pki: &Pki,
    set: ShardSet<SimMount>,
    register: &Register,
) -> Replication<TokioNetwork, SimMount> {
    let clock = Arc::new(skys3_io::MonotonicClock::new());
    Replication::new(
        node(n),
        set,
        pki.transport(&node(n)),
        BTreeMap::new(),
        clock,
        timing(),
    )
    .with_removal(register.clone(), ProposalIds::seeded(u64::from(n)))
    .with_takeover()
}

/// The shard set of a node that restarted without losing its disk: the
/// index and log of `replica`, which stops first, as its process did.
async fn restarted(replica: &Shard<SimMount>, set: &ShardSet<SimMount>) -> ShardSet<SimMount> {
    replica.abandon("the node restarts").await;
    let (_, log) = set.logs().next().unwrap();
    let pool = BlockingPool::new("restarted", NonZeroUsize::MIN).unwrap();
    ShardSet::new(Arc::clone(replica.index()), log.clone(), pool)
}

/// `config()` in `epoch`, with `primary` and `members`.
fn configured(epoch: u64, primary: u8, members: &[u8]) -> ShardConfig {
    ShardConfig {
        epoch: Epoch::new(epoch),
        primary: node(primary),
        members: members.iter().map(|n| node(*n)).collect(),
        proposal_id: ProposalId::new(format!("p-{epoch}-{primary}")).unwrap(),
        ..config()
    }
}

#[test]
fn a_replica_resumes_from_its_kept_configuration_while_its_register_cannot_be_read() {
    run(async {
        let pki = Pki::new();
        let (set, _) = shard_set(61).await;
        let register = Register::holding(Some(config()));
        let first = replication(1, &pki, set.clone(), &register);
        let replica = first.open(&config()).await.unwrap();
        assert_eq!(set.kept_config(&shard()).await.unwrap(), Some(config()));

        // The node restarts while the control store is unreachable.
        register.set(None);
        let set = restarted(&replica, &set).await;
        assert_eq!(
            set.kept_configs().await.unwrap(),
            BTreeMap::from([(shard(), config())])
        );
        let resumed = replication(1, &pki, set.clone(), &register);
        let all = resumed.resume_kept().await.unwrap();
        assert_eq!(all.len(), 1);
        let replica = all[0].1.clone().unwrap().expect("the replica resumed");
        assert_eq!(replica.config(), config());
        assert_eq!(replica.role(), Role::Primary);
        // Resuming an open replica returns it.
        let again = resumed.resume(&shard()).await.unwrap().unwrap();
        assert!(again.durable().same_channel(&replica.durable()));

        // The register answers again: it moved on without this node while
        // the node was down, so the replica stops and redirects.
        let moved = configured(3, 2, &[2]);
        register.set(Some(Some(moved.clone())));
        until(|| replica.is_deposed()).await;
        assert!(replica.is_stopped());
        assert_eq!(replica.config(), moved);
    });
}

#[test]
fn a_replica_resumes_in_the_newer_of_its_register_and_its_kept_configuration() {
    run(async {
        let pki = Pki::new();
        let (set, _) = shard_set(62).await;
        let register = Register::holding(Some(config()));
        let replica = replication(1, &pki, set.clone(), &register)
            .open(&config())
            .await
            .unwrap();
        let mut set = restarted(&replica, &set).await;
        let cases = [
            // The register moved on and still names the node.
            (Some(configured(3, 1, &[1, 2])), Some(Epoch::new(3))),
            // A stale read of the register: the kept one is newer.
            (Some(configured(1, 1, &[1, 2])), Some(Epoch::new(2))),
            // The register no longer names the node, or is gone.
            (Some(configured(3, 2, &[2])), None),
            (None, None),
        ];
        for (held, opened) in cases {
            register.set(Some(held));
            let replication = replication(1, &pki, set.clone(), &register);
            let resumed = replication.resume(&shard()).await.unwrap();
            assert_eq!(resumed.as_ref().map(|r| r.config().epoch), opened);
            if let Some(replica) = resumed {
                set = restarted(&replica, &set).await;
            }
        }
        // A shard the node keeps nothing of, whose register cannot be read,
        // is not opened.
        register.set(None);
        let other = ShardRef::new(shard().bucket, skys3_types::ShardId::new(1));
        let replication = replication(1, &pki, set.clone(), &register);
        assert!(replication.resume(&other).await.unwrap().is_none());
    });
}

#[test]
fn a_resumed_member_that_was_re_admitted_as_a_learner_reopens_as_one() {
    run(async {
        let pki = Pki::new();
        let (set, _) = shard_set(65).await;
        let register = Register::holding(Some(config()));
        let member = replication(2, &pki, set.clone(), &register)
            .open(&config())
            .await
            .unwrap();
        register.set(None);
        let set = restarted(&member, &set).await;
        let resumed = replication(2, &pki, set.clone(), &register);
        let replica = resumed.resume(&shard()).await.unwrap().unwrap();
        assert_eq!(replica.role(), Role::Member);

        // While the node was down, the primary removed it and added it back
        // as a learner (§6.7): the stale member stops, and the shard opens
        // again as a learner.
        let readmitted = ShardConfig {
            learners: vec![node(2)],
            ..configured(4, 1, &[1])
        };
        register.set(Some(Some(readmitted.clone())));
        until(|| replica.is_stopped()).await;
        let learner = loop {
            match set.get(&shard()).await {
                Some(open) if open.role() == Role::Learner => break open,
                _ => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        };
        assert_eq!(learner.config(), readmitted);
        assert!(!learner.durable().same_channel(&replica.durable()));
    });
}

#[test]
fn a_recorded_takeover_survives_a_restart_and_is_sent_again() {
    run(async {
        let pki = Pki::new();
        let (set, _) = shard_set(63).await;
        let register = Register::holding(Some(config()));
        let member = replication(2, &pki, set.clone(), &register)
            .open(&config())
            .await
            .unwrap();
        assert_eq!(member.role(), Role::Member);
        // The member proposed itself, the compare-and-swap landed, and the
        // node went down before it learned so.
        let proposed = configured(3, 2, &[2]);
        member.record_takeover(&proposed).await.unwrap();
        assert_eq!(member.outstanding_takeover(), Some(proposed.clone()));
        register.set(None);
        let set = restarted(&member, &set).await;

        // It resumes from its kept configuration, follows no primary of it,
        // and grants nothing.
        let transport = pki.transport(&node(2));
        let resumed = Replication::new(
            node(2),
            set,
            transport.clone(),
            BTreeMap::new(),
            Arc::new(skys3_io::MonotonicClock::new()),
            timing(),
        )
        .with_removal(register.clone(), ProposalIds::seeded(2))
        .with_takeover();
        let replica = resumed.resume(&shard()).await.unwrap().unwrap();
        assert_eq!(replica.config(), config());
        assert_eq!(replica.outstanding_takeover(), Some(proposed.clone()));
        let listener = transport
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let serving = resumed.clone();
        tokio::spawn(async move { serving.serve(listener).await });
        let mut link = connect(&pki, 1, address).await;
        let answers = sync(&mut link, &config(), 0).await;
        assert!(refusal(&answers).contains("proposed itself"), "{answers:?}");
        let grace = resumed.grace(&shard()).unwrap();
        assert!(grace.is_stopped());

        // Once the register answers, holding the proposal, the member takes
        // over.
        register.set(Some(Some(proposed.clone())));
        until(|| replica.role() == Role::Primary).await;
        assert_eq!(replica.config(), proposed);
        assert_eq!(replica.outstanding_takeover(), None);
    });
}

#[test]
fn a_recorded_takeover_that_lost_is_forgotten() {
    run(async {
        let pki = Pki::new();
        let (set, _) = shard_set(64).await;
        let register = Register::holding(Some(config()));
        let member = replication(2, &pki, set.clone(), &register)
            .open(&config())
            .await
            .unwrap();
        member
            .record_takeover(&configured(3, 2, &[2]))
            .await
            .unwrap();
        // While the node was down, the primary removed it.
        register.set(Some(Some(configured(3, 1, &[1]))));
        let set = restarted(&member, &set).await;
        let resumed = replication(2, &pki, set, &register);
        // The register no longer names the node, so nothing opens, and the
        // record is settled by the next open in a newer configuration.
        assert!(resumed.resume(&shard()).await.unwrap().is_none());
        let read = member.index().read().unwrap().takeover(&shard()).unwrap();
        assert!(read.is_some());

        // A member that opens in the epoch it proposed over and finds the
        // register moved on forgets the proposal.
        register.set(None);
        let replica = resumed.open(&config()).await.unwrap();
        assert!(replica.outstanding_takeover().is_some());
        register.set(Some(Some(configured(3, 1, &[1, 3]))));
        until(|| replica.outstanding_takeover().is_none()).await;
        until(|| {
            replica
                .index()
                .read()
                .unwrap()
                .takeover(&shard())
                .unwrap()
                .is_none()
        })
        .await;
    });
}
