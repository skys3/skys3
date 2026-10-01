use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{Fault, FaultyStore};
use skys3_control::{
    Expected, MemoryControlStore, PutOutcome, bootstrap, propose_document, read, read_cluster,
};
use skys3_io::MonotonicClock;
use skys3_types::{BucketMode, BucketName, DiskInfo, Label, NodeId, ShardCount};

use super::*;
use crate::change::Write;
use crate::join::{NodeProfile, register};
use crate::registry::RegistryConfig;

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

fn node(n: usize) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

/// A `local` bucket named `bucket-<b>` with ID `b-<b>`.
fn bucket(b: usize, shards: u32, replicas: u8) -> BucketDocument {
    BucketDocument {
        bucket_id: BucketId::new(format!("b-{b}")).unwrap(),
        name: BucketName::new(format!("bucket-{b}")).unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(shards).unwrap(),
        replicas,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        proposal_id: ProposalId::new(format!("p-b{b}")).unwrap(),
    }
}

/// A store with nodes 0 to `nodes − 1` registered.
async fn store(nodes: usize) -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(7);
    bootstrap(&store, &cluster(), ids.next_id(), &retry())
        .await
        .unwrap();
    for n in 0..nodes {
        let profile = NodeProfile {
            node: node(n),
            address: format!("10.0.0.{n}:7400").parse().unwrap(),
            zone: None,
            rack: None,
            disks: vec![DiskInfo {
                disk_id: Label::new("nvme0").unwrap(),
                capacity_bytes: 1 << 40,
            }],
        };
        register(&store, &cluster(), &profile, &mut ids, &retry())
            .await
            .unwrap();
    }
    store
}

async fn shard_register(
    store: &impl ControlStore,
    bucket: &BucketDocument,
    shard: u8,
) -> Option<ShardConfig> {
    let key = TypedKey::shard(&bucket.bucket_id, ShardId::new(shard));
    read(store, &key)
        .await
        .unwrap()
        .map(|current| current.value)
}

async fn generation(store: &impl ControlStore) -> u64 {
    read_cluster(store, &cluster(), &retry())
        .await
        .unwrap()
        .value
        .generation
        .get()
}

async fn create(
    store: &impl ControlStore,
    bucket: &BucketDocument,
) -> Result<Creation, CreationError> {
    let mut ids = ProposalIds::seeded(11);
    create_bucket(
        store,
        &cluster(),
        bucket,
        FailureDomain::Node,
        &mut ids,
        &retry(),
    )
    .await
}

#[tokio::test]
async fn a_bucket_is_created_with_placed_shard_registers_in_one_announced_change() {
    let store = store(4).await;
    let before = generation(&store).await;
    let new = bucket(1, 8, 3);
    assert_eq!(
        create(&store, &new).await.unwrap(),
        Creation::Created { complete: true }
    );
    assert_eq!(generation(&store).await, before + 1);
    let found = read(&store, &TypedKey::bucket(&new.name))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.value, new);
    let mut members = BTreeMap::<NodeId, usize>::new();
    for shard in 0..8 {
        let config = shard_register(&store, &new, shard).await.unwrap();
        assert_eq!(config.epoch, Epoch::new(1));
        assert_eq!(config.members.len(), 3);
        assert!(config.is_member(&config.primary));
        assert_eq!((config.replicas, config.min_write_replicas), (3, 1));
        assert!(config.learners.is_empty());
        for member in config.members {
            *members.entry(member).or_default() += 1;
        }
    }
    // 24 members spread evenly over four nodes.
    assert_eq!(members.values().copied().collect::<Vec<_>>(), [6, 6, 6, 6]);

    // The name is taken now, and a second creation writes nothing.
    let mut again = bucket(2, 8, 3);
    again.name = new.name.clone();
    assert_eq!(create(&store, &again).await.unwrap(), Creation::Taken);
    assert_eq!(generation(&store).await, before + 1);
    assert!(shard_register(&store, &again, 0).await.is_none());
}

#[tokio::test]
async fn a_policy_the_registered_nodes_cannot_satisfy_is_rejected() {
    let store = store(2).await;
    let new = bucket(1, 2, 3);
    let error = create(&store, &new).await.unwrap_err();
    let CreationError::Unsatisfiable(unsatisfiable) = error else {
        panic!("{error:?}");
    };
    assert_eq!((unsatisfiable.replicas, unsatisfiable.domains), (3, 2));
    assert!(
        read(&store, &TypedKey::bucket(&new.name))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_shard_register_another_writer_created_is_kept() {
    let store = store(3).await;
    let new = bucket(1, 3, 2);
    // The coordinator finished shard 1 first, on nodes of its choosing.
    let mut theirs = first_config(
        &new,
        NewShard {
            shard: ShardId::new(1),
            primary: node(2),
            members: vec![node(2), node(0)],
        },
        ProposalId::new("p-theirs").unwrap(),
    );
    theirs.min_write_replicas = 2;
    let key = TypedKey::shard(&new.bucket_id, ShardId::new(1));
    let _ = propose_document(&store, &key, Expected::Absent, &theirs, &retry())
        .await
        .unwrap();
    assert_eq!(
        create(&store, &new).await.unwrap(),
        Creation::Created { complete: true }
    );
    assert_eq!(shard_register(&store, &new, 1).await.unwrap(), theirs);
    assert!(shard_register(&store, &new, 0).await.is_some());
    assert!(shard_register(&store, &new, 2).await.is_some());

    // The last register taken too: nothing is left to send.
    let other = bucket(2, 1, 2);
    let mut theirs = theirs.clone();
    theirs.bucket_id = other.bucket_id.clone();
    theirs.shard = ShardId::new(0);
    let key = TypedKey::shard(&other.bucket_id, ShardId::new(0));
    let _ = propose_document(&store, &key, Expected::Absent, &theirs, &retry())
        .await
        .unwrap();
    assert_eq!(
        create(&store, &other).await.unwrap(),
        Creation::Created { complete: true }
    );
}

#[tokio::test]
async fn a_failed_shard_write_leaves_the_creation_to_the_coordinator() {
    let faulty = FaultyStore::new(store(3).await);
    let new = bucket(1, 2, 2);
    // The listing and the three reads of the registrations pass, the
    // bucket register lands, and the first shard register's write fails
    // without an answer: settling sends it again, and it lands, but the
    // second is never sent.
    let mut script = vec![Fault::Pass; 5];
    script.extend([Fault::Fail]);
    faulty.script(script);
    assert_eq!(
        create(&faulty, &new).await.unwrap(),
        Creation::Created { complete: false }
    );
    assert!(
        read(faulty.inner(), &TypedKey::bucket(&new.name))
            .await
            .unwrap()
            .is_some()
    );
    assert!(shard_register(faulty.inner(), &new, 0).await.is_some());
    assert!(shard_register(faulty.inner(), &new, 1).await.is_none());
}

#[tokio::test]
async fn a_bucket_register_without_an_answer_is_settled() {
    let faulty = FaultyStore::new(store(3).await);
    let new = bucket(1, 1, 2);
    // The bucket register's answer is lost on every attempt, and then
    // settled: it landed.
    let mut script = vec![Fault::Pass; 4];
    script.extend(std::iter::repeat_n(Fault::LoseResponse, 3));
    faulty.script(script);
    let before = generation(faulty.inner()).await;
    assert_eq!(
        create(&faulty, &new).await.unwrap(),
        Creation::Created { complete: false }
    );
    assert!(
        read(faulty.inner(), &TypedKey::bucket(&new.name))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        generation(faulty.inner()).await > before,
        "the bucket was not announced"
    );

    // A store that answers nothing creates nothing it knows of.
    let faulty = FaultyStore::new(store(3).await);
    faulty.script(
        std::iter::repeat_n(Fault::Pass, 4).chain(std::iter::repeat_n(Fault::Unavailable, 20)),
    );
    let error = create(&faulty, &bucket(2, 1, 2)).await.unwrap_err();
    assert!(matches!(error, CreationError::Control(_)), "{error:?}");
}

fn registry() -> NodeRegistry {
    NodeRegistry::new(
        Arc::new(MonotonicClock::new()),
        RegistryConfig::new(Duration::from_secs(60)),
    )
}

/// A coordinator round: refresh the registry, then plan.
async fn round(
    shards: &mut BucketShards<NoPlacement>,
    store: &MemoryControlStore,
) -> Option<ChangeSet> {
    shards.registry.refresh(store).await.unwrap();
    let mut ids = ProposalIds::seeded(5);
    shards.plan(store, &mut ids).await.unwrap()
}

async fn make(store: &MemoryControlStore, change: &ChangeSet) -> Applied {
    let mut ids = ProposalIds::seeded(6);
    let applied = apply(store, &cluster(), change, &mut ids, &retry())
        .await
        .unwrap();
    assert!(applied.is_complete(), "{applied:?}");
    applied
}

#[tokio::test]
async fn the_coordinator_completes_a_creation_cut_short_after_two_scans() {
    let store = store(3).await;
    let mut shards = BucketShards::new(registry(), FailureDomain::Node);
    assert!(format!("{shards:?}").contains("BucketShards"));
    assert!(round(&mut shards, &store).await.is_none());

    // A creation wrote its bucket register and one shard register.
    let new = bucket(1, 3, 2);
    let mut ids = ProposalIds::seeded(1);
    let first = first_config(
        &new,
        NewShard {
            shard: ShardId::new(0),
            primary: node(0),
            members: vec![node(0), node(1)],
        },
        ids.next_id(),
    );
    let change = ChangeSet::new()
        .create(&TypedKey::bucket(&new.name), &new)
        .unwrap()
        .create(&TypedKey::shard(&new.bucket_id, ShardId::new(0)), &first)
        .unwrap();
    let _ = make(&store, &change).await;

    // The first scan only notes it; the second finishes it.
    assert!(round(&mut shards, &store).await.is_none());
    let change = round(&mut shards, &store).await.unwrap();
    assert_eq!(change.writes().len(), 2);
    assert!(
        change
            .writes()
            .iter()
            .all(|write| write.expected() == Expected::Absent)
    );
    let applied = make(&store, &change).await;
    shards.applied(&change, &applied);
    for shard in 1..3 {
        let config = shard_register(&store, &new, shard).await.unwrap();
        assert_eq!(config.members.len(), 2);
        assert!(config.is_member(&config.primary));
    }
    assert!(round(&mut shards, &store).await.is_none());
    assert!(round(&mut shards, &store).await.is_none());
}

#[tokio::test]
async fn the_coordinator_deletes_the_shard_registers_of_deleted_buckets() {
    let store = store(2).await;
    let mut shards = BucketShards::new(registry(), FailureDomain::Node);
    let gone = bucket(1, 2, 2);
    assert_eq!(
        create(&store, &gone).await.unwrap(),
        Creation::Created { complete: true }
    );
    assert!(round(&mut shards, &store).await.is_none());

    // The bucket is deleted; its shard registers are left behind.
    let current = read(&store, &TypedKey::bucket(&gone.name))
        .await
        .unwrap()
        .unwrap();
    let _ = make(
        &store,
        &ChangeSet::new()
            .delete(&TypedKey::bucket(&gone.name), &current.version)
            .unwrap(),
    )
    .await;
    assert!(round(&mut shards, &store).await.is_none());

    // A register that does not parse may be the bucket's: nothing goes.
    let unreadable = RegisterKey::new("buckets/garbage.json").unwrap();
    let written = store
        .put_if(&unreadable, Expected::Absent, Bytes::from_static(b"{"))
        .await
        .unwrap();
    let PutOutcome::Written(version) = written else {
        panic!("{written:?}");
    };
    assert!(round(&mut shards, &store).await.is_none());
    let _ = store.delete_if(&unreadable, &version).await.unwrap();

    let change = round(&mut shards, &store).await.unwrap();
    assert_eq!(change.writes().len(), 2);
    assert!(change.writes().iter().all(Write::is_delete));
    let _ = make(&store, &change).await;
    assert!(shard_register(&store, &gone, 0).await.is_none());
    assert!(round(&mut shards, &store).await.is_none());
}

/// A placement that always plans the same change, to show when the
/// wrapped placement plans.
struct Fixed(ChangeSet, usize);

impl Placement for Fixed {
    async fn plan<S: ControlStore>(
        &mut self,
        _store: &S,
        _proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        Ok(Some(self.0.clone()))
    }

    fn applied(&mut self, _change: &ChangeSet, _applied: &Applied) {
        self.1 += 1;
    }
}

#[tokio::test]
async fn the_wrapped_placement_plans_when_nothing_is_missing() {
    let store = store(2).await;
    let theirs = ChangeSet::new()
        .create(&TypedKey::bucket(&bucket(9, 1, 1).name), &bucket(9, 1, 1))
        .unwrap();
    let mut shards =
        BucketShards::new(registry(), FailureDomain::Node).with_placement(Fixed(theirs.clone(), 0));
    shards.begin_tenure();
    let mut ids = ProposalIds::seeded(5);
    let planned = shards.plan(&store, &mut ids).await.unwrap().unwrap();
    assert_eq!(planned, theirs);
    let applied = make(&store, &planned).await;
    shards.applied(&planned, &applied);
    assert_eq!(shards.placement.1, 1);

    // Bucket 9 now misses its shard: after two scans, its creation is
    // this placement's own change, which the wrapped one never learns of.
    shards.registry.refresh(&store).await.unwrap();
    let planned = shards.plan(&store, &mut ids).await.unwrap().unwrap();
    assert_eq!(planned, theirs);
    let ours = shards.plan(&store, &mut ids).await.unwrap().unwrap();
    assert_ne!(ours, theirs);
    let applied = make(&store, &ours).await;
    shards.applied(&ours, &applied);
    assert_eq!(shards.placement.1, 1);
}
