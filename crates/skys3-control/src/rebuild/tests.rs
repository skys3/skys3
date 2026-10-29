use std::sync::{Arc, Mutex, PoisonError};

use proptest::prelude::*;

use skys3_types::{BucketMode, ClusterDocument, RegisterDocument};

use super::*;
use crate::faults::{Fault, FaultyStore};
use crate::memory::MemoryControlStore;
use crate::propose::read_with_retries;
use crate::{Version, bootstrap};
use crate::{key::TypedKey, propose::ProposalIds};

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn bucket_id(b: u8) -> BucketId {
    BucketId::new(format!("b-{b}")).unwrap()
}

/// Bucket `bucket-<b>` with ID `b-<b>` and `shards` shards.
fn bucket(b: u8, shards: u32) -> BucketDocument {
    BucketDocument {
        bucket_id: bucket_id(b),
        name: BucketName::new(format!("bucket-{b}")).unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(shards).unwrap(),
        replicas: 3,
        min_write_replicas: 2,
        clean_copies: 1,
        target: None,
        created_unix_ms: 1,
        proposal_id: ProposalId::from_u128(u128::from(b)),
    }
}

/// Shard `shard` of bucket `b` in `epoch`, led by the first of `members`.
fn config(b: u8, shard: u8, epoch: u64, members: &[u8]) -> ShardConfig {
    ShardConfig {
        bucket_id: bucket_id(b),
        shard: ShardId::new(shard),
        epoch: Epoch::new(epoch),
        primary: node(members[0]),
        members: members.iter().map(|n| node(*n)).collect(),
        learners: Vec::new(),
        min_write_replicas: 2,
        replicas: 3,
        proposal_id: ProposalId::from_u128(u128::from(epoch) << 8 | u128::from(shard)),
    }
}

/// A copy at `generation` of `buckets` and the identity registers named.
fn copy(
    generation: u64,
    synced_at_ms: u64,
    buckets: &[BucketDocument],
    roles: &[&str],
) -> ExportedCopy {
    let mut registers = BTreeMap::new();
    for bucket in buckets {
        let value = String::from_utf8(bucket.to_json().unwrap()).unwrap();
        registers.insert(RegisterKey::bucket(&bucket.name).as_str().to_owned(), value);
    }
    for role in roles {
        registers.insert(
            format!("identity/roles/{role}.json"),
            format!("{{\"role\":\"{role}\"}}"),
        );
    }
    ExportedCopy {
        generation: Generation::new(generation),
        synced_at_ms,
        registers,
    }
}

fn export(n: u8, copy: Option<ExportedCopy>, configs: Vec<ShardConfig>) -> ControlExport {
    let held = configs
        .iter()
        .map(|config| HeldShard {
            bucket_id: config.bucket_id.clone(),
            shard: config.shard,
            objects: true,
        })
        .collect();
    ControlExport {
        format: EXPORT_FORMAT,
        cluster_id: cluster(),
        node_id: node(n),
        instance_id: format!("instance-{n}"),
        copy,
        configs,
        held,
    }
}

/// Three nodes holding bucket 1's two shards. Node 1 applied epoch 3 of
/// shard 0 (node 3 removed); node 2 holds epoch 2 still; node 3 was
/// removed and holds epoch 2. Shard 1 is at epoch 1 everywhere. Node 2's
/// copy is the newest and no longer holds role `gone`.
fn cluster_exports() -> Vec<ControlExport> {
    let buckets = [bucket(1, 2)];
    vec![
        export(
            1,
            Some(copy(4, 100, &buckets, &["admin", "gone"])),
            vec![config(1, 0, 3, &[1, 2]), config(1, 1, 1, &[2, 1, 3])],
        ),
        export(
            2,
            Some(copy(5, 90, &buckets, &["admin"])),
            vec![config(1, 0, 2, &[1, 2, 3]), config(1, 1, 1, &[2, 1, 3])],
        ),
        export(
            3,
            Some(copy(5, 80, &buckets, &["admin"])),
            vec![config(1, 0, 2, &[1, 2, 3]), config(1, 1, 1, &[2, 1, 3])],
        ),
    ]
}

fn plan(exports: &[ControlExport]) -> Result<RebuildPlan, RebuildError> {
    RebuildPlan::new(&cluster(), exports, &RebuildOptions::default())
}

fn key(text: &str) -> RegisterKey {
    RegisterKey::new(text).unwrap()
}

#[test]
fn the_plan_takes_each_shards_newest_configuration_and_the_newest_copy() {
    let rebuilt = plan(&cluster_exports()).unwrap();
    assert_eq!(rebuilt.cluster_id(), &cluster());
    assert_eq!(rebuilt.generation(), Generation::new(6));
    // Equal copies of the newest generation: the lowest node ID names them.
    assert_eq!(rebuilt.copy_from(), &node(2));
    let keys: Vec<&str> = rebuilt
        .registers()
        .keys()
        .map(RegisterKey::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "buckets/bucket-1.json",
            "identity/roles/admin.json",
            "shards/b-1/0.json",
            "shards/b-1/1.json",
        ]
    );
    // Byte for byte: a replica finds its own configuration.
    let shard = &rebuilt.registers()[&key("shards/b-1/0.json")];
    assert_eq!(
        ShardConfig::from_json(shard).unwrap(),
        config(1, 0, 3, &[1, 2])
    );
    let configs: Vec<ShardConfig> = rebuilt.shard_configs().collect();
    assert_eq!(
        configs,
        [config(1, 0, 3, &[1, 2]), config(1, 1, 1, &[2, 1, 3])]
    );
    let bucket = &rebuilt.registers()[&key("buckets/bucket-1.json")];
    assert_eq!(bucket.as_ref(), bucket_json(1, 2).as_slice());

    let document = ClusterDocument::from_json(rebuilt.cluster_json()).unwrap();
    assert_eq!(document.generation, Generation::new(6));
    assert_eq!(document.cluster_id, cluster());
    // The same exports give the same plan, `cluster.json` included.
    assert_eq!(plan(&cluster_exports()).unwrap(), rebuilt);

    let notes: Vec<String> = rebuilt.notes().iter().map(ToString::to_string).collect();
    assert_eq!(
        rebuilt.notes(),
        [
            RebuildNote::Behind {
                shard: key("shards/b-1/0.json"),
                node: node(2),
                epoch: Epoch::new(2),
            },
            RebuildNote::Behind {
                shard: key("shards/b-1/0.json"),
                node: node(3),
                epoch: Epoch::new(2),
            },
        ],
        "{notes:?}"
    );
    assert!(notes[0].contains("node-2 holds epoch 2 of shards/b-1/0.json"));
}

fn bucket_json(b: u8, shards: u32) -> Vec<u8> {
    bucket(b, shards).to_json().unwrap()
}

#[test]
fn two_configurations_of_one_epoch_are_refused() {
    let mut exports = cluster_exports();
    exports[2].configs[1] = config(1, 1, 1, &[3, 1, 2]);
    let error = plan(&exports).unwrap_err();
    assert!(
        matches!(&error, RebuildError::ConflictingConfigs { epoch, first, second, .. }
            if *epoch == Epoch::new(1) && *first == node(1) && *second == node(3)),
        "{error}"
    );
    assert!(error.to_string().contains("shards/b-1/1.json"), "{error}");
}

#[test]
fn every_member_of_a_rebuilt_configuration_must_be_exported_or_lost() {
    let mut exports = cluster_exports();
    // Node 2's disks are gone: it cannot export.
    exports.remove(1);
    let error = plan(&exports).unwrap_err();
    let RebuildError::MissingExports(missing) = &error else {
        panic!("{error}");
    };
    assert_eq!(
        missing[&node(2)],
        [key("shards/b-1/0.json"), key("shards/b-1/1.json")]
    );
    assert!(error.to_string().contains("node-2 (2 shards)"), "{error}");

    let options = RebuildOptions {
        lost: BTreeSet::from([node(2)]),
        ..RebuildOptions::default()
    };
    let plan = RebuildPlan::new(&cluster(), &exports, &options).unwrap();
    assert!(plan.notes().contains(&RebuildNote::LostMember {
        shard: key("shards/b-1/1.json"),
        node: node(2),
    }));
    // The newest copy left is node 3's.
    assert_eq!(plan.copy_from(), &node(3));

    // A node is either exported or lost, not both.
    let error = RebuildPlan::new(&cluster(), &cluster_exports(), &options).unwrap_err();
    assert!(
        matches!(error, RebuildError::ExportedAndLost(ref n) if *n == node(2)),
        "{error}"
    );
}

#[test]
fn shards_of_buckets_the_copy_does_not_name_are_not_rebuilt() {
    let mut exports = cluster_exports();
    // A deleted bucket's empty shard, and one of a bucket the copy missed.
    exports[0].configs.push(config(7, 0, 4, &[1, 2, 3]));
    exports[0].held.push(HeldShard {
        bucket_id: bucket_id(7),
        shard: ShardId::new(0),
        objects: false,
    });
    exports[1].held.push(HeldShard {
        bucket_id: bucket_id(8),
        shard: ShardId::new(2),
        objects: true,
    });
    let error = plan(&exports).unwrap_err();
    assert!(
        matches!(&error, RebuildError::UnnamedShards(keys) if *keys == [key("shards/b-8/2.json")]),
        "{error}"
    );
    assert!(error.to_string().contains("shards/b-8/2.json"));

    let options = RebuildOptions {
        allow_unnamed: true,
        ..RebuildOptions::default()
    };
    let plan = RebuildPlan::new(&cluster(), &exports, &options).unwrap();
    assert!(!plan.registers().contains_key(&key("shards/b-7/0.json")));
    let unnamed: Vec<&RebuildNote> = plan
        .notes()
        .iter()
        .filter(|note| matches!(note, RebuildNote::Unnamed { .. }))
        .collect();
    assert_eq!(
        unnamed,
        [
            &RebuildNote::Unnamed {
                shard: key("shards/b-7/0.json"),
                objects: false,
            },
            &RebuildNote::Unnamed {
                shard: key("shards/b-8/2.json"),
                objects: true,
            },
        ]
    );
    assert!(
        unnamed[1]
            .to_string()
            .ends_with("but holds objects; it is not rebuilt")
    );
}

#[test]
fn a_single_node_keeps_no_shard_configurations() {
    // A single node serves its shards alone and keeps no configuration:
    // the rebuild restores the buckets and the identity registers.
    let mut single = export(
        1,
        Some(copy(3, 10, &[bucket(1, 4), bucket(2, 1)], &["r"])),
        vec![],
    );
    single.held.push(HeldShard {
        bucket_id: bucket_id(1),
        shard: ShardId::new(0),
        objects: true,
    });
    let plan = plan(&[single]).unwrap();
    assert_eq!(plan.registers().len(), 3);
    assert_eq!(plan.generation(), Generation::new(4));
    assert_eq!(
        plan.notes(),
        [
            RebuildNote::Unconfigured {
                bucket: BucketName::new("bucket-1").unwrap(),
                shards: 4,
            },
            RebuildNote::Unconfigured {
                bucket: BucketName::new("bucket-2").unwrap(),
                shards: 1,
            },
        ]
    );
    assert!(
        plan.notes()[0]
            .to_string()
            .starts_with("4 shards of bucket bucket-1")
    );
}

#[test]
fn copies_that_differ_at_the_newest_generation_need_a_choice() {
    let mut exports = cluster_exports();
    // Node 3 listed a role created after node 2's listing, before any
    // increment announced it. Its sync started later by its own clock, but
    // a clock decides nothing.
    exports[2].copy = Some(copy(5, 500, &[bucket(1, 2)], &["admin", "late"]));
    let error = plan(&exports).unwrap_err();
    let RebuildError::DivergentCopies {
        generation,
        copies,
        registers,
    } = &error
    else {
        panic!("{error}");
    };
    assert_eq!(*generation, Generation::new(5));
    assert_eq!(*copies, [vec![node(2)], vec![node(3)]]);
    assert_eq!(*registers, ["identity/roles/late.json"]);
    assert_eq!(
        error.to_string(),
        "the copies of the bucket and identity registers at generation 5 differ, between \
         [node-2] and [node-3], in identity/roles/late.json: nothing shows which is newer, so \
         choose the copy to rebuild from"
    );
    // A register two copies hold with other values differs too, and equal
    // copies are grouped.
    let mut more = exports.clone();
    let mut fourth = export(4, Some(copy(5, 1, &[bucket(1, 2)], &["admin"])), vec![]);
    fourth
        .copy
        .as_mut()
        .unwrap()
        .registers
        .insert("identity/roles/admin.json".to_owned(), "{}".to_owned());
    more.push(fourth);
    more.push(export(5, exports[1].copy.clone(), vec![]));
    let error = plan(&more).unwrap_err();
    assert!(
        matches!(&error, RebuildError::DivergentCopies { copies, registers, .. }
            if *copies == [vec![node(2), node(5)], vec![node(3)], vec![node(4)]]
                && *registers == ["identity/roles/admin.json", "identity/roles/late.json"]),
        "{error}"
    );

    // The operator chooses either copy of the newest generation.
    for (chosen, other, late) in [(3, 2, true), (2, 3, false)] {
        let options = RebuildOptions {
            prefer: Some(node(chosen)),
            ..RebuildOptions::default()
        };
        let plan = RebuildPlan::new(&cluster(), &exports, &options).unwrap();
        assert_eq!(plan.copy_from(), &node(chosen));
        assert_eq!(
            plan.registers()
                .contains_key(&key("identity/roles/late.json")),
            late
        );
        assert!(
            plan.notes()
                .contains(&RebuildNote::CopyDiffers { node: node(other) })
        );
        assert_eq!(plan.generation(), Generation::new(6));
    }
    assert_eq!(
        RebuildNote::CopyDiffers { node: node(3) }.to_string(),
        "the copy of node-3 has the rebuilt generation but other registers; the chosen copy is \
         rebuilt"
    );

    // A higher generation always wins: a copy of an older one, or none,
    // cannot be chosen.
    for (chosen, expected) in [
        (
            1,
            "the chosen copy, of node node-1, is at generation 4; the newest copies are at \
             generation 5",
        ),
        (9, "the chosen copy, of node node-9, does not exist"),
    ] {
        let options = RebuildOptions {
            prefer: Some(node(chosen)),
            ..RebuildOptions::default()
        };
        let error = RebuildPlan::new(&cluster(), &exports, &options).unwrap_err();
        assert!(
            matches!(&error, RebuildError::PreferredNotNewest { node: n, newest, .. }
                if *n == node(chosen) && *newest == Generation::new(5)),
            "{error}"
        );
        assert!(error.to_string().starts_with(expected), "{error}");
    }

    // Equal copies need no choice; a choice takes the copy, without notes.
    let options = RebuildOptions {
        prefer: Some(node(3)),
        ..RebuildOptions::default()
    };
    let plan = RebuildPlan::new(&cluster(), &cluster_exports(), &options).unwrap();
    assert_eq!(plan.copy_from(), &node(3));
    assert!(
        !plan
            .notes()
            .iter()
            .any(|note| matches!(note, RebuildNote::CopyDiffers { .. }))
    );
}

#[test]
fn exports_that_do_not_fit_together_are_refused() {
    let check = |exports: &[ControlExport], expected: &str| {
        let error = plan(exports).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    };
    check(&[], "no node's export");
    let mut exports = cluster_exports();
    exports[1].format = 2;
    check(&exports, "has format 2; this build reads format 1");
    let mut exports = cluster_exports();
    exports[1].cluster_id = ClusterId::new("other").unwrap();
    check(&exports, "belongs to cluster other, not prod");
    let mut exports = cluster_exports();
    exports[2].node_id = node(1);
    check(&exports, "node-1 was exported twice");
    let mut exports = cluster_exports();
    for export in &mut exports {
        export.copy = None;
    }
    check(&exports, "no export holds a copy");
}

#[test]
fn exports_holding_what_no_node_writes_are_refused() {
    let invalid = |change: &dyn Fn(&mut ControlExport), expected: &str| {
        let mut exports = cluster_exports();
        change(&mut exports[1]);
        // Node 3's copy, of the same generation, is equal to node 2's.
        exports[2].copy = exports[1].copy.clone();
        let error = plan(&exports).unwrap_err();
        assert!(
            matches!(&error, RebuildError::InvalidExport { node: n, .. } if *n == node(2)),
            "{error}"
        );
        assert!(error.to_string().contains(expected), "{error}");
    };
    fn registers(export: &mut ControlExport) -> &mut BTreeMap<String, String> {
        &mut export.copy.as_mut().unwrap().registers
    }
    invalid(
        &|export| {
            registers(export).insert("Bad Key".to_owned(), "{}".to_owned());
        },
        "Bad Key",
    );
    invalid(
        &|export| {
            registers(export).insert("nodes/node-1.json".to_owned(), "{}".to_owned());
        },
        "not a bucket or identity register",
    );
    invalid(
        &|export| {
            registers(export).insert("buckets/x.json".to_owned(), "{}".to_owned());
        },
        "buckets/x.json: malformed bucket",
    );
    invalid(
        &|export| {
            let value = String::from_utf8(bucket_json(1, 2)).unwrap();
            registers(export).insert("buckets/other.json".to_owned(), value);
        },
        "buckets/other.json holds bucket bucket-1",
    );
    invalid(
        &|export| export.configs[0].members.clear(),
        "the configuration of shards/b-1/0.json",
    );
    invalid(
        &|export| export.configs.push(config(1, 5, 9, &[1, 2, 3])),
        "shards/b-1/5.json is past the bucket's 2 shards",
    );
    invalid(
        &|export| export.copy.as_mut().unwrap().generation = Generation::MAX,
        "generation is at its maximum",
    );
}

#[test]
fn exports_round_trip_through_json_and_refuse_unknown_fields() {
    let export = cluster_exports().remove(0);
    let json = export.to_json();
    assert_eq!(ControlExport::from_json(&json).unwrap(), export);
    let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    value["extra"] = serde_json::Value::Bool(true);
    let text = serde_json::to_vec(&value).unwrap();
    assert!(ControlExport::from_json(&text).is_err());
    // Optional parts may be left out.
    let minimal =
        br#"{"format": 1, "cluster_id": "prod", "node_id": "node-1", "instance_id": "i"}"#;
    let parsed = ControlExport::from_json(minimal).unwrap();
    assert!(parsed.copy.is_none() && parsed.configs.is_empty() && parsed.held.is_empty());
}

/// A store whose requests are recorded, in order, as `(operation, key)`.
#[derive(Debug, Clone, Default)]
struct Recording {
    store: MemoryControlStore,
    puts: Arc<Mutex<Vec<String>>>,
}

impl Recording {
    fn puts(&self) -> Vec<String> {
        self.puts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ControlStore for Recording {
    type Changes = <MemoryControlStore as ControlStore>::Changes;

    async fn get(&self, key: &RegisterKey) -> Result<Option<crate::Versioned>, ControlError> {
        self.store.get(key).await
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        self.puts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key.as_str().to_owned());
        self.store.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<crate::DeleteOutcome, ControlError> {
        self.store.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.store.list(prefix).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.store.changes(after).await
    }
}

fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        ..RetryPolicy::default()
    }
}

#[tokio::test(start_paused = true)]
async fn the_rebuild_writes_every_register_and_cluster_json_last() {
    let plan = plan(&cluster_exports()).unwrap();
    let store = Recording::default();
    // A node registered itself, and a coordinator renewed its lease, while
    // the store was lost: both stay.
    let registration = Bytes::from_static(b"{\"node\":1}");
    for left in ["nodes/node-1.json", "coordinator.lease"] {
        let _ = store
            .store
            .put_if(&key(left), Expected::Absent, registration.clone())
            .await
            .unwrap();
    }
    let applied = plan.apply(&store, &policy()).await.unwrap();
    assert_eq!(
        applied,
        Applied {
            written: 5,
            present: 0
        }
    );
    let puts = store.puts();
    assert_eq!(puts.last().map(String::as_str), Some("cluster.json"));
    assert_eq!(puts.len(), 5);
    for (key, value) in plan.registers() {
        assert_eq!(store.get(key).await.unwrap().unwrap().value, *value);
    }
    let left = store.get(&key("nodes/node-1.json")).await.unwrap();
    assert_eq!(left.unwrap().value, registration);
    let cluster = read_with_retries(&store, &TypedKey::cluster(), &policy())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cluster.value.generation, Generation::new(6));

    // Running the plan again finds it complete.
    let again = plan.apply(&store, &policy()).await.unwrap();
    assert_eq!(
        again,
        Applied {
            written: 0,
            present: 5
        }
    );
    assert_eq!(store.puts().len(), 5);
}

#[tokio::test(start_paused = true)]
async fn lost_answers_and_an_interrupted_rebuild_are_completed() {
    let plan = plan(&cluster_exports()).unwrap();
    let store = FaultyStore::new(MemoryControlStore::new());
    store.script([
        // The check for cluster.json and the three listings.
        Fault::Pass,
        Fault::Pass,
        Fault::Pass,
        Fault::Pass,
        // The bucket register: written, the answer lost, the retry
        // refused, and the register read back.
        Fault::LoseResponse,
        Fault::Pass,
        Fault::Pass,
        // The identity register: lost before it landed, then written.
        Fault::LoseRequest,
        Fault::Pass,
        // Shard 0: a conflict, then written.
        Fault::Conflict,
        Fault::Pass,
        // Shard 1: a failure that is not retried.
        Fault::Fail,
    ]);
    let error = plan.apply(&store, &policy()).await.unwrap_err();
    assert!(
        matches!(error, RebuildError::Control(ControlError::Io(_))),
        "{error}"
    );
    assert!(store.get(&RegisterKey::cluster()).await.unwrap().is_none());

    // Run again, the rebuild finds the first three registers and writes
    // the rest.
    let applied = plan.apply(&store, &policy()).await.unwrap();
    assert_eq!(
        applied,
        Applied {
            written: 2,
            present: 3
        }
    );
    for (key, value) in plan.registers() {
        assert_eq!(store.get(key).await.unwrap().unwrap().value, *value);
    }
}

#[tokio::test(start_paused = true)]
async fn a_store_that_is_not_lost_is_left_alone() {
    let plan = plan(&cluster_exports()).unwrap();
    let policy = policy();

    // A live store.
    let store = MemoryControlStore::new();
    bootstrap(
        &store,
        &cluster(),
        ProposalIds::seeded(1).next_id(),
        &policy,
    )
    .await
    .unwrap();
    let error = plan.apply(&store, &policy).await.unwrap_err();
    assert!(matches!(error, RebuildError::StoreInUse), "{error}");
    assert!(error.to_string().contains("it is not lost"));

    // A store with registers the plan does not write.
    let store = MemoryControlStore::new();
    for other in ["buckets/other.json", "shards/b-9/0.json"] {
        let _ = store
            .put_if(&key(other), Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
    }
    let error = plan.apply(&store, &policy).await.unwrap_err();
    assert!(
        matches!(&error, RebuildError::StoreNotEmpty(keys)
            if *keys == [key("buckets/other.json"), key("shards/b-9/0.json")]),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .ends_with("buckets/other.json, shards/b-9/0.json")
    );
    assert!(store.get(&RegisterKey::cluster()).await.unwrap().is_none());

    // A register the plan writes, holding another value.
    let store = MemoryControlStore::new();
    let shard = key("shards/b-1/1.json");
    let _ = store
        .put_if(&shard, Expected::Absent, Bytes::from_static(b"{}"))
        .await
        .unwrap();
    let error = plan.apply(&store, &policy).await.unwrap_err();
    assert!(
        matches!(&error, RebuildError::RegisterDiffers(k) if *k == shard),
        "{error}"
    );
    assert!(store.get(&RegisterKey::cluster()).await.unwrap().is_none());
}

#[test]
fn a_store_behind_the_copy_says_so() {
    let error = ControlError::GenerationBehind {
        kept: Generation::new(7),
        found: Generation::new(2),
    };
    assert_eq!(
        error.to_string(),
        "the control store is at generation 2, older than generation 7 of the local copy: it \
         was reset or rebuilt from older state"
    );
    assert!(!error.is_retryable() && !error.may_have_applied());
}

/// The one configuration shard `shard` of bucket 1 has in `epoch`, as the
/// register's compare-and-swaps allow: led by node `1 + epoch % 4`, with
/// the next two nodes.
fn landed(shard: u8, epoch: u64) -> ShardConfig {
    let first = u8::try_from(epoch % 4).unwrap();
    let members: Vec<u8> = (0..3).map(|n| 1 + (first + n) % 4).collect();
    config(1, shard, epoch, &members)
}

proptest! {
    /// Whatever epochs each node holds of each shard, and whatever copies,
    /// the plan writes each shard's newest configuration, the newest
    /// copy's registers, and a generation past every copy's, whatever the
    /// order of the exports. Copies of the newest generation that differ
    /// are refused, whatever their sync times, until one is chosen.
    #[test]
    fn plans_take_the_newest_of_everything_in_any_order(
        held in prop::collection::vec(
            prop::collection::vec(prop::option::of(1_u64..6), 4),
            4,
        ),
        generations in prop::collection::vec(prop::option::of(1_u64..9), 4),
        late in prop::collection::vec(any::<bool>(), 4),
        synced in prop::collection::vec(0_u64..1000, 4),
    ) {
        let mut exports = Vec::new();
        for (n, (epochs, generation)) in held.iter().zip(&generations).enumerate() {
            let roles: &[&str] = if late[n] { &["late"] } else { &[] };
            let copy = generation.map(|g| copy(g, synced[n], &[bucket(1, 4)], roles));
            let n = u8::try_from(n).unwrap() + 1;
            let configs = epochs
                .iter()
                .enumerate()
                .filter_map(|(shard, epoch)| Some(landed(u8::try_from(shard).ok()?, (*epoch)?)))
                .collect();
            exports.push(export(n, copy, configs));
        }
        let Some(newest) = generations.iter().flatten().max() else {
            prop_assert!(matches!(plan(&exports), Err(RebuildError::NoCopy)));
            return Ok(());
        };
        let newest_late: Vec<(u8, bool)> = generations
            .iter()
            .zip(&late)
            .enumerate()
            .filter(|(_, (generation, _))| **generation == Some(*newest))
            .map(|(n, (_, late))| (u8::try_from(n).unwrap() + 1, *late))
            .collect();
        let (chosen, chosen_late) = newest_late[newest_late.len() - 1];
        let options = if newest_late.iter().all(|(_, late)| *late == chosen_late) {
            RebuildOptions::default()
        } else {
            prop_assert!(
                matches!(plan(&exports), Err(RebuildError::DivergentCopies { .. })),
                "{:?}", newest_late
            );
            RebuildOptions {
                prefer: Some(node(chosen)),
                ..RebuildOptions::default()
            }
        };
        let forward = RebuildPlan::new(&cluster(), &exports, &options).unwrap();
        exports.reverse();
        let backward = RebuildPlan::new(&cluster(), &exports, &options).unwrap();
        prop_assert_eq!(forward.registers(), backward.registers());
        prop_assert_eq!(forward.cluster_json(), backward.cluster_json());
        prop_assert_eq!(
            forward.registers().contains_key(&key("identity/roles/late.json")),
            chosen_late
        );
        prop_assert_eq!(forward.generation(), Generation::new(newest + 1));
        let configs: Vec<ShardConfig> = forward.shard_configs().collect();
        let expected: Vec<ShardConfig> = (0..4_u8)
            .filter_map(|shard| {
                let epoch = held.iter().filter_map(|e| e[usize::from(shard)]).max()?;
                Some(landed(shard, epoch))
            })
            .collect();
        prop_assert_eq!(configs, expected);
    }
}
