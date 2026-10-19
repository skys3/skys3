//! [`RegisterFollower`] and [`ControlShardConfigs`]: a node follows the
//! shard registers that name it, reading only those that changed.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{Fault, FaultyStore};
use skys3_control::{
    ControlStore, DeleteOutcome, Expected, MemoryControlStore, PutOutcome, RegisterKey,
    RetryPolicy, TypedKey, read,
};
use skys3_types::Epoch;

use super::{Pki, cluster, config, put_register, register_of, run, shard, until};
use crate::{ControlShardConfigs, ShardConfigs};

#[test]
fn the_follower_opens_the_shards_whose_registers_name_the_node() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        put_register(&store, &config(0, 1, &[1])).await;
        put_register(&store, &config(1, 1, &[2])).await;
        // A register the node cannot parse is skipped.
        let garbage = RegisterKey::shard(&shard(2).bucket, shard(2).shard);
        let written = store.put_if(&garbage, Expected::Absent, Bytes::from_static(b"{"));
        assert!(matches!(written.await, Ok(PutOutcome::Written(_))));
        let nodes = cluster(&pki, 1, &store).await;
        let (shards, seen) = (&nodes[0].shards, &nodes[0].seen);
        let configs = ControlShardConfigs::new(store.clone(), Default::default());
        assert!(format!("{configs:?}").contains("ControlShardConfigs"));
        let follower = shards.follower(Arc::new(configs));
        assert!(format!("{follower:?}").contains("RegisterFollower"));

        assert_eq!(follower.follow().await, 1);
        let set = shards.replication().set();
        let replica = set.get(&(&shard(0)).into()).await.unwrap();
        assert_eq!(replica.config(), config(0, 1, &[1]));
        assert!(set.get(&(&shard(1)).into()).await.is_none());
        assert_eq!(seen.seen().opened, [config(0, 1, &[1])]);

        // Nothing changed: the replica is found, and nothing opens.
        assert_eq!(follower.follow().await, 0);
        assert_eq!(seen.seen().found, [config(0, 1, &[1])]);

        // A newer configuration is adopted.
        until(|| replica.is_serving()).await;
        put_register(&store, &config(0, 2, &[1])).await;
        assert_eq!(follower.follow().await, 1);
        let replica = set.get(&(&shard(0)).into()).await.unwrap();
        assert_eq!(replica.config().epoch, Epoch::new(2));
        assert_eq!(seen.seen().opened.len(), 2);
    });
}

#[test]
fn the_follower_passes_on_its_interval_and_survives_an_outage() {
    run(async {
        let (pki, memory) = (Pki::new(), MemoryControlStore::new());
        let nodes = cluster(&pki, 1, &memory).await;
        let shards = &nodes[0].shards;
        let store = FaultyStore::new(memory.clone());
        // The first listing fails, and so does the first read after it.
        store.script([Fault::Unavailable]);
        let once = RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        };
        let follower = shards.follower(Arc::new(ControlShardConfigs::new(store.clone(), once)));
        assert_eq!(follower.follow().await, 0);
        put_register(&memory, &config(0, 1, &[1])).await;
        store.script([Fault::Pass, Fault::Unavailable]);
        assert_eq!(follower.follow().await, 0);

        tokio::spawn(follower.run(Duration::from_millis(10)));
        let set = shards.replication().set().clone();
        let s = (&shard(0)).into();
        loop {
            if set.get(&s).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

#[test]
fn a_listed_register_is_read_once_per_version() {
    run(async {
        let memory = MemoryControlStore::new();
        put_register(&memory, &config(0, 1, &[1])).await;
        put_register(&memory, &config(1, 1, &[2])).await;
        // Not a shard register: not listed as one.
        let other = RegisterKey::new("shards/b-1/notes.txt").unwrap();
        let written = memory.put_if(&other, Expected::Absent, Bytes::from_static(b"x"));
        assert!(matches!(written.await, Ok(PutOutcome::Written(_))));
        let store = FaultyStore::new(memory.clone());
        let configs = ControlShardConfigs::new(store.clone(), Default::default());

        let listed = configs.shards().await.unwrap();
        assert_eq!(listed, [shard(0), shard(1)].into());
        let before = store.requests();
        assert_eq!(
            configs.config(&shard(0)).await.unwrap(),
            Some(config(0, 1, &[1]))
        );
        assert_eq!(store.requests(), before + 1, "a first read");

        // Listed again at the same version: no read.
        configs.shards().await.unwrap();
        let before = store.requests();
        assert_eq!(
            configs.config(&shard(0)).await.unwrap(),
            Some(config(0, 1, &[1]))
        );
        assert_eq!(store.requests(), before, "read from the cache");
        // Without a new listing, a read goes to the store.
        configs.config(&shard(0)).await.unwrap();
        assert_eq!(store.requests(), before + 1, "no listing vouches for it");

        // A new version is read.
        put_register(&memory, &config(0, 2, &[1])).await;
        configs.shards().await.unwrap();
        assert_eq!(
            configs.config(&shard(0)).await.unwrap(),
            Some(config(0, 2, &[1]))
        );

        // A register deleted since the listing reads as none, and a
        // deleted one leaves the listing and the cache.
        let key = TypedKey::shard(&shard(1).bucket, shard(1).shard);
        let held = read(&memory, &key).await.unwrap().unwrap();
        configs.shards().await.unwrap();
        let deleted = memory.delete_if(key.key(), &held.version).await;
        assert_eq!(deleted.unwrap(), DeleteOutcome::Deleted);
        assert_eq!(configs.config(&shard(1)).await.unwrap(), None);
        assert_eq!(configs.shards().await.unwrap(), [shard(0)].into());
        assert!(register_of(&memory, &shard(1)).await.is_none());
    });
}
