//! [`ReplicatedShards`]: which shards a node opens, and the calls its
//! replicas serve.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::{ControlError, MemoryControlStore};
use skys3_gateway::{Precondition, ShardError, ShardRef, Shards, StreamedBody};
use skys3_index::ListQuery;
use skys3_log::record::Extent;
use skys3_types::{BucketDocument, RegisterDocument, ShardConfig};

use super::{Pki, Recorder, cluster, config, node, put, put_register, run, shard, until};
use crate::{BoxFuture, Opening, ShardConfigs};

/// The document of bucket `b-1`, which opening a replicated shard ignores.
fn bucket_document() -> BucketDocument {
    BucketDocument::from_json(
        br#"{"bucket_id":"b-1","name":"photos","mode":"local","shards":8,"replicas":1,
        "min_write_replicas":1,"clean_copies":0,"created_unix_ms":0,"proposal_id":"p"}"#,
    )
    .unwrap()
}

/// Fixed configurations, or a failure to read any.
struct Fixed(Option<BTreeMap<ShardRef, ShardConfig>>);

impl ShardConfigs for Fixed {
    fn shards(&self) -> BoxFuture<'_, Result<BTreeSet<ShardRef>, ControlError>> {
        let shards = match &self.0 {
            Some(configs) => Ok(configs.keys().cloned().collect()),
            None => Err(ControlError::Unavailable("down".to_owned())),
        };
        Box::pin(async move { shards })
    }

    fn config<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        let config = match &self.0 {
            Some(configs) => Ok(configs.get(shard).cloned()),
            None => Err(ControlError::Unavailable("down".to_owned())),
        };
        Box::pin(async move { config })
    }
}

#[test]
fn a_shard_opens_only_where_its_register_names_the_node() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        put_register(&store, &config(0, 1, &[1])).await;
        put_register(&store, &config(1, 1, &[2])).await;
        let nodes = cluster(&pki, 1, &store).await;
        let (shards, seen) = (&nodes[0].shards, &nodes[0].seen);
        let set = shards.replication().set();

        shards.open(&shard(0), &bucket_document()).await.unwrap();
        let replica = set.get(&(&shard(0)).into()).await.unwrap();
        assert_eq!(replica.config(), config(0, 1, &[1]));
        assert_eq!(seen.seen().opened, [config(0, 1, &[1])]);
        // Opening it again opens nothing new.
        shards.open(&shard(0), &bucket_document()).await.unwrap();
        assert_eq!(seen.seen().opened.len(), 1);

        // Another node's shard, and one without a register.
        shards.open(&shard(1), &bucket_document()).await.unwrap();
        shards.open(&shard(2), &bucket_document()).await.unwrap();
        assert!(set.get(&(&shard(1)).into()).await.is_none());
        assert!(set.get(&(&shard(2)).into()).await.is_none());
        assert_eq!(seen.seen().opened.len(), 1);
        assert_eq!(Shards::node(shards), Some(node(1)));
        assert!(format!("{shards:?}").contains("node-1"));
    });
}

#[test]
fn configurations_from_elsewhere_open_shards_too() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        let nodes = cluster(&pki, 1, &store).await;
        let configs: BTreeMap<ShardRef, ShardConfig> = [
            (shard(0), config(0, 3, &[1])),
            (shard(1), config(1, 1, &[2])),
        ]
        .into();
        let seen = Recorder::default();
        let shards = nodes[0]
            .shards
            .clone()
            .with_opening(Opening::Configs(Arc::new(Fixed(Some(configs)))))
            .with_observer(Arc::new(seen.clone()));
        assert!(format!("{shards:?}").contains("Configs"));
        let set = shards.replication().set();

        shards.open(&shard(0), &bucket_document()).await.unwrap();
        shards.open(&shard(1), &bucket_document()).await.unwrap();
        shards.open(&shard(2), &bucket_document()).await.unwrap();
        let replica = set.get(&(&shard(0)).into()).await.unwrap();
        assert_eq!(replica.config(), config(0, 3, &[1]));
        assert!(set.get(&(&shard(1)).into()).await.is_none());
        assert_eq!(seen.seen().opened, [config(0, 3, &[1])]);

        // A configuration that cannot be read fails the open.
        let down = shards.with_opening(Opening::Configs(Arc::new(Fixed(None))));
        let failed = down.open(&shard(3), &bucket_document()).await;
        assert!(
            matches!(failed, Err(ShardError::Unavailable { .. })),
            "{failed:?}"
        );
        // So does a configuration that does not name the node.
        let foreign = down.open_config(&shard(4), &config(4, 1, &[2])).await;
        assert!(
            matches!(foreign, Err(ShardError::Unavailable { .. })),
            "{foreign:?}"
        );
        assert_eq!(format!("{:?}", Opening::default()), "Resume");
    });
}

#[test]
fn a_primary_serves_every_call_through_its_replica() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        put_register(&store, &config(0, 1, &[1])).await;
        let nodes = cluster(&pki, 1, &store).await;
        let shards = &nodes[0].shards;
        let s = shard(0);
        shards.open(&s, &bucket_document()).await.unwrap();
        let replica = shards.replication().set().get(&(&s).into()).await.unwrap();
        until(|| replica.is_serving()).await;

        let written = shards.write(&s, put("a", 10), Precondition::None).await;
        let version = written.unwrap().unwrap();
        let entry = shards.entry(&s, "a").await.unwrap().unwrap();
        assert_eq!(entry.version, version);
        let query = ListQuery {
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_items: 10,
        };
        assert_eq!(shards.list(&s, &query).await.unwrap().items.len(), 1);
        assert_eq!(shards.import_progress(&s).await.unwrap(), None);
        assert_eq!(shards.payload(&s, version).await.unwrap().len(), 10);

        // A read registered with the primary as its holder.
        let plan = shards.plan(&s, "a").await.unwrap();
        assert_eq!(plan.holders.first(), Some(&node(1)));
        let registered = shards
            .register(&s, &node(1), "a", version, plan.layout.clone())
            .await
            .unwrap()
            .expect("the primary holds the version");
        assert!(shards.renew(&s, &node(1), registered.id).await.unwrap());
        let position = registered.layout.first().map_or(version, |r| r.position);
        let fetched = shards.fetch(&s, &node(1), registered.id, position).await;
        assert_eq!(fetched.unwrap().len(), 10);
        shards.release(&s, &node(1), registered.id).await.unwrap();

        // Multipart and extents.
        assert!(
            shards
                .upload(&s, "a", version, 0, 10)
                .await
                .unwrap()
                .is_none()
        );
        assert!(shards.uploads(&s, "", None, 10).await.unwrap().is_empty());
        assert!(shards.parts(&s, version, 0, 10).await.unwrap().is_empty());
        let extent = Extent {
            key: "big".to_owned(),
            offset: 0,
            data: Bytes::from(vec![1; 600]),
        };
        let extent = shards.append_extent(&s, extent).await.unwrap();
        assert_eq!(extent.len, 600);
        let body = StreamedBody {
            key: "big".to_owned(),
            upload: extent.position,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            extents: vec![(0, extent)],
        };
        // No flusher follows the shard: both are answered, not lost.
        let _ = shards.announce(&s, body).await;
        let _ = shards.flushed(&s, "a", version, Duration::ZERO).await;

        let writes = vec![
            (put("b", 5), Precondition::None),
            (put("a", 5), Precondition::Absent),
        ];
        let outcomes = shards.write_all(&s, writes).await;
        assert!(matches!(outcomes[0], Ok(Ok(_))), "{outcomes:?}");
        assert!(matches!(outcomes[1], Ok(Err(_))), "{outcomes:?}");

        assert_eq!(shards.seal(&s).await.unwrap().objects, 2);
        shards.unseal(&s).await.unwrap();
        shards.remove(&s).await.unwrap();
        assert!(shards.replication().set().get(&(&s).into()).await.is_none());
    });
}

#[test]
fn a_restarted_node_resumes_the_replicas_it_keeps() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        put_register(&store, &config(0, 1, &[1])).await;
        let nodes = cluster(&pki, 1, &store).await;
        let (shards, seen) = (&nodes[0].shards, &nodes[0].seen);
        // Nothing kept yet.
        assert!(shards.resume_kept().await.unwrap().is_empty());

        let replica = shards.open_config(&shard(0), &config(0, 1, &[1])).await;
        let replica = replica.unwrap();
        until(|| replica.is_serving()).await;
        shards
            .write(&shard(0), put("a", 10), Precondition::None)
            .await
            .unwrap()
            .unwrap();
        let resumed = shards.resume_kept().await.unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].config(), config(0, 1, &[1]));
        assert_eq!(seen.seen().opened.len(), 2);
    });
}
