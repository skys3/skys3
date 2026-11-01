//! [`Handoffs`]: a node starts the handoffs a coordinator asks for only
//! on its serving primary, in the epoch asked, to another member.

use skys3_control::MemoryControlStore;
use skys3_coord::{Handoff, HandoffSink};
use skys3_gateway::Shards;
use skys3_types::Epoch;

use super::{
    Pki, bucket, cluster, config, node, put, put_register, register_of, run, shard, until,
};

/// A handoff of shard 0 in `epoch` to `to`.
fn asked(epoch: u64, to: u8) -> Handoff {
    Handoff {
        bucket: bucket(),
        shard: shard(0).shard,
        epoch: Epoch::new(epoch),
        to: node(to),
    }
}

#[test]
fn a_node_starts_only_the_handoffs_it_can_make() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        let initial = config(0, 1, &[1, 2]);
        put_register(&store, &initial).await;
        let nodes = cluster(&pki, 2, &store).await;
        let primary = nodes[0].shards.handoffs();
        assert!(format!("{primary:?}").contains("node-1"));

        // The shard is not open yet.
        let refused = primary.hand_off(asked(1, 2)).await.unwrap_err();
        assert!(refused.contains("not open"), "{refused}");

        let mut replicas = Vec::new();
        for n in &nodes {
            replicas.push(n.shards.open_config(&shard(0), &initial).await.unwrap());
        }
        let old = &replicas[0];
        until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
        nodes[0]
            .shards
            .write(&shard(0), put("a", 10), Default::default())
            .await
            .unwrap()
            .unwrap();

        // A request planned from another epoch, to the primary itself, to
        // a node that is not a member, or to a member's node.
        let refused = primary.hand_off(asked(2, 2)).await.unwrap_err();
        assert!(refused.contains("epoch 1, not 2"), "{refused}");
        let refused = primary.hand_off(asked(1, 1)).await.unwrap_err();
        assert!(refused.contains("not another member"), "{refused}");
        let refused = primary.hand_off(asked(1, 3)).await.unwrap_err();
        assert!(refused.contains("not another member"), "{refused}");
        let member = nodes[1].shards.handoffs();
        let refused = member.hand_off(asked(1, 1)).await.unwrap_err();
        assert!(refused.contains("serving primary"), "{refused}");
        assert!(nodes[0].seen.seen().began.is_empty());

        // The handoff asked for starts, and node 2 takes over.
        primary.hand_off(asked(1, 2)).await.unwrap();
        assert_eq!(
            nodes[0].seen.seen().began,
            [(shard(0), Epoch::new(1))],
            "reported as it begins"
        );
        until(|| !nodes[0].seen.seen().ended.is_empty()).await;
        let (ended, handed) = nodes[0].seen.seen().ended[0].clone();
        assert_eq!(ended, shard(0));
        let handed = handed.expect("the handoff stepped down");
        assert!(handed.sent);
        assert_eq!(handed.epoch, Epoch::new(1));
        loop {
            let held = register_of(&store, &shard(0)).await.unwrap();
            if held.epoch > Epoch::new(1) {
                assert_eq!(held.primary, node(2));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // The old primary serves no more, so asking again is refused.
        let refused = primary.hand_off(asked(1, 2)).await.unwrap_err();
        assert!(refused.contains("serving primary"), "{refused}");
    });
}

#[test]
fn a_failed_handoff_is_reported() {
    run(async {
        let (pki, store) = (Pki::new(), MemoryControlStore::new());
        let initial = config(0, 1, &[1, 2]);
        put_register(&store, &initial).await;
        let nodes = cluster(&pki, 2, &store).await;
        let mut replicas = Vec::new();
        for n in &nodes {
            replicas.push(n.shards.open_config(&shard(0), &initial).await.unwrap());
        }
        let old = &replicas[0];
        until(|| old.is_serving() && old.leader().unwrap().holds_leases()).await;
        // The register moved on unseen, so the handoff itself refuses.
        put_register(&store, &config(0, 2, &[1, 2])).await;
        nodes[0]
            .shards
            .handoffs()
            .hand_off(asked(1, 2))
            .await
            .unwrap();
        until(|| !nodes[0].seen.seen().ended.is_empty()).await;
        assert_eq!(nodes[0].seen.seen().ended[0], (shard(0), None));
    });
}
