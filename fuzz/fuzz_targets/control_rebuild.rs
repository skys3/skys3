#![no_main]
#![forbid(unsafe_code)]
//! Exports of nodes' control state, read from files an operator hands
//! `skys3 control rebuild` (plan M3-07), parsed and merged into a rebuild
//! plan. Never panics, and a plan writes only bucket, identity, and shard
//! registers, each shard register a valid configuration, at a generation
//! past every copy's, from a copy of the newest generation: the one the
//! operator chose when those differ.

use libfuzzer_sys::fuzz_target;
use skys3_control::{ControlExport, KeyPrefix, RebuildError, RebuildOptions, RebuildPlan};
use skys3_types::{ClusterDocument, RegisterDocument};

fuzz_target!(|data: &[u8]| {
    // Several exports, separated by NUL bytes, which JSON never holds.
    let exports: Vec<ControlExport> = data
        .split(|byte| *byte == 0)
        .filter_map(|part| ControlExport::from_json(part).ok())
        .collect();
    let Some(cluster) = exports.first().map(|export| export.cluster_id.clone()) else {
        return;
    };
    for allow_unnamed in [false, true] {
        let mut options = RebuildOptions {
            allow_unnamed,
            ..RebuildOptions::default()
        };
        let plan = match RebuildPlan::new(&cluster, &exports, &options) {
            Ok(plan) => plan,
            // Copies of the newest generation differ: the operator chooses
            // one of them, and the plan rebuilds from it.
            Err(RebuildError::DivergentCopies { copies, .. }) => {
                assert!(copies.len() > 1);
                let chosen = copies[copies.len() - 1][0].clone();
                options.prefer = Some(chosen.clone());
                let Ok(plan) = RebuildPlan::new(&cluster, &exports, &options) else {
                    continue;
                };
                assert_eq!(plan.copy_from(), &chosen);
                plan
            }
            Err(_) => continue,
        };
        let newest = exports
            .iter()
            .filter_map(|export| export.copy.as_ref())
            .map(|copy| copy.generation)
            .max();
        let source = exports
            .iter()
            .find(|export| export.node_id == *plan.copy_from())
            .and_then(|export| export.copy.as_ref())
            .expect("the plan's copy is exported");
        assert_eq!(Some(source.generation), newest);
        for key in plan.registers().keys() {
            assert!(
                key.starts_with(&KeyPrefix::buckets())
                    || key.starts_with(&KeyPrefix::identity())
                    || key.starts_with(&KeyPrefix::shards()),
                "{key}"
            );
        }
        let shards = plan
            .registers()
            .keys()
            .filter(|key| key.starts_with(&KeyPrefix::shards()))
            .count();
        assert_eq!(plan.shard_configs().count(), shards);
        let document = ClusterDocument::from_json(plan.cluster_json()).expect("a valid cluster.json");
        assert_eq!(document.generation, plan.generation());
        for export in &exports {
            if let Some(copy) = &export.copy {
                assert!(copy.generation < plan.generation());
            }
        }
        for note in plan.notes() {
            let _ = note.to_string();
        }
    }
});
