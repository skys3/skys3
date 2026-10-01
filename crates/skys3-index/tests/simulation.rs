//! Simulation scenarios for the index, run by CI's simulation job with a
//! larger seed set.
//!
//! Each seed runs a workload over several restarts of one node. Between
//! restarts the node appends and applies records, takes checkpoints, and
//! changes its control-state copy, and then loses power (the simulated disk
//! keeps only synced bytes, plus torn writes) or has its process killed
//! (the disk keeps everything written). After each restart:
//!
//! - the index is exactly what it was at its last durable commit: redb
//!   reverted to its checkpoint,
//! - replaying the log reproduces the index exactly as it was before the
//!   crash, and
//! - replay reads nothing of a segment a checkpoint released, and every
//!   released segment holds only records behind that checkpoint.

mod support;

use std::sync::Arc;

use rand::Rng;
use skys3_index::{Checkpoint, Checkpointer, IndexDump};
use skys3_io::{SimDiskFaults, SimMount};
use skys3_log::SegmentId;
use skys3_sim::{Runner, SimContext};
use support::{TestApplier, Workload, apply, disk_label, log_of, open_node, pool, runtime};

/// Torn writes on half of the crashes, for segments and for the index file.
const TORN: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.0,
    torn_write_probability: 0.5,
    capacity: None,
};

/// Checks that every record in each released segment is behind
/// `checkpoint`.
async fn check_released(node: &Checkpointer<SimMount>, checkpoint: &Checkpoint) {
    let log = log_of(node);
    for segment in log.released() {
        let mut scanner = log.scan(segment).unwrap();
        while let Some(record) = scanner.next().await.unwrap() {
            let applied = checkpoint.applied.get(&record.header.shard).copied();
            assert!(
                applied.is_some_and(|applied| record.header.position <= applied),
                "released segment {segment} holds {} at {}, past the checkpoint",
                record.header.shard,
                record.header.position,
            );
        }
    }
}

/// Takes a checkpoint, checks what it released, and records the durable
/// index and the released segments.
async fn checkpoint(
    node: &Checkpointer<SimMount>,
    durable: &mut IndexDump,
    released: &mut Vec<SegmentId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let checkpoint = node.checkpoint().await?;
    check_released(node, &checkpoint).await;
    *durable = node.index().read()?.dump()?;
    *released = log_of(node).released().into_iter().collect();
    Ok(())
}

/// Runs one seed, and returns the index as the last restart left it.
fn scenario(context: &mut SimContext) -> Result<IndexDump, Box<dyn std::error::Error>> {
    let disk = context.disk_with_faults(TORN);
    let mut workload = Workload::new(context.fork_seed(), 3);
    let restarts = context.rng().random_range(2..=4);
    let pool = pool();
    runtime().block_on(async {
        // The index at its last durable commit, and just before the crash.
        let mut durable = IndexDump::default();
        let mut before_crash = IndexDump::default();
        let mut released: Vec<SegmentId> = Vec::new();
        for _ in 0..restarts {
            let mount = disk.mount();
            let node = open_node(&mount, &pool).await;
            let index = Arc::clone(node.index());
            assert_eq!(
                index.read()?.dump()?,
                durable,
                "redb reverted to its checkpoint"
            );

            let report = node.replay(Arc::new(TestApplier)).await?;
            assert_eq!(
                index.read()?.dump()?,
                before_crash,
                "replay reproduced the index"
            );
            for segment in &released {
                assert!(
                    !report.scanned.contains_key(&(disk_label(), *segment)),
                    "replay read released segment {segment}"
                );
            }

            let steps = workload.rng().random_range(10..60);
            for _ in 0..steps {
                match workload.rng().random_range(0..20) {
                    0 | 1 => {
                        checkpoint(&node, &mut durable, &mut released).await?;
                    }
                    2 => {
                        workload.control_update(&index);
                        durable = index.read()?.dump()?;
                    }
                    3 | 4 => {
                        // A checkpoint while acknowledged records still wait
                        // to be applied: it must not release their segments,
                        // and replay must not skip them.
                        let records = workload.append(&node).await;
                        checkpoint(&node, &mut durable, &mut released).await?;
                        apply(&node, &records);
                    }
                    _ => workload.step(&node).await,
                }
            }
            before_crash = index.read()?.dump()?;

            if workload.rng().random_bool(0.7) {
                // Power loss: the handles fail from here on, so dropping the
                // index cannot make anything durable.
                disk.crash();
                drop(node);
                drop(index);
            } else {
                // The process is killed: nothing runs its shutdown, and the
                // disk keeps every byte written.
                drop(node);
                std::mem::forget(index);
            }
        }
        Ok(before_crash)
    })
}

#[test]
fn replay_after_a_crash_reproduces_the_index() {
    Runner::new().run(|context| scenario(context).map(drop));
}

#[test]
fn a_seed_replays_exactly() {
    let run = |seed| scenario(&mut SimContext::new(seed)).unwrap();
    let first = run(3);
    assert!(!first.entries.is_empty());
    assert_eq!(run(3), first);
}
