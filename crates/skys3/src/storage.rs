//! Recovering the storage engine at startup (design §10.1, §10.2).
//!
//! [`recover`] is the part of startup that does not depend on where the
//! disks are: it recovers each disk's log, replays every record after its
//! shard's checkpoint into the index, and returns the node's shards. The
//! node runs it over its real disks; the cluster simulation runs the same
//! function over simulated ones.

use std::collections::BTreeMap;
use std::sync::Arc;

use skys3_gateway::LocalShards;
use skys3_index::{Checkpointer, Index};
use skys3_io::{BlockingPool, Clock, Disk};
use skys3_log::{LogConfig, SegmentLog};
use skys3_shard::{ShardSet, StateMachine};
use skys3_types::{Label, NodeId};

use crate::node::StartError;

/// A node's storage engine after recovery.
pub struct Storage<D: Disk> {
    /// Each disk's log, by label.
    pub logs: BTreeMap<Label, SegmentLog<D>>,
    /// The index, replayed up to the end of every log.
    pub index: Arc<Index>,
    /// Takes the index's checkpoints.
    pub checkpointer: Arc<Checkpointer<D>>,
    /// The node's shards. None is open yet.
    pub shards: LocalShards<D>,
}

impl<D: Disk> std::fmt::Debug for Storage<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("disks", &self.logs.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Recovers the log of each of `disks` (torn tails cut, damage refused),
/// replays every record after its shard's checkpoint into `index`, and
/// returns the shards of node `node` over them. Index work runs on `pool`.
///
/// # Errors
///
/// [`StartError::Recovery`] if a disk's log cannot be recovered, and
/// [`StartError::Index`] if replay fails.
pub async fn recover<D: Disk>(
    disks: Vec<(Label, D)>,
    config: LogConfig,
    clock: Arc<dyn Clock>,
    index: Arc<Index>,
    pool: BlockingPool,
    node: NodeId,
) -> Result<Storage<D>, StartError> {
    let mut logs = BTreeMap::new();
    for (label, disk) in disks {
        let (log, report) = SegmentLog::open(disk, config.clone(), Arc::clone(&clock))
            .await
            .map_err(|source| StartError::Recovery {
                disk: label.clone(),
                source,
            })?;
        tracing::info!(disk = %label, ?report, "recovered the log");
        logs.insert(label, log);
    }
    let checkpointer = Arc::new(Checkpointer::new(
        Arc::clone(&index),
        logs.clone(),
        pool.clone(),
    ));
    let replayed = checkpointer.replay(Arc::new(StateMachine)).await?;
    tracing::info!(records = replayed.applied, "replayed the logs");
    let set = ShardSet::with_disks(Arc::clone(&index), logs.clone(), pool);
    Ok(Storage {
        logs,
        index,
        checkpointer,
        shards: LocalShards::new(set, node),
    })
}

/// Runs blocking `job` on `pool`.
pub(crate) async fn on_pool<T, E>(
    pool: &BlockingPool,
    job: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, StartError>
where
    T: Send + 'static,
    E: Send + 'static,
    StartError: From<E>,
{
    match pool.run(job).await {
        Ok(result) => result.map_err(StartError::from),
        Err(closed) => Err(StartError::Io {
            what: format!("the {} pool", pool.name()),
            source: closed.into(),
        }),
    }
}
