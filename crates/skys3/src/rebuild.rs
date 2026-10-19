//! The operator's rebuild of a lost control store (design §6.2, §6.9):
//! `skys3 control export` and `skys3 control rebuild`.
//!
//! The procedure, which never runs by itself:
//!
//! 1. Stop every node. A rebuild races nothing then: no node holds a
//!    proposal in memory that the rebuilt store could accept.
//! 2. On each node, `skys3 control export` recovers the logs into the
//!    index, as a start does, and writes what the node keeps
//!    ([`export`]): its copy of the bucket and identity registers, the
//!    configuration of each shard's newest `CONFIG` record, and which
//!    shards hold objects. The data directory's lock refuses a node that
//!    still runs.
//! 3. `skys3 control rebuild` merges the exports
//!    ([`RebuildPlan`]) and writes them into the empty store, `cluster.json`
//!    last ([`rebuild`]). `--dry-run` only shows the plan.
//! 4. Start the nodes. Each finds `cluster.json` at a newer generation
//!    than its copy and syncs, and each replica finds its register equal
//!    to its own configuration.
//!
//! This build runs every node alone over the file control store
//! (`[control_store] backend = "file"`), so it rebuilds only that store,
//! from the one node's export, and records the node as the store's owner.
//! The plan and its checks are the same for every backend; the cluster
//! simulation rebuilds a shared S3 control store with them.

use std::sync::Arc;

use skys3_config::{Config, ControlStoreBackend};
use skys3_control::{
    Applied, ControlExport, EXPORT_FORMAT, ExportedCopy, HeldShard, RebuildError, RebuildOptions,
    RebuildPlan, RetryPolicy,
};
use skys3_index::{Index, IndexError, ListQuery};
use skys3_io::BlockingPool;
use skys3_types::{ClusterId, NodeId};

use crate::control::{ControlCopy, OpenStoreError, StoreOwner, open_file_store};
use crate::node::{Offline, StartError};
use crate::sessions::SESSIONS_BUCKET_ID;
use crate::storage::on_pool;

/// Why a rebuild command did not complete.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CommandError {
    /// The node's storage could not be opened or read.
    #[error(transparent)]
    Start(#[from] StartError),
    /// The rebuild was refused, or the store failed.
    #[error(transparent)]
    Rebuild(#[from] RebuildError),
    /// The file control store could not be opened for the rebuild.
    #[error("the control store: {0}")]
    Store(#[from] OpenStoreError),
    /// The command does not apply to this node or configuration.
    #[error("{0}")]
    Refused(String),
}

/// What `index`, the recovered index of node `node` in `cluster`, whose
/// data directory has `instance_id`, keeps of the control state. The
/// internal sessions bucket, which no register names, is left out.
///
/// # Errors
///
/// [`StartError::Index`] if the index fails.
pub async fn export(
    index: &Arc<Index>,
    pool: &BlockingPool,
    cluster: &ClusterId,
    node: &NodeId,
    instance_id: &str,
) -> Result<ControlExport, StartError> {
    let index = Arc::clone(index);
    let (configs, held, copy) = on_pool(pool, move || {
        let copy = ControlCopy::load(&index)?;
        let reader = index.read()?;
        let internal = |bucket: &skys3_types::BucketId| bucket.as_str() == SESSIONS_BUCKET_ID;
        let configs: Vec<_> = reader
            .configs()?
            .into_values()
            .filter(|config| !internal(&config.bucket_id))
            .collect();
        let mut held = Vec::new();
        for shard in reader.applied_positions()?.into_keys() {
            if internal(&shard.bucket) {
                continue;
            }
            let first = ListQuery {
                max_items: 1,
                ..ListQuery::default()
            };
            let objects = !reader.list(&shard, &first)?.items.is_empty();
            held.push(HeldShard {
                bucket_id: shard.bucket,
                shard: shard.shard,
                objects,
            });
        }
        Ok::<_, IndexError>((configs, held, copy))
    })
    .await?;
    Ok(ControlExport {
        format: EXPORT_FORMAT,
        cluster_id: cluster.clone(),
        node_id: node.clone(),
        instance_id: instance_id.to_owned(),
        copy: copy.map(exported),
        configs,
        held,
    })
}

/// The copy as an export holds it. A register that is not text, which no
/// node writes, is left out.
fn exported(copy: ControlCopy) -> ExportedCopy {
    let registers = copy
        .registers
        .into_iter()
        .filter_map(
            |(key, value)| match String::from_utf8(value.value.to_vec()) {
                Ok(text) => Some((key.as_str().to_owned(), text)),
                Err(_) => {
                    tracing::warn!(%key, "leaving out a kept register that is not text");
                    None
                }
            },
        )
        .collect();
    ExportedCopy {
        generation: copy.generation,
        synced_at_ms: u64::try_from(copy.synced_at.as_millis()).unwrap_or(u64::MAX),
        registers,
    }
}

/// Exports the control state of the stopped node `config` describes, after
/// recovering its logs (`skys3 control export`).
///
/// This build serves every shard alone, and its control store holds no
/// shard registers, so the export holds no shard configurations: the
/// `CONFIG` record a lone replica keeps is its own, not a register's.
///
/// # Errors
///
/// [`CommandError::Start`] if the data directory holds no node, is in use,
/// or its storage cannot be recovered.
pub async fn export_data_dir(config: &Config) -> Result<ControlExport, CommandError> {
    let offline = Offline::open(config).await?;
    let exported = export(
        &offline.index,
        &offline.pool,
        &config.cluster().cluster_id,
        offline.data_dir.node_id(),
        offline.data_dir.instance_id(),
    )
    .await;
    offline.close().await;
    let mut exported = exported?;
    exported.configs.clear();
    Ok(exported)
}

/// What [`rebuild`] did.
#[derive(Debug)]
pub struct Rebuilt {
    /// The plan.
    pub plan: RebuildPlan,
    /// What was written, or `None` for a dry run.
    pub applied: Option<Applied>,
}

/// Plans the rebuild of the control store of `config`'s cluster from
/// `exports`, and unless `dry_run`, writes it (`skys3 control rebuild`).
///
/// This build rebuilds only the file control store, which serves one node:
/// `exports` must be that node's, the node must be stopped, and its data
/// directory must be the one the export was taken from. The store is
/// claimed for the node, as its first start claims it.
///
/// # Errors
///
/// [`CommandError::Rebuild`] if the plan or the store refuses the
/// rebuild, and [`CommandError::Refused`] for another backend or a
/// mismatched export.
pub async fn rebuild(
    config: &Config,
    exports: &[ControlExport],
    options: &RebuildOptions,
    dry_run: bool,
) -> Result<Rebuilt, CommandError> {
    let plan = RebuildPlan::new(&config.cluster().cluster_id, exports, options)?;
    if dry_run {
        return Ok(Rebuilt {
            plan,
            applied: None,
        });
    }
    let ControlStoreBackend::File { directory } = &config.control_store().backend else {
        return Err(CommandError::Refused(
            "this build rebuilds only the file control store, the only one its nodes run with"
                .to_owned(),
        ));
    };
    let [export] = exports else {
        return Err(CommandError::Refused(format!(
            "the file control store serves one node, but {} exports were given",
            exports.len()
        )));
    };
    // The node is stopped while its data directory is open here, and the
    // export must come from this data directory.
    let offline = Offline::open(config).await?;
    let (node, instance) = (offline.data_dir.node_id(), offline.data_dir.instance_id());
    let applied = if export.node_id != *node || export.instance_id != instance {
        Err(CommandError::Refused(format!(
            "the export is of node {} (data directory instance {}), not of this node {node} \
             (instance {instance})",
            export.node_id, export.instance_id
        )))
    } else {
        let owner = StoreOwner::new(
            config.cluster().cluster_id.clone(),
            node.clone(),
            instance.to_owned(),
        );
        apply_file(&plan, directory, &owner, &offline.pool).await
    };
    offline.close().await;
    Ok(Rebuilt {
        plan,
        applied: Some(applied?),
    })
}

/// Claims the file control store in `directory` for `owner`, and writes
/// `plan` into it.
async fn apply_file(
    plan: &RebuildPlan,
    directory: &std::path::Path,
    owner: &StoreOwner,
    pool: &BlockingPool,
) -> Result<Applied, CommandError> {
    let opened = open_file_store(directory, owner, false, pool).await?;
    Ok(plan.apply(&opened.store, &RetryPolicy::default()).await?)
}

/// A summary of `plan` for the operator, one line each.
#[must_use]
pub fn summary(plan: &RebuildPlan) -> Vec<String> {
    let count = |prefix: &str| {
        plan.registers()
            .keys()
            .filter(|key| key.as_str().starts_with(prefix))
            .count()
    };
    let mut lines = vec![
        format!(
            "cluster {} at generation {}, with the bucket and identity registers of {}'s copy",
            plan.cluster_id(),
            plan.generation(),
            plan.copy_from()
        ),
        format!(
            "{} bucket, {} identity, and {} shard registers, then cluster.json",
            count("buckets/"),
            count("identity/"),
            count("shards/")
        ),
    ];
    lines.extend(plan.notes().iter().map(|note| format!("note: {note}")));
    lines
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use bytes::Bytes;
    use skys3_control::{RegisterKey, Version, Versioned};
    use skys3_index::IndexConfig;
    use skys3_io::SimDisk;
    use skys3_types::Generation;

    use super::*;

    #[tokio::test]
    async fn an_export_holds_the_copy_and_leaves_out_what_is_not_text() {
        let disk = SimDisk::new(9);
        let index = Arc::new(
            Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap(),
        );
        let pool = BlockingPool::inline("index");
        let cluster = ClusterId::new("c").unwrap();
        let node = NodeId::new("node-1").unwrap();
        let empty = export(&index, &pool, &cluster, &node, "i-1").await.unwrap();
        assert_eq!(empty.copy, None);
        assert!(empty.configs.is_empty() && empty.held.is_empty());

        let versioned = |value: &'static [u8]| Versioned {
            value: Bytes::from_static(value),
            version: Version::new("v"),
        };
        let copy = ControlCopy {
            generation: Generation::new(4),
            synced_at: Duration::from_millis(1500),
            registers: BTreeMap::from([
                (
                    RegisterKey::new("buckets/a.json").unwrap(),
                    versioned(b"{}"),
                ),
                (RegisterKey::new("identity/x").unwrap(), versioned(b"\xff")),
            ]),
        };
        copy.save(&index).unwrap();
        let exported = export(&index, &pool, &cluster, &node, "i-1").await.unwrap();
        let kept = exported.copy.unwrap();
        assert_eq!(kept.generation, Generation::new(4));
        assert_eq!(kept.synced_at_ms, 1500);
        assert_eq!(
            kept.registers,
            BTreeMap::from([("buckets/a.json".to_owned(), "{}".to_owned())])
        );
        assert_eq!(exported.instance_id, "i-1");
        assert_eq!(exported.format, EXPORT_FORMAT);
    }
}
