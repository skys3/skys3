//! Crash consistency (plan M1-14): a node's power fails at every sync
//! boundary of a concurrent PUT, DELETE, and multipart workload, one
//! boundary per run, and the history must pass both checkers.
//!
//! A first run of each seed counts the node's syncs, from its first start
//! to the end of the workload. Each further run replays the seed and cuts
//! the power of both of the node's disks at one sync, before it takes
//! effect or after, and the node restarts. One bucket is `write_back`, so
//! every acknowledged write there must also be flushed or still dirty.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_cluster_sim::{
    BoxError, Cluster, ClusterConfig, FaultPlan, FaultRates, LocalServices, NodeEnv, NodeServices,
    Report, RunError, SyncCut, Workload,
};
use skys3_gateway::{
    ConditionFailed, LocalShards, Precondition, ReadId, ReadPlan, Registered, ShardError, ShardRef,
    ShardSummary, Shards, UploadParts,
};
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::SimMount;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_sim::check::Violation;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};
use skys3_types::{BucketDocument, EpochSeq, NodeId};
use tokio::sync::RwLock;

/// What one seed of the sync-boundary scenario costs, in seeds of a typical
/// scenario: it runs the workload twice per sync boundary.
const COST: u64 = 64;

/// One node with two disks, and a `local` and a `write_back` bucket.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 1,
        disks_per_node: 2,
        buckets: 2,
        write_back_buckets: 1,
        shards_per_bucket: 2,
        control_rates: FaultRates::default(),
        ..ClusterConfig::default()
    }
}

/// A short workload on few keys, longer at a larger scale. It lasts long
/// enough for the flusher to start and send some writes.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 3,
        operations: 8 * context.scale() as usize,
        keys: 3,
        think_time: Duration::from_millis(150),
        ..Workload::default()
    }
}

/// Runs the seed of `context` once without a power loss, then once per
/// sync boundary of that run with the power cut there, and returns the
/// first run's report. `services` run on the node.
fn every_sync_boundary<S: NodeServices>(
    context: &SimContext,
    services: &S,
) -> Result<Report, RunError> {
    let (seed, scale) = (context.seed(), context.scale());
    let fresh = || SimContext::with_scale(seed, scale);
    let (config, workload) = (config(), workload(context));
    let cluster = || Cluster::with_services(config.clone(), services.clone());
    let base = cluster().run(&mut fresh(), &workload, &FaultPlan::none())?;
    let syncs = base.syncs[0];
    for sync in 0..syncs {
        for cut in [SyncCut::Before, SyncCut::After] {
            let at = format!("power loss {cut:?} sync {sync} of {syncs}");
            let report = cluster()
                .power_loss_at_sync(0, sync, cut)
                .run(&mut fresh(), &workload, &FaultPlan::none())
                .map_err(|error| match error {
                    RunError::Check(violation) => RunError::Check(Violation {
                        reason: format!("{at}: {}", violation.reason),
                        ..violation
                    }),
                    error => RunError::Simulation(format!("{at}: {error}")),
                })?;
            assert_eq!(report.power_cuts, 1, "{at}");
            assert!(report.lives > base.lives, "{at}");
        }
    }
    Ok(base)
}

#[test]
fn power_losses_at_every_sync_boundary_lose_no_acknowledged_write() {
    // Seeds that ran, that flushed something, and that completed a
    // multipart upload.
    let (mut runs, mut flushed, mut multipart) = (0, 0, 0);
    Runner::with_cost(1, COST).run(|context| {
        let base = every_sync_boundary(context, &LocalServices)?;
        assert!(base.syncs[0] > 10, "only {} syncs", base.syncs[0]);
        assert!(base.count(|o| *o == Outcome::Done) > 0);
        runs += 1;
        flushed += usize::from(base.flushed > 0);
        multipart += usize::from(base.history.iter().any(|op| {
            op.outcome == Outcome::Done && op.written().is_some_and(|v| v.ends_with("-1"))
        }));
        Ok(())
    });
    if runs >= 4 {
        assert!(
            flushed > 0 && multipart > 0,
            "of {runs} seeds, {flushed} flushed and {multipart} completed a multipart upload"
        );
    }
}

/// Shards with a seeded bug that only a crash reveals: an unconditional
/// `PUT` is acknowledged as soon as it is handed to the shard, before its
/// record is durable. Every other request first waits until those writes
/// are done, so without a crash nothing looks wrong.
#[derive(Debug, Clone)]
struct EagerShards {
    inner: LocalShards<SimMount>,
    eager: Arc<RwLock<()>>,
}

impl EagerShards {
    /// Waits until every eagerly acknowledged write is done.
    async fn settle(&self) {
        drop(self.eager.write().await);
    }
}

impl Shards for EagerShards {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        self.inner.open(shard, bucket).await
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.settle().await;
        self.inner.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.inner.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.inner.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        self.settle().await;
        self.inner.entry(shard, key).await
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        self.settle().await;
        self.inner.list(shard, query).await
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<UploadParts>, ShardError> {
        self.inner.upload(shard, key, upload, after, limit).await
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        self.inner.uploads(shard, prefix, after, limit).await
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        self.inner.parts(shard, upload, after, limit).await
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        self.settle().await;
        self.inner.payload(shard, position).await
    }

    fn node(&self) -> Option<NodeId> {
        self.inner.node()
    }

    async fn plan(&self, shard: &ShardRef, key: &str) -> Result<ReadPlan, ShardError> {
        self.settle().await;
        self.inner.plan(shard, key).await
    }

    async fn register(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        key: &str,
        version: EpochSeq,
        layout: Vec<ExtentRef>,
    ) -> Result<Option<Registered>, ShardError> {
        self.inner
            .register(shard, holder, key, version, layout)
            .await
    }

    async fn renew(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<bool, ShardError> {
        self.inner.renew(shard, holder, read).await
    }

    async fn release(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<(), ShardError> {
        self.inner.release(shard, holder, read).await
    }

    async fn fetch(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
        position: EpochSeq,
    ) -> Result<Bytes, ShardError> {
        self.settle().await;
        self.inner.fetch(shard, holder, read, position).await
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        self.inner.append_extent(shard, extent).await
    }

    async fn announce(
        &self,
        shard: &ShardRef,
        body: skys3_gateway::StreamedBody,
    ) -> Result<(), ShardError> {
        self.inner.announce(shard, body).await
    }

    async fn flushed(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        wait: std::time::Duration,
    ) -> Result<skys3_gateway::FlushState, ShardError> {
        self.inner.flushed(shard, key, version, wait).await
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        if matches!(body, RecordBody::Put(_)) && condition == Precondition::None {
            let done = Arc::clone(&self.eager).read_owned().await;
            let (inner, shard) = (self.inner.clone(), shard.clone());
            tokio::spawn(async move {
                let _ = inner.write(&shard, body, condition).await;
                drop(done);
            });
            return Ok(Ok(EpochSeq::default()));
        }
        self.settle().await;
        self.inner.write(shard, body, condition).await
    }
}

#[derive(Debug, Clone, Default)]
struct Eager;

impl NodeServices for Eager {
    type Shards = EagerShards;

    async fn start(&self, env: NodeEnv) -> Result<EagerShards, BoxError> {
        Ok(EagerShards {
            inner: env.shards,
            eager: Arc::default(),
        })
    }
}

#[test]
fn acknowledging_before_the_sync_is_caught_at_a_sync_boundary() {
    Runner::with_cost(1, 4 * COST).run(|context| {
        // Without a crash the bug goes unnoticed...
        let workload = workload(context);
        Cluster::with_services(config(), Eager).run(
            &mut SimContext::with_scale(context.seed(), context.scale()),
            &workload,
            &FaultPlan::none(),
        )?;
        // ...and a power loss at some sync boundary loses an acknowledged
        // write.
        match every_sync_boundary(context, &Eager) {
            Err(RunError::Check(violation)) => {
                assert!(violation.reason.contains("power loss"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
