//! The harness itself: replay, checkers that catch a seeded bug,
//! invariants, and node services that use the transport.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_cluster_sim::{
    BoxError, Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, FaultProfile, LocalServices,
    NodeEnv, NodeServices, RunError, TRANSPORT_PORT, View, Workload,
};
use skys3_gateway::{
    ConditionFailed, LocalShards, Precondition, ReadId, ReadPlan, Registered, ShardError, ShardRef,
    ShardSummary, Shards, UploadParts,
};
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::SimMount;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_net::{Frame, Header, MessageKind};
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;
use skys3_types::{BucketDocument, EpochSeq, NodeId};

fn small() -> (ClusterConfig, Workload) {
    let config = ClusterConfig {
        nodes: 2,
        buckets: 1,
        shards_per_bucket: 2,
        ..ClusterConfig::default()
    };
    let workload = Workload {
        clients: 2,
        operations: 15,
        ..Workload::default()
    };
    (config, workload)
}

#[test]
fn a_seed_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let (config, workload) = small();
        // A second bucket flushes to a remote store with faults, so the
        // flusher replays too.
        let config = ClusterConfig {
            buckets: 2,
            write_back_buckets: 1,
            remote_faults: SimS3Faults {
                max_delay: Duration::from_millis(20),
                internal_error_probability: 0.05,
                lost_response_probability: 0.05,
                ..SimS3Faults::default()
            },
            ..config
        };
        let plan = FaultPlan::random(context.rng(), &FaultProfile::default(), 2, 2, 2);
        Cluster::new(config)
            .run(&mut context, &workload, &plan)
            .unwrap()
    };
    Runner::with_cost(1, COST).run(|context| {
        assert_eq!(run(context.seed()), run(context.seed()));
        Ok(())
    });
}

/// Shards with a seeded bug: every unconditional `PUT` and multipart
/// completion is acknowledged without being made.
#[derive(Debug, Clone)]
struct LosingShards {
    inner: LocalShards<SimMount>,
}

impl Shards for LosingShards {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        self.inner.open(shard, bucket).await
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.inner.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.inner.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.inner.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        self.inner.entry(shard, key).await
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
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
        self.inner.payload(shard, position).await
    }

    fn node(&self) -> Option<NodeId> {
        self.inner.node()
    }

    async fn plan(&self, shard: &ShardRef, key: &str) -> Result<ReadPlan, ShardError> {
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
        let put = matches!(body, RecordBody::Put(_) | RecordBody::MpuComplete(_))
            && condition == Precondition::None;
        if put {
            return Ok(Ok(EpochSeq::default()));
        }
        self.inner.write(shard, body, condition).await
    }
}

#[derive(Debug, Clone, Default)]
struct Losing;

impl NodeServices for Losing {
    type Shards = LosingShards;

    async fn start(&self, env: NodeEnv) -> Result<LosingShards, BoxError> {
        Ok(LosingShards { inner: env.shards })
    }
}

#[test]
fn the_checkers_catch_acknowledged_writes_that_were_never_made() {
    Runner::with_cost(1, COST).run(|context| {
        let (config, workload) = small();
        let workload = Workload {
            operations: 40,
            ..workload
        };
        let outcome =
            Cluster::with_services(config, Losing).run(context, &workload, &FaultPlan::none());
        match outcome {
            Err(RunError::Check(violation)) => {
                assert!(!violation.operations.is_empty(), "{violation}");
                let message = RunError::Check(violation).to_string();
                assert!(message.starts_with("a checker failed: key "), "{message}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

#[test]
fn a_failed_sync_is_fenced_once() {
    Runner::with_cost(1, COST).run(|context| {
        let (config, workload) = small();
        let fail = Fault::FailSync { node: 0, disk: 0 };
        // The node crashes before its fallback fence is due: that crash is
        // its power-loss restart, and the fence leaves the next process
        // alone.
        let crash = Fault::Crash {
            node: 0,
            power_loss: false,
            downtime: Duration::from_millis(200),
        };
        let plan = FaultPlan::none()
            .with(Duration::from_millis(500), fail.clone())
            .with(Duration::from_millis(600), crash);
        let report = Cluster::new(config.clone()).run(context, &workload, &plan)?;
        assert_eq!(report.fences, 0);
        assert!(report.lives >= 3, "{}", report.lives);

        // Alone, the failed sync is fenced, unless the node stopped by
        // itself first and its supervisor restarted it.
        let plan = FaultPlan::none().with(Duration::from_millis(500), fail);
        let report = Cluster::new(config).run(context, &workload, &plan)?;
        let restarts = usize::try_from(report.lives - 2)?;
        assert!(report.fences <= 1 && restarts >= 1, "{report:?}");
        Ok(())
    });
}

#[test]
fn invariants_run_after_every_step() {
    Runner::with_cost(1, COST).run(|context| {
        let (config, workload) = small();
        let steps = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&steps);
        let plan = FaultPlan::none().with(
            Duration::from_millis(100),
            Fault::Crash {
                node: 1,
                power_loss: false,
                downtime: Duration::from_millis(500),
            },
        );
        let mut saw_down = false;
        let report = Cluster::new(config.clone())
            .invariant(move |view: &View<'_, LocalServices>| {
                counted.fetch_add(1, Ordering::SeqCst);
                saw_down |= !view.up[1];
                if view.elapsed > Duration::from_secs(30) && !saw_down {
                    return Err("node 2 never went down".to_owned());
                }
                Ok(())
            })
            .run(context, &workload, &plan)?;
        assert!(steps.load(Ordering::SeqCst) > 100);
        assert_eq!(report.faults, 1);

        // A failing invariant fails the run.
        let failed = Cluster::new(config)
            .invariant(|view: &View<'_, LocalServices>| {
                if view.history.len() > 5 {
                    Err("enough".to_owned())
                } else {
                    Ok(())
                }
            })
            .run(context, &workload, &FaultPlan::none())
            .unwrap_err();
        assert_eq!(
            failed.to_string(),
            "the simulation failed: an invariant failed: enough"
        );
        Ok(())
    });
}

/// Who heard a beacon from whom, by node.
type Heard = Arc<Mutex<BTreeMap<NodeId, BTreeSet<NodeId>>>>;

/// Services that exchange beacons with every peer over the transport,
/// as replication will, and keep the node's shards local.
#[derive(Debug, Clone, Default)]
struct Beacons {
    heard: Heard,
}

impl NodeServices for Beacons {
    type Shards = LocalShards<SimMount>;

    async fn start(&self, env: NodeEnv) -> Result<Self::Shards, BoxError> {
        let addr = (std::net::Ipv4Addr::UNSPECIFIED, TRANSPORT_PORT).into();
        let listener = env.transport.bind(addr).await?;
        let heard = Arc::clone(&self.heard);
        let me = env.node.clone();
        tokio::spawn(async move {
            while let Ok(incoming) = listener.accept().await {
                let heard = Arc::clone(&heard);
                let me = me.clone();
                tokio::spawn(async move {
                    let Ok(mut connection) = incoming.handshake().await else {
                        return;
                    };
                    while let Ok(Some(frame)) = connection.recv().await {
                        if frame.header.kind == MessageKind::Beacon {
                            let peer = connection.peer().node_id().cloned();
                            let mut heard = heard.lock().unwrap();
                            heard.entry(me.clone()).or_default().extend(peer);
                        }
                    }
                });
            }
        });
        for (peer, address) in env.peers.clone() {
            if peer == env.node {
                continue;
            }
            let transport = env.transport.clone();
            tokio::spawn(async move {
                loop {
                    if let Ok(mut connection) = transport.connect(&peer, &address).await {
                        let beacon = Frame::new(Header::new(MessageKind::Beacon), "");
                        while connection.send(&beacon).await.is_ok() {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            });
        }
        Ok(env.shards)
    }
}

#[test]
fn services_reach_their_peers_over_the_transport() {
    Runner::with_cost(1, COST).run(|context| {
        let (config, workload) = small();
        let services = Beacons::default();
        let plan = FaultPlan::none().with(
            Duration::from_millis(50),
            Fault::Partition {
                a: Endpoint::Node(0),
                b: Endpoint::Node(1),
                duration: Duration::from_millis(500),
            },
        );
        Cluster::with_services(config, services.clone()).run(context, &workload, &plan)?;
        let heard = services.heard.lock().unwrap().clone();
        let node = |n: u32| NodeId::new(format!("node-{n}")).unwrap();
        assert_eq!(heard[&node(1)], BTreeSet::from([node(2)]));
        assert_eq!(heard[&node(2)], BTreeSet::from([node(1)]));
        Ok(())
    });
}
