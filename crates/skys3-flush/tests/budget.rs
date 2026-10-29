//! The dirty-data budget end to end (design §7.6), through the gateway,
//! real shards on a simulated disk, and the flush service:
//!
//! - a simulated remote outage fills a bucket's budget, the gateway answers
//!   further writes with `503 SlowDown`, and writes resume as the flush
//!   drains once the remote is back;
//! - seeded scenarios with a faulty remote, outages, and flusher restarts
//!   check that the budget counts exactly what the flushers hold, that a
//!   write is refused exactly when the budget is used up, and that it all
//!   drains once the remote heals.

mod support;

use std::sync::Arc;
use std::time::Duration;

use http::{Method, Request, StatusCode};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
use skys3_flush::{DirtyBudget, Exhausted, FlushMetrics, FlushService, ProbeStatus, Usage};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{
    Admission, Gateway, GatewayConfig, IdSource, MODE_HEADER, Refusal, ShardRef, TARGET_HEADER,
    TrustAll,
};
use skys3_index::EntryState;
use skys3_io::SimMount;
use skys3_obs::MetricsRegistry;
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_sim::{Runner, SimContext, SimS3};
use skys3_types::{BucketDocument, RemoteTarget};
use support::{Patience, cluster, runtime, settings};

/// Admission by the dirty budget alone, as the node checks it.
#[derive(Debug)]
struct BudgetAdmission(Arc<DirtyBudget>);

impl Admission for BudgetAdmission {
    fn admit(&self, bucket: &BucketDocument, _shard: &ShardRef) -> Result<(), Refusal> {
        self.0
            .check(&bucket.bucket_id)
            .map_err(|exhausted| match exhausted {
                Exhausted::Bucket => Refusal::BucketBudget,
                Exhausted::Cluster => Refusal::ClusterBudget,
            })
    }
}

struct World {
    gateway: Gateway<TrustAll>,
    shards: MemoryShards,
    service: FlushService<SimS3, SimMount>,
    budget: Arc<DirtyBudget>,
    store: SimS3,
}

impl World {
    /// A gateway with the `write_back` bucket `photos`, of `shards` shards
    /// and a budget of `budget` bytes, flushed to `store`.
    async fn new(store: SimS3, budget: u64, shards: u32) -> World {
        let config: Config = format!(
            "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.photos]\nmax_dirty_bytes = {budget}\nshards_per_bucket = {shards}"
        )
        .parse()
        .unwrap();
        let budget = Arc::new(
            DirtyBudget::new(config.flush().max_dirty_bytes).with_buckets(config.buckets().clone()),
        );
        let mut gateway_config = GatewayConfig::new(&config);
        gateway_config.admission = Arc::new(BudgetAdmission(Arc::clone(&budget)));
        let control = MemoryControlStore::new();
        let ids = ProposalIds::seeded(1).next_id();
        bootstrap(&control, &cluster(), ids, &RetryPolicy::default())
            .await
            .unwrap();
        let shards = MemoryShards::new().await;
        let gateway = Gateway::new(
            gateway_config,
            control,
            shards.clone(),
            IdSource::seeded(3),
            TrustAll,
        )
        .await
        .unwrap();
        let connect = {
            let store = store.clone();
            move |target: &RemoteTarget| {
                assert_eq!(target.bucket, "remote");
                store.clone()
            }
        };
        let registry = MetricsRegistry::new();
        let service = FlushService::new(
            cluster(),
            settings(),
            Box::new(connect),
            FlushMetrics::register(&registry),
        )
        .with_budget(Arc::clone(&budget));
        let world = World {
            gateway,
            shards,
            service,
            budget,
            store,
        };
        let target = "https://s3.example/remote/team/";
        let mode = [(MODE_HEADER, "write_back"), (TARGET_HEADER, target)];
        let request = request(Method::PUT, "/photos", &mode, 0);
        assert_eq!(world.gateway.handle(request).await.status(), 200);
        world.reconcile().await;
        world
    }

    /// PUTs `len` bytes as `key`.
    async fn put(&self, key: &str, len: usize) -> StatusCode {
        let request = request(Method::PUT, &format!("/photos/{key}"), &[], len);
        self.gateway.handle(request).await.status()
    }

    async fn call(&self, method: Method, uri: &str) -> StatusCode {
        let request = request(method, uri, &[], 0);
        self.gateway.handle(request).await.status()
    }

    fn bucket(&self) -> BucketDocument {
        self.gateway.buckets().pop().unwrap()
    }

    async fn reconcile(&self) {
        let buckets = self.gateway.buckets();
        self.service
            .reconcile(&buckets, self.shards.local().set())
            .await;
    }

    fn usage(&self) -> Usage {
        self.budget.usage(&self.bucket().bucket_id).unwrap()
    }

    /// The dirty bytes the flushers hold, by their status.
    fn held(&self) -> u64 {
        let status = self.service.status(&self.bucket().bucket_id).unwrap();
        status.shards.iter().map(|(_, s)| s.dirty_bytes).sum()
    }

    /// Entries of the bucket that are not clean, by the index.
    async fn unclean(&self) -> usize {
        let mut unclean = 0;
        for shard in ShardRef::all(&self.bucket()) {
            let shard = self.shards.local().set().get(&(&shard).into()).await;
            let entries = shard.unwrap().entries(None, usize::MAX).await.unwrap();
            unclean += entries
                .iter()
                .filter(|(_, e)| !matches!(e.state, EntryState::Clean | EntryState::Evicted))
                .count();
        }
        unclean
    }

    /// Waits, letting the flushers run, until `done` holds.
    async fn until(&self, what: &str, mut done: impl FnMut(&World) -> bool) {
        let patience = Patience::new();
        while !done(self) {
            let status = self.service.status(&self.bucket().bucket_id);
            assert!(
                !patience.is_exhausted(),
                "gave up waiting until {what}: {status:?}, {:?}",
                self.usage()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// A request with `len` bytes of body.
fn request(method: Method, uri: &str, headers: &[(&str, &str)], len: usize) -> Request<s3s::Body> {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let body = bytes::Bytes::from(vec![b'x'; len]);
    request.body(s3s::Body::from(body)).unwrap()
}

#[test]
fn an_outage_fills_the_budget_and_writes_resume_as_the_flush_drains() {
    // Two 400-byte objects fit; a third fills the budget.
    const BUDGET: u64 = 1000;
    const BODY: usize = 400;
    runtime().block_on(async {
        let world = World::new(SimS3::new(30, SimS3Config::default()), BUDGET, 2).await;
        assert_eq!(
            world.usage(),
            Usage {
                dirty: 0,
                share: BUDGET
            }
        );

        // A healthy remote keeps the dirty set empty.
        assert_eq!(world.put("first", BODY).await, 200);
        world
            .until("the first write is flushed", |world| {
                world.store.object("team/first").is_some() && world.usage().dirty == 0
            })
            .await;
        let status = world.service.status(&world.bucket().bucket_id).unwrap();
        assert!(matches!(status.probe, ProbeStatus::Done { .. }));

        // The remote goes down: writes are admitted until the dirty bytes
        // reach the budget, and then refused.
        world.store.set_faults(SimS3Faults::OUTAGE);
        let mut admitted = 0;
        let refused = loop {
            let status = world.put(&format!("k{admitted}"), BODY).await;
            if status != 200 {
                break status;
            }
            admitted += 1;
            assert!(admitted <= 3, "the budget never filled");
            let dirty = (admitted * BODY) as u64;
            world
                .until("the write is counted", |world| world.usage().dirty == dirty)
                .await;
        };
        assert_eq!(refused, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(admitted, 3, "two writes fit; the third fills the budget");
        assert!(world.usage().dirty >= BUDGET);
        // Every write is refused, multipart uploads included, but deletes
        // pass and shrink the dirty set.
        assert_eq!(world.put("k0", BODY).await, 503);
        assert_eq!(world.call(Method::POST, "/photos/m?uploads").await, 503);
        assert_eq!(world.call(Method::DELETE, "/photos/k2").await, 204);
        world
            .until("the delete is counted", |world| {
                world.usage().dirty == 2 * BODY as u64
            })
            .await;
        assert_eq!(world.put("k2", BODY).await, 200);
        assert_eq!(world.put("k3", BODY).await, 503);
        assert!(world.store.object("team/k0").is_none(), "nothing flushed");
        assert_eq!(world.held(), world.usage().dirty);
        assert_eq!(world.budget.cluster_usage().dirty, world.usage().dirty);

        // The remote is back: the flush drains the dirty set, and writes
        // are admitted again.
        world.store.set_faults(SimS3Faults::NONE);
        world
            .until("the flush drains", |world| world.usage().dirty == 0)
            .await;
        for key in ["k0", "k1", "k2"] {
            assert!(world.store.object(&format!("team/{key}")).is_some());
        }
        assert_eq!(world.put("k3", BODY).await, 200);
        world
            .until("the last write is flushed", |world| {
                world.store.object("team/k3").is_some()
            })
            .await;
        world.service.shutdown().await;
        assert_eq!(world.budget.cluster_usage().dirty, 0);
    });
}

/// Delays, errors, and lost requests and responses on every request.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(20),
    internal_error_probability: 0.05,
    slow_down_probability: 0.05,
    lost_request_probability: 0.03,
    lost_response_probability: 0.05,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

#[test]
fn the_budget_tracks_the_dirty_set_through_faults() {
    Runner::with_cost(4, 2).run(scenario);
}

/// A scenario's result: an error names the check that failed.
type Outcome = Result<(), Box<dyn std::error::Error>>;

/// One seed: random PUTs and DELETEs through the gateway while the remote
/// fails now and then, goes down and comes back, and the flushers restart.
fn scenario(context: &mut SimContext) -> Outcome {
    let seed = context.fork_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let store = context.s3(SimS3Config::default());
    store.set_faults(FAULTS);
    let budget = rng.random_range(500..3000);
    let shards = rng.random_range(1..=4);
    runtime().block_on(async move {
        let world = World::new(store, budget, shards).await;
        let mut down = false;
        for op in 0..rng.random_range(40..100) {
            let key = format!("key-{}", rng.random_range(0..8));
            match rng.random_range(0..100) {
                0..=59 => {
                    // Admission decides on the budget as it stands when
                    // the request arrives.
                    let expected = match world.budget.check(&world.bucket().bucket_id) {
                        Ok(()) => StatusCode::OK,
                        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
                    };
                    let status = world.put(&key, rng.random_range(1..600)).await;
                    if status != expected {
                        return Err(format!("op {op}: a PUT got {status}, not {expected}").into());
                    }
                }
                60..=74 => {
                    let status = world.call(Method::DELETE, &format!("/photos/{key}")).await;
                    if status != StatusCode::NO_CONTENT {
                        return Err(format!("op {op}: a DELETE got {status}").into());
                    }
                }
                75..=84 => {
                    down = !down;
                    let faults = if down { SimS3Faults::OUTAGE } else { FAULTS };
                    world.store.set_faults(faults);
                }
                // The flushers stop and start again, as on a restart: what
                // they held is released, and counted again by the scan.
                85..=89 => {
                    let set = world.shards.local().set();
                    world.service.reconcile(&[], set).await;
                    if world.budget.cluster_usage().dirty != 0 {
                        return Err(format!("op {op}: stopped flushers still count").into());
                    }
                    world.reconcile().await;
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(rng.random_range(1..20))).await;
            // The budget counts exactly what the flushers hold.
            let (usage, held) = (world.usage(), world.held());
            if usage.dirty != held || world.budget.cluster_usage().dirty != held {
                let error = format!("op {op}: the budget counts {usage:?}, flushers hold {held}");
                return Err(error.into());
            }
        }
        // Once the remote heals, everything drains and writes are admitted.
        world.store.set_faults(SimS3Faults::NONE);
        let patience = Patience::new();
        while world.usage().dirty > 0 || world.unclean().await > 0 {
            if patience.is_exhausted() {
                return Err(format!("the dirty set never drained: {:?}", world.usage()).into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if world.put("last", 10).await != StatusCode::OK {
            return Err("a write was refused after the drain".into());
        }
        world.service.shutdown().await;
        Ok(())
    })
}
