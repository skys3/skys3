//! The conformance suite (plan M2-06) against every backend: the
//! in-memory store, also with polling change streams and with overlapping
//! requests; the file store; the S3 store over the simulated S3 store;
//! and, when their environment names one, a real etcd cluster and a real
//! S3 provider.
//!
//! - `SKYS3_ETCD_ENDPOINTS`: an etcd cluster's client URLs, separated by
//!   commas. CI's `etcd` job sets it.
//! - `SKYS3_CONTROL_S3_ENDPOINT` and `SKYS3_CONTROL_S3_BUCKET`: an S3
//!   provider's endpoint and a bucket to hold registers in, with
//!   `SKYS3_CONTROL_S3_REGION` (default `us-east-1`; `auto` for R2) and
//!   credentials from the AWS SDK's default chain, such as
//!   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The nightly
//!   workflow sets them for AWS S3 and for R2.
//!
//! Without them, those tests return at once and say they were skipped.
//! Every store on a real backend lives under a fresh prefix,
//! `skys3-test/<random>/` for etcd and `skys3-conformance/<random>/` (or
//! `SKYS3_CONTROL_S3_PREFIX`) for S3, so runs never see each other's
//! registers.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::conformance::{self, Backend, Scale};
use skys3_control::faults::{FaultRates, FaultyStore};
use skys3_control::{
    ChangeFeed, ChangeStream, ControlError, ControlStore, DeleteOutcome, EtcdControlStore,
    EtcdStoreConfig, Expected, FileControlStore, FileStoreConfig, KeyPrefix, MemoryControlStore,
    ProposalIds, PutOutcome, RegisterKey, RetryPolicy, S3ControlStore, S3StoreConfig, Version,
    Versioned, bootstrap, bump_generation,
};
use skys3_io::BlockingPool;
use skys3_remote::aws::{AwsS3, SharedCredentialsProvider, default_credentials};
use skys3_sim::SimS3;
use skys3_sim::s3::{ConditionalSupport, Conditionals, SimS3Config, SimS3Faults};
use skys3_types::{Generation, RemoteTarget};
use tempfile::TempDir;

/// The longest a run against a real backend may take, so a hung store
/// fails the run instead of stalling it.
const REAL_TIMEOUT: Duration = Duration::from_secs(1800);

async fn bounded<T>(test: impl Future<Output = T>) -> T {
    tokio::time::timeout(REAL_TIMEOUT, test)
        .await
        .expect("the run finished in time")
}

/// The value of environment variable `name`, if set and not empty.
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

struct Memory;

impl Backend for Memory {
    type Store = MemoryControlStore;

    async fn store(&mut self) -> MemoryControlStore {
        MemoryControlStore::new()
    }
}

#[tokio::test(start_paused = true)]
async fn the_memory_store_conforms() {
    conformance::run_at(&mut Memory, Scale::LARGE).await;
}

/// The in-memory store behind random delays before and after each
/// request, so that concurrent requests overlap, as on a remote store.
struct Overlapping {
    seed: u64,
}

impl Backend for Overlapping {
    type Store = FaultyStore<MemoryControlStore>;

    async fn store(&mut self) -> Self::Store {
        self.seed += 1;
        let delays = FaultRates {
            max_delay: Duration::from_millis(5),
            ..FaultRates::default()
        };
        FaultyStore::seeded(MemoryControlStore::new(), self.seed, delays)
    }
}

#[tokio::test(start_paused = true)]
async fn the_memory_store_conforms_with_overlapping_requests() {
    conformance::run_at(&mut Overlapping { seed: 0 }, Scale::LARGE).await;
}

/// The in-memory store with a change stream that polls, as the S3 backend
/// does.
#[derive(Debug, Clone)]
struct Polling(MemoryControlStore);

impl ControlStore for Polling {
    type Changes = ChangeFeed<MemoryControlStore>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        self.0.get(key).await
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        self.0.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        self.0.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.0.list(prefix).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        Ok(ChangeFeed::polling(
            self.0.clone(),
            after,
            Duration::from_secs(30),
        ))
    }
}

struct PollingBackend;

impl Backend for PollingBackend {
    type Store = Polling;

    async fn store(&mut self) -> Polling {
        Polling(MemoryControlStore::new())
    }
}

#[tokio::test(start_paused = true)]
async fn a_polling_change_stream_conforms() {
    conformance::run(&mut PollingBackend).await;
}

struct File {
    pool: BlockingPool,
    dirs: Vec<TempDir>,
}

impl Backend for File {
    type Store = FileControlStore;

    async fn store(&mut self) -> FileControlStore {
        let dir = tempfile::tempdir().unwrap();
        let config = FileStoreConfig {
            root: dir.path().join("control"),
            node: "node-1".parse().unwrap(),
        };
        self.dirs.push(dir);
        FileControlStore::open(config, self.pool.clone())
            .await
            .unwrap()
    }
}

// Real time: the file store's I/O runs on pool threads, and a paused clock
// would jump ahead while the runtime waits for them.
#[tokio::test]
async fn the_file_store_conforms() {
    let pool = BlockingPool::new("control", NonZeroUsize::new(2).unwrap()).unwrap();
    let mut backend = File {
        pool,
        dirs: Vec::new(),
    };
    conformance::run_at(&mut backend, Scale::SMALL).await;
}

#[tokio::test]
async fn the_file_store_keeps_generations_across_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BlockingPool::new("control", NonZeroUsize::MIN).unwrap();
    let config = FileStoreConfig {
        root: dir.path().to_path_buf(),
        node: "node-1".parse().unwrap(),
    };
    let cluster = "dev".parse().unwrap();
    let policy = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(0);
    let store = FileControlStore::open(config.clone(), pool.clone())
        .await
        .unwrap();
    bootstrap(&store, &cluster, ids.next_id(), &policy)
        .await
        .unwrap();
    bump_generation(&store, &cluster, &mut ids, &policy)
        .await
        .unwrap();
    drop(store);
    let store = FileControlStore::open(config, pool).await.unwrap();
    let mut changes = store.changes(Generation::ZERO).await.unwrap();
    let change = changes.next().await.unwrap();
    assert_eq!(change.generation, Generation::new(2));
    assert!(change.snapshot);
}

/// S3 control stores over fresh simulated buckets.
struct SimulatedS3 {
    config: SimS3Config,
    faults: SimS3Faults,
    /// Every bucket made so far.
    buckets: Vec<SimS3>,
}

impl SimulatedS3 {
    fn new(config: SimS3Config) -> Self {
        Self {
            config,
            faults: SimS3Faults::NONE,
            buckets: Vec::new(),
        }
    }

    /// The `409 ConditionalRequestConflict` answers of every bucket.
    fn conflicts(&self) -> u64 {
        self.buckets.iter().map(|b| b.stats().conflicts).sum()
    }
}

impl Backend for SimulatedS3 {
    type Store = S3ControlStore<SimS3>;

    async fn store(&mut self) -> S3ControlStore<SimS3> {
        let objects = SimS3::new(self.buckets.len() as u64 + 1, self.config.clone());
        objects.set_faults(self.faults.clone());
        self.buckets.push(objects.clone());
        let config = S3StoreConfig {
            prefix: "skys3-prod-a/".to_owned(),
            poll_interval: Duration::from_secs(30),
        };
        S3ControlStore::new(objects, config).unwrap()
    }
}

#[tokio::test(start_paused = true)]
async fn the_s3_store_conforms() {
    conformance::run_at(&mut SimulatedS3::new(SimS3Config::default()), Scale::LARGE).await;
}

#[tokio::test(start_paused = true)]
async fn the_s3_store_conforms_on_a_versioned_bucket() {
    let config = SimS3Config {
        versioning: true,
        ..SimS3Config::default()
    };
    conformance::run(&mut SimulatedS3::new(config)).await;
}

/// Delays let racing conditional writes overlap, so the store answers some
/// with `409 ConditionalRequestConflict`.
#[tokio::test(start_paused = true)]
async fn the_s3_store_conforms_with_racing_requests() {
    let mut backend = SimulatedS3 {
        faults: SimS3Faults {
            max_delay: Duration::from_millis(4),
            ..SimS3Faults::NONE
        },
        ..SimulatedS3::new(SimS3Config::default())
    };
    conformance::run_at(&mut backend, Scale::LARGE).await;
    assert!(backend.conflicts() > 0, "no request conflicted");
}

/// Handles to one simulated bucket that breaks the contract as `config`
/// and `faults` say.
async fn broken(config: SimS3Config, faults: SimS3Faults) -> Vec<S3ControlStore<SimS3>> {
    let mut backend = SimulatedS3 {
        faults: SimS3Faults {
            max_delay: Duration::from_millis(4),
            ..faults
        },
        ..SimulatedS3::new(config)
    };
    vec![backend.store().await; 6]
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "is not linearizable")]
async fn the_suite_catches_ignored_preconditions() {
    let config = SimS3Config {
        conditionals: Conditionals::all(ConditionalSupport::Ignored),
        ..SimS3Config::default()
    };
    let handles = broken(config, SimS3Faults::NONE).await;
    conformance::histories_are_linearizable(&handles, 32).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "is not linearizable")]
async fn the_suite_catches_stale_reads() {
    let faults = SimS3Faults {
        stale_read_probability: 0.2,
        ..SimS3Faults::NONE
    };
    let handles = broken(SimS3Config::default(), faults).await;
    conformance::histories_are_linearizable(&handles, 32).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "the probe of node")]
async fn the_suite_catches_stale_listings() {
    let faults = SimS3Faults {
        stale_list_probability: 0.2,
        ..SimS3Faults::NONE
    };
    let handles = broken(SimS3Config::default(), faults).await;
    conformance::probes_pass_repeatedly(&handles, 1, 20).await;
}

/// etcd stores under fresh prefixes, each handle on its own connection.
struct Etcd(Vec<String>);

impl Etcd {
    fn store_at(&self, prefix: &str) -> EtcdControlStore {
        EtcdControlStore::new(EtcdStoreConfig::new(self.0.clone(), prefix)).unwrap()
    }
}

impl Backend for Etcd {
    type Store = EtcdControlStore;

    async fn store(&mut self) -> EtcdControlStore {
        self.store_at(&format!("skys3-test/{:016x}/", rand::random::<u64>()))
    }

    async fn connect(&mut self, store: &EtcdControlStore) -> EtcdControlStore {
        self.store_at(store.prefix())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_etcd_store_conforms() {
    let endpoints: Vec<String> = env("SKYS3_ETCD_ENDPOINTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .collect();
    if endpoints.is_empty() {
        eprintln!("skipping the_etcd_store_conforms: SKYS3_ETCD_ENDPOINTS is not set");
        return;
    }
    bounded(conformance::run_at(&mut Etcd(endpoints), Scale::LARGE)).await;
}

/// S3 control stores in a real provider's bucket, under fresh prefixes,
/// each handle with its own client and connections.
struct Provider {
    target: RemoteTarget,
    region: String,
    credentials: SharedCredentialsProvider,
    base: String,
    /// Every store made, for the cleanup.
    stores: Arc<Mutex<Vec<S3ControlStore<AwsS3>>>>,
}

impl Provider {
    /// The provider the environment names, or `None` to skip.
    async fn from_env() -> Option<Self> {
        let (Some(endpoint), Some(bucket)) = (
            env("SKYS3_CONTROL_S3_ENDPOINT"),
            env("SKYS3_CONTROL_S3_BUCKET"),
        ) else {
            eprintln!(
                "skipping an_s3_provider_conforms: \
                 SKYS3_CONTROL_S3_ENDPOINT and SKYS3_CONTROL_S3_BUCKET are not set"
            );
            return None;
        };
        let region = env("SKYS3_CONTROL_S3_REGION").unwrap_or_else(|| "us-east-1".to_owned());
        let credentials = default_credentials(&region).await;
        Some(Self {
            target: RemoteTarget {
                endpoint,
                bucket,
                prefix: None,
            },
            region,
            credentials,
            base: env("SKYS3_CONTROL_S3_PREFIX").unwrap_or_else(|| "skys3-conformance/".to_owned()),
            stores: Arc::default(),
        })
    }

    fn store_with(&self, config: S3StoreConfig) -> S3ControlStore<AwsS3> {
        let objects = AwsS3::builder(&self.target, &self.region, self.credentials.clone())
            .attempt_timeout(Duration::from_secs(10))
            .build();
        S3ControlStore::new(objects, config).unwrap()
    }

    /// Deletes every register under the stores in `stores`, and says how
    /// many it could not.
    async fn clean_up(stores: &Mutex<Vec<S3ControlStore<AwsS3>>>) {
        let stores = stores.lock().unwrap().clone();
        let mut left = 0;
        for store in &stores {
            let Ok(registers) = store.list(&KeyPrefix::root()).await else {
                eprintln!("could not list {} to clean it up", store.config().prefix);
                continue;
            };
            for (key, version) in registers {
                if store.delete_if(&key, &version).await.is_err() {
                    left += 1;
                }
            }
        }
        if left > 0 {
            eprintln!("{left} registers were left behind");
        }
    }
}

impl Backend for Provider {
    type Store = S3ControlStore<AwsS3>;

    async fn store(&mut self) -> S3ControlStore<AwsS3> {
        let config = S3StoreConfig {
            prefix: format!("{}{:016x}/", self.base, rand::random::<u64>()),
            poll_interval: Duration::from_secs(1),
        };
        let store = self.store_with(config);
        self.stores.lock().unwrap().push(store.clone());
        store
    }

    async fn connect(&mut self, store: &S3ControlStore<AwsS3>) -> S3ControlStore<AwsS3> {
        self.store_with(store.config().clone())
    }
}

/// The nightly run against AWS S3 and R2. Requests are billed, so it runs
/// at the default scale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_s3_provider_conforms() {
    let Some(mut provider) = Provider::from_env().await else {
        return;
    };
    eprintln!(
        "an_s3_provider_conforms: {} bucket {} under {}",
        provider.target.endpoint, provider.target.bucket, provider.base
    );
    // Clean up after a failed run too: the run is a task of its own.
    let stores = provider.stores.clone();
    let run = tokio::spawn(bounded(async move {
        conformance::run_at(&mut provider, Scale::DEFAULT).await;
    }));
    let result = run.await;
    Provider::clean_up(&stores).await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}
