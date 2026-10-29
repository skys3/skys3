//! The etcd backend against a real etcd cluster: the conformance suite,
//! the startup probe, listings over several pages, and endpoint failover.
//!
//! Set `SKYS3_ETCD_ENDPOINTS` to the cluster's client URLs, separated by
//! commas, for example `http://127.0.0.1:2379`. CI's `etcd` job runs an
//! etcd container and sets it. Without it, each test returns at once and
//! says it was skipped. Every test uses registers under a fresh prefix,
//! `skys3-test/<random>/`, so runs never see each other's registers.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::conformance::{self, Backend};
use skys3_control::{
    ChangeStream, ControlProbe, ControlStore, EtcdControlStore, EtcdStoreConfig, Expected,
    KeyPrefix, ProposalIds, PutOutcome, RegisterKey, RetryPolicy, bootstrap, bump_generation,
};
use skys3_types::Generation;
use tokio::task::JoinSet;

/// The longest any test here may take, so a hung etcd fails the run
/// instead of stalling it.
const TEST_TIMEOUT: Duration = Duration::from_secs(300);

/// The endpoints from `SKYS3_ETCD_ENDPOINTS`, or `None` to skip.
fn endpoints(test: &str) -> Option<Vec<String>> {
    let endpoints: Vec<String> = std::env::var("SKYS3_ETCD_ENDPOINTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .collect();
    if endpoints.is_empty() {
        eprintln!("skipping {test}: SKYS3_ETCD_ENDPOINTS is not set");
        return None;
    }
    Some(endpoints)
}

/// A store over `endpoints` under a fresh prefix.
fn store(endpoints: &[String]) -> EtcdControlStore {
    let prefix = format!("skys3-test/{:016x}/", rand::random::<u64>());
    store_at(endpoints, &prefix)
}

fn store_at(endpoints: &[String], prefix: &str) -> EtcdControlStore {
    EtcdControlStore::new(EtcdStoreConfig::new(endpoints.to_vec(), prefix)).unwrap()
}

async fn bounded<T>(test: impl Future<Output = T>) -> T {
    tokio::time::timeout(TEST_TIMEOUT, test)
        .await
        .expect("the test finished in time")
}

struct Etcd(Vec<String>);

impl Backend for Etcd {
    type Store = EtcdControlStore;

    async fn store(&mut self) -> EtcdControlStore {
        store(&self.0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_etcd_store_conforms() {
    let Some(endpoints) = endpoints("the_etcd_store_conforms") else {
        return;
    };
    bounded(async {
        let mut backend = Etcd(endpoints);
        conformance::run(&mut backend).await;
        conformance::racing_creates_have_one_winner(&backend.store().await, 32).await;
        conformance::increments_are_linearizable(&backend.store().await, 8, 16).await;
    })
    .await;
}

/// Writers on separate connections, as racing nodes have.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_etcd_store_passes_the_probe() {
    let Some(endpoints) = endpoints("the_etcd_store_passes_the_probe") else {
        return;
    };
    bounded(async {
        let prefix = format!("skys3-test/{:016x}/", rand::random::<u64>());
        let writers: Vec<_> = (0..3).map(|_| store_at(&endpoints, &prefix)).collect();
        let probe = ControlProbe::with_fresh_nonce();
        probe.run(&writers).await.unwrap();
        let left = writers[0].list(&KeyPrefix::root()).await.unwrap();
        assert!(left.is_empty(), "the probe left {left:?}");
    })
    .await;
}

/// A listing longer than a page reads every page at one revision.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listings_span_pages() {
    let Some(endpoints) = endpoints("listings_span_pages") else {
        return;
    };
    bounded(async {
        let store = store(&endpoints);
        let count = usize::try_from(EtcdControlStore::LIST_PAGE).unwrap() * 2 + 5;
        let mut writes = JoinSet::new();
        for n in 0..count {
            let store = store.clone();
            writes.spawn(async move {
                let key = RegisterKey::new(format!("nodes/node-{n:05}.json")).unwrap();
                let value = Bytes::from(format!(r#"{{"proposal_id":"p{n}"}}"#));
                let written = store.put_if(&key, Expected::Absent, value).await.unwrap();
                let PutOutcome::Written(version) = written else {
                    panic!("creating {key} failed its precondition");
                };
                (key, version)
            });
            // Keep a bounded number of requests in flight.
            if writes.len() >= 64 {
                writes.join_next().await.unwrap().unwrap();
            }
        }
        writes.join_all().await;
        let other = RegisterKey::new("buckets/b.json").unwrap();
        let written = store
            .put_if(&other, Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        assert!(matches!(written, PutOutcome::Written(_)));

        let listed = store.list(&KeyPrefix::nodes()).await.unwrap();
        assert_eq!(listed.len(), count);
        let keys: Vec<_> = listed.iter().map(|(key, _)| key.clone()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        assert_eq!(
            store.list(&KeyPrefix::root()).await.unwrap().len(),
            count + 1
        );
        for (key, version) in listed.iter().step_by(97) {
            assert_eq!(&store.get(key).await.unwrap().unwrap().version, version);
        }
    })
    .await;
}

/// An endpoint that refuses connections is passed over.
#[tokio::test]
async fn an_unreachable_endpoint_is_passed_over() {
    let Some(endpoints) = endpoints("an_unreachable_endpoint_is_passed_over") else {
        return;
    };
    bounded(async {
        let mut with_dead = vec!["http://127.0.0.1:1".to_owned()];
        with_dead.extend(endpoints);
        let store = store(&with_dead);
        let cluster = "skys3-test".parse().unwrap();
        let policy = RetryPolicy::default();
        let mut ids = ProposalIds::from_os_rng();
        let first = bootstrap(&store, &cluster, ids.next_id(), &policy)
            .await
            .unwrap();
        assert_eq!(first.cluster().value.generation, Generation::new(1));
    })
    .await;
}

/// A change stream reports each increment through its watch, promptly.
#[tokio::test]
async fn the_watch_reports_increments_promptly() {
    let Some(endpoints) = endpoints("the_watch_reports_increments_promptly") else {
        return;
    };
    bounded(async {
        let store = store(&endpoints);
        let cluster = "skys3-test".parse().unwrap();
        let policy = RetryPolicy::default();
        let mut ids = ProposalIds::from_os_rng();
        bootstrap(&store, &cluster, ids.next_id(), &policy)
            .await
            .unwrap();
        let mut changes = store.changes(Generation::new(1)).await.unwrap();
        for generation in 2..6 {
            bump_generation(&store, &cluster, &mut ids, &policy)
                .await
                .unwrap();
            let change = tokio::time::timeout(Duration::from_secs(10), changes.next())
                .await
                .expect("the watch reported the increment")
                .unwrap();
            assert_eq!(change.generation, Generation::new(generation));
        }
    })
    .await;
}
