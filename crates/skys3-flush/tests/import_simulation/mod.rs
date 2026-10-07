//! The namespace import racing client writes (design §9.1, §16.1).
//!
//! Each seed attaches a bucket whose remote prefix already holds objects,
//! and runs the import, in small pages at a limited rate, with one to four
//! streams listing key ranges in parallel, while clients PUT and DELETE the
//! same keys and keys the remote does not have, and the flusher sends their
//! writes to the remote. On some seeds the node restarts its flush service
//! midway, and the import resumes every range from its checkpoint. The remote delays requests,
//! loses requests and responses, and answers `500` and `503 SlowDown`;
//! sometimes it is versioned. Once the writes stop, the remote heals, the
//! import is done, and the flush has drained, the checks are:
//!
//! - a key whose last acknowledged write is a PUT holds that write, clean,
//!   in the index and at the remote: the import never overwrote it, and
//!   its flush replaced the remote's object without a conflict;
//! - a key whose last acknowledged write is a DELETE has no entry and no
//!   remote object: the import never resurrected it;
//! - a key no client wrote is an imported stub of the remote's object if
//!   the remote had one, and has no entry otherwise.

use std::collections::BTreeMap;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ProbeStatus};
use skys3_index::{EntryState, ImportCheckpoint};
use skys3_obs::MetricsRegistry;
use skys3_remote::{ObjectStore, PutObject};
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use skys3_types::{
    BucketDocument, BucketMode, BucketName, ETag, ProposalId, RemoteTarget, ShardCount,
};

use crate::support::{Node, Patience, cluster, md5_etag, runtime, settings, shard_ref};

const PREFIX: &str = "team/";

/// Keys the remote holds before the bucket is attached, and keys only
/// clients write.
const REMOTE_KEYS: u32 = 30;
const LOCAL_KEYS: u32 = 8;

const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(30),
    internal_error_probability: 0.05,
    slow_down_probability: 0.03,
    lost_request_probability: 0.03,
    lost_response_probability: 0.05,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

type Outcome = Result<(), Box<dyn std::error::Error>>;

fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: Some(RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some(PREFIX.to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

pub fn scenario(context: &mut SimContext) -> Outcome {
    let seed = context.fork_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let store = context.s3(SimS3Config {
        versioning: rng.random_bool(0.5),
        ..SimS3Config::default()
    });
    let settings = FlushSettings {
        import_page_keys: rng.random_range(1..8),
        import_keys_per_second: rng.random_range(20..400),
        import_streams: rng.random_range(1..=4),
        ..settings()
    };
    let ops = rng.random_range(20..80);
    let restart_at = rng.random_bool(0.5).then(|| rng.random_range(0..ops));
    runtime().block_on(async move {
        let node = Node::open(seed).await;
        // What the remote held before the attach.
        let mut remote: BTreeMap<String, ETag> = BTreeMap::new();
        for n in 0..REMOTE_KEYS {
            let key = format!("k{n:02}");
            let request = PutObject::new(format!("{PREFIX}{key}"), format!("remote {key}"));
            remote.insert(key, store.put_object(request).await?.etag);
        }
        let start = || {
            let connect = store.clone();
            FlushService::<SimS3, _>::new(
                cluster(),
                settings.clone(),
                Box::new(move |_: &RemoteTarget| connect.clone()),
                FlushMetrics::register(&MetricsRegistry::new()),
            )
        };
        let mut service = start();
        service.reconcile(&[bucket()], &node.set).await;
        store.set_faults(FAULTS);

        // The latest acknowledged write of each key: its body, or `None`
        // after a delete.
        let mut latest: BTreeMap<String, Option<String>> = BTreeMap::new();
        for op in 0..ops {
            if restart_at == Some(op) {
                service.shutdown().await;
                service = start();
                service.reconcile(&[bucket()], &node.set).await;
            }
            let key = format!("k{:02}", rng.random_range(0..REMOTE_KEYS + LOCAL_KEYS));
            if rng.random_bool(0.55) {
                let body = format!("{key} v{op}");
                node.put(&key, &body).await;
                latest.insert(key, Some(body));
            } else {
                node.delete(&key).await;
                latest.insert(key, None);
            }
            tokio::time::sleep(Duration::from_millis(rng.random_range(0..40))).await;
        }
        store.set_faults(SimS3Faults::NONE);
        settle(&service, &node).await?;
        check(&service, &node, &store, &remote, &latest).await?;
        service.shutdown().await;
        Ok(())
    })
}

/// Waits until the import is done and the flush has drained.
async fn settle(service: &FlushService<SimS3, skys3_io::SimMount>, node: &Node) -> Outcome {
    let patience = Patience::new();
    loop {
        let status = service.status(&bucket().bucket_id).ok_or("no flusher")?;
        let done = status.import.checkpoint == ImportCheckpoint::Done;
        let unclean = node.unclean().await;
        if done && unclean.is_empty() {
            return Ok(());
        }
        if patience.is_exhausted() {
            return Err(format!("never settled: {status:?}, unclean: {unclean:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn check(
    service: &FlushService<SimS3, skys3_io::SimMount>,
    node: &Node,
    store: &SimS3,
    remote: &BTreeMap<String, ETag>,
    latest: &BTreeMap<String, Option<String>>,
) -> Outcome {
    let status = service.status(&bucket().bucket_id).ok_or("no flusher")?;
    // The probe ran under the faults too, and found every precondition.
    if !matches!(&status.probe, ProbeStatus::Done { unprotected } if unprotected.is_empty()) {
        return Err(format!("the probe found {:?}", status.probe).into());
    }
    let conflicts: Vec<_> = status
        .shards
        .iter()
        .flat_map(|(_, s)| &s.conflicts)
        .collect();
    if !conflicts.is_empty() {
        return Err(format!("no one else wrote, yet keys are in conflict: {conflicts:?}").into());
    }
    for n in 0..REMOTE_KEYS + LOCAL_KEYS {
        let key = format!("k{n:02}");
        let entry = node.entry(&key).await;
        let object = store.object(&format!("{PREFIX}{key}"));
        match (latest.get(&key), remote.get(&key)) {
            (Some(Some(body)), _) => {
                let etag = md5_etag(body.as_bytes());
                let indexed = entry.as_ref().is_some_and(|entry| {
                    entry.state == EntryState::Clean
                        && entry
                            .object
                            .as_ref()
                            .is_some_and(|object| object.local_etag == etag)
                });
                let flushed = object.as_ref().is_some_and(|o| o.body == body.as_bytes());
                if !indexed || !flushed {
                    return Err(format!(
                        "{key}: the last write was {body:?}, but the index holds {entry:?} \
                         and the remote {:?}",
                        object.map(|o| o.body)
                    )
                    .into());
                }
            }
            (Some(None), _) => {
                if entry.is_some() || object.is_some() {
                    return Err(format!(
                        "{key} was deleted, but the index holds {entry:?} and the remote {:?}",
                        object.map(|o| o.body)
                    )
                    .into());
                }
            }
            (None, Some(etag)) => {
                let stub = entry.as_ref().is_some_and(|entry| {
                    entry.state == EntryState::Evicted && entry.remote_etag.as_ref() == Some(etag)
                });
                if !stub {
                    return Err(format!("{key} was not imported: {entry:?}").into());
                }
            }
            (None, None) => {
                if entry.is_some() {
                    return Err(format!("{key} was never written, but has {entry:?}").into());
                }
            }
        }
    }
    Ok(())
}
