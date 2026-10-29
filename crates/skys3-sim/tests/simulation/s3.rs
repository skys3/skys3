//! Simulation scenarios for the simulated S3 store.
//!
//! One simulation uses two stores, as every cluster simulation will: a
//! control store and a remote target (design §16.1).
//!
//! - **Register writers** race to append their proposals to one control-store
//!   register with conditional PUTs, following the §6.1 rules: a 412 means
//!   another writer won, a 409 means re-read and retry, and after a lost
//!   response the writer re-reads the register to see whether its own
//!   proposal landed. The control store also suffers outages. No
//!   acknowledged proposal may be lost or applied twice.
//! - **Flushers** write successive versions of their keys to the remote
//!   target with conditional PUTs, multipart uploads, and conditional
//!   DELETEs, following the §7.2 recovery rule: on a 412 or 404, a HEAD that
//!   finds the flush's own write identity means an earlier attempt landed.
//!   Each key must end at its latest version, with the ETag computed locally
//!   (design §7.4) and its write identity, and every upload a flusher
//!   started must be completed or aborted.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::error::Error;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use md5::{Digest, Md5};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CreateMultipartUpload,
    DeleteObject, GetObject, HeadObject, ObjectStore, PutObject, S3Error, S3ErrorKind, S3Result,
    UploadId, UploadPart, UserMetadata, WritePrecondition,
};
use skys3_sim::s3::{SimS3Config, SimS3Faults, SimS3Stats};
use skys3_sim::{Runner, SeedSet, SimContext, SimS3};
use skys3_types::ETag;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const REGISTER: &str = "cluster/coordinator.lease";
const REGISTER_WRITERS: usize = 3;
const PROPOSALS_PER_WRITER: usize = 6;
const FLUSHERS: usize = 2;
const KEYS_PER_FLUSHER: usize = 3;
const VERSIONS_PER_KEY: u64 = 6;
const MIN_PART: u64 = 8;

/// Faults on both stores, often enough to show up in every seed.
const FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(8),
    internal_error_probability: 0.04,
    slow_down_probability: 0.04,
    lost_request_probability: 0.04,
    lost_response_probability: 0.06,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

/// Gives up on a request after this many attempts, so a livelock fails the
/// seed instead of hanging it.
const MAX_ATTEMPTS: u32 = 10_000;

async fn backoff(rng: &mut SmallRng) {
    tokio::time::sleep(Duration::from_millis(rng.random_range(1..=4))).await;
}

/// Whether a request may be sent again as it is.
fn retryable(error: &S3Error) -> bool {
    error.kind().is_transient() || error.kind() == S3ErrorKind::ConditionalRequestConflict
}

/// Sends a request until it gets an answer other than a transient error.
async fn with_retries<T, F>(rng: &mut SmallRng, mut request: impl FnMut() -> F) -> S3Result<T>
where
    F: Future<Output = S3Result<T>>,
{
    let mut attempts = 0;
    loop {
        match request().await {
            Err(error) if retryable(&error) && attempts < MAX_ATTEMPTS => {
                attempts += 1;
                backoff(rng).await;
            }
            result => return result,
        }
    }
}

// Register writers.

/// Reads the register: its ETag and value, or `None` if it does not exist.
async fn read_register(store: &SimS3, rng: &mut SmallRng) -> TestResult<Option<(ETag, String)>> {
    match with_retries(rng, || store.get_object(GetObject::new(REGISTER))).await {
        Ok(output) => Ok(Some((
            output.info.etag,
            String::from_utf8(output.body.to_vec())?,
        ))),
        Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Appends each of the writer's proposals to the register, one conditional
/// write at a time, and records each proposal once it knows it landed.
async fn register_writer(
    store: SimS3,
    node: usize,
    seed: u64,
    acknowledged: Rc<RefCell<Vec<String>>>,
) -> TestResult {
    let mut rng = SmallRng::seed_from_u64(seed);
    for n in 0..PROPOSALS_PER_WRITER {
        let proposal = format!("node-{node}/{n}");
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > MAX_ATTEMPTS {
                return Err(format!("{proposal} did not land").into());
            }
            let current = read_register(&store, &mut rng).await?;
            // The lost-response rule: a proposal found in the register
            // landed, even if its write returned an error.
            if let Some((_, value)) = &current
                && value.lines().any(|line| line == proposal)
            {
                break;
            }
            let (precondition, value) = match current {
                Some((etag, value)) => (WritePrecondition::IfMatch(etag), value + &proposal),
                None => (WritePrecondition::IfAbsent, proposal.clone()),
            };
            let request = PutObject::new(REGISTER, value + "\n").with_precondition(precondition);
            match store.put_object(request).await {
                Ok(_) => break,
                // Another writer won, or raced; a transient error may hide
                // a landed write. Re-read in every case.
                Err(error)
                    if retryable(&error)
                        || error.kind() == S3ErrorKind::PreconditionFailed
                        || error.kind() == S3ErrorKind::NoSuchKey =>
                {
                    backoff(&mut rng).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        acknowledged.borrow_mut().push(proposal);
    }
    Ok(())
}

/// Checks that the register holds every acknowledged proposal exactly once,
/// and nothing else.
fn check_register(store: &SimS3, acknowledged: &[String]) -> TestResult {
    let object = store
        .object(REGISTER)
        .ok_or("the register does not exist")?;
    let value = String::from_utf8(object.body.to_vec())?;
    let mut landed: Vec<_> = value.lines().collect();
    let mut expected: Vec<_> = acknowledged.iter().map(String::as_str).collect();
    landed.sort_unstable();
    expected.sort_unstable();
    if landed != expected {
        return Err(format!("the register holds {landed:?}, expected {expected:?}").into());
    }
    if expected.len() != REGISTER_WRITERS * PROPOSALS_PER_WRITER {
        return Err(format!("only {} proposals were acknowledged", expected.len()).into());
    }
    Ok(())
}

// Flushers.

/// One version of a key, as a flusher knows it locally.
#[derive(Clone, Debug)]
enum Local {
    Put { body: Bytes, multipart: bool },
    Delete,
}

/// The versions a flusher commits for a key: small and multipart PUTs, and
/// deletes. The first is always a PUT.
fn versions(rng: &mut SmallRng, key: &str) -> Vec<Local> {
    (0..VERSIONS_PER_KEY)
        .map(|seq| {
            if seq > 0 && rng.random_bool(0.2) {
                return Local::Delete;
            }
            let len = rng.random_range(0..3 * MIN_PART as usize);
            let body = Bytes::from(format!("{key}@{seq}:{}", "x".repeat(len)));
            Local::Put {
                multipart: rng.random_bool(0.4),
                body,
            }
        })
        .collect()
}

fn write_identity(node: usize, seq: u64) -> String {
    format!("sim/b-{node}/0/1.{seq}")
}

fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The ETag S3 gives a version: the body's MD5, or for a multipart upload
/// with `MIN_PART`-byte parts, the MD5 of the part MD5s and the part count.
fn expected_etag(body: &[u8], multipart: bool) -> String {
    if !multipart {
        return hex(&md5(body));
    }
    let parts: Vec<_> = body.chunks(MIN_PART as usize).collect();
    let digests: Vec<u8> = parts.iter().flat_map(|part| md5(part)).collect();
    if parts.is_empty() {
        // An empty body is one empty part.
        return format!("{}-1", hex(&md5(&md5(b""))));
    }
    format!("{}-{}", hex(&md5(&digests)), parts.len())
}

/// A flusher: the remote ETag it knows for each of its keys, and the uploads
/// it started.
struct Flusher {
    store: SimS3,
    node: usize,
    rng: SmallRng,
    uploads: Vec<(String, UploadId)>,
}

impl Flusher {
    /// Whether the remote object carries `wid`, after a 412 or a 404 (§7.2).
    async fn landed(&mut self, key: &str, wid: &str) -> TestResult<Option<ETag>> {
        let store = self.store.clone();
        match with_retries(&mut self.rng, || store.head_object(HeadObject::new(key))).await {
            Ok(info) if info.metadata.write_identity() == Some(wid) => Ok(Some(info.etag)),
            Ok(info) => Err(format!("{key} is in conflict: found {info:?}, not {wid}").into()),
            Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Flushes one version of `key` conditionally on `remote`, the ETag the
    /// flusher last saw there, and returns the key's new remote ETag.
    async fn flush(
        &mut self,
        key: &str,
        seq: u64,
        version: &Local,
        remote: Option<ETag>,
    ) -> TestResult<Option<ETag>> {
        let wid = write_identity(self.node, seq);
        for _ in 0..MAX_ATTEMPTS {
            let precondition = match &remote {
                Some(etag) => WritePrecondition::IfMatch(etag.clone()),
                None => WritePrecondition::IfAbsent,
            };
            let result = match version {
                Local::Delete => {
                    let Some(etag) = &remote else {
                        return Ok(None);
                    };
                    let request = DeleteObject::new(key).with_if_match(etag.clone());
                    self.store.delete_object(request).await.map(|_| None)
                }
                Local::Put { body, multipart } => {
                    let mut metadata = UserMetadata::new();
                    metadata.insert("skys3-wid", wid.clone())?;
                    if *multipart {
                        self.multipart(key, body, metadata, precondition).await
                    } else {
                        let request = PutObject::new(key, body.clone())
                            .with_metadata(metadata)
                            .with_precondition(precondition);
                        self.store.put_object(request).await
                    }
                    .map(|output| Some(output.etag))
                }
            };
            match result {
                Ok(etag) => return Ok(etag),
                Err(error) if retryable(&error) => backoff(&mut self.rng).await,
                Err(error)
                    if matches!(
                        error.kind(),
                        S3ErrorKind::PreconditionFailed
                            | S3ErrorKind::NoSuchKey
                            | S3ErrorKind::NoSuchUpload
                    ) =>
                {
                    // An earlier attempt may have landed with a lost
                    // response.
                    let landed = self.landed(key, &wid).await?;
                    match version {
                        Local::Delete if landed.is_none() => return Ok(None),
                        Local::Put { .. } if landed.is_some() => return Ok(landed),
                        _ => {
                            return Err(format!(
                                "{key}@{seq}: {error}, and the remote is {landed:?}"
                            )
                            .into());
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(format!("{key}@{seq} did not flush").into())
    }

    /// Uploads `body` in `MIN_PART`-byte parts and completes the upload
    /// under `precondition`.
    async fn multipart(
        &mut self,
        key: &str,
        body: &Bytes,
        metadata: UserMetadata,
        precondition: WritePrecondition,
    ) -> S3Result<skys3_remote::WriteOutput> {
        let store = self.store.clone();
        let create = CreateMultipartUpload::new(key).with_metadata(metadata);
        let upload_id = store.create_multipart_upload(create).await?;
        self.uploads.push((key.to_owned(), upload_id.clone()));
        let chunks: Vec<Bytes> = if body.is_empty() {
            vec![Bytes::new()]
        } else {
            (0..body.len())
                .step_by(MIN_PART as usize)
                .map(|at| body.slice(at..body.len().min(at + MIN_PART as usize)))
                .collect()
        };
        let mut parts = Vec::new();
        for (part_number, chunk) in (1..).zip(chunks) {
            let request = UploadPart {
                key: key.to_owned(),
                upload_id: upload_id.clone(),
                part_number,
                body: chunk,
                content_md5: None,
            };
            let etag = with_retries(&mut self.rng, || store.upload_part(request.clone())).await?;
            parts.push(CompletedPart { part_number, etag });
        }
        let complete = CompleteMultipartUpload {
            key: key.to_owned(),
            upload_id,
            parts,
            precondition,
        };
        store.complete_multipart_upload(complete).await
    }

    /// Aborts every upload it started that is still open, as the flusher
    /// does for orphaned remote uploads (§7.3).
    async fn abort_orphans(&mut self) -> TestResult {
        let store = self.store.clone();
        for (key, upload_id) in std::mem::take(&mut self.uploads) {
            let request = AbortMultipartUpload { key, upload_id };
            match with_retries(&mut self.rng, || {
                store.abort_multipart_upload(request.clone())
            })
            .await
            {
                Ok(()) => {}
                Err(error) if error.kind() == S3ErrorKind::NoSuchUpload => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

/// The versions of every key, by key, and each key's final version.
type Plans = BTreeMap<String, Vec<Local>>;

async fn flusher(
    store: SimS3,
    node: usize,
    seed: u64,
    plans: Plans,
    started: Rc<RefCell<Vec<UploadId>>>,
) -> TestResult {
    let mut flusher = Flusher {
        store,
        node,
        rng: SmallRng::seed_from_u64(seed),
        uploads: Vec::new(),
    };
    for (key, versions) in &plans {
        let mut remote = None;
        for (seq, version) in (0..).zip(versions) {
            remote = flusher.flush(key, seq, version, remote).await?;
        }
    }
    started
        .borrow_mut()
        .extend(flusher.uploads.iter().map(|(_, id)| id.clone()));
    flusher.abort_orphans().await
}

/// Checks that every key holds its final version, with the expected ETag
/// and write identity, and that no upload the flushers started is left
/// open.
///
/// An upload whose `CreateMultipartUpload` response was lost stays open: the
/// flusher never learns its ID. Finding it takes `ListMultipartUploads`, or
/// the bucket's abort-incomplete-uploads lifecycle rule (§7.3).
fn check_target(store: &SimS3, plans: &[(usize, Plans)], started: &[UploadId]) -> TestResult {
    for (node, plans) in plans {
        for (key, versions) in plans {
            let seq = versions.len() as u64 - 1;
            let remote = store.object(key);
            match (versions.last(), remote) {
                (Some(Local::Delete), None) => {}
                (Some(Local::Put { body, multipart }), Some(object)) => {
                    let expected = expected_etag(body, *multipart);
                    if object.body != *body || object.info.etag.as_str() != expected {
                        return Err(format!("{key} holds {object:?}, not {expected}").into());
                    }
                    let wid = write_identity(*node, seq);
                    if object.info.metadata.write_identity() != Some(wid.as_str()) {
                        return Err(format!("{key} does not carry {wid}").into());
                    }
                }
                (version, remote) => {
                    return Err(format!("{key} should be {version:?}, is {remote:?}").into());
                }
            }
        }
    }
    let open: Vec<_> = store
        .uploads()
        .into_iter()
        .filter(|(id, _)| started.contains(id))
        .collect();
    if !open.is_empty() {
        return Err(format!("uploads left open: {open:?}").into());
    }
    Ok(())
}

/// The outcome of one run, to compare replays.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    acknowledged: Vec<String>,
    control: SimS3Stats,
    target: SimS3Stats,
    elapsed: Duration,
}

/// Runs register writers against a control store with outages, and
/// flushers against a remote target, all under `FAULTS`.
fn control_and_target(context: &mut SimContext) -> TestResult<Outcome> {
    let control = context.s3(SimS3Config::default());
    let versioning = context.rng().random_bool(0.5);
    let target = context.s3(SimS3Config {
        min_part_size: MIN_PART,
        versioning,
        ..SimS3Config::default()
    });
    control.set_faults(FAULTS);
    target.set_faults(FAULTS);

    let mut builder = context.builder();
    builder.simulation_duration(Duration::from_secs(600));
    let mut sim = builder.build();

    let acknowledged = Rc::new(RefCell::new(Vec::new()));
    for node in 0..REGISTER_WRITERS {
        let (store, seed) = (control.clone(), context.fork_seed());
        let acknowledged = Rc::clone(&acknowledged);
        sim.client(format!("writer-{node}"), async move {
            register_writer(store, node, seed, acknowledged).await
        });
    }

    let started = Rc::new(RefCell::new(Vec::new()));
    let finished = Rc::new(Cell::new(0));
    let mut all_plans = Vec::new();
    for node in 0..FLUSHERS {
        let plans: Plans = (0..KEYS_PER_FLUSHER)
            .map(|k| {
                let key = format!("data/{node}/{k}");
                let versions = versions(context.rng(), &key);
                (key, versions)
            })
            .collect();
        all_plans.push((node, plans.clone()));
        let (store, seed) = (target.clone(), context.fork_seed());
        let (started, finished) = (Rc::clone(&started), Rc::clone(&finished));
        sim.client(format!("flusher-{node}"), async move {
            flusher(store, node, seed, plans, started).await?;
            finished.set(finished.get() + 1);
            Ok(())
        });
    }

    // Control-store outages come and go, and end by step 3,000.
    let mut outage = false;
    for step in 0.. {
        if sim.step()? {
            break;
        }
        let toggle = step < 3_000 && context.rng().random_bool(0.002);
        if toggle || (outage && step == 3_000) {
            outage = !outage;
            control.set_faults(if outage { SimS3Faults::OUTAGE } else { FAULTS });
        }
    }

    check_register(&control, &acknowledged.borrow())?;
    if finished.get() != FLUSHERS {
        return Err("a flusher did not finish".into());
    }
    check_target(&target, &all_plans, &started.borrow())?;
    Ok(Outcome {
        acknowledged: acknowledged.take(),
        control: control.stats(),
        target: target.stats(),
        elapsed: sim.elapsed(),
    })
}

#[test]
fn control_store_and_target_under_faults_lose_nothing() {
    Runner::new().run(|context| control_and_target(context).map(drop));
}

#[test]
fn the_scenario_injects_every_fault() {
    let mut total = SimS3Stats::default();
    Runner::with_seeds(SeedSet::Range(0..4)).run(|context| {
        let outcome = control_and_target(context)?;
        for stats in [outcome.control, outcome.target] {
            total.requests += stats.requests;
            total.internal_errors += stats.internal_errors;
            total.slow_downs += stats.slow_downs;
            total.lost_requests += stats.lost_requests;
            total.lost_responses += stats.lost_responses;
            total.conflicts += stats.conflicts;
        }
        Ok(())
    });
    let counts = [
        total.internal_errors,
        total.slow_downs,
        total.lost_requests,
        total.lost_responses,
        total.conflicts,
    ];
    assert!(counts.iter().all(|&count| count > 0), "{total:?}");
}

#[test]
fn the_scenario_replays_exactly() {
    let run = |seed| control_and_target(&mut SimContext::new(seed)).unwrap();
    let first = run(1);
    assert_eq!(run(1), first);
    assert_ne!(run(2), first);
}
