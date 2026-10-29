//! `kill -9` loops against the real binary on a real file system (plan
//! M1-14): concurrent AWS SDK clients send PUTs, conditional PUTs,
//! one-part multipart uploads, DELETEs, and GETs while the test kills the
//! node with `SIGKILL` at a random moment and restarts it, round after
//! round. Every operation goes into a history, and at the end the node
//! reads every key back. The history must be linearizable per key, and
//! every acknowledged write must survive unless a later write superseded
//! it; no unacknowledged write may resurface over a later acknowledged one
//! (design §5.2).
//!
//! `SKYS3_KILL_ROUNDS` sets the number of kills (3 by default; the nightly
//! job runs many more), and `SKYS3_KILL_SEED` the seed of the clients'
//! choices and the kill delays (0 by default; the nightly job draws one).
//! The delays are real time, so a seed does not replay a run exactly.

mod support;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use md5::{Digest, Md5};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_sim::check::{Survivors, check_durable, check_linearizable};
use skys3_sim::history::{Call, Condition, History, Outcome};
use support::process::{Process, configure};

/// The bucket the clients write.
const BUCKET: &str = "data";
/// Keys the clients share: few, so that writes contend.
const KEYS: usize = 6;
/// Concurrent clients.
const CLIENTS: usize = 4;
/// The server name the history gives the node: every life shares it, and
/// a kill ends what was sent to the life that died.
const NODE: &str = "node";
/// How long a client waits for any one answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the clients may take to stop after a kill.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

fn env_number(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number, not {value:?}")),
    )
}

/// The MD5 of `bytes`, in hex: the ETag of a single-part object.
fn md5_hex(bytes: &[u8]) -> String {
    Md5::digest(bytes)
        .iter()
        .fold(String::with_capacity(32), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// The ETag of a one-part multipart object.
fn multipart_etag(part: &[u8]) -> String {
    format!("{}-1", md5_hex(&Md5::digest(part)))
}

/// What came back for a request.
enum Answer<T> {
    /// A successful answer.
    Ok(T),
    /// An error answer with this HTTP status.
    Status(u16),
    /// No answer: the connection failed or broke, or the time ran out.
    None,
}

/// Waits up to [`ANSWER_TIMEOUT`] for `request` and sorts its result.
async fn answer<T, E>(request: impl Future<Output = Result<T, SdkError<E>>>) -> Answer<T> {
    match tokio::time::timeout(ANSWER_TIMEOUT, request).await {
        Ok(Ok(output)) => Answer::Ok(output),
        Ok(Err(error)) => match error.raw_response() {
            Some(response) => Answer::Status(response.status().as_u16()),
            None => Answer::None,
        },
        Err(_) => Answer::None,
    }
}

/// One client: a process name, an S3 client of the current node life, and
/// its own seeded choices.
struct Client {
    process: String,
    s3: aws_sdk_s3::Client,
    history: History,
    rng: SmallRng,
    /// The last value this client saw for each key, for `If-Match`.
    seen: BTreeMap<String, String>,
    /// Operations sent so far, which makes every body unique.
    sent: usize,
}

impl Client {
    fn new(process: String, s3: aws_sdk_s3::Client, history: History, seed: u64) -> Self {
        Self {
            process,
            s3,
            history,
            rng: SmallRng::seed_from_u64(seed),
            seen: BTreeMap::new(),
            sent: 0,
        }
    }

    /// Sends operations until `stop` is set.
    async fn run(mut self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::SeqCst) {
            let key = format!("key-{}", self.rng.random_range(0..KEYS));
            match self.rng.random_range(0..100) {
                0..30 => self.put(&key, Condition::None).await,
                30..38 => self.put(&key, Condition::IfAbsent).await,
                38..45 => {
                    let expected = self.seen.get(&key).cloned();
                    let expected = expected.unwrap_or_else(|| md5_hex(b"never written"));
                    self.put(&key, Condition::IfMatch(expected)).await;
                }
                45..53 => self.multipart(&key).await,
                53..68 => self.delete(&key).await,
                _ => {
                    self.get(&key).await;
                }
            }
            let pause = self.rng.random_range(0..20);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
    }

    /// A body no other operation uses, sometimes larger than the node's
    /// `inline_max_bytes`, so it is stored as extents.
    fn body(&mut self) -> Vec<u8> {
        self.sent += 1;
        let mut body = format!("{} operation {}:", self.process, self.sent).into_bytes();
        let len = if self.rng.random_ratio(1, 5) {
            self.rng.random_range(2_000..150_000)
        } else {
            self.rng.random_range(body.len()..1_000)
        };
        body.resize(len.max(body.len()), b'a' + (self.sent % 26) as u8);
        body
    }

    async fn put(&mut self, key: &str, condition: Condition) {
        let body = self.body();
        let value = md5_hex(&body);
        let mut request = self
            .s3
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(body.into());
        match &condition {
            Condition::None => {}
            Condition::IfAbsent => request = request.if_none_match("*"),
            Condition::IfMatch(expected) => request = request.if_match(format!("\"{expected}\"")),
        }
        let call = Call::Put {
            value: value.clone(),
            condition: condition.clone(),
        };
        let pending = self.history.call_to(&self.process, NODE, key, call);
        let outcome = match answer(request.send()).await {
            Answer::Ok(_) => {
                self.seen.insert(key.to_owned(), value);
                Outcome::Done
            }
            Answer::Status(412) if condition != Condition::None => Outcome::ConditionFailed,
            // S3 answers an If-Match on a missing key with 404.
            Answer::Status(404) if matches!(condition, Condition::IfMatch(_)) => {
                Outcome::ConditionFailed
            }
            Answer::Status(status) if status >= 500 => Outcome::Failed,
            Answer::Status(status) => panic!("PUT {key} answered {status}"),
            Answer::None => Outcome::Unknown,
        };
        self.history.answer(pending, outcome);
    }

    /// A one-part multipart upload, completed unconditionally or with
    /// `If-None-Match: *`. Only the completion is recorded: creating the
    /// upload and sending its part change nothing a read sees.
    async fn multipart(&mut self, key: &str) {
        let create = self.s3.create_multipart_upload().bucket(BUCKET).key(key);
        let Answer::Ok(created) = answer(create.send()).await else {
            return;
        };
        let upload_id = created.upload_id().expect("an upload ID").to_owned();
        let part = self.body();
        let value = multipart_etag(&part);
        let upload = self
            .s3
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(part.into());
        let Answer::Ok(uploaded) = answer(upload.send()).await else {
            return;
        };
        let parts = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .set_e_tag(uploaded.e_tag().map(str::to_owned))
                    .build(),
            )
            .build();
        let condition = if self.rng.random_bool(0.5) {
            Condition::IfAbsent
        } else {
            Condition::None
        };
        let mut complete = self
            .s3
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(parts);
        if condition == Condition::IfAbsent {
            complete = complete.if_none_match("*");
        }
        let call = Call::Put {
            value: value.clone(),
            condition: condition.clone(),
        };
        let pending = self.history.call_to(&self.process, NODE, key, call);
        let outcome = match answer(complete.send()).await {
            Answer::Ok(output) => {
                let etag = output.e_tag().unwrap_or_default().trim_matches('"');
                assert_eq!(etag, value, "the multipart ETag of {key}");
                self.seen.insert(key.to_owned(), value);
                Outcome::Done
            }
            Answer::Status(412) if condition == Condition::IfAbsent => Outcome::ConditionFailed,
            Answer::Status(status) if status >= 500 => Outcome::Failed,
            Answer::Status(status) => panic!("CompleteMultipartUpload {key} answered {status}"),
            Answer::None => Outcome::Unknown,
        };
        self.history.answer(pending, outcome);
    }

    async fn delete(&mut self, key: &str) {
        let request = self.s3.delete_object().bucket(BUCKET).key(key);
        let pending = self.history.call_to(&self.process, NODE, key, Call::Delete);
        let outcome = match answer(request.send()).await {
            Answer::Ok(_) => Outcome::Done,
            Answer::Status(status) if status >= 500 => Outcome::Failed,
            Answer::Status(status) => panic!("DELETE {key} answered {status}"),
            Answer::None => Outcome::Unknown,
        };
        self.history.answer(pending, outcome);
    }

    /// Reads `key`, checks the body against its ETag, and returns what it
    /// found, or `None` without a definite answer.
    async fn get(&mut self, key: &str) -> Option<Option<String>> {
        let request = self.s3.get_object().bucket(BUCKET).key(key);
        let pending = self.history.call_to(&self.process, NODE, key, Call::Get);
        let found = match answer(request.send()).await {
            Answer::Ok(object) => {
                let etag = object
                    .e_tag()
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_owned();
                let body = tokio::time::timeout(ANSWER_TIMEOUT, object.body.collect()).await;
                match body {
                    Ok(Ok(body)) => {
                        let body = body.into_bytes();
                        let expected = if etag.ends_with("-1") {
                            multipart_etag(&body)
                        } else {
                            md5_hex(&body)
                        };
                        assert_eq!(etag, expected, "the body of {key} does not match its ETag");
                        Some(Some(etag))
                    }
                    // The body broke off, as a kill does.
                    _ => None,
                }
            }
            Answer::Status(404) => Some(None),
            Answer::Status(status) if status >= 500 => None,
            Answer::Status(status) => panic!("GET {key} answered {status}"),
            Answer::None => None,
        };
        let outcome = match &found {
            Some(value) => {
                if let Some(value) = value {
                    self.seen.insert(key.to_owned(), value.clone());
                }
                Outcome::Read(value.clone())
            }
            None => Outcome::Unknown,
        };
        self.history.answer(pending, outcome);
        found
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_9_loops_lose_no_acknowledged_write() {
    let rounds = env_number("SKYS3_KILL_ROUNDS").unwrap_or(3);
    let seed = env_number("SKYS3_KILL_SEED").unwrap_or(0);
    eprintln!("kill loop: {rounds} rounds, SKYS3_KILL_SEED={seed}");
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("node.log");
    let (config, gateway, admin) = configure(dir.path());
    let history = History::new();

    for round in 0..rounds {
        let node = Process::start(&config, &gateway, &admin, &log).await;
        if round == 0 {
            node.s3()
                .create_bucket()
                .bucket(BUCKET)
                .send()
                .await
                .unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let clients: Vec<_> = (0..CLIENTS)
            .map(|n| {
                let client = Client::new(
                    format!("r{round}c{n}"),
                    node.s3(),
                    history.clone(),
                    rng.random(),
                );
                tokio::spawn(client.run(Arc::clone(&stop)))
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(rng.random_range(200..1_500))).await;
        node.kill();
        // Whatever the dead process was sending took effect before now,
        // or never: the clients stop before the next life starts.
        history.crashed(NODE);
        stop.store(true, Ordering::SeqCst);
        for client in clients {
            tokio::time::timeout(STOP_TIMEOUT, client)
                .await
                .expect("a client stops after the kill")
                .unwrap();
        }
    }

    // Recover once more and read every key back.
    let node = Process::start(&config, &gateway, &admin, &log).await;
    let mut verifier = Client::new("verifier".to_owned(), node.s3(), history.clone(), seed);
    let mut survivors = BTreeMap::new();
    for n in 0..KEYS {
        let key = format!("key-{n}");
        let mut value = None;
        for _ in 0..5 {
            value = verifier.get(&key).await;
            if value.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let value = value.expect("the recovered node answers");
        let survivor = Survivors {
            copies: vec![value],
            ..Survivors::default()
        };
        survivors.insert(key, survivor);
    }
    let status = node.terminate().await;
    assert!(status.success(), "{status:?}");

    let operations = history.operations();
    let report = |what: &str, violation: &dyn std::fmt::Display| {
        let log = std::fs::read_to_string(&log).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(40).collect();
        panic!(
            "{what} (SKYS3_KILL_SEED={seed}): {violation}\nnode log, last lines first:\n{}",
            tail.join("\n")
        );
    };
    if let Err(violation) = check_linearizable(&operations) {
        report("the history is not linearizable", &violation);
    }
    if let Err(violation) = check_durable(&operations, &survivors) {
        report("an acknowledged write was lost", &violation);
    }
    let count = |outcome: Outcome| {
        operations
            .iter()
            .filter(|op| op.call.is_write() && op.outcome == outcome)
            .count()
    };
    let (done, failed) = (count(Outcome::Done), count(Outcome::Failed));
    eprintln!(
        "kill loop: {} operations, {done} writes acknowledged, {failed} cut off by a kill",
        operations.len()
    );
    assert!(done > 0, "no write was acknowledged");
}
