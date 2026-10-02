//! The client workload of M1: concurrent `PUT`s (small ones stored inline,
//! larger ones as extents), conditional `PUT`s, multipart uploads, `GET`,
//! `HEAD`, and `DELETE` on a small set of keys, each sent to the primary of
//! its shard, or to any node with [`Workload::any_gateway`], and recorded
//! in the history.
//!
//! Each operation is recorded once its request has a connection to the
//! node, with the node as its server ([`History::call_to`]): a request
//! that never connected reached no node and is left out, and one that did
//! can only take effect in the life of the node that accepted it, which
//! lets the driver end it when that life crashes ([`History::crashed`]).
//! A request any node may forward has no such server.
//!
//! A multipart upload has one part: S3 wants every part but the last to
//! hold at least 5 MiB, more than a simulated cluster should move. Only
//! its completion is recorded, as a `PUT` of the multipart ETag; an
//! upload whose creation or part gets no answer is left open, which no
//! read sees.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ::http::{Method, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::Full;
use md5::{Digest, Md5};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_gateway::ShardRef;
use skys3_sim::history::{Call, Condition, History, Outcome, Pending};
use skys3_types::{BucketDocument, NodeId, ShardConfig};

use crate::cluster::WriteTiming;
use crate::s3::{self, NoAnswer};

/// The shape of the client workload.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Concurrent clients.
    pub clients: usize,
    /// Operations each client sends.
    pub operations: usize,
    /// Keys per bucket. Few keys mean more contention per key.
    pub keys: usize,
    /// The largest object body. Bodies above the gateway's
    /// `inline_max_bytes` are stored as extents.
    pub max_body: usize,
    /// The longest pause between a client's operations.
    pub think_time: Duration,
    /// How long a client waits for an answer.
    pub timeout: Duration,
    /// Whether clients send each request to a node drawn at random, whose
    /// gateway routes it to the primary, rather than to the primary
    /// itself. A forwarded write may still apply after the gateway gave up
    /// on it or crashed, so writes that fail or reach a node that crashes
    /// are recorded as unknown.
    pub any_gateway: bool,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            clients: 4,
            operations: 40,
            keys: 4,
            max_body: 2048,
            think_time: Duration::from_millis(200),
            timeout: Duration::from_secs(2),
            any_gateway: false,
        }
    }
}

/// Where the workload sends each key: the bucket documents, each shard's
/// configuration, whose primary serves it, and the nodes clients know.
#[derive(Clone, Debug)]
pub(crate) struct Routes {
    pub buckets: Vec<BucketDocument>,
    pub placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    /// The nodes in the cluster from the start, which any-gateway clients
    /// draw from: not those that join later
    /// ([`ClusterConfig::joining`](crate::ClusterConfig::joining)).
    pub nodes: Vec<NodeId>,
}

impl Routes {
    /// The host of the primary of `key` in `bucket`.
    fn primary(&self, bucket: &BucketDocument, key: &str) -> String {
        let shard = ShardRef::for_key(bucket, key);
        self.placement[&shard].primary.to_string()
    }

    /// Every key the workload may touch, as the history names it, with
    /// its bucket and object key.
    pub(crate) fn keys(&self, keys: usize) -> Vec<(String, BucketDocument, String)> {
        let mut all = Vec::new();
        for bucket in &self.buckets {
            for n in 0..keys {
                let key = format!("key-{n}");
                all.push((history_key(bucket, &key), bucket.clone(), key));
            }
        }
        all
    }
}

fn history_key(bucket: &BucketDocument, key: &str) -> String {
    format!("{}/{key}", bucket.name)
}

/// The ETag S3 gives a single-part body: its MD5, in hex.
pub(crate) fn etag_of(body: &[u8]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in Md5::digest(body) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The ETag S3 gives a multipart object of the single part `body`: the
/// MD5 of the part's MD5, in hex, and the part count.
pub(crate) fn multipart_etag_of(body: &[u8]) -> String {
    let mut hex = String::with_capacity(34);
    for byte in Md5::digest(Md5::digest(body)) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex.push_str("-1");
    hex
}

/// Whether `body` is the object `etag` names, of a single-part `PUT` or a
/// one-part multipart upload.
fn body_matches(body: &[u8], etag: &str) -> bool {
    if etag.ends_with("-1") {
        multipart_etag_of(body) == etag
    } else {
        etag_of(body) == etag
    }
}

/// The text of the first `<tag>` element in `xml`.
fn element<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&format!("</{tag}>"))?;
    Some(&xml[start..end])
}

/// An ETag header without its quotes.
fn unquote(etag: &str) -> &str {
    etag.trim_matches('"')
}

#[cfg(test)]
#[test]
fn etags_are_md5_in_hex() {
    assert_eq!(etag_of(b""), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(unquote("\"abc\""), "abc");
    assert_eq!(unquote("abc"), "abc");
}

#[cfg(test)]
#[test]
fn multipart_etags_digest_the_part_digests() {
    // The MD5 of the empty part's 16-byte MD5.
    assert_eq!(multipart_etag_of(b""), "59adb24ef3cdbe0297f05b395827453f-1");
    assert!(body_matches(b"", &multipart_etag_of(b"")));
    assert!(body_matches(b"", &etag_of(b"")));
    assert!(!body_matches(b"x", &multipart_etag_of(b"")));
    let xml = "<R><UploadId>abc</UploadId><UploadId>d</UploadId></R>";
    assert_eq!(element(xml, "UploadId"), Some("abc"));
    assert_eq!(element(xml, "Key"), None);
}

/// Whether `response` refused a body that took longer to stream than the
/// gateway allows (§10.3), as one a fault held up may.
fn timed_out(response: &Response<Bytes>) -> bool {
    response.status() == StatusCode::BAD_REQUEST
        && element(&String::from_utf8_lossy(response.body()), "Code") == Some("RequestTimeout")
}

/// An answer the workload does not expect, which fails the simulation.
fn unexpected(what: &str, response: &Response<Bytes>) -> Box<dyn std::error::Error> {
    format!(
        "{what} got {}: {}",
        response.status(),
        String::from_utf8_lossy(response.body())
    )
    .into()
}

/// Where clients record how long their writes took, and how many of
/// their `GET`s broke off mid-body. Clones share the record.
#[derive(Clone, Debug, Default)]
pub(crate) struct Timings {
    writes: Arc<Mutex<Vec<WriteTiming>>>,
    broken: Arc<AtomicUsize>,
}

impl Timings {
    fn push(&self, timing: WriteTiming) {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(timing);
    }

    /// Every write recorded so far, leaving none.
    pub(crate) fn take(&self) -> Vec<WriteTiming> {
        std::mem::take(&mut *self.writes.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// How many `GET`s were answered and then broke off mid-body.
    pub(crate) fn broken(&self) -> usize {
        self.broken.load(Ordering::Relaxed)
    }
}

/// The simulated time since the run began.
fn elapsed() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}

/// One client of the workload.
pub(crate) struct Client {
    pub process: String,
    pub routes: Routes,
    pub history: History,
    pub workload: Workload,
    pub rng: SmallRng,
    /// The last value this client saw for each key, for `If-Match`.
    seen: BTreeMap<String, String>,
    /// Where the client records its writes' times, if anywhere.
    timings: Option<Timings>,
}

impl Client {
    pub(crate) fn new(
        process: String,
        routes: Routes,
        history: History,
        workload: Workload,
        seed: u64,
    ) -> Self {
        Self {
            process,
            routes,
            history,
            workload,
            rng: SmallRng::seed_from_u64(seed),
            seen: BTreeMap::new(),
            timings: None,
        }
    }

    /// The same client, recording how long each write takes in `timings`.
    pub(crate) fn with_timings(mut self, timings: Timings) -> Self {
        self.timings = Some(timings);
        self
    }

    /// Records a write to `key` in `bucket` sent at `sent` that ended with
    /// `outcome` now. A write that never connected is recorded too, as
    /// unanswered: the history leaves it out, since it reached no node, but
    /// the client waited for it all the same.
    fn time(&self, bucket: &BucketDocument, key: &str, sent: Duration, outcome: &Outcome) {
        if let Some(timings) = &self.timings {
            timings.push(WriteTiming {
                shard: ShardRef::for_key(bucket, key),
                sent,
                took: elapsed().saturating_sub(sent),
                answered: matches!(outcome, Outcome::Done | Outcome::ConditionFailed),
            });
        }
    }

    /// Sends the client's operations, one at a time.
    pub(crate) async fn run(mut self) -> turmoil::Result {
        for n in 0..self.workload.operations {
            let pause = self
                .rng
                .random_range(Duration::ZERO..=self.workload.think_time);
            tokio::time::sleep(pause).await;
            let bucket = self.rng.random_range(0..self.routes.buckets.len());
            let bucket = self.routes.buckets[bucket].clone();
            let key = format!("key-{}", self.rng.random_range(0..self.workload.keys));
            match self.rng.random_range(0..100) {
                0..24 => self.put(&bucket, &key, n, Condition::None).await?,
                24..27 => self.multipart(&bucket, &key, n, Condition::None).await?,
                27..30 => {
                    self.multipart(&bucket, &key, n, Condition::IfAbsent)
                        .await?;
                }
                30..38 => self.put(&bucket, &key, n, Condition::IfAbsent).await?,
                38..46 => {
                    let expected = self.seen.get(&history_key(&bucket, &key)).cloned();
                    let expected = expected.unwrap_or_else(|| etag_of(b"never written"));
                    self.put(&bucket, &key, n, Condition::IfMatch(expected))
                        .await?;
                }
                46..76 => {
                    self.read(&bucket, &key, Method::GET).await?;
                }
                76..84 => {
                    self.read(&bucket, &key, Method::HEAD).await?;
                }
                _ => self.delete(&bucket, &key).await?,
            }
        }
        Ok(())
    }

    /// The node to send a request on `key` in `bucket` to: its primary,
    /// or any node with [`Workload::any_gateway`].
    fn host(&mut self, bucket: &BucketDocument, key: &str) -> String {
        if self.workload.any_gateway {
            let nodes = &self.routes.nodes;
            nodes[self.rng.random_range(0..nodes.len())].to_string()
        } else {
            self.routes.primary(bucket, key)
        }
    }

    /// The outcome of a write answered with a server error: failed, or
    /// unknown if a gateway may have forwarded it.
    fn write_failed(&self) -> Outcome {
        if self.workload.any_gateway {
            Outcome::Unknown
        } else {
            Outcome::Failed
        }
    }

    /// Sends `request` to `host` without recording it.
    async fn send(
        &self,
        host: &str,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Bytes>, NoAnswer> {
        let timeout = self.workload.timeout;
        let answer = match s3::connect(host, timeout).await {
            Ok(connection) => connection.send(request, timeout).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &answer {
            tracing::debug!(process = %self.process, %host, %error, "no answer");
        }
        answer
    }

    /// Sends `request` to `host` and records it as `call` on `key` once it
    /// has a connection. Returns `None`, recording nothing, if no
    /// connection was made: the request then reached no node.
    async fn send_recorded(
        &self,
        host: &str,
        key: &str,
        call: Call,
        request: Request<Full<Bytes>>,
    ) -> Option<(Pending, Result<Response<Bytes>, NoAnswer>)> {
        let timeout = self.workload.timeout;
        let connection = match s3::connect(host, timeout).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::debug!(process = %self.process, %host, %error, "no connection");
                return None;
            }
        };
        // A request the gateway forwards can take effect in another
        // node's life, so only one sent to the primary ends with its
        // receiver's.
        let pending = if self.workload.any_gateway {
            self.history.call(&self.process, key, call)
        } else {
            self.history.call_to(&self.process, host, key, call)
        };
        let answer = connection.send(request, timeout).await;
        if let Err(error) = &answer {
            tracing::debug!(process = %self.process, %host, %error, "no answer");
        }
        Some((pending, answer))
    }

    /// A body no other write uses: the client, the operation, and filler.
    fn body(&mut self, n: usize) -> Bytes {
        let mut body = format!("{} operation {n}:", self.process).into_bytes();
        let len = self
            .rng
            .random_range(body.len()..=self.workload.max_body.max(body.len()));
        body.resize(len, b'a' + (n % 26) as u8);
        Bytes::from(body)
    }

    async fn put(
        &mut self,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
        condition: Condition,
    ) -> turmoil::Result {
        let body = self.body(n);
        let value = etag_of(&body);
        let mut request = Request::put(format!("/{}/{key}", bucket.name));
        match &condition {
            Condition::None => {}
            Condition::IfAbsent => request = request.header("if-none-match", "*"),
            Condition::IfMatch(expected) => {
                request = request.header("if-match", format!("\"{expected}\""));
            }
        }
        let request = request.body(Full::new(body))?;
        let name = history_key(bucket, key);
        let call = Call::Put {
            value: value.clone(),
            condition: condition.clone(),
        };
        let host = self.host(bucket, key);
        let sent = elapsed();
        let Some((pending, answer)) = self.send_recorded(&host, &name, call, request).await else {
            self.time(bucket, key, sent, &Outcome::Unknown);
            return Ok(());
        };
        let outcome = match answer {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    self.seen.insert(name, value);
                    Outcome::Done
                }
                StatusCode::PRECONDITION_FAILED if condition != Condition::None => {
                    Outcome::ConditionFailed
                }
                // S3 answers an If-Match on a missing key with 404 (§7.2).
                StatusCode::NOT_FOUND if matches!(condition, Condition::IfMatch(_)) => {
                    Outcome::ConditionFailed
                }
                status if status.is_server_error() => self.write_failed(),
                _ if timed_out(&response) => self.write_failed(),
                _ => return Err(unexpected("a PUT", &response)),
            },
            Err(_) => Outcome::Unknown,
        };
        self.time(bucket, key, sent, &outcome);
        self.history.answer(pending, outcome);
        Ok(())
    }

    /// Uploads a body as a one-part multipart upload and completes it with
    /// `condition`, which the history records as a `PUT` of its multipart
    /// ETag. Creating the upload and sending its part are not recorded:
    /// neither changes what a read sees.
    async fn multipart(
        &mut self,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
        condition: Condition,
    ) -> turmoil::Result {
        let host = self.host(bucket, key);
        let path = format!("/{}/{key}", bucket.name);
        let request = Request::post(format!("{path}?uploads")).body(Full::default())?;
        let response = match self.send(&host, request).await {
            Ok(response) if response.status().is_server_error() => return Ok(()),
            Ok(response) if response.status() == StatusCode::OK => response,
            Ok(response) => return Err(unexpected("a CreateMultipartUpload", &response)),
            Err(_) => return Ok(()),
        };
        let body = String::from_utf8_lossy(response.body());
        let Some(id) = element(&body, "UploadId").map(str::to_owned) else {
            return Err(unexpected(
                "a CreateMultipartUpload without an UploadId",
                &response,
            ));
        };

        let part = self.body(n);
        let part_etag = etag_of(&part);
        let value = multipart_etag_of(&part);
        let request =
            Request::put(format!("{path}?partNumber=1&uploadId={id}")).body(Full::new(part))?;
        match self.send(&host, request).await {
            Ok(response) if response.status().is_server_error() || timed_out(&response) => {
                return Ok(());
            }
            Ok(response) if response.status() == StatusCode::OK => {}
            Ok(response) => return Err(unexpected("an UploadPart", &response)),
            Err(_) => return Ok(()),
        }

        let completion = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
             <ETag>\"{part_etag}\"</ETag></Part></CompleteMultipartUpload>"
        );
        let mut request = Request::post(format!("{path}?uploadId={id}"));
        if condition == Condition::IfAbsent {
            request = request.header("if-none-match", "*");
        }
        let request = request.body(Full::new(Bytes::from(completion)))?;
        let name = history_key(bucket, key);
        let call = Call::Put {
            value: value.clone(),
            condition: condition.clone(),
        };
        let sent = elapsed();
        let Some((pending, answer)) = self.send_recorded(&host, &name, call, request).await else {
            self.time(bucket, key, sent, &Outcome::Unknown);
            return Ok(());
        };
        let outcome = match answer {
            Ok(response) => match response.status() {
                StatusCode::OK if String::from_utf8_lossy(response.body()).contains(&value) => {
                    self.seen.insert(name, value);
                    Outcome::Done
                }
                StatusCode::PRECONDITION_FAILED if condition != Condition::None => {
                    Outcome::ConditionFailed
                }
                status if status.is_server_error() => self.write_failed(),
                _ => return Err(unexpected("a CompleteMultipartUpload", &response)),
            },
            Err(_) => Outcome::Unknown,
        };
        self.time(bucket, key, sent, &outcome);
        self.history.answer(pending, outcome);
        Ok(())
    }

    /// Reads `key` with `GET` or `HEAD` and records the answer. Returns
    /// whether the answer was definite.
    pub(crate) async fn read(
        &mut self,
        bucket: &BucketDocument,
        key: &str,
        method: Method,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let request = Request::builder()
            .method(method.clone())
            .uri(format!("/{}/{key}", bucket.name))
            .body(Full::new(Bytes::new()))?;
        let name = history_key(bucket, key);
        let host = self.host(bucket, key);
        let Some((pending, answer)) = self.send_recorded(&host, &name, Call::Get, request).await
        else {
            return Ok(false);
        };
        let outcome = match answer {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let etag = response
                        .headers()
                        .get("etag")
                        .and_then(|etag| etag.to_str().ok())
                        .map(|etag| unquote(etag).to_owned())
                        .ok_or_else(|| unexpected("a read without an ETag", &response))?;
                    if method == Method::GET && !body_matches(response.body(), &etag) {
                        return Err(unexpected("a GET whose body does not match", &response));
                    }
                    self.seen.insert(name, etag.clone());
                    Outcome::Read(Some(etag))
                }
                StatusCode::NOT_FOUND => Outcome::Read(None),
                status if status.is_server_error() => Outcome::Failed,
                _ => return Err(unexpected("a read", &response)),
            },
            Err(NoAnswer::Broken(_)) => {
                if let Some(timings) = &self.timings {
                    timings.broken.fetch_add(1, Ordering::Relaxed);
                }
                Outcome::Unknown
            }
            Err(_) => Outcome::Unknown,
        };
        let definite = matches!(outcome, Outcome::Read(_));
        self.history.answer(pending, outcome);
        Ok(definite)
    }

    async fn delete(&mut self, bucket: &BucketDocument, key: &str) -> turmoil::Result {
        let request = Request::delete(format!("/{}/{key}", bucket.name)).body(Full::default())?;
        let name = history_key(bucket, key);
        let host = self.host(bucket, key);
        let sent = elapsed();
        let Some((pending, answer)) = self
            .send_recorded(&host, &name, Call::Delete, request)
            .await
        else {
            self.time(bucket, key, sent, &Outcome::Unknown);
            return Ok(());
        };
        let outcome = match answer {
            Ok(response) => match response.status() {
                StatusCode::OK | StatusCode::NO_CONTENT => Outcome::Done,
                status if status.is_server_error() => self.write_failed(),
                _ => return Err(unexpected("a DELETE", &response)),
            },
            Err(_) => Outcome::Unknown,
        };
        self.time(bucket, key, sent, &outcome);
        self.history.answer(pending, outcome);
        Ok(())
    }

    /// Reads every key from its primary until each gives a definite
    /// answer, after the faults have healed: the reads that close the
    /// history.
    pub(crate) async fn read_all(mut self, attempts: usize) -> turmoil::Result {
        for (_, bucket, key) in self.routes.keys(self.workload.keys) {
            let mut answered = false;
            for _ in 0..attempts {
                if self.read(&bucket, &key, Method::GET).await? {
                    answered = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if !answered {
                return Err(format!("the primary of {}/{key} never answered", bucket.name).into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use skys3_control::ProposalIds;
    use skys3_types::{BucketId, BucketMode, BucketName, ShardCount};

    use super::*;

    /// A write whose connection never opens still counts against the
    /// client's latency: it is timed, as unanswered, for the whole time
    /// the client spent trying.
    #[test]
    fn writes_that_never_connect_are_timed_as_unanswered() -> turmoil::Result {
        const TIMEOUT: Duration = Duration::from_secs(2);
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .build();
        // A node whose messages from the client are held, never delivered.
        sim.host("node-0", || async {
            std::future::pending::<()>().await;
            Ok(())
        });
        let timings = Timings::default();
        let recorded = timings.clone();
        sim.client("client", async move {
            let bucket = BucketDocument {
                bucket_id: BucketId::new("b-test")?,
                name: BucketName::new("bucket-test")?,
                mode: BucketMode::Local,
                shards: ShardCount::new(1)?,
                replicas: 1,
                min_write_replicas: 1,
                clean_copies: 1,
                target: None,
                created_unix_ms: 0,
                proposal_id: ProposalIds::seeded(1).next_id(),
            };
            let routes = Routes {
                buckets: vec![bucket.clone()],
                placement: Arc::new(BTreeMap::new()),
                nodes: vec![NodeId::new("node-0")?],
            };
            let workload = Workload {
                timeout: TIMEOUT,
                any_gateway: true,
                ..Workload::default()
            };
            let mut client = Client::new("client".to_owned(), routes, History::new(), workload, 1)
                .with_timings(timings);
            turmoil::hold("client", "node-0");
            client.put(&bucket, "key-0", 0, Condition::None).await?;
            client
                .multipart(&bucket, "key-0", 1, Condition::None)
                .await?;
            client.delete(&bucket, "key-0").await?;
            Ok(())
        });
        sim.run()?;
        let writes = recorded.take();
        // The multipart upload never got as far as its completion, the
        // only part of it that is a write.
        assert_eq!(writes.len(), 2, "{writes:?}");
        for write in &writes {
            assert!(!write.answered, "{write:?}");
            assert!(write.took >= TIMEOUT, "{write:?}");
        }
        Ok(())
    }
}
