//! Read-only origin buckets (plan M4-12, design §9.5): two `read_only`
//! buckets over one origin, the remote store's `origin/` prefix, read
//! with two credential scopes, while an out-of-band writer changes the
//! origin and clients read through every node.
//!
//! - **The buckets.** `origin-full` reads the origin with the credentials
//!   [`FULL`], which may read every key, and `origin-guest` with [`GUEST`],
//!   which the origin refuses the keys the writer denies it. Both have the
//!   same [`OriginFreshness`]. Their shards are placed and replicated like
//!   the cluster's other buckets, and every node's gateway reads origins
//!   through the node binary's own reads (`skys3::remote::NodeRemote`) and
//!   fills its cache through the flush service.
//! - **The writer** stores every key at the start, then changes a random
//!   key every [`Origin::change_every`]: a new body, a delete, or a denial
//!   of the key to [`GUEST`] lifted or imposed. It writes the remote store
//!   directly, without faults or delays, so each change applies at the
//!   moment it records.
//! - **The readers** send `GET`s and `HEAD`s of random keys of either
//!   bucket, each to a random node, and record when each began and ended
//!   and what it answered. A body must have the MD5 its ETag names.
//! - **The stager** stages the two races random interleavings seldom
//!   reach: a key cached on its primary and then overwritten at the origin
//!   (a read that skips revalidation serves the old copy), and a key
//!   cached by both buckets, then denied to [`GUEST`] while `origin-full`
//!   keeps reading it (an answer kept without its credential scope serves
//!   `origin-guest` past its denial).
//!
//! **The check** ([`audit`]): each answer must be one the origin, as the
//! reader's credentials see it, gave at some moment from the read's start,
//! less the TTL under `freshness = "ttl"`, to its end: the ETag it held,
//! no object for `404`, a denial for `403`. Reads that got `503` or no
//! answer are allowed.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use http_body_util::Full;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_gateway::ShardRef;
use skys3_remote::{DeleteObject, ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_types::{BucketDocument, NodeId, ShardConfig};

use crate::s3;
use crate::workload::etag_of;

/// The credentials that may read every key of the origin.
pub const FULL: &str = "full";

/// The credentials the origin refuses the keys the writer denies them.
pub const GUEST: &str = "guest";

/// The origin's key prefix in the remote store.
pub(crate) const ORIGIN_PREFIX: &str = "origin/";

/// The keys the stager uses, which the writer never touches.
const STAGED_CONTENT: &str = "staged-content";
const STAGED_DENIAL: &str = "staged-denial";

/// How the buckets' reads check the origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginFreshness {
    /// `freshness = "revalidate"`.
    Revalidate,
    /// `freshness = "ttl"` with this many seconds.
    Ttl(u64),
}

impl OriginFreshness {
    /// How stale an answer may be.
    #[must_use]
    pub fn staleness(self) -> Duration {
        match self {
            Self::Revalidate => Duration::ZERO,
            Self::Ttl(seconds) => Duration::from_secs(seconds),
        }
    }
}

/// A bug seeded into every gateway's reads of the origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginBug {
    /// Under `revalidate`, a read is served from a cached copy without
    /// asking the origin.
    SkipRevalidation,
    /// What a gateway learns of the origin is keyed without the scope of
    /// the credentials it was read with.
    IgnoreCredentials,
}

/// The read-only buckets of a run, their writer, and their readers.
#[derive(Clone, Debug, PartialEq)]
pub struct Origin {
    /// How the buckets' reads check the origin.
    pub freshness: OriginFreshness,
    /// The keys the writer changes and the readers read.
    pub keys: usize,
    /// How many changes the writer makes.
    pub changes: usize,
    /// How long the writer waits between changes.
    pub change_every: Duration,
    /// Concurrent readers.
    pub readers: usize,
    /// Reads each reader sends.
    pub reads: usize,
    /// How long a reader waits for an answer.
    pub timeout: Duration,
    /// A bug seeded into every gateway.
    pub bug: Option<OriginBug>,
}

impl Default for Origin {
    fn default() -> Self {
        Self {
            freshness: OriginFreshness::Revalidate,
            keys: 4,
            changes: 40,
            change_every: Duration::from_millis(150),
            readers: 3,
            reads: 60,
            timeout: Duration::from_secs(4),
            bug: None,
        }
    }
}

impl Origin {
    /// The `[buckets.<name>]` tables of the two buckets.
    pub(crate) fn tables(&self) -> String {
        let freshness = match self.freshness {
            OriginFreshness::Revalidate => "freshness = \"revalidate\"\n".to_owned(),
            OriginFreshness::Ttl(seconds) => {
                format!("freshness = \"ttl\"\nfreshness_ttl_seconds = {seconds}\n")
            }
        };
        [FULL, GUEST]
            .iter()
            .map(|credentials| {
                format!(
                    "[buckets.origin-{credentials}]\nmode = \"read_only\"\n{freshness}\
                     origin_profile = \"{credentials}\"\n"
                )
            })
            .collect()
    }
}

/// What the audit of a run found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OriginAudit {
    /// Reads sent.
    pub reads: u64,
    /// Reads answered with an object.
    pub served: u64,
    /// Reads answered `404`.
    pub missing: u64,
    /// Reads answered `403`.
    pub denied: u64,
    /// Reads answered `503`, or not at all.
    pub unavailable: u64,
    /// Reads answered with a version the origin had already replaced when
    /// they began: what a TTL allows.
    pub stale: u64,
    /// Changes the writer made, the initial objects included.
    pub changes: u64,
}

/// One change of the origin.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Change {
    /// An object with this ETag.
    Put(String),
    /// No object.
    Delete,
    /// Reads with [`GUEST`] refused, or allowed again.
    DenyGuest(bool),
}

/// What a read was answered.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Answer {
    /// An object with this ETag.
    Served(String),
    /// `404`.
    Missing,
    /// `403`.
    Denied,
    /// `503`, or no answer.
    Unavailable,
}

/// One read.
#[derive(Clone, Debug)]
struct Read {
    credentials: &'static str,
    key: String,
    start: Duration,
    end: Duration,
    answer: Answer,
    node: String,
}

/// What a run of the origin's clients recorded.
#[derive(Debug, Default)]
pub(crate) struct OriginLog {
    changes: Mutex<Vec<(Duration, String, Change)>>,
    reads: Mutex<Vec<Read>>,
    /// Answers no read may get, such as a body that is not its ETag's.
    wrong: Mutex<Vec<String>>,
}

impl OriginLog {
    fn change(&self, key: &str, change: Change) {
        let mut changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
        changes.push((elapsed(), key.to_owned(), change));
    }

    fn read(&self, read: Read) {
        self.reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(read);
    }

    fn wrong(&self, what: String) {
        self.wrong
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(what);
    }
}

/// The simulated time since the run began.
fn elapsed() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}

/// Where the clients send their requests.
#[derive(Clone, Debug)]
pub(crate) struct OriginRoutes {
    /// `origin-full` and `origin-guest`.
    pub buckets: Vec<BucketDocument>,
    pub placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    pub nodes: Vec<NodeId>,
}

impl OriginRoutes {
    fn bucket(&self, credentials: &str) -> &BucketDocument {
        let name = format!("origin-{credentials}");
        self.buckets
            .iter()
            .find(|bucket| bucket.name.as_str() == name)
            .expect("both origin buckets exist")
    }

    /// The node of the primary of `key` in the bucket read with
    /// `credentials`, which fills it.
    fn primary(&self, credentials: &str, key: &str) -> String {
        let shard = ShardRef::for_key(self.bucket(credentials), key);
        self.placement[&shard].primary.to_string()
    }
}

/// The writer: every key at the start, then a change of a random key every
/// `change_every`.
pub(crate) async fn write(
    origin: Origin,
    remote: SimS3,
    log: Arc<OriginLog>,
    seed: u64,
) -> turmoil::Result {
    let mut rng = SmallRng::seed_from_u64(seed);
    for n in 0..origin.keys {
        let key = format!("key-{n}");
        put(&remote, &log, &key, &body(&key, 0, &mut rng)).await?;
    }
    for change in 1..=origin.changes {
        tokio::time::sleep(origin.change_every).await;
        let key = format!("key-{}", rng.random_range(0..origin.keys));
        match rng.random_range(0..100) {
            0..60 => put(&remote, &log, &key, &body(&key, change, &mut rng)).await?,
            60..75 => {
                remote
                    .delete_object(DeleteObject::new(format!("{ORIGIN_PREFIX}{key}")))
                    .await?;
                log.change(&key, Change::Delete);
            }
            _ => {
                let denied = rng.random_bool(0.5);
                deny(&remote, &log, &key, denied);
            }
        }
    }
    Ok(())
}

/// A body of `key` for change `change`: inline in its record or a few
/// extents long, from 1 to 2,000 bytes.
fn body(key: &str, change: usize, rng: &mut SmallRng) -> Vec<u8> {
    let stamp = format!("{key}@{change};");
    let len = rng.random_range(1..=2000);
    stamp.bytes().cycle().take(len.max(stamp.len())).collect()
}

async fn put(remote: &SimS3, log: &OriginLog, key: &str, body: &[u8]) -> turmoil::Result {
    let request = PutObject::new(
        format!("{ORIGIN_PREFIX}{key}"),
        Bytes::copy_from_slice(body),
    );
    remote.put_object(request).await?;
    log.change(key, Change::Put(etag_of(body)));
    Ok(())
}

/// Refuses `key` to [`GUEST`], or allows it again.
fn deny(remote: &SimS3, log: &OriginLog, key: &str, denied: bool) {
    remote.deny(GUEST, &format!("{ORIGIN_PREFIX}{key}"), denied);
    log.change(key, Change::DenyGuest(denied));
}

/// One reader: `reads` reads of random keys of either bucket, each to a
/// random node.
pub(crate) async fn read(
    origin: Origin,
    routes: OriginRoutes,
    log: Arc<OriginLog>,
    seed: u64,
) -> turmoil::Result {
    let mut rng = SmallRng::seed_from_u64(seed);
    for _ in 0..origin.reads {
        tokio::time::sleep(Duration::from_millis(rng.random_range(10..100))).await;
        let credentials = if rng.random_bool(0.5) { FULL } else { GUEST };
        let key = format!("key-{}", rng.random_range(0..origin.keys));
        let node = routes.nodes[rng.random_range(0..routes.nodes.len())].to_string();
        let method = if rng.random_range(0..100) < 80 {
            Method::GET
        } else {
            Method::HEAD
        };
        send(&routes, &log, &origin, credentials, &key, &node, method).await?;
    }
    Ok(())
}

/// The stager: the two races of the module documentation, on keys of their
/// own, through chosen nodes.
pub(crate) async fn stage(
    origin: Origin,
    remote: SimS3,
    routes: OriginRoutes,
    log: Arc<OriginLog>,
    seed: u64,
) -> turmoil::Result {
    let mut rng = SmallRng::seed_from_u64(seed);
    let ttl = origin.freshness.staleness();
    let get = async |credentials: &'static str, key: &str, node: &str| {
        send(&routes, &log, &origin, credentials, key, node, Method::GET).await
    };
    // A copy cached on its primary, then replaced at the origin: every
    // later read must see the new version, within the TTL.
    put(
        &remote,
        &log,
        STAGED_CONTENT,
        &body(STAGED_CONTENT, 0, &mut rng),
    )
    .await?;
    let primary = routes.primary(FULL, STAGED_CONTENT);
    get(FULL, STAGED_CONTENT, &primary).await?;
    put(
        &remote,
        &log,
        STAGED_CONTENT,
        &body(STAGED_CONTENT, 1, &mut rng),
    )
    .await?;
    tokio::time::sleep(ttl + Duration::from_millis(10)).await;
    for node in &routes.nodes {
        get(FULL, STAGED_CONTENT, node.as_str()).await?;
    }
    // A key both buckets cached, denied to one of them, which then must
    // not be served past the TTL however often the other reads it.
    put(
        &remote,
        &log,
        STAGED_DENIAL,
        &body(STAGED_DENIAL, 0, &mut rng),
    )
    .await?;
    for credentials in [FULL, GUEST] {
        let primary = routes.primary(credentials, STAGED_DENIAL);
        get(credentials, STAGED_DENIAL, &primary).await?;
    }
    deny(&remote, &log, STAGED_DENIAL, true);
    let step = (ttl / 3).max(Duration::from_millis(100));
    let full = routes.primary(FULL, STAGED_DENIAL);
    let guest = routes.primary(GUEST, STAGED_DENIAL);
    for round in 0..9 {
        get(FULL, STAGED_DENIAL, &full).await?;
        if round >= 6 {
            get(GUEST, STAGED_DENIAL, &guest).await?;
        }
        tokio::time::sleep(step).await;
    }
    Ok(())
}

/// Sends one read of `key` with `credentials` to `node`, and records it.
async fn send(
    routes: &OriginRoutes,
    log: &OriginLog,
    origin: &Origin,
    credentials: &'static str,
    key: &str,
    node: &str,
    method: Method,
) -> turmoil::Result {
    let bucket = routes.bucket(credentials);
    let request = Request::builder()
        .method(method.clone())
        .uri(format!("/{}/{key}", bucket.name))
        .body(Full::default())?;
    let start = elapsed();
    let answered = match s3::connect(node, origin.timeout).await {
        Ok(connection) => connection.send(request, origin.timeout).await,
        Err(error) => Err(error),
    };
    let answer = match answered {
        Ok(response) => match response.status() {
            StatusCode::OK => {
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.trim_matches('"').to_owned())
                    .ok_or("an object without an ETag")?;
                if method == Method::GET && etag_of(response.body()) != etag {
                    log.wrong(format!(
                        "a GET of {key} with {credentials} through {node} got bytes that are \
                         not those of their ETag {etag}"
                    ));
                }
                Answer::Served(etag)
            }
            StatusCode::NOT_FOUND => Answer::Missing,
            StatusCode::FORBIDDEN => Answer::Denied,
            status if status.is_server_error() => Answer::Unavailable,
            status => {
                log.wrong(format!("a read of {key} answered {status}"));
                Answer::Unavailable
            }
        },
        Err(_) => Answer::Unavailable,
    };
    log.read(Read {
        credentials,
        key: key.to_owned(),
        start,
        end: elapsed(),
        answer,
        node: node.to_owned(),
    });
    Ok(())
}

/// The origin as `credentials` see one key: its object's ETag, and whether
/// they are refused it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Seen {
    etag: Option<String>,
    denied: bool,
}

/// Checks every read against the origin's history (module docs).
///
/// # Errors
///
/// The first read whose answer the origin never gave in its window.
pub(crate) fn audit(origin: &Origin, log: &OriginLog) -> Result<OriginAudit, String> {
    if let Some(wrong) = log
        .wrong
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .first()
    {
        return Err(wrong.clone());
    }
    let changes = log
        .changes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let reads = log
        .reads
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    // Each key's states, from no object, each with the time it began.
    let mut timelines: BTreeMap<&str, Vec<(Duration, Option<String>, bool)>> = BTreeMap::new();
    for (at, key, change) in &changes {
        let timeline = timelines
            .entry(key.as_str())
            .or_insert_with(|| vec![(Duration::ZERO, None, false)]);
        let (_, etag, denied) = timeline.last().cloned().expect("never empty");
        timeline.push(match change {
            Change::Put(etag) => (*at, Some(etag.clone()), denied),
            Change::Delete => (*at, None, denied),
            Change::DenyGuest(denied) => (*at, etag, *denied),
        });
    }
    let staleness = origin.freshness.staleness();
    let mut audit = OriginAudit {
        changes: changes.len() as u64,
        ..OriginAudit::default()
    };
    let initial = vec![(Duration::ZERO, None, false)];
    for read in &reads {
        audit.reads += 1;
        let timeline = timelines.get(read.key.as_str()).unwrap_or(&initial);
        let seen = |(_, etag, denied): &(Duration, Option<String>, bool)| Seen {
            etag: etag.clone(),
            denied: *denied && read.credentials == GUEST,
        };
        let from = read.start.saturating_sub(staleness);
        // The state in force just before the window, and every one that
        // began in it.
        let before = timeline
            .iter()
            .rposition(|(at, ..)| *at < from)
            .unwrap_or(0);
        let window: Vec<Seen> = timeline[before..]
            .iter()
            .take_while(|(at, ..)| *at <= read.end)
            .map(seen)
            .collect();
        // The states from the read's own start: an answer that needs an
        // earlier one used the TTL.
        let since_start = timeline
            .iter()
            .rposition(|(at, ..)| *at < read.start)
            .unwrap_or(0);
        let during: Vec<Seen> = timeline[since_start..]
            .iter()
            .take_while(|(at, ..)| *at <= read.end)
            .map(seen)
            .collect();
        let gave = |matches: &dyn Fn(&Seen) -> bool| window.iter().any(matches);
        let given = match &read.answer {
            Answer::Served(etag) => {
                audit.served += 1;
                if !during
                    .iter()
                    .any(|seen| !seen.denied && seen.etag.as_ref() == Some(etag))
                {
                    audit.stale += 1;
                }
                gave(&|seen| !seen.denied && seen.etag.as_ref() == Some(etag))
            }
            Answer::Missing => {
                audit.missing += 1;
                gave(&|seen| !seen.denied && seen.etag.is_none())
            }
            Answer::Denied => {
                audit.denied += 1;
                gave(&|seen| seen.denied)
            }
            Answer::Unavailable => {
                audit.unavailable += 1;
                true
            }
        };
        if !given {
            let mut states = String::new();
            for (at, etag, denied) in timeline {
                let _ = write!(states, " {at:?}: {etag:?} denied {denied};");
            }
            return Err(format!(
                "a read of {} with {} through {} from {:?} to {:?} answered {:?}, which the \
                 origin never gave from {from:?}: its states were{states}",
                read.key, read.credentials, read.node, read.start, read.end, read.answer
            ));
        }
    }
    Ok(audit)
}
