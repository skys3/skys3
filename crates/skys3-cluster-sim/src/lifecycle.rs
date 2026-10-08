//! Lifecycle rules in the simulated cluster (plan M5-10, design §8.7):
//! every node runs lifecycle passes on the shards it leads, a client
//! writes objects and uploads for the rules to expire, and an audit of the
//! final logs checks that each version was expired by exactly one
//! committed `DELETE`, and only if a rule expires it.
//!
//! **Time.** Lifecycle rules count days, and a simulation runs for seconds.
//! The passes therefore read a clock of their own ([`Lifecycle::now_ms`]):
//! lifecycle day 0 begins, at a midnight UTC far in the future
//! ([`ORIGIN_DAY`]), when the workload starts, the moment the times of a
//! [`FaultPlan`](crate::FaultPlan) count from, and one lifecycle day
//! passes every [`Lifecycle::day`] of simulated time. Expirations then
//! fall at chosen moments of a run, between and during faults. Objects
//! keep the wall-clock times the gateway gives them, all long before that
//! origin, so a `Days` rule expires its objects at the first pass, and a
//! `Date` rule ([`Lifecycle::date`]) when its day begins. Either way when
//! an object expires does not depend on when the run happened.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ::http::{Method, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::Full;
use skys3_gateway::{Gateway, ShardRef, TrustAll};
use skys3_io::SimMount;
use skys3_log::record::truncated_by;
use skys3_log::{LogRecord, RecordBody};
use skys3_shard::ShardSet;
use skys3_shard::lifecycle::{LifecycleMetrics, run_pass};
use skys3_types::lifecycle::{DAY_MS, LifecycleConfiguration};
use skys3_types::{BucketDocument, BucketMode, EpochSeq};

use crate::s3::{self, NoAnswer};
use crate::workload::Routes;

/// The day, counted from the Unix epoch, at whose midnight UTC lifecycle
/// day 0 begins: in the 23rd century, long after any object's
/// `Last-Modified`.
pub const ORIGIN_DAY: u64 = 100_000;

/// How many times the client sends each request before it gives up.
const ATTEMPTS: usize = 200;

/// The pause between attempts.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Lifecycle rules in the cluster: the configuration every `local` bucket
/// the harness writes has, how the nodes evaluate it, and what the client
/// writes for it to expire.
#[derive(Clone, Debug, PartialEq)]
pub struct Lifecycle {
    /// The lifecycle configuration of every `local` bucket.
    pub configuration: LifecycleConfiguration,
    /// How often each node runs a pass over the shards it leads.
    pub interval: Duration,
    /// How much simulated time a lifecycle day lasts.
    pub day: Duration,
    /// The objects the client writes to every `local` bucket, through the
    /// nodes in turn.
    pub objects: Vec<LifecycleObject>,
    /// The keys of the multipart uploads the client opens in every `local`
    /// bucket.
    pub uploads: Vec<String>,
    /// When the run's workload started, in simulated time: lifecycle day
    /// 0. Clones share it; the harness sets it.
    pub start: LifecycleStart,
}

/// When lifecycle day 0 began in a run, shared by the clones of a
/// [`Lifecycle`]. It is run state rather than configuration, so any two
/// compare equal.
#[derive(Clone, Debug, Default)]
pub struct LifecycleStart(Arc<Mutex<Option<Duration>>>);

impl LifecycleStart {
    fn set(&self, elapsed: Duration) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(elapsed);
    }

    fn get(&self) -> Option<Duration> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl PartialEq for LifecycleStart {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// An object the lifecycle client writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleObject {
    /// The key.
    pub key: String,
    /// The body's size.
    pub size: usize,
    /// The tags, sent as `x-amz-tagging`: letters and digits only.
    pub tags: BTreeMap<String, String>,
}

impl Lifecycle {
    /// The time at which lifecycle day `day` begins, in milliseconds since
    /// the Unix epoch: a midnight UTC, as a `Date` rule needs.
    #[must_use]
    pub const fn date(day: u64) -> u64 {
        (ORIGIN_DAY + day) * DAY_MS
    }

    /// Starts lifecycle day 0 now, `elapsed` into the simulation.
    pub(crate) fn start(&self, elapsed: Duration) {
        self.start.set(elapsed);
    }

    /// The lifecycle clock now, or `None` before the workload started.
    #[must_use]
    pub fn now_ms(&self) -> Option<u64> {
        self.at(turmoil::sim_elapsed().unwrap_or_default())
    }

    /// The lifecycle clock once the simulation has run for `elapsed`: a
    /// day has passed every [`Lifecycle::day`] since the workload started.
    /// `None` before it started.
    #[must_use]
    pub fn at(&self, elapsed: Duration) -> Option<u64> {
        let since = elapsed.checked_sub(self.start.get()?)?;
        let day = self.day.as_millis().max(1);
        let passed = since.as_millis() * u128::from(DAY_MS) / day;
        Some(Self::date(0).saturating_add(u64::try_from(passed).unwrap_or(u64::MAX)))
    }

    /// When the rules expire `object`, written before lifecycle day 0.
    #[must_use]
    pub fn expiry(&self, object: &LifecycleObject) -> Option<u64> {
        let size = u64::try_from(object.size).unwrap_or(u64::MAX);
        self.configuration
            .expires_at(&object.key, size, &object.tags, 0)
    }

    /// When the rules abort an upload of `key`, opened before lifecycle
    /// day 0.
    #[must_use]
    pub fn abort(&self, key: &str) -> Option<u64> {
        self.configuration.aborts_at(key, 0)
    }

    /// The last expiry or abort of the client's objects and uploads.
    fn last_due(&self) -> u64 {
        let objects = self.objects.iter().filter_map(|object| self.expiry(object));
        let uploads = self.uploads.iter().filter_map(|key| self.abort(key));
        objects.chain(uploads).max().unwrap_or(0)
    }
}

/// Runs a lifecycle pass every [`Lifecycle::interval`] over the shards of
/// `set` this node leads, of the buckets `gateway` knows, as the node
/// binary does, once the workload started.
pub(crate) async fn follow(
    lifecycle: &Lifecycle,
    gateway: &Gateway<TrustAll>,
    set: &ShardSet<SimMount>,
) {
    let metrics = LifecycleMetrics::default();
    loop {
        tokio::time::sleep(lifecycle.interval).await;
        if let Some(now) = lifecycle.now_ms() {
            run_pass(set, &gateway.buckets(), now, &metrics).await;
        }
    }
}

/// The lifecycle client: writes the objects and opens the uploads in every
/// `local` bucket, through the nodes in turn, retrying until each is
/// acknowledged; waits until the last of them is due; then checks through
/// the gateways that exactly the objects and uploads the rules expire are
/// gone.
pub(crate) async fn client(
    lifecycle: Lifecycle,
    routes: Routes,
    timeout: Duration,
) -> turmoil::Result {
    let buckets: Vec<&BucketDocument> = routes
        .buckets
        .iter()
        .filter(|bucket| bucket.mode == BucketMode::Local)
        .collect();
    let nodes = &routes.nodes;
    let mut turn = 0;
    let mut next_host = || {
        turn += 1;
        nodes[turn % nodes.len()].to_string()
    };
    for bucket in &buckets {
        for object in &lifecycle.objects {
            let tagging = object
                .tags
                .iter()
                .fold(String::new(), |mut tagging, (key, value)| {
                    let separator = if tagging.is_empty() { "" } else { "&" };
                    let _ = write!(tagging, "{separator}{key}={value}");
                    tagging
                });
            let path = format!("/{}/{}", bucket.name, object.key);
            let body = Bytes::from(vec![b'x'; object.size]);
            send(&next_host(), timeout, || {
                Request::put(&path)
                    .header("x-amz-tagging", &tagging)
                    .body(Full::new(body.clone()))
            })
            .await
            .and_then(|response| expect(&response, StatusCode::OK))
            .map_err(|error| format!("PUT {path}: {error}"))?;
        }
        for key in &lifecycle.uploads {
            let path = format!("/{}/{key}?uploads", bucket.name);
            send(&next_host(), timeout, || {
                Request::post(&path).body(Full::default())
            })
            .await
            .and_then(|response| expect(&response, StatusCode::OK))
            .map_err(|error| format!("POST {path}: {error}"))?;
        }
    }

    // A pass after the last expiry, and a day for good measure.
    let due = lifecycle.last_due().saturating_add(DAY_MS);
    while lifecycle.now_ms().is_none_or(|now| now < due) {
        tokio::time::sleep(lifecycle.interval).await;
    }

    for bucket in &buckets {
        for object in &lifecycle.objects {
            let gone = lifecycle.expiry(object).is_some();
            let path = format!("/{}/{}", bucket.name, object.key);
            let wanted = if gone {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            };
            eventually(&next_host(), timeout, &path, |response| {
                response.status() == wanted
            })
            .await?;
        }
        let path = format!("/{}?uploads", bucket.name);
        eventually(&next_host(), timeout, &path, |response| {
            let listing = String::from_utf8_lossy(response.body());
            response.status() == StatusCode::OK
                && lifecycle.uploads.iter().all(|key| {
                    let listed = listing.contains(&format!("<Key>{key}</Key>"));
                    listed == lifecycle.abort(key).is_none()
                })
        })
        .await?;
    }
    Ok(())
}

/// Sends the request `build` makes to `host` until it is answered with
/// something other than a server error.
async fn send(
    host: &str,
    timeout: Duration,
    build: impl Fn() -> Result<Request<Full<Bytes>>, ::http::Error>,
) -> Result<Response<Bytes>, String> {
    let mut last = String::from("no attempt");
    for _ in 0..ATTEMPTS {
        let answer = match s3::connect(host, timeout).await {
            Ok(connection) => {
                connection
                    .send(build().map_err(|e| e.to_string())?, timeout)
                    .await
            }
            Err(error) => Err(error),
        };
        match answer {
            Ok(response) if !response.status().is_server_error() => return Ok(response),
            Ok(response) => last = response.status().to_string(),
            Err(NoAnswer::Io(error) | NoAnswer::Broken(error)) => last = error,
            Err(NoAnswer::Timeout) => last = "no answer in time".to_owned(),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    Err(format!(
        "no answer in {ATTEMPTS} attempts; the last: {last}"
    ))
}

fn expect(response: &Response<Bytes>, status: StatusCode) -> Result<Response<Bytes>, String> {
    if response.status() == status {
        Ok(response.clone())
    } else {
        Err(format!(
            "{}: {}",
            response.status(),
            String::from_utf8_lossy(response.body())
        ))
    }
}

/// Sends a `GET` of `path` until `done` accepts its answer, which a pass
/// may still have to bring about.
async fn eventually(
    host: &str,
    timeout: Duration,
    path: &str,
    done: impl Fn(&Response<Bytes>) -> bool,
) -> turmoil::Result {
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let response = send(host, timeout, || {
            Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(Full::default())
        })
        .await?;
        if done(&response) {
            return Ok(());
        }
        last = format!(
            "{}: {}",
            response.status(),
            String::from_utf8_lossy(response.body())
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
    Err(format!("GET {path} never answered as the lifecycle rules say; the last: {last}").into())
}

/// What a version of a key in the audited log became.
#[derive(Clone, Copy, Debug)]
enum Version {
    /// A `PUT` stored it; the rules expire it at this time, if at all.
    Stored(Option<u64>),
    /// A `DELETE` removed it.
    Deleted,
}

/// A shard's records in the final logs, in the order each log holds
/// them.
#[derive(Clone, Debug, Default)]
pub(crate) struct ShardLogs {
    /// The final primary's.
    pub(crate) primary: Vec<LogRecord>,
    /// Each other node's that holds some.
    pub(crate) others: Vec<Vec<LogRecord>>,
}

/// What the audit of the final logs found ([`Report::lifecycle`]).
///
/// [`Report::lifecycle`]: crate::Report::lifecycle
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleAudit {
    /// Object versions expired: committed `DELETE`s of the client's keys.
    pub expired: usize,
    /// Of those, the ones committed in a later epoch than the shard's
    /// first in the log: by a primary that took over, or after its
    /// configuration changed.
    pub in_later_epochs: usize,
    /// `DELETE`s of the client's keys that some node's log holds and the
    /// final primary's does not, or that a `TRUNCATE` invalidated:
    /// appended by a primary that lost its shard before they committed,
    /// their versions expired again by the next.
    pub abandoned: usize,
}

/// Audits the log of each `local` bucket shard's final primary, in
/// `logs`, for the lifecycle client's keys: in position order and leaving
/// out what a `TRUNCATE` invalidated, a `DELETE` only ever follows a `PUT`
/// of a version the rules expire by `end_ms`, so no version is expired
/// twice, and none is that the rules keep. The client's keys have no
/// other writer. Also counts the `DELETE`s other logs hold that the
/// final primary's never committed.
///
/// # Errors
///
/// The first key whose log breaks the rule.
pub(crate) fn audit(
    lifecycle: &Lifecycle,
    logs: &BTreeMap<ShardRef, ShardLogs>,
    buckets: &[BucketDocument],
    end_ms: u64,
) -> Result<LifecycleAudit, String> {
    let mut audit = LifecycleAudit::default();
    for bucket in buckets.iter().filter(|b| b.mode == BucketMode::Local) {
        for object in &lifecycle.objects {
            let shard = ShardRef::for_key(bucket, &object.key);
            let empty = ShardLogs::default();
            let shard_logs = logs.get(&shard).unwrap_or(&empty);
            let records = shard_logs.primary.as_slice();
            let first_epoch = records.iter().map(|record| record.position.epoch).min();
            let (valid, _) = split_truncated(records, &object.key);
            // Every `DELETE` of the key in any log, less those the final
            // primary committed.
            let deletes = |log: &[(EpochSeq, &RecordBody)]| -> BTreeSet<EpochSeq> {
                log.iter()
                    .filter(|(_, body)| matches!(body, RecordBody::Delete(_)))
                    .map(|(position, _)| *position)
                    .collect()
            };
            let mut abandoned = BTreeSet::new();
            for log in std::iter::once(records).chain(shard_logs.others.iter().map(Vec::as_slice)) {
                let (valid, invalid) = split_truncated(log, &object.key);
                abandoned.extend(deletes(&valid));
                abandoned.extend(deletes(&invalid));
            }
            let committed = deletes(&valid);
            audit.abandoned += abandoned.difference(&committed).count();
            let mut version: Option<Version> = None;
            for (position, body) in valid {
                match body {
                    RecordBody::Put(put) => {
                        let at = lifecycle.configuration.expires_at(
                            &put.key,
                            put.size,
                            &put.tags,
                            put.last_modified_ms,
                        );
                        version = Some(Version::Stored(at));
                    }
                    RecordBody::Delete(_) => match version {
                        Some(Version::Stored(Some(at))) if at <= end_ms => {
                            audit.expired += 1;
                            if Some(position.epoch) > first_epoch {
                                audit.in_later_epochs += 1;
                            }
                            version = Some(Version::Deleted);
                        }
                        before => {
                            return Err(format!(
                                "{}/{} was deleted at {position} after {before:?}: a version \
                                 expired twice, or one that no rule expires",
                                bucket.name, object.key
                            ));
                        }
                    },
                    _ => {}
                }
            }
        }
    }
    Ok(audit)
}

/// A key's records in a log, each with its position.
type Positioned<'a> = Vec<(EpochSeq, &'a RecordBody)>;

/// The records of `key` in `records`, once each: those no `TRUNCATE`
/// invalidates, in position order, and those one does.
fn split_truncated<'a>(records: &'a [LogRecord], key: &str) -> (Positioned<'a>, Positioned<'a>) {
    let truncates: Vec<EpochSeq> = records
        .iter()
        .filter(|record| matches!(record.body, RecordBody::Truncate))
        .map(|record| record.position)
        .collect();
    let mut valid = BTreeMap::new();
    let mut truncated = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| record.body.key() == Some(key))
    {
        let position = record.position;
        let entry = (position, &record.body);
        if truncates.iter().any(|t| truncated_by(*t, position)) {
            truncated.insert((position.seq, position.epoch), entry);
        } else {
            valid.insert((position.seq, position.epoch), entry);
        }
    }
    (
        valid.into_values().collect(),
        truncated.into_values().collect(),
    )
}

#[cfg(test)]
mod tests {
    use skys3_log::record::{Delete, Put, PutData};
    use skys3_types::lifecycle::{Expiration, LifecycleRule, RuleFilter};
    use skys3_types::{BucketId, BucketName, ETag, Epoch, ProposalId, Seq, ShardCount, ShardId};

    use super::*;

    fn bucket() -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new("b-sim0").unwrap(),
            name: BucketName::new("bucket-0").unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(1).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    fn lifecycle() -> Lifecycle {
        let rule = LifecycleRule {
            id: "tmp".to_owned(),
            enabled: true,
            filter: RuleFilter {
                prefix: "tmp/".to_owned(),
                ..RuleFilter::default()
            },
            expiration: Some(Expiration::DateMs(Lifecycle::date(2))),
            abort_upload_days: None,
        };
        let object = |key: &str| LifecycleObject {
            key: key.to_owned(),
            size: 1,
            tags: BTreeMap::new(),
        };
        Lifecycle {
            configuration: LifecycleConfiguration { rules: vec![rule] },
            interval: Duration::from_millis(100),
            day: Duration::from_secs(1),
            objects: vec![object("tmp/a"), object("keep/a")],
            uploads: Vec::new(),
            start: LifecycleStart::default(),
        }
    }

    fn record(epoch: u64, seq: u64, body: RecordBody) -> LogRecord {
        LogRecord {
            shard: skys3_log::ShardRef::new(bucket().bucket_id, ShardId::new(0)),
            position: EpochSeq::new(Epoch::new(epoch), Seq::new(seq)),
            body,
        }
    }

    fn put(key: &str) -> RecordBody {
        RecordBody::Put(Put {
            key: key.to_owned(),
            size: 1,
            last_modified_ms: 0,
            etag: ETag::new("0".repeat(32)).unwrap(),
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: PutData::Inline(Bytes::from_static(b"x")),
        })
    }

    fn delete(key: &str) -> RecordBody {
        RecordBody::Delete(Delete {
            key: key.to_owned(),
        })
    }

    fn check(lifecycle: &Lifecycle, records: Vec<LogRecord>) -> Result<LifecycleAudit, String> {
        let logs = ShardLogs {
            primary: records,
            others: Vec::new(),
        };
        let logs = BTreeMap::from([(ShardRef::for_key(&bucket(), "tmp/a"), logs)]);
        audit(lifecycle, &logs, &[bucket()], Lifecycle::date(3))
    }

    #[test]
    fn each_expired_version_has_one_delete() {
        let found = check(
            &lifecycle(),
            vec![
                record(1, 1, put("tmp/a")),
                record(1, 2, put("keep/a")),
                // Appended by a primary that lost the shard, and truncated.
                record(1, 3, delete("tmp/a")),
                record(2, 2, RecordBody::Truncate),
                record(2, 3, delete("tmp/a")),
                record(2, 4, put("tmp/a")),
                record(2, 5, delete("tmp/a")),
            ],
        )
        .unwrap();
        let expected = LifecycleAudit {
            expired: 2,
            in_later_epochs: 2,
            abandoned: 1,
        };
        assert_eq!(found, expected);

        // A deposed primary's log, with a `DELETE` it appended but never
        // committed, and one the final primary committed too.
        let logs = ShardLogs {
            primary: vec![
                record(1, 1, put("tmp/a")),
                record(2, 2, RecordBody::Truncate),
                record(2, 3, delete("tmp/a")),
            ],
            others: vec![vec![
                record(1, 1, put("tmp/a")),
                record(1, 2, delete("tmp/a")),
                record(2, 3, delete("tmp/a")),
            ]],
        };
        let logs = BTreeMap::from([(ShardRef::for_key(&bucket(), "tmp/a"), logs)]);
        let found = audit(&lifecycle(), &logs, &[bucket()], Lifecycle::date(3)).unwrap();
        assert_eq!((found.expired, found.abandoned), (1, 1));
    }

    #[test]
    fn a_second_delete_or_one_no_rule_makes_is_caught() {
        let lifecycle = lifecycle();
        let twice = check(
            &lifecycle,
            vec![
                record(1, 1, put("tmp/a")),
                record(1, 2, delete("tmp/a")),
                record(2, 3, delete("tmp/a")),
            ],
        );
        assert!(twice.unwrap_err().contains("bucket-0/tmp/a"));
        let kept = check(
            &lifecycle,
            vec![record(1, 1, put("keep/a")), record(1, 2, delete("keep/a"))],
        );
        assert!(kept.unwrap_err().contains("bucket-0/keep/a"));
        assert!(check(&lifecycle, vec![record(1, 1, delete("tmp/a"))]).is_err());
        // A version the rules expire only after the run ended.
        let mut late = lifecycle.clone();
        late.configuration.rules[0].expiration = Some(Expiration::DateMs(Lifecycle::date(9)));
        let early = vec![record(1, 1, put("tmp/a")), record(1, 2, delete("tmp/a"))];
        assert!(check(&late, early).is_err());
    }

    #[test]
    fn the_clock_runs_a_day_a_second() {
        let lifecycle = lifecycle();
        assert_eq!(lifecycle.at(Duration::from_secs(1)), None);
        lifecycle.clone().start(Duration::from_secs(1));
        assert_eq!(lifecycle.at(Duration::ZERO), None);
        assert_eq!(
            lifecycle.at(Duration::from_secs(1)),
            Some(Lifecycle::date(0))
        );
        let later = lifecycle.at(Duration::from_millis(3500));
        assert_eq!(later, Some(Lifecycle::date(2) + DAY_MS / 2));
        let expiries: Vec<_> = lifecycle
            .objects
            .iter()
            .map(|object| lifecycle.expiry(object))
            .collect();
        assert_eq!(expiries, [Some(Lifecycle::date(2)), None]);
        assert_eq!(lifecycle.last_due(), Lifecycle::date(2));
    }
}
