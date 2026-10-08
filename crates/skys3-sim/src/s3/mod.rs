//! The simulated S3 store (design §16.1).
//!
//! [`SimS3`] is one in-memory bucket behind the [`ObjectStore`] trait.
//! Simulations use it as a remote target and as the S3 control store, so the
//! real flush and control-store code runs against it. It follows S3 where SkyS3 depends on
//! S3's behavior:
//!
//! - **ETags** are the MD5 of the body for single-part objects and copies,
//!   and `<md5 of the part MD5s>-<parts>` for multipart uploads (§7.4).
//! - **Write preconditions** (`If-None-Match: *`, `If-Match`) on
//!   `PutObject`, `CompleteMultipartUpload`, `CopyObject`, and `DeleteObject`
//!   (`If-Match` only), and `x-amz-copy-source-if-match`, are evaluated when
//!   the write is applied. A failed one is `412`; `If-Match` on a key without
//!   a current object is `404 NoSuchKey`. [`Conditionals`] makes each one
//!   honored, ignored, or rejected, to mimic providers without full support
//!   (§7.2).
//! - **`409 ConditionalRequestConflict`**: a conditional write fails with it
//!   if another write to its key is applied while it is in progress, between
//!   its arrival and the moment it would apply. That window is the request's
//!   delay, so conflicts need [`SimS3Faults`] delays or a scripted
//!   [`Fault::Delay`].
//! - **User metadata** is limited to [`SimS3Config::max_metadata_size`]
//!   bytes (S3's 2 KiB by default), and larger metadata is `400
//!   MetadataTooLarge`.
//! - **Versioning**, when enabled, keeps every version, adds delete markers,
//!   and serves reads and deletes of specific versions.
//! - **Credentials.** Requests are made with the credentials a handle
//!   names ([`SimS3::with_credentials`]), and the store refuses `GetObject`
//!   and `HeadObject` of the keys [`SimS3::deny`] denies them with `403
//!   AccessDenied`, as a bucket policy would, whether the key holds an
//!   object or not.
//! - **Read-after-write consistency**, as on AWS S3, unless
//!   [`Fault::StaleRead`] faults make a `GetObject` or `HeadObject`
//!   answer, or a `ListObjectsV2` list, what keys held before their latest
//!   write, to model stores the control-store probe must refuse (design
//!   §6.1).
//!
//! - **The network path** ([`SimLink`], [`SimS3::set_link`]): a round-trip
//!   time, a bandwidth that request and response bodies queue for, and a
//!   request-rate limit beyond which requests get `503 SlowDown`, for
//!   flush concurrency to adapt to (design §7.7). [`SimS3Stats`] keeps the
//!   most requests and request-body bytes the store had in progress at
//!   once.
//! - **Sources.** A handle may name the source of its requests
//!   ([`SimS3::from_source`]), such as the node that sends them. A
//!   source's link to the store can drop ([`SimS3::set_source_down`]):
//!   while it is down, its requests are lost before they are applied, and
//!   a request in progress when it drops loses its answer, applied or not.
//! - **Faults per operation** ([`SimS3::set_operation_faults`]): one
//!   operation, such as `CompleteMultipartUpload`, can fail or lose its
//!   answers more often than the rest.
//! - **Observers** ([`SimS3::observe`]): a function the store calls after
//!   each request it applied, with the operation, its source, and for a
//!   write the key's current object, so that a test can audit every state
//!   the store passes through, including those no read would see.
//!
//! None of these changes what a store does until it is used, so seeds
//! recorded before them replay unchanged.
//!
//! Faults come from a generator seeded at creation, and the store never
//! reads the wall clock, so a simulation seed replays them exactly. Delays
//! sleep on the caller's Tokio clock: under `turmoil`, the calling host's
//! simulated time.
//!
//! ```
//! use skys3_remote::{ObjectStore, PutObject, S3ErrorKind, WritePrecondition};
//! use skys3_sim::s3::{SimS3, SimS3Config};
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let store = SimS3::new(7, SimS3Config::default());
//! let create = || {
//!     PutObject::new("cluster.json", "{}").with_precondition(WritePrecondition::IfAbsent)
//! };
//! let first = store.put_object(create()).await.unwrap();
//! assert_eq!(first.etag.as_str(), "99914b932bd37a50b983c5e7c90ae93b");
//! let second = store.put_object(create()).await.unwrap_err();
//! assert_eq!(second.kind(), S3ErrorKind::PreconditionFailed);
//! # });
//! ```

mod bucket;
mod faults;
mod link;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::SmallRng;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, MetadataDirective, ObjectInfo, ObjectStore, PutObject, S3Error, S3ErrorKind,
    S3Result, TaggingDirective, UploadId, UploadPart, UserMetadata, VersionId, WriteOutput,
    WritePrecondition,
};
use skys3_types::ETag;
use tokio::time::Instant;

use bucket::Bucket;
pub use faults::{Fault, Operation, SimS3Faults};
use faults::{Injector, Plan};
use link::{BodyBytes, Link};
pub use link::{RateLimit, SimLink};

/// How a provider treats one kind of write precondition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConditionalSupport {
    /// The precondition is evaluated, as on AWS S3.
    Honored,
    /// The header is accepted and ignored: the write applies
    /// unconditionally. This is the dangerous case the capability probe
    /// (plan M1-15) must detect.
    Ignored,
    /// The request fails with `501 NotImplemented` and is not applied.
    Rejected,
}

/// Which write preconditions a simulated provider honors, per operation
/// (design §7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Conditionals {
    /// `If-None-Match: *` and `If-Match` on `PutObject`.
    pub put_object: ConditionalSupport,
    /// `If-None-Match: *` and `If-Match` on `CompleteMultipartUpload`.
    pub complete_multipart_upload: ConditionalSupport,
    /// `If-Match` on `DeleteObject`.
    pub delete_object: ConditionalSupport,
    /// `If-None-Match: *` and `If-Match` on the destination of
    /// `CopyObject`.
    pub copy_object: ConditionalSupport,
    /// `x-amz-copy-source-if-match` on `CopyObject`.
    pub copy_source: ConditionalSupport,
}

impl Conditionals {
    /// Every precondition honored, as on AWS S3.
    pub const AWS_S3: Conditionals = Conditionals {
        put_object: ConditionalSupport::Honored,
        complete_multipart_upload: ConditionalSupport::Honored,
        delete_object: ConditionalSupport::Honored,
        copy_object: ConditionalSupport::Honored,
        copy_source: ConditionalSupport::Honored,
    };

    /// The support design §7.2 describes for Cloudflare R2: preconditions
    /// on `PutObject` and on the copy source are honored, and the others are
    /// ignored. The capability probe, not this preset, decides for a real
    /// provider.
    pub const R2: Conditionals = Conditionals {
        put_object: ConditionalSupport::Honored,
        complete_multipart_upload: ConditionalSupport::Ignored,
        delete_object: ConditionalSupport::Ignored,
        copy_object: ConditionalSupport::Ignored,
        copy_source: ConditionalSupport::Honored,
    };

    /// Every precondition treated the same way.
    pub const fn all(support: ConditionalSupport) -> Conditionals {
        Conditionals {
            put_object: support,
            complete_multipart_upload: support,
            delete_object: support,
            copy_object: support,
            copy_source: support,
        }
    }
}

/// How a simulated provider treats the `REPLACE` directives of
/// `CopyObject`, with the outcomes of [`ConditionalSupport`]: an ignored
/// directive copies the source's metadata or tags as `COPY` does, and a
/// rejected one fails the copy with `501 NotImplemented`. A flush sends a
/// copy as `CopyObject` only if both are honored (design §7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CopyDirectives {
    /// `x-amz-metadata-directive: REPLACE`.
    pub metadata: ConditionalSupport,
    /// `x-amz-tagging-directive: REPLACE`.
    pub tagging: ConditionalSupport,
}

impl CopyDirectives {
    /// Both directives honored, as on AWS S3.
    pub const AWS_S3: CopyDirectives = CopyDirectives {
        metadata: ConditionalSupport::Honored,
        tagging: ConditionalSupport::Honored,
    };

    /// A provider without object tags, such as Cloudflare R2: copies keep
    /// whatever tags the source has, here none, whatever the request says.
    pub const WITHOUT_TAGS: CopyDirectives = CopyDirectives {
        metadata: ConditionalSupport::Honored,
        tagging: ConditionalSupport::Ignored,
    };
}

/// The behavior of a simulated bucket, fixed when it is created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimS3Config {
    /// Whether the bucket keeps every version.
    pub versioning: bool,
    /// Which write preconditions it honors.
    pub conditionals: Conditionals,
    /// Which `CopyObject` directives it honors.
    pub copy_directives: CopyDirectives,
    /// The smallest size of every part of a multipart upload but the last.
    /// S3's is 5 MiB; simulations may lower it to keep objects small.
    pub min_part_size: u64,
    /// The user-metadata limit ([`UserMetadata::size`]).
    pub max_metadata_size: usize,
}

impl Default for SimS3Config {
    /// An unversioned bucket that behaves like AWS S3.
    fn default() -> Self {
        SimS3Config {
            versioning: false,
            conditionals: Conditionals::AWS_S3,
            copy_directives: CopyDirectives::AWS_S3,
            min_part_size: 5 << 20,
            max_metadata_size: UserMetadata::S3_LIMIT,
        }
    }
}

/// Counts of what a [`SimS3`] did, for tests and checkers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimS3Stats {
    /// Requests received.
    pub requests: u64,
    /// `500 InternalError` responses injected before applying a request.
    pub internal_errors: u64,
    /// `503 SlowDown` responses injected, including those of the rate
    /// limit.
    pub slow_downs: u64,
    /// `503 SlowDown` responses of the link's rate limit ([`SimLink`]).
    pub rate_limited: u64,
    /// Requests lost before they were applied.
    pub lost_requests: u64,
    /// Responses lost after their request was processed.
    pub lost_responses: u64,
    /// `409 ConditionalRequestConflict` responses, injected or from races.
    pub conflicts: u64,
    /// The most requests in progress at once, from the moment each was
    /// sent until its answer arrived or its caller gave up.
    pub max_in_flight: u64,
    /// The most request-body bytes (`PutObject`, `UploadPart`) in progress
    /// at once.
    pub max_in_flight_bytes: u64,
    /// Requests lost because their source's link was down when they
    /// arrived, and answers lost because it dropped while their request
    /// was in progress ([`SimS3::set_source_down`]).
    pub cut_off: u64,
}

/// A request the store applied, as an observer sees it
/// ([`SimS3::observe`]).
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Applied<'a> {
    /// The operation.
    pub operation: Operation,
    /// The source of the handle that sent it ([`SimS3::from_source`]).
    pub source: Option<&'a str>,
    /// The key it wrote, for `PutObject`, `CopyObject`,
    /// `CompleteMultipartUpload`, and `DeleteObject`.
    pub key: Option<&'a str>,
    /// For a write, the key's current object once it was applied: `None`
    /// if the key has none, or for any other request.
    pub current: Option<&'a GetOutput>,
}

/// What [`SimS3::observe`] calls after each request the store applied.
pub type Observer = Arc<dyn Fn(&Applied<'_>) + Send + Sync>;

/// A simulated S3 bucket.
///
/// Clones share the bucket, so one store can be handed to every node of a
/// simulation. See the [module documentation](self) for the semantics.
#[derive(Clone, Debug)]
pub struct SimS3 {
    shared: Arc<Shared>,
    /// The credentials this handle's requests are made with, if any.
    credentials: Option<Arc<str>>,
    /// The source this handle's requests come from, if it names one.
    source: Option<Arc<str>>,
}

#[derive(Debug)]
struct Shared {
    config: SimS3Config,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    bucket: Bucket,
    injector: Injector,
    /// Conditional writes in progress, by request number: their key, and
    /// whether another write to it has been applied since they arrived.
    in_flight: BTreeMap<u64, (String, bool)>,
    next_request: u64,
    stats: SimS3Stats,
    link: Link,
    /// Requests in progress, and their request-body bytes.
    in_progress: (u64, u64),
    /// Successful requests whose caller never got the answer, by operation.
    unanswered: HashMap<Operation, u64>,
    /// The `(credentials, key)` pairs whose reads are refused.
    denied: BTreeSet<(String, String)>,
    /// Every write the store applied, in order: its operation and key.
    applied: Vec<(Operation, String)>,
    /// The links of the sources whose link dropped at least once.
    sources: BTreeMap<String, SourceLink>,
    /// What is called after each request the store applied.
    observer: Option<Observed>,
}

/// The observer of a store, which `Debug` shows by its presence alone.
#[derive(Clone)]
struct Observed(Observer);

impl std::fmt::Debug for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Observer")
    }
}

impl State {
    fn new(bucket: Bucket, injector: Injector) -> Self {
        State {
            bucket,
            injector,
            in_flight: BTreeMap::new(),
            next_request: 0,
            stats: SimS3Stats::default(),
            link: Link::default(),
            in_progress: (0, 0),
            unanswered: HashMap::new(),
            denied: BTreeSet::new(),
            applied: Vec::new(),
            sources: BTreeMap::new(),
            observer: None,
        }
    }

    /// The link of `source`, up and never dropped if it has none.
    fn source_link(&self, source: Option<&str>) -> SourceLink {
        source
            .and_then(|source| self.sources.get(source))
            .copied()
            .unwrap_or_default()
    }
}

/// The state of one source's link to the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SourceLink {
    /// Whether it is down now.
    down: bool,
    /// How many times it went down.
    drops: u64,
}

/// A request's effect on conflict detection, and its body.
#[derive(Clone, Copy)]
struct Tracking<'a> {
    /// The key the request writes, if it is a write.
    writes: Option<&'a str>,
    /// Whether the request is a conditional write, and so can conflict.
    conditional: bool,
    /// The bytes of the request's body.
    body: u64,
}

impl Tracking<'_> {
    const NONE: Tracking<'static> = Tracking {
        writes: None,
        conditional: false,
        body: 0,
    };
}

impl SimS3 {
    /// Returns an empty bucket whose faults are seeded with `seed`. It
    /// injects no faults until [`SimS3::set_faults`] or [`SimS3::inject`].
    pub fn new(seed: u64, config: SimS3Config) -> Self {
        SimS3 {
            shared: Arc::new(Shared {
                state: Mutex::new(State::new(
                    Bucket::new(config.clone()),
                    Injector::new(SmallRng::seed_from_u64(seed)),
                )),
                config,
            }),
            credentials: None,
            source: None,
        }
    }

    /// Returns a new bucket that holds what this one holds now: its
    /// objects, versions, and multipart uploads. It shares nothing with
    /// this one and injects no faults, so a test can look at the store as
    /// it was at one moment, such as the moment it assumes a whole cluster
    /// lost, while the simulation goes on writing to this one.
    #[must_use]
    pub fn fork(&self) -> Self {
        let bucket = self.state().bucket.clone();
        SimS3 {
            shared: Arc::new(Shared {
                state: Mutex::new(State::new(
                    bucket,
                    Injector::new(SmallRng::seed_from_u64(0)),
                )),
                config: self.shared.config.clone(),
            }),
            credentials: None,
            source: None,
        }
    }

    /// A handle on the same bucket whose requests are made with the
    /// credentials named `credentials`, which [`SimS3::deny`] may refuse
    /// keys. It keeps this handle's source.
    #[must_use]
    pub fn with_credentials(&self, credentials: &str) -> Self {
        SimS3 {
            shared: Arc::clone(&self.shared),
            credentials: Some(credentials.into()),
            source: self.source.clone(),
        }
    }

    /// A handle on the same bucket whose requests come from `source`, such
    /// as a node: their link to the store drops with
    /// [`SimS3::set_source_down`], and observers learn their source. It
    /// keeps this handle's credentials.
    #[must_use]
    pub fn from_source(&self, source: &str) -> Self {
        SimS3 {
            shared: Arc::clone(&self.shared),
            credentials: self.credentials.clone(),
            source: Some(source.into()),
        }
    }

    /// Drops, or with `down` false restores, the link between `source` and
    /// the store. While it is down, the source's requests are lost before
    /// the store applies them. A request in progress when it drops loses
    /// its answer, whether the store applied it or not, as when a
    /// connection breaks: the caller gets [`S3ErrorKind::Timeout`]. Other
    /// sources are unaffected.
    pub fn set_source_down(&self, source: &str, down: bool) {
        let mut state = self.state();
        let link = state.sources.entry(source.to_owned()).or_default();
        if down && !link.down {
            link.drops += 1;
        }
        link.down = down;
    }

    /// Replaces the random faults of the requests of `operation`, or with
    /// `None` makes them follow [`SimS3::set_faults`] again: their error
    /// faults and delays are drawn from `faults` instead. Scripted faults
    /// ([`SimS3::inject`]) still come first.
    ///
    /// # Panics
    ///
    /// Panics if `faults` is not valid, as [`SimS3::set_faults`] does.
    pub fn set_operation_faults(&self, operation: Operation, faults: Option<SimS3Faults>) {
        if let Some(faults) = &faults {
            faults.validate();
        }
        self.state().injector.set_operation(operation, faults);
    }

    /// Calls `observer` after each request the store applies from now on,
    /// in place of any earlier one, or with `None` stops calling it. It is
    /// called once the request took effect, before its answer is delayed or
    /// lost, outside the store's lock; it must not wait for the store.
    pub fn observe(&self, observer: Option<Observer>) {
        self.state().observer = observer.map(Observed);
    }

    /// Refuses, or with `denied` false allows again, reads of `key` made
    /// with the credentials `credentials`, from now on.
    pub fn deny(&self, credentials: &str, key: &str, denied: bool) {
        let pair = (credentials.to_owned(), key.to_owned());
        let mut state = self.state();
        if denied {
            state.denied.insert(pair);
        } else {
            state.denied.remove(&pair);
        }
    }

    /// Whether reads of `key` made with `credentials` are refused now.
    pub fn denies(&self, credentials: &str, key: &str) -> bool {
        let pair = (credentials.to_owned(), key.to_owned());
        self.state().denied.contains(&pair)
    }

    /// The `403` of a read of `key` this handle's credentials may not make,
    /// if they may not.
    fn refusal(&self, key: &str) -> S3Result<()> {
        let Some(credentials) = &self.credentials else {
            return Ok(());
        };
        if self.denies(credentials, key) {
            return Err(S3Error::new(S3ErrorKind::Other, "access denied")
                .with_status(403)
                .with_code("AccessDenied"));
        }
        Ok(())
    }

    /// Returns the bucket's configuration.
    pub fn config(&self) -> &SimS3Config {
        &self.shared.config
    }

    /// Replaces the random faults injected into later requests, for example
    /// [`SimS3Faults::OUTAGE`] to start an outage and [`SimS3Faults::NONE`]
    /// to end it.
    ///
    /// # Panics
    ///
    /// Panics if a probability is outside `0..=1`, the probabilities sum to
    /// more than 1, or `min_delay > max_delay`.
    pub fn set_faults(&self, faults: SimS3Faults) {
        faults.validate();
        self.state().injector.faults = faults;
    }

    /// Returns the random faults currently injected.
    pub fn faults(&self) -> SimS3Faults {
        self.state().injector.faults.clone()
    }

    /// Replaces the network path of later requests and the store's rate
    /// limit: for example a 100 ms round trip at 10 MB/s, or a limit that
    /// starts and later ends. Bodies already queued keep their place.
    pub fn set_link(&self, link: SimLink) {
        self.state().link.config = link;
    }

    /// Returns the network path of requests.
    pub fn link(&self) -> SimLink {
        self.state().link.config
    }

    /// Queues `fault` for the next request of `operation`, in place of a
    /// random fault. Faults queued for one operation apply in order.
    pub fn inject(&self, operation: Operation, fault: Fault) {
        self.state().injector.script(operation, fault);
    }

    /// Returns what the store has done so far.
    pub fn stats(&self) -> SimS3Stats {
        self.state().stats
    }

    /// Returns the keys of every write of `operation` the store applied,
    /// in the order it applied them, answered or not: for example the
    /// destinations of every `CopyObject`.
    pub fn applied(&self, operation: Operation) -> Vec<String> {
        self.state()
            .applied
            .iter()
            .filter(|(applied, _)| *applied == operation)
            .map(|(_, key)| key.clone())
            .collect()
    }

    /// Returns how many requests of `operation` succeeded at the store
    /// without their caller learning it: the response was lost, or the
    /// caller stopped waiting for it after the store had applied it. For
    /// `CreateMultipartUpload`, these are the uploads whose IDs never
    /// reached the caller.
    pub fn unanswered(&self, operation: Operation) -> u64 {
        self.state()
            .unanswered
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    /// Reads the current object at `key` without a request: no faults and
    /// no delay.
    pub fn object(&self, key: &str) -> Option<GetOutput> {
        self.state()
            .bucket
            .get_object(&GetObject::new(key), false)
            .ok()
    }

    /// Returns the tags of the current object at `key`, without a request.
    /// The store keeps the tags a `PutObject` or a multipart upload sets
    /// and a copy copies; it does not keep the other standard headers of
    /// [`PutObject::headers`].
    pub fn tags(&self, key: &str) -> Option<BTreeMap<String, String>> {
        self.state().bucket.tags(key)
    }

    /// Returns every key with a current object, in order, without a
    /// request.
    pub fn keys(&self) -> Vec<String> {
        self.state().bucket.keys()
    }

    /// Returns every version and delete marker of every key, without a
    /// request: the key, the version ID (`None` on an unversioned bucket),
    /// and whether it is a delete marker. Keys are in order, and each key's
    /// versions oldest first.
    pub fn versions(&self) -> Vec<(String, Option<VersionId>, bool)> {
        self.state().bucket.versions()
    }

    /// Returns every multipart upload in progress and its key, without a
    /// request.
    pub fn uploads(&self) -> Vec<(UploadId, String)> {
        self.state().bucket.uploads()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // The state stays consistent if a holder panics: every mutation is a
        // single step under the lock.
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs one request: draws its faults and delays, waits, applies it
    /// unless a fault stops it, waits again, and answers unless the
    /// response is lost. `apply` learns whether a read is stale.
    async fn request<T: BodyBytes>(
        &self,
        operation: Operation,
        tracking: Tracking<'_>,
        apply: impl FnOnce(&mut Bucket, bool) -> S3Result<T>,
    ) -> S3Result<T> {
        let source = self.source.as_deref();
        let (plan, in_flight, arrival, _progress, drops) = {
            let mut state = self.state();
            state.stats.requests += 1;
            let plan = state.injector.plan(operation);
            let arrival = state.link.arrival(Instant::now(), tracking.body);
            let progress = InProgress::start(self, &mut state, tracking.body);
            let in_flight = match tracking.writes {
                Some(key) if tracking.conditional => {
                    let id = state.next_request;
                    state.next_request += 1;
                    state.in_flight.insert(id, (key.to_owned(), false));
                    Some(InFlight { store: self, id })
                }
                _ => None,
            };
            let drops = state.source_link(source).drops;
            (plan, in_flight, arrival, progress, drops)
        };
        sleep_until(arrival).await;
        sleep(plan.request_delay).await;
        let cut = {
            let mut state = self.state();
            let down = state.source_link(source).down;
            state.stats.cut_off += u64::from(down);
            down
        };
        let result = if cut {
            Err(S3Error::new(
                S3ErrorKind::Timeout,
                "the request was lost: the link to the store is down",
            ))
        } else {
            self.process(operation, plan, in_flight, tracking.writes, apply)
        };
        let unanswered = result.is_ok().then(|| Unanswered {
            store: self,
            operation,
        });
        let bytes = result.as_ref().map_or(0, BodyBytes::body_bytes);
        let delivery = self.state().link.delivery(Instant::now(), bytes);
        sleep_until(delivery).await;
        sleep(plan.response_delay).await;
        if plan.fault == Some(Fault::LostResponse) {
            self.state().stats.lost_responses += 1;
            let kind = if plan.lost_as_timeout {
                S3ErrorKind::Timeout
            } else {
                S3ErrorKind::InternalError
            };
            return Err(S3Error::new(kind, "the response was lost"));
        }
        if !cut {
            let mut state = self.state();
            let link = state.source_link(source);
            if link.down || link.drops != drops {
                state.stats.cut_off += 1;
                return Err(S3Error::new(
                    S3ErrorKind::Timeout,
                    "the response was lost: the link to the store dropped",
                ));
            }
        }
        std::mem::forget(unanswered);
        result
    }

    /// The store's side of a request: ends its conflict window, and applies it
    /// unless a fault or a conflict stops it. Tells the observer, if any, of
    /// a request it applied.
    fn process<T>(
        &self,
        operation: Operation,
        plan: Plan,
        in_flight: Option<InFlight<'_>>,
        writes: Option<&str>,
        apply: impl FnOnce(&mut Bucket, bool) -> S3Result<T>,
    ) -> S3Result<T> {
        let mut state = self.state();
        let conflicted = in_flight.is_some_and(|in_flight| in_flight.finish(&mut state));
        let limited = !state.link.admit(Instant::now());
        let stats = &mut state.stats;
        let injected = match plan.fault {
            _ if limited => {
                stats.slow_downs += 1;
                stats.rate_limited += 1;
                Some((S3ErrorKind::SlowDown, "please reduce your request rate"))
            }
            Some(Fault::InternalError) => {
                stats.internal_errors += 1;
                Some((S3ErrorKind::InternalError, "injected internal error"))
            }
            Some(Fault::SlowDown) => {
                stats.slow_downs += 1;
                Some((S3ErrorKind::SlowDown, "please reduce your request rate"))
            }
            Some(Fault::LostRequest) => {
                stats.lost_requests += 1;
                Some((S3ErrorKind::Timeout, "the request was lost"))
            }
            Some(Fault::Conflict) => {
                stats.conflicts += 1;
                Some((
                    S3ErrorKind::ConditionalRequestConflict,
                    "injected conditional request conflict",
                ))
            }
            _ if conflicted => {
                stats.conflicts += 1;
                Some((
                    S3ErrorKind::ConditionalRequestConflict,
                    "a concurrent write to the key was applied first",
                ))
            }
            _ => None,
        };
        if let Some((kind, message)) = injected {
            return Err(S3Error::new(kind, message));
        }
        let result = match writes {
            Some(key) => state
                .bucket
                .apply_write(key, |bucket| apply(bucket, plan.stale)),
            None => apply(&mut state.bucket, plan.stale),
        };
        if let (Ok(_), Some(key)) = (&result, writes) {
            for (in_flight_key, conflicted) in state.in_flight.values_mut() {
                if in_flight_key == key {
                    *conflicted = true;
                }
            }
            state.applied.push((operation, key.to_owned()));
        }
        let observer = state.observer.clone().filter(|_| result.is_ok());
        if let Some(Observed(observer)) = observer {
            let current =
                writes.and_then(|key| state.bucket.get_object(&GetObject::new(key), false).ok());
            drop(state);
            observer(&Applied {
                operation,
                source: self.source.as_deref(),
                key: writes,
                current: current.as_ref(),
            });
        }
        result
    }

    /// Applies `support` to a precondition that is `present` on a request:
    /// whether to keep it, or the `501` a provider that rejects it answers.
    fn gate(support: ConditionalSupport, present: bool, header: &str) -> S3Result<bool> {
        match support {
            ConditionalSupport::Honored => Ok(present),
            ConditionalSupport::Ignored => Ok(false),
            ConditionalSupport::Rejected if present => Err(S3Error::new(
                S3ErrorKind::NotImplemented,
                format!("{header} is not implemented for this operation"),
            )),
            ConditionalSupport::Rejected => Ok(false),
        }
    }

    /// Strips an ignored write precondition, or returns a rejected one's
    /// error.
    fn gate_precondition(
        support: ConditionalSupport,
        precondition: WritePrecondition,
    ) -> S3Result<WritePrecondition> {
        let header = match precondition {
            WritePrecondition::IfMatch(_) => "If-Match",
            _ => "If-None-Match",
        };
        Ok(if Self::gate(support, precondition.is_some(), header)? {
            precondition
        } else {
            WritePrecondition::None
        })
    }

    /// Turns a `REPLACE` directive the provider ignores into `COPY`, or
    /// returns the error of one it rejects.
    fn gate_directives(&self, request: &mut CopyObject) -> S3Result<()> {
        let support = self.shared.config.copy_directives;
        let replaces = !matches!(request.metadata_directive, MetadataDirective::Copy);
        if !Self::gate(support.metadata, replaces, "x-amz-metadata-directive")? {
            request.metadata_directive = MetadataDirective::Copy;
        }
        let replaces = !matches!(request.tagging_directive, TaggingDirective::Copy);
        if !Self::gate(support.tagging, replaces, "x-amz-tagging-directive")? {
            request.tagging_directive = TaggingDirective::Copy;
        }
        Ok(())
    }

    /// Strips an ignored `If-Match`, or returns a rejected one's error.
    fn gate_etag(
        support: ConditionalSupport,
        etag: Option<ETag>,
        header: &str,
    ) -> S3Result<Option<ETag>> {
        let keep = Self::gate(support, etag.is_some(), header)?;
        Ok(etag.filter(|_| keep))
    }
}

/// A conditional write in progress. Dropping it, as when the caller gives
/// up on the request, forgets the write.
struct InFlight<'a> {
    store: &'a SimS3,
    id: u64,
}

impl InFlight<'_> {
    /// Ends the write's window and returns whether another write to its key
    /// was applied during it.
    fn finish(self, state: &mut State) -> bool {
        let conflicted = state.in_flight.remove(&self.id).is_some_and(|(_, c)| c);
        std::mem::forget(self);
        conflicted
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.store.state().in_flight.remove(&self.id);
    }
}

/// A request in progress, counted in the store's peaks until it is
/// answered or its caller gives up.
struct InProgress<'a> {
    store: &'a SimS3,
    body: u64,
}

impl<'a> InProgress<'a> {
    fn start(store: &'a SimS3, state: &mut State, body: u64) -> Self {
        let (requests, bytes) = &mut state.in_progress;
        *requests += 1;
        *bytes += body;
        let (requests, bytes) = (*requests, *bytes);
        state.stats.max_in_flight = state.stats.max_in_flight.max(requests);
        state.stats.max_in_flight_bytes = state.stats.max_in_flight_bytes.max(bytes);
        InProgress { store, body }
    }
}

impl Drop for InProgress<'_> {
    fn drop(&mut self) {
        let mut state = self.store.state();
        let (requests, bytes) = &mut state.in_progress;
        *requests -= 1;
        *bytes -= self.body;
    }
}

/// A successful request whose answer has not reached its caller yet.
/// Dropping it, because the response is lost or the caller gave up,
/// counts it in [`SimS3::unanswered`].
struct Unanswered<'a> {
    store: &'a SimS3,
    operation: Operation,
}

impl Drop for Unanswered<'_> {
    fn drop(&mut self) {
        *self
            .store
            .state()
            .unanswered
            .entry(self.operation)
            .or_default() += 1;
    }
}

async fn sleep(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
    }
}

/// Sleeps until `at`, unless it is now: an instant link never yields.
async fn sleep_until(at: Instant) {
    if at > Instant::now() {
        tokio::time::sleep_until(at).await;
    }
}

impl ObjectStore for SimS3 {
    async fn put_object(&self, mut request: PutObject) -> S3Result<WriteOutput> {
        let support = self.shared.config.conditionals.put_object;
        let gated = Self::gate_precondition(support, request.precondition.clone());
        let key = request.key.clone();
        let tracking = Tracking {
            writes: Some(&key),
            conditional: gated.as_ref().is_ok_and(WritePrecondition::is_some),
            body: request.body.len() as u64,
        };
        self.request(Operation::PutObject, tracking, |bucket, _| {
            request.precondition = gated?;
            bucket.put_object(request)
        })
        .await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        // Refused as the request arrives, before any fault or delay.
        self.refusal(&request.key)?;
        self.request(Operation::GetObject, Tracking::NONE, |bucket, stale| {
            bucket.get_object(&request, stale)
        })
        .await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        // Refused as the request arrives, before any fault or delay.
        self.refusal(&request.key)?;
        self.request(Operation::HeadObject, Tracking::NONE, |bucket, stale| {
            bucket.head_object(&request, stale)
        })
        .await
    }

    async fn delete_object(&self, mut request: DeleteObject) -> S3Result<DeleteOutput> {
        let support = self.shared.config.conditionals.delete_object;
        let gated = Self::gate_etag(support, request.if_match.clone(), "If-Match");
        let key = request.key.clone();
        let tracking = Tracking {
            writes: Some(&key),
            conditional: gated.as_ref().is_ok_and(Option::is_some),
            body: 0,
        };
        self.request(Operation::DeleteObject, tracking, |bucket, _| {
            request.if_match = gated?;
            bucket.delete_object(request)
        })
        .await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.request(Operation::ListObjectsV2, Tracking::NONE, |bucket, stale| {
            bucket.list_objects_v2(&request, stale)
        })
        .await
    }

    async fn copy_object(&self, mut request: CopyObject) -> S3Result<WriteOutput> {
        let conditionals = self.shared.config.conditionals;
        let directives = self.gate_directives(&mut request);
        let gated = directives
            .and_then(|()| {
                Self::gate_precondition(conditionals.copy_object, request.precondition.clone())
            })
            .and_then(|precondition| {
                let source_if_match = Self::gate_etag(
                    conditionals.copy_source,
                    request.source_if_match.clone(),
                    "x-amz-copy-source-if-match",
                )?;
                Ok((precondition, source_if_match))
            });
        let key = request.key.clone();
        let tracking = Tracking {
            writes: Some(&key),
            conditional: gated.as_ref().is_ok_and(|(p, _)| p.is_some()),
            body: 0,
        };
        self.request(Operation::CopyObject, tracking, |bucket, _| {
            (request.precondition, request.source_if_match) = gated?;
            bucket.copy_object(request)
        })
        .await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.request(
            Operation::CreateMultipartUpload,
            Tracking::NONE,
            |bucket, _| bucket.create_multipart_upload(request),
        )
        .await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        let tracking = Tracking {
            body: request.body.len() as u64,
            ..Tracking::NONE
        };
        self.request(Operation::UploadPart, tracking, |bucket, _| {
            bucket.upload_part(request)
        })
        .await
    }

    async fn complete_multipart_upload(
        &self,
        mut request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        let support = self.shared.config.conditionals.complete_multipart_upload;
        let gated = Self::gate_precondition(support, request.precondition.clone());
        let key = request.key.clone();
        let tracking = Tracking {
            writes: Some(&key),
            conditional: gated.as_ref().is_ok_and(WritePrecondition::is_some),
            body: 0,
        };
        self.request(Operation::CompleteMultipartUpload, tracking, |bucket, _| {
            request.precondition = gated?;
            bucket.complete_multipart_upload(request)
        })
        .await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.request(
            Operation::AbortMultipartUpload,
            Tracking::NONE,
            |bucket, _| bucket.abort_multipart_upload(&request),
        )
        .await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        self.request(Operation::ListParts, Tracking::NONE, |bucket, _| {
            bucket.list_parts(&request)
        })
        .await
    }
}

impl BodyBytes for GetOutput {
    fn body_bytes(&self) -> u64 {
        self.body.len() as u64
    }
}

impl BodyBytes for WriteOutput {}
impl BodyBytes for ObjectInfo {}
impl BodyBytes for DeleteOutput {}
impl BodyBytes for ListObjectsV2Output {}
impl BodyBytes for UploadId {}
impl BodyBytes for ETag {}
impl BodyBytes for ListPartsOutput {}
impl BodyBytes for () {}

#[cfg(test)]
mod tests;
