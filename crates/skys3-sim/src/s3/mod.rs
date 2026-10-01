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
//! - **Read-after-write consistency**, as on AWS S3, unless
//!   [`Fault::StaleRead`] faults make a `GetObject` or `HeadObject` answer
//!   with what the key held before its latest write, to model a store the
//!   control-store probe must refuse (design §6.1).
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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::SmallRng;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, ObjectStore, PutObject, S3Error, S3ErrorKind, S3Result, UploadId,
    UploadPart, UserMetadata, VersionId, WriteOutput, WritePrecondition,
};
use skys3_types::ETag;

use bucket::Bucket;
pub use faults::{Fault, Operation, SimS3Faults};
use faults::{Injector, Plan};

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

/// The behavior of a simulated bucket, fixed when it is created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimS3Config {
    /// Whether the bucket keeps every version.
    pub versioning: bool,
    /// Which write preconditions it honors.
    pub conditionals: Conditionals,
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
    /// `503 SlowDown` responses injected.
    pub slow_downs: u64,
    /// Requests lost before they were applied.
    pub lost_requests: u64,
    /// Responses lost after their request was processed.
    pub lost_responses: u64,
    /// `409 ConditionalRequestConflict` responses, injected or from races.
    pub conflicts: u64,
}

/// A simulated S3 bucket.
///
/// Clones share the bucket, so one store can be handed to every node of a
/// simulation. See the [module documentation](self) for the semantics.
#[derive(Clone, Debug)]
pub struct SimS3 {
    shared: Arc<Shared>,
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
}

/// A request's effect on conflict detection.
#[derive(Clone, Copy)]
struct Tracking<'a> {
    /// The key the request writes, if it is a write.
    writes: Option<&'a str>,
    /// Whether the request is a conditional write, and so can conflict.
    conditional: bool,
}

impl Tracking<'_> {
    const NONE: Tracking<'static> = Tracking {
        writes: None,
        conditional: false,
    };
}

impl SimS3 {
    /// Returns an empty bucket whose faults are seeded with `seed`. It
    /// injects no faults until [`SimS3::set_faults`] or [`SimS3::inject`].
    pub fn new(seed: u64, config: SimS3Config) -> Self {
        SimS3 {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    bucket: Bucket::new(config.clone()),
                    injector: Injector::new(SmallRng::seed_from_u64(seed)),
                    in_flight: BTreeMap::new(),
                    next_request: 0,
                    stats: SimS3Stats::default(),
                }),
                config,
            }),
        }
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

    /// Queues `fault` for the next request of `operation`, in place of a
    /// random fault. Faults queued for one operation apply in order.
    pub fn inject(&self, operation: Operation, fault: Fault) {
        self.state().injector.script(operation, fault);
    }

    /// Returns what the store has done so far.
    pub fn stats(&self) -> SimS3Stats {
        self.state().stats
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
    /// The store keeps the tags a `PutObject` sets and a copy copies; it
    /// does not keep the other standard headers of [`PutObject::headers`].
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
    async fn request<T>(
        &self,
        operation: Operation,
        tracking: Tracking<'_>,
        apply: impl FnOnce(&mut Bucket, bool) -> S3Result<T>,
    ) -> S3Result<T> {
        let (plan, in_flight) = {
            let mut state = self.state();
            state.stats.requests += 1;
            let plan = state.injector.plan(operation);
            let in_flight = match tracking.writes {
                Some(key) if tracking.conditional => {
                    let id = state.next_request;
                    state.next_request += 1;
                    state.in_flight.insert(id, (key.to_owned(), false));
                    Some(InFlight { store: self, id })
                }
                _ => None,
            };
            (plan, in_flight)
        };
        sleep(plan.request_delay).await;
        let result = self.process(plan, in_flight, tracking.writes, apply);
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
        result
    }

    /// The store's side of a request: ends its conflict window, and applies it
    /// unless a fault or a conflict stops it.
    fn process<T>(
        &self,
        plan: Plan,
        in_flight: Option<InFlight<'_>>,
        writes: Option<&str>,
        apply: impl FnOnce(&mut Bucket, bool) -> S3Result<T>,
    ) -> S3Result<T> {
        let mut state = self.state();
        let conflicted = in_flight.is_some_and(|in_flight| in_flight.finish(&mut state));
        let stats = &mut state.stats;
        let injected = match plan.fault {
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

async fn sleep(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
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
        };
        self.request(Operation::PutObject, tracking, |bucket, _| {
            request.precondition = gated?;
            bucket.put_object(request)
        })
        .await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.request(Operation::GetObject, Tracking::NONE, |bucket, stale| {
            bucket.get_object(&request, stale)
        })
        .await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
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
        };
        self.request(Operation::DeleteObject, tracking, |bucket, _| {
            request.if_match = gated?;
            bucket.delete_object(request)
        })
        .await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.request(Operation::ListObjectsV2, Tracking::NONE, |bucket, _| {
            bucket.list_objects_v2(&request)
        })
        .await
    }

    async fn copy_object(&self, mut request: CopyObject) -> S3Result<WriteOutput> {
        let conditionals = self.shared.config.conditionals;
        let gated = Self::gate_precondition(conditionals.copy_object, request.precondition.clone())
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
        self.request(Operation::UploadPart, Tracking::NONE, |bucket, _| {
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

#[cfg(test)]
mod tests;
