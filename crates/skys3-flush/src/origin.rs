//! The origins of `read_only` buckets, as a node reads them (§9.5).
//!
//! A `read_only` bucket's target is an origin SkyS3 does not own: nothing
//! is flushed to it, so it has no flushers and no capability probe, and
//! there is no namespace import, since its listings are forwarded to it.
//! What a node keeps of it is how to read it: a [`RemoteReader`] for the
//! gateway's revalidations and forwarded listings, and a [`Filler`] for
//! read-through fills of the versions its shards cache (§9.2).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use skys3_io::WallClock;
use skys3_remote::ObjectStore;
use skys3_types::{BucketDocument, BucketId, BucketMode, RemoteTarget};

use crate::fill::Filler;
use crate::import::{ImportState, RemoteReader};
use crate::metrics::Counters;
use crate::target::FlushSettings;

/// Builds the store of a `read_only` bucket's origin, read with the
/// credentials of the given profile of the shared AWS configuration, or
/// of the default chain for `None` (§9.5).
pub type OriginConnect<S> = Box<dyn Fn(&RemoteTarget, Option<&str>) -> S + Send + Sync>;

/// The credential scope of the default credential chain.
pub const DEFAULT_CREDENTIALS: &str = "default";

/// The credential scope of reads with the profile `profile`.
#[must_use]
pub fn profile_credentials(profile: &str) -> String {
    format!("profile:{profile}")
}

/// How a node reads one `read_only` bucket's origin.
pub struct Origin<S> {
    target: RemoteTarget,
    credentials: String,
    remote: RemoteReader<S>,
    filler: Filler<S>,
}

impl<S> Clone for Origin<S> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            credentials: self.credentials.clone(),
            remote: self.remote.clone(),
            filler: self.filler.clone(),
        }
    }
}

impl<S> fmt::Debug for Origin<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Origin")
            .field("target", &self.target)
            .field("credentials", &self.credentials)
            .finish_non_exhaustive()
    }
}

impl<S: ObjectStore> Origin<S> {
    /// How `bucket`'s origin `target` is read through `store`, with the
    /// credentials of `profile`, or the default chain's.
    pub(crate) fn new(
        target: &RemoteTarget,
        profile: Option<&str>,
        store: S,
        settings: &FlushSettings,
        counters: Counters,
        wall: &Arc<dyn WallClock>,
    ) -> Self {
        let store = Arc::new(store);
        let prefix = target.prefix.clone().unwrap_or_default();
        Self {
            target: target.clone(),
            credentials: profile
                .map_or_else(|| DEFAULT_CREDENTIALS.to_owned(), profile_credentials),
            // Nothing is imported: the reader is never asked how far.
            remote: RemoteReader::new(
                Arc::clone(&store),
                prefix.clone(),
                Arc::new(ImportState::default()),
                Arc::clone(wall),
            ),
            filler: Filler::new(store, prefix, settings.clone(), counters, Arc::clone(wall)),
        }
    }
}

impl<S> Origin<S> {
    /// The origin: endpoint, bucket, and key prefix.
    #[must_use]
    pub fn target(&self) -> &RemoteTarget {
        &self.target
    }

    /// The scope of the credentials the origin is read with:
    /// [`DEFAULT_CREDENTIALS`], or `profile:<name>` for a profile's
    /// ([`profile_credentials`]). Reads under different scopes may see
    /// different objects, so what the gateway knows to be fresh is keyed
    /// by it (§9.5).
    #[must_use]
    pub fn credentials(&self) -> &str {
        &self.credentials
    }

    /// The reader of the origin: HEADs that revalidate a read, reads of
    /// versions a gateway cannot fill, and forwarded listings.
    #[must_use]
    pub fn remote(&self) -> &RemoteReader<S> {
        &self.remote
    }

    /// The read-through fills of the origin's versions into the bucket's
    /// shards.
    #[must_use]
    pub fn filler(&self) -> &Filler<S> {
        &self.filler
    }
}

/// The `read_only` buckets of `buckets` that have an origin, with it.
pub(crate) fn origins(
    buckets: &[BucketDocument],
) -> BTreeMap<&BucketId, (&BucketDocument, &RemoteTarget)> {
    buckets
        .iter()
        .filter(|bucket| bucket.mode == BucketMode::ReadOnly)
        .filter_map(|bucket| Some((&bucket.bucket_id, (bucket, bucket.target.as_ref()?))))
        .collect()
}
