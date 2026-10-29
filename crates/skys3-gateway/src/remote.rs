//! Reads of a `write_back` bucket's remote target, as the gateway makes
//! them (design §9.1).
//!
//! While a bucket's namespace import runs, the index does not yet hold the
//! keys the import has not reached. A read that finds no entry for such a
//! key falls through to a remote HEAD, and a listing merges the remote's
//! listing of them with the index. An imported stub carries no metadata;
//! the first HEAD or GET of it loads its `Content-Type` and user metadata
//! from the remote and commits them as an `ADOPT`. The node implements
//! [`RemoteReads`] over its flush service; without one, none of this
//! happens.

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use bytes::Bytes;
use skys3_index::{ImportCheckpoint, ListItem, ObjectVersion};
use skys3_types::{BucketId, ETag};

/// What a [`RemoteReads`] method returns.
pub type RemoteFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, RemoteError>> + Send + 'a>>;

/// A remote read that failed; the client is answered `503`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RemoteError(pub String);

/// A remote object as the gateway serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteObject {
    /// Its size, ETag, `Last-Modified`, and stored metadata
    /// (`content-type` and `x-amz-meta-*`), with no local bytes.
    pub object: ObjectVersion,
    /// Its remote version ID, on a versioned remote.
    pub version_id: Option<String>,
}

/// One request for a page of a remote listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteListing {
    /// Only keys that start with this.
    pub prefix: String,
    /// The delimiter that rolls keys up, if any.
    pub delimiter: Option<String>,
    /// Only keys after this, for the first page.
    pub start_after: Option<String>,
    /// The token of the page before, which replaces `start_after`.
    pub token: Option<String>,
    /// The most items the page holds.
    pub max_items: usize,
}

/// A page of a remote listing: keys and common prefixes in order, and the
/// token of the next page, if there is one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemotePage {
    /// The page's items, by name.
    pub items: Vec<ListItem>,
    /// The token that continues the listing.
    pub next: Option<String>,
}

/// The remote targets of `write_back` buckets, as the gateway reads them.
///
/// Keys are the bucket's, without the target's prefix. The methods return
/// boxed futures so that the gateway can hold any implementation behind one
/// `Arc<dyn RemoteReads>`.
pub trait RemoteReads: fmt::Debug + Send + Sync + 'static {
    /// How far the namespace import of `bucket` has got, or `None` if this
    /// node does not read the bucket's target.
    fn import(&self, bucket: &BucketId) -> Option<ImportCheckpoint>;

    /// HEADs `key`: `None` if the remote has no object there.
    ///
    /// # Errors
    ///
    /// Why the remote could not answer.
    fn head<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, Option<RemoteObject>>;

    /// The bytes `range` of `key` if its ETag is still `etag`, or `None`
    /// if the object changed or is gone.
    ///
    /// # Errors
    ///
    /// Why the remote could not answer.
    fn get<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        etag: &'a ETag,
        range: Range<u64>,
    ) -> RemoteFuture<'a, Option<Bytes>>;

    /// One page of the remote's listing.
    ///
    /// # Errors
    ///
    /// Why the remote could not answer.
    fn list<'a>(
        &'a self,
        bucket: &'a BucketId,
        listing: RemoteListing,
    ) -> RemoteFuture<'a, RemotePage>;
}
