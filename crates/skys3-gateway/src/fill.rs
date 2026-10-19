//! Read-through fill, as the gateway reaches it (design §9.2).
//!
//! A GET of an evicted version of a `write_back` bucket has no local bytes
//! to stream. The gateway reads it through [`Fills`] instead, which fills
//! the version from the bucket's remote target into the shard and streams
//! the requested range while the fill goes on. The node implements it over
//! `skys3_flush::Filler`; without one, such a GET answers `503`.
//!
//! A fill that finds the remote changed out of band commits `ADOPT`, and
//! answers [`FillError::Changed`]: the gateway then resolves the key again,
//! checks the request's conditions against what the key holds now, and
//! reads that.

use std::fmt;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;

use bytes::Bytes;
use skys3_types::EpochSeq;
use tokio::sync::mpsc;

use crate::shard::ShardRef;

/// The bytes of a range of a filled version, in order. An error ends the
/// stream: the fill failed after the response began.
pub type FillBody = mpsc::Receiver<io::Result<Bytes>>;

/// Why a read through a fill could not begin.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FillError {
    /// The key's entry no longer is the evicted version the read resolved:
    /// a fill adopted the remote's version, a local write replaced it, or
    /// another read filled it. The gateway resolves the key again.
    #[error("the object changed while it was read")]
    Changed,
    /// The version cannot be read from the remote now, or ever: the
    /// gateway answers `503 ServiceUnavailable` with the reason.
    #[error("{0}")]
    Unavailable(String),
}

/// Read-through fill of evicted versions (§9.2).
///
/// The method returns a boxed future so that the gateway can hold any
/// implementation behind one `Arc<dyn Fills>`.
pub trait Fills: fmt::Debug + Send + Sync + 'static {
    /// Reads the bytes `range` of the evicted version `version` of `key`,
    /// in `shard`, by filling it from the bucket's remote target. Reads of
    /// the same version share one fill. The future resolves once the
    /// range's first byte is filled; the body streams the rest.
    ///
    /// # Errors
    ///
    /// [`FillError::Changed`] if the key no longer holds that evicted
    /// version, and [`FillError::Unavailable`] if the fill failed.
    fn read(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<FillBody, FillError>> + Send + '_>>;
}
