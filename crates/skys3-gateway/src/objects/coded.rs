//! A GET's bytes from the fragments of a coded version (§8.5).
//!
//! The read plan of a coded version names no replica: the entry's coded
//! layout names the fragments, and the gateway reads the range from the
//! data fragments that cover it, decoding a stripe that lost some from
//! any `k` of its fragments ([`skys3_ec::read_coded`]). The first piece
//! is read before the response starts, so a stripe with too few readable
//! fragments sends the GET back to resolve the key again. A read of the
//! whole object fills the hot cache once every byte has streamed, as a
//! read from another node's holder does.
//!
//! CopyObject reads a coded source the same way, whole, from the layout of
//! the source's entry ([`Objects::coded_source`]); UploadPartCopy reads
//! its source as a GET does.

use std::sync::Arc;

use s3s::dto::StreamingBlob;
use s3s::{S3Error, S3Result, s3_error};
use skys3_ec::{CodedBody, CodedRead, CodedReadError, read_coded};
use skys3_index::ObjectVersion;
use skys3_types::{BucketDocument, EpochSeq};
use tokio::sync::mpsc;

use super::{Found, Objects, download, empty_blob, hot_names};
use crate::shard::{ShardRef, Shards};

impl<H: Shards> Objects<H> {
    /// The whole of `object`, the coded version at `version` of `key` in
    /// `shard`, read from its fragments for a copy.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if this gateway reads no fragments, or a
    /// stripe has too few readable fragments for the read to start.
    pub(super) async fn coded_source(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        object: &ObjectVersion,
    ) -> S3Result<CodedBody> {
        let (Some(source), Some(coded)) = (&self.fragments, &object.coded) else {
            return Err(download::not_cached());
        };
        let read = CodedRead {
            shard: shard.into(),
            key: key.to_owned(),
            version,
            size: object.size,
            stripes: coded.stripes.clone(),
            range: 0..object.size,
        };
        read_coded(Arc::clone(source), read)
            .await
            .map_err(unreadable_source)
    }

    /// The bytes of `found`, a coded version of `key`, read from its
    /// fragments: the inner error if a stripe has too few readable
    /// fragments for the read to start.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if this gateway reads no fragments.
    pub(super) async fn read_coded(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
    ) -> S3Result<Result<StreamingBlob, CodedReadError>> {
        let (Some(source), Some(coded)) = (&self.fragments, &found.object.coded) else {
            return Err(download::not_cached());
        };
        if found.bytes.is_empty() {
            return Ok(Ok(empty_blob()));
        }
        let read = CodedRead {
            shard: (&found.shard).into(),
            key: key.to_owned(),
            version: found.version,
            size: found.object.size,
            stripes: coded.stripes.clone(),
            range: found.bytes.clone(),
        };
        let body = match read_coded(Arc::clone(source), read).await {
            Ok(body) => body,
            Err(error) => return Ok(Err(error)),
        };
        let remaining = found.bytes.end - found.bytes.start;
        let whole = found.bytes == (0..found.object.size);
        let body = if whole && self.hot_cache.admits(found.object.size) {
            self.keeping(bucket, key, found, body)
        } else {
            body
        };
        Ok(Ok(download::filled(body, remaining)))
    }

    /// `body`, the whole of `found`, passed on while the hot cache keeps
    /// it, if the cache reserves room for it; the cache holds it once every
    /// byte has streamed.
    fn keeping(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
        mut body: CodedBody,
    ) -> CodedBody {
        let (object, version) = hot_names(bucket, key, found);
        let Some(mut fill) = self.hot_cache.reserve(object, version, found.object.size) else {
            return body;
        };
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(piece) = body.recv().await {
                let failed = match &piece {
                    Ok(data) => {
                        fill.push(data);
                        false
                    }
                    Err(_) => true,
                };
                // A read that broke off, or whose response was dropped,
                // releases its room as the fill drops.
                if sender.send(piece).await.is_err() || failed {
                    return;
                }
            }
            fill.finish();
        });
        receiver
    }
}

/// A copy source whose fragments could not be read, before or while the
/// copy streamed them.
pub(super) fn unreadable_source(error: impl std::fmt::Display) -> S3Error {
    s3_error!(
        ServiceUnavailable,
        "The copy source's fragments could not be read: {error}"
    )
}
