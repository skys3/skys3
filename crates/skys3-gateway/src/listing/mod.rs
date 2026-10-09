//! ListObjectsV2 and ListObjects (V1) (design §9.4, §11).
//!
//! Both list one page across every shard of the bucket ([`merge`]): the
//! live objects and common prefixes after a starting point, in UTF-8 byte
//! order, at most `max-keys` of them (1,000 when absent or larger). An
//! item is a key or a common prefix, and pages resume after an item: a
//! listing resumed after a common prefix skips every key under it.
//!
//! - **V2** resumes from `continuation-token`, which takes precedence over
//!   `start-after`. `NextContinuationToken` is an HMAC-authenticated token
//!   holding the page's last item ([`token`]); a token that this gateway's
//!   keys did not sign for the same bucket, prefix, and delimiter answers
//!   `400 InvalidArgument`. Objects carry their owner only with
//!   `fetch-owner=true`.
//! - **V1** resumes after `marker`, any string. `NextMarker`, the page's
//!   last item, is returned when the page is truncated and a delimiter was
//!   given, as S3 does; without one, clients resume after the last key.
//!   Objects always carry their owner.
//!
//! With `encoding-type=url`, keys, common prefixes, and the echoed prefix,
//! delimiter, `start-after`, and markers are URL-encoded ([`url_encode`]).
//! Without it, a page holding a key with a control character, which XML
//! cannot carry, answers `400 InvalidArgument` (design §12); clients cannot
//! write such keys, but an import can find them at the remote.
//!
//! While a `write_back` bucket's namespace import runs, the page also
//! merges the remote's listing of the keys the import has not reached
//! (§9.1, [`merge`]). A `read_only` bucket's listings are forwarded to its
//! origin, which alone knows its keys (§9.5, [`forward`]); the pages and
//! tokens are the gateway's own, as for any bucket.
//! The owner of every object is the bucket owner, as in a bucket with
//! `BucketOwnerEnforced` ownership; SkyS3 names it by the cluster ID.

mod forward;
mod merge;
mod token;

use s3s::dto::{
    ChecksumAlgorithm as S3ChecksumAlgorithm, ChecksumType, CommonPrefix, EncodingType,
    ListObjectsInput, ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
    ObjectStorageClass, Owner,
};
use std::sync::Arc;

use s3s::{S3Error, S3Result, s3_error};
use skys3_index::{ImportCheckpoint, ListItem, ListQuery, ObjectVersion};
use skys3_types::checksum::ChecksumAlgorithm;
use skys3_types::limits::is_xml_text;
use skys3_types::{BucketDocument, BucketMode};

use crate::buckets::{GatewayConfig, shard_error};
use crate::conditions::{last_modified, s3_etag};
use crate::remote::{RemoteError, RemoteReads};
use crate::shard::Shards;
use merge::{MergeError, MergedPage, RemoteSide};
pub(crate) use token::TokenScope;
pub use token::{ListTokenKeys, ShortTokenKey};

/// The most items one page holds, and the default page size (S3's).
pub const MAX_KEYS: usize = 1000;

/// The storage class of an object that has no other.
const STANDARD: &str = "STANDARD";

/// The listing operations, over the shards.
pub(crate) struct Listings<H> {
    shards: H,
    keys: ListTokenKeys,
    owner: Owner,
    remote: Option<Arc<dyn RemoteReads>>,
}

/// The parameters V1 and V2 share.
struct Request<'a> {
    bucket: &'a BucketDocument,
    prefix: String,
    /// The delimiter as given, which is echoed.
    delimiter: Option<String>,
    max_keys: usize,
    url: bool,
}

impl Request<'_> {
    /// The delimiter that rolls keys up: an empty one rolls up nothing.
    fn effective_delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref().filter(|d| !d.is_empty())
    }

    fn scope(&self) -> TokenScope<'_> {
        TokenScope {
            bucket: &self.bucket.bucket_id,
            prefix: &self.prefix,
            delimiter: self.effective_delimiter(),
        }
    }

    fn query(&self, start_after: Option<String>) -> ListQuery {
        ListQuery {
            prefix: self.prefix.clone(),
            delimiter: self.effective_delimiter().map(str::to_owned),
            start_after,
            max_items: self.max_keys,
        }
    }

    /// `value` as the response shows it.
    fn encode(&self, value: &str) -> String {
        if self.url {
            url_encode(value)
        } else {
            value.to_owned()
        }
    }
}

/// A merged page, as the response shows it.
struct Page {
    contents: Vec<Object>,
    common_prefixes: Vec<CommonPrefix>,
    /// The name of the last item.
    last: Option<String>,
    truncated: bool,
}

impl<H: Shards> Listings<H> {
    pub(crate) fn new(shards: H, config: &GatewayConfig) -> Self {
        Self {
            shards,
            keys: config.list_token_keys.clone(),
            owner: Owner {
                id: Some(config.cluster_id.to_string()),
                display_name: None,
            },
            remote: config.remote.clone(),
        }
    }

    pub(crate) async fn list_v2(
        &self,
        bucket: &BucketDocument,
        input: ListObjectsV2Input,
    ) -> S3Result<ListObjectsV2Output> {
        let request = Request {
            bucket,
            prefix: input.prefix.unwrap_or_default(),
            delimiter: input.delimiter,
            max_keys: max_keys(input.max_keys)?,
            url: url_encoding(input.encoding_type.as_ref())?,
        };
        let start_after = match &input.continuation_token {
            Some(token) => Some(self.keys.open(request.scope(), token).map_err(|_| {
                s3_error!(
                    InvalidArgument,
                    "The continuation token provided is incorrect"
                )
            })?),
            None => input.start_after.clone().filter(|s| !s.is_empty()),
        };
        let owner = input.fetch_owner.unwrap_or(false);
        let page = self.page(&request, start_after, owner).await?;
        let next_continuation_token = page
            .last
            .as_deref()
            .filter(|_| page.truncated)
            .map(|last| self.keys.seal(request.scope(), last));
        Ok(ListObjectsV2Output {
            name: Some(bucket.name.to_string()),
            prefix: Some(request.encode(&request.prefix)),
            delimiter: request.delimiter.as_deref().map(|d| request.encode(d)),
            max_keys: i32::try_from(request.max_keys).ok(),
            key_count: i32::try_from(page.contents.len() + page.common_prefixes.len()).ok(),
            continuation_token: input.continuation_token,
            next_continuation_token,
            start_after: input.start_after.as_deref().map(|s| request.encode(s)),
            is_truncated: Some(page.truncated),
            contents: Some(page.contents),
            common_prefixes: Some(page.common_prefixes),
            encoding_type: input.encoding_type,
            ..ListObjectsV2Output::default()
        })
    }

    pub(crate) async fn list_v1(
        &self,
        bucket: &BucketDocument,
        input: ListObjectsInput,
    ) -> S3Result<ListObjectsOutput> {
        let request = Request {
            bucket,
            prefix: input.prefix.unwrap_or_default(),
            delimiter: input.delimiter,
            max_keys: max_keys(input.max_keys)?,
            url: url_encoding(input.encoding_type.as_ref())?,
        };
        let marker = input.marker.unwrap_or_default();
        let start_after = Some(marker.clone()).filter(|m| !m.is_empty());
        let page = self.page(&request, start_after, true).await?;
        let next_marker = page
            .last
            .as_deref()
            .filter(|_| page.truncated && request.delimiter.is_some())
            .map(|last| request.encode(last));
        Ok(ListObjectsOutput {
            name: Some(bucket.name.to_string()),
            prefix: Some(request.encode(&request.prefix)),
            delimiter: request.delimiter.as_deref().map(|d| request.encode(d)),
            marker: Some(request.encode(&marker)),
            next_marker,
            max_keys: i32::try_from(request.max_keys).ok(),
            is_truncated: Some(page.truncated),
            contents: Some(page.contents),
            common_prefixes: Some(page.common_prefixes),
            encoding_type: input.encoding_type,
            ..ListObjectsOutput::default()
        })
    }

    /// Lists one page after `start_after`.
    async fn page(
        &self,
        request: &Request<'_>,
        start_after: Option<String>,
        owner: bool,
    ) -> S3Result<Page> {
        let query = request.query(start_after);
        let bucket = request.bucket;
        let merged = if bucket.mode == BucketMode::ReadOnly {
            let origin = self.remote.as_deref().ok_or_else(|| {
                merge_error(MergeError::Remote(RemoteError(
                    "this node does not read origins".to_owned(),
                )))
            })?;
            forward::list(origin, bucket, &query)
                .await
                .map_err(|error| merge_error(MergeError::Remote(error)))?
        } else {
            self.merge(bucket, &query).await?
        };
        // Clients cannot write such keys (`check_new_key`), but an import
        // can find them at the remote.
        if !request.url && !merged.items.iter().all(|item| is_xml_text(item.name())) {
            return Err(s3_error!(
                InvalidArgument,
                "The listing holds a key with a control character, which XML cannot carry; \
                 list with encoding-type=url"
            ));
        }
        let last = merged.items.last().map(|item| item.name().to_owned());
        let mut page = Page {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            last,
            truncated: merged.truncated,
        };
        for item in merged.items {
            match item {
                ListItem::Object { key, object } => {
                    let owner = owner.then(|| self.owner.clone());
                    page.contents
                        .push(describe(request.encode(&key), &object, owner));
                }
                ListItem::Prefix(prefix) => page.common_prefixes.push(CommonPrefix {
                    prefix: Some(request.encode(&prefix)),
                }),
            }
        }
        Ok(page)
    }

    /// One page of `bucket`'s shards, merged (§9.4), with the remote's
    /// keys while a `write_back` bucket's import runs (§9.1).
    async fn merge(&self, bucket: &BucketDocument, query: &ListQuery) -> S3Result<MergedPage> {
        // While a `write_back` bucket's import runs, the remote lists the
        // keys the import has not reached (§9.1).
        let remote = self
            .remote
            .as_deref()
            .filter(|_| bucket.mode == BucketMode::WriteBack)
            .and_then(|remote| match remote.import(&bucket.bucket_id)? {
                ImportCheckpoint::Running { after } => Some(RemoteSide {
                    remote,
                    passed: after,
                }),
                ImportCheckpoint::Done => None,
            });
        merge::list(&self.shards, bucket, query, remote)
            .await
            .map_err(merge_error)
    }
}

fn merge_error(error: MergeError) -> S3Error {
    match error {
        MergeError::Shard(error) => shard_error(error),
        MergeError::Remote(error) => s3_error!(
            ServiceUnavailable,
            "The bucket's remote target or origin cannot be listed: {error}"
        ),
    }
}

/// An object as a listing shows it.
fn describe(key: String, object: &ObjectVersion, owner: Option<Owner>) -> Object {
    let mut algorithms = Vec::new();
    let mut checksum_type = None;
    for (algorithm, checksum) in &object.checksums {
        if *algorithm != ChecksumAlgorithm::Md5 {
            algorithms.push(S3ChecksumAlgorithm::from(algorithm.name().to_owned()));
            checksum_type = Some(ChecksumType::from(
                checksum.checksum_type().as_str().to_owned(),
            ));
        }
    }
    let storage_class = object.storage_class.as_deref().unwrap_or(STANDARD);
    Object {
        key: Some(key),
        last_modified: Some(last_modified(object)),
        e_tag: Some(s3_etag(object)),
        size: i64::try_from(object.size).ok(),
        storage_class: Some(ObjectStorageClass::from(storage_class.to_owned())),
        checksum_algorithm: (!algorithms.is_empty()).then_some(algorithms),
        checksum_type,
        owner,
        ..Object::default()
    }
}

/// The page size `max-keys` asks for.
fn max_keys(value: Option<i32>) -> S3Result<usize> {
    match value {
        None => Ok(MAX_KEYS),
        Some(n) => usize::try_from(n)
            .map(|n| n.min(MAX_KEYS))
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must not be negative")),
    }
}

/// Whether `encoding-type` asks for URL encoding, its only value.
fn url_encoding(value: Option<&EncodingType>) -> S3Result<bool> {
    match value.map(EncodingType::as_str) {
        None => Ok(false),
        Some(EncodingType::URL) => Ok(true),
        Some(_) => Err(s3_error!(
            InvalidArgument,
            "Invalid Encoding Method specified in Request"
        )),
    }
}

/// `value` URL-encoded as S3 encodes listings: unreserved characters and
/// `/` stay, a space becomes `+`, and every other byte is `%XX`. Clients
/// decode it as a form value.
fn url_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0xf)]));
            }
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listings_url_encode_as_forms() {
        assert_eq!(url_encode("photos/a b+c%d.jpg"), "photos/a+b%2Bc%25d.jpg");
        assert_eq!(url_encode("日\u{1}~"), "%E6%97%A5%01~");
        assert_eq!(url_encode(""), "");
    }

    #[test]
    fn page_sizes_are_bounded() {
        assert_eq!(max_keys(None).unwrap(), 1000);
        assert_eq!(max_keys(Some(0)).unwrap(), 0);
        assert_eq!(max_keys(Some(5)).unwrap(), 5);
        assert_eq!(max_keys(Some(5000)).unwrap(), 1000);
        assert!(max_keys(Some(-1)).is_err());
        assert!(url_encoding(Some(&EncodingType::from("url".to_owned()))).unwrap());
        assert!(!url_encoding(None).unwrap());
        assert!(url_encoding(Some(&EncodingType::from("base64".to_owned()))).is_err());
    }
}
