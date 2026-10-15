//! The S3 endpoint of the SkyS3 peer, as the nodes reach it (§7.8, plan
//! M6-07): the destination's own gateway, through a relay host.
//!
//! The destination serves its gateway on [`S3_PORT`] of its host, beside
//! its native peer service. The nodes send their S3 requests to the relay
//! host [`RELAY`], which forwards each connection to the destination, so
//! that the faults that cut or hold the links between the nodes and the
//! destination ([`Endpoint::Peer`](crate::Endpoint::Peer)) block only the
//! native transport, as a firewall that blocks UDP does, and S3 REST goes
//! on.
//!
//! [`PeerS3`] is the S3 client the flushers use for such a target: plain
//! HTTP/1.1 over the simulated network, one connection per request,
//! signed with nothing but the access key of the source cluster's
//! flushers in a header the destination's authenticator reads. It speaks
//! the REST API the flushers need, and records for the audits when each of
//! its writes was acknowledged ([`PeerLog`](crate::peer::PeerLog)).
//! [`SimStore`] is the store of every target the nodes flush to: the
//! harness's remote store, or the peer's gateway.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ListedObject, ListedPart, MetadataDirective, ObjectInfo, ObjectStore,
    PutObject, S3Error, S3ErrorKind, S3Result, TaggingDirective, UploadId, UploadPart,
    UserMetadata, VersionId, WriteOutput, WritePrecondition,
};
use skys3_sim::SimS3;
use skys3_types::{ETag, WriteIdentity};

use crate::peer::PeerLog;
use crate::s3::{self, S3_PORT};

/// The relay host the nodes reach the peer's gateway through.
pub(crate) const RELAY: &str = "peer-s3";
/// The header the destination's authenticator reads the access key from.
pub(crate) const ACCESS_KEY_HEADER: &str = "x-sim-access-key";
/// The access key of the source cluster's flushers, which the destination
/// lists in `s3_access_key_ids`.
pub(crate) const ACCESS_KEY: &str = "AKIASIMSOURCE";
/// How long a request waits for its connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a request waits for its whole answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(5);

/// The store of a target the nodes flush to.
#[derive(Clone, Debug)]
pub(crate) enum SimStore {
    /// The harness's remote store.
    Remote(SimS3),
    /// The SkyS3 peer's gateway.
    Peer(PeerS3),
}

/// Calls `$call` on the store either variant holds.
macro_rules! either {
    ($store:expr, $request:ident, $call:ident) => {
        match $store {
            SimStore::Remote(store) => store.$call($request).await,
            SimStore::Peer(store) => store.$call($request).await,
        }
    };
}

impl ObjectStore for SimStore {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        either!(self, request, put_object)
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        either!(self, request, get_object)
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        either!(self, request, head_object)
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        either!(self, request, delete_object)
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        either!(self, request, list_objects_v2)
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        either!(self, request, copy_object)
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        either!(self, request, create_multipart_upload)
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        either!(self, request, upload_part)
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        either!(self, request, complete_multipart_upload)
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        either!(self, request, abort_multipart_upload)
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        either!(self, request, list_parts)
    }
}

/// Forwards every connection accepted on [`S3_PORT`] to the peer's gateway,
/// until the relay host stops.
pub(crate) async fn relay(peer: &'static str) -> turmoil::Result {
    let listener = s3::bind().await?;
    loop {
        let (mut inbound, _) = listener.accept().await?;
        tokio::spawn(async move {
            let Ok(Ok(mut outbound)) = tokio::time::timeout(
                CONNECT_TIMEOUT,
                turmoil::net::TcpStream::connect((peer, S3_PORT)),
            )
            .await
            else {
                // The peer is down: the node's connection ends unanswered.
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        });
    }
}

/// A SkyS3 peer's bucket over S3 REST, through the relay.
#[derive(Clone, Debug)]
pub(crate) struct PeerS3 {
    bucket: String,
    log: Arc<PeerLog>,
}

impl PeerS3 {
    /// The peer's `bucket`, whose acknowledged writes `log` records.
    pub(crate) fn new(bucket: impl Into<String>, log: Arc<PeerLog>) -> Self {
        Self {
            bucket: bucket.into(),
            log,
        }
    }

    /// The path of `key` in the bucket, with `query`.
    fn path(&self, key: &str, query: &[(&str, String)]) -> String {
        let mut path = format!("/{}/{}", self.bucket, encode(key, true));
        append_query(&mut path, query);
        path
    }

    /// A request to the peer, as the source's flushers sign it.
    fn request(&self, method: Method, uri: &str) -> http::request::Builder {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("host", RELAY)
            .header(ACCESS_KEY_HEADER, ACCESS_KEY)
    }

    /// Sends `request` and returns its answer if it is a success.
    async fn send(
        &self,
        request: http::request::Builder,
        body: Bytes,
    ) -> S3Result<Response<Bytes>> {
        let request = request
            .body(Full::new(body))
            .map_err(|error| S3Error::new(S3ErrorKind::NotSent, error.to_string()))?;
        let lost = |error: s3::NoAnswer| S3Error::new(S3ErrorKind::Timeout, error.to_string());
        let connection = s3::connect(RELAY, CONNECT_TIMEOUT).await.map_err(lost)?;
        let response = connection
            .send(request, ANSWER_TIMEOUT)
            .await
            .map_err(lost)?;
        let status = response.status();
        if status.is_success() && !body_is_error(&response) {
            Ok(response)
        } else {
            Err(error_of(&response))
        }
    }

    /// Records that a write of `key` was acknowledged.
    fn acknowledged(&self, key: &str) {
        self.log.s3_write(key);
    }

    /// Sends the apply-by time of a write of `key`, if it has one, and
    /// records that the write was sent (design §7.8).
    fn bounded(
        &self,
        builder: http::request::Builder,
        key: &str,
        apply_by_ms: Option<u64>,
    ) -> http::request::Builder {
        self.log.s3_sent(key, apply_by_ms);
        match apply_by_ms {
            Some(apply_by) => builder.header(WriteIdentity::APPLY_BY_HEADER, apply_by),
            None => builder,
        }
    }
}

impl ObjectStore for PeerS3 {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        let mut builder = self.request(Method::PUT, &self.path(&request.key, &[]));
        builder = with_object_headers(
            builder,
            &request.metadata,
            request.content_type.as_deref(),
            &request.headers,
        );
        if !request.tags.is_empty() {
            builder = builder.header("x-amz-tagging", tagging(&request.tags));
        }
        if let Some(md5) = &request.content_md5 {
            builder = builder.header("content-md5", md5.as_str());
        }
        builder = with_precondition(builder, &request.precondition);
        builder = self.bounded(builder, &request.key, request.apply_by_ms);
        let response = self.send(builder, request.body).await?;
        self.acknowledged(&request.key);
        write_output(response.headers(), None)
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        let mut query = Vec::new();
        if let Some(version) = &request.version_id {
            query.push(("versionId", version.0.clone()));
        }
        let mut builder = self.request(Method::GET, &self.path(&request.key, &query));
        if let Some(range) = &request.range {
            builder = builder.header("range", range.to_string());
        }
        if let Some(etag) = &request.if_match {
            builder = builder.header("if-match", etag.to_quoted());
        }
        if let Some(etag) = &request.if_none_match {
            builder = builder.header("if-none-match", etag.to_quoted());
        }
        let response = self.send(builder, Bytes::new()).await?;
        let body = response.body().clone();
        let range = (response.status() == StatusCode::PARTIAL_CONTENT)
            .then(|| content_range(response.headers()))
            .flatten();
        let size = match (range.as_ref(), total_size(response.headers())) {
            (Some(_), Some(total)) => total,
            _ => body.len() as u64,
        };
        let info = object_info(response.headers(), size)?;
        Ok(GetOutput {
            info,
            body,
            range: range.map(|(first, last)| first..last + 1),
        })
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        let mut query = Vec::new();
        if let Some(version) = &request.version_id {
            query.push(("versionId", version.0.clone()));
        }
        let mut builder = self.request(Method::HEAD, &self.path(&request.key, &query));
        if let Some(etag) = &request.if_match {
            builder = builder.header("if-match", etag.to_quoted());
        }
        if let Some(etag) = &request.if_none_match {
            builder = builder.header("if-none-match", etag.to_quoted());
        }
        let response = self.send(builder, Bytes::new()).await?;
        let size = header(response.headers(), "content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        object_info(response.headers(), size)
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        let mut query = Vec::new();
        if let Some(version) = &request.version_id {
            query.push(("versionId", version.0.clone()));
        }
        let mut builder = self.request(Method::DELETE, &self.path(&request.key, &query));
        if let Some(etag) = &request.if_match {
            builder = builder.header("if-match", etag.to_quoted());
        }
        builder = self.bounded(builder, &request.key, request.apply_by_ms);
        let response = self.send(builder, Bytes::new()).await?;
        self.acknowledged(&request.key);
        Ok(DeleteOutput {
            version_id: header(response.headers(), "x-amz-version-id")
                .map(|v| VersionId(v.to_owned())),
            delete_marker: header(response.headers(), "x-amz-delete-marker") == Some("true"),
        })
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        let mut query = vec![
            ("list-type", "2".to_owned()),
            ("max-keys", request.max_keys.to_string()),
            ("prefix", request.prefix.clone()),
        ];
        if let Some(delimiter) = &request.delimiter {
            query.push(("delimiter", delimiter.clone()));
        }
        if let Some(token) = &request.continuation_token {
            query.push(("continuation-token", token.clone()));
        }
        if let Some(after) = &request.start_after {
            query.push(("start-after", after.clone()));
        }
        let mut uri = format!("/{}", self.bucket);
        append_query(&mut uri, &query);
        let response = self
            .send(self.request(Method::GET, &uri), Bytes::new())
            .await?;
        let xml = String::from_utf8_lossy(response.body()).into_owned();
        let mut objects = Vec::new();
        for contents in elements(&xml, "Contents") {
            let key = text(contents, "Key").ok_or_else(|| malformed("a listed object's key"))?;
            objects.push(ListedObject {
                key,
                etag: xml_etag(contents)?,
                size: text(contents, "Size")
                    .and_then(|size| size.parse().ok())
                    .ok_or_else(|| malformed("a listed object's size"))?,
                last_modified_ms: text(contents, "LastModified").and_then(|t| iso_ms(&t)),
                storage_class: text(contents, "StorageClass"),
            });
        }
        let common_prefixes = elements(&xml, "CommonPrefixes")
            .filter_map(|prefixes| text(prefixes, "Prefix"))
            .collect();
        Ok(ListObjectsV2Output {
            objects,
            common_prefixes,
            is_truncated: text(&xml, "IsTruncated").as_deref() == Some("true"),
            next_continuation_token: text(&xml, "NextContinuationToken"),
        })
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        let mut source = format!("/{}/{}", self.bucket, encode(&request.source_key, true));
        if let Some(version) = &request.source_version_id {
            append_query(&mut source, &[("versionId", version.0.clone())]);
        }
        let mut builder = self
            .request(Method::PUT, &self.path(&request.key, &[]))
            .header("x-amz-copy-source", source);
        if let Some(etag) = &request.source_if_match {
            builder = builder.header("x-amz-copy-source-if-match", etag.to_quoted());
        }
        match &request.metadata_directive {
            MetadataDirective::Copy => builder = builder.header("x-amz-metadata-directive", "COPY"),
            MetadataDirective::Replace {
                metadata,
                content_type,
                headers,
            } => {
                builder = builder.header("x-amz-metadata-directive", "REPLACE");
                builder = with_object_headers(builder, metadata, content_type.as_deref(), headers);
            }
        }
        match &request.tagging_directive {
            TaggingDirective::Copy => builder = builder.header("x-amz-tagging-directive", "COPY"),
            TaggingDirective::Replace(tags) => {
                builder = builder
                    .header("x-amz-tagging-directive", "REPLACE")
                    .header("x-amz-tagging", tagging(tags));
            }
        }
        builder = with_precondition(builder, &request.precondition);
        builder = self.bounded(builder, &request.key, request.apply_by_ms);
        let response = self.send(builder, Bytes::new()).await?;
        self.acknowledged(&request.key);
        let xml = String::from_utf8_lossy(response.body()).into_owned();
        write_output(response.headers(), Some(xml_etag(&xml)?))
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        let query = [("uploads", String::new())];
        let mut builder = self.request(Method::POST, &self.path(&request.key, &query));
        builder = with_object_headers(
            builder,
            &request.metadata,
            request.content_type.as_deref(),
            &request.headers,
        );
        if !request.tags.is_empty() {
            builder = builder.header("x-amz-tagging", tagging(&request.tags));
        }
        let response = self.send(builder, Bytes::new()).await?;
        let xml = String::from_utf8_lossy(response.body()).into_owned();
        text(&xml, "UploadId")
            .map(UploadId)
            .ok_or_else(|| malformed("the upload ID"))
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        let query = [
            ("partNumber", request.part_number.to_string()),
            ("uploadId", request.upload_id.0.clone()),
        ];
        let mut builder = self.request(Method::PUT, &self.path(&request.key, &query));
        if let Some(md5) = &request.content_md5 {
            builder = builder.header("content-md5", md5.as_str());
        }
        let response = self.send(builder, request.body).await?;
        header_etag(response.headers())
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        let query = [("uploadId", request.upload_id.0.clone())];
        let mut builder = self.request(Method::POST, &self.path(&request.key, &query));
        builder = with_precondition(builder, &request.precondition);
        builder = self.bounded(builder, &request.key, request.apply_by_ms);
        let mut xml = String::from("<CompleteMultipartUpload>");
        for part in &request.parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                part.part_number,
                escape(&part.etag.to_quoted())
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let response = self.send(builder, Bytes::from(xml)).await?;
        self.acknowledged(&request.key);
        let xml = String::from_utf8_lossy(response.body()).into_owned();
        write_output(response.headers(), Some(xml_etag(&xml)?))
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        let query = [("uploadId", request.upload_id.0.clone())];
        let builder = self.request(Method::DELETE, &self.path(&request.key, &query));
        self.send(builder, Bytes::new()).await.map(drop)
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        let mut query = vec![
            ("max-parts", request.max_parts.to_string()),
            ("uploadId", request.upload_id.0.clone()),
        ];
        if let Some(marker) = request.part_number_marker {
            query.push(("part-number-marker", marker.to_string()));
        }
        let builder = self.request(Method::GET, &self.path(&request.key, &query));
        let response = self.send(builder, Bytes::new()).await?;
        let xml = String::from_utf8_lossy(response.body()).into_owned();
        let mut parts = Vec::new();
        for part in elements(&xml, "Part") {
            parts.push(ListedPart {
                part_number: text(part, "PartNumber")
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| malformed("a part's number"))?,
                etag: xml_etag(part)?,
                size: text(part, "Size")
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| malformed("a part's size"))?,
            });
        }
        Ok(ListPartsOutput {
            parts,
            is_truncated: text(&xml, "IsTruncated").as_deref() == Some("true"),
            next_part_number_marker: text(&xml, "NextPartNumberMarker")
                .and_then(|marker| marker.parse().ok()),
        })
    }
}

/// Adds the user metadata, content type, and standard headers of an
/// object to `builder`.
fn with_object_headers(
    mut builder: http::request::Builder,
    metadata: &UserMetadata,
    content_type: Option<&str>,
    headers: &BTreeMap<String, String>,
) -> http::request::Builder {
    for (name, value) in metadata.iter() {
        builder = builder.header(format!("x-amz-meta-{name}"), value);
    }
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    for (name, value) in headers {
        if PutObject::HEADERS.contains(&name.as_str()) {
            builder = builder.header(name.as_str(), value.as_str());
        }
    }
    builder
}

/// Adds the headers of `precondition` to `builder`.
fn with_precondition(
    builder: http::request::Builder,
    precondition: &WritePrecondition,
) -> http::request::Builder {
    match precondition {
        WritePrecondition::None => builder,
        WritePrecondition::IfAbsent => builder.header("if-none-match", "*"),
        WritePrecondition::IfMatch(etag) => builder.header("if-match", etag.to_quoted()),
    }
}

/// `tags` as the value of `x-amz-tagging`.
fn tagging(tags: &BTreeMap<String, String>) -> String {
    tags.iter()
        .map(|(key, value)| format!("{}={}", encode(key, false), encode(value, false)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Appends `query` to `uri`, its values percent-encoded; an empty value
/// leaves a bare name, as `?uploads`.
fn append_query(uri: &mut String, query: &[(&str, String)]) {
    for (index, (name, value)) in query.iter().enumerate() {
        uri.push(if index == 0 { '?' } else { '&' });
        uri.push_str(name);
        if !value.is_empty() {
            uri.push('=');
            uri.push_str(&encode(value, false));
        }
    }
}

/// `text` percent-encoded but for unreserved characters, and `/` if
/// `path`.
fn encode(text: &str, path: bool) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            b'/' if path => encoded.push('/'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// The value of header `name`, if it is text.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn malformed(what: &str) -> S3Error {
    S3Error::new(
        S3ErrorKind::Other,
        format!("the peer's answer lacks {what}"),
    )
}

/// The `ETag` header of an answer.
fn header_etag(headers: &HeaderMap) -> S3Result<ETag> {
    header(headers, "etag")
        .and_then(|etag| ETag::from_quoted(etag).ok())
        .ok_or_else(|| malformed("an ETag"))
}

/// The ETag of the first `<ETag>` element of `xml`.
fn xml_etag(xml: &str) -> S3Result<ETag> {
    text(xml, "ETag")
        .and_then(|etag| ETag::from_quoted(&etag).or_else(|_| ETag::new(etag)).ok())
        .ok_or_else(|| malformed("an ETag"))
}

/// What a write answered: its ETag, from `etag` or the headers, and its
/// version.
fn write_output(headers: &HeaderMap, etag: Option<ETag>) -> S3Result<WriteOutput> {
    let etag = match etag {
        Some(etag) => etag,
        None => header_etag(headers)?,
    };
    Ok(WriteOutput {
        etag,
        version_id: header(headers, "x-amz-version-id").map(|v| VersionId(v.to_owned())),
    })
}

/// What the headers of a `GET` or `HEAD` say of the object, of `size`
/// bytes.
fn object_info(headers: &HeaderMap, size: u64) -> S3Result<ObjectInfo> {
    let mut metadata = UserMetadata::new();
    for (name, value) in headers {
        if let (Some(name), Ok(value)) = (name.as_str().strip_prefix("x-amz-meta-"), value.to_str())
        {
            // A key or value S3 would not take is not the flushers'.
            let _ = metadata.insert(name, value);
        }
    }
    Ok(ObjectInfo {
        etag: header_etag(headers)?,
        size,
        version_id: header(headers, "x-amz-version-id").map(|v| VersionId(v.to_owned())),
        metadata,
        content_type: header(headers, "content-type").map(str::to_owned),
        last_modified_ms: header(headers, "last-modified").and_then(http_date_ms),
        tag_count: header(headers, "x-amz-tagging-count")
            .and_then(|count| count.parse().ok())
            .unwrap_or(0),
    })
}

/// The first and last byte of a `Content-Range` answer.
fn content_range(headers: &HeaderMap) -> Option<(u64, u64)> {
    let range = header(headers, "content-range")?.strip_prefix("bytes ")?;
    let (span, _) = range.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    Some((first.parse().ok()?, last.parse().ok()?))
}

/// The object's size a `Content-Range` answer names.
fn total_size(headers: &HeaderMap) -> Option<u64> {
    let (_, total) = header(headers, "content-range")?.split_once('/')?;
    total.parse().ok()
}

/// Whether a `200` answer carries an error, as a `CompleteMultipartUpload`
/// may.
fn body_is_error(response: &Response<Bytes>) -> bool {
    response.status() == StatusCode::OK
        && response.body().starts_with(b"<?xml")
        && String::from_utf8_lossy(response.body()).contains("<Error>")
}

/// The error an answer that is not a success stands for.
fn error_of(response: &Response<Bytes>) -> S3Error {
    let status = response.status().as_u16();
    let xml = String::from_utf8_lossy(response.body()).into_owned();
    let code = text(&xml, "Code");
    let message = text(&xml, "Message").unwrap_or_else(|| format!("HTTP {status}"));
    let kind = kind_of(code.as_deref(), status);
    let error = S3Error::new(kind, message).with_status(status);
    match code {
        Some(code) => error.with_code(code),
        None => error,
    }
}

/// The kind of an error answered with `code` and `status`, as the AWS
/// store maps them.
fn kind_of(code: Option<&str>, status: u16) -> S3ErrorKind {
    const CODED: [S3ErrorKind; 18] = [
        S3ErrorKind::NotModified,
        S3ErrorKind::InvalidArgument,
        S3ErrorKind::InvalidRequest,
        S3ErrorKind::MetadataTooLarge,
        S3ErrorKind::InvalidPart,
        S3ErrorKind::InvalidPartOrder,
        S3ErrorKind::EntityTooSmall,
        S3ErrorKind::NoSuchKey,
        S3ErrorKind::NoSuchUpload,
        S3ErrorKind::NoSuchVersion,
        S3ErrorKind::MethodNotAllowed,
        S3ErrorKind::ConditionalRequestConflict,
        S3ErrorKind::PreconditionFailed,
        S3ErrorKind::InvalidRange,
        S3ErrorKind::InternalError,
        S3ErrorKind::NotImplemented,
        S3ErrorKind::SlowDown,
        S3ErrorKind::ServiceUnavailable,
    ];
    if let Some(kind) = code.and_then(|code| CODED.into_iter().find(|kind| kind.code() == code)) {
        return kind;
    }
    match (code, status) {
        (Some("RequestTimeout"), _) => S3ErrorKind::Timeout,
        (_, 304) => S3ErrorKind::NotModified,
        (_, 412) => S3ErrorKind::PreconditionFailed,
        (None | Some("NotFound"), 404) => S3ErrorKind::NoSuchKey,
        (None, 405) => S3ErrorKind::MethodNotAllowed,
        (None, 416) => S3ErrorKind::InvalidRange,
        (None, 429) => S3ErrorKind::SlowDown,
        (None, 501) => S3ErrorKind::NotImplemented,
        (None, 503) => S3ErrorKind::ServiceUnavailable,
        (None, 500 | 502 | 504) => S3ErrorKind::InternalError,
        _ => S3ErrorKind::Other,
    }
}

/// Every `<tag>` element of `xml`, by its inner text, in order.
fn elements<'a>(xml: &'a str, tag: &str) -> impl Iterator<Item = &'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let end = start + rest[start..].find(&close)?;
        let inner = &rest[start..end];
        rest = &rest[end + close.len()..];
        Some(inner)
    })
}

/// The text of the first `<tag>` element of `xml`, unescaped.
fn text(xml: &str, tag: &str) -> Option<String> {
    elements(xml, tag).next().map(unescape)
}

/// `text` with the XML entities S3 uses written out.
fn unescape(text: &str) -> String {
    text.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#34;", "\"")
        .replace("&amp;", "&")
}

/// `text` escaped for XML.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Days since the Unix epoch of a proleptic Gregorian date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let of_cycle = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    era * 146_097 + of_cycle - 719_468
}

/// Milliseconds since the Unix epoch of a time of day on a date.
fn epoch_ms(date: (i64, i64, i64), time: (i64, i64, i64), millis: i64) -> Option<u64> {
    let days = days_from_civil(date.0, date.1, date.2);
    let seconds = days * 86_400 + time.0 * 3600 + time.1 * 60 + time.2;
    u64::try_from(seconds * 1000 + millis).ok()
}

/// An HTTP date, as `Wed, 21 Oct 2015 07:28:00 GMT`.
fn http_date_ms(date: &str) -> Option<u64> {
    let mut parts = date.split_whitespace().skip(1);
    let day: i64 = parts.next()?.parse().ok()?;
    let month = parts.next()?;
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|name| *name == month)?;
    let year: i64 = parts.next()?.parse().ok()?;
    let mut time = parts.next()?.split(':').map(|n| n.parse::<i64>().ok());
    let (hours, minutes, seconds) = (time.next()??, time.next()??, time.next()??);
    epoch_ms(
        (year, i64::try_from(month).ok()? + 1, day),
        (hours, minutes, seconds),
        0,
    )
}

/// An ISO 8601 time, as `2015-10-21T07:28:00.000Z`.
fn iso_ms(time: &str) -> Option<u64> {
    let (date, rest) = time.split_once('T')?;
    let mut date = date.split('-').map(|n| n.parse::<i64>().ok());
    let (year, month, day) = (date.next()??, date.next()??, date.next()??);
    let rest = rest.trim_end_matches('Z');
    let (clock, fraction) = rest.split_once('.').unwrap_or((rest, "0"));
    let mut clock = clock.split(':').map(|n| n.parse::<i64>().ok());
    let (hours, minutes, seconds) = (clock.next()??, clock.next()??, clock.next()??);
    let millis = fraction.get(..3.min(fraction.len()))?.parse::<i64>().ok()?;
    epoch_ms((year, month, day), (hours, minutes, seconds), millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_queries_are_percent_encoded() {
        assert_eq!(encode("a/b c+d", true), "a/b%20c%2Bd");
        assert_eq!(encode("a/b", false), "a%2Fb");
        let mut uri = "/b".to_owned();
        append_query(&mut uri, &[("uploads", String::new()), ("x", "1 2".into())]);
        assert_eq!(uri, "/b?uploads&x=1%202");
        let tags = BTreeMap::from([("a b".to_owned(), "c".to_owned())]);
        assert_eq!(tagging(&tags), "a%20b=c");
    }

    #[test]
    fn listings_are_parsed() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
                   <Contents><Key>a&amp;b</Key><ETag>&quot;abc&quot;</ETag><Size>3</Size>\
                   <LastModified>1970-01-02T00:00:01.500Z</LastModified></Contents>\
                   <Contents><Key>c</Key></Contents>\
                   <NextContinuationToken>t</NextContinuationToken></ListBucketResult>";
        let contents: Vec<&str> = elements(xml, "Contents").collect();
        assert_eq!(contents.len(), 2);
        assert_eq!(text(contents[0], "Key").as_deref(), Some("a&b"));
        assert_eq!(xml_etag(contents[0]).unwrap().as_str(), "abc");
        assert!(xml_etag(contents[1]).is_err());
        assert_eq!(text(xml, "NextContinuationToken").as_deref(), Some("t"));
        assert_eq!(iso_ms("1970-01-02T00:00:01.500Z"), Some(86_401_500));
        assert_eq!(iso_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(http_date_ms("Thu, 01 Jan 1970 00:00:02 GMT"), Some(2000));
        assert_eq!(
            http_date_ms("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(1_445_412_480_000)
        );
        assert_eq!(http_date_ms("garbage"), None);
        assert_eq!(escape("\"a\"&"), "&quot;a&quot;&amp;");
    }

    #[test]
    fn errors_map_like_the_aws_store() {
        assert_eq!(kind_of(Some("NoSuchKey"), 404), S3ErrorKind::NoSuchKey);
        assert_eq!(kind_of(None, 404), S3ErrorKind::NoSuchKey);
        assert_eq!(kind_of(None, 412), S3ErrorKind::PreconditionFailed);
        assert_eq!(kind_of(Some("RequestTimeout"), 400), S3ErrorKind::Timeout);
        assert_eq!(kind_of(None, 503), S3ErrorKind::ServiceUnavailable);
        assert_eq!(kind_of(None, 502), S3ErrorKind::InternalError);
        assert_eq!(kind_of(Some("AccessDenied"), 403), S3ErrorKind::Other);
        let response = Response::builder()
            .status(409)
            .body(Bytes::from_static(
                b"<Error><Code>ConditionalRequestConflict</Code><Message>m</Message></Error>",
            ))
            .unwrap();
        let error = error_of(&response);
        assert_eq!(error.kind(), S3ErrorKind::ConditionalRequestConflict);
        assert_eq!(error.message(), "m");
        assert_eq!(error.status(), Some(409));
    }
}
