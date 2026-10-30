//! The large-object mix of the workload (plan M4-13): streamed single
//! `PUT`s, some of whose bodies fail validation after they streamed;
//! multipart uploads of two to four parts, with parts uploaded again,
//! parts copied from another key with UploadPartCopy, completions that
//! leave a part out, and uploads aborted or abandoned; small `PUT`s,
//! reads, and deletes.
//!
//! Every write carries its operation in its metadata (`x-amz-meta-op`),
//! and the client registers the version it may make before it sends the
//! request that would make it ([`Versions::expect`]), so that the audit
//! can tell exactly which complete version each object at the remote must
//! be. Parts of a multipart upload but the last are 520 to 900 bytes
//! long, never the 512 bytes a streamed single `PUT`'s parts are, so a
//! multipart ETag tells the two apart (`cluster::written`).

use base64::Engine as _;

use super::*;
use crate::large_objects::Expected;

/// The metadata header that names the operation that wrote an object.
pub(crate) const OP_HEADER: &str = "x-amz-meta-op";

/// The part size of a streamed single `PUT` at the remote: the nodes'
/// `flush_part_bytes` (§7.3).
pub(crate) const STREAM_PART_BYTES: usize = 512;

/// The smallest body that streams: the nodes'
/// `streaming_flush_min_bytes`.
const STREAM_MIN_BYTES: usize = 1024;

/// The sizes of every part of a multipart upload but the last.
const PART_BYTES: std::ops::RangeInclusive<usize> = 520..=900;

/// How many times the client tries to abort an upload it left open.
const ABORT_ATTEMPTS: usize = 40;

/// A multipart upload the client left open: its bucket, key, and ID.
pub(crate) struct OpenUpload {
    bucket: BucketDocument,
    key: String,
    id: String,
}

impl OpenUpload {
    /// Whether this is the upload `id` of the object at `path`. An ID alone
    /// does not name an upload: each shard numbers its own.
    fn is(&self, path: &str, id: &str) -> bool {
        self.id == id && path == format!("/{}/{}", self.bucket.name, self.key)
    }
}

/// The remote ETags a single `PUT` of `body` may get: its MD5, or, if it
/// streamed, the multipart ETag of its parts of `STREAM_PART_BYTES`.
pub(crate) fn single_put_etags(body: &[u8]) -> Vec<String> {
    let mut etags = vec![etag_of(body)];
    if body.len() >= STREAM_MIN_BYTES {
        let parts: Vec<&[u8]> = body.chunks(STREAM_PART_BYTES).collect();
        etags.push(multipart_etag(&parts));
    }
    etags
}

/// What sending a part of an upload left it as.
enum Sent {
    /// The part is stored: its bytes and ETag.
    Stored(Bytes, String),
    /// No answer, or a server error: the upload is given up.
    Failed,
}

impl Client {
    /// Sends one operation of the large-object mix on `key` in `bucket`.
    pub(super) async fn large_operation(
        &mut self,
        versions: &Versions,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
    ) -> turmoil::Result {
        match self.rng.random_range(0..100) {
            0..28 => {
                let len = self.rng.random_range(1100..=3000);
                let fails = self.rng.random_bool(0.15);
                self.streamed_put(versions, bucket, key, n, len, fails)
                    .await?;
            }
            28..58 => self.upload_parts(versions, bucket, key, n).await?,
            58..66 => {
                let len = self.rng.random_range(1..=1000);
                self.streamed_put(versions, bucket, key, n, len, false)
                    .await?;
            }
            66..80 => {
                self.read(bucket, key, Method::GET).await?;
            }
            80..88 => {
                self.read(bucket, key, Method::HEAD).await?;
            }
            _ => self.delete(bucket, key).await?,
        }
        Ok(())
    }

    /// The operation's name, which its metadata carries.
    fn op(&self, n: usize) -> String {
        format!("{}.{n}", self.process)
    }

    /// A body of `len` bytes no other write uses, tagged with `tag`.
    fn sized_body(&mut self, n: usize, tag: &str, len: usize) -> Bytes {
        let mut body = format!("{} operation {n}{tag}:", self.process).into_bytes();
        body.resize(len.max(body.len()), b'a' + (n % 26) as u8);
        Bytes::from(body)
    }

    /// A single `PUT` of a body of about `len` bytes, which streams from
    /// 1,024 bytes on. With `fails`, its `Content-MD5` is wrong: the body
    /// streams, and the gateway refuses it once it has all of it (`400
    /// BadDigest`), so it is never committed.
    async fn streamed_put(
        &mut self,
        versions: &Versions,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
        len: usize,
        fails: bool,
    ) -> turmoil::Result {
        let op = self.op(n);
        let body = self.sized_body(n, "", len);
        versions.expect(
            op.clone(),
            Expected {
                name: history_key(bucket, key),
                body: body.clone(),
                local_etag: etag_of(&body),
                remote_etags: single_put_etags(&body),
                committable: !fails,
            },
        );
        if !fails {
            let condition = if self.rng.random_bool(0.2) {
                Condition::IfAbsent
            } else {
                Condition::None
            };
            return self
                .put_body(bucket, key, body, condition, &[(OP_HEADER, &op)])
                .await;
        }
        let digest = base64::engine::general_purpose::STANDARD.encode(Md5::digest(b"other"));
        let request = Request::put(format!("/{}/{key}", bucket.name))
            .header(OP_HEADER, op)
            .header("content-md5", digest)
            .body(Full::new(body))?;
        let host = self.host(bucket, key);
        match self.send(&host, request).await {
            Ok(response) if response.status() == StatusCode::BAD_REQUEST => {
                let body = String::from_utf8_lossy(response.body());
                let code = element(&body, "Code");
                if !matches!(code, Some("BadDigest" | "RequestTimeout")) {
                    return Err(unexpected("a PUT with a wrong Content-MD5", &response));
                }
            }
            Ok(response) if response.status().is_server_error() => {}
            Ok(response) => return Err(unexpected("a PUT with a wrong Content-MD5", &response)),
            Err(_) => {}
        }
        Ok(())
    }

    /// A multipart upload of two to four parts: some uploaded again, some
    /// copied from another key, then completed with every part or all but
    /// one, aborted, or abandoned. Only the completion is recorded, as a
    /// `PUT` of the parts' multipart ETag.
    async fn upload_parts(
        &mut self,
        versions: &Versions,
        bucket: &BucketDocument,
        key: &str,
        n: usize,
    ) -> turmoil::Result {
        let op = self.op(n);
        let host = self.host(bucket, key);
        let path = format!("/{}/{key}", bucket.name);
        let request = Request::post(format!("{path}?uploads"))
            .header(OP_HEADER, op.as_str())
            .body(Full::default())?;
        let response = match self.send(&host, request).await {
            Ok(response) if response.status() == StatusCode::OK => response,
            // An upload the gateway made but did not answer stays open
            // until the verifier aborts what is listed.
            Ok(response) if response.status().is_server_error() => return Ok(()),
            Ok(response) => return Err(unexpected("a CreateMultipartUpload", &response)),
            Err(_) => return Ok(()),
        };
        let body = String::from_utf8_lossy(response.body());
        let Some(id) = element(&body, "UploadId").map(str::to_owned) else {
            return Err(unexpected(
                "a CreateMultipartUpload without an ID",
                &response,
            ));
        };
        self.uploads.push(OpenUpload {
            bucket: bucket.clone(),
            key: key.to_owned(),
            id: id.clone(),
        });

        let count: u16 = self.rng.random_range(2..=4);
        let mut parts: Vec<(u16, Bytes, String)> = Vec::new();
        for number in 1..=count {
            let last = number == count;
            let tries = if self.rng.random_bool(0.15) { 2 } else { 1 };
            for attempt in 0..tries {
                let len = if last {
                    self.rng.random_range(1..=*PART_BYTES.end())
                } else {
                    self.rng.random_range(PART_BYTES)
                };
                let copied = if self.rng.random_bool(0.2) {
                    self.copy_part(bucket, key, &host, &id, number, last)
                        .await?
                } else {
                    None
                };
                let sent = match copied {
                    Some(sent) => sent,
                    None => {
                        let tag = format!(" part {number} try {attempt}");
                        let body = self.sized_body(n, &tag, len);
                        self.upload_part(&host, &path, &id, number, body).await?
                    }
                };
                let Sent::Stored(body, etag) = sent else {
                    return Ok(());
                };
                parts.retain(|(stored, _, _)| *stored != number);
                parts.push((number, body, etag));
            }
        }

        match self.rng.random_range(0..100) {
            0..12 => return self.abort_upload(&host, &path, &id).await,
            // Left open: the client aborts it once it is done.
            12..20 => return Ok(()),
            // A completion that leaves out a part other than the last.
            20..35 if parts.len() > 1 => {
                let skipped = self.rng.random_range(0..parts.len() - 1);
                parts.remove(skipped);
            }
            _ => {}
        }
        let bodies: Vec<&[u8]> = parts.iter().map(|(_, body, _)| &body[..]).collect();
        let value = multipart_etag(&bodies);
        versions.expect(
            op,
            Expected {
                name: history_key(bucket, key),
                body: Bytes::from(bodies.concat()),
                local_etag: value.clone(),
                remote_etags: vec![value.clone()],
                committable: true,
            },
        );
        let listed: String = parts
            .iter()
            .map(|(number, _, etag)| {
                format!("<Part><PartNumber>{number}</PartNumber><ETag>\"{etag}\"</ETag></Part>")
            })
            .collect();
        let completion = format!("<CompleteMultipartUpload>{listed}</CompleteMultipartUpload>");
        let condition = if self.rng.random_bool(0.2) {
            Condition::IfAbsent
        } else {
            Condition::None
        };
        let mut request = Request::post(format!("{path}?uploadId={id}"));
        if condition == Condition::IfAbsent {
            request = request.header("if-none-match", "*");
        }
        let request = request.body(Full::new(Bytes::from(completion)))?;
        let name = history_key(bucket, key);
        let call = Call::Put {
            value: value.clone(),
            condition: condition.clone(),
        };
        let sent = elapsed();
        let Some((pending, answer)) = self.send_recorded(&host, &name, call, request).await else {
            self.time(bucket, key, sent, &Outcome::Unknown);
            return Ok(());
        };
        let outcome = match answer {
            Ok(response) => match response.status() {
                StatusCode::OK if String::from_utf8_lossy(response.body()).contains(&value) => {
                    self.uploads.retain(|upload| !upload.is(&path, &id));
                    self.seen.insert(name.clone(), value);
                    Outcome::Done
                }
                StatusCode::PRECONDITION_FAILED if condition != Condition::None => {
                    Outcome::ConditionFailed
                }
                status if status.is_server_error() => self.write_failed(),
                _ => return Err(unexpected("a CompleteMultipartUpload", &response)),
            },
            Err(_) => Outcome::Unknown,
        };
        self.time(bucket, key, sent, &outcome);
        self.answer_write(pending, outcome, &name);
        Ok(())
    }

    /// Uploads `body` as part `number` of the upload `id` of the object at
    /// `path`.
    async fn upload_part(
        &mut self,
        host: &str,
        path: &str,
        id: &str,
        number: u16,
        body: Bytes,
    ) -> Result<Sent, Box<dyn std::error::Error>> {
        let etag = etag_of(&body);
        let request = Request::put(format!("{path}?partNumber={number}&uploadId={id}"))
            .body(Full::new(body.clone()))?;
        Ok(match self.send(host, request).await {
            Ok(response) if response.status() == StatusCode::OK => Sent::Stored(body, etag),
            Ok(response) if response.status().is_server_error() || timed_out(&response) => {
                Sent::Failed
            }
            Ok(response) => return Err(unexpected("an UploadPart", &response)),
            Err(_) => Sent::Failed,
        })
    }

    /// Copies part `number` of the upload `id` of `key` from the whole of
    /// another key of `bucket` with UploadPartCopy, read first so that the
    /// copy, conditioned on the source's ETag, has known bytes. S3 copies
    /// ranges only of sources over 5 MiB, so the source is copied whole; a
    /// part but the `last` must not be 512 bytes long (see the module
    /// documentation). `None` if there is nothing to copy, in which case
    /// the part is uploaded.
    async fn copy_part(
        &mut self,
        bucket: &BucketDocument,
        key: &str,
        host: &str,
        id: &str,
        number: u16,
        last: bool,
    ) -> Result<Option<Sent>, Box<dyn std::error::Error>> {
        let source = format!("key-{}", self.rng.random_range(0..self.workload.keys));
        if source == key {
            return Ok(None);
        }
        let read = Request::get(format!("/{}/{source}", bucket.name)).body(Full::default())?;
        let (body, etag) = match self.send(host, read).await {
            Ok(response) if response.status() == StatusCode::OK => {
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|etag| etag.to_str().ok())
                    .map(|etag| unquote(etag).to_owned());
                match etag {
                    Some(etag) => (response.into_body(), etag),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        if body.is_empty() || (!last && body.len() == STREAM_PART_BYTES) {
            return Ok(None);
        }
        let request = Request::put(format!(
            "/{}/{key}?partNumber={number}&uploadId={id}",
            bucket.name
        ))
        .header("x-amz-copy-source", format!("{}/{source}", bucket.name))
        .header("x-amz-copy-source-if-match", format!("\"{etag}\""))
        .body(Full::default())?;
        Ok(match self.send(host, request).await {
            Ok(response) if response.status() == StatusCode::OK => {
                let answer = String::from_utf8_lossy(response.body());
                let part = element(&answer, "ETag").map(|etag| unquote(etag).to_owned());
                if part.as_deref() != Some(etag_of(&body).as_str()) {
                    return Err(unexpected("an UploadPartCopy of other bytes", &response));
                }
                let etag = etag_of(&body);
                Some(Sent::Stored(body, etag))
            }
            // The source changed or went since it was read.
            Ok(response)
                if matches!(
                    response.status(),
                    StatusCode::PRECONDITION_FAILED | StatusCode::NOT_FOUND
                ) =>
            {
                None
            }
            Ok(response) if response.status().is_server_error() => Some(Sent::Failed),
            Ok(response) => return Err(unexpected("an UploadPartCopy", &response)),
            Err(_) => Some(Sent::Failed),
        })
    }

    /// Aborts the upload `id` of the object at `path`; returns whether it
    /// is known to be gone.
    async fn abort(
        &self,
        host: &str,
        path: &str,
        id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let request = Request::delete(format!("{path}?uploadId={id}")).body(Full::default())?;
        Ok(match self.send(host, request).await {
            Ok(response)
                if matches!(
                    response.status(),
                    StatusCode::OK | StatusCode::NO_CONTENT | StatusCode::NOT_FOUND
                ) =>
            {
                true
            }
            Ok(response) if response.status().is_server_error() => false,
            Ok(response) => return Err(unexpected("an AbortMultipartUpload", &response)),
            Err(_) => false,
        })
    }

    /// Aborts the upload `id`, forgetting it if the abort is answered.
    async fn abort_upload(&mut self, host: &str, path: &str, id: &str) -> turmoil::Result {
        if self.abort(host, path, id).await? {
            self.uploads.retain(|upload| !upload.is(path, id));
        }
        Ok(())
    }

    /// Aborts every multipart upload still open in the buckets of the keys,
    /// as any node lists them, until none is listed or `attempts` listings
    /// were made: those whose `CreateMultipartUpload` answer no client got,
    /// as an operator or a lifecycle rule would end them.
    pub(super) async fn abort_listed_uploads(&mut self, attempts: usize) -> turmoil::Result {
        let mut buckets: Vec<(BucketDocument, String)> = Vec::new();
        for (_, bucket, key) in self.routes.keys(self.workload.keys) {
            if !buckets.iter().any(|(listed, _)| listed.name == bucket.name) {
                buckets.push((bucket, key));
            }
        }
        for (bucket, key) in buckets {
            for _ in 0..attempts {
                let host = self.host(&bucket, &key);
                let request =
                    Request::get(format!("/{}?uploads", bucket.name)).body(Full::default())?;
                let listed = match self.send(&host, request).await {
                    Ok(response) if response.status() == StatusCode::OK => {
                        String::from_utf8_lossy(response.body()).into_owned()
                    }
                    Ok(response) if response.status().is_server_error() || timed_out(&response) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                    Ok(response) => return Err(unexpected("a ListMultipartUploads", &response)),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                };
                let open: Vec<(String, String)> = listed
                    .split("<Upload>")
                    .skip(1)
                    .filter_map(|upload| {
                        Some((
                            element(upload, "Key")?.to_owned(),
                            element(upload, "UploadId")?.to_owned(),
                        ))
                    })
                    .collect();
                if open.is_empty() {
                    break;
                }
                for (key, id) in open {
                    let host = self.host(&bucket, &key);
                    self.abort(&host, &format!("/{}/{key}", bucket.name), &id)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Aborts every upload the client left open, retrying each until it is
    /// answered, through any node.
    pub(super) async fn abort_open_uploads(&mut self) -> turmoil::Result {
        for upload in std::mem::take(&mut self.uploads) {
            let path = format!("/{}/{}", upload.bucket.name, upload.key);
            for _ in 0..ABORT_ATTEMPTS {
                let host = self.host(&upload.bucket, &upload.key);
                if self.abort(&host, &path, &upload.id).await? {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        Ok(())
    }
}
