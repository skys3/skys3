//! The bucket model: what each S3 operation does to the stored objects, with
//! no faults and no timing. [`SimS3`](super::SimS3) adds those around it.
//!
//! Preconditions reaching this model are already the ones the configured
//! provider honors: an ignored precondition has been removed, and a rejected
//! one never gets here.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use md5::{Digest, Md5};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ListedObject, ListedPart, MAX_KEY_LEN, MAX_LIST_KEYS, MAX_LIST_PARTS,
    MetadataDirective, ObjectInfo, PART_NUMBERS, PutObject, S3Error, S3ErrorKind, S3Result,
    UploadId, UploadPart, UserMetadata, VersionId, WriteOutput, WritePrecondition,
};
use skys3_types::ETag;

use super::SimS3Config;

/// A key's current object at some moment, with its version ID.
type Snapshot = (Option<VersionId>, Arc<Stored>);

/// One stored object version's contents.
#[derive(Debug)]
struct Stored {
    body: Bytes,
    etag: ETag,
    metadata: UserMetadata,
    content_type: Option<String>,
}

/// One entry in a key's version history: an object, or a delete marker.
#[derive(Debug)]
struct Version {
    /// `None` on an unversioned bucket.
    id: Option<VersionId>,
    /// `None` for a delete marker.
    object: Option<Arc<Stored>>,
}

#[derive(Debug)]
struct Part {
    body: Bytes,
    md5: [u8; 16],
    etag: ETag,
}

#[derive(Debug)]
struct Upload {
    key: String,
    metadata: UserMetadata,
    content_type: Option<String>,
    parts: BTreeMap<u32, Part>,
}

/// The objects and uploads of one simulated bucket.
#[derive(Debug)]
pub(super) struct Bucket {
    config: SimS3Config,
    /// Each key's versions, oldest first. On an unversioned bucket a key
    /// has exactly one, an object; deleting it removes the key.
    keys: BTreeMap<String, Vec<Version>>,
    uploads: BTreeMap<UploadId, Upload>,
    /// Continuation tokens issued, and the last entry of the page each one
    /// continues, so that tokens stay opaque and nothing parses them.
    list_tokens: HashMap<String, String>,
    /// Numbers version IDs, upload IDs, and continuation tokens.
    next_id: u64,
    /// What each written key's current object was before its latest
    /// write, with its version ID, or `None` if it had none: what a stale
    /// read answers with.
    previous: BTreeMap<String, Option<Snapshot>>,
}

impl Bucket {
    pub(super) fn new(config: SimS3Config) -> Self {
        Bucket {
            config,
            keys: BTreeMap::new(),
            uploads: BTreeMap::new(),
            list_tokens: HashMap::new(),
            next_id: 1,
            previous: BTreeMap::new(),
        }
    }

    /// The key's current object and its version ID, if it has one.
    fn current_version(&self, key: &str) -> Option<Snapshot> {
        let version = self.keys.get(key)?.last()?;
        Some((version.id.clone(), Arc::clone(version.object.as_ref()?)))
    }

    /// Applies a write to `key`, and remembers the key's current object
    /// from before it if the write changed it, for stale reads.
    pub(super) fn apply_write<T>(
        &mut self,
        key: &str,
        apply: impl FnOnce(&mut Self) -> S3Result<T>,
    ) -> S3Result<T> {
        let before = self.current_version(key);
        let result = apply(self);
        let unchanged = |after: &Option<Snapshot>| match (&before, after) {
            (None, None) => true,
            (Some((a, x)), Some((b, y))) => a == b && Arc::ptr_eq(x, y),
            _ => false,
        };
        if result.is_ok() && !unchanged(&self.current_version(key)) {
            self.previous.insert(key.to_owned(), before);
        }
        result
    }

    fn next_id(&mut self, kind: &str) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("{kind}-{id:016x}")
    }

    /// Returns the key's current object, if it has one that is not a delete
    /// marker.
    fn current(&self, key: &str) -> Option<&Arc<Stored>> {
        self.keys.get(key)?.last()?.object.as_ref()
    }

    /// Returns the object a read names, and its version ID if the read
    /// names the object it held before its latest write: a version, the
    /// current object, or with `stale`, the key's object from before its
    /// latest write.
    fn read_target(
        &self,
        key: &str,
        version_id: Option<&VersionId>,
        stale: bool,
    ) -> S3Result<(&Arc<Stored>, Option<Option<VersionId>>)> {
        let Some(version_id) = version_id else {
            if stale && let Some(previous) = self.previous.get(key) {
                return match previous {
                    Some((id, object)) => Ok((object, Some(id.clone()))),
                    None => Err(no_such_key(key)),
                };
            }
            return Ok((self.current(key).ok_or_else(|| no_such_key(key))?, None));
        };
        self.check_versioned()?;
        let version = self
            .keys
            .get(key)
            .and_then(|versions| versions.iter().find(|v| v.id.as_ref() == Some(version_id)))
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorKind::NoSuchVersion,
                    format!("{key:?} has no version {version_id}"),
                )
            })?;
        let object = version.object.as_ref().ok_or_else(|| {
            S3Error::new(
                S3ErrorKind::MethodNotAllowed,
                format!("version {version_id} of {key:?} is a delete marker"),
            )
        })?;
        Ok((object, None))
    }

    fn check_versioned(&self) -> S3Result<()> {
        if self.config.versioning {
            Ok(())
        } else {
            Err(S3Error::new(
                S3ErrorKind::InvalidArgument,
                "the bucket is not versioned",
            ))
        }
    }

    /// Evaluates a write precondition against the key's current object.
    fn check_precondition(&self, key: &str, precondition: &WritePrecondition) -> S3Result<()> {
        let current = self.current(key);
        match (precondition, current) {
            (WritePrecondition::None, _) | (WritePrecondition::IfAbsent, None) => Ok(()),
            (WritePrecondition::IfAbsent, Some(_)) => Err(S3Error::new(
                S3ErrorKind::PreconditionFailed,
                format!("{key:?} exists"),
            )),
            (WritePrecondition::IfMatch(_), None) => Err(no_such_key(key)),
            (WritePrecondition::IfMatch(etag), Some(current)) => check_etag(key, etag, current),
        }
    }

    fn check_metadata(&self, metadata: &UserMetadata) -> S3Result<()> {
        let size = metadata.size();
        if size > self.config.max_metadata_size {
            return Err(S3Error::new(
                S3ErrorKind::MetadataTooLarge,
                format!(
                    "user metadata is {size} bytes; the limit is {}",
                    self.config.max_metadata_size
                ),
            ));
        }
        Ok(())
    }

    /// Stores `object` as the key's new current version.
    fn store(&mut self, key: String, object: Stored) -> WriteOutput {
        let etag = object.etag.clone();
        let id = self.config.versioning.then(|| VersionId(self.next_id("v")));
        let version = Version {
            id: id.clone(),
            object: Some(Arc::new(object)),
        };
        if self.config.versioning {
            self.keys.entry(key).or_default().push(version);
        } else {
            self.keys.insert(key, vec![version]);
        }
        WriteOutput {
            etag,
            version_id: id,
        }
    }

    pub(super) fn put_object(&mut self, request: PutObject) -> S3Result<WriteOutput> {
        check_key(&request.key)?;
        self.check_metadata(&request.metadata)?;
        self.check_precondition(&request.key, &request.precondition)?;
        let etag = md5_etag(&md5(&request.body));
        Ok(self.store(
            request.key,
            Stored {
                body: request.body,
                etag,
                metadata: request.metadata,
                content_type: request.content_type,
            },
        ))
    }

    /// `GetObject`, answered from before the key's latest write if `stale`.
    pub(super) fn get_object(&self, request: &GetObject, stale: bool) -> S3Result<GetOutput> {
        let (object, stale_id) =
            self.read_target(&request.key, request.version_id.as_ref(), stale)?;
        check_read_conditions(
            &request.key,
            object,
            request.if_match.as_ref(),
            request.if_none_match.as_ref(),
        )?;
        let info = self.info(&request.key, object, request.version_id.as_ref(), stale_id);
        let Some(range) = request.range else {
            return Ok(GetOutput {
                info,
                body: object.body.clone(),
                range: None,
            });
        };
        let size = object.body.len() as u64;
        let selected = range.resolve(size).ok_or_else(|| {
            S3Error::new(
                S3ErrorKind::InvalidRange,
                format!("{range} is outside a {size}-byte object"),
            )
        })?;
        // Both ends are at most the body's length, which fits in usize.
        let body = object
            .body
            .slice(selected.start as usize..selected.end as usize);
        Ok(GetOutput {
            info,
            body,
            range: Some(selected),
        })
    }

    /// `HeadObject`, answered from before the key's latest write if
    /// `stale`.
    pub(super) fn head_object(&self, request: &HeadObject, stale: bool) -> S3Result<ObjectInfo> {
        let (object, stale_id) =
            self.read_target(&request.key, request.version_id.as_ref(), stale)?;
        check_read_conditions(
            &request.key,
            object,
            request.if_match.as_ref(),
            request.if_none_match.as_ref(),
        )?;
        Ok(self.info(&request.key, object, request.version_id.as_ref(), stale_id))
    }

    /// The attributes of `object`, read as `version_id`, or as the stale
    /// version `stale_id`, or as the key's current version.
    fn info(
        &self,
        key: &str,
        object: &Stored,
        version_id: Option<&VersionId>,
        stale_id: Option<Option<VersionId>>,
    ) -> ObjectInfo {
        let version_id = version_id.cloned().or_else(|| {
            stale_id.unwrap_or_else(|| {
                self.keys
                    .get(key)
                    .and_then(|versions| versions.last())
                    .and_then(|version| version.id.clone())
            })
        });
        ObjectInfo {
            etag: object.etag.clone(),
            size: object.body.len() as u64,
            version_id,
            metadata: object.metadata.clone(),
            content_type: object.content_type.clone(),
        }
    }

    pub(super) fn delete_object(&mut self, request: DeleteObject) -> S3Result<DeleteOutput> {
        let key = request.key;
        if let Some(version_id) = request.version_id {
            self.check_versioned()?;
            if let Some(etag) = &request.if_match {
                check_etag(
                    &key,
                    etag,
                    self.read_target(&key, Some(&version_id), false)?.0,
                )?;
            }
            let Some(versions) = self.keys.get_mut(&key) else {
                return Ok(DeleteOutput {
                    version_id: Some(version_id),
                    delete_marker: false,
                });
            };
            let removed = versions
                .iter()
                .position(|v| v.id.as_ref() == Some(&version_id))
                .map(|index| versions.remove(index));
            if versions.is_empty() {
                self.keys.remove(&key);
            }
            return Ok(DeleteOutput {
                version_id: Some(version_id),
                delete_marker: removed.is_some_and(|v| v.object.is_none()),
            });
        }

        if let Some(etag) = &request.if_match {
            self.check_precondition(&key, &WritePrecondition::IfMatch(etag.clone()))?;
        }
        if !self.config.versioning {
            self.keys.remove(&key);
            return Ok(DeleteOutput::default());
        }
        let id = VersionId(self.next_id("v"));
        self.keys.entry(key).or_default().push(Version {
            id: Some(id.clone()),
            object: None,
        });
        Ok(DeleteOutput {
            version_id: Some(id),
            delete_marker: true,
        })
    }

    /// Lists one page. Keys roll up into common prefixes, and S3 returns an
    /// entry, key or common prefix, only if it sorts after the position the
    /// page starts from: `start_after`, or the last entry of the previous
    /// page. So a common prefix at or before `start_after` is left out even
    /// if keys under it come after, and a common prefix is never returned on
    /// two pages.
    ///
    /// `max_keys == 0` returns an empty page that is not truncated, as S3
    /// does. It tells the caller nothing about the keys, so callers never
    /// send it.
    ///
    /// A `stale` listing lists every key as it was before its latest write.
    pub(super) fn list_objects_v2(
        &mut self,
        request: &ListObjectsV2,
        stale: bool,
    ) -> S3Result<ListObjectsV2Output> {
        let position = match &request.continuation_token {
            Some(token) => Some(self.list_tokens.get(token).cloned().ok_or_else(|| {
                S3Error::new(
                    S3ErrorKind::InvalidArgument,
                    "the continuation token is not valid",
                )
            })?),
            None => request.start_after.clone(),
        };
        let prefix = request.prefix.as_str();
        let delimiter = request.delimiter.as_deref().filter(|d| !d.is_empty());
        let max_keys = request.max_keys.min(MAX_LIST_KEYS) as usize;
        let mut output = ListObjectsV2Output::default();
        if max_keys == 0 {
            return Ok(output);
        }

        let start = match &position {
            Some(after) if after.as_str() >= prefix => Bound::Excluded(after.clone()),
            _ => Bound::Included(prefix.to_owned()),
        };
        // The last entry returned, or the start position: every entry
        // returned sorts after it.
        let mut floor = position;
        let mut emitted = 0;
        let range = (start, Bound::Unbounded);
        let current = self
            .keys
            .range::<String, _>(range.clone())
            .map(|(key, versions)| (key, versions.last().and_then(|v| v.object.as_ref())));
        let entries: Box<dyn Iterator<Item = (&String, Option<&Arc<Stored>>)>> = if stale {
            let mut entries: BTreeMap<_, _> = current.collect();
            for (key, before) in self.previous.range::<String, _>(range) {
                entries.insert(key, before.as_ref().map(|(_, object)| object));
            }
            Box::new(entries.into_iter())
        } else {
            Box::new(current)
        };
        for (key, object) in entries {
            if !key.starts_with(prefix) {
                break;
            }
            let Some(object) = object else {
                continue;
            };
            let common_prefix = delimiter.and_then(|delimiter| {
                let rest = &key[prefix.len()..];
                rest.find(delimiter)
                    .map(|at| &key[..prefix.len() + at + delimiter.len()])
            });
            let entry = common_prefix.unwrap_or(key);
            if floor.as_deref().is_some_and(|floor| entry <= floor) {
                continue;
            }
            if emitted == max_keys {
                output.is_truncated = true;
                break;
            }
            emitted += 1;
            match common_prefix {
                Some(common_prefix) => output.common_prefixes.push(common_prefix.to_owned()),
                None => output.objects.push(ListedObject {
                    key: key.clone(),
                    etag: object.etag.clone(),
                    size: object.body.len() as u64,
                }),
            }
            floor = Some(entry.to_owned());
        }
        if output.is_truncated
            && let Some(floor) = floor
        {
            let token = self.next_id("t");
            self.list_tokens.insert(token.clone(), floor);
            output.next_continuation_token = Some(token);
        }
        Ok(output)
    }

    pub(super) fn copy_object(&mut self, request: CopyObject) -> S3Result<WriteOutput> {
        check_key(&request.key)?;
        let source = Arc::clone(
            self.read_target(
                &request.source_key,
                request.source_version_id.as_ref(),
                false,
            )?
            .0,
        );
        if let Some(etag) = &request.source_if_match {
            check_etag(&request.source_key, etag, &source)?;
        }
        let (metadata, content_type) = match request.metadata_directive {
            MetadataDirective::Copy => {
                if request.source_key == request.key && request.source_version_id.is_none() {
                    return Err(S3Error::new(
                        S3ErrorKind::InvalidRequest,
                        "copying an object to itself must replace its metadata",
                    ));
                }
                (source.metadata.clone(), source.content_type.clone())
            }
            MetadataDirective::Replace {
                metadata,
                content_type,
            } => (metadata, content_type),
        };
        self.check_metadata(&metadata)?;
        self.check_precondition(&request.key, &request.precondition)?;
        // The copy is a single-part object, so its ETag is its body's MD5
        // even when the source was a multipart upload.
        let etag = md5_etag(&md5(&source.body));
        Ok(self.store(
            request.key,
            Stored {
                body: source.body.clone(),
                etag,
                metadata,
                content_type,
            },
        ))
    }

    pub(super) fn create_multipart_upload(
        &mut self,
        request: CreateMultipartUpload,
    ) -> S3Result<UploadId> {
        check_key(&request.key)?;
        self.check_metadata(&request.metadata)?;
        let upload_id = UploadId(self.next_id("upload"));
        self.uploads.insert(
            upload_id.clone(),
            Upload {
                key: request.key,
                metadata: request.metadata,
                content_type: request.content_type,
                parts: BTreeMap::new(),
            },
        );
        Ok(upload_id)
    }

    fn upload_mut(&mut self, key: &str, upload_id: &UploadId) -> S3Result<&mut Upload> {
        self.uploads
            .get_mut(upload_id)
            .filter(|upload| upload.key == key)
            .ok_or_else(|| no_such_upload(upload_id))
    }

    pub(super) fn upload_part(&mut self, request: UploadPart) -> S3Result<ETag> {
        let upload = self.upload_mut(&request.key, &request.upload_id)?;
        if !PART_NUMBERS.contains(&request.part_number) {
            return Err(S3Error::new(
                S3ErrorKind::InvalidArgument,
                format!("part number {} is out of range", request.part_number),
            ));
        }
        let md5 = md5(&request.body);
        let etag = md5_etag(&md5);
        upload.parts.insert(
            request.part_number,
            Part {
                body: request.body,
                md5,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    pub(super) fn complete_multipart_upload(
        &mut self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        let min_part_size = self.config.min_part_size;
        let upload = self.upload_mut(&request.key, &request.upload_id)?;
        if request.parts.is_empty() {
            return Err(S3Error::new(
                S3ErrorKind::InvalidRequest,
                "a multipart upload must be completed with at least one part",
            ));
        }
        if request
            .parts
            .windows(2)
            .any(|pair| pair[0].part_number >= pair[1].part_number)
        {
            return Err(S3Error::new(
                S3ErrorKind::InvalidPartOrder,
                "parts must be listed in ascending order",
            ));
        }
        let mut parts = Vec::with_capacity(request.parts.len());
        for completed in &request.parts {
            match upload.parts.get(&completed.part_number) {
                Some(part) if part.etag == completed.etag => parts.push(part),
                _ => {
                    return Err(S3Error::new(
                        S3ErrorKind::InvalidPart,
                        format!(
                            "part {} with ETag {} was not uploaded",
                            completed.part_number, completed.etag
                        ),
                    ));
                }
            }
        }
        if let Some((number, _)) = request
            .parts
            .iter()
            .zip(&parts)
            .rev()
            .skip(1)
            .find(|(_, part)| (part.body.len() as u64) < min_part_size)
        {
            return Err(S3Error::new(
                S3ErrorKind::EntityTooSmall,
                format!(
                    "part {} is smaller than the minimum part size, {min_part_size} bytes",
                    number.part_number
                ),
            ));
        }

        let mut body = BytesMut::with_capacity(parts.iter().map(|p| p.body.len()).sum());
        let mut digests = Vec::with_capacity(16 * parts.len());
        for part in &parts {
            body.extend_from_slice(&part.body);
            digests.extend_from_slice(&part.md5);
        }
        let etag = multipart_etag(&digests, parts.len());
        self.check_precondition(&request.key, &request.precondition)?;
        let Some(upload) = self.uploads.remove(&request.upload_id) else {
            unreachable!("the upload was found above");
        };
        Ok(self.store(
            request.key,
            Stored {
                body: body.freeze(),
                etag,
                metadata: upload.metadata,
                content_type: upload.content_type,
            },
        ))
    }

    pub(super) fn abort_multipart_upload(
        &mut self,
        request: &AbortMultipartUpload,
    ) -> S3Result<()> {
        self.upload_mut(&request.key, &request.upload_id)?;
        self.uploads.remove(&request.upload_id);
        Ok(())
    }

    /// Lists one page of an upload's parts. As with
    /// [`Bucket::list_objects_v2`], `max_parts == 0` returns an empty page
    /// that is not truncated, so a truncated page always has a marker.
    pub(super) fn list_parts(&mut self, request: &ListParts) -> S3Result<ListPartsOutput> {
        let upload = self.upload_mut(&request.key, &request.upload_id)?;
        let max_parts = request.max_parts.min(MAX_LIST_PARTS) as usize;
        if max_parts == 0 {
            return Ok(ListPartsOutput::default());
        }
        let after = request.part_number_marker.unwrap_or(0);
        let mut remaining = upload
            .parts
            .range((Bound::Excluded(after), Bound::Unbounded))
            .map(|(&part_number, part)| ListedPart {
                part_number,
                etag: part.etag.clone(),
                size: part.body.len() as u64,
            });
        let parts: Vec<_> = remaining.by_ref().take(max_parts).collect();
        let is_truncated = remaining.next().is_some();
        Ok(ListPartsOutput {
            next_part_number_marker: parts.last().filter(|_| is_truncated).map(|p| p.part_number),
            is_truncated,
            parts,
        })
    }

    /// Returns every key with a current object, in order.
    pub(super) fn keys(&self) -> Vec<String> {
        self.keys
            .iter()
            .filter(|(_, versions)| versions.last().is_some_and(|v| v.object.is_some()))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Returns every version and delete marker of every key, in key order
    /// and oldest first: the key, the version ID (`None` on an unversioned
    /// bucket), and whether it is a delete marker.
    pub(super) fn versions(&self) -> Vec<(String, Option<VersionId>, bool)> {
        self.keys
            .iter()
            .flat_map(|(key, versions)| {
                versions
                    .iter()
                    .map(move |v| (key.clone(), v.id.clone(), v.object.is_none()))
            })
            .collect()
    }

    /// Returns every open multipart upload and its key, in upload-ID order.
    pub(super) fn uploads(&self) -> Vec<(UploadId, String)> {
        self.uploads
            .iter()
            .map(|(id, upload)| (id.clone(), upload.key.clone()))
            .collect()
    }
}

fn check_key(key: &str) -> S3Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(S3Error::new(
            S3ErrorKind::InvalidArgument,
            format!("object keys must be 1 to {MAX_KEY_LEN} bytes long"),
        ));
    }
    Ok(())
}

fn check_etag(key: &str, expected: &ETag, object: &Stored) -> S3Result<()> {
    if object.etag == *expected {
        Ok(())
    } else {
        Err(S3Error::new(
            S3ErrorKind::PreconditionFailed,
            format!("{key:?} has ETag {}, not {expected}", object.etag),
        ))
    }
}

fn check_read_conditions(
    key: &str,
    object: &Stored,
    if_match: Option<&ETag>,
    if_none_match: Option<&ETag>,
) -> S3Result<()> {
    if let Some(etag) = if_match {
        check_etag(key, etag, object)?;
    }
    if if_none_match == Some(&object.etag) {
        return Err(S3Error::new(
            S3ErrorKind::NotModified,
            format!("{key:?} still has ETag {}", object.etag),
        ));
    }
    Ok(())
}

fn no_such_key(key: &str) -> S3Error {
    S3Error::new(S3ErrorKind::NoSuchKey, format!("{key:?} does not exist"))
}

fn no_such_upload(upload_id: &UploadId) -> S3Error {
    S3Error::new(
        S3ErrorKind::NoSuchUpload,
        format!("upload {upload_id} does not exist"),
    )
}

fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 0xf)]])
        .map(char::from)
        .collect()
}

/// The ETag of a single-part object: the hex MD5 of its body.
pub(super) fn md5_etag(md5: &[u8; 16]) -> ETag {
    ETag::new(hex(md5)).expect("a hex digest is a valid ETag")
}

/// The ETag of a multipart object: the hex MD5 of the concatenated binary
/// MD5s of its parts, then `-` and the number of parts (design §7.4).
pub(super) fn multipart_etag(part_digests: &[u8], parts: usize) -> ETag {
    ETag::new(format!("{}-{parts}", hex(&md5(part_digests)))).expect("a valid ETag")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etags_follow_s3() {
        // MD5("") and MD5("abc"), the RFC 1321 test vectors.
        assert_eq!(
            md5_etag(&md5(b"")).as_str(),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
        assert_eq!(
            md5_etag(&md5(b"abc")).as_str(),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        // Two parts, "a" and "b": MD5(MD5("a") || MD5("b")), then "-2".
        let mut digests = Vec::new();
        digests.extend_from_slice(&md5(b"a"));
        digests.extend_from_slice(&md5(b"b"));
        let etag = multipart_etag(&digests, 2);
        assert_eq!(etag.as_str(), format!("{}-2", hex(&md5(&digests))));
        assert!(etag.as_str().ends_with("-2"));
        assert_eq!(etag.as_str().len(), 34);
    }
}
