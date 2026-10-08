//! What the multipart records do to the index (§7.2, §7.4, §10.3).
//!
//! - `MPU_CREATE` stores an open upload, named by the record's position.
//! - `MPU_PART` stores a part of an open upload, replacing any part with
//!   the same number.
//! - `MPU_COMPLETE` turns an open upload into a new version of its key. The
//!   object keeps the parts the record lists, with their boundaries, so a
//!   flush can reproduce the multipart ETag, and inherits the upload's write
//!   identity: the position of its `MPU_CREATE`.
//! - `MPU_ABORT` removes an open upload and its parts.
//! - `PART_FLUSHED` follows the remote upload a streaming flush keeps for a
//!   local one (§7.3): its ID once opened, the part each remote part holds,
//!   and nothing once it ended. It changes no entry and no local upload.
//!
//! **Releasing bytes.** A part that no upload or object can reach any more
//! (an aborted upload's, a part replaced by a later one with its number, a
//! part a completion leaves out, or a part refused because its upload is
//! gone) has its bytes *released*: their records leave the node-local
//! location map. Nothing reads a position the map does not locate, and
//! compaction drops the records of released positions without checking
//! whether anything still references them (§10.3). The bytes of an object
//! version that a later write replaces are not released this way, since a
//! read of the old version may still be streaming them; compaction finds
//! those by reference.

use skys3_index::{
    Entry, IndexError, IndexWriter, ObjectPart, ObjectVersion, Part, Payload, RemotePart,
    RemoteUpload, Upload,
};
use skys3_log::record::{
    MpuAbort, MpuComplete, MpuCreate, MpuPart, PartFlushed, PutData, RemoteStep,
};
use skys3_log::{RecordLocation, ShardRef};
use skys3_types::EpochSeq;

use crate::machine::{Effect, Rejection, Rule, store, written_state};

/// `MPU_CREATE`: an open upload.
pub(crate) fn create(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    create: &MpuCreate,
) -> Result<Rule, IndexError> {
    let upload = Upload {
        initiated_ms: create.initiated_ms,
        metadata: create.metadata.clone(),
        tags: create.tags.clone(),
        checksum: create.checksum,
    };
    index.put_upload(shard, &create.key, position, &upload)?;
    Ok(Ok(Effect::UploadCreated))
}

/// `MPU_PART`: a part of an open upload. A part of an upload that is gone
/// is refused, and its bytes released.
pub(crate) fn store_part(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    part: &MpuPart,
    location: RecordLocation,
) -> Result<Rule, IndexError> {
    let payload = match &part.data {
        PutData::Inline(_) => Payload::Inline(position),
        PutData::Extents(extents) => Payload::Extents(extents.clone()),
    };
    if index.upload(shard, &part.key, part.upload)?.is_none() {
        release(index, shard, &payload)?;
        return Ok(Err(Rejection::NoSuchUpload));
    }
    if let PutData::Inline(_) = part.data {
        index.put_location(shard, position, &location)?;
    }
    if let Some(replaced) = index.part(shard, part.upload, part.part_number)? {
        release(index, shard, &replaced.payload)?;
    }
    let stored = Part {
        position,
        size: part.size,
        last_modified_ms: part.last_modified_ms,
        etag: part.etag.clone(),
        checksums: part.checksums.clone(),
        payload,
    };
    index.put_part(shard, part.upload, part.part_number, &stored)?;
    Ok(Ok(Effect::PartStored))
}

/// `MPU_COMPLETE`: the upload becomes the key's new version, made of the
/// parts the record lists. Each must still be the part stored at the
/// position the record names; a part replaced since, or missing, refuses
/// the completion and changes nothing. Parts the record leaves out are
/// released.
pub(crate) fn complete(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    complete: &MpuComplete,
) -> Result<Rule, IndexError> {
    let Some(upload) = index.upload(shard, &complete.key, complete.upload)? else {
        return Ok(Err(Rejection::NoSuchUpload));
    };
    let mut stored = index.parts(shard, complete.upload)?.into_iter().peekable();
    let mut kept = Vec::with_capacity(complete.parts.len());
    let mut unlisted = Vec::new();
    for listed in &complete.parts {
        while let Some((number, part)) = stored.next_if(|(number, _)| *number < listed.number) {
            unlisted.push((number, part));
        }
        match stored.next() {
            Some((number, part)) if number == listed.number && part.position == listed.position => {
                kept.push(ObjectPart {
                    number,
                    size: part.size,
                });
            }
            _ => {
                return Ok(Err(Rejection::PartChanged {
                    number: listed.number,
                }));
            }
        }
    }
    unlisted.extend(stored);

    index.remove_upload(shard, &complete.key, complete.upload)?;
    for (number, part) in unlisted {
        release(index, shard, &part.payload)?;
        index.remove_part(shard, complete.upload, number)?;
    }
    let prior = index.entry(shard, &complete.key)?;
    let state = written_state(prior.as_ref());
    let object = ObjectVersion {
        size: complete.size,
        last_modified_ms: complete.last_modified_ms,
        local_etag: complete.etag.clone(),
        write_identity: Some(complete.upload),
        metadata: upload.metadata,
        tags: upload.tags,
        checksums: complete.checksums.clone(),
        storage_class: None,
        copy_source: None,
        payload: Payload::Parts {
            upload: complete.upload,
            parts: kept,
        },
        coded: None,
    };
    store(
        index,
        shard,
        &complete.key,
        position,
        state,
        Some(object),
        prior,
    )?;
    Ok(Ok(Effect::Stored { state }))
}

/// `MPU_ABORT`: the upload and its parts are removed, and the parts'
/// bytes released.
pub(crate) fn abort(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    abort: &MpuAbort,
) -> Result<Rule, IndexError> {
    if !index.remove_upload(shard, &abort.key, abort.upload)? {
        return Ok(Err(Rejection::NoSuchUpload));
    }
    let parts = index.parts(shard, abort.upload)?;
    let released = parts.len();
    for (number, part) in parts {
        release(index, shard, &part.payload)?;
        index.remove_part(shard, abort.upload, number)?;
    }
    Ok(Ok(Effect::Aborted { parts: released }))
}

/// `PART_FLUSHED`: a step of the remote upload of a local upload (§7.3).
///
/// - Opened: the remote upload is recorded, replacing any other, whatever
///   became of the local upload meanwhile: the remote one must still be
///   completed or aborted.
/// - A part: recorded as what the remote holds under its number, if the
///   remote upload is the one recorded.
/// - Ended: the remote upload and its parts are forgotten, if it is the one
///   recorded.
pub(crate) fn part_flushed(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    flushed: &PartFlushed,
) -> Result<Rule, IndexError> {
    if flushed.step == RemoteStep::Opened {
        let remote = RemoteUpload {
            key: flushed.key.clone(),
            id: flushed.remote_upload_id.clone(),
        };
        index.put_remote_upload(shard, flushed.upload, &remote)?;
        return Ok(Ok(Effect::RemoteUploadMoved));
    }
    let recorded = index.remote_upload(shard, flushed.upload)?;
    if recorded.is_none_or(|remote| remote.id != flushed.remote_upload_id) {
        return Ok(Err(Rejection::NoRemoteUpload));
    }
    match &flushed.step {
        RemoteStep::Part {
            number,
            position,
            remote_etag,
        } => {
            let part = RemotePart {
                position: *position,
                etag: remote_etag.clone(),
            };
            index.put_remote_part(shard, flushed.upload, *number, &part)?;
        }
        RemoteStep::Ended | RemoteStep::Opened => {
            index.remove_remote_upload(shard, flushed.upload)?;
        }
    }
    Ok(Ok(Effect::RemoteUploadMoved))
}

/// Removes the parts of the multipart object `prior` held, if it was one,
/// once a write replaces it. Their bytes are left to compaction (see the
/// module documentation).
pub(crate) fn drop_parts(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    prior: Option<&Entry>,
) -> Result<(), IndexError> {
    let Some(Payload::Parts { upload, parts }) = prior
        .and_then(|entry| entry.object.as_ref())
        .map(|object| &object.payload)
    else {
        return Ok(());
    };
    for part in parts {
        index.remove_part(shard, *upload, part.number)?;
    }
    Ok(())
}

/// Releases the bytes of `payload`: its records leave the location map.
fn release(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    payload: &Payload,
) -> Result<(), IndexError> {
    match payload {
        Payload::Inline(position) => {
            index.remove_location(shard, *position)?;
        }
        Payload::Extents(extents) => {
            for extent in extents {
                index.remove_location(shard, extent.position)?;
            }
        }
        Payload::None | Payload::Parts { .. } => {}
    }
    Ok(())
}
