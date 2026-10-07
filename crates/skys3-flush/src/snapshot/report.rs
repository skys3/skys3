//! The lost-key report of a shard whose members are all lost (§6.9).

use skys3_index::{Entry, EntryState};
use skys3_log::ShardRef;
use skys3_remote::{HeadObject, ObjectInfo, ObjectStore, S3ErrorKind};
use skys3_types::{ClusterId, ETag, EpochSeq, WriteIdentity};

use super::SnapshotError;
use super::format::ChainId;
use super::restore::Restored;

/// Where a bucket's data is durable besides its members: a `write_back`
/// bucket's remote target or a `local` bucket's backup target (§8.1,
/// §8.9).
#[derive(Debug, Clone, Copy)]
pub struct DurableHome<'a, S> {
    /// The target's store.
    pub store: &'a S,
    /// The target's key prefix.
    pub prefix: &'a str,
    /// The cluster whose write identities the target's objects carry.
    pub cluster: &'a ClusterId,
}

/// What a shard's members held that nothing else holds, as far as its
/// latest snapshot tells (§6.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostKeyReport {
    /// The shard.
    pub shard: ShardRef,
    /// The snapshot the report was built from, or `None` if the target
    /// held none.
    pub snapshot: Option<SnapshotInfo>,
    /// When other keys may have been lost: any key written in it.
    pub window: LossWindow,
    /// The keys whose latest version, as of the snapshot, existed only on
    /// the lost members, in key order.
    pub lost: Vec<LostKey>,
    /// The multipart uploads in progress as of the snapshot, which are
    /// lost with the members.
    pub uploads: Vec<LostUpload>,
    /// Keys whose version is erasure-coded: their fragments outlive the
    /// members, and re-indexing them from their fragment headers recovers
    /// them (plan M5-11).
    pub coded: Vec<String>,
}

/// The snapshot a report was built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Its chain.
    pub chain: ChainId,
    /// The last snapshot of the chain applied.
    pub number: u32,
    /// The applied position it was taken at.
    pub position: EpochSeq,
    /// When it was taken, in milliseconds since the Unix epoch.
    pub taken_ms: u64,
}

/// The time window, on the primaries' clocks, in which other keys than
/// the ones reported may have been lost: every key written in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossWindow {
    /// When the snapshot was taken, in milliseconds since the Unix epoch;
    /// `None` without a snapshot: since the shard was created.
    pub from_ms: Option<u64>,
    /// When the members were lost, as the operator gives it.
    pub until_ms: u64,
}

/// A key whose latest version existed only on the lost members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostKey {
    /// The object key.
    pub key: String,
    /// The position of the version.
    pub version: EpochSeq,
    /// The entry's state in the snapshot.
    pub state: EntryState,
    /// The object, or `None` for a delete that did not reach the durable
    /// home: the home may still hold an older object at the key.
    pub object: Option<LostObject>,
}

/// What a lost object was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostObject {
    /// Its size in bytes.
    pub size: u64,
    /// The ETag clients saw.
    pub etag: ETag,
    /// `Last-Modified`, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
}

/// A multipart upload in progress that is lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostUpload {
    /// The object key.
    pub key: String,
    /// The position of its `MPU_CREATE`.
    pub upload: EpochSeq,
    /// When it was started, in milliseconds since the Unix epoch.
    pub initiated_ms: u64,
    /// The parts it held.
    pub parts: usize,
}

/// Builds the lost-key report of `shard`, whose every member was lost at
/// `until_ms`, from its latest snapshot `restored` ([`super::latest`]).
///
/// Without a durable `home` (a `local` bucket without a backup target),
/// every object the snapshot holds is lost, except coded ones. With one,
/// only an entry that is not clean is a candidate, and it is lost unless
/// the home holds its version, or a later write of the shard: an object
/// whose write identity names this cluster, bucket, and shard, at or after
/// the version's identity; for a delete, no object, or one written after
/// it. The window starts when the snapshot was taken, and any key written
/// after that may be lost too.
///
/// # Errors
///
/// [`SnapshotError::Row`] if a row does not decode, and
/// [`SnapshotError::Remote`] if a `HeadObject` of the home fails.
pub async fn lost_keys<S: ObjectStore>(
    shard: &ShardRef,
    restored: Option<&Restored>,
    home: Option<DurableHome<'_, S>>,
    until_ms: u64,
) -> Result<LostKeyReport, SnapshotError> {
    let mut report = LostKeyReport {
        shard: shard.clone(),
        snapshot: restored.map(|restored| SnapshotInfo {
            chain: restored.chain,
            number: restored.number,
            position: restored.position,
            taken_ms: restored.taken_ms,
        }),
        window: LossWindow {
            from_ms: restored.map(|restored| restored.taken_ms),
            until_ms,
        },
        lost: Vec::new(),
        uploads: Vec::new(),
        coded: Vec::new(),
    };
    let Some(restored) = restored else {
        return Ok(report);
    };
    for (key, entry) in restored.entries()? {
        if matches!(entry.state, EntryState::Clean | EntryState::Evicted) && home.is_some() {
            continue;
        }
        let Some(object) = &entry.object else {
            // A delete of a bucket without a home lost nothing.
            if let Some(home) = &home
                && !holds(home, shard, &key, &entry).await?
            {
                report.lost.push(lost(key, &entry));
            }
            continue;
        };
        if object.coded.is_some() {
            report.coded.push(key);
            continue;
        }
        let held = match &home {
            Some(home) => holds(home, shard, &key, &entry).await?,
            None => false,
        };
        if !held {
            report.lost.push(lost(key, &entry));
        }
    }
    report.uploads = restored
        .uploads()?
        .into_iter()
        .map(|(key, upload, open, parts)| LostUpload {
            key,
            upload,
            initiated_ms: open.initiated_ms,
            parts,
        })
        .collect();
    Ok(report)
}

fn lost(key: String, entry: &Entry) -> LostKey {
    LostKey {
        key,
        version: entry.version,
        state: entry.state,
        object: entry.object.as_ref().map(|object| LostObject {
            size: object.size,
            etag: object.local_etag.clone(),
            last_modified_ms: object.last_modified_ms,
        }),
    }
}

/// Whether `home` holds `entry`'s version of `key`, or a later write of
/// the shard.
async fn holds<S: ObjectStore>(
    home: &DurableHome<'_, S>,
    shard: &ShardRef,
    key: &str,
    entry: &Entry,
) -> Result<bool, SnapshotError> {
    let request = HeadObject::new(format!("{}{key}", home.prefix));
    let found = match home.store.head_object(request).await {
        Ok(info) => Some(info),
        Err(error) if error.kind() == S3ErrorKind::NoSuchKey => None,
        Err(error) => return Err(error.into()),
    };
    let written = |info: &ObjectInfo| {
        let wid = info
            .metadata
            .write_identity()?
            .parse::<WriteIdentity>()
            .ok()?;
        (wid.cluster == *home.cluster && wid.bucket == shard.bucket && wid.shard == shard.shard)
            .then_some(wid.position)
    };
    Ok(match (&entry.object, found) {
        (None, None) => true,
        (None, Some(info)) => written(&info).is_some_and(|at| at > entry.version),
        (Some(_), None) => false,
        (Some(object), Some(info)) => {
            written(&info).is_some_and(|at| at >= object.write_identity.unwrap_or(entry.version))
        }
    })
}
