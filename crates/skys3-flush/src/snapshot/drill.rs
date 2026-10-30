//! The restore drill of a shard whose members are all lost (§6.9, §8.9):
//! its latest snapshot and the fragment headers every surviving node holds
//! re-index its coded objects, and the lost-key report names what nothing
//! restores.

use std::collections::{BTreeMap, BTreeSet};

use skys3_ec::reindex::{HeaderError, HeaderSource, Placement, SnapshotState, reindex};
use skys3_index::{Entry, EntryState, ShardRow, ShardTable, codec};
use skys3_log::ShardRef;
use skys3_remote::ObjectStore;
use skys3_types::{ETag, Epoch, EpochSeq, NodeId};

use super::SnapshotError;
use super::report::{DurableHome, LostKey, LostKeyReport, LostObject, at_home, holds, lost_keys};
use super::restore::latest;

/// What a restore drill needs to know of the lost shard and its cluster.
#[derive(Debug)]
pub struct DrillRequest<'a, S> {
    /// The shard whose members are all lost.
    pub shard: &'a ShardRef,
    /// The store of the bucket's snapshot target.
    pub snapshots: &'a S,
    /// The snapshot target's key prefix.
    pub prefix: &'a str,
    /// The bucket's durable home, if it has one: a `local` bucket's backup
    /// target.
    pub home: Option<DurableHome<'a, S>>,
    /// Every node of the cluster, departing ones included: fragments may
    /// be on any of them (§8.3).
    pub nodes: &'a [NodeId],
    /// The nodes lost with the shard: its members, and any other node lost
    /// with them. Their fragments are gone, and they are not asked.
    pub lost: &'a BTreeSet<NodeId>,
    /// When the members were lost, in milliseconds since the Unix epoch.
    pub until_ms: u64,
}

impl<S> Clone for DrillRequest<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for DrillRequest<'_, S> {}

/// What a restore drill found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drill {
    /// The lost-key report: the keys nothing restores, coded versions that
    /// cannot be re-indexed among them, and in [`LostKeyReport::coded`]
    /// the keys restored.
    pub report: LostKeyReport,
    /// The coded objects restored, in key order.
    pub restored: Vec<RestoredKey>,
    /// The nodes whose fragment headers could not be listed. Their
    /// fragments count as missing; a drill run again once they answer may
    /// restore more.
    pub unreachable: Vec<HeaderError>,
    /// The shard's index as re-indexing restored it.
    pub index: RestoredIndex,
}

/// A coded object a restore drill restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredKey {
    /// The object key.
    pub key: String,
    /// The entry's version: the position of its committing record or of
    /// its latest retag known.
    pub version: EpochSeq,
    /// The ETag clients saw.
    pub etag: ETag,
    /// Whether the version was written or coded after the snapshot, and
    /// so restored from fragment headers that the snapshot does not name.
    pub after_snapshot: bool,
    /// The fragments no header was found for, which repair rebuilds once
    /// the shard serves again.
    pub missing: usize,
}

/// A lost shard's index as a restore drill rebuilt it: its coded objects.
/// Replicated objects and uploads in progress are not in it: their bytes
/// were on the lost members, so the report lists them, and a backup holds
/// what it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredIndex {
    /// The shard.
    pub shard: ShardRef,
    /// The position to install the index at: after every record, version,
    /// and write identity the snapshot and the headers name.
    pub applied: EpochSeq,
    /// The earliest epoch the restored shard may start in: after every
    /// epoch the snapshot and the headers name, so that no attempt or
    /// record of the restored shard repeats an earlier one's.
    pub epoch: Epoch,
    /// The entries, by object key.
    pub entries: BTreeMap<String, Entry>,
}

impl RestoredIndex {
    /// The index's rows, as a learner installs a snapshot's
    /// (`skys3_index::Index::install_rows`): each entry, and each fragment
    /// in the shard's index from node to fragments, which repair reads.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Row`] if an entry does not encode.
    pub fn rows(&self) -> Result<Vec<(ShardTable, Vec<ShardRow>)>, SnapshotError> {
        let mut namespace = Vec::with_capacity(self.entries.len());
        let mut fragments = Vec::new();
        for (key, entry) in &self.entries {
            let value =
                codec::encode_entry(entry).map_err(|e| SnapshotError::Row(e.to_string()))?;
            namespace.push((codec::entry_key(&self.shard, key), value));
            let coded = entry.object.as_ref().and_then(|o| o.coded.as_ref());
            for stripe in coded.iter().flat_map(|coded| &coded.stripes) {
                for (index, location) in stripe.fragments().iter().enumerate() {
                    // A stripe holds at most 255 fragments (§8.4).
                    let index = u8::try_from(index).unwrap_or(u8::MAX);
                    let row = codec::fragment_key(
                        &self.shard,
                        &location.node,
                        key,
                        stripe.number(),
                        index,
                    );
                    fragments.push((row, codec::encode_fragment(location.fragment)));
                }
            }
        }
        fragments.sort();
        Ok(vec![
            (ShardTable::Namespace, namespace),
            (ShardTable::Fragments, fragments),
        ])
    }
}

/// Runs the restore drill of the lost shard `request` names, listing
/// fragment headers through `headers`.
///
/// It restores the shard's latest snapshot ([`latest`]), asks every node
/// that is not lost for the headers of the shard's fragments, and
/// re-indexes the shard's coded objects from both (`skys3_ec::reindex`):
/// a version written or coded after the snapshot from its headers alone.
/// With a durable home, an object restored from headers is dropped again
/// if the home holds no object at its key, or a later write of the shard:
/// a coded version reached the backup before it was coded (§8.4), so it
/// was deleted or overwritten after the snapshot, and the home holds the
/// newer state. The home's object of the very version makes the entry
/// clean.
///
/// The report is the lost-key report of the snapshot ([`lost_keys`]),
/// except that the keys restored are not lost but listed in
/// [`LostKeyReport::coded`], and that a coded version that cannot be
/// re-indexed is lost unless the home holds it.
///
/// # Errors
///
/// [`SnapshotError::Remote`] if the snapshot target or the home fails,
/// and [`SnapshotError::Row`] if a snapshot row does not decode.
pub async fn drill<S: ObjectStore, H: HeaderSource>(
    request: DrillRequest<'_, S>,
    headers: &H,
) -> Result<Drill, SnapshotError> {
    let shard = request.shard;
    let restored = latest(request.snapshots, request.prefix, shard).await?;
    let snapshot = match &restored {
        Some(restored) => Some(SnapshotState {
            position: restored.position,
            entries: restored.entries()?,
        }),
        None => None,
    };
    let mut found = Vec::new();
    let mut unreachable = Vec::new();
    for node in request.nodes.iter().filter(|n| !request.lost.contains(*n)) {
        match headers.headers(node, shard).await {
            Ok(fragments) => found.extend(fragments),
            Err(error) => {
                tracing::warn!(%error, "a node's fragments count as missing");
                unreachable.push(error);
            }
        }
    }
    let placement = Placement {
        nodes: request.nodes,
        lost: request.lost,
    };
    let reindexed = reindex(shard, snapshot.as_ref(), found, placement);
    let applied = reindexed.applied();
    let epoch = reindexed.epoch.checked_next().unwrap_or(Epoch::MAX);
    let (objects, unrecoverable) = (reindexed.restored, reindexed.unrecoverable);

    let mut entries = BTreeMap::new();
    let mut restored_keys = Vec::new();
    for (key, mut object) in objects {
        if let Some(home) = &request.home {
            let Some(found) = at_home(home, shard, &key).await? else {
                continue;
            };
            if found.written.is_some_and(|at| at > object.entry.version) {
                continue;
            }
            // The write identity names the version; the home's ETag may
            // differ from the local one, as a multipart flush's does.
            let same = object.entry.object.as_ref().is_some_and(|o| {
                found.written == Some(o.write_identity.unwrap_or(object.entry.version))
            });
            if same && object.entry.state == EntryState::Dirty {
                object.entry.state = EntryState::Clean;
                object.entry.remote_etag = Some(found.etag);
            }
        }
        let etag = match &object.entry.object {
            Some(o) => o.local_etag.clone(),
            None => continue,
        };
        restored_keys.push(RestoredKey {
            key: key.clone(),
            version: object.entry.version,
            etag,
            after_snapshot: object.after_snapshot,
            missing: object.missing,
        });
        entries.insert(key, object.entry);
    }

    let mut report = lost_keys(shard, restored.as_ref(), request.home, request.until_ms).await?;
    report
        .lost
        .retain(|lost| !entries.contains_key(&lost.key) && !unrecoverable.contains_key(&lost.key));
    for (key, lost) in unrecoverable {
        if let Some(home) = &request.home
            && holds(home, shard, &key, lost.version, Some(lost.identity)).await?
        {
            continue;
        }
        let state = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.entries.get(&key))
            .map_or(EntryState::Dirty, |entry| entry.state);
        report.lost.push(LostKey {
            key,
            version: lost.version,
            state,
            object: Some(LostObject {
                size: lost.size,
                etag: lost.etag,
                last_modified_ms: lost.last_modified_ms,
            }),
            coded: Some(lost.error),
        });
    }
    report.lost.sort_by(|a, b| a.key.cmp(&b.key));
    report.coded = entries.keys().cloned().collect();
    Ok(Drill {
        report,
        restored: restored_keys,
        unreachable,
        index: RestoredIndex {
            shard: shard.clone(),
            applied,
            epoch,
            entries,
        },
    })
}
