//! Re-indexing a lost shard's coded objects from their fragment headers
//! (design §6.9, §8.4, §8.9).
//!
//! Losing every member of a shard loses its index, but not its coded
//! objects: their fragments live on any eligible node, and each fragment's
//! header names its object version, stripe, and attempt. [`reindex`]
//! combines the shard's latest index snapshot, if it has one, with the
//! headers the surviving nodes hold ([`HeaderSource`]), key by key:
//!
//! - **A version written after the snapshot.** Headers whose version
//!   follows the snapshot's applied position name a version committed after
//!   it, which supersedes whatever the snapshot holds of the key. Only the
//!   newest such version is restored, from its headers alone. If its
//!   stripes cannot be rebuilt, the key is [unrecoverable](Unrecoverable):
//!   an older version is never restored in its place, since the newer one
//!   replaced it.
//! - **The snapshot's version.** Otherwise the snapshot decides. An entry
//!   whose version's fragments are found is restored with its layout from
//!   the headers: the version its coded layout names
//!   ([`Coded::version`]), or the entry's own version if it was coded after
//!   the snapshot. A coded entry whose fragments cannot be rebuilt is
//!   unrecoverable.
//! - **Everything else is superseded.** Header versions at or before the
//!   snapshot's position that its entry does not name were overwritten or
//!   deleted by then: their fragments are orphans, and restoring them
//!   would undo the write that replaced them.
//! - **Tags and write identity.** A `TAGS` record changes them without a
//!   new version, and each attempt's headers hold them as the attempt read
//!   the entry (§8.4), with the retag's position as the write identity. The
//!   restored entry takes them, with its version, from whichever of the
//!   snapshot's entry and the version's latest attempt names the later
//!   position; what only an entry holds (payload positions, a copy's
//!   source, the storage class, the remote ETag) comes from the snapshot.
//! - **Locations.** Each fragment's location comes from the latest attempt
//!   that holds it ([`rebuild_layouts`]). A fragment no header was found
//!   for keeps the location the snapshot's layout gives it, if any, or is
//!   placed on a lost node the stripe does not use, else on any node it
//!   does not use, as [`UNKNOWN_FRAGMENT`]: reads find it missing and
//!   decode around it, and repair rebuilds it (§8.6).
//!
//! The restored shard must start in an epoch after [`Reindexed::epoch`],
//! so that its attempts and records follow every one the headers and the
//! snapshot name.

mod headers;

use std::collections::{BTreeMap, BTreeSet};

use skys3_index::{Coded, Entry, EntryState, ObjectPart, ObjectVersion, Payload};
use skys3_log::record::ShardRef;
use skys3_types::{
    AttemptId, CodecId, CodedStripe, ETag, Epoch, EpochSeq, FragmentId, FragmentLocation, Geometry,
    NodeId, Seq,
};

use crate::fragment::ObjectMeta;
use crate::layout::{FoundFragment, ObjectLayout, RebuildError, StripeLayout, rebuild_layouts};

pub use headers::{HeaderError, HeaderSource};

/// The fragment ID a restored layout gives a fragment no header was found
/// for. No store assigns it, since no record starts at the greatest
/// offset, so a read or a repair's check of it finds it missing.
pub const UNKNOWN_FRAGMENT: FragmentId = FragmentId::new(u128::MAX);

/// The part of a shard's latest index snapshot that re-indexing reads: its
/// applied position and its entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotState {
    /// The applied position the snapshot was taken at: every record at or
    /// before it that committed is in the entries.
    pub position: EpochSeq,
    /// The namespace entries, by object key.
    pub entries: BTreeMap<String, Entry>,
}

/// The nodes re-indexing may place a fragment it found no header for on
/// (see the module documentation).
#[derive(Debug, Clone, Copy)]
pub struct Placement<'a> {
    /// Every node of the cluster.
    pub nodes: &'a [NodeId],
    /// The nodes lost with the shard, which the fragments no header was
    /// found for were most likely on.
    pub lost: &'a BTreeSet<NodeId>,
}

/// What re-indexing a shard restored and what it could not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reindexed {
    /// The coded objects restored, by key: each entry as the shard's index
    /// would hold it.
    pub restored: BTreeMap<String, RestoredObject>,
    /// The keys whose current version, as far as the snapshot and the
    /// headers tell, is coded but cannot be restored.
    pub unrecoverable: BTreeMap<String, Unrecoverable>,
    /// The object versions headers name that were superseded, by key.
    pub superseded: BTreeMap<String, Vec<EpochSeq>>,
    /// The greatest position the snapshot and the headers name, of a
    /// record, an object version, or a write identity.
    pub position: EpochSeq,
    /// The greatest epoch they name, positions and attempts alike. The
    /// restored shard must start in a later one.
    pub epoch: Epoch,
}

impl Reindexed {
    /// The position after [`Reindexed::position`]: the restored index's
    /// applied position, and the publish position of the layouts restored
    /// from headers alone, whose `EC_PUBLISH` is lost.
    #[must_use]
    pub fn applied(&self) -> EpochSeq {
        next(self.position)
    }
}

/// A coded object restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredObject {
    /// Its entry, with the coded layout rebuilt from the headers.
    pub entry: Entry,
    /// Whether the version was written, or coded, after the snapshot:
    /// restored from headers that the snapshot's entry does not name.
    pub after_snapshot: bool,
    /// The fragments no header was found for, which repair must rebuild.
    pub missing: usize,
}

/// A key whose current version is coded but cannot be restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unrecoverable {
    /// The position of the record that committed the version.
    pub version: EpochSeq,
    /// The position its write identity names (§7.2), as the latest
    /// attempt or the snapshot knew it.
    pub identity: EpochSeq,
    /// The object's size.
    pub size: u64,
    /// The ETag clients saw.
    pub etag: ETag,
    /// `Last-Modified`, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
    /// Why it cannot be restored.
    pub error: ReindexError,
}

/// Why a coded version cannot be restored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReindexError {
    /// Its stripes cannot be rebuilt from the headers found.
    #[error(transparent)]
    Rebuild(#[from] RebuildError),
    /// The snapshot holds it coded, and no header of it was found.
    #[error("no fragment of the version was found")]
    NoFragments,
    /// A stripe has no node left for a fragment no header was found for.
    #[error("stripe {stripe} cannot be laid out: {reason}")]
    Layout {
        /// The stripe's number.
        stripe: u32,
        /// Why.
        reason: String,
    },
}

/// The headers of one object version: the latest attempt that wrote one,
/// and the object's metadata as that attempt's headers hold it.
struct Written {
    attempt: AttemptId,
    object: ObjectMeta,
}

/// Re-indexes `shard`'s coded objects from its latest snapshot, if any, and
/// the fragment headers `found` on the surviving nodes, placing fragments
/// no header was found for by `placement`. Headers of other shards are
/// ignored. The module documentation gives the rules.
#[must_use]
pub fn reindex(
    shard: &ShardRef,
    snapshot: Option<&SnapshotState>,
    found: impl IntoIterator<Item = FoundFragment>,
    placement: Placement<'_>,
) -> Reindexed {
    let mut found: Vec<FoundFragment> = found
        .into_iter()
        .filter(|fragment| fragment.header.shard == *shard)
        .collect();
    let mut position = snapshot
        .map(|snapshot| snapshot.position)
        .unwrap_or_default();
    let mut epoch = position.epoch;
    let mut written = BTreeMap::<(String, EpochSeq), Written>::new();
    for fragment in &found {
        let header = &fragment.header;
        position = position.max(header.version).max(header.object.identity);
        epoch = epoch.max(header.attempt.epoch);
        let version = (header.key.clone(), header.version);
        match written.get_mut(&version) {
            Some(known) if known.attempt >= header.attempt => {}
            Some(known) => {
                known.attempt = header.attempt;
                known.object = header.object.clone();
            }
            None => {
                written.insert(
                    version,
                    Written {
                        attempt: header.attempt,
                        object: header.object.clone(),
                    },
                );
            }
        }
    }
    for entry in snapshot
        .iter()
        .flat_map(|snapshot| snapshot.entries.values())
    {
        position = position.max(entry.version);
        if let Some(coded) = entry.object.as_ref().and_then(|o| o.coded.as_ref()) {
            position = position.max(coded.publish);
            epoch = epoch.max(coded.attempt.epoch);
        }
    }
    epoch = epoch.max(position.epoch);
    if seeded::older_attempts() {
        for fragment in &mut found {
            let attempt = fragment.header.attempt;
            fragment.header.attempt = AttemptId::new(
                Epoch::new(u64::MAX - attempt.epoch.get()),
                u64::MAX - attempt.number,
            );
        }
    }

    // Every copy of each version's fragments, latest attempt first: a
    // layout may take an older copy where the latest shares a node.
    let mut copies = BTreeMap::<(String, EpochSeq), Vec<Copy>>::new();
    for fragment in &found {
        let header = &fragment.header;
        let stripe = &header.stripe;
        copies
            .entry((header.key.clone(), header.version))
            .or_default()
            .push(Copy {
                shape: (
                    stripe.number,
                    stripe.offset,
                    stripe.data_len,
                    stripe.geometry,
                    stripe.codec,
                ),
                index: header.index,
                attempt: header.attempt,
                location: fragment.location.clone(),
            });
    }
    for of_version in copies.values_mut() {
        of_version.sort_by(|a, b| (b.attempt, &a.location).cmp(&(a.attempt, &b.location)));
    }

    let mut versions =
        BTreeMap::<String, BTreeMap<EpochSeq, Result<ObjectLayout, RebuildError>>>::new();
    for (version, layout) in rebuild_layouts(found) {
        versions
            .entry(version.key)
            .or_default()
            .insert(version.version, layout);
    }
    let mut reindexed = Reindexed {
        restored: BTreeMap::new(),
        unrecoverable: BTreeMap::new(),
        superseded: BTreeMap::new(),
        position,
        epoch,
    };
    let none = BTreeMap::new();
    let mut keys: BTreeSet<&str> = versions.keys().map(String::as_str).collect();
    if let Some(snapshot) = snapshot {
        keys.extend(snapshot.entries.keys().map(String::as_str));
    }
    for key in keys {
        let entry = snapshot.and_then(|snapshot| snapshot.entries.get(key));
        let of_key = versions.get(key).unwrap_or(&none);
        let decision = decide(snapshot.map(|s| s.position), entry, of_key);
        let chosen = match &decision {
            Decision::Restore { version, .. } | Decision::Unrecoverable { version, .. } => {
                Some(*version)
            }
            Decision::Nothing => None,
        };
        let superseded: Vec<EpochSeq> = of_key
            .keys()
            .copied()
            .filter(|version| Some(*version) != chosen)
            .collect();
        if !superseded.is_empty() {
            reindexed.superseded.insert(key.to_owned(), superseded);
        }
        let outcome = match decision {
            Decision::Restore {
                version,
                layout,
                snapshot: same,
                after_snapshot,
            } => {
                let of_version = (key.to_owned(), version);
                restore(
                    version,
                    written[&of_version].attempt,
                    layout,
                    &copies[&of_version],
                    same,
                    placement,
                    reindexed.applied(),
                )
                .map(|(entry, missing)| RestoredObject {
                    entry,
                    after_snapshot,
                    missing,
                })
                .map_err(|error| unrecoverable(version, Some(&layout.object), entry, error))
            }
            Decision::Unrecoverable { version, error } => {
                let object = written.get(&(key.to_owned(), version)).map(|w| &w.object);
                Err(unrecoverable(version, object, entry, error))
            }
            Decision::Nothing => continue,
        };
        match outcome {
            Ok(restored) => {
                reindexed.restored.insert(key.to_owned(), restored);
            }
            Err(Some(lost)) => {
                reindexed.unrecoverable.insert(key.to_owned(), lost);
            }
            Err(None) => {}
        }
    }
    reindexed
}

/// What becomes of one key.
enum Decision<'a> {
    /// Restore `version` with `layout`; `snapshot` is the snapshot's entry
    /// if it holds that version.
    Restore {
        version: EpochSeq,
        layout: &'a ObjectLayout,
        snapshot: Option<&'a Entry>,
        after_snapshot: bool,
    },
    /// The key's current version is coded and cannot be restored.
    Unrecoverable {
        version: EpochSeq,
        error: ReindexError,
    },
    /// Re-indexing has nothing of the key: it is deleted, or replicated,
    /// as far as anything tells.
    Nothing,
}

/// Decides what becomes of a key that the snapshot taken at `position`
/// holds as `entry`, and whose headers name `versions`.
fn decide<'a>(
    position: Option<EpochSeq>,
    entry: Option<&'a Entry>,
    versions: &'a BTreeMap<EpochSeq, Result<ObjectLayout, RebuildError>>,
) -> Decision<'a> {
    let object = entry.and_then(|entry| entry.object.as_ref());
    let newest = versions
        .iter()
        .next_back()
        .filter(|(version, _)| position.is_none_or(|position| **version > position))
        .filter(|_| !(seeded::prefers_snapshot() && object.is_some()));
    if let Some((&version, rebuilt)) = newest {
        return match rebuilt {
            Ok(layout) => Decision::Restore {
                version,
                layout,
                snapshot: None,
                after_snapshot: true,
            },
            Err(_) if seeded::falls_back() => {
                match versions.iter().rev().find(|(_, r)| r.is_ok()) {
                    Some((&version, Ok(layout))) => Decision::Restore {
                        version,
                        layout,
                        snapshot: None,
                        after_snapshot: true,
                    },
                    _ => Decision::Nothing,
                }
            }
            Err(error) => Decision::Unrecoverable {
                version,
                error: error.clone().into(),
            },
        };
    }
    let (Some(entry), Some(object)) = (entry, object) else {
        if seeded::resurrects()
            && let Some((&version, Ok(layout))) = versions.iter().next_back()
        {
            return Decision::Restore {
                version,
                layout,
                snapshot: None,
                after_snapshot: true,
            };
        }
        return Decision::Nothing;
    };
    let fragments_version = object.coded.as_ref().map_or(entry.version, |c| c.version);
    match versions.get(&fragments_version) {
        Some(Ok(layout)) if layout.object.etag == object.local_etag => Decision::Restore {
            version: fragments_version,
            layout,
            snapshot: Some(entry),
            after_snapshot: object.coded.is_none(),
        },
        // Coded, or coded after the snapshot as its headers show: either
        // way its replicas are lost, and its stripes cannot be rebuilt.
        Some(Err(error)) => Decision::Unrecoverable {
            version: fragments_version,
            error: error.clone().into(),
        },
        _ if object.coded.is_some() => Decision::Unrecoverable {
            version: fragments_version,
            error: ReindexError::NoFragments,
        },
        // A replicated version: it is lost with the replicas, as the
        // lost-key report says.
        _ => Decision::Nothing,
    }
}

/// The values that place a stripe in an object and fix its fragments.
type Shape = (u32, u64, u64, Geometry, CodecId);

/// A copy of a fragment found: its stripe's shape, its index, the attempt
/// that wrote it, and where it is.
struct Copy {
    shape: Shape,
    index: u8,
    attempt: AttemptId,
    location: FragmentLocation,
}

/// The entry of a key's `version`, coded as `layout` by attempts up to
/// `attempt`, whose fragments' copies are `copies`, merged with
/// `snapshot`, the snapshot's entry of that version; and the number of
/// fragments no copy is located for. A layout restored from headers alone
/// is published at `applied`.
fn restore(
    version: EpochSeq,
    attempt: AttemptId,
    layout: &ObjectLayout,
    copies: &[Copy],
    snapshot: Option<&Entry>,
    placement: Placement<'_>,
    applied: EpochSeq,
) -> Result<(Entry, usize), ReindexError> {
    let object = snapshot.and_then(|entry| entry.object.as_ref());
    let known = object
        .and_then(|o| o.coded.as_ref())
        .filter(|c| c.version == version);
    let mut stripes = Vec::with_capacity(layout.stripes.len());
    let mut missing = 0;
    for stripe in &layout.stripes {
        let known = known.and_then(|coded| coded.stripes.get(stripe.number as usize));
        let (stripe, unlocated) = fill(stripe, copies, known, placement)?;
        missing += unlocated;
        stripes.push(stripe);
    }
    let coded = Coded {
        publish: known.map_or(applied, |coded| coded.publish),
        version,
        attempt: known.map_or(attempt, |coded| coded.attempt),
        stripes,
    };
    // The latest attempt read the entry at the version, or at a retag of
    // it, which its write identity then names.
    let meta = &layout.object;
    let read_at = version.max(meta.identity);
    let entry = match (snapshot, object) {
        (Some(entry), Some(object)) if entry.version >= read_at && !seeded::header_tags() => {
            Entry {
                object: Some(ObjectVersion {
                    coded: Some(coded),
                    ..object.clone()
                }),
                ..entry.clone()
            }
        }
        _ => {
            let payload = match object {
                Some(object) => object.payload.clone(),
                None if meta.parts.is_empty() => Payload::None,
                None => Payload::Parts {
                    upload: meta.identity.min(version),
                    parts: meta
                        .parts
                        .iter()
                        .map(|part| ObjectPart {
                            number: part.number,
                            size: part.size,
                        })
                        .collect(),
                },
            };
            Entry {
                version: read_at,
                state: EntryState::Dirty,
                object: Some(ObjectVersion {
                    size: meta.size,
                    last_modified_ms: meta.last_modified_ms,
                    local_etag: meta.etag.clone(),
                    write_identity: (meta.identity < version).then_some(meta.identity),
                    metadata: meta.metadata.clone(),
                    tags: meta.tags.clone(),
                    checksums: meta.checksums.clone(),
                    storage_class: object.and_then(|o| o.storage_class.clone()),
                    copy_source: object.and_then(|o| o.copy_source.clone()),
                    payload,
                    coded: Some(coded),
                }),
                remote_etag: snapshot.and_then(|entry| entry.remote_etag.clone()),
                remote_version_id: snapshot.and_then(|entry| entry.remote_version_id.clone()),
            }
        }
    };
    Ok((entry, missing))
}

/// Locates every fragment of `stripe` from `copies`, one fragment per
/// node, and returns the stripe with the number of fragments no copy is
/// located for. A fragment whose copies all share nodes with other
/// fragments' goes where `known`, the snapshot's layout of the stripe, put
/// it, or else on a node of `placement` the stripe does not use.
fn fill(
    stripe: &StripeLayout,
    copies: &[Copy],
    known: Option<&CodedStripe>,
    placement: Placement<'_>,
) -> Result<(CodedStripe, usize), ReindexError> {
    let shape = (
        stripe.number,
        stripe.offset,
        stripe.data_len,
        stripe.geometry,
        stripe.codec,
    );
    let mut candidates: Vec<Vec<&FragmentLocation>> = vec![Vec::new(); stripe.fragments.len()];
    for copy in copies.iter().filter(|copy| copy.shape == shape) {
        if let Some(of_index) = candidates.get_mut(usize::from(copy.index)) {
            of_index.push(&copy.location);
        }
    }
    let matched = match_nodes(&candidates);
    let located = matched.iter().flatten().count();
    if located < stripe.geometry.data_fragments() {
        return Err(ReindexError::Layout {
            stripe: stripe.number,
            reason: format!("only {located} of its fragments are on distinct nodes"),
        });
    }
    let known = known.filter(|known| {
        (
            known.number(),
            known.offset(),
            known.data_len(),
            known.geometry(),
            known.codec(),
        ) == shape
    });
    let mut used: BTreeSet<NodeId> = matched.iter().flatten().map(|l| l.node.clone()).collect();
    let mut fragments = Vec::with_capacity(matched.len());
    for (index, slot) in matched.iter().enumerate() {
        let location = match slot {
            Some(location) => (*location).clone(),
            None => {
                let location = known
                    .map(|known| &known.fragments()[index])
                    .filter(|location| !used.contains(&location.node))
                    .cloned()
                    .or_else(|| {
                        let node = placement
                            .lost
                            .iter()
                            .chain(placement.nodes)
                            .find(|node| !used.contains(*node))?;
                        Some(FragmentLocation {
                            node: node.clone(),
                            fragment: UNKNOWN_FRAGMENT,
                        })
                    })
                    .ok_or_else(|| ReindexError::Layout {
                        stripe: stripe.number,
                        reason: format!("no node is left for fragment {index}"),
                    })?;
                used.insert(location.node.clone());
                location
            }
        };
        fragments.push(location);
    }
    let coded = CodedStripe::new(
        stripe.number,
        stripe.offset,
        stripe.data_len,
        stripe.geometry,
        stripe.codec,
        fragments,
    )
    .map_err(|error| ReindexError::Layout {
        stripe: stripe.number,
        reason: error.to_string(),
    })?;
    Ok((coded, matched.len() - located))
}

/// Picks for each fragment index one of its `candidates`, in order of
/// preference, so that no two share a node and as many as possible are
/// picked: a maximum matching of indices to nodes, by augmenting paths.
fn match_nodes<'a>(candidates: &[Vec<&'a FragmentLocation>]) -> Vec<Option<&'a FragmentLocation>> {
    let mut matched = vec![None; candidates.len()];
    let mut owners = BTreeMap::<&NodeId, usize>::new();
    for index in 0..candidates.len() {
        augment(
            index,
            candidates,
            &mut matched,
            &mut owners,
            &mut BTreeSet::new(),
        );
    }
    matched
}

/// Finds `index` a node, moving the index that holds it to another of its
/// candidates if needed; `seen` holds the nodes tried on this path.
fn augment<'a>(
    index: usize,
    candidates: &[Vec<&'a FragmentLocation>],
    matched: &mut [Option<&'a FragmentLocation>],
    owners: &mut BTreeMap<&'a NodeId, usize>,
    seen: &mut BTreeSet<&'a NodeId>,
) -> bool {
    for &location in &candidates[index] {
        if !seen.insert(&location.node) {
            continue;
        }
        let free = match owners.get(&location.node) {
            None => true,
            Some(&other) => augment(other, candidates, matched, owners, seen),
        };
        if free {
            owners.insert(&location.node, index);
            matched[index] = Some(location);
            return true;
        }
    }
    false
}

/// The unrecoverable `version`, described by the headers' `object` or the
/// snapshot's `entry`: every unrecoverable version is named by one of them.
fn unrecoverable(
    version: EpochSeq,
    object: Option<&ObjectMeta>,
    entry: Option<&Entry>,
    error: ReindexError,
) -> Option<Unrecoverable> {
    let (identity, size, etag, last_modified_ms) = match (
        object,
        entry.and_then(|e| Some((e.version, e.object.as_ref()?))),
    ) {
        (Some(meta), _) => (
            meta.identity,
            meta.size,
            meta.etag.clone(),
            meta.last_modified_ms,
        ),
        (None, Some((at, object))) => (
            object.write_identity.unwrap_or(at),
            object.size,
            object.local_etag.clone(),
            object.last_modified_ms,
        ),
        (None, None) => return None,
    };
    Some(Unrecoverable {
        version,
        identity,
        size,
        etag,
        last_modified_ms,
        error,
    })
}

/// The position right after `position`.
fn next(position: EpochSeq) -> EpochSeq {
    EpochSeq::new(
        position.epoch,
        position.seq.checked_next().unwrap_or(Seq::MAX),
    )
}

/// Seeded bugs of re-indexing, for simulations that must catch them.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod seeded {
    use std::cell::Cell;

    /// A bug re-indexing can be seeded with.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReindexBug {
        /// Prefers the oldest attempt's headers to the latest's, for the
        /// fragments' copies and the object's tags.
        OlderAttempt,
        /// Takes the tags and write identity of the latest attempt's
        /// headers even when the snapshot's entry names a later retag.
        HeaderTags,
        /// Keeps the snapshot's version of a key it holds as an object,
        /// ignoring versions written after the snapshot.
        PrefersSnapshot,
        /// Restores a key the snapshot does not hold from headers that
        /// predate it, ignoring the delete that removed it.
        Resurrects,
        /// Falls back to an older version when the newest cannot be
        /// rebuilt.
        FallsBack,
    }

    thread_local! {
        static BUG: Cell<Option<ReindexBug>> = const { Cell::new(None) };
    }

    /// Seeds `bug` into every re-indexing on this thread, or removes it.
    pub fn seed_reindex_bug(bug: Option<ReindexBug>) {
        BUG.set(bug);
    }

    fn seeded(bug: ReindexBug) -> bool {
        BUG.get() == Some(bug)
    }

    pub(super) fn older_attempts() -> bool {
        seeded(ReindexBug::OlderAttempt)
    }

    pub(super) fn header_tags() -> bool {
        seeded(ReindexBug::HeaderTags)
    }

    pub(super) fn prefers_snapshot() -> bool {
        seeded(ReindexBug::PrefersSnapshot)
    }

    pub(super) fn resurrects() -> bool {
        seeded(ReindexBug::Resurrects)
    }

    pub(super) fn falls_back() -> bool {
        seeded(ReindexBug::FallsBack)
    }
}

#[cfg(not(feature = "test-util"))]
mod seeded {
    pub(super) fn older_attempts() -> bool {
        false
    }

    pub(super) fn header_tags() -> bool {
        false
    }

    pub(super) fn prefers_snapshot() -> bool {
        false
    }

    pub(super) fn resurrects() -> bool {
        false
    }

    pub(super) fn falls_back() -> bool {
        false
    }
}
