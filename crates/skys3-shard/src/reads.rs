//! Read plans and read registrations (§8.7, §9.2).
//!
//! **Read plans.** For a GET, the primary resolves the key under its lease
//! and answers a [`ReadPlan`] ([`Shard::plan`](crate::Shard::plan)): the
//! key's entry, which names the version (its position and ETag) and size,
//! the version's *layout*, and its *holders*. The layout is the version's
//! bytes as positions of the shard's log, in body order: the `EXTENT`
//! records of a large body, the `PUT` or `MPU_PART` record of an inline
//! one, and the parts of a multipart object, read with the entry in one
//! index transaction. Positions are the same on every replica, since every
//! replica holds the records its primary sequenced at the positions it
//! gave them, and the payload at a position never changes; so any replica
//! that still locates a position holds the right bytes for it. The holders
//! are the members that should have a copy: every member while the
//! version is not clean, since each holds it durably, and the first
//! `clean_copies` of the primary and the other members, in configuration
//! order, once it is (§9.3). An evicted version has no holder; its bytes
//! are at the remote. A coded version has neither layout nor holders: its
//! entry's coded layout names the fragment nodes the gateway reads it
//! from (§8.5), since the replicas drop its bytes. Holder lists are hints.
//!
//! **Registrations.** Before it fetches, a gateway registers the plan's
//! version with the holder it reads from ([`Shard::register_read`](crate::Shard::register_read)).
//! The holder serves its own copy if its entry still names the version
//! with local bytes, and otherwise the plan's layout, if it still locates
//! every position of it: payload a later write or an eviction left
//! unreferenced.
//! Otherwise it says it does not hold the version, and the gateway asks
//! the next holder. A registration pins the positions it serves until it
//! is released, or lapses `read_registration_ttl_seconds` after it was
//! made or last renewed; a fetch under a registration that lapsed is
//! refused, so the GET fails mid-stream, and the client retries.
//!
//! **What compaction keeps** (§10.3). Registrations are node-local and
//! in memory, in [`Reads`], one per node, which the
//! [`Compactor`](crate::Compactor) asks. It copies, rather than drops or
//! evicts, every record whose position a live registration pins
//! ([`Reads::is_pinned`]), and checks the pins again when it removes the
//! locations of what it drops, in the same [`Reads::exclusive`] decision
//! as a registration's pin and check, so neither misses the other. It
//! drops payload that nothing references only once it has found it
//! unreferenced for [`ReadSettings::release_delay`]
//! (`fragment_release_delay_seconds`): the delay covers a plan issued
//! just before the write or eviction that left the payload unreferenced,
//! whose gateway has not registered it yet.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_index::{Entry, EntryState, IndexError, IndexReader, ObjectVersion, Payload};
use skys3_log::ShardRef;
use skys3_log::record::ExtentRef;
use skys3_types::{EpochSeq, NodeId, ShardConfig};
use tokio::time::Instant;

/// A read registration's number on its holder's node.
pub type ReadId = u64;

/// The timers of read registrations (§8.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSettings {
    /// `read_registration_ttl_seconds`: how long a registration lasts
    /// after it was made or last renewed.
    pub ttl: Duration,
    /// `fragment_release_delay_seconds`: how long compaction keeps
    /// unreferenced payload before it may reclaim it, for plans issued
    /// before a write or an eviction left it unreferenced (see the
    /// [module](self) docs).
    pub release_delay: Duration,
}

impl Default for ReadSettings {
    /// The design's defaults: a 30-second TTL and a 60-second delay.
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            release_delay: Duration::from_secs(60),
        }
    }
}

/// A primary's read plan of a key (§9.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan {
    /// The key's entry: its latest version, a delete tombstone, or `None`.
    pub entry: Option<Entry>,
    /// The version's bytes as log positions, in body order; empty if the
    /// version has no local bytes, none at all, or is coded: its entry's
    /// coded layout then names its fragments (§8.5).
    pub layout: Vec<ExtentRef>,
    /// The nodes whose replicas should hold the bytes, the primary first;
    /// empty if no replica does, as for an evicted version.
    pub holders: Vec<NodeId>,
}

/// A read a holder registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// The registration, which fetches and renewals name.
    pub id: ReadId,
    /// The positions the holder serves the version from, in body order.
    pub layout: Vec<ExtentRef>,
}

/// What a node's read registrations did, for metrics and audits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadCounts {
    /// Registrations made.
    pub registered: u64,
    /// Registrations refused because the replica no longer held the
    /// version.
    pub not_held: u64,
    /// Fetches and renewals refused because their registration had lapsed
    /// or was never made on this node in its current life.
    pub lapsed: u64,
}

/// The read registrations of one node's replicas (see the [module](self)
/// docs). Clones share the registrations.
#[derive(Debug, Clone, Default)]
pub struct Reads {
    state: Arc<Mutex<State>>,
    /// Serializes a registration's pin and check against compaction's
    /// decision to drop payload ([`Reads::exclusive`]).
    decisions: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct State {
    settings: ReadSettings,
    next: ReadId,
    reads: HashMap<ReadId, Registration>,
    /// Every live registration by when it lapses.
    expiry: BTreeSet<(Instant, ReadId)>,
    /// How many live registrations pin each position.
    pins: BTreeMap<(ShardRef, EpochSeq), usize>,
    counts: ReadCounts,
}

#[derive(Debug)]
struct Registration {
    shard: ShardRef,
    /// The positions it pins, sorted.
    positions: Vec<EpochSeq>,
    expires: Instant,
}

impl Reads {
    /// Registrations with `settings`.
    #[must_use]
    pub fn new(settings: ReadSettings) -> Self {
        let reads = Self::default();
        reads.configure(settings);
        reads
    }

    /// Uses `settings` for registrations made or renewed from now on.
    pub fn configure(&self, settings: ReadSettings) {
        self.lock().settings = settings;
    }

    /// Runs `decide` while no other caller of this method runs, on this
    /// node: a registration pins its positions and checks that the index
    /// still locates them in one such call, and compaction checks the
    /// pins and removes the locations of what it drops in another, so
    /// neither misses the other. `decide` must not call it again.
    pub fn exclusive<T>(&self, decide: impl FnOnce() -> T) -> T {
        // The lock guards no data.
        let _decision = self
            .decisions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        decide()
    }

    /// The settings in use.
    #[must_use]
    pub fn settings(&self) -> ReadSettings {
        self.lock().settings
    }

    /// Registers a read that pins `layout` of `shard`, for the TTL.
    pub(crate) fn pin(&self, shard: &ShardRef, layout: &[ExtentRef]) -> ReadId {
        let mut state = self.lock();
        let now = Instant::now();
        state.expire(now);
        state.next += 1;
        let id = state.next;
        let mut positions: Vec<_> = layout.iter().map(|extent| extent.position).collect();
        positions.sort_unstable();
        positions.dedup();
        for position in &positions {
            *state.pins.entry((shard.clone(), *position)).or_default() += 1;
        }
        let expires = now + state.settings.ttl;
        state.expiry.insert((expires, id));
        state.counts.registered += 1;
        state.reads.insert(
            id,
            Registration {
                shard: shard.clone(),
                positions,
                expires,
            },
        );
        id
    }

    /// Counts a registration refused because the replica did not hold the
    /// version.
    pub(crate) fn not_held(&self) {
        self.lock().counts.not_held += 1;
    }

    /// Extends registration `id` by the TTL from now. `false` if it lapsed,
    /// or was never made on this node in its current life.
    pub fn renew(&self, id: ReadId) -> bool {
        let mut state = self.lock();
        let now = Instant::now();
        state.expire(now);
        let ttl = state.settings.ttl;
        let Some(read) = state.reads.get_mut(&id) else {
            state.counts.lapsed += 1;
            return false;
        };
        let lapsed = std::mem::replace(&mut read.expires, now + ttl);
        state.expiry.remove(&(lapsed, id));
        state.expiry.insert((now + ttl, id));
        true
    }

    /// Releases registration `id`, if it is still live: its positions are
    /// no longer pinned by it.
    pub fn release(&self, id: ReadId) {
        let mut state = self.lock();
        if let Some(read) = state.reads.remove(&id) {
            state.expiry.remove(&(read.expires, id));
            state.unpin(&read);
        }
    }

    /// Whether registration `id` is live and pins `position` of `shard`;
    /// counts it as lapsed if it is not live.
    pub(crate) fn admits(&self, id: ReadId, shard: &ShardRef, position: EpochSeq) -> bool {
        let mut state = self.lock();
        state.expire(Instant::now());
        match state.reads.get(&id) {
            Some(read) => read.shard == *shard && read.positions.binary_search(&position).is_ok(),
            None => {
                state.counts.lapsed += 1;
                false
            }
        }
    }

    /// Whether registration `id` is live.
    #[must_use]
    pub fn is_live(&self, id: ReadId) -> bool {
        let mut state = self.lock();
        state.expire(Instant::now());
        state.reads.contains_key(&id)
    }

    /// Whether a live registration pins `position` of `shard`: compaction
    /// keeps its record (see the [module](self) docs).
    #[must_use]
    pub fn is_pinned(&self, shard: &ShardRef, position: EpochSeq) -> bool {
        let mut state = self.lock();
        state.expire(Instant::now());
        state.pins.contains_key(&(shard.clone(), position))
    }

    /// Every position of `shard` a live registration pins, in order.
    #[must_use]
    pub fn pinned(&self, shard: &ShardRef) -> Vec<EpochSeq> {
        let mut state = self.lock();
        state.expire(Instant::now());
        state
            .pins
            .keys()
            .filter(|(of, _)| of == shard)
            .map(|(_, position)| *position)
            .collect()
    }

    /// How many registrations are live.
    #[must_use]
    pub fn live(&self) -> usize {
        let mut state = self.lock();
        state.expire(Instant::now());
        state.reads.len()
    }

    /// What the node's registrations did so far.
    #[must_use]
    pub fn counts(&self) -> ReadCounts {
        self.lock().counts
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Every update leaves the state consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// Drops every registration that lapsed by `now`.
    fn expire(&mut self, now: Instant) {
        while let Some(&(expires, id)) = self.expiry.first() {
            if expires > now {
                return;
            }
            self.expiry.pop_first();
            if let Some(read) = self.reads.remove(&id) {
                self.unpin(&read);
            }
        }
    }

    fn unpin(&mut self, read: &Registration) {
        for position in &read.positions {
            let key = (read.shard.clone(), *position);
            if let Some(count) = self.pins.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    self.pins.remove(&key);
                }
            }
        }
    }
}

/// The bytes of `object`, a version of a key in `shard`, as log positions
/// in body order, or `None` if this replica holds no bytes for it: an
/// evicted version, an imported stub, or a multipart object whose parts
/// have none. A multipart object's parts are read from `reader`, and must
/// be the ones the object lists.
///
/// # Errors
///
/// The index's.
pub fn layout(
    reader: &IndexReader,
    shard: &ShardRef,
    object: &ObjectVersion,
) -> Result<Option<Vec<ExtentRef>>, IndexError> {
    Ok(match &object.payload {
        Payload::None => None,
        Payload::Inline(position) => Some(inline(*position, object.size)),
        Payload::Extents(extents) => Some(extents.clone()),
        Payload::Parts { upload, parts } => {
            let rows = reader.parts(shard, *upload, 0, parts.len())?;
            let listed = parts.iter().map(|part| (part.number, part.size));
            if !rows
                .iter()
                .map(|(number, part)| (*number, part.size))
                .eq(listed)
            {
                return Ok(None);
            }
            let mut layout = Vec::new();
            for (_, part) in rows {
                match part.payload {
                    Payload::Inline(position) => layout.extend(inline(position, part.size)),
                    Payload::Extents(extents) => layout.extend(extents),
                    Payload::None | Payload::Parts { .. } => return Ok(None),
                }
            }
            Some(layout)
        }
    })
}

/// The inline bytes of the record at `position`, `size` of them, as a
/// layout: nothing for an empty body.
fn inline(position: EpochSeq, size: u64) -> Vec<ExtentRef> {
    // An inline body is at most `inline_max_bytes`, far below `u32::MAX`.
    u32::try_from(size)
        .ok()
        .filter(|len| *len > 0)
        .map(|len| ExtentRef { position, len })
        .into_iter()
        .collect()
}

/// The holders of `entry` in `config`, given the bucket's `clean_copies`
/// if known, when the primary holds its bytes: see [`ReadPlan::holders`].
pub(crate) fn holders(
    config: &ShardConfig,
    entry: &Entry,
    clean_copies: Option<u8>,
) -> Vec<NodeId> {
    let others = config.members.iter().filter(|m| **m != config.primary);
    let ranked = std::iter::once(&config.primary).chain(others).cloned();
    match entry.state {
        EntryState::Evicted => Vec::new(),
        EntryState::Clean => match clean_copies {
            // The plan has a layout only if the primary holds the bytes.
            Some(copies) => ranked.take(usize::from(copies).max(1)).collect(),
            None => ranked.collect(),
        },
        // Dirty, flushing, and conflicted versions are on every member.
        _ => ranked.collect(),
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, Epoch, Seq, ShardId};

    use super::*;

    fn shard() -> ShardRef {
        ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0))
    }

    fn at(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    fn extents(seqs: &[u64]) -> Vec<ExtentRef> {
        seqs.iter()
            .map(|seq| ExtentRef {
                position: at(*seq),
                len: 10,
            })
            .collect()
    }

    fn settings() -> ReadSettings {
        ReadSettings {
            ttl: Duration::from_secs(3),
            release_delay: Duration::from_secs(5),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_registration_pins_its_positions_until_it_lapses() {
        let reads = Reads::new(settings());
        let id = reads.pin(&shard(), &extents(&[3, 1, 3]));
        assert!(reads.is_pinned(&shard(), at(1)));
        assert!(reads.is_pinned(&shard(), at(3)));
        assert!(!reads.is_pinned(&shard(), at(2)));
        assert_eq!(reads.pinned(&shard()), [at(1), at(3)]);
        assert!(reads.admits(id, &shard(), at(1)));
        // A position the read does not pin, or another shard's, is refused.
        assert!(!reads.admits(id, &shard(), at(2)));
        let other = ShardRef::new(BucketId::new("b-2").unwrap(), ShardId::new(0));
        assert!(!reads.admits(id, &other, at(1)));
        assert_eq!(reads.counts().lapsed, 0);

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(reads.renew(id));
        tokio::time::advance(Duration::from_secs(2)).await;
        // Renewed two seconds ago, so one more second to go.
        assert!(reads.is_live(id));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!reads.is_live(id));
        assert!(!reads.is_pinned(&shard(), at(1)));
        assert!(!reads.admits(id, &shard(), at(1)));
        assert!(!reads.renew(id));
        assert_eq!(reads.counts().lapsed, 2);
        assert_eq!(reads.live(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn pins_are_counted_across_registrations_and_released() {
        let reads = Reads::new(settings());
        let first = reads.pin(&shard(), &extents(&[1, 2]));
        let second = reads.pin(&shard(), &extents(&[2]));
        assert_ne!(first, second);
        assert_eq!(reads.live(), 2);
        reads.release(first);
        assert!(!reads.is_pinned(&shard(), at(1)));
        assert!(reads.is_pinned(&shard(), at(2)));
        // Releasing twice, or an unknown read, changes nothing.
        reads.release(first);
        reads.release(99);
        reads.release(second);
        assert!(reads.pinned(&shard()).is_empty());
        reads.not_held();
        assert_eq!(
            reads.counts(),
            ReadCounts {
                registered: 2,
                not_held: 1,
                lapsed: 0
            }
        );
        assert_eq!(reads.settings(), settings());
    }

    fn config(members: &[&str]) -> ShardConfig {
        let nodes: Vec<NodeId> = members.iter().map(|m| m.parse().unwrap()).collect();
        ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
            epoch: Epoch::new(1),
            primary: nodes[1].clone(),
            members: nodes,
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 3,
            proposal_id: skys3_types::ProposalId::new("p").unwrap(),
        }
    }

    fn entry(state: EntryState) -> Entry {
        Entry {
            version: at(1),
            state,
            object: None,
            remote_etag: None,
            remote_version_id: None,
        }
    }

    #[test]
    fn holders_rank_the_primary_first() {
        let config = config(&["node-a", "node-b", "node-c"]);
        let names = |holders: Vec<NodeId>| -> Vec<String> {
            holders.iter().map(ToString::to_string).collect()
        };
        assert_eq!(
            names(holders(&config, &entry(EntryState::Dirty), Some(1))),
            ["node-b", "node-a", "node-c"]
        );
        assert_eq!(
            names(holders(&config, &entry(EntryState::Clean), Some(2))),
            ["node-b", "node-a"]
        );
        assert_eq!(
            names(holders(&config, &entry(EntryState::Clean), None)),
            ["node-b", "node-a", "node-c"]
        );
        assert_eq!(
            names(holders(&config, &entry(EntryState::Clean), Some(0))),
            ["node-b"]
        );
        assert!(holders(&config, &entry(EntryState::Evicted), None).is_empty());
    }

    #[test]
    fn inline_bodies_are_one_extent_unless_empty() {
        assert_eq!(
            inline(at(4), 7),
            [ExtentRef {
                position: at(4),
                len: 7
            }]
        );
        assert!(inline(at(4), 0).is_empty());
    }
}
