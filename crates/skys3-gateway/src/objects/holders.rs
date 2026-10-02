//! A GET's bytes, read from a holder its read plan names (§8.7, §9.2).
//!
//! The gateway asks the holders in order, its own node first, then the
//! one it has the fewest reads in progress from, and registers the read
//! with the first that holds the version. The response streams from that
//! holder alone: the first piece is fetched before the response starts,
//! so a holder that fails at once is passed over, and the rest is fetched
//! at most one piece ahead of the client. From registration until the
//! stream ends, the first fetch included, the gateway renews the
//! registration every `read_registration_renew_interval_seconds`;
//! a holder that lets it lapse refuses the next fetch, and the response
//! fails mid-stream. The registration is released once the stream ends.
//! A read the gateway may keep in its hot cache ([`Keep`]) reserves room
//! for the object when its stream starts, collects the pieces it streams,
//! and fills the cache only once it streamed them all; a read that cannot
//! reserve room streams without keeping anything.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use s3s::dto::StreamingBlob;
use skys3_log::record::ExtentRef;
use skys3_shard::{ReadId, Registered};
use skys3_types::{EpochSeq, NodeId};
use tokio::sync::mpsc;

use super::download::ExtentBody;
use crate::hot_cache::{HotCache, ObjectName, VersionName};
use crate::shard::{ShardError, ShardRef, Shards};

/// The gateway's reads from holders: how many it has in progress from
/// each node, which decides whom it asks first, and how often it renews
/// their registrations. Clones share the counts.
#[derive(Debug, Clone)]
pub(crate) struct HolderReads {
    load: Arc<Mutex<BTreeMap<NodeId, usize>>>,
    renew_every: Duration,
}

impl HolderReads {
    /// Reads that renew their registrations every `renew_every`.
    pub(crate) fn new(renew_every: Duration) -> Self {
        Self {
            load: Arc::default(),
            renew_every,
        }
    }

    /// The order to ask `holders` in: `local`, this gateway's node, first
    /// if it is one, then the others by how many reads are in progress
    /// from each, fewest first, in plan order among equals.
    pub(crate) fn order(&self, holders: &[NodeId], local: Option<&NodeId>) -> Vec<NodeId> {
        let load = self.lock();
        let mut ordered = holders.to_vec();
        ordered.sort_by_key(|holder| {
            (
                Some(holder) != local,
                load.get(holder).copied().unwrap_or_default(),
            )
        });
        ordered
    }

    /// Opens the bytes `range` of `version` of `key`, whose plan gave
    /// `layout`, from `holder`: `Ok(None)` if the holder does not hold the
    /// version. With `keep`, the bytes fill the hot cache once every one
    /// of them has streamed.
    ///
    /// # Errors
    ///
    /// Why the holder could not register the read or serve its first
    /// piece; the read is then released.
    #[allow(clippy::too_many_arguments, reason = "the parts of a planned read")]
    pub(crate) async fn open<H: Shards>(
        &self,
        shards: &H,
        target: (&ShardRef, &NodeId),
        key: &str,
        version: EpochSeq,
        layout: Vec<ExtentRef>,
        range: Range<u64>,
        keep: Option<Keep>,
    ) -> Result<Option<StreamingBlob>, ShardError> {
        let (shard, holder) = target;
        let Some(Registered { id, layout }) =
            shards.register(shard, holder, key, version, layout).await?
        else {
            return Ok(None);
        };
        // Renewals start now, so a first fetch slower than the TTL does not
        // outlive the registration.
        let read = HolderRead::start(
            shards.clone(),
            (shard.clone(), holder.clone(), id),
            Load::start(&self.load, holder),
            self.renew_every,
        );
        let mut pieces = match pieces(&layout, &range) {
            Some(pieces) => pieces.into_iter(),
            None => {
                read.release().await;
                return Err(ShardError::Unavailable {
                    shard: shard.clone(),
                    reason: format!("{holder} holds fewer bytes than the object's size"),
                });
            }
        };
        let first = match pieces.next() {
            Some(piece) => match read.fetch(&piece).await {
                Ok(data) => Some(data),
                Err(error) => {
                    read.release().await;
                    return Err(error);
                }
            },
            None => None,
        };
        let (sender, receiver) = mpsc::channel(1);
        // Room in the cache is reserved only once the stream starts; a read
        // that cannot reserve it streams all the same and keeps nothing.
        let mut fill =
            keep.and_then(|keep| keep.cache.reserve(keep.object, keep.version, keep.size));
        tokio::spawn(async move {
            let mut streamed = false;
            if let Some(first) = first {
                if let Some(fill) = &mut fill {
                    fill.push(&first);
                }
                streamed = sender.send(Ok(first)).await.is_ok();
                for piece in pieces {
                    if !streamed {
                        break;
                    }
                    let fetched = read.fetch(&piece).await;
                    if let (Some(fill), Ok(data)) = (&mut fill, &fetched) {
                        fill.push(data);
                    }
                    let failed = fetched.is_err();
                    streamed = sender.send(fetched).await.is_ok() && !failed;
                }
            }
            read.release().await;
            // A read that broke off, or whose body was dropped, releases its
            // room as the fill drops.
            if let Some(fill) = fill.filter(|_| streamed) {
                fill.finish();
            }
        });
        Ok(Some(StreamingBlob::from(s3s::Body::http_body(
            ExtentBody {
                receiver,
                remaining: range.end - range.start,
            },
        ))))
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<NodeId, usize>> {
        // Every update leaves the counts consistent.
        self.load.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where a read's bytes go once it has streamed them all: the hot cache,
/// under the version the read plan named, if it reserves room for them
/// when the stream starts.
#[derive(Debug)]
pub(crate) struct Keep {
    pub cache: HotCache,
    pub object: ObjectName,
    pub version: VersionName,
    /// The object's size: the room reserved, and the bytes the read must
    /// stream for the cache to keep them.
    pub size: u64,
}

/// One read registered with a holder.
struct HolderRead<H> {
    shards: H,
    shard: ShardRef,
    holder: NodeId,
    id: ReadId,
    /// Renews the registration until the read is released or dropped.
    renewing: Renewing,
    /// Counts the read against the holder while it lasts.
    _load: Load,
}

impl<H: Shards> HolderRead<H> {
    /// The read registered as `id` with `holder` for `shard`, which renews
    /// its registration every `renew_every` from now on.
    fn start(
        shards: H,
        (shard, holder, id): (ShardRef, NodeId, ReadId),
        load: Load,
        renew_every: Duration,
    ) -> Self {
        let renewal = Renewal {
            shards: shards.clone(),
            shard: shard.clone(),
            holder: holder.clone(),
            id,
        };
        Self {
            shards,
            shard,
            holder,
            id,
            renewing: Renewing(tokio::spawn(renewal.renew_every(renew_every))),
            _load: load,
        }
    }

    /// The bytes of `piece`.
    async fn fetch(&self, piece: &Piece) -> Result<Bytes, ShardError> {
        let data = self
            .shards
            .fetch(&self.shard, &self.holder, self.id, piece.extent.position)
            .await?;
        if data.len() as u64 != u64::from(piece.extent.len) {
            return Err(ShardError::Unavailable {
                shard: self.shard.clone(),
                reason: format!(
                    "the payload at {} has the wrong length",
                    piece.extent.position
                ),
            });
        }
        Ok(data.slice(piece.bytes.clone()))
    }

    /// Stops renewing the registration and releases it, as a courtesy: one
    /// that is not released lapses.
    async fn release(self) {
        drop(self.renewing);
        if let Err(error) = self
            .shards
            .release(&self.shard, &self.holder, self.id)
            .await
        {
            tracing::debug!(shard = %self.shard, holder = %self.holder, %error,
                "a read registration was not released; it lapses");
        }
    }
}

/// The task renewing one registration, stopped when this is dropped: on
/// every path that releases the read, and when the read itself is dropped.
struct Renewing(tokio::task::JoinHandle<()>);

impl Drop for Renewing {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What renews one registration.
struct Renewal<H> {
    shards: H,
    shard: ShardRef,
    holder: NodeId,
    id: ReadId,
}

impl<H: Shards> Renewal<H> {
    /// Renews the registration every `every`, until it lapsed. A renewal
    /// that fails is not retried before the next: the holder decides
    /// whether the registration lapsed, and the next fetch hears it.
    async fn renew_every(self, every: Duration) {
        loop {
            tokio::time::sleep(every).await;
            match self.shards.renew(&self.shard, &self.holder, self.id).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    tracing::debug!(shard = %self.shard, holder = %self.holder, %error,
                        "a read registration was not renewed");
                }
            }
        }
    }
}

/// One read in progress from a node, counted while it lasts.
struct Load {
    counts: Arc<Mutex<BTreeMap<NodeId, usize>>>,
    node: NodeId,
}

impl Load {
    fn start(counts: &Arc<Mutex<BTreeMap<NodeId, usize>>>, node: &NodeId) -> Self {
        *lock(counts).entry(node.clone()).or_default() += 1;
        Self {
            counts: Arc::clone(counts),
            node: node.clone(),
        }
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        let mut counts = lock(&self.counts);
        if let Some(count) = counts.get_mut(&self.node) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.node);
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update leaves the value consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The part of one extent a read serves.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Piece {
    extent: ExtentRef,
    /// The bytes of the extent's payload to serve.
    bytes: Range<usize>,
}

/// The pieces of the extents of `layout` that hold the body bytes `range`,
/// in order, or `None` if the layout ends before `range` does.
fn pieces(layout: &[ExtentRef], range: &Range<u64>) -> Option<Vec<Piece>> {
    let mut pieces = Vec::new();
    let mut start = 0u64;
    for extent in layout {
        if start >= range.end {
            break;
        }
        let end = start + u64::from(extent.len);
        if end > range.start {
            // Both bounds are within the extent, so they fit in `usize`.
            let from = range.start.saturating_sub(start) as usize;
            let to = (range.end - start).min(u64::from(extent.len)) as usize;
            pieces.push(Piece {
                extent: *extent,
                bytes: from..to,
            });
        }
        start = end;
    }
    (start >= range.end).then_some(pieces)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use http_body_util::BodyExt;
    use skys3_log::RecordBody;
    use skys3_log::record::{Extent, Put, PutData};
    use skys3_shard::ReadSettings;
    use skys3_types::{ETag, Epoch, Seq};

    use super::*;
    use crate::conditions::Precondition;
    use crate::shard::tests::bucket;
    use crate::stub::MemoryShards;

    /// Shards on one node, with `key` written twice: five extents of four
    /// `a`s, then five of `b`s. Returns the shards, the shard, and the
    /// plan of the first version.
    async fn overwritten(key: &str) -> (MemoryShards, ShardRef, EpochSeq, Vec<ExtentRef>) {
        let shards = MemoryShards::new().await;
        let doc = bucket("b-1", 1);
        let shard = ShardRef::for_key(&doc, key);
        shards.open(&shard, &doc).await.unwrap();
        let mut first = None;
        for fill in *b"ab" {
            let mut extents = Vec::new();
            for n in 0..5u64 {
                let extent = Extent {
                    key: key.to_owned(),
                    offset: n * 4,
                    data: Bytes::from(vec![fill; 4]),
                };
                extents.push(shards.append_extent(&shard, extent).await.unwrap());
            }
            let put = Put {
                key: key.to_owned(),
                size: 20,
                last_modified_ms: 0,
                etag: ETag::new("0123456789abcdef0123456789abcdef").unwrap(),
                inherited_identity: None,
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
                checksums: BTreeMap::new(),
                copy_source: None,
                data: PutData::Extents(extents),
            };
            let written = shards
                .write(&shard, RecordBody::Put(put), Precondition::None)
                .await
                .unwrap()
                .unwrap();
            if first.is_none() {
                let plan = shards.plan(&shard, key).await.unwrap();
                assert_eq!(plan.holders, [shards.node().unwrap()]);
                first = Some((written, plan.layout));
            }
        }
        let (version, layout) = first.unwrap();
        (shards, shard, version, layout)
    }

    fn name(key: &str) -> ObjectName {
        ObjectName {
            bucket: bucket("b-1", 1).bucket_id,
            key: key.to_owned(),
        }
    }

    fn named(version: EpochSeq) -> VersionName {
        VersionName {
            position: version,
            etag: ETag::new("0123456789abcdef0123456789abcdef").unwrap(),
        }
    }

    /// Keeps the bytes of `version` of `k`, `size` bytes long, in `cache`.
    fn keep(cache: &HotCache, version: EpochSeq, size: u64) -> Keep {
        Keep {
            cache: cache.clone(),
            object: name("k"),
            version: named(version),
            size,
        }
    }

    /// Waits until `cache` reserves nothing for fills: the stream tasks
    /// release their room once they end, just after the client has the
    /// last byte.
    async fn released(cache: &HotCache) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while cache.usage().reserved > 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the fill releases its room");
    }

    async fn collect(body: StreamingBlob) -> Result<Vec<u8>, String> {
        let collected = s3s::Body::from(body).collect().await;
        collected
            .map(|body| body.to_bytes().to_vec())
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn a_holder_serves_the_planned_version_after_an_overwrite() {
        let (shards, shard, version, layout) = overwritten("k").await;
        let reads = HolderReads::new(Duration::from_secs(10));
        let holder = shards.node().unwrap();
        let body = reads
            .open(
                &shards,
                (&shard, &holder),
                "k",
                version,
                layout.clone(),
                2..11,
                None,
            )
            .await
            .unwrap()
            .expect("the holder still locates the first version");
        assert_eq!(collect(body).await.unwrap(), vec![b'a'; 9]);
        // The stream released its registration once it ended.
        tokio::task::yield_now().await;
        let set = shards.local().set();
        tokio::time::timeout(Duration::from_secs(5), async {
            while set.reads().live() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(reads.lock().is_empty());

        // A whole read that may keep its bytes fills the cache with them,
        // under the planned version, once it streamed them all.
        let cache = HotCache::new(1024);
        let body = reads
            .open(
                &shards,
                (&shard, &holder),
                "k",
                version,
                layout.clone(),
                0..20,
                Some(keep(&cache, version, 20)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(collect(body).await.unwrap(), vec![b'a'; 20]);
        let cached = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(data) = cache.get(&name("k"), &named(version), 0..20) {
                    return data;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cached, vec![b'a'; 20]);
        // A read that streams fewer bytes than the object keeps none.
        let cache = HotCache::new(1024);
        let body = reads
            .open(
                &shards,
                (&shard, &holder),
                "k",
                version,
                layout.clone(),
                0..8,
                Some(keep(&cache, version, 20)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(collect(body).await.unwrap(), vec![b'a'; 8]);
        released(&cache).await;
        assert_eq!(cache.usage().entries, 0);
        // A body the client drops mid-stream releases its room too.
        let body = reads
            .open(
                &shards,
                (&shard, &holder),
                "k",
                version,
                layout.clone(),
                0..20,
                Some(keep(&cache, version, 20)),
            )
            .await
            .unwrap()
            .unwrap();
        let mut body = s3s::Body::from(body);
        body.frame().await.unwrap().unwrap();
        assert_eq!(cache.usage().reserved, 20);
        drop(body);
        released(&cache).await;
        assert_eq!(cache.usage().entries, 0);
        // A read that cannot reserve room, as the same version already
        // fills, streams all the same.
        let filling = cache.reserve(name("k"), named(version), 20).unwrap();
        let body = reads
            .open(
                &shards,
                (&shard, &holder),
                "k",
                version,
                layout.clone(),
                0..20,
                Some(keep(&cache, version, 20)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(collect(body).await.unwrap(), vec![b'a'; 20]);
        assert_eq!(cache.usage().refused, 1);
        drop(filling);
        released(&cache).await;
        assert_eq!(cache.usage().entries, 0);

        // A layout the holder does not locate: it does not hold the version.
        let gone = vec![ExtentRef {
            position: EpochSeq::new(Epoch::new(9), Seq::new(9)),
            len: 12,
        }];
        let missing = reads
            .open(&shards, (&shard, &holder), "k", version, gone, 0..12, None)
            .await
            .unwrap();
        assert!(missing.is_none());
        assert_eq!(set.reads().counts().not_held, 1);

        // Another node has no replica here.
        let other: NodeId = "node-9".parse().unwrap();
        let refused = reads
            .open(&shards, (&shard, &other), "k", version, layout, 0..12, None)
            .await;
        assert!(
            matches!(refused, Err(ShardError::NotFound(_))),
            "{refused:?}"
        );
    }

    // Real time: the shards' index work runs on a blocking pool, and a
    // paused clock would jump to the next renewal while it waits, lapsing
    // registrations before their first fetch.
    #[tokio::test]
    async fn a_lapsed_registration_fails_the_stream_and_a_renewed_one_does_not() {
        let (shards, shard, version, layout) = overwritten("k").await;
        let set = shards.local().set().clone();
        set.reads().configure(ReadSettings {
            ttl: Duration::from_secs(1),
            ..ReadSettings::default()
        });
        let holder = shards.node().unwrap();
        for (renew_every, lapses) in [
            (Duration::from_secs(60), true),
            (Duration::from_millis(200), false),
        ] {
            let reads = HolderReads::new(renew_every);
            let cache = HotCache::new(1024);
            let body = reads
                .open(
                    &shards,
                    (&shard, &holder),
                    "k",
                    version,
                    layout.clone(),
                    0..20,
                    Some(keep(&cache, version, 20)),
                )
                .await
                .unwrap()
                .unwrap();
            let mut body = s3s::Body::from(body);
            let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(first, Bytes::from(vec![b'a'; 4]));
            // The client stalls for longer than the TTL.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut rest = Vec::new();
            let mut failed = None;
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => rest.extend_from_slice(&frame.into_data().unwrap()),
                    Err(error) => {
                        failed = Some(error.to_string());
                        break;
                    }
                }
            }
            if lapses {
                let failed = failed.expect("the stream fails mid-way");
                assert!(failed.contains("lapsed"), "{failed}");
                // Only what was fetched ahead of the client arrived.
                assert!(
                    rest.len() < 16 && rest.iter().all(|b| *b == b'a'),
                    "{rest:?}"
                );
                // A read that broke off keeps nothing, and releases its
                // room.
                released(&cache).await;
                assert_eq!(cache.usage().entries, 0);
            } else {
                assert_eq!(failed, None);
                assert_eq!(rest, vec![b'a'; 16]);
                released(&cache).await;
                assert_eq!(cache.usage().bytes, 20);
            }
        }
        assert!(set.reads().counts().lapsed >= 1);
    }

    // Real time, as above. Without renewals from the registration on, the
    // first fetch, which takes more than twice the TTL, finds it lapsed.
    #[tokio::test]
    async fn renewals_cover_fetches_slower_than_the_ttl_from_the_first() {
        let (shards, shard, version, layout) = overwritten("k").await;
        let set = shards.local().set().clone();
        set.reads().configure(ReadSettings {
            ttl: Duration::from_millis(500),
            ..ReadSettings::default()
        });
        shards.set_fetch_delay(Duration::from_millis(1200));
        let holder = shards.node().unwrap();
        let reads = HolderReads::new(Duration::from_millis(100));
        let body = reads
            .open(&shards, (&shard, &holder), "k", version, layout, 0..8, None)
            .await
            .expect("the first fetch is served under a renewed registration")
            .unwrap();
        assert_eq!(collect(body).await.unwrap(), vec![b'a'; 8]);
        assert_eq!(set.reads().counts().lapsed, 0);
        // The stream released the read, and renewals stopped with it.
        for _ in 0..100 {
            if set.reads().live() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(set.reads().live(), 0);
    }

    fn extent(seq: u64, len: u32) -> ExtentRef {
        ExtentRef {
            position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
            len,
        }
    }

    #[test]
    fn pieces_clip_the_extents_a_range_overlaps() {
        let layout = [extent(1, 4), extent(2, 4), extent(3, 4)];
        let served = |range: Range<u64>| {
            pieces(&layout, &range).map(|pieces| {
                pieces
                    .into_iter()
                    .map(|piece| (piece.extent.position.seq.get(), piece.bytes))
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(served(0..12), Some(vec![(1, 0..4), (2, 0..4), (3, 0..4)]));
        assert_eq!(served(5..7), Some(vec![(2, 1..3)]));
        assert_eq!(served(3..9), Some(vec![(1, 3..4), (2, 0..4), (3, 0..1)]));
        assert_eq!(served(8..12), Some(vec![(3, 0..4)]));
        assert_eq!(served(4..4), Some(vec![]));
        // A layout shorter than the range does not hold it.
        assert_eq!(served(0..13), None);
        assert_eq!(pieces(&[], &(0..0)), Some(vec![]));
    }

    #[test]
    fn the_local_node_and_the_least_loaded_come_first() {
        let reads = HolderReads::new(Duration::from_secs(1));
        let node = |n: u8| -> NodeId { format!("node-{n}").parse().unwrap() };
        let holders = [node(1), node(2), node(3)];
        assert_eq!(reads.order(&holders, None), holders);
        assert_eq!(
            reads.order(&holders, Some(&node(3))),
            [node(3), node(1), node(2)]
        );
        let busy = Load::start(&reads.load, &node(1));
        let busier = [
            Load::start(&reads.load, &node(2)),
            Load::start(&reads.load, &node(2)),
        ];
        assert_eq!(reads.order(&holders, None), [node(3), node(1), node(2)]);
        // The local node comes first however busy it is.
        assert_eq!(
            reads.order(&holders, Some(&node(2))),
            [node(2), node(3), node(1)]
        );
        drop(busier);
        drop(busy);
        assert!(reads.lock().is_empty());
    }
}
