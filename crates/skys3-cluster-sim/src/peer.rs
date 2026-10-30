//! A SkyS3 peer that the cluster's buckets flush to over the native peer
//! protocol (§7.8, plan M6-06).
//!
//! With [`ClusterConfig::peer`](crate::ClusterConfig::peer), the targets
//! of the `write_back` buckets and the backup targets of the `local` ones
//! name the bucket [`BUCKET`] of a destination cluster of one node, the
//! host [`HOST`], with `target_transport = "native"`. The destination runs
//! the real thing on a simulated disk: its shards and index, recovered in
//! each life, and the staging service, whose `COMMIT`s apply through the
//! gateway's peer commits. The nodes reach it over TCP on the simulated
//! network, one connection per stream, so that partitions, holds, message
//! loss, and restarts of the peer ([`Fault::PeerRestart`]) cut streams in
//! the middle.
//!
//! The audits:
//!
//! - **`FLUSHED` only after `APPLIED`.** Each `FLUSHED` record a flusher
//!   decides on for a native target names the write identity of the
//!   `COMMIT` whose `APPLIED` it follows; the destination must have
//!   answered that identity `committed`, or named it as the key's current
//!   identity, before (`PeerBug::FlushedOnCommit` breaks this).
//! - **At each acknowledgement**, with `ack_policy` or `backup_ack =
//!   "write_through"`: the destination holds the write, as the remote
//!   store does without a peer (`PeerBug::AnsweredOnCommit` breaks this).
//! - **At the end**, after a power loss of the destination too: it holds
//!   exactly what every member holds of each key, and no write identity in
//!   more than one of its `PUT` records: each write applied once.
//!
//! [`Fault::PeerRestart`]: crate::Fault::PeerRestart

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_flush::{
    BoxFuture, FlushedRecord, LinkError, PeerBug, PeerLink, PeerReceive, PeerSend, PeerStream,
    PeerTransport,
};
use skys3_gateway::{BucketLookup, GatewayConfig, PeerCommits, PeerExtents, ShardRef, Shards};
use skys3_index::{Index, IndexConfig, ListItem, ListQuery};
use skys3_io::{BlockingPool, Drift, MonoTime, SimDisk};
use skys3_log::record::IDENTITY_METADATA;
use skys3_log::{LogConfig, RecordBody};
use skys3_peer::{
    Inbound, Message, Outbound, Outcome, PREFIX_LEN, Staging, StagingLimits, StagingService,
    StreamError, parse_prefix,
};
use skys3_sim::NodeClock;
use skys3_sim::check::Violation;
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Label, NodeId, ProposalId,
    ShardCount,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use turmoil::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use turmoil::net::{TcpListener, TcpStream};

use crate::node::{BoxError, INDEX_FILE};

/// The destination's host.
pub(crate) const HOST: &str = "peer";
/// The port its peer service listens on.
const PORT: u16 = 7600;
/// The endpoint that native targets name.
pub(crate) const ENDPOINT: &str = "https://peer.sim.internal";
/// The destination bucket, which every source bucket flushes to under a
/// key prefix of its own, as it would to the remote store.
pub(crate) const BUCKET: &str = "peer-sink";
/// The destination cluster.
const CLUSTER: &str = "peer-sim";

/// A SkyS3 peer that the buckets flush to over the native protocol, and
/// the seeded bug the flushers run, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Peer {
    /// `peer_frame_bytes`: the size of `DATA` frames, and of the largest
    /// object a `BATCH` carries.
    pub frame_bytes: u64,
    /// How long a stream may make no progress before its flush fails.
    pub timeout: Duration,
    /// `peer_staging_ttl_seconds` at the destination.
    pub staging_ttl: Duration,
    /// The latency of every message between a node and the peer, each
    /// way: another cluster is further away than the nodes are from each
    /// other.
    pub latency: Duration,
    /// A seeded bug of the flushers' native transport.
    pub bug: PeerBug,
}

impl Default for Peer {
    /// Frames of 256 bytes, so that the workload's bodies go in batches
    /// and in several frames, short timeouts, and 20 ms each way.
    fn default() -> Self {
        Self {
            frame_bytes: 256,
            timeout: Duration::from_secs(2),
            staging_ttl: Duration::from_secs(5),
            latency: Duration::from_millis(20),
            bug: PeerBug::None,
        }
    }
}

impl Peer {
    /// How the nodes' flush services reach the peer.
    pub(crate) fn transport(self) -> PeerTransport {
        PeerTransport::new(
            Box::new(|_| Some(Arc::new(TcpLink) as Arc<dyn PeerLink>)),
            self.frame_bytes,
        )
        .with_timeout(self.timeout)
    }
}

/// What the run found at the peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerAudit {
    /// `FLUSHED` records checked against the destination's answers.
    pub flushed: u64,
    /// The destination's lives.
    pub lives: u64,
    /// Objects the destination holds at the end.
    pub objects: usize,
    /// `PUT` records in the destination's log at the end.
    pub writes: usize,
}

/// What the destination holds at the end, by key, and what the run found.
pub(crate) struct Finished {
    pub objects: BTreeMap<String, String>,
    pub audit: PeerAudit,
}

/// What the destination holds now: read from its index while it runs,
/// and what it held when it last stopped while it is down.
#[derive(Default)]
struct View {
    index: Option<Arc<Index>>,
    frozen: BTreeMap<String, String>,
}

/// The destination cluster, over every life of its node.
pub(crate) struct Destination {
    node: NodeId,
    disk: SimDisk,
    document: BucketDocument,
    /// The configuration its commits apply under.
    config: skys3_config::Config,
    log: LogConfig,
    index: IndexConfig,
    checkpoints: Duration,
    limits: StagingLimits,
    view: Mutex<View>,
    /// The write identities the destination answered `committed`, or
    /// named as a key's current one.
    applied: Mutex<BTreeSet<String>>,
    violation: Mutex<Option<Violation>>,
    flushed: AtomicU64,
    lives: AtomicU64,
}

impl Destination {
    /// A destination on `disk` that receives from `source`, with the log
    /// and index settings of the nodes.
    pub(crate) fn new(
        disk: SimDisk,
        source: &ClusterId,
        peer: Peer,
        log: LogConfig,
        index: IndexConfig,
        checkpoints: Duration,
    ) -> Result<Self, BoxError> {
        let text = format!(
            "[cluster]\ncluster_id = \"{CLUSTER}\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.sim.internal:2379\"]\n\
             [transport]\ntls_cert_file = \"/n.crt\"\ntls_key_file = \"/n.key\"\n\
             tls_ca_file = \"/ca.crt\"\n\
             [buckets.{BUCKET}]\nmode = \"local\"\npeer_source = \"{source}\"\n\
             [peering.peers.{source}]\nca_file = \"/peer.crt\"\n\
             buckets = [{{ source = \"b-sim0\", destination = \"{BUCKET}\" }}]\n"
        );
        Ok(Self {
            node: NodeId::new("peer-1")?,
            disk,
            document: BucketDocument {
                bucket_id: BucketId::new("b-peer")?,
                name: BucketName::new(BUCKET)?,
                mode: BucketMode::Local,
                shards: ShardCount::new(2)?,
                replicas: 1,
                min_write_replicas: 1,
                clean_copies: 1,
                target: None,
                created_unix_ms: 0,
                lifecycle: None,
                proposal_id: ProposalId::new("p-peer")?,
            },
            config: text.parse()?,
            log,
            index,
            checkpoints,
            limits: StagingLimits {
                quota_bytes: 1 << 30,
                ttl: peer.staging_ttl,
            },
            view: Mutex::default(),
            applied: Mutex::default(),
            violation: Mutex::default(),
            flushed: AtomicU64::new(0),
            lives: AtomicU64::new(0),
        })
    }

    /// Stops the destination's disk: a power loss drops what was not
    /// synced. What it held is kept for the audits while it is down.
    pub(crate) fn stop(&self, power_loss: bool) {
        let mut view = lock(&self.view);
        if let Some(index) = view.index.take() {
            match self.objects(&index) {
                Ok(objects) => view.frozen = objects,
                Err(error) => tracing::warn!(%error, "the peer's objects could not be read"),
            }
        }
        if power_loss {
            self.disk.crash();
        } else {
            self.disk.kill();
        }
    }

    /// The value the destination holds for `key` of its bucket: the ETag
    /// of its object, or `None`.
    pub(crate) fn value(&self, key: &str) -> Option<String> {
        let view = lock(&self.view);
        if let Some(index) = &view.index {
            let shard = ShardRef::for_key(&self.document, key);
            let read = index
                .read()
                .and_then(|read| read.entry(&(&shard).into(), key));
            match read {
                Ok(entry) => {
                    return entry
                        .and_then(|entry| entry.object)
                        .map(|object| object.local_etag.to_string());
                }
                Err(error) => tracing::warn!(%error, "the peer's index could not be read"),
            }
        }
        view.frozen.get(key).cloned()
    }

    /// Every object of the destination bucket in `index`, by key.
    fn objects(&self, index: &Index) -> Result<BTreeMap<String, String>, BoxError> {
        let read = index.read()?;
        let mut objects = BTreeMap::new();
        for shard in ShardRef::all(&self.document) {
            let mut query = ListQuery {
                max_items: 256,
                ..ListQuery::default()
            };
            loop {
                let page = read.list(&(&shard).into(), &query)?;
                for item in page.items {
                    if let ListItem::Object { key, object } = item {
                        query.start_after = Some(key.clone());
                        objects.insert(key, object.local_etag.to_string());
                    }
                }
                if !page.truncated {
                    break;
                }
            }
        }
        Ok(objects)
    }

    /// Checks a `FLUSHED` record a flusher decided on: the destination
    /// must have applied the identity it names.
    fn flushed(&self, record: &FlushedRecord) {
        self.flushed.fetch_add(1, Ordering::Relaxed);
        if lock(&self.applied).contains(&record.identity) {
            return;
        }
        let what = if record.delete { "delete" } else { "version" };
        lock(&self.violation).get_or_insert(Violation {
            key: format!("{}/{}", record.shard.bucket, record.key),
            reason: format!(
                "the {what} at {} is recorded flushed as {}, which the peer never applied",
                record.version, record.identity
            ),
            operations: Vec::new(),
        });
    }

    /// Calls [`Destination::flushed`] for each `FLUSHED` record a flusher
    /// on this thread decides on, until the next call.
    pub(crate) fn observe(self: &Arc<Self>) {
        let destination = Arc::clone(self);
        skys3_flush::test_hooks::observe_flushed(Some(Box::new(move |record| {
            destination.flushed(record);
        })));
    }

    /// The first violation the audits found while the run went on.
    pub(crate) fn violation(&self) -> Option<Violation> {
        lock(&self.violation).take()
    }

    /// Recovers the destination from its disk, outside the simulation,
    /// and returns what it holds, by key, with what the run found: the
    /// `PUT` records in its log, each of a write identity of its own, or
    /// the violation if one repeats.
    pub(crate) fn finish(&self) -> Result<Result<Finished, Violation>, BoxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let storage = runtime.block_on(self.recover())?;
        let objects = self.objects(&storage.index)?;
        let mut writes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, log) in storage.shards.set().logs() {
            for mut scanner in log.scan_all()? {
                while let Some(scanned) = runtime.block_on(scanner.next())? {
                    let record = scanned.decode()?;
                    if let RecordBody::Put(put) = record.body
                        && let Some(identity) = put.metadata.get(IDENTITY_METADATA)
                    {
                        writes
                            .entry(identity.clone())
                            .or_default()
                            .push(format!("PUT {} at {}", put.key, record.position));
                    }
                }
            }
        }
        if let Some((identity, puts)) = writes.iter().find(|(_, puts)| puts.len() > 1) {
            return Ok(Err(Violation {
                key: identity.clone(),
                reason: format!("the peer applied {identity} {} times", puts.len()),
                operations: puts.clone(),
            }));
        }
        let audit = PeerAudit {
            flushed: self.flushed.load(Ordering::Relaxed),
            lives: self.lives.load(Ordering::Relaxed),
            objects: objects.len(),
            writes: writes.len(),
        };
        Ok(Ok(Finished { objects, audit }))
    }

    /// Recovers the destination's storage from its disk.
    async fn recover(&self) -> Result<skys3::storage::Storage<skys3_io::SimMount>, BoxError> {
        let mount = self.disk.mount();
        let index = Index::open_sim(&mount, INDEX_FILE, &self.index)?;
        let clock = NodeClock {
            drift: Drift::NONE,
            start: MonoTime::from_nanos(0),
        };
        Ok(skys3::storage::recover(
            vec![(Label::new("disk-0")?, mount)],
            self.log.clone(),
            Arc::new(clock.start()),
            Arc::new(index),
            BlockingPool::inline("peer"),
            self.node.clone(),
        )
        .await?)
    }
}

/// Runs one life of the destination until it crashes.
pub(crate) async fn run(destination: Arc<Destination>) -> Result<(), BoxError> {
    destination.lives.fetch_add(1, Ordering::Relaxed);
    let storage = destination.recover().await?;
    let document = destination.document.clone();
    for shard in ShardRef::all(&document) {
        storage.shards.open(&shard, &document).await?;
    }
    let (checkpointer, interval) = (Arc::clone(&storage.checkpointer), destination.checkpoints);
    tokio::spawn(async move { checkpointer.run(interval).await });
    *lock(&destination.view) = View {
        index: Some(Arc::clone(&storage.index)),
        frozen: BTreeMap::new(),
    };
    let lookup: BucketLookup =
        Arc::new(move |name: &BucketName| (*name == document.name).then(|| document.clone()));
    let mut gateway = GatewayConfig::new(&destination.config);
    gateway.inline_max_bytes = 512;
    gateway.extent_bytes = 512;
    let service = StagingService::new(
        Arc::new(Staging::new(destination.limits)),
        PeerExtents::new(storage.shards.clone(), Arc::clone(&lookup)),
    )
    .with_commits(PeerCommits::new(storage.shards.clone(), lookup, &gateway));
    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let (read, write) = stream.into_split();
        let inbound = TcpInbound {
            read,
            sender: TcpOutbound {
                write: Arc::new(tokio::sync::Mutex::new(Some(write))),
                destination: Arc::clone(&destination),
            },
        };
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(error) = service.serve(inbound).await {
                tracing::debug!(%error, "a peer stream failed");
            }
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads the next frame from `read`, or `None` at the end of the stream.
async fn read_frame<R: AsyncRead + Unpin>(read: &mut R) -> io::Result<Option<Message>> {
    let mut prefix = [0; PREFIX_LEN];
    let mut filled = 0;
    while filled < PREFIX_LEN {
        match read.read(&mut prefix[filled..]).await? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            n => filled += n,
        }
    }
    let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
    let (header_len, payload_len) = parse_prefix(&prefix).map_err(invalid)?;
    let mut header = vec![0; header_len];
    read.read_exact(&mut header).await?;
    let mut payload = vec![0; payload_len];
    read.read_exact(&mut payload).await?;
    Message::decode_parts(&header, Bytes::from(payload))
        .map(Some)
        .map_err(invalid)
}

/// Writes `message` as one frame to `write`.
async fn write_frame(write: &mut OwnedWriteHalf, message: &Message) -> io::Result<()> {
    let frame = message
        .encode()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    write.write_all(&frame).await
}

/// A node's link to the peer: a TCP connection per stream.
struct TcpLink;

impl PeerLink for TcpLink {
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>> {
        Box::pin(async {
            let stream = TcpStream::connect((HOST, PORT)).await.map_err(link)?;
            let (read, write) = stream.into_split();
            Ok(PeerStream {
                sender: Box::new(TcpSend(Some(write))),
                receiver: Box::new(TcpReceive(read)),
                batches: true,
            })
        })
    }
}

fn link(error: io::Error) -> LinkError {
    LinkError::new(error.to_string())
}

struct TcpSend(Option<OwnedWriteHalf>);

impl PeerSend for TcpSend {
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>> {
        Box::pin(async move {
            let write = self
                .0
                .as_mut()
                .ok_or_else(|| LinkError::new("the stream is finished"))?;
            write_frame(write, message).await.map_err(link)
        })
    }

    fn finish(&mut self) -> Result<(), LinkError> {
        // Dropping the write half ends the stream.
        self.0.take();
        Ok(())
    }
}

struct TcpReceive(OwnedReadHalf);

impl PeerReceive for TcpReceive {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<Message>, LinkError>> {
        Box::pin(async { read_frame(&mut self.0).await.map_err(link) })
    }
}

/// A stream the destination accepted.
struct TcpInbound {
    read: OwnedReadHalf,
    sender: TcpOutbound,
}

impl Inbound for TcpInbound {
    type Sender = TcpOutbound;

    async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        read_frame(&mut self.read)
            .await
            .map_err(|error| StreamError::Transport(error.to_string()))
    }

    fn sender(&self) -> TcpOutbound {
        self.sender.clone()
    }
}

/// The destination's side of a stream it answers on, which notes what it
/// answers for the audits.
#[derive(Clone)]
struct TcpOutbound {
    write: Arc<tokio::sync::Mutex<Option<OwnedWriteHalf>>>,
    destination: Arc<Destination>,
}

impl Outbound for TcpOutbound {
    async fn send(&self, message: &Message) -> Result<(), StreamError> {
        if let Message::Applied(applied) = message {
            let mut known = lock(&self.destination.applied);
            match &applied.outcome {
                Outcome::Committed { .. } => {
                    known.insert(applied.identity.to_string());
                }
                Outcome::PreconditionFailed {
                    current: Some(current),
                } => {
                    known.insert(current.to_string());
                }
                _ => {}
            }
        }
        let mut write = self.write.lock().await;
        let write = write
            .as_mut()
            .ok_or_else(|| StreamError::Transport("the stream is finished".to_owned()))?;
        write_frame(write, message)
            .await
            .map_err(|error| StreamError::Transport(error.to_string()))
    }

    async fn finish(&self) -> Result<(), StreamError> {
        self.write.lock().await.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_read_back_whole_or_not_at_all() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let message = Message::Abort(skys3_peer::Abort {
            identity: "src/b-1/0/1.2".parse().unwrap(),
            reason: skys3_peer::AbortReason::Cancelled,
            detail: "gone".to_owned(),
        });
        let frame = message.encode().unwrap();
        runtime.block_on(async {
            let mut whole = &frame[..];
            assert_eq!(read_frame(&mut whole).await.unwrap(), Some(message));
            assert_eq!(read_frame(&mut whole).await.unwrap(), None);
            let mut cut = &frame[..3];
            assert!(read_frame(&mut cut).await.is_err());
            let mut garbage = &[0xff; 16][..];
            assert!(read_frame(&mut garbage).await.is_err());
        });
    }
}
