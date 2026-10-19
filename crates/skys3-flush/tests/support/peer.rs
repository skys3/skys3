//! A SkyS3 destination cluster in memory, for flushes to a native target
//! (§7.8): the real staging and commits of `skys3-peer` and
//! `skys3-gateway` over shards on a simulated disk, reached by a link
//! whose streams are channels. Every message is encoded and decoded on
//! the way, so the protocol's limits apply, and the link can fail as a
//! network does: streams cut after some frames, and answers lost.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_config::Config;
use skys3_flush::{
    BoxFuture, FlushSettings, LinkError, PeerLink, PeerReceive, PeerSend, PeerStream, Target,
};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{BucketLookup, GatewayConfig, PeerCommits, PeerExtents, ShardRef, Shards};
use skys3_index::Entry;
use skys3_peer::{
    Inbound, Message, Outbound, StagedObject, Staging, StagingLimits, StagingService, StreamError,
};
use skys3_sim::SimS3;
use skys3_types::{BucketDocument, BucketId, BucketMode, BucketName, ProposalId, ShardCount};
use tokio::sync::mpsc;

use super::{cluster, settings};

/// The destination bucket, which receives from the tests' cluster.
pub const BUCKET: &str = "archive";

/// How a link fails, decided when a stream opens.
#[derive(Debug, Default)]
pub struct Faults {
    /// Cut the next stream once the destination received this many `DATA`
    /// frames on it.
    pub cut_after_data: Option<usize>,
    /// Lose this many `APPLIED`s, the next ones the destination sends.
    pub lose_applied: usize,
    /// Refuse to open streams.
    pub down: bool,
}

/// What the destination received and answered.
#[derive(Debug, Default)]
pub struct Seen {
    /// Every message the destination received, in order.
    pub received: Vec<Message>,
    /// Every message it sent, lost ones included.
    pub sent: Vec<Message>,
    /// Streams opened.
    pub streams: usize,
}

impl Seen {
    /// How many received messages `count` matches.
    pub fn count(&self, count: impl Fn(&Message) -> bool) -> usize {
        self.received.iter().filter(|m| count(m)).count()
    }
}

/// A destination cluster of one node.
pub struct Destination {
    pub shards: MemoryShards,
    pub bucket: BucketDocument,
    pub staging: Arc<Staging>,
    service: StagingService<PeerExtents<MemoryShards>, PeerCommits<MemoryShards>>,
    faults: Mutex<Faults>,
    seen: Arc<Mutex<Seen>>,
    batches: bool,
}

impl Destination {
    /// A destination whose bucket `archive` receives from the tests'
    /// cluster, in sessions that accept `BATCH` if `batches`.
    pub async fn start(batches: bool) -> Arc<Destination> {
        let text = format!(
            "[cluster]\ncluster_id = \"dest\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [transport]\ntls_cert_file = \"/n.crt\"\ntls_key_file = \"/n.key\"\n\
             tls_ca_file = \"/ca.crt\"\n\
             [buckets.{BUCKET}]\nmode = \"local\"\npeer_source = \"{}\"\n\
             [peering.peers.{}]\nca_file = \"/peer.crt\"\n\
             buckets = [{{ source = \"b-flush\", destination = \"{BUCKET}\" }}]\n",
            cluster(),
            cluster(),
        );
        let config = GatewayConfig::new(&text.parse::<Config>().unwrap());
        let shards = MemoryShards::new().await;
        let bucket = BucketDocument {
            bucket_id: BucketId::new("b-archive").unwrap(),
            name: BucketName::new(BUCKET).unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(1).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p-archive").unwrap(),
        };
        let shard = ShardRef::for_key(&bucket, "any");
        shards.open(&shard, &bucket).await.unwrap();
        let known = bucket.clone();
        let lookup: BucketLookup =
            Arc::new(move |name: &BucketName| (*name == known.name).then(|| known.clone()));
        let staging = Arc::new(Staging::new(StagingLimits {
            quota_bytes: 1 << 30,
            ttl: Duration::from_secs(3600),
        }));
        let service = StagingService::new(
            Arc::clone(&staging),
            PeerExtents::new(shards.clone(), Arc::clone(&lookup)),
        )
        .with_commits(PeerCommits::new(shards.clone(), lookup, &config));
        Arc::new(Destination {
            shards,
            bucket,
            staging,
            service,
            faults: Mutex::default(),
            seen: Arc::default(),
            batches,
        })
    }

    /// How the link fails from now on.
    pub fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the destination received and answered so far.
    pub fn seen(&self) -> MutexGuard<'_, Seen> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The destination's entry of `key`.
    pub async fn entry(&self, key: &str) -> Option<Entry> {
        let shard = ShardRef::for_key(&self.bucket, key);
        self.shards.entry(&shard, key).await.unwrap()
    }

    /// The bytes of the destination's current version of `key`.
    pub async fn bytes(&self, key: &str) -> Option<Bytes> {
        let entry = self.entry(key).await?;
        let object = entry.object?;
        let shard = ShardRef::for_key(&self.bucket, key);
        let mut body = Vec::new();
        match &object.payload {
            skys3_index::Payload::Inline(position) => {
                body.extend_from_slice(&self.shards.payload(&shard, *position).await.unwrap());
            }
            skys3_index::Payload::Extents(extents) => {
                for extent in extents {
                    let bytes = self.shards.payload(&shard, extent.position).await.unwrap();
                    body.extend_from_slice(&bytes);
                }
            }
            other => panic!("unexpected payload {other:?}"),
        }
        Some(Bytes::from(body))
    }

    /// What the destination stages for the write identity `identity`.
    pub fn staged(&self, identity: &str) -> Option<StagedObject> {
        self.staging.staged(&identity.parse().unwrap())
    }

    /// A target on a store the native transport never uses, that flushes
    /// to this destination with `settings` and `DATA` frames of `frame`
    /// bytes.
    pub fn target(self: &Arc<Self>, settings: FlushSettings, frame: u64) -> Arc<Target<SimS3>> {
        let store = Arc::new(super::remote(1, false));
        let target = Target::new(
            store,
            "",
            super::writes(true, true, true),
            cluster(),
            settings,
        )
        .with_peer(
            Arc::new(Link(Arc::clone(self))),
            BucketName::new(BUCKET).unwrap(),
            frame,
            Duration::from_secs(5),
        );
        Arc::new(target)
    }

    /// A native target with the tests' settings.
    pub fn default_target(self: &Arc<Self>, frame: u64) -> Arc<Target<SimS3>> {
        self.target(settings(), frame)
    }
}

/// The source's link to a [`Destination`].
pub struct Link(pub Arc<Destination>);

impl PeerLink for Link {
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>> {
        Box::pin(async move {
            let destination = &self.0;
            let (cut_after_data, down) = {
                let mut faults = destination.faults();
                (faults.cut_after_data.take(), faults.down)
            };
            if down {
                return Err(LinkError::new("the destination is unreachable"));
            }
            destination.seen().streams += 1;
            let (to_destination, from_source) = mpsc::unbounded_channel();
            let (to_source, from_destination) = mpsc::unbounded_channel();
            let cut = Arc::new(AtomicBool::new(false));
            let inbound = ChannelInbound {
                received: from_source,
                sender: ChannelOutbound {
                    sender: Arc::new(Mutex::new(Some(to_source))),
                    cut: Arc::clone(&cut),
                    destination: Arc::clone(destination),
                },
                cut: Arc::clone(&cut),
                cut_after_data,
                data: 0,
                seen: Arc::clone(&destination.seen),
            };
            let service = destination.service.clone();
            tokio::spawn(async move {
                let _ = service.serve(inbound).await;
            });
            Ok(PeerStream {
                sender: Box::new(ChannelSend {
                    sender: Some(to_destination),
                    cut: Arc::clone(&cut),
                }),
                receiver: Box::new(ChannelReceive {
                    received: from_destination,
                    cut,
                }),
                batches: destination.batches,
            })
        })
    }
}

/// A message after the trip over the wire: encoded and decoded again.
fn wire(message: &Message) -> Message {
    let encoded = message.encode().expect("the message is valid");
    let (decoded, used) = Message::decode(&encoded).unwrap().unwrap();
    assert_eq!(used, encoded.len());
    decoded
}

fn cut_error() -> LinkError {
    LinkError::new("the link dropped")
}

struct ChannelSend {
    sender: Option<mpsc::UnboundedSender<Message>>,
    cut: Arc<AtomicBool>,
}

impl PeerSend for ChannelSend {
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>> {
        Box::pin(async move {
            let sender = self
                .sender
                .as_ref()
                .ok_or_else(|| LinkError::new("finished"))?;
            if self.cut.load(Ordering::SeqCst) || sender.send(wire(message)).is_err() {
                return Err(cut_error());
            }
            // Let the destination take it, as a network would.
            tokio::task::yield_now().await;
            Ok(())
        })
    }

    fn finish(&mut self) -> Result<(), LinkError> {
        self.sender.take();
        Ok(())
    }
}

struct ChannelReceive {
    received: mpsc::UnboundedReceiver<Message>,
    cut: Arc<AtomicBool>,
}

impl PeerReceive for ChannelReceive {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<Message>, LinkError>> {
        Box::pin(async move {
            if self.cut.load(Ordering::SeqCst) {
                return Err(cut_error());
            }
            let message = self.received.recv().await;
            if self.cut.load(Ordering::SeqCst) {
                return Err(cut_error());
            }
            Ok(message)
        })
    }
}

struct ChannelInbound {
    received: mpsc::UnboundedReceiver<Message>,
    sender: ChannelOutbound,
    cut: Arc<AtomicBool>,
    cut_after_data: Option<usize>,
    data: usize,
    seen: Arc<Mutex<Seen>>,
}

impl Inbound for ChannelInbound {
    type Sender = ChannelOutbound;

    async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        if self.cut.load(Ordering::SeqCst) {
            return Err(StreamError::Transport("the link dropped".to_owned()));
        }
        let Some(message) = self.received.recv().await else {
            return Ok(None);
        };
        if matches!(message, Message::Data(_)) {
            self.data += 1;
            if self.cut_after_data.is_some_and(|limit| self.data > limit) {
                self.cut.store(true, Ordering::SeqCst);
                return Err(StreamError::Transport("the link dropped".to_owned()));
            }
        }
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .received
            .push(message.clone());
        Ok(Some(message))
    }

    fn sender(&self) -> ChannelOutbound {
        self.sender.clone()
    }
}

#[derive(Clone)]
struct ChannelOutbound {
    sender: Arc<Mutex<Option<mpsc::UnboundedSender<Message>>>>,
    cut: Arc<AtomicBool>,
    destination: Arc<Destination>,
}

impl Outbound for ChannelOutbound {
    async fn send(&self, message: &Message) -> Result<(), StreamError> {
        let message = wire(message);
        self.destination.seen().sent.push(message.clone());
        if matches!(message, Message::Applied(_)) {
            let mut faults = self.destination.faults();
            if faults.lose_applied > 0 {
                faults.lose_applied -= 1;
                return Ok(());
            }
        }
        let sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        match sender.as_ref() {
            Some(sender) if !self.cut.load(Ordering::SeqCst) => {
                let _ = sender.send(message);
                Ok(())
            }
            _ => Err(StreamError::Transport("the link dropped".to_owned())),
        }
    }

    async fn finish(&self) -> Result<(), StreamError> {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Ok(())
    }
}

/// The write identities of the destination's commits that were answered
/// `committed`, in order.
pub fn committed(seen: &Seen) -> Vec<String> {
    seen.sent
        .iter()
        .filter_map(|message| match message {
            Message::Applied(applied)
                if matches!(applied.outcome, skys3_peer::Outcome::Committed { .. }) =>
            {
                Some(applied.identity.to_string())
            }
            _ => None,
        })
        .collect()
}

/// The preconditions of the `COMMIT`s and batch items received, by write
/// identity, in order.
pub fn preconditions(seen: &Seen) -> Vec<(String, skys3_peer::Precondition)> {
    let mut found = Vec::new();
    for message in &seen.received {
        match message {
            Message::Commit(commit) => {
                found.push((commit.identity.to_string(), commit.precondition.clone()));
            }
            Message::Batch(batch) => {
                for item in &batch.items {
                    found.push((item.identity.to_string(), item.precondition.clone()));
                }
            }
            _ => {}
        }
    }
    found
}

/// Stored metadata the destination keeps, without the identity it adds.
pub fn without_identity(mut metadata: BTreeMap<String, String>) -> BTreeMap<String, String> {
    metadata.remove(skys3_log::record::IDENTITY_METADATA);
    metadata
}
