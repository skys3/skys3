//! The native transport between the nodes and the SkyS3 peer (plan
//! M6-08): real QUIC over the simulated network ([`SimUdp`]), with the
//! node binary's endpoint, handshake, and connection pool on the source
//! side, and on the destination side its endpoint's connections served by
//! the staging service.
//!
//! Every stream the destination accepts is tapped. The tap checks what
//! the source sends and what the destination answers, for the audits of
//! the protocol itself, and injects the faults of [`ProtocolFaults`]:
//!
//! - **No durable range sent again.** A `DATA` frame must not repeat a
//!   byte that its stream's `RESUME` reported durable: after a reconnect
//!   the source resends only what the destination lacks. Bytes of `DATA`
//!   and of the ranges each `RESUME` reported are counted.
//! - **Consistent answers.** An `APPLIED` for a write identity that the
//!   key's current version carries says `committed`, with that version's
//!   ETag, however many copies of the `COMMIT` arrive and from whichever
//!   node: a replay after a lost `APPLIED`, or the copies of a deposed
//!   primary and of its successor.
//! - **Staging a live `COMMIT` needs stays.** A `COMMIT` that arrives on a
//!   stream whose `RESUME` and `DATA` covered its whole object is never
//!   answered `incomplete`: expiry must not take staging from under it.
//!
//! What a node sends and what the destination answers are recorded for
//! the audits of [`peer`](crate::peer), and `COMMIT`s on their way can aim
//! faults at their sender ([`AimedBlocks`](crate::AimedBlocks)).

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_flush::{BoxFuture, LinkError, PeerLink, PeerSend, PeerStream, PeerTransport};
use skys3_peer::{
    AbortReason, ApplyError, ByteRanges, CommitSink, ConnectionPool, Destination as PeerAddress,
    ExtentSink, Inbound, InboundSender, InboundStream, Message, Outbound, Outcome, PeerConnection,
    PeerDescriptor, PeerEndpoint, PutData, ShardLease, StagingService, StreamError, Write,
};
use skys3_types::WriteIdentity;

use crate::node::BoxError;
use crate::peer::{Destination, Peer, PeerLog, PeerSide, lock};
use crate::quic::SimUdp;

/// The port of every node's peer endpoint.
const SOURCE_PORT: u16 = 7601;

/// `peer_connections_per_shard` of the nodes.
const CONNECTIONS_PER_SHARD: u32 = 2;

/// How often a node's pool adapts its limits, as the node binary's.
const POOL_ADAPT_INTERVAL: Duration = Duration::from_secs(1);

/// A node's peer endpoint and connection pool, in one of its lives.
pub(crate) struct NodePeering {
    endpoint: PeerEndpoint,
    pool: ConnectionPool,
}

impl NodePeering {
    /// Binds the peer endpoint of the node at `position`, whose draws come
    /// from `seed`, and adapts its pool as the node binary does.
    pub(crate) async fn bind(
        side: &PeerSide,
        position: usize,
        seed: u64,
    ) -> Result<Self, BoxError> {
        let tls = side
            .nodes
            .get(position)
            .ok_or("the node has no peer certificate")?;
        let socket = SimUdp::bind(SOURCE_PORT).await?;
        let endpoint = PeerEndpoint::with_socket(socket, tls, side.settings.clone(), Some(seed))?;
        let pool = ConnectionPool::new(endpoint.clone(), CONNECTIONS_PER_SHARD);
        let adapting = pool.clone();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(POOL_ADAPT_INTERVAL);
            loop {
                ticks.tick().await;
                adapting.adapt();
            }
        });
        Ok(Self { endpoint, pool })
    }

    /// How the flush service of the node at `node` reaches the peer: as
    /// the node binary's, a verified descriptor's first address must
    /// complete a fresh handshake, `HELLO`s included, before the target
    /// uses QUIC through the pool. The node's `COMMIT`s are recorded in
    /// `side`'s log.
    pub(crate) fn transport(&self, peer: Peer, side: &PeerSide, node: usize) -> PeerTransport {
        let (endpoint, pool, log) = (
            self.endpoint.clone(),
            self.pool.clone(),
            Arc::clone(&side.log),
        );
        PeerTransport::new(
            Box::new(move |descriptor: &PeerDescriptor| {
                let (endpoint, pool, log) = (endpoint.clone(), pool.clone(), Arc::clone(&log));
                let cluster = descriptor.cluster.clone();
                let address = descriptor.addresses.first().cloned();
                Box::pin(async move {
                    let address = address.ok_or_else(|| LinkError::new("no address"))?;
                    let destination = PeerAddress {
                        cluster,
                        address: resolve(&address)?,
                    };
                    let probe = endpoint
                        .connect(&destination)
                        .await
                        .map_err(LinkError::new)?;
                    probe.close();
                    Ok(Arc::new(NodeLink {
                        lease: pool.attach(destination),
                        log,
                        node,
                    }) as Arc<dyn PeerLink>)
                })
            }),
            side.verifier.clone(),
            peer.frame_bytes,
        )
        .with_timeout(peer.timeout)
        .with_connect_timeout(peer.connect_timeout)
        .with_reprobe(peer.reprobe.0, peer.reprobe.1)
        .with_quarantine(peer.quarantine())
    }
}

/// The address of `host:port` on the simulated network.
fn resolve(address: &str) -> Result<SocketAddr, LinkError> {
    let (host, port) = address
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse().ok()?)))
        .ok_or_else(|| LinkError::new("not host:port"))?;
    Ok(SocketAddr::new(turmoil::lookup(host), port))
}

/// A node's link to the peer: a lease of its connection pool, whose
/// `COMMIT`s the log records.
struct NodeLink {
    lease: ShardLease,
    log: Arc<PeerLog>,
    node: usize,
}

impl PeerLink for NodeLink {
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>> {
        Box::pin(async move {
            let stream = PeerLink::open(&self.lease).await?;
            Ok(PeerStream {
                sender: Box::new(NodeSend {
                    inner: stream.sender,
                    log: Arc::clone(&self.log),
                    node: self.node,
                }),
                ..stream
            })
        })
    }
}

struct NodeSend {
    inner: Box<dyn PeerSend>,
    log: Arc<PeerLog>,
    node: usize,
}

impl PeerSend for NodeSend {
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>> {
        if let Message::Commit(commit) = message {
            self.log
                .commit_sent(commit.identity.to_string(), commit.key.clone(), self.node);
        }
        self.inner.send(message)
    }

    fn finish(&mut self) -> Result<(), LinkError> {
        self.inner.finish()
    }
}

/// Faults the destination injects into the native protocol itself, each
/// drawn per message from the run's seed. Rates are per mille.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProtocolFaults {
    /// Each message is lost with this probability: a source's `BEGIN`,
    /// `DATA`, `COMMIT`, or `BATCH` is never acted on, and the
    /// destination's `RESUME`, `DURABLE`, or `APPLIED` never reaches the
    /// source. The stream goes on: a lost `DATA` leaves a hole that the
    /// `COMMIT` finds `incomplete`, a lost answer leaves the source
    /// waiting until its stream ends or times out.
    pub loss_per_mille: u16,
    /// The destination drops the whole connection with this probability:
    /// as a `DATA` frame other than its stream's first arrives, right
    /// after it sends a `DURABLE`, and right after it sends an `APPLIED`,
    /// which may then be lost with it. Its source reconnects.
    pub drop_per_mille: u16,
}

/// What the tap does with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Passes it on.
    Pass,
    /// Loses it.
    Lose,
    /// Passes an answer on, then drops the connection; drops the
    /// connection before an arriving message is acted on.
    Drop,
}

/// Accepts the sources' connections on the destination's endpoint, bound
/// for its `life`, and serves each of their streams with `service`, tapped.
pub(crate) async fn serve<S: ExtentSink, C: CommitSink>(
    destination: Arc<Destination>,
    service: StagingService<S, C>,
    life: u64,
) -> Result<(), BoxError> {
    let socket = SimUdp::bind(crate::peer::PORT).await?;
    let endpoint = PeerEndpoint::with_socket(
        socket,
        destination.tls(),
        destination.settings().clone(),
        Some(destination.seed() ^ life),
    )?;
    while let Some(incoming) = endpoint.accept().await {
        let (destination, service) = (Arc::clone(&destination), service.clone());
        tokio::spawn(async move {
            let connection = match incoming.establish().await {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::debug!(%error, "a peer connection failed");
                    return;
                }
            };
            loop {
                match connection.accept_stream().await {
                    Ok(stream) => {
                        let tapped = Tapped::new(stream, connection.clone(), &destination);
                        let service = service.clone();
                        tokio::spawn(async move {
                            if let Err(error) = service.serve(tapped).await {
                                tracing::debug!(%error, "a peer stream failed");
                            }
                        });
                    }
                    Err(StreamError::ZeroRtt) => {}
                    Err(_) => return,
                }
            }
        });
    }
    Ok(())
}

/// A stream the destination accepted, tapped.
struct Tapped {
    stream: InboundStream,
    sender: TappedSender,
}

impl Tapped {
    fn new(
        stream: InboundStream,
        connection: PeerConnection,
        destination: &Arc<Destination>,
    ) -> Self {
        let tap = Arc::new(Tap {
            destination: Arc::clone(destination),
            node: connection.peer_node().to_string(),
            connection,
            state: Mutex::default(),
        });
        let sender = TappedSender {
            inner: stream.sender(),
            tap,
        };
        Self { stream, sender }
    }
}

impl Inbound for Tapped {
    type Sender = TappedSender;

    async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        loop {
            let Some(message) = self.stream.recv().await? else {
                return Ok(None);
            };
            match self.sender.tap.arrived(&message) {
                Verdict::Pass => return Ok(Some(message)),
                Verdict::Lose => {}
                Verdict::Drop => {
                    self.sender.tap.connection.close();
                    return Err(StreamError::Transport(
                        "the destination dropped the connection".to_owned(),
                    ));
                }
            }
        }
    }

    fn sender(&self) -> TappedSender {
        self.sender.clone()
    }
}

/// The destination's answers on a tapped stream.
#[derive(Clone)]
struct TappedSender {
    inner: InboundSender,
    tap: Arc<Tap>,
}

impl Outbound for TappedSender {
    async fn send(&self, message: &Message) -> Result<(), StreamError> {
        match self.tap.answering(message) {
            Verdict::Lose => Ok(()),
            Verdict::Pass => self.inner.send(message).await,
            Verdict::Drop => {
                let sent = self.inner.send(message).await;
                self.tap.connection.close();
                sent
            }
        }
    }

    async fn finish(&self) -> Result<(), StreamError> {
        self.inner.finish().await
    }
}

/// What the tap knows of one stream.
struct Tap {
    destination: Arc<Destination>,
    connection: PeerConnection,
    /// The source node, as its certificate names it.
    node: String,
    state: Mutex<TapState>,
}

#[derive(Default)]
struct TapState {
    /// The identity of the stream's `BEGIN`, and what its `RESUME` said
    /// is durable, by piece.
    begun: Option<WriteIdentity>,
    told: BTreeMap<u64, ByteRanges>,
    /// What the stream's `RESUME` and `DATA` covered, by piece.
    covered: BTreeMap<u64, ByteRanges>,
    /// `DATA` frames that arrived.
    frames: u64,
    /// The key of each identity the stream commits.
    keys: BTreeMap<WriteIdentity, String>,
    /// Identities whose `COMMIT` arrived with its whole object covered.
    whole: BTreeSet<WriteIdentity>,
}

impl Tap {
    /// Checks a message that arrived from the source.
    fn arrived(&self, message: &Message) -> Verdict {
        let destination = &*self.destination;
        let faults = destination.faults();
        let mut state = lock(&self.state);
        if let Message::Data(_) = message
            && state.frames > 0
            && destination.draw(faults.drop_per_mille)
        {
            destination.counts().dropped();
            return Verdict::Drop;
        }
        let losable = matches!(
            message,
            Message::Begin(_) | Message::Data(_) | Message::Commit(_) | Message::Batch(_)
        );
        if losable && destination.draw(faults.loss_per_mille) {
            destination.counts().lost();
            return Verdict::Lose;
        }
        match message {
            Message::Begin(begin) => {
                state.begun = Some(begin.identity.clone());
                state.told.clear();
                state.covered.clear();
            }
            Message::Data(data) => {
                state.frames += 1;
                let range = data.offset..data.offset + data.bytes.len() as u64;
                let resent = overlap(state.told.get(&data.piece), &range);
                state
                    .covered
                    .entry(data.piece)
                    .or_default()
                    .insert(range.clone());
                destination.counts().data(range.end - range.start);
                if resent > 0 {
                    let identity = state.begun.as_ref().map(ToString::to_string);
                    destination.violate(
                        identity.clone().unwrap_or_default(),
                        format!(
                            "a DATA frame of {identity:?} at {}..{} of piece {} resent {resent} \
                             bytes that its stream's RESUME reported durable",
                            range.start, range.end, data.piece
                        ),
                    );
                }
            }
            Message::Commit(commit) => {
                destination.commit_arrived(&commit.identity, &commit.key, &self.node);
                state
                    .keys
                    .insert(commit.identity.clone(), commit.key.clone());
                if let Write::Put(put) = &commit.write
                    && let PutData::Staged { piece } = put.data
                    && state.begun.as_ref() == Some(&commit.identity)
                    && covers(state.covered.get(&piece), put.size)
                {
                    state.whole.insert(commit.identity.clone());
                }
            }
            Message::Batch(batch) => {
                for item in &batch.items {
                    destination.commit_arrived(&item.identity, &item.key, &self.node);
                    state.keys.insert(item.identity.clone(), item.key.clone());
                }
            }
            Message::Abort(abort) => destination.aborted(&abort.identity),
            _ => {}
        }
        Verdict::Pass
    }

    /// Checks an answer the destination is about to send.
    fn answering(&self, message: &Message) -> Verdict {
        let destination = &*self.destination;
        let faults = destination.faults();
        let mut state = lock(&self.state);
        match message {
            Message::Applied(applied) => {
                let key = state.keys.get(&applied.identity).cloned();
                destination.answered(applied, key.as_deref());
                if matches!(
                    applied.outcome,
                    Outcome::Failed {
                        error: ApplyError::Incomplete,
                        ..
                    }
                ) {
                    destination.counts().incomplete();
                    if state.whole.contains(&applied.identity)
                        && !destination.was_aborted(&applied.identity)
                    {
                        destination.violate(
                            applied.identity.to_string(),
                            format!(
                                "the COMMIT of {} arrived on a stream whose RESUME and DATA \
                                 covered the whole object, yet it was answered incomplete: \
                                 the staging went while a COMMIT still needed it",
                                applied.identity
                            ),
                        );
                    }
                }
            }
            Message::Resume(resume) => {
                if destination.draw(faults.loss_per_mille) {
                    destination.counts().lost();
                    return Verdict::Lose;
                }
                let bytes: u64 = resume.pieces.values().map(length).sum();
                destination.counts().resumed(bytes);
                state.told = resume.pieces.clone();
                state.covered = resume.pieces.clone();
                return Verdict::Pass;
            }
            Message::Abort(abort) if abort.reason == AbortReason::Expired => {
                destination.counts().expired();
                return Verdict::Pass;
            }
            Message::Durable(_) => {}
            _ => return Verdict::Pass,
        }
        drop(state);
        if destination.draw(faults.loss_per_mille) {
            destination.counts().lost();
            Verdict::Lose
        } else if destination.draw(faults.drop_per_mille) {
            destination.counts().dropped();
            Verdict::Drop
        } else {
            Verdict::Pass
        }
    }
}

/// How many bytes of `range` `ranges` hold.
fn overlap(ranges: Option<&ByteRanges>, range: &Range<u64>) -> u64 {
    ranges
        .into_iter()
        .flat_map(ByteRanges::as_slice)
        .map(|held| {
            held.end
                .min(range.end)
                .saturating_sub(held.start.max(range.start))
        })
        .sum()
}

/// Whether `ranges` hold every byte of `0..size`.
fn covers(ranges: Option<&ByteRanges>, size: u64) -> bool {
    size == 0 || overlap(ranges, &(0..size)) == size
}

/// How many bytes `ranges` hold.
fn length(ranges: &ByteRanges) -> u64 {
    ranges
        .as_slice()
        .iter()
        .map(|range| range.end - range.start)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(list: &[Range<u64>]) -> ByteRanges {
        let mut ranges = ByteRanges::default();
        for range in list {
            ranges.insert(range.clone());
        }
        ranges
    }

    #[test]
    fn overlaps_count_the_bytes_held() {
        let held = ranges(&[0..10, 20..30]);
        assert_eq!(overlap(Some(&held), &(5..25)), 10);
        assert_eq!(overlap(Some(&held), &(10..20)), 0);
        assert_eq!(overlap(None, &(0..5)), 0);
        assert_eq!(length(&held), 20);
        assert!(covers(Some(&ranges(&[0..10, 10..12])), 12));
        assert!(!covers(Some(&held), 30));
        assert!(covers(None, 0));
    }
}
