//! Fragment releases and read holds between nodes, on the intra-cluster
//! transport:
//!
//! ```text
//! shard primary                                   fragment node
//!   FragmentRelease(shard, fragments)          ->
//!                                              <-  FragmentNoted(counts | error)
//!
//! gateway                                         fragment node
//!   FragmentHold(registration, fragments, end) ->
//!                                              <-  FragmentNoted(counts | error)
//! ```
//!
//! Every body is a `prost` message in the frame's payload, since a release
//! of [`MAX_RELEASES`] fragments with long keys exceeds what a frame header
//! holds. Mutual TLS names both ends, so an admin certificate can neither
//! release nor hold fragments, and a hold is the reading node's own. A
//! release uses a connection of its own; a hold travels on the
//! connections the gateway reads fragments on, paired with its answer by
//! the header's request ID ([`FragmentReadClient`](crate::FragmentReadClient)).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_log::record::MAX_KEY_LEN;
use skys3_net::{Frame, Header, MessageKind, Network, Peers, Receiver, Sender, Transport};
use skys3_types::{BucketId, Epoch, EpochSeq, FragmentId, NodeId, Seq, ShardId};

use super::{ReleaseSink, ReleasedFragment};
use crate::FragmentServer;

/// The most fragments one `FragmentRelease` names.
pub const MAX_RELEASES: usize = 256;

/// The most fragments one `FragmentHold` names.
pub const MAX_HOLDS: usize = 4096;

/// How long a primary waits for a node's answer to a release, connecting
/// included. A node that does not answer is told again at the next pass.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(5);

/// The body of a `FragmentRelease` frame.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentRelease {
    /// The shard's bucket ID.
    #[prost(string, tag = "1")]
    pub bucket: String,
    /// The shard's number, at most 255.
    #[prost(uint32, tag = "2")]
    pub shard: u32,
    /// The fragments released, at most [`MAX_RELEASES`].
    #[prost(message, repeated, tag = "3")]
    pub fragments: Vec<ReleasedBody>,
}

/// One fragment of a [`FragmentRelease`].
#[derive(Clone, PartialEq, Message)]
pub struct ReleasedBody {
    /// The fragment's ID, 16 bytes little-endian.
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    /// The object key its header must name.
    #[prost(string, tag = "2")]
    pub key: String,
    /// The epoch of the version its header must name.
    #[prost(uint64, tag = "3")]
    pub version_epoch: u64,
    /// That version's sequence number.
    #[prost(uint64, tag = "4")]
    pub version_seq: u64,
    /// The stripe number its header must name.
    #[prost(uint32, tag = "5")]
    pub stripe: u32,
    /// The fragment index its header must name, below 255.
    #[prost(uint32, tag = "6")]
    pub index: u32,
}

/// The body of a `FragmentHold` frame.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentHold {
    /// The read's registration number, which the reading node chooses.
    #[prost(uint64, tag = "1")]
    pub registration: u64,
    /// The fragments the read may fetch from the node, 16 bytes
    /// little-endian each, at most [`MAX_HOLDS`]: a new hold, or a renewal
    /// that replaces the fragments.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub fragments: Vec<Vec<u8>>,
    /// Whether the read is over and its hold ends.
    #[prost(bool, tag = "3")]
    pub end: bool,
}

/// The body of a `FragmentNoted` frame.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentNoted {
    /// Fragments the node held and noted.
    #[prost(uint32, tag = "1")]
    pub noted: u32,
    /// Fragments of a release that the node does not hold, or holds under
    /// a header naming another object or version.
    #[prost(uint32, tag = "2")]
    pub not_held: u32,
    /// Why the node noted nothing; empty if it did.
    #[prost(string, tag = "3")]
    pub error: String,
}

impl FragmentRelease {
    /// The body that releases `fragments` of `shard`.
    #[must_use]
    pub fn new(shard: &ShardRef, fragments: &[ReleasedFragment]) -> Self {
        Self {
            bucket: shard.bucket.as_str().to_owned(),
            shard: u32::from(shard.shard.get()),
            fragments: fragments
                .iter()
                .map(|fragment| ReleasedBody {
                    id: fragment.fragment.get().to_le_bytes().to_vec(),
                    key: fragment.key.clone(),
                    version_epoch: fragment.version.epoch.get(),
                    version_seq: fragment.version.seq.get(),
                    stripe: fragment.stripe,
                    index: u32::from(fragment.index),
                })
                .collect(),
        }
    }

    /// The shard and the fragments the body names, checked.
    ///
    /// # Errors
    ///
    /// Why the body is malformed.
    pub fn parse(&self) -> Result<(ShardRef, Vec<ReleasedFragment>), String> {
        let bucket = BucketId::new(self.bucket.clone()).map_err(|e| e.to_string())?;
        let shard = u8::try_from(self.shard).map_err(|_| format!("shard {}", self.shard))?;
        if self.fragments.is_empty() || self.fragments.len() > MAX_RELEASES {
            return Err(format!("a release of {} fragments", self.fragments.len()));
        }
        let fragments = self
            .fragments
            .iter()
            .map(|body| {
                if body.key.is_empty() || body.key.len() > MAX_KEY_LEN {
                    return Err(format!("a key of {} bytes", body.key.len()));
                }
                let index = u8::try_from(body.index)
                    .ok()
                    .filter(|&index| index < u8::MAX)
                    .ok_or_else(|| format!("fragment index {}", body.index))?;
                Ok(ReleasedFragment {
                    fragment: fragment_id(&body.id)?,
                    key: body.key.clone(),
                    version: EpochSeq::new(
                        Epoch::new(body.version_epoch),
                        Seq::new(body.version_seq),
                    ),
                    stripe: body.stripe,
                    index,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok((ShardRef::new(bucket, ShardId::new(shard)), fragments))
    }
}

impl FragmentHold {
    /// The body of a hold of read `registration` on `fragments`, or of its
    /// end.
    #[must_use]
    pub fn new(registration: u64, fragments: &[FragmentId], end: bool) -> Self {
        Self {
            registration,
            fragments: fragments
                .iter()
                .map(|id| id.get().to_le_bytes().to_vec())
                .collect(),
            end,
        }
    }

    /// The fragments the body names, checked.
    ///
    /// # Errors
    ///
    /// Why the body is malformed.
    pub fn parse(&self) -> Result<Vec<FragmentId>, String> {
        if self.fragments.len() > MAX_HOLDS {
            return Err(format!("a hold of {} fragments", self.fragments.len()));
        }
        self.fragments.iter().map(|id| fragment_id(id)).collect()
    }
}

impl FragmentNoted {
    /// What a node answers to a request it noted: `noted` fragments, and
    /// `not_held` it does not hold.
    fn of(result: Result<(usize, usize), String>) -> Self {
        match result {
            Ok((noted, not_held)) => Self {
                noted: u32::try_from(noted).unwrap_or(u32::MAX),
                not_held: u32::try_from(not_held).unwrap_or(u32::MAX),
                error: String::new(),
            },
            Err(error) => Self {
                noted: 0,
                not_held: 0,
                error,
            },
        }
    }

    /// The answer's counts, or the node's error.
    ///
    /// # Errors
    ///
    /// The node's reason for noting nothing.
    pub fn parse(&self) -> Result<(usize, usize), String> {
        if self.error.is_empty() {
            Ok((self.noted as usize, self.not_held as usize))
        } else {
            Err(self.error.clone())
        }
    }

    /// The frame that carries this answer to the request `request_id`.
    pub(crate) fn frame(&self, request_id: u64) -> Frame {
        let head = Header::new(MessageKind::FragmentNoted).with_request_id(request_id);
        Frame::new(head, Bytes::from(self.encode_to_vec()))
    }
}

fn fragment_id(bytes: &[u8]) -> Result<FragmentId, String> {
    let id: [u8; 16] = bytes
        .try_into()
        .map_err(|_| format!("a fragment ID of {} bytes", bytes.len()))?;
    Ok(FragmentId::new(u128::from_le_bytes(id)))
}

/// Tells fragment nodes over the transport which fragments their shard
/// primary released, and this node's own [`FragmentServer`] directly.
/// Clones share the client.
pub struct ReleaseClient<N: Network, D: Disk> {
    inner: Arc<ClientInner<N, D>>,
}

struct ClientInner<N: Network, D: Disk> {
    node: NodeId,
    transport: Transport<N>,
    peers: Peers,
    local: Option<FragmentServer<D>>,
}

impl<N: Network, D: Disk> Clone for ReleaseClient<N, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<N: Network, D: Disk> ReleaseClient<N, D> {
    /// The client of node `node`, which reaches the nodes at `peers`
    /// through `transport`, and tells `local`, its own server, if any,
    /// without a connection.
    #[must_use]
    pub fn new(
        node: NodeId,
        transport: Transport<N>,
        peers: impl Into<Peers>,
        local: Option<FragmentServer<D>>,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                node,
                transport,
                peers: peers.into(),
                local,
            }),
        }
    }
}

impl<N: Network, D: Disk> ReleaseSink for ReleaseClient<N, D> {
    async fn release(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> Result<(), String> {
        let inner = &self.inner;
        if *node == inner.node
            && let Some(local) = &inner.local
        {
            return local.release(shard, fragments).await.map(drop);
        }
        let address = inner
            .peers
            .get(node)
            .ok_or_else(|| format!("no address for node {node}"))?;
        let exchange = async {
            let mut connection = inner
                .transport
                .connect(node, &address)
                .await
                .map_err(|e| e.to_string())?;
            for batch in fragments.chunks(MAX_RELEASES) {
                let body = FragmentRelease::new(shard, batch).encode_to_vec();
                let frame = Frame::new(Header::new(MessageKind::FragmentRelease), body);
                connection.send(&frame).await.map_err(|e| e.to_string())?;
                let answer = match connection.recv().await {
                    Ok(Some(frame)) if frame.header.kind == MessageKind::FragmentNoted => frame,
                    Ok(_) => return Err("no answer".to_owned()),
                    Err(error) => return Err(error.to_string()),
                };
                FragmentNoted::decode(answer.payload.as_ref())
                    .map_err(|e| format!("a malformed answer: {e}"))?
                    .parse()?;
            }
            let _ = connection.close().await;
            Ok(())
        };
        tokio::time::timeout(ANSWER_TIMEOUT, exchange)
            .await
            .unwrap_or_else(|_| Err("no answer in time".to_owned()))
    }
}

impl<D: Disk> FragmentServer<D> {
    /// Releases `fragments` of `shard` that one of this node's stores holds
    /// under a header naming the same shard, key, and version: they stay
    /// for the release delay and while reads hold them (§8.7). A fragment
    /// the node does not hold, or holds under another header, such as an
    /// ID reused on a replaced disk, is left alone. Returns how many were
    /// released and how many were not held.
    ///
    /// # Errors
    ///
    /// Why the node releases nothing: it keeps no holds.
    pub async fn release(
        &self,
        shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> Result<(usize, usize), String> {
        let holds = self.holds().ok_or("this node does not release fragments")?;
        let (mut released, mut not_held) = (0, 0);
        for fragment in fragments {
            let id = fragment.fragment;
            let Some(store) = self.stores().iter().find(|store| store.len(id).is_some()) else {
                not_held += 1;
                continue;
            };
            let named = match store.header(id).await {
                Ok(header) => {
                    header.shard == *shard
                        && header.key == fragment.key
                        && header.version == fragment.version
                        && header.stripe.number == fragment.stripe
                        && header.index == fragment.index
                }
                // A damaged header names nothing a release can check;
                // reads find the fragment missing, and repair rebuilds it.
                Err(_) => false,
            };
            match store.len(id) {
                Some(bytes) if named => {
                    holds.release(id, bytes);
                    released += 1;
                }
                _ => not_held += 1,
            }
        }
        Ok((released, not_held))
    }

    /// Registers, renews, or ends the hold of read `registration` of
    /// `reader` on `fragments` (§8.7). Returns how many fragments the hold
    /// names that this node holds, and how many it does not.
    ///
    /// # Errors
    ///
    /// Why the node keeps no hold: it keeps no holds.
    pub fn hold(
        &self,
        reader: &NodeId,
        registration: u64,
        fragments: &[FragmentId],
        end: bool,
    ) -> Result<(usize, usize), String> {
        let holds = self.holds().ok_or("this node keeps no holds")?;
        if end {
            holds.end(reader, registration);
            return Ok((0, 0));
        }
        holds.hold(reader, registration, fragments.iter().copied());
        let held = fragments
            .iter()
            .filter(|&&id| self.stores().iter().any(|store| store.len(id).is_some()))
            .count();
        Ok((held, fragments.len() - held))
    }

    /// The answer to a `FragmentHold` frame from `reader`.
    pub(crate) fn answer_hold(&self, reader: &NodeId, frame: &Frame) -> FragmentNoted {
        let result = FragmentHold::decode(frame.payload.as_ref())
            .map_err(|error| format!("a malformed hold: {error}"))
            .and_then(|body| {
                let fragments = body.parse()?;
                self.hold(reader, body.registration, &fragments, body.end)
            });
        FragmentNoted::of(result)
    }

    /// Serves the releases of one connection, starting with its first
    /// frame `first`, until the connection fails, sends something else, or
    /// stays idle for long.
    pub async fn serve_releases<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut receiver, mut sender) = link;
        let from_node = receiver.peer().node_id().is_some();
        let mut frame = first;
        while frame.header.kind == MessageKind::FragmentRelease {
            let result = if from_node {
                match FragmentRelease::decode(frame.payload.as_ref())
                    .map_err(|error| format!("a malformed release: {error}"))
                    .and_then(|body| body.parse())
                {
                    Ok((shard, fragments)) => self.release(&shard, &fragments).await,
                    Err(reason) => Err(reason),
                }
            } else {
                Err("only nodes release fragments".to_owned())
            };
            let answer = FragmentNoted::of(result).frame(frame.header.request_id);
            if let Err(error) = sender.send(&answer).await {
                tracing::debug!(%error, "a release's answer was not sent");
                return;
            }
            frame = match tokio::time::timeout(ANSWER_TIMEOUT, receiver.recv()).await {
                Ok(Ok(Some(frame))) => frame,
                _ => return,
            };
        }
    }
}
