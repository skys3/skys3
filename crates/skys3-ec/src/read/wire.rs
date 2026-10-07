//! Fragment reads between nodes, on the intra-cluster transport:
//!
//! ```text
//! gateway                                            fragment node
//!   FragmentRead(fragment, identity, range)      ->
//!                                                <-  FragmentData(crc32c | failure) + bytes
//!   FragmentRead ...                             ->
//! ```
//!
//! The request is a `prost` body in the frame header; the answer's body
//! carries the CRC32C of the bytes, which are its payload, or why the node
//! does not serve them. Requests and answers pair by the header's request
//! ID. A connection carries one read at a time, and the client keeps
//! connections open for its next reads of the node. Mutual TLS names
//! both ends, as for fragment writes, so an admin certificate cannot read
//! fragments.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_log::record::MAX_KEY_LEN;
use skys3_net::{Connection, Frame, Header, MessageKind, Network, Receiver, Sender, Transport};
use skys3_types::{
    BucketId, CodecId, Epoch, EpochSeq, FragmentId, Geometry, NodeAddress, NodeId, Seq, ShardId,
};
use tokio::time::Instant;

use super::{
    FragmentBytes, FragmentIdentity, FragmentReadError, FragmentRequest, FragmentSource, ReadFuture,
};
use crate::fragment::StripeInfo;
use crate::{FragmentError, FragmentServer, FragmentStore};

/// The most bytes one fragment read asks for.
pub const MAX_READ_LEN: u64 = 8 << 20;

/// How long the client waits for a connection to a node.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How long the client waits for a node's answer to a read.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a connection stays open with no read on it, at either end.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How many idle connections to each node the client keeps.
const IDLE_PER_NODE: usize = 4;

/// What a [`FragmentData`] answer's `failure` field says.
const SERVED: u32 = 0;
const NOT_HELD: u32 = 1;
const DAMAGED: u32 = 2;

/// The body of a `FragmentRead` frame.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentRead {
    /// The fragment's ID, 16 bytes little-endian.
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    /// The bucket ID of the object's shard.
    #[prost(string, tag = "2")]
    pub bucket: String,
    /// The shard's number, at most 255.
    #[prost(uint32, tag = "3")]
    pub shard: u32,
    /// The object's key.
    #[prost(string, tag = "4")]
    pub key: String,
    /// The epoch of the record that committed the version.
    #[prost(uint64, tag = "5")]
    pub version_epoch: u64,
    /// That record's sequence number.
    #[prost(uint64, tag = "6")]
    pub version_seq: u64,
    /// The stripe's number.
    #[prost(uint32, tag = "7")]
    pub stripe: u32,
    /// How many stripes the object has.
    #[prost(uint32, tag = "8")]
    pub stripes: u32,
    /// Where the stripe's data starts in the object.
    #[prost(uint64, tag = "9")]
    pub offset: u64,
    /// The stripe's data length.
    #[prost(uint64, tag = "10")]
    pub data_len: u64,
    /// `k`.
    #[prost(uint32, tag = "11")]
    pub data_fragments: u32,
    /// `m`.
    #[prost(uint32, tag = "12")]
    pub parity_fragments: u32,
    /// The stripe's codec ID.
    #[prost(uint32, tag = "13")]
    pub codec: u32,
    /// The fragment's index within the stripe.
    #[prost(uint32, tag = "14")]
    pub index: u32,
    /// The first byte of the fragment to read.
    #[prost(uint64, tag = "15")]
    pub start: u64,
    /// The byte after the last one to read.
    #[prost(uint64, tag = "16")]
    pub end: u64,
}

/// The body of a `FragmentData` frame, whose payload holds the bytes read.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentData {
    /// The CRC32C of the payload, computed from the bytes the node
    /// verified against the fragment's block checksums.
    #[prost(uint32, tag = "1")]
    pub crc32c: u32,
    /// 0 if the bytes are served, 1 if the node holds no such fragment,
    /// 2 if it holds it damaged.
    #[prost(uint32, tag = "2")]
    pub failure: u32,
    /// Why the bytes are not served; empty if they are.
    #[prost(string, tag = "3")]
    pub reason: String,
}

impl FragmentRead {
    /// The body of `request`.
    #[must_use]
    pub fn new(request: &FragmentRequest) -> Self {
        let identity = &request.identity;
        let stripe = &identity.stripe;
        Self {
            id: request.fragment.get().to_le_bytes().to_vec(),
            bucket: identity.shard.bucket.as_str().to_owned(),
            shard: u32::from(identity.shard.shard.get()),
            key: identity.key.clone(),
            version_epoch: identity.version.epoch.get(),
            version_seq: identity.version.seq.get(),
            stripe: stripe.number,
            stripes: stripe.count,
            offset: stripe.offset,
            data_len: stripe.data_len,
            data_fragments: stripe.geometry.data_fragments() as u32,
            parity_fragments: stripe.geometry.parity_fragments() as u32,
            codec: u32::from(stripe.codec.get()),
            index: u32::from(identity.index),
            start: request.range.start,
            end: request.range.end,
        }
    }

    /// The fragment, identity, and range the body names, checked.
    ///
    /// # Errors
    ///
    /// Why the body is malformed.
    pub fn parse(&self) -> Result<(FragmentId, FragmentIdentity, Range<u64>), String> {
        let id: [u8; 16] = self
            .id
            .as_slice()
            .try_into()
            .map_err(|_| format!("a fragment ID of {} bytes", self.id.len()))?;
        let bucket = BucketId::new(self.bucket.clone()).map_err(|e| e.to_string())?;
        let shard = u8::try_from(self.shard).map_err(|_| format!("shard {}", self.shard))?;
        if self.key.is_empty() || self.key.len() > MAX_KEY_LEN {
            return Err(format!("a key of {} bytes", self.key.len()));
        }
        let geometry = Geometry::new(self.data_fragments as usize, self.parity_fragments as usize)
            .map_err(|e| e.to_string())?;
        let codec = u16::try_from(self.codec).map_err(|_| format!("codec {}", self.codec))?;
        let index = u8::try_from(self.index)
            .ok()
            .filter(|index| usize::from(*index) < geometry.total_fragments())
            .ok_or_else(|| format!("fragment {} of a {geometry} stripe", self.index))?;
        if self.stripe >= self.stripes || self.data_len == 0 {
            return Err(format!(
                "stripe {} of {}, of {} bytes",
                self.stripe, self.stripes, self.data_len
            ));
        }
        if self.offset.checked_add(self.data_len).is_none() {
            return Err("a stripe that ends past the largest object".to_owned());
        }
        if self.start >= self.end || self.end - self.start > MAX_READ_LEN {
            return Err(format!("bytes {}..{}", self.start, self.end));
        }
        let identity = FragmentIdentity {
            shard: ShardRef::new(bucket, ShardId::new(shard)),
            key: self.key.clone(),
            version: EpochSeq::new(Epoch::new(self.version_epoch), Seq::new(self.version_seq)),
            stripe: StripeInfo {
                number: self.stripe,
                count: self.stripes,
                offset: self.offset,
                data_len: self.data_len,
                geometry,
                codec: CodecId::new(codec),
            },
            index,
        };
        let id = FragmentId::new(u128::from_le_bytes(id));
        Ok((id, identity, self.start..self.end))
    }
}

impl FragmentData {
    /// The answer to a read: its bytes' CRC32C, or why there are none.
    fn of(result: &Result<FragmentBytes, NotServed>) -> Self {
        let (crc32c, failure, reason) = match result {
            Ok(bytes) => (bytes.crc32c, SERVED, String::new()),
            Err(NotServed::NotHeld(reason)) => (0, NOT_HELD, reason.clone()),
            Err(NotServed::Damaged(reason)) => (0, DAMAGED, reason.clone()),
        };
        Self {
            crc32c,
            failure,
            reason,
        }
    }

    /// The bytes `payload` this answer of `node` carries, or its failure.
    ///
    /// # Errors
    ///
    /// The node's failure, or [`FragmentReadError::Damaged`] for an answer
    /// that says neither.
    pub fn parse(&self, node: &NodeId, payload: Bytes) -> Result<FragmentBytes, FragmentReadError> {
        let reason = self.reason.clone();
        match self.failure {
            SERVED => Ok(FragmentBytes {
                data: payload,
                crc32c: self.crc32c,
            }),
            NOT_HELD => Err(FragmentReadError::NotHeld {
                node: node.clone(),
                reason,
            }),
            DAMAGED => Err(FragmentReadError::Damaged {
                node: node.clone(),
                reason,
            }),
            other => Err(FragmentReadError::Damaged {
                node: node.clone(),
                reason: format!("an answer with failure {other}"),
            }),
        }
    }
}

/// The idle connections to each node, with when each became idle.
type Idle<S> = BTreeMap<NodeId, Vec<(Instant, Connection<S>)>>;

/// Reads fragments from other nodes over the transport, and from this
/// node's own [`FragmentServer`] directly. Clones share connections.
pub struct FragmentReadClient<N: Network, D: Disk> {
    inner: Arc<ClientInner<N, D>>,
}

struct ClientInner<N: Network, D: Disk> {
    node: NodeId,
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    local: Option<FragmentServer<D>>,
    idle: Mutex<Idle<N::Stream>>,
    next_request: AtomicU64,
}

impl<N: Network, D: Disk> Clone for FragmentReadClient<N, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<N: Network, D: Disk> fmt::Debug for FragmentReadClient<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FragmentReadClient")
            .field("node", &self.inner.node)
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> FragmentReadClient<N, D> {
    /// The client of node `node`, which reaches the nodes at `peers`
    /// through `transport`.
    #[must_use]
    pub fn new(
        node: NodeId,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                node,
                transport,
                peers,
                local: None,
                idle: Mutex::default(),
                next_request: AtomicU64::new(1),
            }),
        }
    }

    /// Reads this node's own fragments from `server` without a connection.
    ///
    /// # Panics
    ///
    /// Panics if a clone of the client exists already.
    #[must_use]
    pub fn with_local(mut self, server: FragmentServer<D>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("the client is not shared yet")
            .local = Some(server);
        self
    }

    /// Reads `request` from its node over the transport.
    async fn ask(&self, request: &FragmentRequest) -> Result<FragmentBytes, FragmentReadError> {
        let node = &request.node;
        let unreachable = |reason: String| FragmentReadError::Unreachable {
            node: node.clone(),
            reason,
        };
        let mut connection = self.connection(node).await.map_err(unreachable)?;
        let id = self.inner.next_request.fetch_add(1, Ordering::Relaxed);
        let head = Header::new(MessageKind::FragmentRead)
            .with_request_id(id)
            .with_body(FragmentRead::new(request).encode_to_vec());
        let exchange = async {
            connection
                .send(&Frame::new(head, Bytes::new()))
                .await
                .map_err(|e| e.to_string())?;
            match connection.recv().await {
                Ok(Some(answer))
                    if answer.header.kind == MessageKind::FragmentData
                        && answer.header.request_id == id =>
                {
                    Ok(answer)
                }
                Ok(Some(_)) => Err("an answer to another request".to_owned()),
                Ok(None) => Err("the node closed the connection".to_owned()),
                Err(error) => Err(error.to_string()),
            }
        };
        let answer = match tokio::time::timeout(ANSWER_TIMEOUT, exchange).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(reason)) => return Err(unreachable(reason)),
            Err(_) => return Err(unreachable("no answer in time".to_owned())),
        };
        let data = FragmentData::decode(answer.header.body.as_ref())
            .map_err(|e| unreachable(format!("a malformed answer: {e}")))?;
        self.idle(node, connection);
        data.parse(node, answer.payload)
    }

    /// An idle connection to `node`, or a new one.
    async fn connection(&self, node: &NodeId) -> Result<Connection<N::Stream>, String> {
        {
            let mut idle = lock(&self.inner.idle);
            let kept = idle.entry(node.clone()).or_default();
            while let Some((since, connection)) = kept.pop() {
                if since.elapsed() < IDLE_TIMEOUT {
                    return Ok(connection);
                }
            }
        }
        let address = self
            .inner
            .peers
            .get(node)
            .ok_or_else(|| format!("no address for node {node}"))?;
        let connecting = self.inner.transport.connect(node, address);
        match tokio::time::timeout(CONNECT_TIMEOUT, connecting).await {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("no connection in time".to_owned()),
        }
    }

    /// Keeps `connection` to `node` for the next read.
    fn idle(&self, node: &NodeId, connection: Connection<N::Stream>) {
        let mut idle = lock(&self.inner.idle);
        let kept = idle.entry(node.clone()).or_default();
        if kept.len() < IDLE_PER_NODE {
            kept.push((Instant::now(), connection));
        }
    }
}

impl<N: Network, D: Disk> FragmentSource for FragmentReadClient<N, D> {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            if request.node == self.inner.node
                && let Some(local) = &self.inner.local
            {
                return local
                    .read(request.fragment, &request.identity, request.range)
                    .await
                    .map_err(|error| error.on(&request.node));
            }
            self.ask(&request).await
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Plain inserts and removals, which a panic cannot leave half done.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Why a node does not serve a fragment read, before the node is named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NotServed {
    NotHeld(String),
    Damaged(String),
}

impl NotServed {
    fn on(self, node: &NodeId) -> FragmentReadError {
        match self {
            Self::NotHeld(reason) => FragmentReadError::NotHeld {
                node: node.clone(),
                reason,
            },
            Self::Damaged(reason) => FragmentReadError::Damaged {
                node: node.clone(),
                reason,
            },
        }
    }
}

impl<D: Disk> FragmentServer<D> {
    /// Reads bytes `range` of fragment `id` from the store that holds it,
    /// verified against its block checksums, if its header is the one
    /// `identity` describes.
    pub(crate) async fn read(
        &self,
        id: FragmentId,
        identity: &FragmentIdentity,
        range: Range<u64>,
    ) -> Result<FragmentBytes, NotServed> {
        let store = self
            .stores()
            .iter()
            .find(|store| store.len(id).is_some())
            .ok_or_else(|| NotServed::NotHeld(format!("no fragment {id}")))?;
        let read = FragmentStore::read(store, id, range)
            .await
            .map_err(|error| match error {
                FragmentError::UnknownFragment(_) | FragmentError::OutOfRange { .. } => {
                    NotServed::NotHeld(error.to_string())
                }
                other => NotServed::Damaged(other.to_string()),
            })?;
        if !identity.matches(&read.header) {
            return Err(NotServed::NotHeld(format!(
                "fragment {id} is fragment {} of stripe {} of {} at {}",
                read.header.index, read.header.stripe.number, read.header.key, read.header.version
            )));
        }
        Ok(FragmentBytes {
            data: read.data,
            crc32c: read.crc32c,
        })
    }

    /// Serves the fragment reads of one connection, starting with its
    /// first frame `first`, until the connection fails, sends something
    /// else, or stays idle for long.
    pub async fn serve_reads<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut receiver, mut sender) = link;
        let node = receiver.peer().node_id().cloned();
        let mut frame = first;
        while frame.header.kind == MessageKind::FragmentRead && node.is_some() {
            let result = match FragmentRead::decode(frame.header.body.as_ref())
                .map_err(|error| error.to_string())
                .and_then(|body| body.parse())
            {
                Ok((id, identity, range)) => self.read(id, &identity, range).await,
                Err(reason) => Err(NotServed::NotHeld(format!("a malformed read: {reason}"))),
            };
            let head = Header::new(MessageKind::FragmentData)
                .with_request_id(frame.header.request_id)
                .with_body(FragmentData::of(&result).encode_to_vec());
            let payload = result.map(|bytes| bytes.data).unwrap_or_default();
            if let Err(error) = sender.send(&Frame::new(head, payload)).await {
                tracing::debug!(%error, "a fragment read's answer was not sent");
                return;
            }
            frame = match tokio::time::timeout(IDLE_TIMEOUT, receiver.recv()).await {
                Ok(Ok(Some(frame))) => frame,
                _ => return,
            };
        }
    }
}
