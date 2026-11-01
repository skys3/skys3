//! Fragment writes between nodes (design §8.4, step 2): a shard primary's
//! encoder sends each fragment to the node that is to hold it, which makes
//! it durable in one of its fragment stores and answers with its ID.
//!
//! The writes run on the intra-cluster transport (`skys3-net`), whose
//! mutual TLS identifies both ends, as replication does:
//!
//! ```text
//! primary                                              fragment node
//!   FragmentWrite(header_len, len, crc32c) + bytes  ->
//!   FragmentWrite + bytes ...                        ->
//!                                                   <-  FragmentWritten(id | error)
//! ```
//!
//! The payloads of the `FragmentWrite` frames, in order, are the encoded
//! [`FragmentHeader`] ([`FragmentHeader::to_bytes`]) followed by the
//! fragment's bytes; only the first frame carries the lengths and the
//! fragment's CRC32C in its body. Frames hold at most [`CHUNK_LEN`] bytes,
//! so a fragment of up to [`MAX_FRAGMENT_LEN`] bytes needs no frame larger
//! than the transport allows. A node acknowledges a fragment only once its
//! store made it durable, so an ID in a `FragmentWritten` names a fragment
//! that survives a power loss. Each write uses a connection of its own.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use prost::Message;
use skys3_io::Disk;
use skys3_net::{
    Frame, Header, Listener, MessageKind, Network, Receiver, Sender, Transport, TransportError,
};
use skys3_types::{NodeAddress, NodeId};

use crate::fragment::{FragmentHeader, MAX_FRAGMENT_LEN, MAX_HEADER_LEN};
use crate::{FragmentId, FragmentStore};

/// The most fragment bytes one frame carries.
pub const CHUNK_LEN: usize = 8 << 20;

/// How long either end waits for the other's next frame.
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// The body of the first `FragmentWrite` frame.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentWrite {
    /// The length of the encoded fragment header that starts the bytes.
    #[prost(uint32, tag = "1")]
    pub header_len: u32,
    /// The length of the fragment, which follows the header.
    #[prost(uint64, tag = "2")]
    pub len: u64,
    /// The CRC32C of the fragment's bytes.
    #[prost(uint32, tag = "3")]
    pub crc32c: u32,
}

/// The body of a `FragmentWritten` frame: the fragment's ID, or why the
/// node did not make it durable.
#[derive(Clone, PartialEq, Message)]
pub struct FragmentWritten {
    /// The fragment's ID, 16 bytes little-endian, once it is durable;
    /// empty otherwise.
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    /// Why the fragment was not written; empty on success.
    #[prost(string, tag = "2")]
    pub error: String,
}

/// Why a fragment write failed. Nothing is known of the fragment then: it
/// may be durable as an orphan, which reclamation removes (§8.4).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransferError {
    /// The node is not one the writer knows the address of.
    #[error("no address for node {0}")]
    UnknownNode(NodeId),
    /// The connection to the node failed.
    #[error("the link to node {node} failed: {source}")]
    Transport {
        /// The node.
        node: NodeId,
        /// What failed.
        source: TransportError,
    },
    /// The node did not answer in time, or closed the link.
    #[error("node {0} did not answer")]
    NoAnswer(NodeId),
    /// The node refused or failed the write.
    #[error("node {node} did not write the fragment: {reason}")]
    Refused {
        /// The node.
        node: NodeId,
        /// Its reason.
        reason: String,
    },
    /// The fragment cannot be sent: its header does not encode, or it is
    /// longer than a fragment may be.
    #[error("the fragment cannot be sent: {0}")]
    Invalid(String),
}

/// What writes fragments to nodes for an encoder: [`FragmentClient`] over
/// the transport, or a test's stand-in.
pub trait FragmentWriter: Send + Sync + 'static {
    /// Writes `data`, a fragment described by `header`, to `node`, and
    /// returns its ID once the node holds it durably.
    fn write(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> impl Future<Output = Result<FragmentId, TransferError>> + Send;
}

/// Writes fragments to other nodes over the transport, and to this node's
/// own [`FragmentServer`] directly.
pub struct FragmentClient<N: Network, D: Disk> {
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    local: Option<(NodeId, FragmentServer<D>)>,
}

impl<N: Network, D: Disk> FragmentClient<N, D> {
    /// A client that reaches the nodes at `peers` through `transport`.
    #[must_use]
    pub fn new(transport: Transport<N>, peers: BTreeMap<NodeId, NodeAddress>) -> Self {
        Self {
            transport,
            peers,
            local: None,
        }
    }

    /// Writes the fragments meant for `node`, this one, to `server`
    /// without a connection.
    #[must_use]
    pub fn with_local(mut self, node: NodeId, server: FragmentServer<D>) -> Self {
        self.local = Some((node, server));
        self
    }

    async fn send(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let address = self
            .peers
            .get(node)
            .ok_or_else(|| TransferError::UnknownNode(node.clone()))?;
        let header = header
            .to_bytes()
            .map_err(|error| TransferError::Invalid(error.to_string()))?;
        let transport = |source| TransferError::Transport {
            node: node.clone(),
            source,
        };
        let connection = self
            .transport
            .connect(node, address)
            .await
            .map_err(transport)?;
        let (mut receiver, mut sender) = connection.into_split();
        let body = FragmentWrite {
            // A header is at most `MAX_HEADER_LEN`, a u32.
            header_len: header.len() as u32,
            len: data.len() as u64,
            crc32c: crc32c::crc32c(&data),
        };
        let mut stream = BytesMut::from(header.as_slice());
        stream.extend_from_slice(&data);
        let stream = stream.freeze();
        // The node may refuse the write before it has read every frame, so
        // its answer is read while the frames go out; a full send buffer
        // never stalls both ends.
        let sending = async {
            for (n, start) in (0..stream.len()).step_by(CHUNK_LEN).enumerate() {
                let chunk = stream.slice(start..stream.len().min(start + CHUNK_LEN));
                let mut head = Header::new(MessageKind::FragmentWrite);
                if n == 0 {
                    head = head.with_body(body.encode_to_vec());
                }
                sender.send(&Frame::new(head, chunk)).await?;
            }
            Ok::<_, TransportError>(())
        };
        let chunks = stream.len().div_ceil(CHUNK_LEN) as u32;
        let deadline = FRAME_TIMEOUT * (chunks + 1);
        // A refusal answers before the node has read every frame, and the
        // send then fails; the answer is what counts.
        let exchange = async { tokio::join!(sending, receiver.recv()) };
        let Ok((_, answer)) = tokio::time::timeout(deadline, exchange).await else {
            return Err(TransferError::NoAnswer(node.clone()));
        };
        let answer = match answer {
            Ok(Some(frame)) if frame.header.kind == MessageKind::FragmentWritten => frame,
            Ok(_) => return Err(TransferError::NoAnswer(node.clone())),
            Err(source) => return Err(transport(source)),
        };
        let _ = sender.close().await;
        let written = FragmentWritten::decode(answer.header.body.as_ref())
            .map_err(|_| TransferError::NoAnswer(node.clone()))?;
        parse_written(node, written)
    }
}

impl<N: Network, D: Disk> FragmentWriter for FragmentClient<N, D> {
    async fn write(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        if let Some((local, server)) = &self.local
            && local == node
        {
            return server.store(header.clone(), data).await.map_err(|reason| {
                TransferError::Refused {
                    node: node.clone(),
                    reason,
                }
            });
        }
        self.send(node, header, data).await
    }
}

/// Turns a node's answer into the fragment's ID or the node's reason.
fn parse_written(node: &NodeId, written: FragmentWritten) -> Result<FragmentId, TransferError> {
    if !written.error.is_empty() {
        return Err(TransferError::Refused {
            node: node.clone(),
            reason: written.error,
        });
    }
    let id: [u8; 16] = written
        .id
        .try_into()
        .map_err(|_| TransferError::NoAnswer(node.clone()))?;
    Ok(FragmentId::new(u128::from_le_bytes(id)))
}

/// A node's end of fragment writes: it keeps each fragment it is sent in
/// one of the node's fragment stores, the one holding the fewest bytes.
pub struct FragmentServer<D: Disk> {
    stores: Arc<Vec<FragmentStore<D>>>,
}

impl<D: Disk> Clone for FragmentServer<D> {
    fn clone(&self) -> Self {
        Self {
            stores: Arc::clone(&self.stores),
        }
    }
}

impl<D: Disk> FragmentServer<D> {
    /// The server of a node whose disks' fragment stores are `stores`, in
    /// disk order.
    ///
    /// # Panics
    ///
    /// Panics if `stores` is empty.
    #[must_use]
    pub fn new(stores: Vec<FragmentStore<D>>) -> Self {
        assert!(!stores.is_empty(), "a fragment server needs a store");
        Self {
            stores: Arc::new(stores),
        }
    }

    /// The node's fragment stores, in disk order.
    #[must_use]
    pub fn stores(&self) -> &[FragmentStore<D>] {
        &self.stores
    }

    /// Accepts connections on `listener` until it fails, serving each
    /// fragment write, or each connection's fragment reads, on a task of
    /// its own. A node whose transport carries other messages too accepts
    /// connections itself and hands the links whose first frame is a
    /// `FragmentWrite` to [`FragmentServer::serve`], and those whose first
    /// frame is a `FragmentRead` to [`FragmentServer::serve_reads`].
    pub async fn serve_listener<N: Network>(&self, listener: Listener<N>) {
        loop {
            let Ok(incoming) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let server = self.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.handshake().await else {
                    return;
                };
                let (mut receiver, sender) = connection.into_split();
                if let Ok(Ok(Some(first))) =
                    tokio::time::timeout(FRAME_TIMEOUT, receiver.recv()).await
                {
                    if first.header.kind == MessageKind::FragmentRead {
                        server.serve_reads((receiver, sender), first).await;
                    } else {
                        server.serve((receiver, sender), first).await;
                    }
                }
            });
        }
    }

    /// Serves one fragment write whose first frame, `first`, arrived on
    /// `link`: receives the rest, stores the fragment, and answers.
    pub async fn serve<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut receiver, mut sender) = link;
        let result = match self.receive(&mut receiver, first).await {
            Ok((header, data)) => self.store(header, data).await,
            Err(reason) => Err(reason),
        };
        let written = match result {
            Ok(id) => FragmentWritten {
                id: id.get().to_le_bytes().to_vec(),
                error: String::new(),
            },
            Err(error) => FragmentWritten {
                id: Vec::new(),
                error,
            },
        };
        let head = Header::new(MessageKind::FragmentWritten).with_body(written.encode_to_vec());
        if let Err(error) = sender.send(&Frame::new(head, Bytes::new())).await {
            tracing::debug!(%error, "a fragment write's answer was not sent");
        }
    }

    /// Receives a fragment's header and bytes, checking every length
    /// before it holds the bytes, and the fragment's CRC32C.
    async fn receive<S>(
        &self,
        receiver: &mut Receiver<S>,
        first: Frame,
    ) -> Result<(FragmentHeader, Bytes), String>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if first.header.kind != MessageKind::FragmentWrite {
            return Err(format!(
                "expected a fragment write, got {:?}",
                first.header.kind
            ));
        }
        let body = FragmentWrite::decode(first.header.body.as_ref())
            .map_err(|error| format!("a malformed fragment write: {error}"))?;
        if body.header_len == 0 || body.header_len > MAX_HEADER_LEN {
            return Err(format!("a fragment header of {} bytes", body.header_len));
        }
        if body.len == 0 || body.len > MAX_FRAGMENT_LEN {
            return Err(format!("a fragment of {} bytes", body.len));
        }
        // Both lengths are bounded, so their sum fits in memory.
        let total = u64::from(body.header_len) + body.len;
        let mut stream = BytesMut::new();
        let mut frame = first;
        loop {
            if (stream.len() + frame.payload.len()) as u64 > total {
                return Err("more bytes than the fragment write announced".to_owned());
            }
            stream.extend_from_slice(&frame.payload);
            if stream.len() as u64 == total {
                break;
            }
            frame = match tokio::time::timeout(FRAME_TIMEOUT, receiver.recv()).await {
                Ok(Ok(Some(frame))) if frame.header.kind == MessageKind::FragmentWrite => frame,
                _ => return Err("the fragment write broke off".to_owned()),
            };
        }
        let mut stream = stream.freeze();
        let data = stream.split_off(body.header_len as usize);
        let header = FragmentHeader::from_bytes(&stream).map_err(|error| error.to_string())?;
        if crc32c::crc32c(&data) != body.crc32c {
            return Err("the fragment's bytes fail their CRC32C".to_owned());
        }
        Ok((header, data))
    }

    /// Writes a fragment to the store holding the fewest bytes, and
    /// returns its ID once it is durable.
    async fn store(&self, header: FragmentHeader, data: Bytes) -> Result<FragmentId, String> {
        let held = |store: &FragmentStore<D>| -> u64 {
            store.segments().iter().map(|segment| segment.len).sum()
        };
        let store = self
            .stores
            .iter()
            .filter(|store| store.is_in_service())
            .min_by_key(|store| held(store))
            .ok_or_else(|| "no fragment store is in service".to_owned())?;
        store
            .write(&header, data)
            .await
            .map_err(|error| error.to_string())
    }
}
