//! Orphan queries between nodes, on the intra-cluster transport:
//!
//! ```text
//! fragment node                                   shard primary
//!   OrphanQuery(shard, suspects)               ->
//!                                              <-  OrphanVerdicts(verdicts | error)
//! ```
//!
//! Both bodies are `prost` messages in the frame's payload, since a query
//! of [`MAX_SUSPECTS`] fragments with long keys exceeds what a frame
//! header holds. The asking node is the one mutual TLS names, so a node
//! only learns verdicts on fragments it says it holds, and reclaims only
//! its own. Each query uses a connection of its own, and goes to every
//! node that may lead the shard at once (see [`OrphanClient`]).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_io::Disk;
use skys3_log::ShardRef;
use skys3_log::record::MAX_KEY_LEN;
use skys3_net::{Frame, Header, MessageKind, Network, Receiver, Sender, Transport};
use skys3_types::{AttemptId, BucketId, Epoch, FragmentId, NodeAddress, NodeId, ShardId};
use tokio::task::JoinSet;

use super::{OrphanConfirmer, OrphanJudge, Suspect, Unanswered, Verdict};

/// The most fragments one query names.
pub const MAX_SUSPECTS: usize = 256;

/// How long a node waits for another node's verdicts, connecting
/// included. A query waits for every node it asks unless one decides
/// every fragment asked about, so a node that is down holds a sweep up
/// this long; unanswered fragments are asked about again at the next
/// sweep.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(1);

/// The body of an `OrphanQuery` frame.
#[derive(Clone, PartialEq, Message)]
pub struct OrphanQuery {
    /// The shard's bucket ID.
    #[prost(string, tag = "1")]
    pub bucket: String,
    /// The shard's number, at most 255.
    #[prost(uint32, tag = "2")]
    pub shard: u32,
    /// The fragments asked about, at most [`MAX_SUSPECTS`].
    #[prost(message, repeated, tag = "3")]
    pub suspects: Vec<SuspectBody>,
}

/// One fragment of an [`OrphanQuery`].
#[derive(Clone, PartialEq, Message)]
pub struct SuspectBody {
    /// The fragment's ID, 16 bytes little-endian.
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    /// The key of its object.
    #[prost(string, tag = "2")]
    pub key: String,
    /// The epoch of the attempt that wrote it.
    #[prost(uint64, tag = "3")]
    pub attempt_epoch: u64,
    /// The attempt's number in that epoch.
    #[prost(uint64, tag = "4")]
    pub attempt_number: u64,
}

/// The body of an `OrphanVerdicts` frame: one verdict byte per suspect, in
/// the query's order ([`Verdict::to_byte`]), or why there are none.
#[derive(Clone, PartialEq, Message)]
pub struct OrphanVerdicts {
    /// The verdicts; empty on a refusal.
    #[prost(bytes = "vec", tag = "1")]
    pub verdicts: Vec<u8>,
    /// Why the node gives no verdicts; empty on success.
    #[prost(string, tag = "2")]
    pub error: String,
}

impl OrphanQuery {
    /// The query about `suspects` of `shard`.
    #[must_use]
    pub fn new(shard: &ShardRef, suspects: &[Suspect]) -> Self {
        Self {
            bucket: shard.bucket.as_str().to_owned(),
            shard: u32::from(shard.shard.get()),
            suspects: suspects
                .iter()
                .map(|suspect| SuspectBody {
                    id: suspect.id.get().to_le_bytes().to_vec(),
                    key: suspect.key.clone(),
                    attempt_epoch: suspect.attempt.epoch.get(),
                    attempt_number: suspect.attempt.number,
                })
                .collect(),
        }
    }

    /// The shard and suspects the query names, checked.
    ///
    /// # Errors
    ///
    /// Why the query is malformed.
    pub fn parse(&self) -> Result<(ShardRef, Vec<Suspect>), String> {
        let bucket = BucketId::new(self.bucket.clone()).map_err(|e| e.to_string())?;
        let number = u8::try_from(self.shard).map_err(|_| format!("shard {}", self.shard))?;
        if self.suspects.is_empty() || self.suspects.len() > MAX_SUSPECTS {
            return Err(format!("a query of {} fragments", self.suspects.len()));
        }
        let suspects = self
            .suspects
            .iter()
            .map(|body| {
                let id: [u8; 16] = body
                    .id
                    .as_slice()
                    .try_into()
                    .map_err(|_| format!("a fragment ID of {} bytes", body.id.len()))?;
                if body.key.is_empty() || body.key.len() > MAX_KEY_LEN {
                    return Err(format!("a key of {} bytes", body.key.len()));
                }
                Ok(Suspect {
                    id: FragmentId::new(u128::from_le_bytes(id)),
                    key: body.key.clone(),
                    attempt: AttemptId::new(Epoch::new(body.attempt_epoch), body.attempt_number),
                })
            })
            .collect::<Result<_, String>>()?;
        Ok((ShardRef::new(bucket, ShardId::new(number)), suspects))
    }
}

impl OrphanVerdicts {
    /// The verdicts on `count` suspects, checked.
    ///
    /// # Errors
    ///
    /// The node's refusal, or why the answer is malformed.
    pub fn parse(&self, count: usize) -> Result<Vec<Verdict>, String> {
        if !self.error.is_empty() {
            return Err(self.error.clone());
        }
        if self.verdicts.len() != count {
            return Err(format!(
                "{} verdicts on {count} fragments",
                self.verdicts.len()
            ));
        }
        self.verdicts
            .iter()
            .map(|&byte| Verdict::from_byte(byte).ok_or_else(|| format!("a verdict of {byte}")))
            .collect()
    }
}

/// The nodes that may lead a shard: whom a fragment node asks.
pub type PrimariesOf = Arc<dyn Fn(&ShardRef) -> Vec<NodeId> + Send + Sync>;

/// Asks shard primaries about this node's fragments over the transport,
/// and this node's own judges directly.
///
/// A query goes to every node that may lead the shard at once. Only a
/// replica that leads gives verdicts, and any such replica's orphan and
/// referenced verdicts are final (see [the module](crate::orphans)), so
/// the first answer that decides every suspect counts, and a node that is
/// down, or a member that no longer leads, never holds a query up. An
/// answer with fragments in progress waits for the others: a primary
/// deposed without knowing it keeps its own unfinished attempt in progress
/// for good, while its successor finds it orphaned. The answers are then
/// combined, a decided verdict winning over one in progress and referenced
/// over orphan.
pub struct OrphanClient<N: Network, D: Disk> {
    inner: Arc<ClientInner<N, D>>,
}

struct ClientInner<N: Network, D: Disk> {
    node: NodeId,
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    primaries: PrimariesOf,
    local: OrphanServer<D>,
}

impl<N: Network, D: Disk> OrphanClient<N, D> {
    /// The client of node `node`, which reaches the nodes at `peers`
    /// through `transport`, asks the nodes `primaries` names for a shard,
    /// and asks `local`, its own server, without a connection.
    #[must_use]
    pub fn new(
        node: NodeId,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        primaries: PrimariesOf,
        local: OrphanServer<D>,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                node,
                transport,
                peers,
                primaries,
                local,
            }),
        }
    }
}

impl<N: Network, D: Disk> ClientInner<N, D> {
    /// Asks `node` about `suspects` of `shard`.
    async fn ask(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, String> {
        if *node == self.node {
            return self.local.judge(node, shard, suspects).await;
        }
        let address = self
            .peers
            .get(node)
            .ok_or_else(|| format!("no address for node {node}"))?;
        let exchange = async {
            let mut connection = self
                .transport
                .connect(node, address)
                .await
                .map_err(|e| e.to_string())?;
            let query = OrphanQuery::new(shard, suspects).encode_to_vec();
            let frame = Frame::new(Header::new(MessageKind::OrphanQuery), query);
            connection.send(&frame).await.map_err(|e| e.to_string())?;
            let answer = match connection.recv().await {
                Ok(Some(frame)) if frame.header.kind == MessageKind::OrphanVerdicts => frame,
                Ok(_) => return Err("no verdicts".to_owned()),
                Err(error) => return Err(error.to_string()),
            };
            let _ = connection.close().await;
            OrphanVerdicts::decode(answer.payload.as_ref())
                .map_err(|e| format!("malformed verdicts: {e}"))?
                .parse(suspects.len())
        };
        tokio::time::timeout(ANSWER_TIMEOUT, exchange)
            .await
            .unwrap_or_else(|_| Err("no answer in time".to_owned()))
    }
}

impl<N: Network, D: Disk> OrphanConfirmer for OrphanClient<N, D> {
    async fn confirm(
        &self,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, Unanswered> {
        let mut asks = JoinSet::new();
        for node in (self.inner.primaries)(shard) {
            let inner = Arc::clone(&self.inner);
            let (shard, suspects) = (shard.clone(), suspects.to_vec());
            asks.spawn(async move {
                let answer = inner.ask(&node, &shard, &suspects).await;
                (node, answer)
            });
        }
        // Dropping the set once every suspect is decided stops the other
        // asks.
        let mut merged: Option<Vec<Verdict>> = None;
        let mut reasons = Vec::new();
        while let Some(joined) = asks.join_next().await {
            match joined {
                Ok((_, Ok(verdicts))) => {
                    let verdicts = match merged.take() {
                        Some(earlier) => merge(earlier, &verdicts),
                        None => verdicts,
                    };
                    if !verdicts.contains(&Verdict::InProgress) {
                        return Ok(verdicts);
                    }
                    merged = Some(verdicts);
                }
                Ok((node, Err(reason))) => reasons.push(format!("{node}: {reason}")),
                Err(error) => reasons.push(error.to_string()),
            }
        }
        if let Some(verdicts) = merged {
            return Ok(verdicts);
        }
        reasons.sort();
        Err(Unanswered {
            shard: shard.clone(),
            reason: if reasons.is_empty() {
                "no node may lead it".to_owned()
            } else {
                reasons.join("; ")
            },
        })
    }
}

/// Combines two replicas' verdicts on the same suspects. Each replica
/// that leads gives safe verdicts, but only its decided ones are final: a
/// primary deposed without knowing it may call an attempt of its own in
/// progress for good. So a decided verdict wins over one in progress, and
/// a fragment one replica still finds referenced is kept.
fn merge(earlier: Vec<Verdict>, later: &[Verdict]) -> Vec<Verdict> {
    fn rank(verdict: Verdict) -> u8 {
        match verdict {
            Verdict::InProgress => 0,
            Verdict::Orphan => 1,
            Verdict::Referenced => 2,
        }
    }
    earlier
        .into_iter()
        .zip(later.iter().copied())
        .map(|(a, b)| if rank(b) > rank(a) { b } else { a })
        .collect()
}

/// A node's end of orphan queries: the judges of the shards it has
/// replicas of, which answer while their replica leads.
pub struct OrphanServer<D: Disk> {
    judges: Arc<Mutex<BTreeMap<ShardRef, OrphanJudge<D>>>>,
}

impl<D: Disk> Clone for OrphanServer<D> {
    fn clone(&self) -> Self {
        Self {
            judges: Arc::clone(&self.judges),
        }
    }
}

impl<D: Disk> Default for OrphanServer<D> {
    fn default() -> Self {
        Self {
            judges: Arc::default(),
        }
    }
}

impl<D: Disk> OrphanServer<D> {
    /// Answers for `judge`'s shard, replacing the judge it had.
    pub fn insert(&self, judge: OrphanJudge<D>) {
        self.lock().insert(judge.shard().clone(), judge);
    }

    /// Stops answering for `shard`.
    pub fn remove(&self, shard: &ShardRef) {
        self.lock().remove(shard);
    }

    /// The verdicts of this node's judge of `shard` on `suspects`, which
    /// `node` holds.
    ///
    /// # Errors
    ///
    /// Why the node gives none: it has no replica of the shard, or as
    /// [`OrphanJudge::judge`].
    pub async fn judge(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, String> {
        let judge = self.lock().get(shard).cloned();
        let judge = judge.ok_or_else(|| format!("no replica of shard {shard} here"))?;
        judge
            .judge(node, suspects)
            .await
            .map_err(|error| error.to_string())
    }

    /// Answers one orphan query whose frame, `first`, arrived on `link`.
    pub async fn serve<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (receiver, mut sender) = link;
        let answer = match self.answer(receiver.peer().node_id(), &first).await {
            Ok(verdicts) => OrphanVerdicts {
                verdicts: verdicts.into_iter().map(Verdict::to_byte).collect(),
                error: String::new(),
            },
            Err(error) => OrphanVerdicts {
                verdicts: Vec::new(),
                error,
            },
        };
        let frame = Frame::new(
            Header::new(MessageKind::OrphanVerdicts),
            Bytes::from(answer.encode_to_vec()),
        );
        if let Err(error) = sender.send(&frame).await {
            tracing::debug!(%error, "an orphan query's answer was not sent");
        }
    }

    async fn answer(&self, node: Option<&NodeId>, frame: &Frame) -> Result<Vec<Verdict>, String> {
        let node = node.ok_or("only nodes ask about fragments")?;
        if frame.header.kind != MessageKind::OrphanQuery {
            return Err(format!(
                "expected an orphan query, got {:?}",
                frame.header.kind
            ));
        }
        let query = OrphanQuery::decode(frame.payload.as_ref())
            .map_err(|error| format!("a malformed orphan query: {error}"))?;
        let (shard, suspects) = query.parse()?;
        self.judge(node, &shard, &suspects).await
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<ShardRef, OrphanJudge<D>>> {
        // Plain inserts and lookups, which a panic cannot leave half done.
        self.judges.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
