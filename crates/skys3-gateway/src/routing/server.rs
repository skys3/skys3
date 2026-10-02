//! A node's end of forwarded requests, and the acceptor of its
//! intra-cluster connections.

use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use skys3_io::Disk;
use skys3_net::{Frame, Listener, MessageKind, Network, Receiver, Sender};
use skys3_shard::ShardSet;
use skys3_shard::replication::Replication;
use skys3_types::{Epoch, NodeId};
use tokio::io::{AsyncRead, AsyncWrite};

use super::{Reply, Request, Response, wire};
use skys3_log::RecordBody;

use crate::conditions::Precondition;
use crate::shard::{ShardError, ShardRef, Shards, WriteOutcome};

/// How long an accepted connection may take to send its first frame.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a gateway's connection may stay idle before the server drops
/// it. Gateways drop idle connections well before
/// ([`RoutingConfig::idle_timeout`](super::RoutingConfig::idle_timeout)),
/// so a request rarely meets a connection the server just dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// A request a node's replica served, as an observer of its
/// [`ForwardServer`] sees it (see [`ForwardServer::observe`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Served {
    /// The shard.
    pub shard: ShardRef,
    /// The node whose replica served it.
    pub node: NodeId,
    /// The epoch of the configuration the replica served it in.
    pub epoch: Epoch,
}

type Observer = Arc<dyn Fn(&Served) + Send + Sync>;

/// Serves forwarded requests on a node's shard replicas.
///
/// Each request goes to the node's replica of its shard, through `S`, the
/// node's shards. The replica serves it only as the serving primary of its
/// configuration: a member refuses (see
/// [`Shard::check_readable`](skys3_shard::Shard::check_readable)), and the
/// server answers with the replica's configuration as the redirect hint.
/// A request that names a newer epoch than the replica's is not served
/// either: the replica has not adopted that configuration yet, and may no
/// longer be its shard's primary.
pub struct ForwardServer<S, D: Disk> {
    inner: Arc<Inner<S, D>>,
}

struct Inner<S, D: Disk> {
    node: NodeId,
    set: ShardSet<D>,
    shards: S,
    observer: OnceLock<Observer>,
}

impl<S, D: Disk> Clone for ForwardServer<S, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: fmt::Debug, D: Disk> fmt::Debug for ForwardServer<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForwardServer")
            .field("node", &self.inner.node)
            .field("shards", &self.inner.shards)
            .finish_non_exhaustive()
    }
}

impl<S: Shards, D: Disk> ForwardServer<S, D> {
    /// Serves requests on the replicas in `set` of node `node` through
    /// `shards`, which calls them.
    #[must_use]
    pub fn new(node: NodeId, set: ShardSet<D>, shards: S) -> Self {
        Self {
            inner: Arc::new(Inner {
                node,
                set,
                shards,
                observer: OnceLock::new(),
            }),
        }
    }

    /// Reports every request a replica of this node serves to `observer`,
    /// for metrics and audits, whichever gateway sent it. The report comes
    /// as the replica answers, before the answer is sent, so a request is
    /// observed even if its answer is then lost. A server has at most one
    /// observer: the first one set stays.
    pub fn observe(&self, observer: impl Fn(&Served) + Send + Sync + 'static) {
        if self.inner.observer.set(Arc::new(observer)).is_err() {
            tracing::warn!("a forward server already has an observer");
        }
    }

    /// Answers `request` to `shard` from a gateway that knows its
    /// configuration in `epoch`.
    pub async fn handle(&self, shard: &ShardRef, epoch: Epoch, request: Request) -> Reply {
        if request.is_holder() {
            // Any replica holds the payload it has, whatever its role and
            // epoch, and the routing audit is of primaries only.
            return match self.execute(shard, request).await {
                Ok(response) => Reply::Served {
                    response,
                    epoch,
                    hint: None,
                },
                Err(error) => Reply::Refused(error),
            };
        }
        let Some(replica) = self.inner.set.get(&shard.into()).await else {
            return Reply::Refused(ShardError::NotFound(shard.clone()));
        };
        let config = replica.config();
        if config.epoch < epoch {
            return Reply::Behind(config.epoch);
        }
        match self.execute(shard, request).await {
            Ok(response) => {
                if let Some(observer) = self.inner.observer.get() {
                    observer(&Served {
                        shard: shard.clone(),
                        node: self.inner.node.clone(),
                        epoch: config.epoch,
                    });
                }
                Reply::Served {
                    response,
                    epoch: config.epoch,
                    hint: (config.epoch > epoch).then_some(config),
                }
            }
            Err(ShardError::NotPrimary { .. }) => Reply::Redirect(replica.config()),
            Err(error) => Reply::Refused(error),
        }
    }

    /// Commits `writes` on this node's replica of `shard` together, as
    /// [`Shards::write_all`] does, for a gateway of this node that knows the
    /// shard's configuration in `epoch`. `None`, with nothing written, if
    /// the replica does not serve as the primary of that configuration or a
    /// newer one; a write the replica refuses as it steps down meanwhile
    /// fails with [`ShardError::NotPrimary`].
    pub async fn write_all(
        &self,
        shard: &ShardRef,
        epoch: Epoch,
        writes: Vec<(RecordBody, Precondition)>,
    ) -> Option<Vec<WriteOutcome>> {
        let replica = self.inner.set.get(&shard.into()).await?;
        let config = replica.config();
        if config.epoch < epoch || replica.check_readable().is_err() {
            return None;
        }
        let written = self.inner.shards.write_all(shard, writes).await;
        if let Some(observer) = self.inner.observer.get() {
            observer(&Served {
                shard: shard.clone(),
                node: self.inner.node.clone(),
                epoch: config.epoch,
            });
        }
        Some(written)
    }

    async fn execute(&self, shard: &ShardRef, request: Request) -> Result<Response, ShardError> {
        let (shards, node) = (&self.inner.shards, &self.inner.node);
        Ok(match request {
            Request::Entry { key } => Response::Entry(shards.entry(shard, &key).await?),
            Request::List(query) => Response::List(shards.list(shard, &query).await?),
            Request::Upload {
                key,
                upload,
                after,
                limit,
            } => Response::Upload(shards.upload(shard, &key, upload, after, limit).await?),
            Request::Uploads {
                prefix,
                after,
                limit,
            } => Response::Uploads(shards.uploads(shard, &prefix, after, limit).await?),
            Request::Parts {
                upload,
                after,
                limit,
            } => Response::Parts(shards.parts(shard, upload, after, limit).await?),
            Request::Payload(position) => Response::Payload(shards.payload(shard, position).await?),
            Request::Plan { key } => Response::Plan(shards.plan(shard, &key).await?),
            Request::Register {
                key,
                version,
                layout,
            } => Response::Registered(
                shards
                    .register(shard, node, &key, version, layout)
                    .await?,
            ),
            Request::Renew(read) => Response::Renewed(shards.renew(shard, node, read).await?),
            Request::Release(read) => {
                shards.release(shard, node, read).await?;
                Response::Released
            }
            Request::Fetch { read, position } => {
                Response::Payload(shards.fetch(shard, node, read, position).await?)
            }
            Request::AppendExtent(extent) => {
                Response::Extent(shards.append_extent(shard, extent).await?)
            }
            Request::Write { body, condition } => {
                Response::Written(shards.write(shard, body, condition).await?)
            }
            Request::Seal => Response::Sealed(shards.seal(shard).await?),
            Request::Unseal => {
                shards.unseal(shard).await?;
                Response::Unsealed
            }
        })
    }

    /// Serves the forwarded requests of one connection from a gateway, one
    /// at a time, starting with its first frame `first`, until the
    /// connection fails, sends something else, or stays idle for long.
    pub async fn serve<St>(
        &self,
        (mut receiver, mut sender): (Receiver<St>, Sender<St>),
        first: Frame,
    ) where
        St: AsyncRead + AsyncWrite + Unpin,
    {
        let mut frame = first;
        loop {
            let (shard, epoch, request) = match wire::decode_request(&frame) {
                Ok(request) => request,
                Err(error) => {
                    tracing::debug!(%error, "a forwarded request does not decode");
                    return;
                }
            };
            let reply = self.handle(&shard, epoch, request).await;
            let reply = wire::reply_frame(&reply, frame.header.request_id);
            if let Err(error) = sender.send(&reply).await {
                tracing::debug!(%error, "a forwarded request's answer was not sent");
                return;
            }
            frame = match tokio::time::timeout(IDLE_TIMEOUT, receiver.recv()).await {
                Ok(Ok(Some(frame))) => frame,
                Ok(Ok(None)) | Err(_) => return,
                Ok(Err(error)) => {
                    tracing::debug!(%error, "a gateway's connection failed");
                    return;
                }
            };
        }
    }
}

/// Accepts the intra-cluster connections of a node on `listener` until it
/// fails, each on a task of its own: a connection whose first frame is a
/// forwarded request goes to `server`, and every other one to
/// `replication`, which serves links from primaries.
pub async fn serve_peers<N, D, S>(
    listener: Listener<N>,
    replication: Replication<N, D>,
    server: ForwardServer<S, D>,
) where
    N: Network,
    D: Disk,
    S: Shards,
{
    loop {
        let incoming = match listener.accept().await {
            Ok(incoming) => incoming,
            Err(error) => {
                tracing::warn!(%error, "accepting an intra-cluster connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (replication, server) = (replication.clone(), server.clone());
        tokio::spawn(async move {
            let connection = match incoming.handshake().await {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::debug!(%error, "a connection failed its handshake");
                    return;
                }
            };
            let (mut receiver, sender) = connection.into_split();
            let first = match tokio::time::timeout(FIRST_FRAME_TIMEOUT, receiver.recv()).await {
                Ok(Ok(Some(frame))) => frame,
                _ => return,
            };
            if first.header.kind == MessageKind::Forward {
                server.serve((receiver, sender), first).await;
            } else {
                replication.follow((receiver, sender), first).await;
            }
        });
    }
}
