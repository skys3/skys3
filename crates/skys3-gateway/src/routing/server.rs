//! A node's end of forwarded requests, and the acceptor of its
//! intra-cluster connections.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use skys3_io::Disk;
use skys3_net::{Frame, Listener, MessageKind, Network, Receiver, Sender};
use skys3_shard::ShardSet;
use skys3_shard::replication::Replication;
use skys3_types::Epoch;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{Reply, Request, Response, wire};
use crate::shard::{ShardError, ShardRef, Shards};

/// How long an accepted connection may take to send its first frame.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a gateway's connection may stay idle before the server drops
/// it. Gateways drop idle connections well before
/// ([`RoutingConfig::idle_timeout`](super::RoutingConfig::idle_timeout)),
/// so a request rarely meets a connection the server just dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

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
    set: ShardSet<D>,
    shards: S,
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
            .field("shards", &self.inner.shards)
            .finish_non_exhaustive()
    }
}

impl<S: Shards, D: Disk> ForwardServer<S, D> {
    /// Serves requests on the replicas in `set` through `shards`, which
    /// calls them.
    #[must_use]
    pub fn new(set: ShardSet<D>, shards: S) -> Self {
        Self {
            inner: Arc::new(Inner { set, shards }),
        }
    }

    /// Answers `request` to `shard` from a gateway that knows its
    /// configuration in `epoch`.
    pub async fn handle(&self, shard: &ShardRef, epoch: Epoch, request: Request) -> Reply {
        let Some(replica) = self.inner.set.get(&shard.into()).await else {
            return Reply::Refused(ShardError::NotFound(shard.clone()));
        };
        let config = replica.config();
        if config.epoch < epoch {
            return Reply::Behind(config.epoch);
        }
        match self.execute(shard, request).await {
            Ok(response) => Reply::Served {
                response,
                epoch: config.epoch,
                hint: (config.epoch > epoch).then_some(config),
            },
            Err(ShardError::NotPrimary { .. }) => Reply::Redirect(replica.config()),
            Err(error) => Reply::Refused(error),
        }
    }

    async fn execute(&self, shard: &ShardRef, request: Request) -> Result<Response, ShardError> {
        let shards = &self.inner.shards;
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
