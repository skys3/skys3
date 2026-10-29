//! The gateway's end of routing: [`RoutedShards`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::{ControlStore, RetryPolicy, TypedKey, read_with_retries};
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_net::{Connection, Frame, Network, Transport};
use skys3_shard::ShardSet;
use skys3_types::{BucketDocument, Epoch, EpochSeq, NodeAddress, NodeId, ShardConfig};
use tokio::time::Instant;

use super::{ForwardServer, Reply, Request, Response, Served, ShardMap, wire};
use crate::conditions::{ConditionFailed, Precondition};
use crate::shard::{ShardError, ShardRef, ShardSummary, Shards, UploadParts};

/// How many times one call follows a redirect or reads the shard's
/// register before it gives up. Each redirect it follows names a newer
/// epoch, so this bounds only a configuration changing many times at once.
const MAX_ROUNDS: usize = 8;

/// The timing of [`RoutedShards`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingConfig {
    /// How long connecting to a replica's node may take. A node that does
    /// not answer in time did not get the request, and the gateway asks
    /// the next member.
    pub connect_timeout: Duration,
    /// How long a forwarded request may take once sent, its answer
    /// included. A write still waiting then fails with `503`: it may yet
    /// be applied, but it is not acknowledged (§5.2).
    pub request_timeout: Duration,
    /// How long an idle connection to a node is kept for the next request.
    pub idle_timeout: Duration,
    /// How many idle connections to each node are kept.
    pub idle_per_node: usize,
    /// How long after a read of one shard's register finishes the gateway
    /// waits before it reads it again, when no member it knows answers;
    /// requests in between take that read's result. A shard whose members
    /// are all down then does not turn every request into a control-store
    /// read (§6.2).
    pub register_interval: Duration,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            idle_per_node: 8,
            register_interval: Duration::from_secs(1),
        }
    }
}

/// Counts of how [`RoutedShards`] routed its calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoutingStats {
    /// Calls sent to another node.
    pub forwarded: u64,
    /// Redirect hints that updated the shard map.
    pub redirects: u64,
    /// Reads of a shard's register.
    pub register_reads: u64,
}

/// The idle connections to each node, with when each became idle.
type Idle<S> = BTreeMap<NodeId, Vec<(Instant, Connection<S>)>>;

/// The gateway's shards on a replicated node: every call goes to the
/// primary of its shard, as the [`ShardMap`] names it (see the
/// [module](super) docs).
///
/// `L` is the node's own shards, which open and remove its replicas and
/// serve the calls that reach this node, whether from this gateway or
/// another; `C` is the control store, read only for a shard none of whose
/// known members answers.
pub struct RoutedShards<L, D: Disk, N: Network, C> {
    inner: Arc<Inner<L, D, N, C>>,
    config: RoutingConfig,
    retry: RetryPolicy,
}

struct Inner<L, D: Disk, N: Network, C> {
    node: NodeId,
    local: L,
    server: ForwardServer<L, D>,
    map: ShardMap,
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    store: C,
    idle: Mutex<Idle<N::Stream>>,
    /// The last read of each shard's register: when it finished, and the
    /// error it ended with, if any.
    register_reads: tokio::sync::Mutex<BTreeMap<ShardRef, (Instant, Option<ShardError>)>>,
    next_request: AtomicU64,
    forwarded: AtomicU64,
    redirects: AtomicU64,
    reads: AtomicU64,
}

impl<L, D: Disk, N: Network, C> Clone for RoutedShards<L, D, N, C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            config: self.config,
            retry: self.retry,
        }
    }
}

impl<L, D: Disk, N: Network, C> fmt::Debug for RoutedShards<L, D, N, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutedShards")
            .field("node", &self.inner.node)
            .field("map", &self.inner.map)
            .finish_non_exhaustive()
    }
}

/// What asking one node gave.
#[expect(
    clippy::large_enum_variant,
    reason = "one value per request, moved a few times; boxing would allocate for each"
)]
enum Attempt {
    /// The node answered.
    Reply(Reply),
    /// The request did not reach the node.
    Unreached(String),
    /// The request may have reached the node, but no answer came.
    Lost(String),
}

/// What asking the members of one configuration gave.
#[expect(
    clippy::large_enum_variant,
    reason = "one value per request, moved a few times; boxing would allocate for each"
)]
enum Round {
    /// An answer for the caller.
    Done(Result<Response, ShardError>),
    /// A member's hint updated the map: ask again.
    Redirected,
    /// No member answered, for the reason given.
    Unanswered(String),
}

impl<L: Shards, D: Disk, N: Network, C: ControlStore> RoutedShards<L, D, N, C> {
    /// The shards of node `node`, whose own shards are `local` over the
    /// replicas in `set`, routing by `map` to the other nodes at `peers`
    /// through `transport`, and reading shard registers from `store`.
    #[must_use]
    pub fn new(
        node: NodeId,
        local: L,
        set: ShardSet<D>,
        map: ShardMap,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        store: C,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                server: ForwardServer::new(node.clone(), set, local.clone()),
                node,
                local,
                map,
                transport,
                peers,
                store,
                idle: Mutex::default(),
                register_reads: tokio::sync::Mutex::default(),
                next_request: AtomicU64::new(1),
                forwarded: AtomicU64::new(0),
                redirects: AtomicU64::new(0),
                reads: AtomicU64::new(0),
            }),
            config: RoutingConfig::default(),
            retry: RetryPolicy::default(),
        }
    }

    /// Uses `config` for timing and `retry` for register reads.
    #[must_use]
    pub fn with_config(mut self, config: RoutingConfig, retry: RetryPolicy) -> Self {
        self.config = config;
        self.retry = retry;
        self
    }

    /// Reports every request this node's replicas serve to `observer`,
    /// for metrics and audits, whichever gateway sent it (see
    /// [`ForwardServer::observe`]).
    #[must_use]
    pub fn with_observer(self, observer: impl Fn(&Served) + Send + Sync + 'static) -> Self {
        self.inner.server.observe(observer);
        self
    }

    /// The server of the requests other gateways forward to this node,
    /// which [`serve_peers`](super::serve_peers) runs.
    #[must_use]
    pub fn server(&self) -> &ForwardServer<L, D> {
        &self.inner.server
    }

    /// The shard map.
    #[must_use]
    pub fn map(&self) -> &ShardMap {
        &self.inner.map
    }

    /// How the calls so far were routed.
    #[must_use]
    pub fn stats(&self) -> RoutingStats {
        let inner = &self.inner;
        RoutingStats {
            forwarded: inner.forwarded.load(Ordering::Relaxed),
            redirects: inner.redirects.load(Ordering::Relaxed),
            register_reads: inner.reads.load(Ordering::Relaxed),
        }
    }

    /// Sends `request` to the primary of `shard` and returns its answer.
    async fn call(&self, shard: &ShardRef, request: Request) -> Result<Response, ShardError> {
        let mut unanswered = "the gateway knows no configuration of the shard".to_owned();
        // The epoch of the configuration whose members were asked last.
        let mut asked = None;
        let mut read_register = false;
        for _ in 0..MAX_ROUNDS {
            if let Some(config) = self.newer(shard, asked) {
                asked = Some(config.epoch);
                match self.ask_members(shard, &config, &request).await {
                    Round::Done(result) => return result,
                    Round::Redirected => continue,
                    Round::Unanswered(why) => unanswered = why,
                }
            }
            if read_register || !self.read_register(shard, asked).await? {
                break;
            }
            read_register = true;
        }
        Err(ShardError::Unavailable {
            shard: shard.clone(),
            reason: format!("no replica served the request: {unanswered}"),
        })
    }

    /// Asks the primary of `config`, then its other members, until one
    /// answers.
    async fn ask_members(
        &self,
        shard: &ShardRef,
        config: &ShardConfig,
        request: &Request,
    ) -> Round {
        let others = config.members.iter().filter(|m| **m != config.primary);
        let mut why = String::new();
        for node in std::iter::once(&config.primary).chain(others) {
            let reply = match self.ask(node, shard, config.epoch, request).await {
                Attempt::Reply(reply) => reply,
                Attempt::Unreached(error) => {
                    why = format!("{node} did not get the request: {error}");
                    continue;
                }
                Attempt::Lost(error) if request.is_read() => {
                    why = format!("{node} did not answer: {error}");
                    continue;
                }
                Attempt::Lost(error) => {
                    return Round::Done(Err(ShardError::Unavailable {
                        shard: shard.clone(),
                        reason: format!("{node} did not answer, and may have applied it: {error}"),
                    }));
                }
            };
            match reply {
                Reply::Served { response, hint, .. } => {
                    if let Some(hint) = hint {
                        self.inner.map.learn(hint).await;
                    }
                    return Round::Done(Ok(response));
                }
                Reply::Redirect(hint) => {
                    let epoch = hint.epoch;
                    if self.inner.map.learn(hint).await {
                        self.inner.redirects.fetch_add(1, Ordering::Relaxed);
                        return Round::Redirected;
                    }
                    why = format!("{node} names epoch {epoch}, no newer than the map's");
                }
                Reply::Behind(epoch) => why = format!("{node} is still in epoch {epoch}"),
                Reply::Refused(ShardError::NotFound(_)) => {
                    why = format!("the shard is not open on {node}");
                }
                Reply::Refused(error) => return Round::Done(Err(error)),
            }
        }
        Round::Unanswered(why)
    }

    /// Sends `request` to `node`'s replica, in process if `node` is this
    /// one.
    async fn ask(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        epoch: Epoch,
        request: &Request,
    ) -> Attempt {
        let inner = &self.inner;
        if *node == inner.node {
            return Attempt::Reply(inner.server.handle(shard, epoch, request.clone()).await);
        }
        let Some(address) = inner.peers.get(node) else {
            return Attempt::Unreached("the node's address is unknown".to_owned());
        };
        let mut frame = match wire::request_frame(shard, epoch, request) {
            Ok(frame) => frame,
            Err(reason) => {
                return Attempt::Reply(Reply::Refused(ShardError::Invalid {
                    shard: shard.clone(),
                    reason,
                }));
            }
        };
        let mut connection = match self.connection(node, address).await {
            Ok(connection) => connection,
            Err(error) => return Attempt::Unreached(error),
        };
        inner.forwarded.fetch_add(1, Ordering::Relaxed);
        let id = inner.next_request.fetch_add(1, Ordering::Relaxed);
        frame.header.request_id = id;
        let exchange = async {
            connection.send(&frame).await.map_err(|e| e.to_string())?;
            match connection.recv().await {
                Ok(Some(reply)) if reply.header.request_id == id => Ok(reply),
                Ok(Some(_)) => Err("an answer to another request".to_owned()),
                Ok(None) => Err("the node closed the connection".to_owned()),
                Err(error) => Err(error.to_string()),
            }
        };
        let reply: Frame = match tokio::time::timeout(self.config.request_timeout, exchange).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => return Attempt::Lost(error),
            Err(_) => {
                return Attempt::Lost(format!(
                    "no answer within {:?}",
                    self.config.request_timeout
                ));
            }
        };
        match wire::decode_reply(&reply, shard) {
            Ok(reply) => {
                self.idle(node, connection);
                Attempt::Reply(reply)
            }
            Err(error) => Attempt::Lost(format!("an answer that does not decode: {error}")),
        }
    }

    /// An idle connection to `node`, or a new one.
    async fn connection(
        &self,
        node: &NodeId,
        address: &NodeAddress,
    ) -> Result<Connection<N::Stream>, String> {
        {
            let mut idle = lock(&self.inner.idle);
            let kept = idle.entry(node.clone()).or_default();
            while let Some((since, connection)) = kept.pop() {
                if since.elapsed() < self.config.idle_timeout {
                    return Ok(connection);
                }
            }
        }
        let connecting = self.inner.transport.connect(node, address);
        match tokio::time::timeout(self.config.connect_timeout, connecting).await {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err(format!(
                "no connection within {:?}",
                self.config.connect_timeout
            )),
        }
    }

    /// Keeps `connection` to `node` for the next request.
    fn idle(&self, node: &NodeId, connection: Connection<N::Stream>) {
        let mut idle = lock(&self.inner.idle);
        let kept = idle.entry(node.clone()).or_default();
        if kept.len() < self.config.idle_per_node {
            kept.push((Instant::now(), connection));
        }
    }

    /// The map's configuration of `shard`, if it is newer than `asked`.
    fn newer(&self, shard: &ShardRef, asked: Option<Epoch>) -> Option<ShardConfig> {
        let config = self.inner.map.get(shard)?;
        asked
            .is_none_or(|asked| config.epoch > asked)
            .then_some(config)
    }

    /// Reads `shard`'s register, and returns whether the map now holds a
    /// newer configuration than `asked`, the one whose members did not
    /// answer. Reads of one node wait for each other, and a shard's
    /// register is read again only
    /// [`RoutingConfig::register_interval`] after its last read finished:
    /// concurrent requests to a shard whose members are all down share
    /// one read, and its result, however long it takes.
    async fn read_register(
        &self,
        shard: &ShardRef,
        asked: Option<Epoch>,
    ) -> Result<bool, ShardError> {
        let mut reads = self.inner.register_reads.lock().await;
        if self.newer(shard, asked).is_some() {
            return Ok(true);
        }
        if let Some((at, error)) = reads.get(shard)
            && at.elapsed() < self.config.register_interval
        {
            return error.clone().map_or(Ok(false), Err);
        }
        self.inner.reads.fetch_add(1, Ordering::Relaxed);
        let key = TypedKey::shard(&shard.bucket, shard.shard);
        let read = match read_with_retries(&self.inner.store, &key, &self.retry).await {
            Ok(Some(register)) => {
                self.inner.map.learn(register.value).await;
                Ok(())
            }
            Ok(None) => Err(ShardError::NotFound(shard.clone())),
            Err(error) => Err(ShardError::Unavailable {
                shard: shard.clone(),
                reason: format!("its register could not be read: {error}"),
            }),
        };
        // The interval starts when the read finishes, so requests that
        // waited for it take its result rather than read again.
        reads.insert(shard.clone(), (Instant::now(), read.clone().err()));
        read.map(|()| self.newer(shard, asked).is_some())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update leaves the value consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The error of an answer to another call than the one asked.
fn mismatched(shard: &ShardRef) -> ShardError {
    ShardError::Unavailable {
        shard: shard.clone(),
        reason: "the replica answered another call".to_owned(),
    }
}

impl<L: Shards, D: Disk, N: Network, C: ControlStore> Shards for RoutedShards<L, D, N, C> {
    /// Opens this node's replica of the shard, if it has one.
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        self.inner.local.open(shard, bucket).await
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        match self.call(shard, Request::Seal).await? {
            Response::Sealed(summary) => Ok(summary),
            _ => Err(mismatched(shard)),
        }
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        match self.call(shard, Request::Unseal).await? {
            Response::Unsealed => Ok(()),
            _ => Err(mismatched(shard)),
        }
    }

    /// Removes this node's replica of the shard and forgets its route.
    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.inner.local.remove(shard).await?;
        self.inner.map.forget(shard).await;
        Ok(())
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        let request = Request::Entry {
            key: key.to_owned(),
        };
        match self.call(shard, request).await? {
            Response::Entry(entry) => Ok(entry),
            _ => Err(mismatched(shard)),
        }
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        match self.call(shard, Request::List(query.clone())).await? {
            Response::List(page) => Ok(page),
            _ => Err(mismatched(shard)),
        }
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<UploadParts>, ShardError> {
        let request = Request::Upload {
            key: key.to_owned(),
            upload,
            after,
            limit,
        };
        match self.call(shard, request).await? {
            Response::Upload(found) => Ok(found),
            _ => Err(mismatched(shard)),
        }
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        let request = Request::Uploads {
            prefix: prefix.to_owned(),
            after,
            limit,
        };
        match self.call(shard, request).await? {
            Response::Uploads(uploads) => Ok(uploads),
            _ => Err(mismatched(shard)),
        }
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        let request = Request::Parts {
            upload,
            after,
            limit,
        };
        match self.call(shard, request).await? {
            Response::Parts(parts) => Ok(parts),
            _ => Err(mismatched(shard)),
        }
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        match self.call(shard, Request::Payload(position)).await? {
            Response::Payload(bytes) => Ok(bytes),
            _ => Err(mismatched(shard)),
        }
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        match self.call(shard, Request::AppendExtent(extent)).await? {
            Response::Extent(extent) => Ok(extent),
            _ => Err(mismatched(shard)),
        }
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        match self.call(shard, Request::Write { body, condition }).await? {
            Response::Written(written) => Ok(written),
            _ => Err(mismatched(shard)),
        }
    }
}
