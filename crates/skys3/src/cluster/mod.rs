//! The node's cluster services, one file per service.
//!
//! - [`control`]: following the control store (design §6.2), which keeps
//!   the node's copy of bucket bindings and identity configuration current
//!   and reopens a store that failed.
//! - [`peering`]: native replication with peer clusters over QUIC (design
//!   §7.8).
//! - [`transport`]: the intra-cluster transport listener (design §12),
//!   with its cap on in-flight handshakes.
//! - [`registration`]: node registration in `nodes/<node-id>.json` and
//!   heartbeats to the coordinator (design §6.7).
//! - [`hints`]: the receiver of admin messages from other nodes, pushed
//!   generations above all (design §6.2).
//! - [`replication`]: the shards replicated across the nodes their
//!   registers name, and the gateway's routing of every request to its
//!   shard's primary (design §5, §6.3–§6.6, §9.2).
//!
//! The last four run only with `[cluster] multi_node = true`
//! ([`ClusterServices`], [`replication::ClusterShards`]); without it, the
//! node behaves as a single node, serves every shard alone, and never
//! binds the transport.

pub(crate) mod control;
pub(crate) mod hints;
pub(crate) mod peering;
pub(crate) mod registration;
pub(crate) mod replication;
pub(crate) mod transport;

use std::net::SocketAddr;
use std::sync::Arc;

use skys3_config::Config;
use skys3_coord::{ControlHints, HeartbeatStatus};
use skys3_io::{BlockingPool, Clock, MonotonicClock};
use skys3_net::{Credentials, Listener, PeerIdentity, TokioNetwork};
use skys3_obs::{HandshakeLimit, HandshakeMetrics};
use skys3_types::NodeId;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::admin::ClusterView;
use crate::control::backend::Backend;
use crate::datadir::DataDir;
use crate::node::StartError;
use registration::NodeHeartbeater;
use replication::ClusterShards;

/// Loads the node's `[transport]` credentials for `[cluster] multi_node =
/// true`, and checks that the certificate names this node: a node's ID is
/// the one its certificate names (design §12). `None` for a single node.
///
/// # Errors
///
/// [`StartError::Cluster`] for credentials that cannot be loaded, such as
/// a certificate of another cluster, or one that names another node.
pub(crate) fn credentials(
    config: &Config,
    node: &NodeId,
) -> Result<Option<Credentials>, StartError> {
    if !config.cluster().multi_node {
        return Ok(None);
    }
    let cluster = &config.cluster().cluster_id;
    let files = config.transport().tls_files().ok_or_else(|| {
        StartError::Cluster("the [transport] certificate files are not set".to_owned())
    })?;
    let credentials =
        Credentials::load(cluster.clone(), files.cert, files.key, files.ca).map_err(|error| {
            StartError::Cluster(format!(
                "the [transport] certificate of cluster {cluster} is not usable: {error}"
            ))
        })?;
    match credentials.identity() {
        PeerIdentity::Node(named) if named == node => Ok(Some(credentials)),
        other => Err(StartError::Cluster(format!(
            "the [transport] certificate names {other}, but this node is {node}: a node's ID is \
             the one its certificate names, so set [node] node_id to it, or use the certificate \
             issued for {node}"
        ))),
    }
}

/// The cluster services of a node with `[cluster] multi_node = true`, bound
/// and registered, ready to run ([`ClusterServices::spawn`]).
pub(crate) struct ClusterServices {
    listener: Listener<TokioNetwork>,
    limit: HandshakeLimit,
    services: transport::Services,
    heartbeater: NodeHeartbeater,
    view: ClusterView,
}

impl ClusterServices {
    /// Binds the transport listener with the credentials of the transport
    /// of `shards`, and registers the
    /// node in `store` (the control store's own client, so that the node
    /// registers even while it runs from its copy). Pushed generations go
    /// to `hints`, the links and forwarded requests of other nodes to
    /// `shards`, and the listener's handshakes are exported in
    /// `handshakes`.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the listener cannot bind, a disk's capacity cannot
    /// be read, or the register holds the node's ID for another data
    /// directory.
    pub(crate) async fn start(
        config: &Config,
        data_dir: &DataDir,
        store: Backend,
        hints: ControlHints,
        handshakes: &HandshakeMetrics,
        pool: &BlockingPool,
        shards: &ClusterShards,
    ) -> Result<Self, StartError> {
        let settings = config.transport();
        let advertise = settings.advertised().ok_or_else(|| {
            StartError::Cluster(
                "[transport] advertise is required with this listen address".to_owned(),
            )
        })?;
        let transport = shards.transport().clone();
        let listener = transport
            .bind(settings.listen)
            .await
            .map_err(|error| StartError::Io {
                what: format!("binding the transport to {}", settings.listen),
                source: std::io::Error::other(error.to_string()),
            })?;
        let listen: SocketAddr = listener.local_addr().map_err(|source| StartError::Io {
            what: "reading the transport's address".to_owned(),
            source,
        })?;
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let limit = HandshakeLimit::new(settings.max_inflight_handshakes)
            .with_metrics(handshakes, "transport");
        let services = transport::Services {
            admin: hints::endpoint(config, hints.clone(), Arc::clone(&clock)),
            replication: shards.replication().clone(),
            forwarded: shards.server().clone(),
        };
        let profile = registration::profile(config, data_dir, advertise.clone(), pool).await?;
        let mut heartbeater = registration::heartbeater(config, store, transport, profile, clock);
        registration::register_at_start(&mut heartbeater).await?;
        tracing::info!(%listen, %advertise, "the transport listens");
        let view = ClusterView {
            listen,
            advertise,
            heartbeats: heartbeater.subscribe(),
            hints,
            handshakes: limit.clone(),
        };
        Ok(Self {
            listener,
            limit,
            services,
            heartbeater,
            view,
        })
    }

    /// What the admin API reports of the services.
    pub(crate) fn view(&self) -> ClusterView {
        self.view.clone()
    }

    /// What the heartbeater reports, which [`claimed`] follows.
    pub(crate) fn status(&self) -> watch::Receiver<HeartbeatStatus> {
        self.view.heartbeats.clone()
    }

    /// Serves the transport and sends heartbeats, for as long as the
    /// node's background tasks run.
    pub(crate) fn spawn(self, background: &mut JoinSet<()>) {
        let Self {
            listener,
            limit,
            services,
            heartbeater,
            ..
        } = self;
        background.spawn(transport::serve(listener, limit, services));
        background.spawn(heartbeater.run());
    }
}

/// Completes once the heartbeater finds the node's register held by another
/// data directory ([`HeartbeatStatus::claimed_by`]), however late, for
/// example when the node started from its copy while the store did not
/// answer: the node must then stop, as when it finds the claim at startup,
/// rather than serve under an ID another node's registration holds. It
/// returns why, naming both instances and the runbook. `ours` is this
/// node's data directory instance. Without a heartbeater, or once it is
/// gone, it never completes.
pub(crate) async fn claimed(
    status: Option<watch::Receiver<HeartbeatStatus>>,
    node: NodeId,
    ours: String,
) -> String {
    let Some(mut status) = status else {
        return std::future::pending().await;
    };
    let theirs = match status.wait_for(|status| status.claimed_by.is_some()).await {
        Ok(status) => status.claimed_by.clone(),
        Err(_) => None,
    };
    let Some(theirs) = theirs else {
        return std::future::pending().await;
    };
    format!(
        "nodes/{node}.json registers node {node} for data directory instance {theirs}, but this \
         node runs on instance {ours}: another node holds this node ID, so this one stops (see \
         the runbook \"Node ID in use\")"
    )
}
