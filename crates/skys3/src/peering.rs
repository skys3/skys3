//! Native replication between clusters in the node (design §7.8): the
//! QUIC endpoint, the source side's peer transport, and the destination
//! side's staging, commits, and descriptors.
//!
//! A node with at least one `[peering.peers]` table binds its QUIC endpoint
//! on `quic_listen`, presenting its `[transport]` certificate, and:
//!
//! - **as a source**, gives its flushers a [`PeerTransport`]: a target's
//!   descriptor is verified against the trust bundle of the cluster it
//!   names, and a fresh connection to its addresses, handshake and `HELLO`s
//!   included, must succeed within `peer_connect_timeout_ms` before the
//!   target uses QUIC, through the node's connection pool;
//! - **as a destination**, accepts peer connections and serves their
//!   streams with staging and commits over the node's shards, and signs
//!   the descriptor of each bucket that receives native replication, if
//!   `quic_advertise` names its addresses. A descriptor is signed again
//!   once half its lifetime passed.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_config::Config;
use skys3_flush::{LinkError, PeerLink, PeerTransport};
use skys3_gateway::{
    BucketLookup, GatewayConfig, PeerCommits, PeerDescriptors, PeerExtents, Shards,
};
use skys3_io::WallClock;
use skys3_net::Credentials;
use skys3_peer::{
    ConnectionPool, DESCRIPTOR_LIFETIME, DescriptorSigner, Destination, EndpointSettings,
    PeerDescriptor, PeerEndpoint, PeerTls, PeerTrust, Staging, StagingLimits, StagingService,
};
use skys3_types::{BucketName, ClusterId};

use crate::node::StartError;

/// How often the connection pool adapts its limits: a few round trips of
/// a long link apart (§7.8).
const POOL_ADAPT_INTERVAL: Duration = Duration::from_secs(1);

/// The node's peer endpoint and what runs on it.
pub(crate) struct NodePeering {
    endpoint: PeerEndpoint,
    pool: ConnectionPool,
    tls: PeerTls,
    descriptors: Option<Arc<NodeDescriptors>>,
}

impl std::fmt::Debug for NodePeering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodePeering")
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl NodePeering {
    /// Binds the node's peer endpoint, if `config` names any peer. Reads
    /// the certificate files with blocking I/O, once, at startup.
    ///
    /// # Errors
    ///
    /// [`StartError::Unsupported`] for certificate and trust files that
    /// cannot be used, and [`StartError::Io`] if the endpoint cannot bind.
    pub(crate) fn open(
        config: &Config,
        wall: Arc<dyn WallClock>,
    ) -> Result<Option<Self>, StartError> {
        let peering = config.peering();
        if peering.peers.is_empty() {
            return Ok(None);
        }
        let unusable = |what: &str, error: &dyn std::fmt::Display| {
            StartError::Unsupported(format!("peering: {what}: {error}"))
        };
        let files = config
            .transport()
            .tls_files()
            .ok_or_else(|| unusable("the [transport] certificate", &"is not configured"))?;
        let credentials = Credentials::load(
            config.cluster().cluster_id.clone(),
            files.cert,
            files.key,
            files.ca,
        )
        .map_err(|error| unusable("the node certificate", &error))?;
        let trust =
            PeerTrust::load(peering).map_err(|error| unusable("the peer trust bundles", &error))?;
        let tls = PeerTls::new(&credentials, Arc::new(trust))
            .ok_or_else(|| unusable("the node certificate", &"does not name a node"))?;
        let endpoint = PeerEndpoint::bind(
            peering.quic_listen,
            &tls,
            EndpointSettings::from_config(peering),
        )
        .map_err(|source| StartError::Io {
            what: format!("binding the peer endpoint on {}", peering.quic_listen),
            source,
        })?;
        let pool = ConnectionPool::new(endpoint.clone(), peering.peer_connections_per_shard);
        let descriptors = (!peering.quic_advertise.is_empty()).then(|| {
            Arc::new(NodeDescriptors {
                cluster: config.cluster().cluster_id.clone(),
                addresses: peering.quic_advertise.clone(),
                signer: tls.descriptor_signer(),
                wall,
                signed: Mutex::default(),
            })
        });
        Ok(Some(Self {
            endpoint,
            pool,
            tls,
            descriptors,
        }))
    }

    /// How the node's flushers reach peers.
    pub(crate) fn transport(&self, config: &Config) -> PeerTransport {
        let peering = config.peering();
        let (endpoint, pool) = (self.endpoint.clone(), self.pool.clone());
        PeerTransport::new(
            Box::new(move |descriptor: &PeerDescriptor| {
                let (endpoint, pool) = (endpoint.clone(), pool.clone());
                let (cluster, addresses) =
                    (descriptor.cluster.clone(), descriptor.addresses.clone());
                Box::pin(async move { handshake(&endpoint, &pool, cluster, &addresses).await })
            }),
            self.tls.descriptor_verifier(),
            peering.peer_frame_bytes,
        )
        .with_connect_timeout(peering.peer_connect_timeout())
    }

    /// The descriptors the gateway serves, if the node advertises its
    /// endpoint.
    pub(crate) fn descriptors(&self) -> Option<Arc<dyn PeerDescriptors>> {
        self.descriptors
            .clone()
            .map(|descriptors| descriptors as Arc<dyn PeerDescriptors>)
    }

    /// Accepts peer connections until the endpoint closes, and serves
    /// their streams: staging on, and commits into, `shards`.
    pub(crate) async fn serve<S: Shards>(
        self,
        config: &Config,
        gateway: &GatewayConfig,
        shards: S,
        buckets: BucketLookup,
    ) {
        let peering = config.peering();
        let staging = Arc::new(Staging::new(StagingLimits {
            quota_bytes: peering.peer_staging_quota_bytes,
            ttl: peering.peer_staging_ttl(),
        }));
        let service = StagingService::new(
            staging,
            PeerExtents::new(shards.clone(), Arc::clone(&buckets)),
        )
        .with_commits(PeerCommits::new(shards, buckets, gateway));
        while let Some(incoming) = self.endpoint.accept().await {
            let service = service.clone();
            tokio::spawn(async move {
                let from = incoming.remote_address();
                match incoming.establish().await {
                    Ok(connection) => service.serve_connection(connection).await,
                    Err(error) => tracing::info!(%from, %error, "a peer connection failed"),
                }
            });
        }
    }

    /// Adapts the connection pool's limits every second, for as long as
    /// the node runs (§7.8).
    pub(crate) fn adapt_pool(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let pool = self.pool.clone();
        async move {
            let mut ticks = tokio::time::interval(POOL_ADAPT_INTERVAL);
            loop {
                ticks.tick().await;
                pool.adapt();
            }
        }
    }

    /// Closes the endpoint: connections end, and [`NodePeering::serve`]
    /// returns.
    pub(crate) fn closer(&self) -> PeerEndpoint {
        self.endpoint.clone()
    }
}

/// Connects to the first of `addresses` of `cluster` that completes a
/// fresh handshake, `HELLO`s included, and returns a link through `pool`
/// to it. The probe connection is closed: flushes use the pool's.
async fn handshake(
    endpoint: &PeerEndpoint,
    pool: &ConnectionPool,
    cluster: ClusterId,
    addresses: &[String],
) -> Result<Arc<dyn PeerLink>, LinkError> {
    let mut failures = Vec::new();
    for address in addresses {
        for address in resolve(address).await {
            let destination = Destination {
                cluster: cluster.clone(),
                address,
            };
            match endpoint.connect(&destination).await {
                Ok(probe) => {
                    probe.close();
                    return Ok(Arc::new(pool.attach(destination)));
                }
                Err(error) => failures.push(format!("{address}: {error}")),
            }
        }
    }
    Err(LinkError::new(if failures.is_empty() {
        "no advertised address resolves".to_owned()
    } else {
        failures.join("; ")
    }))
}

/// The socket addresses `address` (`host:port`) resolves to.
async fn resolve(address: &str) -> Vec<SocketAddr> {
    match tokio::net::lookup_host(address).await {
        Ok(addresses) => addresses.collect(),
        Err(error) => {
            tracing::debug!(address, %error, "an advertised peer address does not resolve");
            Vec::new()
        }
    }
}

/// The descriptors of the node's receiving buckets, each signed once and
/// signed again after half its lifetime.
struct NodeDescriptors {
    cluster: ClusterId,
    addresses: Vec<String>,
    signer: DescriptorSigner,
    wall: Arc<dyn WallClock>,
    /// Each bucket's descriptor and when it was issued.
    signed: Mutex<BTreeMap<(BucketName, ClusterId), (Duration, Bytes)>>,
}

impl std::fmt::Debug for NodeDescriptors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDescriptors")
            .field("cluster", &self.cluster)
            .field("addresses", &self.addresses)
            .finish_non_exhaustive()
    }
}

impl PeerDescriptors for NodeDescriptors {
    fn descriptor(&self, bucket: &BucketName, source: &ClusterId) -> Option<Bytes> {
        let now = self.wall.now();
        let mut signed = self.signed.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = (bucket.clone(), source.clone());
        if let Some((issued, bytes)) = signed.get(&entry)
            && now.saturating_sub(*issued) < DESCRIPTOR_LIFETIME / 2
        {
            return Some(bytes.clone());
        }
        let descriptor = PeerDescriptor::new(
            self.cluster.clone(),
            bucket.clone(),
            source.clone(),
            self.addresses.clone(),
            now,
        );
        match self.signer.sign(&descriptor) {
            Ok(bytes) => {
                signed.insert(entry, (now, bytes.clone()));
                Some(bytes)
            }
            Err(error) => {
                tracing::warn!(%bucket, %error, "the peer descriptor cannot be signed");
                None
            }
        }
    }
}
