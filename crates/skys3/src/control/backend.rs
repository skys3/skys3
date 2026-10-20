//! The control-store backends a node runs (design §6.1), behind one type,
//! [`Backend`], and how a node opens the one `[control_store]` names
//! ([`Opener`]).
//!
//! - **file**: [`open_file_store`], which records the store's owner and
//!   refuses another node's store.
//! - **etcd**: over `https://`, verified with `etcd_tls_ca_file` or the
//!   system's roots and authenticated with `etcd_tls_cert_file` and
//!   `etcd_tls_key_file` (design §12), or plain HTTP on loopback only,
//!   which configuration loading enforces.
//! - **s3**: through the AWS SDK client, signed with the credentials of
//!   `profile` or of the default chain. The node runs the startup probe
//!   ([`ControlProbe`]) each time it opens the store, and refuses a store
//!   that breaks a property; one that only fails to answer is probed again
//!   at the next attempt. At startup it also reads the control bucket's
//!   versioning ([`read_versioning`]).
//!
//! **One node per store, unless `[cluster] multi_node`.** A single node
//! serves each of its shards alone, so two of them on one store would
//! serve the same buckets apart. A node without `multi_node` therefore
//! refuses an etcd or S3 store whose `nodes/` registers another node
//! ([`check_single_node`]), as the file store refuses a second node
//! itself. With `multi_node = true`, nodes share the store and register in
//! it, and the guard gives way to registration, which refuses only a
//! registration of the node's own ID from another data directory
//! (`crate::cluster::registration`).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use rustls::ClientConfig;
use skys3_config::{ControlStoreBackend, ControlStoreConfig};
use skys3_control::{
    Change, ChangeFeed, ChangeStream, ControlError, ControlProbe, ControlStore, DeleteOutcome,
    EtcdControlStore, EtcdStoreConfig, Expected, FileControlStore, KeyPrefix, PutOutcome,
    RegisterKey, RegisterKind, RetryPolicy, S3Changes, S3ControlStore, S3StoreConfig, Version,
    Versioned,
};
use skys3_io::BlockingPool;
use skys3_remote::aws::{AwsS3, BucketVersioning, default_credentials, profile_credentials};
use skys3_types::{Generation, NodeId, RemoteTarget};

use super::{OpenStoreError, OpenedStore, StoreOwner, open_file_store, retrying};
use crate::tls::{self, TlsError};

/// How long one request to an S3 control store may take, its whole answer
/// included.
const S3_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A control store of any backend the node runs. Clones share the store.
#[derive(Debug, Clone)]
pub enum Backend {
    /// A directory on this node.
    File(FileControlStore),
    /// An etcd v3 cluster.
    Etcd(EtcdControlStore),
    /// A bucket of an S3-compatible store.
    S3(S3ControlStore<AwsS3>),
}

/// The change stream of a [`Backend`].
#[derive(Debug)]
pub enum BackendChanges {
    /// A file store's.
    File(ChangeFeed<FileControlStore>),
    /// An etcd store's.
    Etcd(ChangeFeed<EtcdControlStore>),
    /// An S3 store's.
    S3(S3Changes<AwsS3>),
}

/// Runs `$body` with `$store` bound to the backend's own store.
macro_rules! dispatch {
    ($backend:expr, $store:ident => $body:expr) => {
        match $backend {
            Backend::File($store) => $body,
            Backend::Etcd($store) => $body,
            Backend::S3($store) => $body,
        }
    };
}

impl Backend {
    /// The backend's name in `[control_store] backend`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::Etcd(_) => "etcd",
            Self::S3(_) => "s3",
        }
    }

    /// Whether the store refuses every request until it is opened again:
    /// a file store after a failed write.
    #[must_use]
    pub fn failed(&self) -> bool {
        matches!(self, Self::File(store) if store.failed())
    }
}

impl ChangeStream for BackendChanges {
    async fn next(&mut self) -> Result<Change, ControlError> {
        match self {
            Self::File(changes) => changes.next().await,
            Self::Etcd(changes) => changes.next().await,
            Self::S3(changes) => changes.next().await,
        }
    }
}

impl ControlStore for Backend {
    type Changes = BackendChanges;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        dispatch!(self, store => store.get(key).await)
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        dispatch!(self, store => store.put_if(key, expected, value).await)
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        dispatch!(self, store => store.delete_if(key, expected).await)
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        dispatch!(self, store => store.list(prefix).await)
    }

    async fn changes(&self, after: Generation) -> Result<BackendChanges, ControlError> {
        Ok(match self {
            Self::File(store) => BackendChanges::File(store.changes(after).await?),
            Self::Etcd(store) => BackendChanges::Etcd(store.changes(after).await?),
            Self::S3(store) => BackendChanges::S3(store.changes(after).await?),
        })
    }
}

/// Why [`connect`] could not build a client for the configured store.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// A TLS file of `[control_store]` is unusable.
    #[error(transparent)]
    Tls(#[from] TlsError),
    /// The store's client refused the configuration.
    #[error("{0}")]
    Invalid(String),
    /// The file store is opened through its node's data directory.
    #[error("the file control store is opened through its node's data directory")]
    Local,
}

/// A client for the etcd or S3 store `config` names. It contacts nothing:
/// the first request connects.
///
/// # Errors
///
/// [`ConnectError`]: a TLS file that cannot be used, a configuration the
/// client refuses (among them a plain `http://` endpoint off loopback,
/// design §12, checked again here whoever built the configuration), or the
/// file backend.
pub async fn connect(config: &ControlStoreConfig) -> Result<Backend, ConnectError> {
    config.check_transport().map_err(ConnectError::Invalid)?;
    match &config.backend {
        ControlStoreBackend::Etcd { endpoints, tls } => {
            let tls = match tls {
                Some(tls) => Some(etcd_tls(tls)?),
                None => None,
            };
            let store = EtcdControlStore::new(EtcdStoreConfig {
                tls,
                ..EtcdStoreConfig::new(endpoints.clone(), config.prefix.clone())
            })
            .map_err(|error| ConnectError::Invalid(error.to_string()))?;
            Ok(Backend::Etcd(store))
        }
        ControlStoreBackend::S3 {
            endpoint,
            bucket,
            region,
            profile,
        } => {
            let target = RemoteTarget {
                endpoint: endpoint.clone(),
                bucket: bucket.clone(),
                prefix: None,
            };
            // The control store's own credential, never a target's
            // (design §6.1), when the configuration names its profile.
            let credentials = match profile {
                Some(profile) => profile_credentials(profile, region),
                None => default_credentials(region).await,
            };
            let objects = AwsS3::builder(&target, region.clone(), credentials)
                .attempt_timeout(S3_REQUEST_TIMEOUT)
                .build();
            let settings = S3StoreConfig {
                prefix: config.prefix.clone(),
                poll_interval: config.config_poll_interval(),
            };
            let store = S3ControlStore::new(objects, settings)
                .map_err(|error| ConnectError::Invalid(error.to_string()))?;
            Ok(Backend::S3(store))
        }
        ControlStoreBackend::File { .. } => Err(ConnectError::Local),
    }
}

/// The client TLS configuration of `https://` etcd endpoints.
fn etcd_tls(tls: &skys3_config::EtcdTls) -> Result<Arc<ClientConfig>, TlsError> {
    let client = tls
        .client
        .as_ref()
        .map(|(cert, key)| (cert.as_path(), key.as_path()));
    tls::client_config(tls.ca_file.as_deref(), client)
}

/// Runs the startup probe against an S3 control store (design §6.1).
///
/// # Errors
///
/// [`OpenStoreError::Refused`] if the store breaks a property the probe
/// checks, and [`OpenStoreError::Unavailable`] if it only failed to
/// answer, so that probing again later may pass.
pub async fn probe<C: ControlStore>(store: &C) -> Result<(), OpenStoreError> {
    match ControlProbe::with_fresh_nonce()
        .run(&[store.clone(), store.clone()])
        .await
    {
        Ok(()) => {
            tracing::info!("the control store passed the startup probe");
            Ok(())
        }
        Err(error) if error.refuses_store() => Err(OpenStoreError::Refused(format!(
            "the control store failed the startup probe, so its conditional writes, \
             conditional deletes, or reads after writes cannot be trusted (design §6.1): {error}"
        ))),
        Err(error) => Err(OpenStoreError::Unavailable(ControlError::Unavailable(
            error.to_string(),
        ))),
    }
}

/// Refuses a store whose `nodes/` registers a node other than `node`: the
/// single-node guard of this build (see the [module
/// documentation](self)).
///
/// # Errors
///
/// [`OpenStoreError::Refused`] naming the other nodes, and
/// [`OpenStoreError::Unavailable`] if the listing fails.
pub async fn check_single_node<C: ControlStore>(
    store: &C,
    node: &NodeId,
    retry: &RetryPolicy,
) -> Result<(), OpenStoreError> {
    let nodes = KeyPrefix::nodes();
    let others: Vec<String> = retrying(retry, || store.list(&nodes))
        .await?
        .into_iter()
        .filter_map(|(key, _)| match key.kind() {
            RegisterKind::Node(other) if other != *node => Some(other.to_string()),
            _ => None,
        })
        .collect();
    if others.is_empty() {
        return Ok(());
    }
    Err(OpenStoreError::Refused(format!(
        "the control store registers node(s) {} besides this node {node}, but this build runs \
         one node per control store: until replication is wired in (plan M8-05b), each node \
         serves every shard of its buckets alone, so nodes sharing a store would serve the \
         same buckets apart. Give each node a store of its own ([control_store] prefix or \
         backend in its configuration), or delete the registration of a node that is gone",
        others.join(", ")
    )))
}

/// Reads the S3 control bucket's versioning at startup, for the gauge and
/// a warning when it is not `Enabled` (design §12): without it, an
/// overwritten or deleted register leaves no history for an audit or a
/// rebuild. `None` if it could not be read, typically because the
/// least-privilege credential lacks `s3:GetBucketVersioning`.
pub async fn read_versioning(store: &Backend) -> Option<BucketVersioning> {
    let Backend::S3(store) = store else {
        return None;
    };
    let bucket = store.objects().bucket().to_owned();
    match store.objects().bucket_versioning().await {
        Ok(BucketVersioning::Enabled) => {
            tracing::info!(%bucket, "the control bucket keeps versions");
            Some(BucketVersioning::Enabled)
        }
        Ok(status) => {
            tracing::warn!(
                %bucket,
                ?status,
                "the control bucket's versioning is not enabled: an overwritten or deleted \
                 register leaves no history for an audit or a rebuild; enable it"
            );
            Some(status)
        }
        Err(error) => {
            tracing::info!(
                %bucket,
                %error,
                "cannot read the control bucket's versioning (it needs s3:GetBucketVersioning)"
            );
            None
        }
    }
}

/// Opens the control store `[control_store]` names, for one node: at
/// startup, and again while the node runs from its copy without a store.
#[derive(Debug)]
pub struct Opener {
    node: NodeId,
    retry: RetryPolicy,
    kind: Kind,
    /// Whether nodes share the store (`[cluster] multi_node`), so that
    /// other nodes' registrations are expected.
    multi_node: bool,
}

#[derive(Debug)]
enum Kind {
    File {
        directory: PathBuf,
        owner: StoreOwner,
        pool: BlockingPool,
    },
    /// An etcd or S3 store's client, built once.
    Shared(Backend),
}

impl Opener {
    /// Opens the file store in `directory` for `owner`, running its I/O on
    /// `pool`.
    #[must_use]
    pub fn file(directory: PathBuf, owner: StoreOwner, pool: BlockingPool) -> Self {
        Self {
            node: owner.node_id().clone(),
            retry: RetryPolicy::default(),
            kind: Kind::File {
                directory,
                owner,
                pool,
            },
            multi_node: false,
        }
    }

    /// Opens `store`, an etcd or S3 store [`connect`] built, for `node`,
    /// one of several nodes sharing it if `multi_node`.
    #[must_use]
    pub fn shared(store: Backend, node: NodeId, multi_node: bool) -> Self {
        Self {
            node,
            retry: RetryPolicy::default(),
            kind: Kind::Shared(store),
            multi_node,
        }
    }

    /// The store the opener opens, if it is shared: its client exists
    /// before the store is opened.
    #[must_use]
    pub fn shared_store(&self) -> Option<&Backend> {
        match &self.kind {
            Kind::Shared(store) => Some(store),
            Kind::File { .. } => None,
        }
    }

    /// Opens the store for the node, which has synced from it before if
    /// `initialized`.
    ///
    /// A file store is opened as [`open_file_store`] opens it. A shared
    /// store must not register another node ([`check_single_node`]) unless
    /// nodes share it (`multi_node`), and an S3 store must pass the
    /// startup probe ([`probe`]). A node that
    /// never synced from a shared store counts as claiming it, so that it
    /// drops no shard on the strength of registers it did not write.
    ///
    /// # Errors
    ///
    /// [`OpenStoreError`].
    pub async fn open(&self, initialized: bool) -> Result<OpenedStore<Backend>, OpenStoreError> {
        match &self.kind {
            Kind::File {
                directory,
                owner,
                pool,
            } => {
                let opened = open_file_store(directory, owner, initialized, pool).await?;
                Ok(OpenedStore {
                    store: Backend::File(opened.store),
                    claimed: opened.claimed,
                })
            }
            Kind::Shared(store) => {
                if !self.multi_node {
                    check_single_node(store, &self.node, &self.retry).await?;
                }
                if let Backend::S3(_) = store {
                    probe(store).await?;
                }
                Ok(OpenedStore {
                    store: store.clone(),
                    claimed: !initialized,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use skys3_control::{
        MemoryControlStore,
        faults::{Fault, FaultyStore},
    };

    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    async fn register(store: &MemoryControlStore, name: &str) {
        let key = RegisterKey::new(format!("nodes/{name}.json")).unwrap();
        let written = store
            .put_if(&key, Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        assert!(matches!(written, PutOutcome::Written(_)));
    }

    /// A control store configuration built in code, past the checks of
    /// configuration loading.
    fn store_config(backend: ControlStoreBackend) -> ControlStoreConfig {
        ControlStoreConfig {
            backend,
            prefix: "c1/".to_owned(),
            allow_correlated_control_store: false,
            coordinator_lease_seconds: 10,
            config_poll_interval_seconds: 1,
        }
    }

    #[tokio::test]
    async fn plain_http_off_loopback_is_never_connected() {
        let s3 = |endpoint: &str| {
            store_config(ControlStoreBackend::S3 {
                endpoint: endpoint.to_owned(),
                bucket: "control".to_owned(),
                region: "us-east-1".to_owned(),
                profile: Some("skys3-control".to_owned()),
            })
        };
        let etcd = |endpoints: &[&str]| {
            store_config(ControlStoreBackend::Etcd {
                endpoints: endpoints.iter().map(|&url| url.to_owned()).collect(),
                tls: None,
            })
        };
        for config in [
            s3("http://10.0.0.5:9000"),
            s3("http://control.example"),
            etcd(&["http://127.0.0.1:2379", "http://etcd-1:2379"]),
        ] {
            let Err(ConnectError::Invalid(message)) = connect(&config).await else {
                panic!("connected over plain HTTP: {config:?}");
            };
            assert!(message.contains("plain HTTP off loopback"), "{message}");
        }
        for config in [
            s3("http://127.0.0.1:9000"),
            s3("http://[::1]:9000"),
            s3("http://localhost:9000"),
            s3("https://control.example"),
            etcd(&["http://127.0.0.1:2379"]),
        ] {
            let backend = connect(&config).await.unwrap();
            assert!(matches!(backend.kind(), "s3" | "etcd"), "{config:?}");
        }
    }

    #[tokio::test]
    async fn a_store_that_registers_another_node_is_refused() {
        let store = MemoryControlStore::new();
        let retry = RetryPolicy::default();
        check_single_node(&store, &node("n1"), &retry)
            .await
            .unwrap();
        // The node's own registration is no other node's.
        register(&store, "n1").await;
        check_single_node(&store, &node("n1"), &retry)
            .await
            .unwrap();
        register(&store, "n2").await;
        register(&store, "n3").await;
        let error = check_single_node(&store, &node("n1"), &retry)
            .await
            .unwrap_err();
        let OpenStoreError::Refused(message) = &error else {
            panic!("{error}");
        };
        assert!(
            message.contains("node(s) n2, n3 besides this node n1"),
            "{message}"
        );
        assert!(message.contains("[control_store] prefix"), "{message}");
        assert!(message.contains("M8-05b"), "{message}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_that_does_not_answer_is_tried_again_and_a_broken_one_refused() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let retry = RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        };
        store.script([Fault::Unavailable, Fault::Unavailable]);
        let error = check_single_node(&store, &node("n1"), &retry)
            .await
            .unwrap_err();
        assert!(matches!(error, OpenStoreError::Unavailable(_)), "{error}");

        // Both racing writers exhaust their retries in the first round.
        let attempts = RetryPolicy::default().max_attempts as usize;
        store.script(vec![Fault::Unavailable; 2 * attempts]);
        let error = probe(&store).await.unwrap_err();
        assert!(matches!(error, OpenStoreError::Unavailable(_)), "{error}");
        // An error that is not retried breaks the probe for good.
        store.script([Fault::Fail]);
        let error = probe(&store).await.unwrap_err();
        let OpenStoreError::Refused(message) = &error else {
            panic!("{error}");
        };
        assert!(message.contains("failed the startup probe"), "{message}");
        probe(&MemoryControlStore::new()).await.unwrap();
    }
}
