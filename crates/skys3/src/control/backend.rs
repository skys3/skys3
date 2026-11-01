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
//! **Shared stores make a cluster.** The file store serves one node, a
//! single node that serves each of its shards alone, and refuses a second
//! node itself. The nodes over an etcd or S3 store are the nodes of a
//! cluster (plan M8-09): they register in it, and registration refuses only
//! a registration of the node's own ID from another data directory
//! (`crate::cluster::registration`).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use rustls::ClientConfig;
use skys3_config::{ControlStoreBackend, ControlStoreConfig};
use skys3_control::{
    Change, ChangeFeed, ChangeStream, ControlError, ControlProbe, ControlStore, DeleteOutcome,
    EtcdControlStore, EtcdStoreConfig, Expected, FileControlStore, KeyPrefix, PutOutcome,
    RegisterKey, S3Changes, S3ControlStore, S3StoreConfig, Version, Versioned,
};
use skys3_io::BlockingPool;
use skys3_remote::aws::{AwsS3, BucketVersioning, default_credentials, profile_credentials};
use skys3_types::{Generation, RemoteTarget};

use super::{OpenStoreError, OpenedStore, StoreOwner, open_file_store};
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
    kind: Kind,
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
            kind: Kind::File {
                directory,
                owner,
                pool,
            },
        }
    }

    /// Opens `store`, an etcd or S3 store [`connect`] built, which the
    /// node shares with the other nodes of its cluster.
    #[must_use]
    pub fn shared(store: Backend) -> Self {
        Self {
            kind: Kind::Shared(store),
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
    /// A file store is opened as [`open_file_store`] opens it, and an S3
    /// store must pass the startup probe ([`probe`]). A node that never
    /// synced from a shared store counts as claiming it, so that it drops
    /// no shard on the strength of registers it did not write.
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

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_gets_no_answer_is_tried_again_and_a_broken_store_refused() {
        let store = FaultyStore::new(MemoryControlStore::new());
        // Both racing writers exhaust their retries in the first round.
        let attempts = skys3_control::RetryPolicy::default().max_attempts as usize;
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
