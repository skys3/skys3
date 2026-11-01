//! `[cluster]` and `[control_store]` (§6.1, §6.2, §6.7).

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use skys3_types::{ClusterId, RemoteTarget};

use crate::error::Checker;
use crate::target;

/// The placement level at which shard members and fragments are kept
/// apart (§6.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomain {
    /// At most one member of a shard per node.
    #[default]
    Node,
    /// At most one member of a shard per rack.
    Rack,
    /// At most one member of a shard per zone.
    Zone,
}

/// `[cluster]`: the cluster's identity and placement level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// `cluster_id`: the cluster's ID, which appears in every write
    /// identity (§7.2).
    pub cluster_id: ClusterId,
    /// `failure_domain`: the level placement must respect.
    pub failure_domain: FailureDomain,
    /// `multi_node`: whether the node runs as one of several nodes of the
    /// cluster (§6.2): it binds the intra-cluster transport, registers in
    /// `nodes/`, sends heartbeats, and receives pushed generations. Off by
    /// default, until the cluster is complete in the binary (plan M8-09).
    pub multi_node: bool,
    /// `coordinator`: whether the node may be elected coordinator (§6.7):
    /// it contends for `coordinator.lease` and, while it holds the lease,
    /// places, replaces, and rebalances shards. A node without it never
    /// contends, and follows whichever node holds the lease. Needs
    /// `multi_node`; off by default, until plan M8-09.
    pub coordinator: bool,
}

/// `[cluster]` as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCluster {
    cluster_id: String,
    #[serde(default)]
    failure_domain: FailureDomain,
    #[serde(default)]
    pub(crate) multi_node: bool,
    #[serde(default)]
    coordinator: bool,
}

impl RawCluster {
    pub(crate) fn resolve(&self, checker: &mut Checker) -> Option<ClusterConfig> {
        checker.require(
            self.multi_node || !self.coordinator,
            "cluster.coordinator",
            || {
                "needs [cluster] multi_node = true: a single node has nothing to coordinate"
                    .to_owned()
            },
        );
        match ClusterId::new(self.cluster_id.as_str()) {
            Ok(cluster_id) => Some(ClusterConfig {
                cluster_id,
                failure_domain: self.failure_domain,
                multi_node: self.multi_node,
                coordinator: self.coordinator,
            }),
            Err(error) => {
                checker.report("cluster.cluster_id", error);
                None
            }
        }
    }
}

/// Which kind of control store holds the registers (§6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BackendKind {
    #[default]
    Etcd,
    S3,
    File,
}

/// The control-store backend and where it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlStoreBackend {
    /// An etcd v3 cluster, the production default.
    Etcd {
        /// `etcd_endpoints`: client URLs, without a path. A plain `http://`
        /// one is on loopback (§12).
        endpoints: Vec<String>,
        /// TLS for `https://` endpoints: `Some` exactly when there is one.
        tls: Option<EtcdTls>,
    },
    /// An S3-compatible store that passes the conditional-write probe.
    S3 {
        /// `endpoint`: the S3 endpoint URL, without a path.
        endpoint: String,
        /// `bucket`: the control bucket.
        bucket: String,
        /// `region`: the region requests are signed for.
        region: String,
        /// `profile`: the profile of the shared AWS configuration the
        /// control store's credentials come from; `None` for the default
        /// credential chain.
        profile: Option<String>,
    },
    /// A directory on this node, for a single-node cluster. It refuses to
    /// serve a second node.
    File {
        /// `directory`: where the registers are kept. Defaults to
        /// `<data_dir>/control`.
        directory: PathBuf,
    },
}

/// How a node reaches `https://` etcd endpoints (§12): the CA that
/// verifies etcd's certificates, and the client certificate etcd's
/// `--client-cert-auth` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdTls {
    /// `etcd_tls_ca_file`: PEM CA certificates; `None` for the system's
    /// root certificates.
    pub ca_file: Option<PathBuf>,
    /// `etcd_tls_cert_file` and `etcd_tls_key_file`: the client's PEM
    /// certificate chain, leaf first, and its private key.
    pub client: Option<(PathBuf, PathBuf)>,
}

/// The region an S3 control store signs for unless `region` is set.
pub const DEFAULT_CONTROL_REGION: &str = "us-east-1";

/// `[control_store]`: where the cluster's registers live (§6.1) and how
/// often nodes and the coordinator talk to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStoreConfig {
    /// `backend` with its endpoints.
    pub backend: ControlStoreBackend,
    /// `prefix`: the key prefix of every register, ending in `/`. Defaults
    /// to the cluster ID followed by `/`.
    pub prefix: String,
    /// `allow_correlated_control_store`: accept an S3 control store in the
    /// failure scope of a data target.
    pub allow_correlated_control_store: bool,
    /// `coordinator_lease_seconds`.
    pub coordinator_lease_seconds: u64,
    /// `config_poll_interval_seconds`.
    pub config_poll_interval_seconds: u64,
}

impl ControlStoreConfig {
    /// The longest `prefix`, in bytes: S3's 1,024-byte limit on object
    /// keys, less the longest register key (512 bytes), so that every
    /// register fits under the prefix (§6.1). `skys3-control` checks that
    /// its S3 backend uses the same bound.
    pub const MAX_PREFIX_LEN: usize = 512;

    /// Checks a data target against an S3 control store (§6.1), as
    /// configuration loading does for backup and snapshot targets and
    /// attaching a bucket does for its target.
    ///
    /// - The target must not hold keys under the control prefix, nor the
    ///   control prefix keys of the target: in one bucket, one prefix must
    ///   not start with the other. Endpoints of one host or address reach
    ///   the same buckets, and so do AWS S3 endpoints of every region of
    ///   one partition, since AWS bucket names are unique per partition
    ///   (`aws`, `aws-cn`, `aws-us-gov`). Otherwise a credential for the target
    ///   would also reach the registers. This holds even with
    ///   `allow_correlated_control_store`.
    /// - The target must not share the control store's failure scope: the
    ///   host name or IP address of its endpoint, or the region of an AWS
    ///   S3 endpoint, unless `allow_correlated_control_store` is set.
    ///
    /// An etcd control store passes every target.
    ///
    /// # Errors
    ///
    /// A message naming the shared bucket or scope.
    pub fn check_target_independence(&self, target: &RemoteTarget) -> Result<(), String> {
        let ControlStoreBackend::S3 {
            endpoint, bucket, ..
        } = &self.backend
        else {
            return Ok(());
        };
        let Some(scope) = target::failure_scope(endpoint) else {
            return Ok(());
        };
        let target_scope = target::failure_scope(&target.endpoint);
        let target_prefix = target.prefix.as_deref().unwrap_or_default();
        // AWS bucket names are unique per partition: an endpoint of any
        // region of the partition reaches the same bucket, so a control
        // bucket named through another region's endpoint is still the
        // control bucket.
        let same_buckets = target_scope.as_deref().is_some_and(|other| {
            target::bucket_namespace(other) == target::bucket_namespace(&scope)
        });
        if same_buckets
            && target.bucket == *bucket
            && (self.prefix.starts_with(target_prefix) || target_prefix.starts_with(&self.prefix))
        {
            return Err(format!(
                "the target's keys overlap the control store's, {bucket}/{}; a credential for \
                 the target would reach the registers (§6.1). Give the control store a prefix \
                 of its own",
                self.prefix
            ));
        }
        if target_scope.as_ref() != Some(&scope) || self.allow_correlated_control_store {
            return Ok(());
        }
        Err(format!(
            "the target shares its failure scope ({scope}) with the control store; one outage \
             would stop both flushing and membership changes (§6.1). Move the control store, \
             or set allow_correlated_control_store = true"
        ))
    }

    /// Checks that no endpoint of the backend is plain `http://` off
    /// loopback (§12), as configuration loading does. For callers that
    /// build a configuration themselves, and to check again before a store
    /// is opened.
    ///
    /// Whoever can write the store can reassign shards, and an S3 store
    /// authenticates only the requests (SigV4 covers no answer): on the
    /// path of a plain-HTTP store, anyone could forge register reads and
    /// listings, or acknowledge writes that never happened.
    ///
    /// # Errors
    ///
    /// A message naming the first such endpoint.
    pub fn check_transport(&self) -> Result<(), String> {
        let endpoints = match &self.backend {
            ControlStoreBackend::Etcd { endpoints, .. } => endpoints.as_slice(),
            ControlStoreBackend::S3 { endpoint, .. } => std::slice::from_ref(endpoint),
            ControlStoreBackend::File { .. } => &[],
        };
        match endpoints
            .iter()
            .find(|endpoint| target::plaintext_off_loopback(endpoint))
        {
            Some(endpoint) => Err(plaintext_refusal(endpoint)),
            None => Ok(()),
        }
    }
}

/// Why a plain `http://` control-store endpoint off loopback is refused
/// (§12).
fn plaintext_refusal(url: &str) -> String {
    format!(
        "{url:?} is plain HTTP off loopback, where anyone on the network path could forge the \
         store's answers or rewrite the registers; use https://"
    )
}

crate::durations! {
    ControlStoreConfig {
        /// The coordinator lease; the holder renews it every third of this
        /// (§6.7).
        coordinator_lease => coordinator_lease_seconds, Duration::from_secs;
        /// How often a node polls `cluster.json` as a backstop to pushes
        /// (§6.2).
        config_poll_interval => config_poll_interval_seconds, Duration::from_secs;
    }
}

/// `[control_store]` as written.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawControlStore {
    backend: BackendKind,
    etcd_endpoints: Option<Vec<String>>,
    etcd_tls_ca_file: Option<PathBuf>,
    etcd_tls_cert_file: Option<PathBuf>,
    etcd_tls_key_file: Option<PathBuf>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    profile: Option<String>,
    directory: Option<PathBuf>,
    prefix: Option<String>,
    allow_correlated_control_store: bool,
    coordinator_lease_seconds: u64,
    config_poll_interval_seconds: u64,
}

impl Default for RawControlStore {
    fn default() -> Self {
        Self {
            backend: BackendKind::Etcd,
            etcd_endpoints: None,
            etcd_tls_ca_file: None,
            etcd_tls_cert_file: None,
            etcd_tls_key_file: None,
            endpoint: None,
            bucket: None,
            region: None,
            profile: None,
            directory: None,
            prefix: None,
            allow_correlated_control_store: false,
            coordinator_lease_seconds: 10,
            config_poll_interval_seconds: 30,
        }
    }
}

impl RawControlStore {
    /// Checks every key of the section and resolves it. `cluster_id` is
    /// `None` when the cluster ID is itself invalid. A value that breaks a
    /// rule, or the default prefix without a cluster ID, is left empty; the
    /// violation is reported, so the result is never used as a [`Config`].
    ///
    /// [`Config`]: crate::Config
    pub(crate) fn resolve(
        &self,
        cluster_id: Option<&ClusterId>,
        data_dir: &Path,
        checker: &mut Checker,
    ) -> ControlStoreConfig {
        if self.backend != BackendKind::File && self.directory.is_some() {
            checker.report(
                "control_store.directory",
                "applies only to backend = \"file\"",
            );
        }
        let backend = match self.backend {
            BackendKind::Etcd => self.etcd_backend(checker),
            BackendKind::S3 => self.s3_backend(checker),
            BackendKind::File => self.file_backend(data_dir, checker),
        };
        let prefix = match &self.prefix {
            Some(prefix) => {
                let valid = prefix.len() > 1
                    && prefix.len() <= ControlStoreConfig::MAX_PREFIX_LEN
                    && prefix.ends_with('/')
                    && !prefix.starts_with('/')
                    && prefix.bytes().all(|b| b.is_ascii_graphic());
                checker.require(valid, "control_store.prefix", || {
                    format!(
                        "{prefix:?} must be at most {} bytes of visible ASCII that ends with '/' \
                         and does not start with it",
                        ControlStoreConfig::MAX_PREFIX_LEN
                    )
                });
                prefix.clone()
            }
            None => cluster_id.map(|id| format!("{id}/")).unwrap_or_default(),
        };
        checker.nonzero(
            "control_store.coordinator_lease_seconds",
            self.coordinator_lease_seconds,
        );
        checker.nonzero(
            "control_store.config_poll_interval_seconds",
            self.config_poll_interval_seconds,
        );
        ControlStoreConfig {
            backend,
            prefix,
            allow_correlated_control_store: self.allow_correlated_control_store,
            coordinator_lease_seconds: self.coordinator_lease_seconds,
            config_poll_interval_seconds: self.config_poll_interval_seconds,
        }
    }

    fn etcd_backend(&self, checker: &mut Checker) -> ControlStoreBackend {
        const ENDPOINTS: &str = "control_store.etcd_endpoints";
        self.refuse_other_keys(ETCD_KEYS, "etcd", checker);
        let urls = self.etcd_endpoints.as_deref().unwrap_or_default();
        checker.require(!urls.is_empty(), ENDPOINTS, || {
            "backend = \"etcd\" needs at least one endpoint".to_owned()
        });
        let mut endpoints = Vec::with_capacity(urls.len());
        for url in urls {
            match target::parse_endpoint(url) {
                Ok(endpoint) if target::plaintext_off_loopback(&endpoint) => {
                    checker.report(ENDPOINTS, plaintext_refusal(url));
                }
                Ok(endpoint) => endpoints.push(endpoint),
                Err(error) => checker.report(ENDPOINTS, error),
            }
        }
        let https = endpoints.iter().any(|url| url.starts_with("https://"));
        let tls = if https {
            if let Some(ca_file) = &self.etcd_tls_ca_file {
                crate::node::nonempty_path(checker, "control_store.etcd_tls_ca_file", ca_file);
            }
            Some(EtcdTls {
                ca_file: self.etcd_tls_ca_file.clone(),
                client: self.etcd_client_certificate(checker),
            })
        } else {
            let set = [
                ("etcd_tls_ca_file", self.etcd_tls_ca_file.is_some()),
                ("etcd_tls_cert_file", self.etcd_tls_cert_file.is_some()),
                ("etcd_tls_key_file", self.etcd_tls_key_file.is_some()),
            ];
            for (key, _) in set.into_iter().filter(|(_, set)| *set) {
                checker.report(
                    format!("control_store.{key}"),
                    "applies only to https:// endpoints",
                );
            }
            None
        };
        ControlStoreBackend::Etcd { endpoints, tls }
    }

    /// `etcd_tls_cert_file` and `etcd_tls_key_file`, which are set together.
    fn etcd_client_certificate(&self, checker: &mut Checker) -> Option<(PathBuf, PathBuf)> {
        match (&self.etcd_tls_cert_file, &self.etcd_tls_key_file) {
            (Some(cert), Some(key)) => {
                crate::node::nonempty_path(checker, "control_store.etcd_tls_cert_file", cert);
                crate::node::nonempty_path(checker, "control_store.etcd_tls_key_file", key);
                Some((cert.clone(), key.clone()))
            }
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                checker.report(
                    "control_store.etcd_tls_cert_file",
                    "etcd_tls_cert_file and etcd_tls_key_file are set together",
                );
                None
            }
        }
    }

    /// Reports every backend-specific key `allowed` does not name.
    fn refuse_other_keys(&self, allowed: &[&str], backend: &str, checker: &mut Checker) {
        let keys = [
            ("etcd_endpoints", self.etcd_endpoints.is_some()),
            ("etcd_tls_ca_file", self.etcd_tls_ca_file.is_some()),
            ("etcd_tls_cert_file", self.etcd_tls_cert_file.is_some()),
            ("etcd_tls_key_file", self.etcd_tls_key_file.is_some()),
            ("endpoint", self.endpoint.is_some()),
            ("bucket", self.bucket.is_some()),
            ("region", self.region.is_some()),
            ("profile", self.profile.is_some()),
        ];
        for (key, set) in keys {
            if set && !allowed.contains(&key) {
                checker.report(
                    format!("control_store.{key}"),
                    format!("does not apply to backend = \"{backend}\""),
                );
            }
        }
    }

    fn file_backend(&self, data_dir: &Path, checker: &mut Checker) -> ControlStoreBackend {
        self.refuse_other_keys(&[], "file", checker);
        let directory = match &self.directory {
            Some(directory) => {
                crate::node::nonempty_path(checker, "control_store.directory", directory);
                directory.clone()
            }
            None => data_dir.join("control"),
        };
        ControlStoreBackend::File { directory }
    }

    fn s3_backend(&self, checker: &mut Checker) -> ControlStoreBackend {
        self.refuse_other_keys(&["endpoint", "bucket", "region", "profile"], "s3", checker);
        let endpoint = match self.endpoint.as_deref().map(target::parse_endpoint) {
            Some(Ok(endpoint)) if target::plaintext_off_loopback(&endpoint) => {
                checker.report("control_store.endpoint", plaintext_refusal(&endpoint));
                endpoint
            }
            Some(Ok(endpoint)) => endpoint,
            Some(Err(error)) => {
                checker.report("control_store.endpoint", error);
                String::new()
            }
            None => {
                checker.report(
                    "control_store.endpoint",
                    "backend = \"s3\" needs the S3 endpoint URL",
                );
                String::new()
            }
        };
        let bucket = self.bucket.clone().unwrap_or_default();
        let bucket_ok =
            !bucket.is_empty() && bucket.bytes().all(|b| b.is_ascii_graphic() && b != b'/');
        checker.require(bucket_ok, "control_store.bucket", || {
            "backend = \"s3\" needs the control bucket's name, in visible ASCII without '/'"
                .to_owned()
        });
        let region = self
            .region
            .clone()
            .unwrap_or_else(|| DEFAULT_CONTROL_REGION.to_owned());
        checker.require(
            !region.is_empty()
                && region
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "control_store.region",
            || format!("is {region:?}; a region is ASCII letters, numbers, and '-'"),
        );
        if let Some(profile) = &self.profile {
            checker.require(
                crate::buckets::valid_profile(profile),
                "control_store.profile",
                || {
                    format!(
                        "must be 1 to {} visible ASCII characters other than `[` and `]`",
                        crate::buckets::MAX_ORIGIN_PROFILE_BYTES
                    )
                },
            );
        }
        ControlStoreBackend::S3 {
            endpoint,
            bucket,
            region,
            profile: self.profile.clone(),
        }
    }
}

/// The keys of `backend = "etcd"`.
const ETCD_KEYS: &[&str] = &[
    "etcd_endpoints",
    "etcd_tls_ca_file",
    "etcd_tls_cert_file",
    "etcd_tls_key_file",
];
