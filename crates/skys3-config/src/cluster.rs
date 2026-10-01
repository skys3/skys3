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
}

/// `[cluster]` as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCluster {
    cluster_id: String,
    #[serde(default)]
    failure_domain: FailureDomain,
}

impl RawCluster {
    pub(crate) fn resolve(&self, checker: &mut Checker) -> Option<ClusterConfig> {
        match ClusterId::new(self.cluster_id.as_str()) {
            Ok(cluster_id) => Some(ClusterConfig {
                cluster_id,
                failure_domain: self.failure_domain,
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
        /// `etcd_endpoints`: client URLs, without a path.
        endpoints: Vec<String>,
    },
    /// An S3-compatible store that passes the conditional-write probe.
    S3 {
        /// `endpoint`: the S3 endpoint URL, without a path.
        endpoint: String,
        /// `bucket`: the control bucket.
        bucket: String,
    },
    /// A directory on this node, for a single-node cluster. It refuses to
    /// serve a second node.
    File {
        /// `directory`: where the registers are kept. Defaults to
        /// `<data_dir>/control`.
        directory: PathBuf,
    },
}

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
    /// Checks a data target against an S3 control store (§6.1), as
    /// configuration loading does for backup and snapshot targets and
    /// attaching a bucket does for its target.
    ///
    /// - The target must not hold keys under the control prefix, nor the
    ///   control prefix keys of the target: in one bucket, one prefix must
    ///   not start with the other. Otherwise a credential for the target
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
        let ControlStoreBackend::S3 { endpoint, bucket } = &self.backend else {
            return Ok(());
        };
        let Some(scope) = target::failure_scope(endpoint) else {
            return Ok(());
        };
        if target::failure_scope(&target.endpoint).as_ref() != Some(&scope) {
            return Ok(());
        }
        let target_prefix = target.prefix.as_deref().unwrap_or_default();
        if target.bucket == *bucket
            && (self.prefix.starts_with(target_prefix) || target_prefix.starts_with(&self.prefix))
        {
            return Err(format!(
                "the target's keys overlap the control store's, {bucket}/{}; a credential for \
                 the target would reach the registers (§6.1). Give the control store a prefix \
                 of its own",
                self.prefix
            ));
        }
        if self.allow_correlated_control_store {
            return Ok(());
        }
        Err(format!(
            "the target shares its failure scope ({scope}) with the control store; one outage \
             would stop both flushing and membership changes (§6.1). Move the control store, \
             or set allow_correlated_control_store = true"
        ))
    }
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
    endpoint: Option<String>,
    bucket: Option<String>,
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
            endpoint: None,
            bucket: None,
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
                    && prefix.ends_with('/')
                    && !prefix.starts_with('/')
                    && prefix.bytes().all(|b| b.is_ascii_graphic());
                checker.require(valid, "control_store.prefix", || {
                    format!(
                        "{prefix:?} must be visible ASCII that ends with '/' and does not start \
                         with it"
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
        self.refuse_other_keys(&["etcd_endpoints"], "etcd", checker);
        let urls = self.etcd_endpoints.as_deref().unwrap_or_default();
        checker.require(!urls.is_empty(), "control_store.etcd_endpoints", || {
            "backend = \"etcd\" needs at least one endpoint".to_owned()
        });
        let mut endpoints = Vec::with_capacity(urls.len());
        for url in urls {
            match target::parse_endpoint(url) {
                Ok(endpoint) => endpoints.push(endpoint),
                Err(error) => checker.report("control_store.etcd_endpoints", error),
            }
        }
        ControlStoreBackend::Etcd { endpoints }
    }

    /// Reports `etcd_endpoints`, `endpoint`, and `bucket` unless `allowed`
    /// names them.
    fn refuse_other_keys(&self, allowed: &[&str], backend: &str, checker: &mut Checker) {
        let keys = [
            ("etcd_endpoints", self.etcd_endpoints.is_some()),
            ("endpoint", self.endpoint.is_some()),
            ("bucket", self.bucket.is_some()),
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
        self.refuse_other_keys(&["endpoint", "bucket"], "s3", checker);
        let endpoint = match self.endpoint.as_deref().map(target::parse_endpoint) {
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
        ControlStoreBackend::S3 { endpoint, bucket }
    }
}
