#![forbid(unsafe_code)]
//! SkyS3 node configuration: the TOML schema of design §14, its defaults,
//! and the rules checked when it is loaded.
//!
//! Section numbers (§) refer to the [SkyS3 design], and every key is listed
//! in the [configuration reference].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//! [configuration reference]: https://github.com/skys3/skys3/blob/main/docs/skys3-config.md
//!
//! Loading runs in two stages:
//!
//! 1. **Parsing.** The text must be TOML, every value must have its key's
//!    type, and every key must be known: a misspelled key is an error, never
//!    silently ignored. Parsing stops at the first problem and reports it
//!    with its key and line ([`ConfigError::Parse`]).
//! 2. **Validation.** Every rule is checked, and all broken rules are
//!    reported together, each at its dotted key path
//!    ([`ConfigError::Invalid`]).
//!
//! A [`Config`] exists only once both stages pass, so its values always
//! satisfy the rules. Durations are integers whose key names their unit
//! (`_ms`, `_seconds`, `_hours`), as in §14; each section also returns them
//! as [`Duration`]s.
//!
//! ```
//! use skys3_config::Config;
//!
//! let config: Config = r#"
//!     [cluster]
//!     cluster_id = "skys3-prod-a"
//!
//!     [control_store]
//!     etcd_endpoints = ["https://etcd-1.example.internal:2379"]
//! "#
//! .parse()?;
//! assert_eq!(config.control_store().prefix, "skys3-prod-a/");
//! assert_eq!(config.replication().primary_lease().as_secs(), 4);
//! # Ok::<(), skys3_config::ConfigError>(())
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use skys3_types::BucketName;

/// Adds accessors that return integer duration fields as [`Duration`]s.
macro_rules! durations {
    ($section:ty { $( $(#[$doc:meta])* $name:ident => $field:ident, $unit:path; )* }) => {
        impl $section {
            $(
                $(#[$doc])*
                #[must_use]
                pub fn $name(&self) -> Duration {
                    $unit(self.$field)
                }
            )*
        }
    };
}
pub(crate) use durations;

mod admin;
mod buckets;
mod cluster;
mod ec;
mod error;
mod flush;
mod identity;
mod node;
mod peering;
mod replication;
mod storage;
mod target;

pub use admin::{AdminConfig, LogFormat, LoggingConfig};
pub use buckets::{BucketSettings, BucketsConfig, TargetTransport};
pub use cluster::{ClusterConfig, ControlStoreBackend, ControlStoreConfig, FailureDomain};
pub use ec::EcConfig;
pub use error::{ConfigError, Violation, Violations};
pub use flush::{AckPolicy, ConflictPolicy, FlushConfig};
pub use identity::{IdentityConfig, StaticCredentialConfig};
pub use node::{GatewayListenConfig, NodeConfig};
pub use peering::{CongestionControl, PeeringConfig};
pub use replication::{AckTimeoutMode, ReplicationConfig};
pub use storage::{CacheConfig, StorageConfig};
pub use target::parse_target;

use buckets::BucketTable;
use cluster::{RawCluster, RawControlStore};
use error::Checker;
use node::RawNode;

/// A duration of `hours` hours, saturating.
pub(crate) const fn hours(hours: u64) -> Duration {
    Duration::from_secs(hours.saturating_mul(3600))
}

/// A validated node configuration.
///
/// Built only by [`Config::load`], [`Config::from_toml_str`], or
/// [`str::parse`], so every rule holds. Sections are read through
/// accessors and cannot be changed afterwards.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    cluster: ClusterConfig,
    node: NodeConfig,
    gateway: GatewayListenConfig,
    control_store: ControlStoreConfig,
    replication: ReplicationConfig,
    storage: StorageConfig,
    cache: CacheConfig,
    flush: FlushConfig,
    ec: EcConfig,
    buckets: BucketsConfig,
    peering: PeeringConfig,
    identity: IdentityConfig,
    admin: AdminConfig,
    logging: LoggingConfig,
}

/// The file as written. Only `[cluster]` is required.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    cluster: RawCluster,
    #[serde(default)]
    node: RawNode,
    #[serde(default)]
    gateway: GatewayListenConfig,
    #[serde(default)]
    control_store: RawControlStore,
    #[serde(default)]
    replication: ReplicationConfig,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    cache: CacheConfig,
    #[serde(default)]
    flush: FlushConfig,
    #[serde(default)]
    ec: EcConfig,
    #[serde(default)]
    buckets: BTreeMap<String, BucketTable>,
    #[serde(default)]
    peering: PeeringConfig,
    #[serde(default)]
    identity: IdentityConfig,
    #[serde(default)]
    admin: AdminConfig,
    #[serde(default)]
    logging: LoggingConfig,
}

impl Config {
    /// Reads, parses, and validates the configuration file at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Read`] if the file cannot be read, and
    /// otherwise the errors of [`Config::from_toml_str`].
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml_str(&text)
    }

    /// Parses and validates a configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] for malformed TOML, a value of the
    /// wrong type, a missing `[cluster]` or `cluster_id`, or an unknown key,
    /// and [`ConfigError::Invalid`] with every broken rule.
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(text)?;
        Ok(Self::validate(raw)?)
    }

    fn validate(raw: RawConfig) -> Result<Self, Violations> {
        let mut checker = Checker::default();
        let cluster = raw.cluster.resolve(&mut checker);
        let cluster_id = cluster.as_ref().map(|cluster| &cluster.cluster_id);
        let node = raw.node.resolve(&mut checker);
        raw.gateway.check(&mut checker);
        let control_store = raw
            .control_store
            .resolve(cluster_id, &node.data_dir, &mut checker);
        raw.replication.check(&mut checker);
        raw.storage.check(&mut checker);
        raw.cache.check(&mut checker);
        raw.flush.check(&mut checker);
        raw.ec.check(&mut checker);
        let (bucket_defaults, named_buckets) =
            buckets::resolve(&raw.buckets, &raw.flush, cluster_id, &mut checker);
        raw.peering.check(&mut checker);
        raw.identity.check(&mut checker);
        raw.admin.check(&mut checker);
        check_control_store_independence(&control_store, &named_buckets, &mut checker);
        checker.finish()?;

        // Each resolver returns `None` only after reporting a violation.
        let (Some(cluster), Some(defaults)) = (cluster, bucket_defaults) else {
            unreachable!("a section failed to resolve without reporting a violation");
        };
        let buckets = BucketsConfig {
            defaults,
            named: named_buckets,
        };
        Ok(Self {
            cluster,
            node,
            gateway: raw.gateway,
            control_store,
            replication: raw.replication,
            storage: raw.storage,
            cache: raw.cache,
            flush: raw.flush,
            ec: raw.ec,
            buckets,
            peering: raw.peering,
            identity: raw.identity,
            admin: raw.admin,
            logging: raw.logging,
        })
    }

    /// `[cluster]`.
    #[must_use]
    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// `[node]`.
    #[must_use]
    pub fn node(&self) -> &NodeConfig {
        &self.node
    }

    /// `[gateway]`.
    #[must_use]
    pub fn gateway(&self) -> &GatewayListenConfig {
        &self.gateway
    }

    /// `[control_store]`.
    #[must_use]
    pub fn control_store(&self) -> &ControlStoreConfig {
        &self.control_store
    }

    /// `[replication]`.
    #[must_use]
    pub fn replication(&self) -> &ReplicationConfig {
        &self.replication
    }

    /// `[storage]`.
    #[must_use]
    pub fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    /// `[cache]`.
    #[must_use]
    pub fn cache(&self) -> &CacheConfig {
        &self.cache
    }

    /// `[flush]`.
    #[must_use]
    pub fn flush(&self) -> &FlushConfig {
        &self.flush
    }

    /// `[ec]`.
    #[must_use]
    pub fn ec(&self) -> &EcConfig {
        &self.ec
    }

    /// `[buckets.defaults]` and every `[buckets.<name>]`, resolved.
    #[must_use]
    pub fn buckets(&self) -> &BucketsConfig {
        &self.buckets
    }

    /// `[peering]`.
    #[must_use]
    pub fn peering(&self) -> &PeeringConfig {
        &self.peering
    }

    /// `[identity]`.
    #[must_use]
    pub fn identity(&self) -> &IdentityConfig {
        &self.identity
    }

    /// `[admin]`.
    #[must_use]
    pub fn admin(&self) -> &AdminConfig {
        &self.admin
    }

    /// `[logging]`.
    #[must_use]
    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }
}

impl FromStr for Config {
    type Err = ConfigError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::from_toml_str(text)
    }
}

/// Refuses an S3 control store in the failure scope of a configured backup
/// target, unless `allow_correlated_control_store` is set (§6.1).
///
/// Write-back targets are bound when a bucket is attached, not here; the
/// attach path applies the same check to them.
fn check_control_store_independence(
    control_store: &ControlStoreConfig,
    buckets: &BTreeMap<BucketName, BucketSettings>,
    checker: &mut Checker,
) {
    let ControlStoreBackend::S3 { endpoint, .. } = &control_store.backend else {
        return;
    };
    if control_store.allow_correlated_control_store {
        return;
    }
    // An invalid endpoint is already reported and has no scope.
    let Some(scope) = target::failure_scope(endpoint) else {
        return;
    };
    for (name, settings) in buckets {
        let Some(backup) = &settings.backup_target else {
            continue;
        };
        if target::failure_scope(&backup.endpoint).as_ref() == Some(&scope) {
            checker.report(
                "control_store.endpoint",
                format!(
                    "shares its failure scope ({scope}) with the backup target of bucket \
                     {name}; one outage would stop both flushing and membership changes \
                     (§6.1). Move the control store, or set allow_correlated_control_store = true"
                ),
            );
        }
    }
}
