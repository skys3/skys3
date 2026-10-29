//! `[buckets.defaults]` and `[buckets.<name>]`: per-bucket settings (§4.1,
//! §7.2, §7.5, §7.6, §7.8, §8, §9.1).
//!
//! Settings resolve in three layers: built-in defaults, then
//! `[buckets.defaults]`, then the `[buckets.<name>]` table of the bucket with
//! that S3 name. `ack_policy` and `flush_conflict_policy` take their
//! defaults from `[flush]` and may be overridden only per bucket;
//! `max_dirty_bytes` defaults to `flush.max_dirty_bytes`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use skys3_types::{
    BucketMode, BucketName, ClusterId, RemoteTarget, ReplicationError, ReplicationSettings,
    ShardCount,
};

use crate::error::{Checker, key_path};
use crate::flush::{AckPolicy, ConflictPolicy, FlushConfig};
use crate::storage::MIB;
use crate::target;

/// Where a bucket's flusher sends data to a target (§7.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetTransport {
    /// The native peer protocol when the target is a SkyS3 cluster that
    /// answers in time, otherwise S3 REST.
    #[default]
    Auto,
    /// The native peer protocol only.
    Native,
    /// S3 REST only.
    S3,
}

/// The resolved settings of one bucket, or of every bucket without a table
/// of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSettings {
    /// `mode`: the bucket's mode, fixed at creation.
    pub mode: BucketMode,
    /// `shards_per_bucket`: the bucket's shard count, fixed at creation.
    pub shards_per_bucket: ShardCount,
    /// `replicas`, `min_write_replicas`, and `clean_copies`.
    pub replication: ReplicationSettings,
    /// `import_parallel_streams`: listing streams of a namespace import
    /// (§9.1).
    pub import_parallel_streams: u32,
    /// `ec_min_object_bytes`: the smallest object that is erasure-coded
    /// (§8.2).
    pub ec_min_object_bytes: u64,
    /// `ec_stripe_data_bytes`: the data bytes per stripe (§8.2).
    pub ec_stripe_data_bytes: u64,
    /// `ec_after_seconds`: how long an object stays replicated before it is
    /// encoded (§8.2).
    pub ec_after_seconds: u64,
    /// `backup_ack`: whether writes wait for the backup target (§8.9).
    pub backup_ack: AckPolicy,
    /// `index_snapshot_interval_seconds`: how often per-shard index
    /// snapshots are written (§8.9).
    pub index_snapshot_interval_seconds: u64,
    /// `target_transport` (§7.8).
    pub target_transport: TargetTransport,
    /// `ack_policy` (§7.5).
    pub ack_policy: AckPolicy,
    /// `flush_conflict_policy` (§7.2).
    pub flush_conflict_policy: ConflictPolicy,
    /// `backup_target`: where a `local` bucket's changes are also flushed
    /// (§8.9).
    pub backup_target: Option<RemoteTarget>,
    /// `snapshot_target`: where index snapshots go; defaults to the backup
    /// target (§8.9).
    pub snapshot_target: Option<RemoteTarget>,
    /// `peer_source`: the cluster this bucket receives native replication
    /// from (§7.8).
    pub peer_source: Option<ClusterId>,
    /// `max_dirty_bytes`: the bucket's dirty-data budget (§7.6); by default
    /// the cluster's, `flush.max_dirty_bytes`.
    pub max_dirty_bytes: u64,
}

crate::durations! {
    BucketSettings {
        /// `ec_after_seconds`.
        ec_after => ec_after_seconds, Duration::from_secs;
        /// `index_snapshot_interval_seconds`.
        index_snapshot_interval => index_snapshot_interval_seconds, Duration::from_secs;
    }
}

/// `[buckets]`: the default bucket settings and per-bucket overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketsConfig {
    /// `[buckets.defaults]`, resolved over the built-in defaults.
    pub defaults: BucketSettings,
    /// Each `[buckets.<name>]` table, resolved over the defaults.
    pub named: BTreeMap<BucketName, BucketSettings>,
}

impl BucketsConfig {
    /// The settings of the bucket named `name`: its own table's, or the
    /// defaults.
    #[must_use]
    pub fn get(&self, name: &BucketName) -> &BucketSettings {
        self.named.get(name).unwrap_or(&self.defaults)
    }
}

/// The table name that holds the defaults. No bucket of that name can have
/// a table of its own.
pub(crate) const DEFAULTS: &str = "defaults";

/// A `[buckets.*]` table as written: every key is optional.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BucketTable {
    mode: Option<BucketMode>,
    shards_per_bucket: Option<u32>,
    replicas: Option<u8>,
    min_write_replicas: Option<u8>,
    clean_copies: Option<u8>,
    import_parallel_streams: Option<u32>,
    ec_min_object_bytes: Option<u64>,
    ec_stripe_data_bytes: Option<u64>,
    ec_after_seconds: Option<u64>,
    backup_ack: Option<AckPolicy>,
    index_snapshot_interval_seconds: Option<u64>,
    target_transport: Option<TargetTransport>,
    max_dirty_bytes: Option<u64>,
    ack_policy: Option<AckPolicy>,
    flush_conflict_policy: Option<ConflictPolicy>,
    backup_target: Option<String>,
    snapshot_target: Option<String>,
    peer_source: Option<String>,
}

/// The built-in defaults of every bucket-level key that has one.
struct Defaults {
    mode: BucketMode,
    shards_per_bucket: u32,
    replicas: u8,
    min_write_replicas: u8,
    clean_copies: u8,
    import_parallel_streams: u32,
    ec_min_object_bytes: u64,
    ec_stripe_data_bytes: u64,
    ec_after_seconds: u64,
    backup_ack: AckPolicy,
    index_snapshot_interval_seconds: u64,
    target_transport: TargetTransport,
    max_dirty_bytes: u64,
    ack_policy: AckPolicy,
    flush_conflict_policy: ConflictPolicy,
}

impl Defaults {
    const fn builtin(flush: &FlushConfig) -> Self {
        Self {
            mode: BucketMode::WriteBack,
            shards_per_bucket: 8,
            replicas: 3,
            min_write_replicas: 2,
            clean_copies: 1,
            import_parallel_streams: 32,
            ec_min_object_bytes: 4 * MIB,
            ec_stripe_data_bytes: 64 * MIB,
            ec_after_seconds: 600,
            backup_ack: AckPolicy::Local,
            index_snapshot_interval_seconds: 3600,
            target_transport: TargetTransport::Auto,
            max_dirty_bytes: flush.max_dirty_bytes,
            ack_policy: flush.ack_policy,
            flush_conflict_policy: flush.flush_conflict_policy,
        }
    }

    /// Applies the keys `table` sets.
    fn overlay(&self, table: &BucketTable) -> Self {
        Self {
            mode: table.mode.unwrap_or(self.mode),
            shards_per_bucket: table.shards_per_bucket.unwrap_or(self.shards_per_bucket),
            replicas: table.replicas.unwrap_or(self.replicas),
            min_write_replicas: table.min_write_replicas.unwrap_or(self.min_write_replicas),
            clean_copies: table.clean_copies.unwrap_or(self.clean_copies),
            import_parallel_streams: table
                .import_parallel_streams
                .unwrap_or(self.import_parallel_streams),
            ec_min_object_bytes: table
                .ec_min_object_bytes
                .unwrap_or(self.ec_min_object_bytes),
            ec_stripe_data_bytes: table
                .ec_stripe_data_bytes
                .unwrap_or(self.ec_stripe_data_bytes),
            ec_after_seconds: table.ec_after_seconds.unwrap_or(self.ec_after_seconds),
            backup_ack: table.backup_ack.unwrap_or(self.backup_ack),
            index_snapshot_interval_seconds: table
                .index_snapshot_interval_seconds
                .unwrap_or(self.index_snapshot_interval_seconds),
            target_transport: table.target_transport.unwrap_or(self.target_transport),
            max_dirty_bytes: table.max_dirty_bytes.unwrap_or(self.max_dirty_bytes),
            ack_policy: table.ack_policy.unwrap_or(self.ack_policy),
            flush_conflict_policy: table
                .flush_conflict_policy
                .unwrap_or(self.flush_conflict_policy),
        }
    }
}

/// Checks every `[buckets]` table and resolves them. `cluster_id` is
/// `None` when the cluster ID is itself invalid.
///
/// Returns the defaults, `None` only after reporting a violation, and every
/// named bucket whose table resolved; a table that breaks a rule is reported
/// and left out.
pub(crate) fn resolve(
    tables: &BTreeMap<String, BucketTable>,
    flush: &FlushConfig,
    cluster_id: Option<&ClusterId>,
    checker: &mut Checker,
) -> (Option<BucketSettings>, BTreeMap<BucketName, BucketSettings>) {
    let empty = BucketTable::default();
    let defaults_table = tables.get(DEFAULTS).unwrap_or(&empty);
    let prefix = key_path("buckets", DEFAULTS);
    for (key, set) in [
        ("backup_target", defaults_table.backup_target.is_some()),
        ("snapshot_target", defaults_table.snapshot_target.is_some()),
        ("peer_source", defaults_table.peer_source.is_some()),
    ] {
        if set {
            checker.report(
                format!("{prefix}.{key}"),
                "names one bucket's target or source; set it in a [buckets.<name>] table",
            );
        }
    }
    for (key, set) in [
        ("ack_policy", defaults_table.ack_policy.is_some()),
        (
            "flush_conflict_policy",
            defaults_table.flush_conflict_policy.is_some(),
        ),
    ] {
        if set {
            checker.report(
                format!("{prefix}.{key}"),
                format!("the default for every bucket is flush.{key}; set it there"),
            );
        }
    }
    let base = Defaults::builtin(flush).overlay(defaults_table);
    let defaults = settle(&base, defaults_table, &prefix, false, cluster_id, checker);

    let mut named = BTreeMap::new();
    for (name, table) in tables.iter().filter(|(name, _)| *name != DEFAULTS) {
        let prefix = key_path("buckets", name);
        let bucket_name = BucketName::new(name.as_str())
            .map_err(|error| checker.report(prefix.as_str(), error))
            .ok();
        let settings = settle(
            &base.overlay(table),
            table,
            &prefix,
            true,
            cluster_id,
            checker,
        );
        if let (Some(bucket_name), Some(settings)) = (bucket_name, settings) {
            named.insert(bucket_name, settings);
        }
    }

    (defaults, named)
}

/// Checks one table's resolved values and builds its settings. `named` is
/// false for `[buckets.defaults]`. Only keys
/// the table sets are reported, so a bad default is reported once, at
/// `[buckets.defaults]`, not again at every bucket that inherits it.
fn settle<'a>(
    values: &Defaults,
    table: &'a BucketTable,
    prefix: &str,
    named: bool,
    cluster_id: Option<&ClusterId>,
    checker: &mut Checker,
) -> Option<BucketSettings> {
    let before = checker.count();
    let mut valid = true;
    let key = |name: &str| format!("{prefix}.{name}");

    let shards_per_bucket = ShardCount::new(values.shards_per_bucket)
        .map_err(|error| {
            if table.shards_per_bucket.is_some() {
                checker.report(key("shards_per_bucket"), error);
            }
            valid = false;
        })
        .ok();

    let replication = ReplicationSettings {
        replicas: values.replicas,
        min_write_replicas: values.min_write_replicas,
        clean_copies: values.clean_copies,
    };
    let sets_replication = table.replicas.is_some()
        || table.min_write_replicas.is_some()
        || table.clean_copies.is_some();
    for error in replication_errors(replication) {
        valid = false;
        if sets_replication {
            let name = match error {
                ReplicationError::MinWriteReplicas { .. } => "min_write_replicas",
                ReplicationError::CleanCopies { .. } => "clean_copies",
                _ => "replicas",
            };
            checker.report(key(name), error);
        }
    }

    for (name, set, value) in [
        (
            "import_parallel_streams",
            table.import_parallel_streams.is_some(),
            u64::from(values.import_parallel_streams),
        ),
        (
            "ec_min_object_bytes",
            table.ec_min_object_bytes.is_some(),
            values.ec_min_object_bytes,
        ),
        (
            "ec_stripe_data_bytes",
            table.ec_stripe_data_bytes.is_some(),
            values.ec_stripe_data_bytes,
        ),
        (
            "index_snapshot_interval_seconds",
            table.index_snapshot_interval_seconds.is_some(),
            values.index_snapshot_interval_seconds,
        ),
        (
            "max_dirty_bytes",
            table.max_dirty_bytes.is_some(),
            values.max_dirty_bytes,
        ),
    ] {
        if value == 0 {
            valid = false;
            if set {
                checker.nonzero(&key(name), value);
            }
        }
    }

    let mut parse_target = |name: &str, url: Option<&String>| {
        url.and_then(|url| {
            target::parse_target(url)
                .map_err(|error| checker.report(key(name), error))
                .ok()
        })
    };
    // `[buckets.defaults]` cannot name targets or a source; `resolve` has
    // reported any it sets.
    let own = |value: &'a Option<String>| value.as_ref().filter(|_| named);
    let backup_target = parse_target("backup_target", own(&table.backup_target));
    let snapshot_target = parse_target("snapshot_target", own(&table.snapshot_target))
        .or_else(|| backup_target.clone());

    if own(&table.backup_target).is_some() && values.mode != BucketMode::Local {
        checker.report(
            key("backup_target"),
            format!(
                "only local buckets have a backup target (§8.9); this bucket's mode is {}",
                values.mode
            ),
        );
    }
    // Checked for named buckets only: the defaults cannot name a target.
    if named
        && values.mode == BucketMode::Local
        && values.backup_ack == AckPolicy::WriteThrough
        && table.backup_target.is_none()
    {
        checker.report(
            key("backup_ack"),
            "is \"write_through\", which needs a backup_target to wait for (§8.9)",
        );
    }

    let peer_source =
        own(&table.peer_source).and_then(|source| match ClusterId::new(source.as_str()) {
            Ok(id) if Some(&id) == cluster_id => {
                checker.report(
                    key("peer_source"),
                    format!(
                        "is this cluster's own ID ({id}); a bucket cannot replicate from itself"
                    ),
                );
                None
            }
            Ok(id) => Some(id),
            Err(error) => {
                checker.report(key("peer_source"), error);
                None
            }
        });

    if !valid || checker.count() != before {
        return None;
    }
    Some(BucketSettings {
        mode: values.mode,
        shards_per_bucket: shards_per_bucket?,
        replication,
        import_parallel_streams: values.import_parallel_streams,
        ec_min_object_bytes: values.ec_min_object_bytes,
        ec_stripe_data_bytes: values.ec_stripe_data_bytes,
        ec_after_seconds: values.ec_after_seconds,
        backup_ack: values.backup_ack,
        index_snapshot_interval_seconds: values.index_snapshot_interval_seconds,
        target_transport: values.target_transport,
        ack_policy: values.ack_policy,
        flush_conflict_policy: values.flush_conflict_policy,
        backup_target,
        snapshot_target,
        peer_source,
        max_dirty_bytes: values.max_dirty_bytes,
    })
}

/// Every replication rule `settings` breaks. [`ReplicationSettings::validate`]
/// stops at the first; a configuration reports them all.
fn replication_errors(settings: ReplicationSettings) -> Vec<ReplicationError> {
    let mut errors = Vec::new();
    if let Err(error) = settings.validate() {
        let clean_copies_too = !matches!(error, ReplicationError::CleanCopies { .. })
            && settings.clean_copies > settings.replicas;
        errors.push(error);
        if clean_copies_too {
            errors.push(ReplicationError::CleanCopies {
                clean_copies: settings.clean_copies,
                replicas: settings.replicas,
            });
        }
    }
    errors
}
