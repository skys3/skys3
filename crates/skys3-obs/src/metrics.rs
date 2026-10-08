//! The node's metrics registry and the metric naming conventions.
//!
//! Every metric a node exports is registered in one [`MetricsRegistry`] and
//! served by the admin listener's `/metrics` endpoint in the OpenMetrics
//! text format. The conventions below are enforced at registration, so a
//! metric cannot be exported under a name that breaks them. The metrics
//! reference, `docs/skys3-metrics.md`, lists every metric.
//!
//! # Naming conventions
//!
//! - A metric is registered under its base name: lowercase `snake_case`
//!   ASCII, starting with a letter, such as `dirty` or `flush_lag`. Names the
//!   design gives, such as `dirty_bytes` or `flush_lag_seconds`, are split
//!   into a base name and a [`Unit`].
//! - The registry adds the `skys3_` prefix. A base name never contains it.
//! - The unit is declared with [`Unit`], never written into the base name.
//!   The exporter appends it (`_bytes`, `_seconds`), so `dirty` with
//!   [`Unit::Bytes`] is exported as `skys3_dirty_bytes`. Durations are in
//!   seconds and sizes in bytes, never milliseconds or kibibytes.
//! - The exporter appends the type suffix: `_total` for counters and `_info`
//!   for info metrics. A base name never ends with a unit or type suffix.
//! - Label values come from a bounded set, such as an endpoint name or a
//!   bucket name. Object keys, request IDs, and other unbounded or untrusted
//!   values are never labels.
//!
//! [`Unit`]: prometheus_client::registry::Unit
//! [`Unit::Bytes`]: prometheus_client::registry::Unit::Bytes

use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use prometheus_client::encoding::{EncodeMetric, text};
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::{Metric, Registry, Unit};

/// The prefix of every exported metric name, without its trailing `_`.
pub const METRIC_PREFIX: &str = "skys3";

/// The `Content-Type` of a scrape response: the OpenMetrics text format.
pub const OPENMETRICS_CONTENT_TYPE: &str =
    "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Suffixes that the exporter appends, and that a base name therefore must
/// not end with: the units in use and the per-type suffixes.
const RESERVED_SUFFIXES: &[&str] = &[
    "_bytes", "_seconds", "_ratio", "_ratios", "_total", "_info", "_count", "_sum", "_bucket",
    "_created",
];

/// The metrics registry of one node.
///
/// Cloning the registry is cheap, and clones share the same metrics. Metrics
/// can be registered at any time, including while the admin listener serves
/// scrapes. Each node owns its own registry rather than using a
/// process-global one, so several simulated nodes can run in one process
/// without mixing their metrics.
///
/// A new registry already holds `skys3_build_info`, which carries the
/// version of the running binary as a label.
#[derive(Clone)]
pub struct MetricsRegistry {
    inner: Arc<RwLock<Inner>>,
}

/// The registry and what was registered in it.
struct Inner {
    registry: Registry,
    registered: Vec<Registered>,
}

/// A metric family as it was registered, for listing what a registry
/// holds without encoding it: a family with labels but no series yet is
/// left out of the encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// The family's exported name without its type suffix: the prefix, the
    /// base name, and the unit, such as `skys3_dirty_bytes` or
    /// `skys3_flushes`.
    pub family: String,
    /// The family's OpenMetrics type, such as `counter`, `gauge`,
    /// `histogram`, or `info`.
    pub kind: String,
}

impl MetricsRegistry {
    /// Creates a registry that holds only `skys3_build_info`.
    #[must_use]
    pub fn new() -> Self {
        let registry = Self {
            inner: Arc::new(RwLock::new(Inner {
                registry: Registry::with_prefix(METRIC_PREFIX),
                registered: Vec::new(),
            })),
        };
        registry.register(
            "build",
            "Build information about the running SkyS3 binary.",
            Info::new([("version", env!("CARGO_PKG_VERSION"))]),
        );
        registry
    }

    /// Registers a metric that has no unit, such as a count of objects.
    ///
    /// The metric is exported as `skys3_<name>`, plus `_total` for a
    /// counter. `help` is a one-line description that ends with a period.
    ///
    /// # Panics
    ///
    /// Panics if `name` breaks the [naming conventions](self). A bad name is
    /// a programming error, caught by the first test that builds the
    /// component registering it.
    pub fn register(&self, name: &str, help: &str, metric: impl Metric) {
        assert_valid_base_name(name);
        let mut inner = self.write();
        inner.record(format!("{METRIC_PREFIX}_{name}"), &metric);
        inner.registry.register(name, help, metric);
    }

    /// Registers a metric measured in `unit`.
    ///
    /// The metric is exported as `skys3_<name>_<unit>`, plus `_total` for a
    /// counter: `register_with_unit("dirty", _, Unit::Bytes, _)` is exported
    /// as `skys3_dirty_bytes`.
    ///
    /// # Panics
    ///
    /// Panics if `name` breaks the [naming conventions](self).
    pub fn register_with_unit(&self, name: &str, help: &str, unit: Unit, metric: impl Metric) {
        assert_valid_base_name(name);
        let mut inner = self.write();
        inner.record(format!("{METRIC_PREFIX}_{name}_{}", unit.as_str()), &metric);
        inner.registry.register_with_unit(name, help, unit, metric);
    }

    /// Every metric family registered so far, in registration order,
    /// whether or not it has a series yet.
    #[must_use]
    pub fn registered(&self) -> Vec<Registered> {
        self.read().registered.clone()
    }

    /// Encodes every registered metric in the OpenMetrics text format,
    /// including the closing `# EOF` line.
    ///
    /// # Errors
    ///
    /// Returns an error if a metric fails to encode itself, which the
    /// metric types this crate re-exports never do.
    pub fn encode(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();
        text::encode(&mut out, &self.read().registry)?;
        Ok(out)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        // Registration either completes or panics before it mutates the
        // registry, so a poisoned lock still guards a consistent registry.
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Inner {
    fn record(&mut self, family: String, metric: &impl Metric) {
        let kind = metric.metric_type().as_str().to_owned();
        self.registered.push(Registered { family, kind });
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MetricsRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsRegistry").finish_non_exhaustive()
    }
}

/// Checks a base name against the naming conventions.
fn assert_valid_base_name(name: &str) {
    if let Err(reason) = check_base_name(name) {
        panic!("invalid metric name {name:?}: {reason}");
    }
}

fn check_base_name(name: &str) -> Result<(), &'static str> {
    let mut chars = name.chars();
    if !chars.next().is_some_and(|c| c.is_ascii_lowercase()) {
        return Err("must start with a lowercase ASCII letter");
    }
    if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return Err("must contain only lowercase ASCII letters, digits, and '_'");
    }
    if name.ends_with('_') || name.contains("__") {
        return Err("must not end with '_' or contain '__'");
    }
    if name.starts_with(METRIC_PREFIX) {
        return Err("must not start with the prefix, which the registry adds");
    }
    if RESERVED_SUFFIXES
        .iter()
        .any(|suffix| name.ends_with(suffix))
    {
        return Err("must not end with a unit or type suffix, which the exporter adds");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::family::Family;
    use prometheus_client::metrics::gauge::Gauge;

    use super::*;

    #[test]
    fn new_registry_exports_build_info() {
        let text = MetricsRegistry::new().encode().unwrap();
        let expected = format!(
            "skys3_build_info{{version=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        );
        assert!(text.contains(&expected), "{text}");
        assert!(text.ends_with("# EOF\n"), "{text}");
    }

    #[test]
    fn units_and_type_suffixes_are_appended() {
        let registry = MetricsRegistry::default();
        let gauge: Gauge = Gauge::default();
        let counter: Counter = Counter::default();
        registry.register_with_unit("dirty", "Dirty bytes.", Unit::Bytes, gauge.clone());
        registry.register("conflicts", "Conflicts.", counter.clone());
        gauge.set(42);
        counter.inc_by(3);

        let text = registry.encode().unwrap();
        assert!(text.contains("# UNIT skys3_dirty_bytes bytes\n"), "{text}");
        assert!(text.contains("skys3_dirty_bytes 42\n"), "{text}");
        assert!(text.contains("skys3_conflicts_total 3\n"), "{text}");
    }

    #[test]
    fn registered_families_are_listed_before_they_have_series() {
        let registry = MetricsRegistry::new();
        let family = Family::<Vec<(String, String)>, Counter>::default();
        registry.register("refusals", "Refusals.", family);
        registry.register_with_unit("lag", "Lag.", Unit::Seconds, Gauge::<i64>::default());
        let text = registry.encode().unwrap();
        assert!(!text.contains("refusals"), "{text}");
        let listed: Vec<_> = registry
            .registered()
            .into_iter()
            .map(|registered| (registered.family, registered.kind))
            .collect();
        let expected = [
            ("skys3_build", "info"),
            ("skys3_refusals", "counter"),
            ("skys3_lag_seconds", "gauge"),
        ]
        .map(|(family, kind)| (family.to_owned(), kind.to_owned()));
        assert_eq!(listed, expected);
    }

    #[test]
    fn clones_share_metrics() {
        let registry = MetricsRegistry::new();
        let clone = registry.clone();
        clone.register("shards", "Shards.", Gauge::<i64>::default());
        assert!(registry.encode().unwrap().contains("skys3_shards 0\n"));
    }

    #[test]
    fn design_names_fit_the_conventions() {
        // The design's metric names split into a valid base name and a unit.
        for base in [
            "dirty",
            "oldest_dirty_age",
            "flush_lag",
            "under_replicated",
            "oldest_under_replicated_age",
        ] {
            assert_eq!(check_base_name(base), Ok(()), "{base}");
        }
    }

    #[test]
    fn bad_names_are_rejected() {
        for name in [
            "",
            "Dirty",
            "1dirty",
            "_dirty",
            "dirty-bytes",
            "dirty_",
            "dirty__age",
            "skys3_dirty",
            "dirty_bytes",
            "flush_lag_seconds",
            "requests_total",
            "build_info",
        ] {
            assert!(check_base_name(name).is_err(), "{name:?} was accepted");
        }
    }

    #[test]
    #[should_panic(expected = "invalid metric name \"dirty_bytes\"")]
    fn registering_a_bad_name_panics() {
        MetricsRegistry::new().register("dirty_bytes", "Dirty bytes.", Gauge::<i64>::default());
    }

    #[test]
    fn debug_does_not_dump_the_registry() {
        assert_eq!(
            format!("{:?}", MetricsRegistry::new()),
            "MetricsRegistry { .. }"
        );
    }
}
