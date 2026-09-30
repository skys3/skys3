//! Configuration errors, and the collector that gathers every violation of
//! a configuration before it is rejected.

use std::fmt;
use std::path::PathBuf;

/// Why a configuration could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("cannot read configuration file {path}: {source}")]
    Read {
        /// The file that was read.
        path: PathBuf,
        /// The I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The text is not TOML, a value has the wrong type, a required key is
    /// missing, or a key is unknown. The error names the key and its line.
    #[error("invalid configuration: {0}")]
    Parse(#[from] toml::de::Error),
    /// The configuration parsed but breaks one or more rules.
    #[error(transparent)]
    Invalid(#[from] Violations),
}

/// One broken rule, reported at the key it concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The dotted key path, for example `replication.primary_grace_ms` or
    /// `buckets.archive.backup_target`.
    pub key: String,
    /// What is wrong and what the rule requires.
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.message)
    }
}

/// Every rule a configuration breaks, in the order the checks ran.
///
/// Loading reports all of them at once, so an operator can fix a
/// configuration in one pass.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct Violations(Vec<Violation>);

impl Violations {
    /// The violations, in the order the checks ran.
    #[must_use]
    pub fn as_slice(&self) -> &[Violation] {
        &self.0
    }

    /// Whether any violation is reported at `key`.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.iter().any(|violation| violation.key == key)
    }

    /// The number of violations; never 0.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always `false`: a `Violations` value holds at least one violation.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for Violations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.0.len();
        let noun = if count == 1 { "problem" } else { "problems" };
        write!(f, "invalid configuration ({count} {noun}):")?;
        for violation in &self.0 {
            write!(f, "\n  - {violation}")?;
        }
        Ok(())
    }
}

impl IntoIterator for Violations {
    type Item = Violation;
    type IntoIter = std::vec::IntoIter<Violation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Collects violations while the checks run.
#[derive(Debug, Default)]
pub(crate) struct Checker {
    violations: Vec<Violation>,
}

impl Checker {
    /// Records a violation at `key`.
    pub(crate) fn report(&mut self, key: impl Into<String>, message: impl fmt::Display) {
        self.violations.push(Violation {
            key: key.into(),
            message: message.to_string(),
        });
    }

    /// Records a violation at `key` unless `ok` holds.
    pub(crate) fn require(&mut self, ok: bool, key: &str, message: impl FnOnce() -> String) {
        if !ok {
            self.report(key, message());
        }
    }

    /// Requires `value` to be at least 1.
    pub(crate) fn nonzero(&mut self, key: &str, value: u64) {
        self.require(value > 0, key, || "must be at least 1".to_owned());
    }

    /// Requires `value` to be a finite number in `[0, max)`, or in
    /// `[0, max]` when `max_inclusive` holds.
    pub(crate) fn fraction(&mut self, key: &str, value: f64, max: f64, max_inclusive: bool) {
        let below_max = if max_inclusive {
            value <= max
        } else {
            value < max
        };
        let bracket = if max_inclusive { ']' } else { ')' };
        self.require(value >= 0.0 && below_max, key, || {
            format!("is {value}; it must be in [0, {max}{bracket}")
        });
    }

    /// The number of violations recorded so far.
    pub(crate) fn count(&self) -> usize {
        self.violations.len()
    }

    /// Ends the checks.
    pub(crate) fn finish(self) -> Result<(), Violations> {
        if self.violations.is_empty() {
            Ok(())
        } else {
            Err(Violations(self.violations))
        }
    }
}

/// Joins a table path and a key, quoting the key when it is not a bare TOML
/// key (bucket names may contain `.`).
pub(crate) fn key_path(table: &str, key: &str) -> String {
    let bare = !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if bare {
        format!("{table}.{key}")
    } else {
        format!("{table}.{key:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn violations_list_every_problem() {
        let mut checker = Checker::default();
        assert_eq!(checker.count(), 0);
        checker.nonzero("storage.extent_bytes", 0);
        checker.nonzero("storage.segment_bytes", 1);
        checker.fraction("cache.reserve_fraction", 1.0, 1.0, false);
        checker.fraction("storage.compaction_live_threshold", 1.0, 1.0, true);
        checker.fraction("replication.assumed_clock_drift", f64::NAN, 1.0, false);
        let violations = checker.finish().unwrap_err();
        assert_eq!(violations.len(), 3);
        assert!(!violations.is_empty());
        assert!(violations.contains_key("storage.extent_bytes"));
        assert!(!violations.contains_key("storage.segment_bytes"));
        assert_eq!(
            violations.to_string(),
            "invalid configuration (3 problems):\n  \
             - storage.extent_bytes: must be at least 1\n  \
             - cache.reserve_fraction: is 1; it must be in [0, 1)\n  \
             - replication.assumed_clock_drift: is NaN; it must be in [0, 1)"
        );
        let keys: Vec<_> = violations.into_iter().map(|v| v.key).collect();
        assert_eq!(keys[0], "storage.extent_bytes");
    }

    #[test]
    fn one_violation_reads_as_one_problem() {
        let mut checker = Checker::default();
        checker.report("cluster.cluster_id", "is missing");
        let violations = checker.finish().unwrap_err();
        assert_eq!(
            violations.as_slice()[0].to_string(),
            "cluster.cluster_id: is missing"
        );
        assert!(
            violations
                .to_string()
                .starts_with("invalid configuration (1 problem):")
        );
    }

    #[test]
    fn key_paths_quote_keys_that_are_not_bare() {
        assert_eq!(
            key_path("buckets", "archive-from-eu"),
            "buckets.archive-from-eu"
        );
        assert_eq!(
            key_path("buckets", "logs.example"),
            "buckets.\"logs.example\""
        );
        assert_eq!(key_path("buckets", ""), "buckets.\"\"");
    }
}
