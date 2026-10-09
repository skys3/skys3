//! Bucket lifecycle configurations (design §8.7, §11): which objects of a
//! `local` bucket expire, and when abandoned multipart uploads are aborted.
//!
//! A configuration is stored in its bucket's register
//! ([`BucketDocument::lifecycle`](crate::BucketDocument::lifecycle)), so it
//! reaches every node with the bucket, and each shard primary evaluates it
//! over its index. This module holds the stored form, its rules
//! ([`LifecycleConfiguration::validate`]), and the evaluation itself:
//!
//! - [`LifecycleConfiguration::expires_at`]: when an object version
//!   expires, the earliest time any enabled rule whose filter matches it
//!   expires it;
//! - [`LifecycleConfiguration::aborts_at`]: when an open upload is aborted.
//!
//! As in S3, a rule's `Days` count from the version's `Last-Modified` (or
//! the upload's initiation), and the result is rounded up to the next
//! midnight UTC: an object written at 10:30 on the 15th with `Days = 3`
//! expires at 00:00 on the 19th. A `Date` rule expires every matching
//! object once that midnight has passed, whatever its age.
//!
//! ```
//! use std::collections::BTreeMap;
//! use skys3_types::lifecycle::{Expiration, LifecycleConfiguration, LifecycleRule, RuleFilter};
//!
//! let config = LifecycleConfiguration {
//!     rules: vec![LifecycleRule {
//!         id: "logs".into(),
//!         enabled: true,
//!         filter: RuleFilter { prefix: "logs/".into(), ..RuleFilter::default() },
//!         expiration: Some(Expiration::Days(3)),
//!         abort_upload_days: None,
//!     }],
//! };
//! config.validate()?;
//! const DAY: u64 = 86_400_000;
//! let written = 14 * DAY + 37_800_000; // 10:30 UTC on day 14
//! let tags = BTreeMap::new();
//! assert_eq!(config.expires_at("logs/a", 10, &tags, written), Some(18 * DAY));
//! assert_eq!(config.expires_at("data/a", 10, &tags, written), None);
//! # Ok::<(), skys3_types::lifecycle::LifecycleError>(())
//! ```

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::limits::is_xml_text;

/// A day, in milliseconds.
pub const DAY_MS: u64 = 86_400_000;

/// The most rules a configuration holds (the S3 limit).
pub const MAX_RULES: usize = 1000;

/// The longest rule ID, in characters (the S3 limit).
pub const MAX_RULE_ID_CHARS: usize = 255;

/// The longest filter prefix, in bytes: the longest key.
pub const MAX_PREFIX_BYTES: usize = 1024;

/// The most tags a filter names: an object has at most 10, so a filter
/// with more could match nothing.
pub const MAX_FILTER_TAGS: usize = 10;

/// The largest stored configuration, in bytes of JSON. It keeps the bucket
/// register well within what every control-store backend takes in one
/// value (etcd's default is 1.5 MiB).
pub const MAX_JSON_BYTES: usize = 256 * 1024;

/// A bucket's lifecycle configuration: its rules, in the order given.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfiguration {
    /// The rules, 1 to [`MAX_RULES`] of them, with distinct IDs.
    pub rules: Vec<LifecycleRule>,
}

/// One lifecycle rule: which keys it applies to, and what it does to them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRule {
    /// The rule's ID: 1 to [`MAX_RULE_ID_CHARS`] characters, unique in the
    /// configuration.
    pub id: String,
    /// Whether the rule is in force (`Status` `Enabled`); a disabled rule
    /// is kept but does nothing.
    pub enabled: bool,
    /// Which objects and uploads the rule applies to.
    #[serde(default)]
    pub filter: RuleFilter,
    /// When matching objects expire, if the rule expires objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<Expiration>,
    /// `AbortIncompleteMultipartUpload`: how many days after its initiation
    /// a matching upload that is still open is aborted, if the rule aborts
    /// uploads. Positive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_upload_days: Option<u32>,
}

/// When the objects a rule matches expire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expiration {
    /// This many days after the version was written, rounded up to the next
    /// midnight UTC. Positive.
    Days(u32),
    /// At this midnight UTC, in milliseconds since the Unix epoch, whatever
    /// the object's age.
    DateMs(u64),
}

/// The objects a rule applies to: every condition given must hold. An
/// empty filter matches every key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleFilter {
    /// Keys that start with this; empty for every key.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    /// Tags the object must have, each with this value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    /// `ObjectSizeGreaterThan`: objects larger than this many bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_greater_than: Option<u64>,
    /// `ObjectSizeLessThan`: objects smaller than this many bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_less_than: Option<u64>,
    /// Whether the rule gave its prefix in the rule's own `Prefix` element,
    /// which S3 deprecated, instead of a `Filter`. It changes only how the
    /// configuration is returned, and such a filter has no other condition.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub legacy_prefix: bool,
}

impl RuleFilter {
    /// Whether an object with `key`, `size`, and `tags` matches.
    #[must_use]
    pub fn matches(&self, key: &str, size: u64, tags: &BTreeMap<String, String>) -> bool {
        key.starts_with(&self.prefix)
            && self.size_greater_than.is_none_or(|bound| size > bound)
            && self.size_less_than.is_none_or(|bound| size < bound)
            && self
                .tags
                .iter()
                .all(|(name, value)| tags.get(name) == Some(value))
    }

    /// Whether the filter has a condition beyond its prefix.
    #[must_use]
    pub fn has_object_conditions(&self) -> bool {
        !self.tags.is_empty() || self.size_greater_than.is_some() || self.size_less_than.is_some()
    }
}

/// A lifecycle configuration that breaks a rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The configuration has no rules.
    #[error("a lifecycle configuration needs at least one rule")]
    NoRules,
    /// The configuration has more than [`MAX_RULES`] rules.
    #[error("a lifecycle configuration has at most {MAX_RULES} rules, not {0}")]
    TooManyRules(usize),
    /// A rule's ID is empty or longer than [`MAX_RULE_ID_CHARS`].
    #[error("a rule ID has 1 to {MAX_RULE_ID_CHARS} characters")]
    InvalidId,
    /// Two rules share an ID.
    #[error("rule ID {0:?} is used by more than one rule")]
    DuplicateId(String),
    /// A rule neither expires objects nor aborts uploads.
    #[error("rule {0:?} has no action")]
    NoAction(String),
    /// A rule's `Days` is 0.
    #[error("rule {0:?}: Days must be a positive integer")]
    ZeroDays(String),
    /// A rule's `DaysAfterInitiation` is 0.
    #[error("rule {0:?}: DaysAfterInitiation must be a positive integer")]
    ZeroUploadDays(String),
    /// A rule's `Date` is not at midnight UTC.
    #[error("rule {0:?}: Date must be at midnight UTC")]
    DateNotMidnight(String),
    /// A filter's prefix is longer than [`MAX_PREFIX_BYTES`].
    #[error("rule {0:?}: the prefix is longer than {MAX_PREFIX_BYTES} bytes")]
    PrefixTooLong(String),
    /// A filter names more than [`MAX_FILTER_TAGS`] tags, or a tag with an
    /// empty key.
    #[error("rule {0:?}: a filter names 1 to {MAX_FILTER_TAGS} tags, each with a key")]
    InvalidTags(String),
    /// A filter's size bounds leave no size.
    #[error("rule {0:?}: ObjectSizeLessThan must be greater than ObjectSizeGreaterThan")]
    EmptySizeRange(String),
    /// A filter's legacy prefix comes with other conditions.
    #[error("rule {0:?}: a rule's own Prefix cannot come with other conditions")]
    LegacyPrefixWithConditions(String),
    /// A rule that aborts uploads filters by tags or size, which uploads do
    /// not have.
    #[error("rule {0:?}: AbortIncompleteMultipartUpload cannot be used with a tag or size filter")]
    UploadFilter(String),
    /// A rule's ID, prefix, or a tag of its filter holds a control
    /// character, which the XML of GetBucketLifecycleConfiguration cannot
    /// carry ([`is_xml_text`]).
    #[error("rule {0:?}: the ID, prefix, and tags cannot hold control characters")]
    ControlCharacter(String),
    /// The configuration is larger than [`MAX_JSON_BYTES`] stored.
    #[error(
        "the lifecycle configuration takes {0} bytes stored; at most {MAX_JSON_BYTES} are allowed"
    )]
    TooLarge(usize),
}

impl LifecycleConfiguration {
    /// Checks the configuration's rules.
    ///
    /// # Errors
    ///
    /// The first [`LifecycleError`] found.
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.rules.is_empty() {
            return Err(LifecycleError::NoRules);
        }
        if self.rules.len() > MAX_RULES {
            return Err(LifecycleError::TooManyRules(self.rules.len()));
        }
        let mut ids = BTreeSet::new();
        for rule in &self.rules {
            rule.validate()?;
            if !ids.insert(rule.id.as_str()) {
                return Err(LifecycleError::DuplicateId(rule.id.clone()));
            }
        }
        let stored = serde_json::to_vec(self).map_or(usize::MAX, |json| json.len());
        if stored > MAX_JSON_BYTES {
            return Err(LifecycleError::TooLarge(stored));
        }
        Ok(())
    }

    /// When an object version expires: the earliest time, in milliseconds
    /// since the Unix epoch, at which an enabled rule whose filter matches
    /// `key`, `size`, and `tags` expires a version last modified at
    /// `last_modified_ms`. `None` if no rule expires it.
    #[must_use]
    pub fn expires_at(
        &self,
        key: &str,
        size: u64,
        tags: &BTreeMap<String, String>,
        last_modified_ms: u64,
    ) -> Option<u64> {
        self.rules
            .iter()
            .filter(|rule| rule.enabled && rule.filter.matches(key, size, tags))
            .filter_map(|rule| match rule.expiration? {
                Expiration::Days(days) => Some(after_days(last_modified_ms, days)),
                Expiration::DateMs(date) => Some(date),
            })
            .min()
    }

    /// When an open upload of `key` initiated at `initiated_ms` is aborted:
    /// the earliest time an enabled rule whose prefix matches aborts it, or
    /// `None` if no rule does. Rules that abort uploads filter by prefix
    /// only.
    #[must_use]
    pub fn aborts_at(&self, key: &str, initiated_ms: u64) -> Option<u64> {
        self.rules
            .iter()
            .filter(|rule| rule.enabled && key.starts_with(&rule.filter.prefix))
            .filter_map(|rule| Some(after_days(initiated_ms, rule.abort_upload_days?)))
            .min()
    }

    /// Whether an enabled rule expires objects.
    #[must_use]
    pub fn expires_objects(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.enabled && rule.expiration.is_some())
    }

    /// Whether an enabled rule aborts uploads.
    #[must_use]
    pub fn aborts_uploads(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.enabled && rule.abort_upload_days.is_some())
    }
}

impl LifecycleRule {
    fn validate(&self) -> Result<(), LifecycleError> {
        let id = || self.id.clone();
        let chars = self.id.chars().count();
        if chars == 0 || chars > MAX_RULE_ID_CHARS {
            return Err(LifecycleError::InvalidId);
        }
        if self.expiration.is_none() && self.abort_upload_days.is_none() {
            return Err(LifecycleError::NoAction(id()));
        }
        match self.expiration {
            Some(Expiration::Days(0)) => return Err(LifecycleError::ZeroDays(id())),
            Some(Expiration::DateMs(date)) if date % DAY_MS != 0 => {
                return Err(LifecycleError::DateNotMidnight(id()));
            }
            _ => {}
        }
        if self.abort_upload_days == Some(0) {
            return Err(LifecycleError::ZeroUploadDays(id()));
        }
        let filter = &self.filter;
        if filter.prefix.len() > MAX_PREFIX_BYTES {
            return Err(LifecycleError::PrefixTooLong(id()));
        }
        if filter.tags.len() > MAX_FILTER_TAGS || filter.tags.contains_key("") {
            return Err(LifecycleError::InvalidTags(id()));
        }
        let texts = [self.id.as_str(), filter.prefix.as_str()].into_iter();
        let tags = filter
            .tags
            .iter()
            .flat_map(|(k, v)| [k.as_str(), v.as_str()]);
        if !texts.chain(tags).all(is_xml_text) {
            return Err(LifecycleError::ControlCharacter(id()));
        }
        if let (Some(greater), Some(less)) = (filter.size_greater_than, filter.size_less_than)
            && less <= greater
        {
            return Err(LifecycleError::EmptySizeRange(id()));
        }
        if filter.legacy_prefix && filter.has_object_conditions() {
            return Err(LifecycleError::LegacyPrefixWithConditions(id()));
        }
        if self.abort_upload_days.is_some() && filter.has_object_conditions() {
            return Err(LifecycleError::UploadFilter(id()));
        }
        Ok(())
    }
}

/// `days` after `start_ms`, rounded up to the next midnight UTC, as S3
/// counts lifecycle days. A time at midnight moves to the next one.
#[must_use]
pub fn after_days(start_ms: u64, days: u32) -> u64 {
    let due = start_ms.saturating_add(u64::from(days).saturating_mul(DAY_MS));
    (due / DAY_MS).saturating_add(1).saturating_mul(DAY_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(id: &str) -> LifecycleRule {
        LifecycleRule {
            id: id.to_owned(),
            enabled: true,
            filter: RuleFilter::default(),
            expiration: Some(Expiration::Days(1)),
            abort_upload_days: None,
        }
    }

    fn config(rules: Vec<LifecycleRule>) -> LifecycleConfiguration {
        LifecycleConfiguration { rules }
    }

    fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn days_round_up_to_the_next_midnight() {
        // S3's example: written 10:30 on the 15th, 3 days: the 19th.
        assert_eq!(after_days(15 * DAY_MS + 37_800_000, 3), 19 * DAY_MS);
        assert_eq!(after_days(15 * DAY_MS, 1), 17 * DAY_MS);
        assert_eq!(after_days(15 * DAY_MS - 1, 1), 16 * DAY_MS);
        // Past the last midnight `u64` can hold: never.
        assert_eq!(after_days(u64::MAX - 5, 3), u64::MAX);
    }

    #[test]
    fn filters_need_every_condition() {
        let filter = RuleFilter {
            prefix: "logs/".into(),
            tags: tags(&[("class", "temp")]),
            size_greater_than: Some(10),
            size_less_than: Some(100),
            legacy_prefix: false,
        };
        let temp = tags(&[("class", "temp"), ("other", "x")]);
        assert!(filter.matches("logs/a", 50, &temp));
        assert!(!filter.matches("data/a", 50, &temp));
        assert!(!filter.matches("logs/a", 10, &temp));
        assert!(!filter.matches("logs/a", 100, &temp));
        assert!(!filter.matches("logs/a", 50, &tags(&[("class", "keep")])));
        assert!(!filter.matches("logs/a", 50, &BTreeMap::new()));
        assert!(RuleFilter::default().matches("", 0, &BTreeMap::new()));
    }

    #[test]
    fn the_earliest_enabled_rule_decides() {
        let mut late = rule("late");
        late.expiration = Some(Expiration::Days(30));
        let mut early = rule("early");
        early.expiration = Some(Expiration::DateMs(5 * DAY_MS));
        early.filter.prefix = "tmp/".into();
        let mut off = rule("off");
        off.enabled = false;
        let config = config(vec![late, early, off]);
        let none = BTreeMap::new();
        assert_eq!(config.expires_at("tmp/a", 1, &none, 0), Some(5 * DAY_MS));
        assert_eq!(config.expires_at("a", 1, &none, 0), Some(31 * DAY_MS));
        assert!(config.expires_objects());
        assert!(!config.aborts_uploads());
        assert_eq!(config.aborts_at("a", 0), None);
    }

    #[test]
    fn uploads_follow_prefix_rules() {
        let mut uploads = rule("uploads");
        uploads.expiration = None;
        uploads.abort_upload_days = Some(2);
        uploads.filter.prefix = "big/".into();
        let config = config(vec![uploads]);
        assert!(config.aborts_uploads());
        assert!(!config.expires_objects());
        assert_eq!(config.aborts_at("big/a", DAY_MS), Some(4 * DAY_MS));
        assert_eq!(config.aborts_at("small/a", DAY_MS), None);
        assert_eq!(config.expires_at("big/a", 1, &BTreeMap::new(), 0), None);
    }

    #[test]
    fn invalid_configurations_are_refused() {
        let check = |rules: Vec<LifecycleRule>| config(rules).validate().unwrap_err();
        assert_eq!(check(vec![]), LifecycleError::NoRules);
        let many = (0..=MAX_RULES).map(|n| rule(&n.to_string())).collect();
        assert_eq!(check(many), LifecycleError::TooManyRules(MAX_RULES + 1));
        assert_eq!(check(vec![rule("")]), LifecycleError::InvalidId);
        assert_eq!(
            check(vec![rule(&"x".repeat(256))]),
            LifecycleError::InvalidId
        );
        assert!(config(vec![rule(&"é".repeat(255))]).validate().is_ok());
        assert_eq!(
            check(vec![rule("a"), rule("a")]),
            LifecycleError::DuplicateId("a".into())
        );

        let with = |change: &dyn Fn(&mut LifecycleRule)| {
            let mut rule = rule("r");
            change(&mut rule);
            config(vec![rule]).validate()
        };
        let r = || "r".to_owned();
        assert_eq!(
            with(&|rule| rule.expiration = None),
            Err(LifecycleError::NoAction(r()))
        );
        assert_eq!(
            with(&|rule| rule.expiration = Some(Expiration::Days(0))),
            Err(LifecycleError::ZeroDays(r()))
        );
        assert_eq!(
            with(&|rule| rule.expiration = Some(Expiration::DateMs(DAY_MS + 1))),
            Err(LifecycleError::DateNotMidnight(r()))
        );
        assert_eq!(
            with(&|rule| rule.abort_upload_days = Some(0)),
            Err(LifecycleError::ZeroUploadDays(r()))
        );
        assert_eq!(
            with(&|rule| rule.filter.prefix = "p".repeat(1025)),
            Err(LifecycleError::PrefixTooLong(r()))
        );
        assert_eq!(
            with(&|rule| rule.filter.tags = tags(&[("", "v")])),
            Err(LifecycleError::InvalidTags(r()))
        );
        // GetBucketLifecycleConfiguration echoes these in XML (M7-03).
        assert_eq!(
            check(vec![rule("a\u{1}")]),
            LifecycleError::ControlCharacter("a\u{1}".into())
        );
        let control: [&dyn Fn(&mut LifecycleRule); 3] = [
            &|rule| rule.filter.prefix = "logs/\u{1f}".into(),
            &|rule| rule.filter.tags = tags(&[("k\u{0}", "v")]),
            &|rule| rule.filter.tags = tags(&[("k", "\u{8}")]),
        ];
        for change in control {
            assert_eq!(with(change), Err(LifecycleError::ControlCharacter(r())));
        }
        assert!(with(&|rule| rule.filter.prefix = "tab\tline\n".into()).is_ok());
        assert_eq!(
            with(&|rule| {
                rule.filter.size_greater_than = Some(5);
                rule.filter.size_less_than = Some(5);
            }),
            Err(LifecycleError::EmptySizeRange(r()))
        );
        assert_eq!(
            with(&|rule| {
                rule.filter.legacy_prefix = true;
                rule.filter.size_less_than = Some(5);
            }),
            Err(LifecycleError::LegacyPrefixWithConditions(r()))
        );
        assert_eq!(
            with(&|rule| {
                rule.abort_upload_days = Some(1);
                rule.filter.tags = tags(&[("k", "v")]);
            }),
            Err(LifecycleError::UploadFilter(r()))
        );
        // A thousand rules with long prefixes are too large to store.
        let large = (0..MAX_RULES)
            .map(|n| {
                let mut rule = rule(&n.to_string());
                rule.filter.prefix = "p".repeat(MAX_PREFIX_BYTES);
                rule
            })
            .collect();
        assert!(matches!(check(large), LifecycleError::TooLarge(_)));
    }

    #[test]
    fn the_stored_form_is_stable() {
        let mut first = rule("logs");
        first.filter = RuleFilter {
            prefix: "logs/".into(),
            tags: tags(&[("class", "temp")]),
            size_greater_than: Some(1),
            size_less_than: None,
            legacy_prefix: false,
        };
        first.abort_upload_days = None;
        let mut second = rule("old");
        second.enabled = false;
        second.expiration = Some(Expiration::DateMs(DAY_MS));
        second.abort_upload_days = Some(7);
        second.filter.legacy_prefix = true;
        let config = config(vec![first, second]);
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"rules":[{"id":"logs","enabled":true,"filter":{"prefix":"logs/","tags":{"class":"temp"},"size_greater_than":1},"expiration":{"days":1}},{"id":"old","enabled":false,"filter":{"legacy_prefix":true},"expiration":{"date_ms":86400000},"abort_upload_days":7}]}"#
        );
        assert_eq!(
            serde_json::from_str::<LifecycleConfiguration>(&json).unwrap(),
            config
        );
        assert!(serde_json::from_str::<LifecycleConfiguration>(r#"{"rules":[],"x":1}"#).is_err());
    }
}
