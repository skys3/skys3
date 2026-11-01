//! Bucket lifecycle configurations (design §8.7, §11):
//! PutBucketLifecycleConfiguration, GetBucketLifecycleConfiguration, and
//! DeleteBucketLifecycle.
//!
//! A configuration lives in its bucket's register
//! ([`BucketDocument::lifecycle`]), so Put and Delete write the register
//! and announce the change, and Get answers from the gateway's local copy.
//! Each shard primary evaluates the configuration over its index
//! (`skys3_shard::lifecycle`). Only `local` buckets have one: a
//! `write_back` bucket's objects belong to its remote target, whose own
//! lifecycle rules govern them, so Put answers `501 NotImplemented` there,
//! for `write_through` buckets too.
//!
//! What is supported, and what S3 answers otherwise:
//!
//! | Element | Answer |
//! |---|---|
//! | `Expiration` with `Days` or `Date` | supported; `Date` must be at midnight UTC |
//! | `AbortIncompleteMultipartUpload` | supported, with a prefix filter only |
//! | `Filter` with `Prefix`, `Tag`, `ObjectSizeGreaterThan`, `ObjectSizeLessThan`, or `And` of them; a rule's own `Prefix` | supported |
//! | `Expiration` with `ExpiredObjectDeleteMarker` true, `NoncurrentVersionExpiration`, `NoncurrentVersionTransition` | `501 NotImplemented`: buckets are unversioned |
//! | `Transition` | `501 NotImplemented`: SkyS3 has no storage classes |
//!
//! A rule without an ID gets a random one, as in S3. The configuration is
//! returned in an equivalent form: a filter with one condition as that
//! condition, with several as `And`, and with none as an empty `Prefix`.

use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use s3s::dto::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule as S3Rule, LifecycleRuleAndOperator, LifecycleRuleFilter,
    Tag, Timestamp,
};
use s3s::{S3Error, S3Result, s3_error};
use skys3_types::lifecycle::{
    DAY_MS, Expiration, LifecycleConfiguration, LifecycleError, LifecycleRule, RuleFilter,
};

#[cfg(doc)]
use skys3_types::BucketDocument;

use crate::objects::tags_from_xml;

/// The configuration a PutBucketLifecycleConfiguration body describes.
///
/// # Errors
///
/// The error S3 answers for a configuration it refuses, and
/// `501 NotImplemented` for the actions SkyS3 does not support.
pub(crate) fn from_s3(input: BucketLifecycleConfiguration) -> S3Result<LifecycleConfiguration> {
    let rules = input
        .rules
        .into_iter()
        .map(rule_from_s3)
        .collect::<S3Result<Vec<_>>>()?;
    let config = LifecycleConfiguration { rules };
    config.validate().map_err(refusal)?;
    Ok(config)
}

/// A GetBucketLifecycleConfiguration answer's rules for `config`.
pub(crate) fn to_s3(config: &LifecycleConfiguration) -> Vec<S3Rule> {
    config.rules.iter().map(rule_to_s3).collect()
}

fn rule_from_s3(rule: S3Rule) -> S3Result<LifecycleRule> {
    if rule.transitions.is_some_and(|list| !list.is_empty()) {
        return Err(s3_error!(
            NotImplemented,
            "Transition actions are not supported: SkyS3 has no storage classes"
        ));
    }
    if rule.noncurrent_version_expiration.is_some()
        || rule
            .noncurrent_version_transitions
            .is_some_and(|list| !list.is_empty())
    {
        return Err(s3_error!(
            NotImplemented,
            "Noncurrent version actions are not supported: SkyS3 buckets are unversioned"
        ));
    }
    let enabled = match rule.status.as_str() {
        ExpirationStatus::ENABLED => true,
        ExpirationStatus::DISABLED => false,
        _ => return Err(malformed("Status must be Enabled or Disabled")),
    };
    let filter = match (rule.filter, rule.prefix) {
        (Some(filter), None) => filter_from_s3(filter)?,
        (None, Some(prefix)) => RuleFilter {
            prefix,
            legacy_prefix: true,
            ..RuleFilter::default()
        },
        (Some(_), Some(_)) => return Err(malformed("A rule has a Filter or a Prefix, not both")),
        (None, None) => return Err(malformed("A rule needs a Filter or a Prefix")),
    };
    let expiration = rule
        .expiration
        .map(expiration_from_s3)
        .transpose()?
        .flatten();
    let abort_upload_days = rule
        .abort_incomplete_multipart_upload
        .map(|abort| match abort.days_after_initiation {
            Some(days) => positive(
                days,
                "DaysAfterInitiation",
                "AbortIncompleteMultipartUpload",
            ),
            None => Err(malformed(
                "AbortIncompleteMultipartUpload needs DaysAfterInitiation",
            )),
        })
        .transpose()?;
    Ok(LifecycleRule {
        id: rule.id.unwrap_or_else(generated_id),
        enabled,
        filter,
        expiration,
        abort_upload_days,
    })
}

/// A rule's filter: at most one condition, or an `And` of several.
fn filter_from_s3(filter: LifecycleRuleFilter) -> S3Result<RuleFilter> {
    let given = usize::from(filter.prefix.is_some())
        + usize::from(filter.tag.is_some())
        + usize::from(filter.and.is_some())
        + usize::from(filter.object_size_greater_than.is_some())
        + usize::from(filter.object_size_less_than.is_some());
    if given > 1 {
        return Err(malformed(
            "A Filter has one of Prefix, Tag, ObjectSizeGreaterThan, ObjectSizeLessThan, \
             or And",
        ));
    }
    let (prefix, tags, greater, less) = match filter.and {
        Some(and) => (
            and.prefix,
            and.tags.unwrap_or_default(),
            and.object_size_greater_than,
            and.object_size_less_than,
        ),
        None => (
            filter.prefix,
            filter.tag.into_iter().collect(),
            filter.object_size_greater_than,
            filter.object_size_less_than,
        ),
    };
    Ok(RuleFilter {
        prefix: prefix.unwrap_or_default(),
        tags: if tags.is_empty() {
            BTreeMap::new()
        } else {
            tags_from_xml(tags)?
        },
        size_greater_than: greater.map(size).transpose()?,
        size_less_than: less.map(size).transpose()?,
        legacy_prefix: false,
    })
}

/// An `Expiration`: `None` for one that only sets
/// `ExpiredObjectDeleteMarker` to false, which does nothing.
fn expiration_from_s3(expiration: LifecycleExpiration) -> S3Result<Option<Expiration>> {
    if expiration.expired_object_delete_marker == Some(true) {
        return Err(s3_error!(
            NotImplemented,
            "ExpiredObjectDeleteMarker is not supported: SkyS3 buckets are unversioned"
        ));
    }
    match (expiration.days, expiration.date) {
        (Some(days), None) => Ok(Some(Expiration::Days(positive(
            days,
            "Days",
            "Expiration",
        )?))),
        (None, Some(date)) => {
            let nanos = time::OffsetDateTime::from(date).unix_timestamp_nanos();
            let ms = u64::try_from(nanos / 1_000_000)
                .map_err(|_| s3_error!(InvalidArgument, "'Date' must be after 1970"))?;
            if ms % DAY_MS != 0 || nanos % 1_000_000 != 0 {
                return Err(s3_error!(InvalidArgument, "'Date' must be at midnight GMT"));
            }
            Ok(Some(Expiration::DateMs(ms)))
        }
        (Some(_), Some(_)) => Err(malformed("An Expiration has Days or Date, not both")),
        (None, None) if expiration.expired_object_delete_marker.is_some() => Ok(None),
        (None, None) => Err(malformed("An Expiration needs Days or Date")),
    }
}

fn positive(days: i32, name: &str, action: &str) -> S3Result<u32> {
    u32::try_from(days)
        .ok()
        .filter(|&days| days > 0)
        .ok_or_else(|| {
            s3_error!(
                InvalidArgument,
                "'{name}' for {action} action must be a positive integer"
            )
        })
}

fn size(bytes: i64) -> S3Result<u64> {
    u64::try_from(bytes).map_err(|_| {
        s3_error!(
            InvalidArgument,
            "ObjectSizeGreaterThan and ObjectSizeLessThan must not be negative"
        )
    })
}

/// A fresh rule ID, as S3 gives a rule without one.
fn generated_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

fn malformed(reason: &str) -> S3Error {
    s3_error!(
        MalformedXML,
        "The XML you provided was not well-formed or did not validate against our published \
         schema: {reason}"
    )
}

/// The answer S3 gives for a configuration that breaks `error`'s rule.
fn refusal(error: LifecycleError) -> S3Error {
    match error {
        LifecycleError::NoRules | LifecycleError::LegacyPrefixWithConditions(_) => {
            malformed(&error.to_string())
        }
        LifecycleError::NoAction(_)
        | LifecycleError::UploadFilter(_)
        | LifecycleError::TooManyRules(_)
        | LifecycleError::TooLarge(_) => s3_error!(InvalidRequest, "{error}"),
        LifecycleError::InvalidTags(_) => s3_error!(InvalidTag, "{error}"),
        _ => s3_error!(InvalidArgument, "{error}"),
    }
}

fn rule_to_s3(rule: &LifecycleRule) -> S3Rule {
    let status = if rule.enabled {
        ExpirationStatus::ENABLED
    } else {
        ExpirationStatus::DISABLED
    };
    let (filter, prefix) = if rule.filter.legacy_prefix {
        (None, Some(rule.filter.prefix.clone()))
    } else {
        (Some(filter_to_s3(&rule.filter)), None)
    };
    S3Rule {
        abort_incomplete_multipart_upload: rule.abort_upload_days.map(|days| {
            AbortIncompleteMultipartUpload {
                days_after_initiation: Some(clamp(days)),
            }
        }),
        expiration: rule.expiration.map(|expiration| match expiration {
            Expiration::Days(days) => LifecycleExpiration {
                days: Some(clamp(days)),
                ..LifecycleExpiration::default()
            },
            Expiration::DateMs(ms) => LifecycleExpiration {
                date: Some(Timestamp::from(UNIX_EPOCH + Duration::from_millis(ms))),
                ..LifecycleExpiration::default()
            },
        }),
        filter,
        id: Some(rule.id.clone()),
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
        prefix,
        status: ExpirationStatus::from_static(status),
        transitions: None,
    }
}

fn filter_to_s3(filter: &RuleFilter) -> LifecycleRuleFilter {
    let prefix = (!filter.prefix.is_empty()).then(|| filter.prefix.clone());
    let tags: Vec<Tag> = filter
        .tags
        .iter()
        .map(|(key, value)| Tag {
            key: Some(key.clone()),
            value: Some(value.clone()),
        })
        .collect();
    let greater = filter.size_greater_than.map(signed);
    let less = filter.size_less_than.map(signed);
    let conditions = usize::from(prefix.is_some())
        + tags.len()
        + usize::from(greater.is_some())
        + usize::from(less.is_some());
    if conditions > 1 {
        return LifecycleRuleFilter {
            and: Some(LifecycleRuleAndOperator {
                object_size_greater_than: greater,
                object_size_less_than: less,
                prefix,
                tags: (!tags.is_empty()).then_some(tags),
            }),
            ..LifecycleRuleFilter::default()
        };
    }
    LifecycleRuleFilter {
        // A filter without conditions matches every key: an empty prefix.
        prefix: if conditions == 0 {
            Some(String::new())
        } else {
            prefix
        },
        tag: tags.into_iter().next(),
        object_size_greater_than: greater,
        object_size_less_than: less,
        and: None,
    }
}

fn clamp(days: u32) -> i32 {
    i32::try_from(days).unwrap_or(i32::MAX)
}

fn signed(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}
