//! Object tags (design §10.1, §11): S3's limits on them, the
//! `x-amz-tagging` header of PutObject and CopyObject, and
//! GetObjectTagging, PutObjectTagging, and DeleteObjectTagging.
//!
//! Tags given with an upload are part of its `PUT` record. A later change
//! is a `TAGS` record that replaces the whole set (an empty one for
//! DeleteObjectTagging), committed only if the key has an object. Applying
//! it makes a new version of the object with the same bytes, ETag, and
//! `Last-Modified`, which the flusher sends to the remote (§4.2).
//!
//! A tag set holds at most [`MAX_OBJECT_TAGS`] tags with distinct keys. A
//! key is 1 to [`MAX_TAG_KEY_CHARS`] Unicode characters and a value at most
//! [`MAX_TAG_VALUE_CHARS`], both made of letters, digits, spaces, and
//! `+ - = . _ : / @`; keys starting with `aws:` are reserved. A request
//! that breaks a limit gets `400 InvalidTag`, and an `x-amz-tagging` header
//! that is not a query string of distinct keys `400 InvalidArgument`, as
//! from S3.

use s3s::dto::{
    DeleteObjectTaggingInput, DeleteObjectTaggingOutput, GetObjectTaggingInput,
    GetObjectTaggingOutput, PutObjectTaggingInput, PutObjectTaggingOutput, Tag,
};
use s3s::{S3Error, S3Result, s3_error};
use skys3_log::RecordBody;
use skys3_log::record::{TagSet, Tags};
use skys3_types::BucketDocument;

use super::Objects;
use crate::buckets::shard_error;
use crate::conditions::{Precondition, no_such_key};
use crate::shard::{ShardRef, Shards};

/// The most tags on an object (the S3 limit).
pub const MAX_OBJECT_TAGS: usize = 10;

/// The longest tag key, in Unicode characters (the S3 limit).
pub const MAX_TAG_KEY_CHARS: usize = 128;

/// The longest tag value, in Unicode characters (the S3 limit).
pub const MAX_TAG_VALUE_CHARS: usize = 256;

/// The prefix of tag keys reserved for AWS.
const RESERVED_PREFIX: &str = "aws:";

/// Parses an `x-amz-tagging` header: tags as URL query parameters,
/// `key1=value1&key2=value2`, with distinct keys.
///
/// # Errors
///
/// `400 InvalidArgument` for a header that is not a query string of
/// distinct keys, and `400 InvalidTag` for tags that break S3's limits.
pub(crate) fn parse_tagging_header(header: &str) -> S3Result<TagSet> {
    let invalid = || {
        s3_error!(
            InvalidArgument,
            "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query \
             parameters without tag name duplicates."
        )
    };
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(header).map_err(|_| invalid())?;
    let mut tags = TagSet::new();
    for (key, value) in pairs {
        if tags.contains_key(&key) {
            return Err(invalid());
        }
        tags.insert(key, value);
    }
    check_tags(&tags)?;
    Ok(tags)
}

/// The tag set of a PutObjectTagging body.
///
/// # Errors
///
/// `400 MalformedXML` for a tag without a key or a value, and
/// `400 InvalidTag` for a repeated key or tags that break S3's limits.
pub(crate) fn tags_from_xml(tag_set: Vec<Tag>) -> S3Result<TagSet> {
    if tag_set.len() > MAX_OBJECT_TAGS {
        return Err(too_many());
    }
    let mut tags = TagSet::new();
    for tag in tag_set {
        let (Some(key), Some(value)) = (tag.key, tag.value) else {
            return Err(s3_error!(
                MalformedXML,
                "Each Tag needs a Key and a Value element"
            ));
        };
        if tags.insert(key, value).is_some() {
            return Err(s3_error!(
                InvalidTag,
                "Cannot provide multiple Tags with the same key"
            ));
        }
    }
    check_tags(&tags)?;
    Ok(tags)
}

/// Encodes `tags` as an `x-amz-tagging` header value.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn tagging_header(tags: &TagSet) -> String {
    serde_urlencoded::to_string(tags).unwrap_or_default()
}

/// Checks a tag set against S3's limits.
///
/// # Errors
///
/// `400 InvalidTag`.
pub(crate) fn check_tags(tags: &TagSet) -> S3Result<()> {
    if tags.len() > MAX_OBJECT_TAGS {
        return Err(too_many());
    }
    for (key, value) in tags {
        if key.is_empty() || !key.chars().all(allowed) {
            return Err(s3_error!(
                InvalidTag,
                "The TagKey you have provided is invalid"
            ));
        }
        if key.chars().count() > MAX_TAG_KEY_CHARS {
            return Err(s3_error!(
                InvalidTag,
                "The TagKey you have provided is too long, max {MAX_TAG_KEY_CHARS}"
            ));
        }
        if key.starts_with(RESERVED_PREFIX) {
            return Err(s3_error!(
                InvalidTag,
                "Your TagKey cannot be prefixed with {RESERVED_PREFIX}"
            ));
        }
        if !value.chars().all(allowed) {
            return Err(s3_error!(
                InvalidTag,
                "The TagValue you have provided is invalid"
            ));
        }
        if value.chars().count() > MAX_TAG_VALUE_CHARS {
            return Err(s3_error!(
                InvalidTag,
                "The TagValue you have provided is too long, max {MAX_TAG_VALUE_CHARS}"
            ));
        }
    }
    Ok(())
}

/// Whether a tag key or value may contain `c`: S3 allows letters, digits,
/// and space separators (`[\p{L}\p{Z}\p{N}]`), and `_ . : / = + - @`.
fn allowed(c: char) -> bool {
    c.is_alphanumeric() || (c.is_whitespace() && !c.is_control()) || "_.:/=+-@".contains(c)
}

fn too_many() -> S3Error {
    s3_error!(
        InvalidTag,
        "Object tags cannot be greater than {MAX_OBJECT_TAGS}"
    )
}

/// The tags of a GetObjectTagging answer, in key order.
fn s3_tags(tags: &TagSet) -> Vec<Tag> {
    tags.iter()
        .map(|(key, value)| Tag {
            key: Some(key.clone()),
            value: Some(value.clone()),
        })
        .collect()
}

impl<H: Shards> Objects<H> {
    pub(crate) async fn get_tagging(
        &self,
        bucket: &BucketDocument,
        input: GetObjectTaggingInput,
    ) -> S3Result<GetObjectTaggingOutput> {
        let shard = ShardRef::for_key(bucket, &input.key);
        let object = self
            .shards
            .entry(&shard, &input.key)
            .await
            .map_err(shard_error)?
            .and_then(|entry| entry.object)
            .ok_or_else(no_such_key)?;
        Ok(GetObjectTaggingOutput {
            tag_set: s3_tags(&object.tags),
            version_id: None,
        })
    }

    pub(crate) async fn put_tagging(
        &self,
        bucket: &BucketDocument,
        input: PutObjectTaggingInput,
        apply_by: Option<u64>,
    ) -> S3Result<PutObjectTaggingOutput> {
        let tags = tags_from_xml(input.tagging.tag_set)?;
        self.set_tags(bucket, input.key, tags, apply_by).await?;
        Ok(PutObjectTaggingOutput::default())
    }

    pub(crate) async fn delete_tagging(
        &self,
        bucket: &BucketDocument,
        input: DeleteObjectTaggingInput,
        apply_by: Option<u64>,
    ) -> S3Result<DeleteObjectTaggingOutput> {
        self.set_tags(bucket, input.key, TagSet::new(), apply_by)
            .await?;
        Ok(DeleteObjectTaggingOutput::default())
    }

    /// Commits a `TAGS` record that replaces the tags of `key`, if it has
    /// an object when the record is sequenced, by `apply_by` for a peer's
    /// write (§7.8).
    async fn set_tags(
        &self,
        bucket: &BucketDocument,
        key: String,
        tags: TagSet,
        apply_by: Option<u64>,
    ) -> S3Result<()> {
        let shard = ShardRef::for_key(bucket, &key);
        self.admit(bucket, &shard)?;
        let record = RecordBody::Tags(Tags {
            key: key.clone(),
            tags,
        });
        let version = self
            .shards
            .write(&shard, record, Precondition::Exists.apply_by(apply_by))
            .await
            .map_err(shard_error)??;
        self.acknowledge(bucket, &shard, &key, version).await
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use s3s::S3ErrorCode;

    use super::*;

    /// Tag sets from S3's tag alphabet, with multibyte letters.
    fn valid_tags() -> impl Strategy<Value = TagSet> {
        let key = "[a-zA-Z0-9é語 _.:/=+@-]{1,24}"
            .prop_filter("reserved", |key: &String| !key.starts_with(RESERVED_PREFIX));
        let value = "[a-zA-Z0-9é語 _.:/=+@-]{0,24}";
        prop::collection::btree_map(key, value, 0..=MAX_OBJECT_TAGS)
    }

    fn code(result: S3Result<TagSet>) -> S3ErrorCode {
        result.unwrap_err().code().clone()
    }

    proptest! {
        #[test]
        fn valid_tag_sets_round_trip(tags in valid_tags()) {
            let header = tagging_header(&tags);
            prop_assert_eq!(parse_tagging_header(&header).unwrap(), tags.clone());
            prop_assert_eq!(tags_from_xml(s3_tags(&tags)).unwrap(), tags);
        }

        #[test]
        fn accepted_headers_are_within_the_limits(header in "[a-z&=%2B0-9:é ]{0,64}") {
            if let Ok(tags) = parse_tagging_header(&header) {
                prop_assert!(check_tags(&tags).is_ok());
                prop_assert_eq!(parse_tagging_header(&tagging_header(&tags)).unwrap(), tags);
            }
        }
    }

    #[test]
    fn limits_are_counted_in_characters() {
        let set = |key: String, value: String| TagSet::from([(key, value)]);
        let widest = set(
            "ü".repeat(MAX_TAG_KEY_CHARS),
            "語".repeat(MAX_TAG_VALUE_CHARS),
        );
        assert!(check_tags(&widest).is_ok());
        for tags in [
            set("ü".repeat(MAX_TAG_KEY_CHARS + 1), String::new()),
            set("k".to_owned(), "語".repeat(MAX_TAG_VALUE_CHARS + 1)),
            set(String::new(), String::new()),
            set("tab\tkey".to_owned(), String::new()),
            set("k".to_owned(), "new\nline".to_owned()),
            set("aws:k".to_owned(), String::new()),
        ] {
            assert_eq!(
                *check_tags(&tags).unwrap_err().code(),
                S3ErrorCode::InvalidTag,
                "{tags:?}"
            );
        }
    }

    #[test]
    fn malformed_input_is_refused() {
        assert_eq!(
            code(parse_tagging_header("a=1&a=2")),
            S3ErrorCode::InvalidArgument
        );
        assert!(parse_tagging_header("").unwrap().is_empty());
        assert_eq!(
            parse_tagging_header("a&b=").unwrap(),
            TagSet::from([
                ("a".to_owned(), String::new()),
                ("b".to_owned(), String::new())
            ])
        );
        let tag = |key: Option<&str>, value: Option<&str>| Tag {
            key: key.map(str::to_owned),
            value: value.map(str::to_owned),
        };
        assert_eq!(
            code(tags_from_xml(vec![tag(Some("k"), None)])),
            S3ErrorCode::MalformedXML
        );
        let twice = vec![tag(Some("k"), Some("1")), tag(Some("k"), Some("2"))];
        assert_eq!(code(tags_from_xml(twice)), S3ErrorCode::InvalidTag);
        let eleven = (0..=MAX_OBJECT_TAGS)
            .map(|n| tag(Some(&n.to_string()), Some("")))
            .collect();
        assert_eq!(code(tags_from_xml(eleven)), S3ErrorCode::InvalidTag);
    }
}
