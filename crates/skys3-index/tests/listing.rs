//! Tests of one shard's listing pages (§9.4): prefixes, delimiters,
//! `start_after`, tombstones, and page limits, against a sorted model.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use proptest::prelude::*;
use skys3_index::{Index, ListItem, ListPage, ListQuery};
use skys3_io::SimDisk;
use skys3_log::record::{Delete, Put, PutData, RecordBody};
use skys3_log::{LogRecord, RecordLocation, SegmentId};
use skys3_types::{ETag, Epoch, EpochSeq, Seq};
use support::{TestApplier, index_config, shard};

/// An index holding `keys` in shard 0, each a live object (`true`) or a
/// tombstone, and the key `other` in shard 2 of the same bucket.
fn index_with(keys: &BTreeMap<String, bool>) -> (SimDisk, Index) {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let records: Vec<_> = keys
        .iter()
        .map(|(key, live)| (0, key.as_str(), *live))
        .chain([(2, "other", true)])
        .zip(1..)
        .map(|((shard_no, key, live), seq)| record(shard_no, seq, key, live))
        .collect();
    index.apply(&TestApplier, &records).unwrap();
    (disk, index)
}

fn record(shard_no: u8, seq: u64, key: &str, live: bool) -> (LogRecord, RecordLocation) {
    let body = if live {
        RecordBody::Put(Put {
            key: key.into(),
            size: seq,
            last_modified_ms: 0,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: PutData::Inline(Bytes::new()),
        })
    } else {
        RecordBody::Delete(Delete { key: key.into() })
    };
    let record = LogRecord {
        shard: shard(shard_no),
        position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
        body,
    };
    let location = RecordLocation {
        segment: SegmentId::new(0),
        offset: seq * 100,
        len: 100,
    };
    (record, location)
}

fn query(
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
    max: usize,
) -> ListQuery {
    ListQuery {
        prefix: prefix.into(),
        delimiter: delimiter.map(Into::into),
        start_after: start_after.map(Into::into),
        max_items: max,
    }
}

/// A page's item names, with common prefixes marked by a leading `+`.
fn names(page: &ListPage) -> Vec<String> {
    page.items
        .iter()
        .map(|item| match item {
            ListItem::Object { key, .. } => key.clone(),
            ListItem::Prefix(prefix) => format!("+{prefix}"),
        })
        .collect()
}

/// The page a single sorted model of the live keys gives.
fn model(keys: &BTreeMap<String, bool>, query: &ListQuery) -> (Vec<String>, bool) {
    let items: BTreeSet<(String, bool)> = keys
        .iter()
        .filter(|(key, live)| **live && key.starts_with(&query.prefix))
        .map(|(key, _)| match query.common_prefix(key) {
            Some(prefix) => (prefix.to_owned(), true),
            None => (key.clone(), false),
        })
        .filter(|(name, _)| query.start_after.as_ref().is_none_or(|after| name > after))
        .collect();
    let mut items: Vec<_> = items.into_iter().collect();
    // Sort by name alone: a common prefix and a key never share a name.
    items.sort();
    let truncated = items.len() > query.max_items;
    let names = items
        .into_iter()
        .take(query.max_items)
        .map(|(name, prefix)| if prefix { format!("+{name}") } else { name })
        .collect();
    (names, truncated)
}

fn keys(live: &[&str], dead: &[&str]) -> BTreeMap<String, bool> {
    live.iter()
        .map(|key| ((*key).to_owned(), true))
        .chain(dead.iter().map(|key| ((*key).to_owned(), false)))
        .collect()
}

#[test]
fn a_page_lists_live_objects_and_common_prefixes_in_order() {
    let keys = keys(
        &["a", "b/1", "b/2", "c/x/1", "d", "日本/語"],
        &["c/1", "e", "f/1", "f/2"],
    );
    let (_disk, index) = index_with(&keys);
    let read = index.read().unwrap();
    let list = |query: ListQuery| {
        let page = read.list(&shard(0), &query).unwrap();
        assert_eq!(
            (names(&page), page.truncated),
            model(&keys, &query),
            "{query:?}"
        );
        (names(&page), page.truncated)
    };

    let (all, truncated) = list(query("", None, None, 100));
    assert_eq!(all, ["a", "b/1", "b/2", "c/x/1", "d", "日本/語"]);
    assert!(!truncated);

    // A common prefix whose only keys are tombstones is not listed.
    let (rolled, _) = list(query("", Some("/"), None, 100));
    assert_eq!(rolled, ["a", "+b/", "+c/", "d", "+日本/"]);
    let (under, _) = list(query("c/", Some("/"), None, 100));
    assert_eq!(under, ["+c/x/"]);

    // Truncation says whether an item follows, not whether a key does.
    assert_eq!(
        list(query("", Some("/"), None, 2)),
        (vec!["a".into(), "+b/".into()], true)
    );
    assert_eq!(
        list(query("b", Some("/"), None, 1)),
        (vec!["+b/".into()], false)
    );
    assert_eq!(list(query("", None, None, 0)), (vec![], true));
    assert_eq!(list(query("zzz", None, None, 0)), (vec![], false));

    // Resuming after a common prefix skips all its keys, and an item
    // greater than start_after is listed even where it starts before it.
    assert_eq!(list(query("", Some("/"), Some("b/"), 2)).0, ["+c/", "d"]);
    assert_eq!(
        list(query("", Some("/"), Some("b/1"), 10)).0,
        ["+c/", "d", "+日本/"]
    );
    assert_eq!(list(query("", Some("/"), Some("b"), 1)).0, ["+b/"]);
    assert_eq!(list(query("b/", None, Some("a"), 10)).0, ["b/1", "b/2"]);
    assert_eq!(list(query("b/", None, Some("b/1"), 10)).0, ["b/2"]);

    // An empty delimiter rolls up nothing; a longer one works like any.
    assert_eq!(list(query("", Some(""), None, 100)).0, all);
    assert_eq!(
        list(query("", Some("/x/"), None, 100)).0,
        ["a", "b/1", "b/2", "+c/x/", "d", "日本/語"]
    );

    // Another shard of the bucket lists only its own keys.
    let other = read.list(&shard(2), &query("", None, None, 10)).unwrap();
    assert_eq!(names(&other), ["other"]);
    assert!(
        read.list(&shard(4), &query("", None, None, 10))
            .unwrap()
            .items
            .is_empty()
    );
}

#[test]
fn objects_carry_their_latest_version() {
    let keys = keys(&["k"], &[]);
    let (_disk, index) = index_with(&keys);
    let page = index
        .read()
        .unwrap()
        .list(&shard(0), &query("", None, None, 1))
        .unwrap();
    let [ListItem::Object { key, object }] = page.items.as_slice() else {
        panic!("{page:?}");
    };
    assert_eq!(key, "k");
    assert_eq!(object.size, 1);
    assert_eq!(page.items[0].name(), "k");
}

fn key_strategy() -> impl Strategy<Value = String> {
    // A small alphabet makes shared prefixes and delimiters common.
    proptest::string::string_regex("[ab/.é]{1,6}").unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn pages_match_a_sorted_model(
        keys in proptest::collection::btree_map(key_strategy(), any::<bool>(), 0..40),
        prefix in "[ab/]{0,2}",
        delimiter in proptest::option::of("[/.é]|a/|"),
        max in 0usize..6,
    ) {
        let (_disk, index) = index_with(&keys);
        let read = index.read().unwrap();
        // Page through the whole listing, resuming after each page's last
        // item, and check every page against the model.
        let mut start_after: Option<String> = None;
        for _ in 0..100 {
            let query = ListQuery {
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                start_after: start_after.clone(),
                max_items: max.max(1),
            };
            let page = read.list(&shard(0), &query).unwrap();
            prop_assert_eq!((names(&page), page.truncated), model(&keys, &query));
            if !page.truncated {
                break;
            }
            start_after = page.items.last().map(|item| item.name().to_owned());
        }
        // A start_after that is not an item works too.
        let query = ListQuery {
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            start_after: Some(format!("{prefix}a")),
            max_items: max,
        };
        let page = read.list(&shard(0), &query).unwrap();
        prop_assert_eq!((names(&page), page.truncated), model(&keys, &query));
    }
}
