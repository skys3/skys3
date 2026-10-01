//! One shard's page of a listing (§9.4): the shard's live objects in key
//! order, with prefix and delimiter handling.
//!
//! A listing is a sorted sequence of **items**. A key under the query's
//! prefix lists as itself, unless the rest of the key after the prefix
//! contains the delimiter: then it rolls up into the **common prefix** that
//! ends with the delimiter's first occurrence there, and the common prefix
//! is the item. A common prefix is listed once, however many keys roll up
//! into it, and only if at least one of them has a live object.
//!
//! Items compare as strings, by their UTF-8 bytes, which is the order S3
//! lists in. `start_after` names an item, not only a key: a page holds the
//! items greater than it. A common prefix at or before `start_after` is
//! therefore skipped whole, even where some of its keys sort after
//! `start_after`, so a listing resumed after a common prefix does not list
//! it again.
//!
//! Every page skips delete tombstones: they hide nothing (§4.2). Every other
//! entry has a live object, whatever its state (dirty, flushing, clean, in
//! conflict, or an evicted stub), and lists.

use std::ops::Bound;

use redb::ReadableTable;
use skys3_log::record::ShardRef;

use crate::codec;
use crate::entry::ObjectVersion;
use crate::error::IndexError;
use crate::tables::prefix_end;

/// What a page of a shard's listing asks for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListQuery {
    /// Only keys that start with this are listed; empty for every key.
    pub prefix: String,
    /// The delimiter that rolls keys up into common prefixes, or `None`.
    /// An empty delimiter rolls up nothing, like `None`.
    pub delimiter: Option<String>,
    /// Only items greater than this are listed.
    pub start_after: Option<String>,
    /// The most items the page holds, objects and common prefixes
    /// together.
    pub max_items: usize,
}

impl ListQuery {
    /// The common prefix `key` rolls up into, or `None` if it lists as
    /// itself. `key` must start with the query's prefix.
    ///
    /// ```
    /// use skys3_index::ListQuery;
    ///
    /// let query = ListQuery {
    ///     prefix: "photos/".into(),
    ///     delimiter: Some("/".into()),
    ///     ..ListQuery::default()
    /// };
    /// assert_eq!(query.common_prefix("photos/2024/cat.jpg"), Some("photos/2024/"));
    /// assert_eq!(query.common_prefix("photos/cat.jpg"), None);
    /// ```
    #[must_use]
    pub fn common_prefix<'k>(&self, key: &'k str) -> Option<&'k str> {
        let delimiter = self.delimiter.as_deref().filter(|d| !d.is_empty())?;
        let rest = key.get(self.prefix.len()..)?;
        let at = rest.find(delimiter)?;
        Some(&key[..self.prefix.len() + at + delimiter.len()])
    }

    /// Whether the item `name` is after the query's `start_after`.
    fn is_after_start(&self, name: &str) -> bool {
        self.start_after
            .as_deref()
            .is_none_or(|start_after| name > start_after)
    }
}

/// One item of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListItem {
    /// A key and its live object.
    Object {
        /// The key.
        key: String,
        /// The key's latest version.
        object: Box<ObjectVersion>,
    },
    /// A common prefix: keys that roll up into it have live objects.
    Prefix(String),
}

impl ListItem {
    /// The item's name, which listings sort by: the key or the common
    /// prefix.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Object { key, .. } => key,
            Self::Prefix(prefix) => prefix,
        }
    }
}

/// One shard's page of a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    /// The first items after the query's `start_after`, in order, at most
    /// `max_items` of them.
    pub items: Vec<ListItem>,
    /// Whether the shard has more items after the last one in the page.
    pub truncated: bool,
}

/// Reads one page of `shard`'s listing from the namespace table.
pub(crate) fn list<T: ReadableTable<&'static [u8], &'static [u8]>>(
    namespace: &T,
    shard: &ShardRef,
    query: &ListQuery,
) -> Result<ListPage, IndexError> {
    // Every key of the shard under the prefix sorts before `end`.
    let end = prefix_end(&codec::entry_key(shard, &query.prefix));
    let mut start = match query.start_after.as_deref() {
        Some(after) if after >= query.prefix.as_str() => {
            Bound::Excluded(codec::entry_key(shard, after))
        }
        _ => Bound::Included(codec::entry_key(shard, &query.prefix)),
    };
    let mut page = ListPage::default();
    while let Some(scan) = next(namespace, &start, &end, query)? {
        // Past the item, and past every key that rolls up into a common
        // prefix.
        start = match &scan {
            Scan::Item(ListItem::Object { key, .. }) => {
                Bound::Excluded(codec::entry_key(shard, key))
            }
            Scan::Item(ListItem::Prefix(prefix)) | Scan::Skip(prefix) => {
                Bound::Included(prefix_end(&codec::entry_key(shard, prefix)))
            }
        };
        if let Scan::Item(item) = scan {
            if page.items.len() == query.max_items {
                page.truncated = true;
                break;
            }
            page.items.push(item);
        }
    }
    Ok(page)
}

/// What a page's scan found next.
enum Scan {
    /// The next item.
    Item(ListItem),
    /// A common prefix at or before `start_after`, whose keys the scan
    /// skips.
    Skip(String),
}

/// Scans from `start` for the next item, skipping tombstones; `None` once
/// no item is left before `end`.
fn next<T: ReadableTable<&'static [u8], &'static [u8]>>(
    namespace: &T,
    start: &Bound<Vec<u8>>,
    end: &[u8],
    query: &ListQuery,
) -> Result<Option<Scan>, IndexError> {
    let lower = start.as_ref().map(Vec::as_slice);
    for row in namespace.range::<&[u8]>((lower, Bound::Excluded(end)))? {
        let (key, value) = row?;
        let (_, key) =
            codec::decode_entry_key(key.value()).map_err(IndexError::codec("namespace"))?;
        let prefix = query.common_prefix(&key);
        if let Some(prefix) = prefix
            && !query.is_after_start(prefix)
        {
            return Ok(Some(Scan::Skip(prefix.to_owned())));
        }
        let entry = codec::decode_entry(value.value()).map_err(IndexError::codec("namespace"))?;
        let Some(object) = entry.object else {
            continue;
        };
        let item = match prefix {
            Some(prefix) => ListItem::Prefix(prefix.to_owned()),
            None => ListItem::Object {
                key,
                object: Box::new(object),
            },
        };
        return Ok(Some(Scan::Item(item)));
    }
    Ok(None)
}
