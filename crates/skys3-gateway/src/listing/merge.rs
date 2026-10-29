//! The k-way merge of a bucket's shard pages into one page (§9.4).
//!
//! Every shard of the bucket is asked for a page after the listing's
//! `start_after`. The merge then repeatedly takes the smallest head among
//! the shards' pages; a common prefix may come from several shards, and is
//! taken once, dropping it from the heads of the others. A shard asks for
//! roughly its share of the page first. Before an item is taken, a shard
//! whose page is used up but has more is asked for its next page, since
//! its next item may be the smallest; so the merge never takes an item past
//! one it has not seen. When the page is full, the listing is truncated if
//! any shard has an item left, in its page or beyond it.

use std::collections::VecDeque;

use skys3_index::{ListItem, ListPage, ListQuery};
use skys3_types::BucketDocument;
use tokio::task::JoinSet;

use crate::shard::{ShardError, ShardRef, Shards};

/// The fewest items a shard is asked for at a time, so that a bucket with
/// many shards does not ask each for one or two items.
const MIN_BATCH: usize = 16;

/// One merged page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MergedPage {
    /// The page's items in order, at most `max_items` of them.
    pub(crate) items: Vec<ListItem>,
    /// Whether more items follow the last.
    pub(crate) truncated: bool,
}

/// What the merge holds of one shard.
struct Cursor {
    shard: ShardRef,
    /// Fetched items not yet taken.
    items: VecDeque<ListItem>,
    /// Whether the shard has items after the last one fetched.
    more: bool,
    /// The last item fetched, which the next fetch starts after.
    last: Option<String>,
}

/// Lists one page of `bucket` across its shards, as `query` describes.
///
/// # Errors
///
/// The first error of a shard; a listing needs every shard.
pub(crate) async fn list<H: Shards>(
    shards: &H,
    bucket: &BucketDocument,
    query: &ListQuery,
) -> Result<MergedPage, ShardError> {
    list_in_batches(shards, bucket, query, MIN_BATCH).await
}

/// [`list`], asking shards for at least `min_batch` items at a time.
async fn list_in_batches<H: Shards>(
    shards: &H,
    bucket: &BucketDocument,
    query: &ListQuery,
    min_batch: usize,
) -> Result<MergedPage, ShardError> {
    let max = query.max_items;
    if max == 0 {
        return Ok(MergedPage::default());
    }
    let mut cursors: Vec<_> = ShardRef::all(bucket)
        .map(|shard| Cursor {
            shard,
            items: VecDeque::new(),
            more: true,
            last: query.start_after.clone(),
        })
        .collect();
    let mut batch = max.div_ceil(cursors.len().max(1)).max(min_batch).min(max);
    let mut page = Vec::new();
    while page.len() < max {
        let stale: Vec<usize> = (0..cursors.len())
            .filter(|&i| cursors[i].items.is_empty() && cursors[i].more)
            .collect();
        if !stale.is_empty() {
            refill(shards, query, &mut cursors, &stale, batch).await?;
            batch = (max - page.len()).max(min_batch).min(max);
            continue;
        }
        let Some(item) = cursors
            .iter_mut()
            .filter(|cursor| !cursor.items.is_empty())
            .min_by(|a, b| a.items[0].name().cmp(b.items[0].name()))
            .and_then(|cursor| cursor.items.pop_front())
        else {
            break;
        };
        if let ListItem::Prefix(prefix) = &item {
            for cursor in &mut cursors {
                if cursor
                    .items
                    .front()
                    .is_some_and(|head| head.name() == prefix)
                {
                    cursor.items.pop_front();
                }
            }
        }
        page.push(item);
    }
    let truncated = cursors
        .iter()
        .any(|cursor| cursor.more || !cursor.items.is_empty());
    Ok(MergedPage {
        items: page,
        truncated,
    })
}

/// Fetches the next page of each cursor in `stale`, concurrently.
async fn refill<H: Shards>(
    shards: &H,
    query: &ListQuery,
    cursors: &mut [Cursor],
    stale: &[usize],
    batch: usize,
) -> Result<(), ShardError> {
    let mut fetches = JoinSet::new();
    for &i in stale {
        let shards = shards.clone();
        let shard = cursors[i].shard.clone();
        let query = ListQuery {
            start_after: cursors[i].last.clone(),
            max_items: batch,
            ..query.clone()
        };
        fetches.spawn(async move { (i, shards.list(&shard, &query).await) });
    }
    while let Some(fetched) = fetches.join_next().await {
        let (i, page) = match fetched {
            Ok(fetched) => fetched,
            Err(error) => match error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(error) => {
                    return Err(ShardError::Unavailable {
                        shard: cursors[stale[0]].shard.clone(),
                        reason: error.to_string(),
                    });
                }
            },
        };
        let ListPage { items, truncated } = page?;
        let cursor = &mut cursors[i];
        // A page that is empty ends the shard, whatever it claims, so a
        // faulty shard cannot keep the merge asking.
        cursor.more = truncated && !items.is_empty();
        if let Some(last) = items.last() {
            cursor.last = Some(last.name().to_owned());
        }
        cursor.items.extend(items);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use skys3_types::{BucketId, BucketMode, ProposalId, ShardCount};

    use super::*;
    use crate::stub::{EntryState, MemoryShards};

    fn bucket(shards: u32) -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new(format!("b-{shards}")).unwrap(),
            name: "photos".parse().unwrap(),
            mode: BucketMode::WriteBack,
            shards: ShardCount::new(shards).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    /// Shards of `bucket` holding `keys` in the given states.
    async fn shards_with(
        bucket: &BucketDocument,
        keys: &BTreeMap<String, EntryState>,
    ) -> MemoryShards {
        let shards = MemoryShards::new().await;
        for shard in ShardRef::all(bucket) {
            shards.open(&shard, bucket).await.unwrap();
        }
        for (key, state) in keys {
            shards.put(bucket, key, *state).await.unwrap();
        }
        shards
    }

    /// Item names, with common prefixes marked by a leading `+`.
    fn names(items: &[ListItem]) -> Vec<String> {
        items
            .iter()
            .map(|item| match item {
                ListItem::Object { key, .. } => key.clone(),
                ListItem::Prefix(prefix) => format!("+{prefix}"),
            })
            .collect()
    }

    /// The whole listing, from one sorted model of the live keys.
    fn model(keys: &BTreeMap<String, EntryState>, query: &ListQuery) -> Vec<String> {
        let items: BTreeSet<(String, bool)> = keys
            .iter()
            .filter(|(key, state)| {
                **state != EntryState::Tombstone && key.starts_with(&query.prefix)
            })
            .map(|(key, _)| match query.common_prefix(key) {
                Some(prefix) => (prefix.to_owned(), true),
                None => (key.clone(), false),
            })
            .filter(|(name, _)| query.start_after.as_ref().is_none_or(|after| name > after))
            .collect();
        items
            .into_iter()
            .map(|(name, prefix)| if prefix { format!("+{name}") } else { name })
            .collect()
    }

    /// Lists everything in pages of `max_items`, resuming after each page's
    /// last item as a continuation token does, and checks each page
    /// against the model.
    async fn list_all(
        shards: &MemoryShards,
        bucket: &BucketDocument,
        keys: &BTreeMap<String, EntryState>,
        query: &ListQuery,
        min_batch: usize,
    ) -> Vec<String> {
        let expected = model(keys, query);
        let mut listed = Vec::new();
        let mut query = query.clone();
        loop {
            let page = list_in_batches(shards, bucket, &query, min_batch)
                .await
                .unwrap();
            let names = names(&page.items);
            let from = listed.len();
            let want = &expected[from..expected.len().min(from + query.max_items)];
            assert_eq!(names, want, "{query:?}");
            listed.extend(names);
            assert_eq!(page.truncated, listed.len() < expected.len(), "{query:?}");
            if !page.truncated {
                return listed;
            }
            query.start_after = page.items.last().map(|item| item.name().to_owned());
        }
    }

    #[tokio::test]
    async fn pages_merge_across_shards() {
        let doc = bucket(4);
        let keys: BTreeMap<String, EntryState> = [
            ("a/1", EntryState::Dirty),
            ("a/2", EntryState::Clean),
            ("a/3", EntryState::Dirty),
            ("b", EntryState::Clean),
            ("c/1", EntryState::Tombstone),
            ("d/x/1", EntryState::Dirty),
            ("d/y", EntryState::Dirty),
            ("e", EntryState::Tombstone),
        ]
        .into_iter()
        .map(|(key, state)| (key.to_owned(), state))
        .collect();
        let shards = shards_with(&doc, &keys).await;
        // The keys are spread over several shards.
        let used: BTreeSet<_> = keys
            .keys()
            .map(|key| ShardRef::for_key(&doc, key))
            .collect();
        assert!(used.len() > 1);
        let query = |delimiter: Option<&str>, max| ListQuery {
            prefix: String::new(),
            delimiter: delimiter.map(str::to_owned),
            start_after: None,
            max_items: max,
        };
        let all = list_all(&shards, &doc, &keys, &query(None, 2), 1).await;
        assert_eq!(all, ["a/1", "a/2", "a/3", "b", "d/x/1", "d/y"]);
        let rolled = list_all(&shards, &doc, &keys, &query(Some("/"), 1), 1).await;
        assert_eq!(rolled, ["+a/", "b", "+d/"]);
        let whole = list(&shards, &doc, &query(Some("/"), 1000)).await.unwrap();
        assert_eq!(names(&whole.items), rolled);
        assert!(!whole.truncated);
        let empty = list(&shards, &doc, &query(Some("/"), 0)).await.unwrap();
        assert_eq!(empty, MergedPage::default());

        shards.set_unavailable(true);
        let error = list(&shards, &doc, &query(None, 10)).await.unwrap_err();
        assert!(matches!(error, ShardError::Unavailable { .. }), "{error}");
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        // A small alphabet makes shared prefixes and delimiters common.
        proptest::string::string_regex("[ab/.é]{1,6}").unwrap()
    }

    fn state_strategy() -> impl Strategy<Value = EntryState> {
        prop_oneof![
            Just(EntryState::Dirty),
            Just(EntryState::Clean),
            Just(EntryState::Tombstone)
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// Merged listings, paged with any page size and batch size, equal
        /// one sorted model of every shard's live keys.
        #[test]
        fn merged_listings_match_a_sorted_model(
            keys in proptest::collection::btree_map(key_strategy(), state_strategy(), 0..32),
            shard_count in 1u32..6,
            prefix in "[ab/]{0,2}",
            delimiter in proptest::option::of("[/.é]|a/"),
            max in 1usize..7,
            min_batch in 1usize..20,
            start_after in proptest::option::of("[ab/.]{0,3}"),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let doc = bucket(shard_count);
                let shards = shards_with(&doc, &keys).await;
                let query = ListQuery {
                    prefix,
                    delimiter,
                    start_after,
                    max_items: max,
                };
                let listed = list_all(&shards, &doc, &keys, &query, min_batch).await;
                assert_eq!(listed, model(&keys, &query));
            });
        }
    }
}
