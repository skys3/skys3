//! Split points of a parallel namespace import (§9.1): where the key space
//! under a remote prefix is cut into ranges that streams list apart.
//!
//! S3 cannot say how many keys a key range holds, so discovery builds a
//! small trie of **names** instead, each standing for the keys that start
//! with it, and cuts it into ranges of about as many names. It explores
//! the trie a level at a time, from the empty name:
//!
//! - A **delimiter listing** of a node (`/`, one page) gives its children
//!   whole: its keys and its common prefixes. A namespace laid out in
//!   folders is cut along them, at a request per node.
//! - A node whose listing is truncated has too many children to list.
//!   They are **sampled** by the next character after the node instead:
//!   the listing's page gives the first children, and probes that each
//!   list one key, after the node followed by each printable ASCII
//!   character past them, find the others, all at once.
//!
//! A level is explored only while the names found are fewer than a few
//! per stream and the request budget covers the whole level, so the names
//! stay evenly spread over the trie. The split points are then every
//! `n / streams`-th name. A key equal to a split point ends its range;
//! keys that start with it begin the next one.

use std::collections::BTreeSet;
use std::sync::Arc;

use skys3_remote::{ListObjectsV2, ListObjectsV2Output, ObjectStore, S3Error, S3ErrorKind};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::IMPORT_PAGE_KEYS;

/// How many names discovery looks for per stream.
const NAMES_PER_STREAM: usize = 4;

/// The fewest listing requests discovery may make, and how many more it
/// may make per stream.
const MIN_REQUESTS: usize = 1024;
const REQUESTS_PER_STREAM: usize = 32;

/// The most levels of the trie discovery explores.
const MAX_LEVELS: usize = 32;

/// The most discovery requests in flight at once.
const CONCURRENCY: usize = 64;

/// The delimiter of a node's listing.
const DELIMITER: &str = "/";

/// The characters a truncated node's children are sampled at: printable
/// ASCII.
const PROBE_CHARS: std::ops::RangeInclusive<u8> = b' '..=b'~';

/// A name of the trie, without the target's prefix.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Name {
    /// A key with nothing under it to explore.
    Leaf(String),
    /// A name that keys start with, explored further if need be.
    Node(String),
}

impl Name {
    fn as_str(&self) -> &str {
        match self {
            Self::Leaf(name) | Self::Node(name) => name,
        }
    }
}

/// The split points of an import of `store` under `prefix` by `streams`
/// streams, after `after`: at most `streams - 1`, strictly increasing,
/// each after `after`. Fewer if the remote holds too few names.
///
/// # Errors
///
/// The first listing request that fails.
pub(super) async fn split_points<S: ObjectStore>(
    store: &Arc<S>,
    prefix: &str,
    after: Option<&str>,
    streams: usize,
) -> Result<Vec<String>, S3Error> {
    if streams < 2 {
        return Ok(Vec::new());
    }
    let want = streams.saturating_mul(NAMES_PER_STREAM);
    let mut budget = MIN_REQUESTS.saturating_add(streams.saturating_mul(REQUESTS_PER_STREAM));
    let mut names = BTreeSet::from([Name::Node(String::new())]);
    for _ in 0..MAX_LEVELS {
        let level: Vec<String> = names
            .iter()
            .filter_map(|name| match name {
                Name::Node(node) => Some(node.clone()),
                Name::Leaf(_) => None,
            })
            .collect();
        if level.is_empty() || names.len() >= want {
            break;
        }
        let Some(children) = expand(store, prefix, &level, &mut budget).await? else {
            break;
        };
        for node in level {
            names.remove(&Name::Node(node));
        }
        names.extend(children);
    }
    let names: Vec<String> = names
        .into_iter()
        .map(|name| name.as_str().to_owned())
        .filter(|name| !name.is_empty() && after.is_none_or(|after| name.as_str() > after))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let groups = streams.min(names.len());
    Ok((1..groups)
        .map(|group| names[group * names.len() / groups].clone())
        .collect())
}

/// The children of every node of `level`: a delimiter listing of each,
/// and for each whose listing is truncated, probes by next character.
/// `None` if `budget` does not cover every request the level needs; the
/// requests are taken from it.
async fn expand<S: ObjectStore>(
    store: &Arc<S>,
    prefix: &str,
    level: &[String],
    budget: &mut usize,
) -> Result<Option<Vec<Name>>, S3Error> {
    let Some(left) = budget.checked_sub(level.len()) else {
        return Ok(None);
    };
    *budget = left;
    let listings = level.iter().map(|node| {
        ListObjectsV2::new(format!("{prefix}{node}"))
            .with_delimiter(DELIMITER)
            .with_max_keys(IMPORT_PAGE_KEYS)
    });
    let pages = list_all(store, listings.collect()).await?;
    let relative = |name: &str| name.strip_prefix(prefix).map(str::to_owned);
    let mut children = Vec::new();
    let mut probes = Vec::new();
    for (node, page) in level.iter().zip(pages) {
        if !page.is_truncated {
            let prefixes = page.common_prefixes.iter().filter_map(|p| relative(p));
            let keys = page.objects.iter().filter_map(|o| relative(&o.key));
            children.extend(prefixes.map(Name::Node).chain(keys.map(Name::Leaf)));
            continue;
        }
        let listed: BTreeSet<String> = page
            .common_prefixes
            .iter()
            .map(String::as_str)
            .chain(page.objects.iter().map(|object| object.key.as_str()))
            .filter_map(|name| child(node, name.strip_prefix(prefix)?))
            .collect();
        let Some(last) = listed.last().and_then(|last| last.chars().last()) else {
            children.push(Name::Leaf(node.clone()));
            continue;
        };
        // The first key after every key under the last child listed, and
        // the first key after each printable character past it.
        let mut starts = vec![format!("{node}{last}{}", char::MAX)];
        starts.extend(
            PROBE_CHARS
                .map(char::from)
                .filter(|c| *c > last)
                .map(|c| format!("{node}{c}")),
        );
        for start in starts {
            probes.push((
                node.clone(),
                ListObjectsV2::new(format!("{prefix}{node}"))
                    .with_start_after(format!("{prefix}{start}"))
                    .with_max_keys(1),
            ));
        }
        children.extend(listed.into_iter().map(Name::Node));
    }
    let Some(left) = budget.checked_sub(probes.len()) else {
        return Ok(None);
    };
    *budget = left;
    let (nodes, requests): (Vec<String>, Vec<ListObjectsV2>) = probes.into_iter().unzip();
    let found = list_all(store, requests).await?;
    for (node, page) in nodes.iter().zip(found) {
        let first = page.objects.first().and_then(|o| relative(&o.key));
        if let Some(name) = first.and_then(|key| child(node, &key)) {
            children.push(Name::Node(name));
        }
    }
    Ok(Some(children))
}

/// The child of `node` that `name`, a name under it, is under: the node
/// followed by the next character of `name`, if it has one.
fn child(node: &str, name: &str) -> Option<String> {
    let next = name.strip_prefix(node)?.chars().next()?;
    Some(format!("{node}{next}"))
}

/// Sends `requests`, at most [`CONCURRENCY`] at a time, and returns their
/// answers in order.
async fn list_all<S: ObjectStore>(
    store: &Arc<S>,
    requests: Vec<ListObjectsV2>,
) -> Result<Vec<ListObjectsV2Output>, S3Error> {
    let permits = Arc::new(Semaphore::new(CONCURRENCY));
    let mut tasks = JoinSet::new();
    for (i, request) in requests.into_iter().enumerate() {
        let (store, permits) = (Arc::clone(store), Arc::clone(&permits));
        tasks.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (i, store.list_objects_v2(request).await)
        });
    }
    let mut answers = Vec::with_capacity(tasks.len());
    while let Some(done) = tasks.join_next().await {
        match done {
            Ok((i, answer)) => answers.push((i, answer?)),
            Err(error) => match error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(error) => {
                    return Err(S3Error::new(S3ErrorKind::InternalError, error.to_string()));
                }
            },
        }
    }
    answers.sort_by_key(|(i, _)| *i);
    Ok(answers.into_iter().map(|(_, answer)| answer).collect())
}

#[cfg(test)]
mod tests {
    use skys3_remote::PutObject;
    use skys3_sim::SimS3;
    use skys3_sim::s3::{SimS3Config, SimS3Faults};

    use super::*;

    const PREFIX: &str = "team/";

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
    }

    /// A remote holding `keys` under the prefix, and one key outside it.
    async fn remote(keys: &[String]) -> Arc<SimS3> {
        let store = SimS3::new(5, SimS3Config::default());
        for key in keys
            .iter()
            .map(|key| format!("{PREFIX}{key}"))
            .chain(["other".into()])
        {
            store.put_object(PutObject::new(key, "x")).await.unwrap();
        }
        Arc::new(store)
    }

    /// How many of `keys` each range that `splits` makes holds.
    fn sizes(keys: &[String], splits: &[String]) -> Vec<usize> {
        let mut sizes = vec![0; splits.len() + 1];
        for key in keys {
            sizes[splits.partition_point(|split| split < key)] += 1;
        }
        sizes
    }

    /// Checks that `splits` increase and cut `keys` into `ranges` ranges
    /// of between half and twice the average size.
    fn assert_balanced(keys: &[String], splits: &[String], ranges: usize) {
        assert!(splits.is_sorted_by(|a, b| a < b), "{splits:?}");
        let sizes = sizes(keys, splits);
        assert_eq!(sizes.len(), ranges, "{splits:?}");
        let average = keys.len() / ranges;
        assert!(
            sizes
                .iter()
                .all(|&size| size >= average / 2 && size <= average * 2),
            "{sizes:?} from {splits:?}"
        );
    }

    #[test]
    fn folders_are_split_by_delimiter_listings() {
        runtime().block_on(async {
            let mut keys: Vec<String> = (0..12)
                .flat_map(|dir| (0..20).map(move |n| format!("d{dir:02}/k{n:02}")))
                .collect();
            keys.extend(["a".to_owned(), "z".to_owned()]);
            keys.sort();
            let store = remote(&keys).await;
            let splits = split_points(&store, PREFIX, None, 4).await.unwrap();
            assert_balanced(&keys, &splits, 4);
            // The root and the twelve folders: one listing each.
            let puts = keys.len() as u64 + 1;
            assert_eq!(store.stats().requests, puts + 1 + 12);

            // Fourteen names, the folders among them, are enough for three
            // streams.
            let splits = split_points(&store, PREFIX, None, 3).await.unwrap();
            assert_eq!(splits, ["d03/", "d08/"]);
        });
    }

    #[test]
    fn flat_keys_are_split_by_sampled_characters() {
        runtime().block_on(async {
            let keys: Vec<String> = (0..4500).map(|n| format!("k{n:04}")).collect();
            let store = remote(&keys).await;
            let splits = split_points(&store, PREFIX, None, 8).await.unwrap();
            assert_balanced(&keys, &splits, 8);
            // Only the names after a checkpoint split.
            let after = split_points(&store, PREFIX, Some("k3"), 8).await.unwrap();
            assert!(after.iter().all(|split| split.as_str() > "k3"), "{after:?}");
            assert_balanced(&keys[3000..], &after, after.len() + 1);
        });
    }

    #[test]
    fn a_skewed_namespace_splits_its_large_part() {
        runtime().block_on(async {
            let mut keys: Vec<String> = (0..3000).map(|n| format!("big/{n:04}")).collect();
            keys.extend(["small/1".to_owned(), "small/2".to_owned()]);
            keys.sort();
            let store = remote(&keys).await;
            let splits = split_points(&store, PREFIX, None, 6).await.unwrap();
            assert_balanced(&keys, &splits, 6);
        });
    }

    #[test]
    fn few_keys_make_few_ranges() {
        runtime().block_on(async {
            let store = remote(&[]).await;
            assert!(
                split_points(&store, PREFIX, None, 8)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let keys = ["a".to_owned(), "b".to_owned(), "c".to_owned()];
            let store = remote(&keys).await;
            assert_eq!(
                split_points(&store, PREFIX, None, 8).await.unwrap(),
                ["b", "c"]
            );
            assert!(
                split_points(&store, PREFIX, None, 1)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                split_points(&store, PREFIX, Some("c"), 8)
                    .await
                    .unwrap()
                    .is_empty()
            );

            store.set_faults(SimS3Faults::OUTAGE);
            assert!(split_points(&store, PREFIX, None, 8).await.is_err());
        });
    }
}
