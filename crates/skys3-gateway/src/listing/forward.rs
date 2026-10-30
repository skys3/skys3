//! Listings of a `read_only` bucket, forwarded to its origin (§9.5).
//!
//! The bucket's shards hold only the versions reads cached, so the origin
//! alone knows which keys exist: a page asks it for the items after the
//! page's starting point, with the listing's prefix and delimiter, and
//! follows its continuation tokens until the page is full or the origin
//! has no more. The origin's tokens never reach the client, which resumes
//! from the gateway's own token or marker; S3 leaves out a common prefix
//! at or before `StartAfter`, so a listing resumed after a common prefix
//! skips every key under it, as for any bucket (§9.4). Items at or before
//! the starting point are dropped all the same, in case an origin returns
//! them.

use skys3_index::{ListItem, ListQuery};
use skys3_types::BucketDocument;

use super::merge::MergedPage;
use crate::remote::{RemoteError, RemoteListing, RemoteReads};

/// The most origin pages one page of a listing reads: an origin that keeps
/// answering empty pages with a token is given up on.
const MAX_ORIGIN_PAGES: usize = 64;

/// One page of `bucket`'s listing as `query` describes it, from its origin.
///
/// # Errors
///
/// The origin's, or [`MAX_ORIGIN_PAGES`] pages without filling the page.
pub(super) async fn list(
    origin: &dyn RemoteReads,
    bucket: &BucketDocument,
    query: &ListQuery,
) -> Result<MergedPage, RemoteError> {
    let max = query.max_items;
    let mut items: Vec<ListItem> = Vec::new();
    if max == 0 {
        return Ok(MergedPage::default());
    }
    let after = query.start_after.as_deref();
    let mut token = None;
    for _ in 0..MAX_ORIGIN_PAGES {
        let listing = RemoteListing {
            prefix: query.prefix.clone(),
            delimiter: query.delimiter.clone().filter(|d| !d.is_empty()),
            start_after: query.start_after.clone(),
            token: token.take(),
            max_items: max - items.len(),
        };
        let page = origin.list(&bucket.bucket_id, listing).await?;
        let last = items.last().map(|item| item.name().to_owned());
        items.extend(page.items.into_iter().filter(|item| {
            after.is_none_or(|after| item.name() > after)
                && last.as_deref().is_none_or(|last| item.name() > last)
        }));
        if items.len() >= max || page.next.is_none() {
            let truncated = items.len() > max || page.next.is_some();
            items.truncate(max);
            return Ok(MergedPage { items, truncated });
        }
        token = page.next;
    }
    Err(RemoteError(format!(
        "the origin answered {MAX_ORIGIN_PAGES} pages without filling one of {max} items"
    )))
}
