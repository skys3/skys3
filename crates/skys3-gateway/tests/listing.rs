//! ListObjectsV2 and ListObjects (V1) through the gateway's pipeline, over
//! real shards on a simulated disk: pages across shards, delimiters,
//! continuation tokens and markers, URL encoding, owners, and errors.

mod common;

use common::{Answer, Setup, config, setup, setup_with};
use http::Method;
use skys3_gateway::ListTokenKeys;

/// Every text of `<tag>` in `body`, in order.
fn texts(body: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut found = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        rest = &rest[start + open.len()..];
        let end = rest.find(&close).unwrap();
        found.push(rest[..end].to_owned());
        rest = &rest[end..];
    }
    found
}

/// The one text of `<tag>` in `body`, if any.
fn text(body: &str, tag: &str) -> Option<String> {
    let mut all = texts(body, tag);
    assert!(all.len() <= 1, "{tag} in {body}");
    all.pop()
}

/// The keys of the `<Contents>` of a listing.
fn keys(answer: &Answer) -> Vec<String> {
    texts(&answer.body, "Contents")
        .iter()
        .filter_map(|contents| text(contents, "Key"))
        .collect()
}

/// The common prefixes of a listing.
fn prefixes(answer: &Answer) -> Vec<String> {
    texts(&answer.body, "CommonPrefixes")
        .iter()
        .filter_map(|prefix| text(prefix, "Prefix"))
        .collect()
}

/// A query parameter's value, percent-encoded.
fn escape(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

impl Setup {
    async fn put_key(&self, key: &str) {
        let uri = format!("/photos/{}", key.replace(' ', "%20").replace('+', "%2B"));
        self.call(Method::PUT, &uri, &[], key)
            .await
            .assert(200, None);
    }

    async fn list(&self, query: &str) -> Answer {
        self.call(Method::GET, &format!("/photos?{query}"), &[], "")
            .await
    }
}

async fn photos(keys: &[&str]) -> Setup {
    let setup = setup("").await;
    setup.create_local("photos").await;
    for key in keys {
        setup.put_key(key).await;
    }
    setup
}

#[tokio::test]
async fn list_objects_v2_pages_with_continuation_tokens() {
    let setup = photos(&["a/1", "a/2", "b", "c/x/1", "d"]).await;
    // A deleted key is not listed.
    setup
        .call(Method::DELETE, "/photos/d", &[], "")
        .await
        .assert(204, None);

    let all = setup.list("list-type=2").await;
    all.assert(200, None);
    assert_eq!(keys(&all), ["a/1", "a/2", "b", "c/x/1"]);
    assert_eq!(text(&all.body, "Name").as_deref(), Some("photos"));
    assert_eq!(text(&all.body, "KeyCount").as_deref(), Some("4"));
    assert_eq!(text(&all.body, "MaxKeys").as_deref(), Some("1000"));
    assert_eq!(text(&all.body, "IsTruncated").as_deref(), Some("false"));
    assert_eq!(text(&all.body, "NextContinuationToken"), None);
    let first = &texts(&all.body, "Contents")[0];
    assert_eq!(text(first, "Size").as_deref(), Some("3"));
    assert_eq!(text(first, "StorageClass").as_deref(), Some("STANDARD"));
    assert!(text(first, "ETag").is_some(), "{first}");
    assert!(text(first, "LastModified").is_some(), "{first}");
    assert_eq!(text(first, "Owner"), None);

    // Two items a page, with a delimiter.
    let page = setup.list("list-type=2&delimiter=/&max-keys=2").await;
    page.assert(200, None);
    assert_eq!(prefixes(&page), ["a/"]);
    assert_eq!(keys(&page), ["b"]);
    assert_eq!(text(&page.body, "KeyCount").as_deref(), Some("2"));
    assert_eq!(text(&page.body, "Delimiter").as_deref(), Some("/"));
    assert_eq!(text(&page.body, "IsTruncated").as_deref(), Some("true"));
    let token = text(&page.body, "NextContinuationToken").unwrap();
    let query = format!(
        "list-type=2&delimiter=/&max-keys=2&continuation-token={}",
        escape(&token)
    );
    let next = setup.list(&query).await;
    next.assert(200, None);
    assert_eq!(prefixes(&next), ["c/"]);
    assert!(keys(&next).is_empty());
    assert_eq!(text(&next.body, "ContinuationToken"), Some(token.clone()));
    assert_eq!(text(&next.body, "IsTruncated").as_deref(), Some("false"));

    // The token takes precedence over start-after.
    let resumed = setup.list(&format!("{query}&start-after=0")).await;
    assert_eq!(prefixes(&resumed), ["c/"]);
    assert_eq!(text(&resumed.body, "StartAfter").as_deref(), Some("0"));
    let after = setup.list("list-type=2&delimiter=/&start-after=a/1").await;
    assert_eq!(
        (prefixes(&after), keys(&after)),
        (vec!["c/".to_owned()], vec!["b".to_owned()])
    );

    // Owners only when asked for.
    let owned = setup.list("list-type=2&fetch-owner=true&prefix=b").await;
    assert_eq!(keys(&owned), ["b"]);
    assert_eq!(text(&owned.body, "Prefix").as_deref(), Some("b"));
    let owner = text(&owned.body, "Owner").unwrap();
    assert_eq!(text(&owner, "ID").as_deref(), Some("test"));
}

#[tokio::test]
async fn tampered_and_foreign_tokens_are_refused() {
    let setup = photos(&["a", "b", "c"]).await;
    let page = setup.list("list-type=2&max-keys=1").await;
    let token = text(&page.body, "NextContinuationToken").unwrap();
    let valid = setup
        .list(&format!(
            "list-type=2&max-keys=1&continuation-token={}",
            escape(&token)
        ))
        .await;
    assert_eq!(keys(&valid), ["b"]);

    let mut tampered = token.clone().into_bytes();
    tampered[3] = if tampered[3] == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).unwrap();
    for query in [
        format!("list-type=2&continuation-token={}", escape(&tampered)),
        // A token is bound to its listing's prefix and delimiter.
        format!("list-type=2&prefix=b&continuation-token={}", escape(&token)),
        format!(
            "list-type=2&delimiter=/&continuation-token={}",
            escape(&token)
        ),
        "list-type=2&continuation-token=bm90LWEtdG9rZW4".to_owned(),
        "list-type=2&continuation-token=%21".to_owned(),
    ] {
        let answer = setup.list(&query).await;
        answer.assert(400, Some("InvalidArgument"));
        assert!(
            answer
                .body
                .contains("The continuation token provided is incorrect"),
            "{answer:?}"
        );
    }

    // Another gateway, with other keys, refuses the token too.
    let mut other = config("");
    other.list_token_keys = ListTokenKeys::new(&[7; 32], &[]).unwrap();
    let other = setup_with(other).await;
    other.create_local("photos").await;
    let answer = other
        .list(&format!(
            "list-type=2&continuation-token={}",
            escape(&token)
        ))
        .await;
    answer.assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn list_objects_v1_pages_with_markers() {
    let setup = photos(&["a/1", "a/2", "b", "c/x/1"]).await;
    let page = setup.list("delimiter=/&max-keys=2").await;
    page.assert(200, None);
    assert_eq!(
        (prefixes(&page), keys(&page)),
        (vec!["a/".to_owned()], vec!["b".to_owned()])
    );
    assert_eq!(text(&page.body, "Marker").as_deref(), Some(""));
    assert_eq!(text(&page.body, "NextMarker").as_deref(), Some("b"));
    assert_eq!(text(&page.body, "IsTruncated").as_deref(), Some("true"));
    // V1 always names owners.
    let owner = text(&texts(&page.body, "Contents")[0], "Owner").unwrap();
    assert_eq!(text(&owner, "ID").as_deref(), Some("test"));

    let next = setup.list("delimiter=/&max-keys=2&marker=b").await;
    assert_eq!(prefixes(&next), ["c/"]);
    assert_eq!(text(&next.body, "Marker").as_deref(), Some("b"));
    assert_eq!(text(&next.body, "NextMarker"), None);
    assert_eq!(text(&next.body, "IsTruncated").as_deref(), Some("false"));

    // Resuming after a common prefix skips its keys.
    let after = setup.list("delimiter=/&marker=a/").await;
    assert_eq!(keys(&after), ["b"]);

    // Without a delimiter there is no NextMarker; clients resume after the
    // last key, and any string is a marker.
    let page = setup.list("max-keys=3").await;
    assert_eq!(keys(&page), ["a/1", "a/2", "b"]);
    assert_eq!(text(&page.body, "IsTruncated").as_deref(), Some("true"));
    assert_eq!(text(&page.body, "NextMarker"), None);
    let rest = setup.list("marker=a/1x").await;
    assert_eq!(keys(&rest), ["a/2", "b", "c/x/1"]);
}

#[tokio::test]
async fn listings_url_encode_on_request() {
    let setup = photos(&["dir a/b+c é", "dir a/plain"]).await;
    let encoded = setup
        .list("list-type=2&encoding-type=url&prefix=dir%20a/&start-after=dir%20a/a")
        .await;
    encoded.assert(200, None);
    assert_eq!(keys(&encoded), ["dir+a/b%2Bc+%C3%A9", "dir+a/plain"]);
    assert_eq!(text(&encoded.body, "Prefix").as_deref(), Some("dir+a/"));
    assert_eq!(
        text(&encoded.body, "StartAfter").as_deref(),
        Some("dir+a/a")
    );
    assert_eq!(text(&encoded.body, "EncodingType").as_deref(), Some("url"));

    let rolled = setup.list("encoding-type=url&delimiter=%20").await;
    assert_eq!(prefixes(&rolled), ["dir+"]);
    assert_eq!(text(&rolled.body, "Delimiter").as_deref(), Some("+"));
    assert_eq!(text(&rolled.body, "Marker").as_deref(), Some(""));

    let plain = setup.list("list-type=2&delimiter=%20").await;
    assert_eq!(prefixes(&plain), ["dir "]);

    setup
        .list("list-type=2&encoding-type=base64")
        .await
        .assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn listing_requests_are_checked() {
    let setup = photos(&["a"]).await;
    setup
        .list("list-type=2&max-keys=-1")
        .await
        .assert(400, Some("InvalidArgument"));
    let none = setup.list("list-type=2&max-keys=0").await;
    none.assert(200, None);
    assert!(keys(&none).is_empty());
    assert_eq!(text(&none.body, "IsTruncated").as_deref(), Some("false"));
    let capped = setup.list("max-keys=5000").await;
    assert_eq!(text(&capped.body, "MaxKeys").as_deref(), Some("1000"));
    // An empty delimiter rolls up nothing.
    let flat = setup.list("list-type=2&delimiter=").await;
    assert_eq!(keys(&flat), ["a"]);

    setup
        .call(Method::GET, "/missing?list-type=2", &[], "")
        .await
        .assert(404, Some("NoSuchBucket"));
    setup.shards.set_unavailable(true);
    setup
        .list("list-type=2")
        .await
        .assert(503, Some("ServiceUnavailable"));
}
