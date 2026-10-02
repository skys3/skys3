//! Reads and listings of a `write_back` bucket while its namespace import
//! runs, and lazily loaded metadata of imported stubs (design §9.1),
//! against a scripted remote.

mod common;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use common::{Setup, config, setup_with};
use http::Method;
use skys3_gateway::{
    Precondition, RemoteError, RemoteFuture, RemoteListing, RemoteObject, RemotePage, RemoteReads,
    ShardRef, Shards,
};
use skys3_index::{EntryState, ImportCheckpoint, ListItem, ListQuery, ObjectVersion, Payload};
use skys3_log::RecordBody;
use skys3_log::record::Import;
use skys3_types::{BucketDocument, BucketId, ETag};

/// A remote whose objects, import progress, and failures a test sets.
#[derive(Debug, Default)]
struct Remote(Mutex<State>);

#[derive(Debug, Default)]
struct State {
    import: Option<ImportCheckpoint>,
    /// Keys after the first and up to the second that a range listed in
    /// parallel has passed ahead of the import's position.
    ahead: Option<(String, String)>,
    objects: BTreeMap<String, (RemoteObject, Bytes)>,
    failing: bool,
    heads: usize,
}

impl Remote {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }

    fn set_import(&self, import: Option<ImportCheckpoint>) {
        self.lock().import = import;
    }

    fn running(&self, after: Option<&str>) {
        self.set_import(Some(ImportCheckpoint::Running {
            after: after.map(str::to_owned),
        }));
    }

    /// Stores `body` at `key` with a `Content-Type` and a user metadata
    /// value, and returns its ETag.
    fn put(&self, key: &str, body: &str) -> ETag {
        let etag = ETag::new(format!("{:032x}", body.len() * 7919 + key.len())).unwrap();
        let object = ObjectVersion {
            size: body.len() as u64,
            last_modified_ms: 1_700_000_000_000,
            local_etag: etag.clone(),
            write_identity: None,
            metadata: BTreeMap::from([
                ("content-type".to_owned(), "text/plain".to_owned()),
                ("x-amz-meta-color".to_owned(), "red".to_owned()),
            ]),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            storage_class: None,
            copy_source: None,
            payload: Payload::None,
        };
        let remote = RemoteObject {
            object,
            version_id: Some("v1".to_owned()),
        };
        let bytes = Bytes::copy_from_slice(body.as_bytes());
        self.lock().objects.insert(key.to_owned(), (remote, bytes));
        etag
    }

    fn check(&self) -> Result<MutexGuard<'_, State>, RemoteError> {
        let state = self.lock();
        if state.failing {
            return Err(RemoteError("the remote is down".to_owned()));
        }
        Ok(state)
    }
}

impl RemoteReads for Remote {
    fn import(&self, _bucket: &BucketId) -> Option<ImportCheckpoint> {
        self.lock().import.clone()
    }

    fn passed(&self, _bucket: &BucketId, key: &str) -> bool {
        let state = self.lock();
        let ahead = state.ahead.as_ref();
        state
            .import
            .as_ref()
            .is_none_or(|import| import.passed(key))
            || ahead.is_some_and(|(start, end)| key > start.as_str() && key <= end.as_str())
    }

    fn head<'a>(
        &'a self,
        _bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, Option<RemoteObject>> {
        Box::pin(async move {
            let mut state = self.check()?;
            state.heads += 1;
            Ok(state.objects.get(key).map(|(object, _)| object.clone()))
        })
    }

    fn get<'a>(
        &'a self,
        _bucket: &'a BucketId,
        key: &'a str,
        etag: &'a ETag,
        range: Range<u64>,
    ) -> RemoteFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let state = self.check()?;
            Ok(state
                .objects
                .get(key)
                .filter(|(object, _)| &object.object.local_etag == etag)
                .map(|(_, body)| body.slice(range.start as usize..range.end as usize)))
        })
    }

    fn list<'a>(
        &'a self,
        _bucket: &'a BucketId,
        listing: RemoteListing,
    ) -> RemoteFuture<'a, RemotePage> {
        Box::pin(async move {
            let state = self.check()?;
            let query = ListQuery {
                prefix: listing.prefix.clone(),
                delimiter: listing.delimiter.clone(),
                ..ListQuery::default()
            };
            // A token is the last item of the page before, and a start is
            // a key. As S3 does, an item, key or common prefix, lists only
            // if it sorts after either: a common prefix at or before a
            // start inside it is left out.
            let after = listing.token.or(listing.start_after);
            let mut items: Vec<ListItem> = Vec::new();
            for (key, (object, _)) in &state.objects {
                if !key.starts_with(&query.prefix) {
                    continue;
                }
                let item = match query.common_prefix(key) {
                    Some(prefix) => ListItem::Prefix(prefix.to_owned()),
                    None => ListItem::Object {
                        key: key.clone(),
                        object: Box::new(object.object.clone()),
                    },
                };
                if after.as_deref().is_some_and(|after| item.name() <= after) {
                    continue;
                }
                if items.last().is_none_or(|last| last.name() != item.name()) {
                    items.push(item);
                }
            }
            let more = items.len() > listing.max_items;
            items.truncate(listing.max_items);
            let next = more.then(|| items.last().map(|item| item.name().to_owned()));
            Ok(RemotePage {
                items,
                next: next.flatten(),
            })
        })
    }
}

async fn setup(remote: &Arc<Remote>) -> (Setup, BucketDocument) {
    let mut config = config("[buckets.defaults]\nshards_per_bucket = 4");
    config.remote = Some(Arc::clone(remote) as Arc<dyn RemoteReads>);
    let setup = setup_with(config).await;
    let bucket = setup.create_write_back("photos").await;
    (setup, bucket)
}

/// Commits the `IMPORT` of `key`, as the import does.
async fn import(setup: &Setup, bucket: &BucketDocument, key: &str, etag: &ETag) {
    let import = Import {
        key: key.to_owned(),
        size: 5,
        last_modified_ms: 1_600_000_000_000,
        etag: etag.clone(),
        storage_class: Some("STANDARD".to_owned()),
    };
    let shard = ShardRef::for_key(bucket, key);
    setup
        .shards
        .write(&shard, RecordBody::Import(import), Precondition::None)
        .await
        .unwrap()
        .unwrap();
}

async fn entry(setup: &Setup, bucket: &BucketDocument, key: &str) -> Option<skys3_index::Entry> {
    let shard = ShardRef::for_key(bucket, key);
    setup.shards.entry(&shard, key).await.unwrap()
}

/// The keys and common prefixes of a listing's answer, in order; common
/// prefixes end with their delimiter.
fn listed(body: &str) -> Vec<String> {
    let mut names = Vec::new();
    for tag in ["<Key>", "<Prefix>"] {
        let close = tag.replace('<', "</");
        let mut rest = body;
        while let Some(start) = rest.find(tag) {
            let from = start + tag.len();
            let end = rest[from..].find(&close).unwrap() + from;
            // The echoed request prefix is empty here.
            if !rest[from..end].is_empty() {
                names.push(rest[from..end].to_owned());
            }
            rest = &rest[end..];
        }
    }
    names.sort();
    names
}

#[tokio::test]
async fn a_miss_falls_through_to_the_remote_until_the_import_passes_the_key() {
    let remote = Arc::new(Remote::default());
    let (setup, _) = setup(&remote).await;
    remote.running(None);
    let etag = remote.put("a.txt", "hello");
    remote.put("b.txt", "world");

    let head = setup.call(Method::HEAD, "/photos/a.txt", &[], "").await;
    head.assert(200, None);
    assert_eq!(head.headers["content-type"], "text/plain");
    assert_eq!(head.headers["x-amz-meta-color"], "red");
    assert_eq!(head.headers["etag"], format!("\"{etag}\""));
    let get = setup.call(Method::GET, "/photos/a.txt", &[], "").await;
    get.assert(200, None);
    assert_eq!(get.body, "hello");
    let range = [("range", "bytes=1-2")];
    let part = setup.call(Method::GET, "/photos/a.txt", &range, "").await;
    part.assert(206, None);
    assert_eq!(part.body, "el");
    let missing = setup.call(Method::GET, "/photos/none", &[], "").await;
    missing.assert(404, Some("NoSuchKey"));

    // A local delete of a key the import has not reached leaves a
    // tombstone, which hides the remote object.
    setup
        .call(Method::DELETE, "/photos/a.txt", &[], "")
        .await
        .assert(204, None);
    let deleted = setup.call(Method::HEAD, "/photos/a.txt", &[], "").await;
    assert_eq!(deleted.status, 404);

    let failing = {
        remote.lock().failing = true;
        setup.call(Method::HEAD, "/photos/b.txt", &[], "").await
    };
    failing.assert(503, Some("ServiceUnavailable"));
    remote.lock().failing = false;

    // Once the import has passed a key, or is done, a miss is a 404.
    remote.running(Some("c"));
    let passed = setup.call(Method::HEAD, "/photos/b.txt", &[], "").await;
    assert_eq!(passed.status, 404);
    remote.set_import(Some(ImportCheckpoint::Done));
    let done = setup.call(Method::GET, "/photos/b.txt", &[], "").await;
    done.assert(404, Some("NoSuchKey"));
    remote.set_import(None);
    let unknown = setup.call(Method::GET, "/photos/b.txt", &[], "").await;
    unknown.assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn a_get_fails_if_the_object_changes_after_its_head() {
    #[derive(Debug)]
    struct Changing(Remote);
    impl RemoteReads for Changing {
        fn import(&self, bucket: &BucketId) -> Option<ImportCheckpoint> {
            self.0.import(bucket)
        }
        fn head<'a>(
            &'a self,
            b: &'a BucketId,
            k: &'a str,
        ) -> RemoteFuture<'a, Option<RemoteObject>> {
            self.0.head(b, k)
        }
        fn get<'a>(
            &'a self,
            _: &'a BucketId,
            _: &'a str,
            _: &'a ETag,
            _: Range<u64>,
        ) -> RemoteFuture<'a, Option<Bytes>> {
            Box::pin(async { Ok(None) })
        }
        fn list<'a>(&'a self, b: &'a BucketId, l: RemoteListing) -> RemoteFuture<'a, RemotePage> {
            self.0.list(b, l)
        }
    }
    let remote = Changing(Remote::default());
    remote.0.running(None);
    remote.0.put("a.txt", "hello");
    let mut config = config("");
    config.remote = Some(Arc::new(remote));
    let setup = setup_with(config).await;
    setup.create_write_back("photos").await;
    let get = setup.call(Method::GET, "/photos/a.txt", &[], "").await;
    get.assert(503, Some("ServiceUnavailable"));
}

#[tokio::test]
async fn the_first_read_of_an_imported_stub_loads_its_metadata() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    remote.set_import(Some(ImportCheckpoint::Done));
    let etag = remote.put("c.txt", "hello");
    import(&setup, &bucket, "c.txt", &etag).await;
    let stub = entry(&setup, &bucket, "c.txt").await.unwrap();
    assert!(stub.object.as_ref().unwrap().metadata.is_empty());

    let head = setup.call(Method::HEAD, "/photos/c.txt", &[], "").await;
    head.assert(200, None);
    assert_eq!(head.headers["content-type"], "text/plain");
    assert_eq!(head.headers["x-amz-meta-color"], "red");
    // The listing's `Last-Modified` stays.
    assert_eq!(
        head.headers["last-modified"],
        "Sun, 13 Sep 2020 12:26:40 GMT"
    );
    let loaded = entry(&setup, &bucket, "c.txt").await.unwrap();
    assert!(loaded.version > stub.version, "an ADOPT of the stub");
    assert_eq!(loaded.state, EntryState::Evicted);
    assert_eq!(loaded.remote_version_id.as_deref(), Some("v1"));
    let object = loaded.object.unwrap();
    assert_eq!(object.metadata["content-type"], "text/plain");
    assert_eq!(object.storage_class, None);
    // Loaded once: later reads are local.
    remote.lock().failing = true;
    let again = setup.call(Method::HEAD, "/photos/c.txt", &[], "").await;
    again.assert(200, None);
    assert_eq!(remote.lock().heads, 1);

    // A stub of a remote object that changed out of band adopts it.
    remote.lock().failing = false;
    let old = ETag::new("0123456789abcdef0123456789abcdef").unwrap();
    import(&setup, &bucket, "d.txt", &old).await;
    let current = remote.put("d.txt", "newer bytes");
    let head = setup.call(Method::HEAD, "/photos/d.txt", &[], "").await;
    assert_eq!(head.headers["etag"], format!("\"{current}\""));
    assert_eq!(
        head.headers["last-modified"],
        "Tue, 14 Nov 2023 22:13:20 GMT"
    );

    // A stub whose remote object is gone stays as it is.
    import(&setup, &bucket, "e.txt", &old).await;
    let head = setup.call(Method::HEAD, "/photos/e.txt", &[], "").await;
    head.assert(200, None);
    assert_eq!(head.headers["etag"], format!("\"{old}\""));
    assert_eq!(head.headers["content-type"], "binary/octet-stream");

    // A remote that cannot answer fails the read.
    import(&setup, &bucket, "f.txt", &old).await;
    remote.lock().failing = true;
    let head = setup.call(Method::HEAD, "/photos/f.txt", &[], "").await;
    head.assert(503, Some("ServiceUnavailable"));
    let get = setup.call(Method::GET, "/photos/f.txt", &[], "").await;
    get.assert(503, Some("ServiceUnavailable"));

    // A local write replaces a stub before its metadata loads; reading it
    // needs no remote.
    setup
        .call(Method::PUT, "/photos/f.txt", &[], "mine")
        .await
        .assert(200, None);
    let get = setup.call(Method::GET, "/photos/f.txt", &[], "").await;
    get.assert(200, None);
    assert_eq!(get.body, "mine");
}

#[tokio::test]
async fn listings_merge_the_remote_while_the_import_runs() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    remote.running(Some("b"));
    // Local: two writes, a stub the import committed, and a tombstone of a
    // key the import has not reached.
    for key in ["a/1", "c"] {
        setup
            .call(Method::PUT, &format!("/photos/{key}"), &[], "local")
            .await
            .assert(200, None);
    }
    let etag = remote.put("b", "remote");
    import(&setup, &bucket, "b", &etag).await;
    for key in ["a/2", "c", "d", "e/1", "x/1", "y/1", "y/2"] {
        remote.put(key, "remote");
    }
    for key in ["x/1", "y/1"] {
        setup
            .call(Method::DELETE, &format!("/photos/{key}"), &[], "")
            .await
            .assert(204, None);
    }

    let all = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    all.assert(200, None);
    // `a/2` is before the import's position, so the index has it or it is
    // gone; `x/1` and `y/1` are deleted locally.
    assert_eq!(listed(&all.body), ["a/1", "b", "c", "d", "e/1", "y/2"]);
    let rolled = setup
        .call(Method::GET, "/photos?list-type=2&delimiter=/", &[], "")
        .await;
    assert_eq!(listed(&rolled.body), ["a/", "b", "c", "d", "e/", "y/"]);

    // Page by page, each page continuing the last.
    let mut names = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut uri = "/photos?list-type=2&delimiter=/&max-keys=2".to_owned();
        if let Some(token) = &token {
            uri.push_str("&continuation-token=");
            uri.push_str(&token.replace('=', "%3D"));
        }
        let page = setup.call(Method::GET, &uri, &[], "").await;
        page.assert(200, None);
        names.extend(listed(&page.body));
        token = page.body.find("<NextContinuationToken>").map(|start| {
            let from = start + "<NextContinuationToken>".len();
            let end = page.body[from..].find('<').unwrap() + from;
            page.body[from..end].to_owned()
        });
        if token.is_none() {
            break;
        }
    }
    assert_eq!(names, ["a/", "b", "c", "d", "e/", "y/"]);

    // Once the import is done, the index alone lists.
    remote.set_import(Some(ImportCheckpoint::Done));
    let done = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    assert_eq!(listed(&done.body), ["a/1", "b", "c"]);

    remote.running(None);
    remote.lock().failing = true;
    let failing = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    failing.assert(503, Some("ServiceUnavailable"));
}

#[tokio::test]
async fn a_common_prefix_the_import_is_inside_still_lists() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    // The import has passed `a/x`, which was then deleted; `a/y` is only
    // on the remote. The remote's listing starts after `a/x`, so it leaves
    // the common prefix `a/` out.
    remote.running(Some("a/x"));
    let etag = remote.put("a/x", "remote");
    import(&setup, &bucket, "a/x", &etag).await;
    remote.put("a/y", "remote");
    remote.put("b/1", "remote");
    setup
        .call(Method::DELETE, "/photos/a/x", &[], "")
        .await
        .assert(204, None);

    let rolled = setup
        .call(Method::GET, "/photos?list-type=2&delimiter=/", &[], "")
        .await;
    rolled.assert(200, None);
    assert_eq!(listed(&rolled.body), ["a/", "b/"]);
    let all = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    assert_eq!(listed(&all.body), ["a/y", "b/1"]);
    // A listing that starts past the prefix, or inside it, leaves it out.
    for start in ["a/", "a/z"] {
        let uri = format!("/photos?list-type=2&delimiter=/&start-after={start}");
        let page = setup.call(Method::GET, &uri, &[], "").await;
        assert_eq!(listed(&page.body), ["b/"], "after {start}");
    }
    // A prefix with nothing left to import under it does not list.
    remote.lock().objects.remove("a/y");
    let rolled = setup
        .call(Method::GET, "/photos?list-type=2&delimiter=/", &[], "")
        .await;
    assert_eq!(listed(&rolled.body), ["b/"]);
}

#[tokio::test]
async fn a_stopped_shard_never_falls_through_to_the_remote() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    remote.running(None);
    remote.put("a.txt", "hello");
    // The key's shard stops: it refuses reads, so a miss cannot be told
    // from an entry it would have held.
    let shard = ShardRef::for_key(&bucket, "a.txt");
    let local = setup.shards.local().set().get(&(&shard).into()).await;
    local.unwrap().close().await.unwrap();

    let head = setup.call(Method::HEAD, "/photos/a.txt", &[], "").await;
    assert_eq!(head.status, 503);
    let get = setup.call(Method::GET, "/photos/a.txt", &[], "").await;
    get.assert(503, Some("ServiceUnavailable"));
    let list = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    list.assert(503, Some("ServiceUnavailable"));
    assert_eq!(remote.lock().heads, 0, "nothing was read from the remote");
}

#[tokio::test]
async fn keys_a_later_range_has_passed_are_read_from_the_index_alone() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    // The first range has passed nothing yet; a later one has passed
    // every key after `m` up to `m/9`.
    remote.running(None);
    remote.lock().ahead = Some(("m".to_owned(), "m/9".to_owned()));
    for key in ["a", "m/2", "z"] {
        remote.put(key, "remote");
    }
    let etag = remote.put("m/1", "remote");
    import(&setup, &bucket, "m/1", &etag).await;
    // `m/2` came after its range passed it: as once the import is done,
    // neither a read nor a listing sees it.
    let head = setup.call(Method::HEAD, "/photos/m/2", &[], "").await;
    assert_eq!(head.status, 404);
    assert_eq!(remote.lock().heads, 0);
    setup
        .call(Method::HEAD, "/photos/a", &[], "")
        .await
        .assert(200, None);
    let all = setup
        .call(Method::GET, "/photos?list-type=2", &[], "")
        .await;
    assert_eq!(listed(&all.body), ["a", "m/1", "z"]);
    let rolled = setup
        .call(Method::GET, "/photos?list-type=2&delimiter=/", &[], "")
        .await;
    assert_eq!(listed(&rolled.body), ["a", "m/", "z"]);

    // Once `m/1` is deleted, no key under `m/` lists, so neither does
    // the prefix the remote still has.
    setup
        .call(Method::DELETE, "/photos/m/1", &[], "")
        .await
        .assert(204, None);
    let rolled = setup
        .call(Method::GET, "/photos?list-type=2&delimiter=/", &[], "")
        .await;
    assert_eq!(listed(&rolled.body), ["a", "z"]);
}

#[tokio::test]
async fn a_conditional_write_sees_an_object_only_at_the_remote() {
    let remote = Arc::new(Remote::default());
    let (setup, bucket) = setup(&remote).await;
    remote.running(None);
    let a = remote.put("a.txt", "hello");
    let b = remote.put("b.txt", "world");
    let c = remote.put("c.txt", "again");

    // The remote's object fails `If-None-Match: *`, and its `IMPORT`,
    // committed first, leaves the key's stub.
    let absent = [("if-none-match", "*")];
    let put = setup.call(Method::PUT, "/photos/a.txt", &absent, "x").await;
    put.assert(412, Some("PreconditionFailed"));
    let stub = entry(&setup, &bucket, "a.txt").await.unwrap();
    assert_eq!(stub.state, EntryState::Evicted);
    assert_eq!(stub.remote_etag.as_ref(), Some(&a));

    // `If-Match` on its ETag holds, and the write replaces it there.
    let quoted = format!("\"{b}\"");
    let matches = [("if-match", quoted.as_str())];
    let put = setup
        .call(Method::PUT, "/photos/b.txt", &matches, "x")
        .await;
    put.assert(200, None);
    let written = entry(&setup, &bucket, "b.txt").await.unwrap();
    assert_eq!(written.state, EntryState::Dirty);
    assert_eq!(written.remote_etag.as_ref(), Some(&b));

    // A conditional delete is checked the same way.
    let wrong = [("if-match", "\"0123456789abcdef0123456789abcdef\"")];
    let delete = setup
        .call(Method::DELETE, "/photos/c.txt", &wrong, "")
        .await;
    delete.assert(412, Some("PreconditionFailed"));
    let quoted = format!("\"{c}\"");
    let matches = [("if-match", quoted.as_str())];
    let delete = setup
        .call(Method::DELETE, "/photos/c.txt", &matches, "")
        .await;
    delete.assert(204, None);

    // A key the remote does not hold is absent.
    let put = setup.call(Method::PUT, "/photos/new", &absent, "x").await;
    put.assert(200, None);

    // A remote that cannot answer cannot decide the condition.
    remote.put("d.txt", "later");
    remote.lock().failing = true;
    let put = setup.call(Method::PUT, "/photos/d.txt", &absent, "x").await;
    put.assert(503, Some("ServiceUnavailable"));
    remote.lock().failing = false;

    // Once the import has passed a key, the index alone decides.
    remote.running(Some("z"));
    let heads = remote.lock().heads;
    let put = setup.call(Method::PUT, "/photos/d.txt", &absent, "x").await;
    put.assert(200, None);
    assert_eq!(remote.lock().heads, heads);
}
