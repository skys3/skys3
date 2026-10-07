//! `read_only` buckets (design §9.5) against a simulated origin: writes are
//! rejected, GETs and HEADs revalidate with the origin or, under a TTL,
//! trust what it said for a while, the cache serves only the version the
//! origin holds, LIST is forwarded, and what a gateway learns is keyed by
//! the origin and the scope of the credentials it was read with.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use md5::{Digest, Md5};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{
    FillBody, FillError, Fills, OriginHead, OriginScope, RemoteError, RemoteFuture, RemoteListing,
    RemoteObject, RemotePage, RemoteReads, ShardRef, Shards,
};
use skys3_index::{EntryState, ListItem, ListQuery, ObjectVersion, Payload};
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_types::{BucketDocument, BucketId, ETag, EpochSeq, RemoteTarget};
use tokio::sync::mpsc;

/// The origin every bucket of these tests reads.
const ORIGIN: &str = "https://origin.example/datasets/";

/// A simulated origin: objects, the keys each credential scope may not
/// read, and the credentials each bucket reads it with.
#[derive(Debug, Default)]
struct Origin(Mutex<State>);

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<String, (RemoteObject, Bytes)>,
    /// `(credentials, key)` pairs the origin refuses.
    denied: BTreeSet<(String, String)>,
    /// The credentials of each bucket, by ID.
    credentials: BTreeMap<BucketId, String>,
    failing: bool,
    heads: usize,
    gets: usize,
    lists: usize,
    /// What the origin stores at the key right after the next HEAD
    /// answers: a writer racing the read.
    change_after_head: Option<String>,
    /// The most items one page of the origin's listing holds.
    page_limit: Option<usize>,
}

fn etag_of(body: &[u8]) -> ETag {
    let mut hex = String::new();
    for byte in Md5::digest(body) {
        let _ = write!(hex, "{byte:02x}");
    }
    ETag::new(hex).unwrap()
}

impl Origin {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }

    /// Stores `body` at `key`, as a writer SkyS3 does not know does.
    fn put(&self, key: &str, body: &[u8]) -> ETag {
        let etag = etag_of(body);
        let object = ObjectVersion {
            size: body.len() as u64,
            last_modified_ms: 1_700_000_000_000,
            local_etag: etag.clone(),
            write_identity: None,
            metadata: BTreeMap::from([
                ("content-type".to_owned(), "text/plain".to_owned()),
                ("x-amz-meta-source".to_owned(), "origin".to_owned()),
            ]),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            storage_class: None,
            copy_source: None,
            payload: Payload::None,
            coded: None,
        };
        let remote = RemoteObject {
            object,
            version_id: None,
        };
        let bytes = Bytes::copy_from_slice(body);
        self.lock().objects.insert(key.to_owned(), (remote, bytes));
        etag
    }

    fn delete(&self, key: &str) {
        self.lock().objects.remove(key);
    }

    fn deny(&self, credentials: &str, key: &str, denied: bool) {
        let pair = (credentials.to_owned(), key.to_owned());
        let mut state = self.lock();
        if denied {
            state.denied.insert(pair);
        } else {
            state.denied.remove(&pair);
        }
    }

    fn reads_with(&self, bucket: &BucketDocument, credentials: &str) {
        let id = bucket.bucket_id.clone();
        self.lock().credentials.insert(id, credentials.to_owned());
    }

    fn counts(&self) -> (usize, usize, usize) {
        let state = self.lock();
        (state.heads, state.gets, state.lists)
    }

    fn check(&self) -> Result<MutexGuard<'_, State>, RemoteError> {
        let state = self.lock();
        if state.failing {
            return Err(RemoteError("the origin is down".to_owned()));
        }
        Ok(state)
    }

    fn denies(state: &State, bucket: &BucketId, key: &str) -> bool {
        let credentials = state.credentials.get(bucket).cloned().unwrap_or_default();
        state.denied.contains(&(credentials, key.to_owned()))
    }
}

impl RemoteReads for Origin {
    fn import(&self, _bucket: &BucketId) -> Option<skys3_index::ImportCheckpoint> {
        None
    }

    fn head<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, Option<RemoteObject>> {
        Box::pin(async move {
            match self.revalidate(bucket, key).await? {
                OriginHead::Found(found) => Ok(Some(found)),
                OriginHead::Missing => Ok(None),
                OriginHead::Denied => Err(RemoteError("403 AccessDenied".to_owned())),
            }
        })
    }

    fn get<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        etag: &'a ETag,
        range: Range<u64>,
    ) -> RemoteFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let mut state = self.check()?;
            state.gets += 1;
            if Origin::denies(&state, bucket, key) {
                return Err(RemoteError("403 AccessDenied".to_owned()));
            }
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
            let mut state = self.check()?;
            state.lists += 1;
            let query = ListQuery {
                prefix: listing.prefix.clone(),
                delimiter: listing.delimiter.clone(),
                ..ListQuery::default()
            };
            // As S3 does, an item lists only if it sorts after the token
            // or the start: a common prefix at or before it is left out.
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
            let max = listing
                .max_items
                .min(state.page_limit.unwrap_or(usize::MAX));
            let more = items.len() > max;
            items.truncate(max);
            // An empty page that has more continues where it started.
            let last = items.last().map(|item| item.name().to_owned());
            let next = more.then(|| last.or(after).unwrap_or_default());
            Ok(RemotePage { items, next })
        })
    }

    fn origin_scope(&self, bucket: &BucketId) -> Option<OriginScope> {
        let credentials = self.lock().credentials.get(bucket).cloned()?;
        let target = skys3_config::parse_target(ORIGIN).unwrap();
        Some(OriginScope::new(&target, credentials))
    }

    fn revalidate<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, OriginHead> {
        Box::pin(async move {
            let mut state = self.check()?;
            state.heads += 1;
            let head = if Origin::denies(&state, bucket, key) {
                OriginHead::Denied
            } else {
                match state.objects.get(key) {
                    Some((object, _)) => OriginHead::Found(object.clone()),
                    None => OriginHead::Missing,
                }
            };
            if let Some(body) = state.change_after_head.take() {
                drop(state);
                self.put(key, body.as_bytes());
            }
            Ok(head)
        })
    }
}

/// Fills as a shard's primary makes them: the key's evicted version is
/// read from the origin under `If-Match` and cached as one extent, or,
/// once the origin changed, the read is told so. While unavailable, as on
/// a node that is not the primary, it fills nothing.
#[derive(Debug)]
struct OriginFills {
    origin: Arc<Origin>,
    shards: MemoryShards,
    available: AtomicBool,
    fills: Mutex<usize>,
}

impl Fills for OriginFills {
    fn read(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<FillBody, FillError>> + Send + '_>> {
        let (shard, key) = (shard.clone(), key.to_owned());
        Box::pin(async move {
            if !self.available.load(Ordering::SeqCst) {
                return Err(FillError::Unavailable("not the primary".to_owned()));
            }
            let local = self
                .shards
                .local()
                .set()
                .get(&(&shard).into())
                .await
                .unwrap();
            let entry = local.entry(&key).await.unwrap().ok_or(FillError::Changed)?;
            let object = entry.object.clone().ok_or(FillError::Changed)?;
            if entry.version != version || entry.state != EntryState::Evicted {
                return Err(FillError::Changed);
            }
            let bucket = shard.bucket.clone();
            let data = self
                .origin
                .get(&bucket, &key, &object.local_etag, 0..object.size)
                .await
                .map_err(|error| FillError::Unavailable(error.0))?
                .ok_or(FillError::Changed)?;
            *self.fills.lock().unwrap() += 1;
            if !data.is_empty() {
                let extent = Extent {
                    key: key.clone(),
                    offset: 0,
                    data: data.clone(),
                };
                let committed = local.commit(RecordBody::Extent(extent)).await.unwrap();
                let extents = vec![ExtentRef {
                    position: committed.position,
                    len: u32::try_from(data.len()).unwrap(),
                }];
                local
                    .fill(&key, version, Payload::Extents(extents))
                    .await
                    .unwrap()
                    .unwrap();
            }
            let (sender, receiver) = mpsc::channel(1);
            let slice = data.slice(range.start as usize..range.end as usize);
            sender.try_send(Ok(slice)).unwrap();
            Ok(receiver)
        })
    }
}

/// A gateway whose `read_only` buckets read `origin`, with `fills` that
/// cache what they read, and buckets with the TOML settings `buckets`.
struct World {
    setup: Setup,
    origin: Arc<Origin>,
    fills: Arc<OriginFills>,
}

impl World {
    async fn new(buckets: &str) -> Self {
        let origin = Arc::new(Origin::default());
        let shards = MemoryShards::new().await;
        let fills = Arc::new(OriginFills {
            origin: Arc::clone(&origin),
            shards: shards.clone(),
            available: AtomicBool::new(true),
            fills: Mutex::default(),
        });
        let mut config = config(&format!(
            "[buckets.defaults]\nshards_per_bucket = 2\n{buckets}"
        ));
        config.remote = Some(Arc::clone(&origin) as Arc<dyn RemoteReads>);
        config.fills = Some(Arc::clone(&fills) as Arc<dyn Fills>);
        let setup = common::setup_over(config, shards).await;
        Self {
            setup,
            origin,
            fills,
        }
    }

    /// Creates the `read_only` bucket `name`, read with `credentials`.
    async fn bucket(&self, name: &str, credentials: &str) -> BucketDocument {
        self.setup
            .create(name, "read_only", Some(ORIGIN))
            .await
            .assert(200, None);
        let bucket = self.setup.register(name).await.unwrap();
        self.origin.reads_with(&bucket, credentials);
        bucket
    }

    async fn get(&self, path: &str) -> Answer {
        self.setup.call(Method::GET, path, &[], "").await
    }

    async fn head(&self, path: &str) -> Answer {
        self.setup.call(Method::HEAD, path, &[], "").await
    }

    fn fills(&self) -> usize {
        *self.fills.fills.lock().unwrap()
    }

    async fn entry_state(&self, bucket: &BucketDocument, key: &str) -> Option<EntryState> {
        let shard = ShardRef::for_key(bucket, key);
        let entry = self.setup.shards.entry(&shard, key).await.unwrap();
        entry.map(|entry| entry.state)
    }
}

/// The ETag header of an object whose body is `body`.
fn quoted(body: &[u8]) -> String {
    format!("\"{}\"", etag_of(body).as_str())
}

#[tokio::test]
async fn read_only_buckets_take_no_writes() {
    let world = World::new("").await;
    world.bucket("mirror", "default").await;
    world.setup.create_local("plain").await;
    world.origin.put("k", b"origin bytes");
    let setup = &world.setup;
    for (method, path, headers) in [
        (Method::PUT, "/mirror/k", vec![]),
        (Method::DELETE, "/mirror/k", vec![]),
        (Method::POST, "/mirror/k?uploads", vec![]),
        (Method::PUT, "/mirror/k?tagging", vec![]),
        (Method::DELETE, "/mirror/k?tagging", vec![]),
        (
            Method::PUT,
            "/mirror/copy",
            vec![("x-amz-copy-source", "/plain/k")],
        ),
    ] {
        let body = if path.ends_with("?tagging") && method == Method::PUT {
            "<Tagging><TagSet></TagSet></Tagging>"
        } else {
            ""
        };
        setup
            .call(method.clone(), path, &headers, body)
            .await
            .assert(403, Some("AccessDenied"));
    }
    let delete = "<Delete><Object><Key>k</Key></Object></Delete>";
    let md5 = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(Md5::digest(delete.as_bytes()))
    };
    let answer = setup
        .call(
            Method::POST,
            "/mirror?delete",
            &[("content-md5", &md5)],
            delete,
        )
        .await;
    answer.assert(403, Some("AccessDenied"));
    // A copy would read the cached entry without asking the origin, and
    // tags are not read from it.
    setup
        .call(
            Method::PUT,
            "/plain/copy",
            &[("x-amz-copy-source", "/mirror/k")],
            "",
        )
        .await
        .assert(501, Some("NotImplemented"));
    setup
        .call(Method::GET, "/mirror/k?tagging", &[], "")
        .await
        .assert(501, Some("NotImplemented"));
    // The origin was never written, and still serves.
    let got = world.get("/mirror/k").await;
    got.assert(200, None);
    assert_eq!(got.body, "origin bytes");
}

#[tokio::test]
async fn a_read_only_bucket_needs_an_origin() {
    let world = World::new("").await;
    let answer = world.setup.create("mirror", "read_only", None).await;
    answer.assert(400, Some("InvalidArgument"));
    assert!(
        answer.body.contains("read_only bucket needs a target"),
        "{answer:?}"
    );
    let answer = world
        .setup
        .create("mirror", "read_only", Some("ftp://origin/x"))
        .await;
    answer.assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn under_revalidate_every_origin_change_is_seen_by_the_next_get() {
    let world = World::new("").await;
    let bucket = world.bucket("mirror", "default").await;
    world.origin.put("k", b"first");

    let got = world.get("/mirror/k").await;
    got.assert(200, None);
    assert_eq!(got.body, "first");
    assert_eq!(got.headers["etag"], quoted(b"first").as_str());
    assert_eq!(got.headers["content-type"], "text/plain");
    assert_eq!(got.headers["x-amz-meta-source"], "origin");
    assert_eq!(world.fills(), 1);
    assert_eq!(
        world.entry_state(&bucket, "k").await,
        Some(EntryState::Clean)
    );

    // The cached copy serves while the origin holds its version: one HEAD
    // per read, and no more GETs of the origin.
    let (heads, gets, _) = world.origin.counts();
    let again = world.get("/mirror/k").await;
    assert_eq!(again.body, "first");
    assert_eq!(world.origin.counts(), (heads + 1, gets, 0));
    let ranged = world
        .setup
        .call(Method::GET, "/mirror/k", &[("range", "bytes=1-3")], "")
        .await;
    ranged.assert(206, None);
    assert_eq!(ranged.body, "irs");
    assert_eq!(world.fills(), 1);

    // An overwrite at the origin is the next GET's answer.
    world.origin.put("k", b"second version");
    let changed = world.get("/mirror/k").await;
    changed.assert(200, None);
    assert_eq!(changed.body, "second version");
    assert_eq!(changed.headers["etag"], quoted(b"second version").as_str());
    assert_eq!(world.fills(), 2);

    // A HEAD sees the change too, and caches nothing.
    world.origin.put("k", b"third");
    let head = world.head("/mirror/k").await;
    head.assert(200, None);
    assert_eq!(head.headers["etag"], quoted(b"third").as_str());
    assert_eq!(head.headers["content-length"], "5");
    assert_eq!(world.fills(), 2);

    // A delete at the origin is a 404, though the cache holds a copy.
    world.origin.delete("k");
    world.get("/mirror/k").await.assert(404, Some("NoSuchKey"));
    world.head("/mirror/k").await.assert(404, Some("NoSuchKey"));
    world
        .get("/mirror/never")
        .await
        .assert(404, Some("NoSuchKey"));
    world.origin.put("k", b"back");
    assert_eq!(world.get("/mirror/k").await.body, "back");

    // Conditional reads are checked against the origin's version.
    let etag = quoted(b"back");
    world
        .setup
        .call(Method::GET, "/mirror/k", &[("if-none-match", &etag)], "")
        .await
        .assert(304, None);
    world.origin.put("k", b"moved on");
    let fresh = world
        .setup
        .call(Method::GET, "/mirror/k", &[("if-none-match", &etag)], "")
        .await;
    fresh.assert(200, None);
    assert_eq!(fresh.body, "moved on");
}

#[tokio::test]
async fn an_origin_change_racing_a_get_is_never_served_as_the_old_version() {
    let world = World::new("").await;
    world.bucket("mirror", "default").await;
    world.origin.put("k", b"before");
    // The origin changes right after the GET's HEAD: the fill finds its
    // version gone, and the GET asks again.
    world.origin.lock().change_after_head = Some("after".to_owned());
    let got = world.get("/mirror/k").await;
    got.assert(200, None);
    assert_eq!(got.body, "after");
    assert_eq!(got.headers["etag"], quoted(b"after").as_str());
}

#[tokio::test]
async fn under_a_ttl_reads_trust_the_origin_for_the_ttl_only() {
    let world = World::new("freshness = \"ttl\"\nfreshness_ttl_seconds = 1\n").await;
    world.bucket("mirror", "default").await;
    world.origin.put("k", b"first");
    assert_eq!(world.get("/mirror/k").await.body, "first");
    let (heads, gets, _) = world.origin.counts();

    // Within the TTL, the origin is not asked, and its change not seen.
    world.origin.put("k", b"second");
    let stale = world.get("/mirror/k").await;
    assert_eq!(stale.body, "first");
    world.head("/mirror/k").await.assert(200, None);
    assert_eq!(world.origin.counts(), (heads, gets, 0));

    // After it, the next read asks, and sees the change.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(world.get("/mirror/k").await.body, "second");
    assert_eq!(world.origin.counts().0, heads + 1);

    // A missing object is remembered for the TTL too.
    world.origin.delete("k");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    world.get("/mirror/k").await.assert(404, Some("NoSuchKey"));
    world.origin.put("k", b"third");
    world.get("/mirror/k").await.assert(404, Some("NoSuchKey"));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(world.get("/mirror/k").await.body, "third");
}

#[tokio::test]
async fn credential_scopes_never_share_what_they_learn() {
    let world = World::new("freshness = \"ttl\"\nfreshness_ttl_seconds = 1\n").await;
    world.bucket("open", "profile:reader").await;
    world.bucket("open-too", "profile:reader").await;
    world.bucket("restricted", "profile:guest").await;
    world.origin.put("secret", b"classified");
    world.origin.deny("profile:guest", "secret", true);

    // The same origin and key, under two scopes: one may read it, one not.
    assert_eq!(world.get("/open/secret").await.body, "classified");
    world
        .get("/restricted/secret")
        .await
        .assert(403, Some("AccessDenied"));
    world
        .head("/restricted/secret")
        .await
        .assert(403, Some("AccessDenied"));

    // What one scope learned serves every bucket of the same scope, and
    // no bucket of another: a key `open` saw missing is missing to
    // `open-too` for the TTL, but `restricted` asks for itself.
    world
        .get("/open/later")
        .await
        .assert(404, Some("NoSuchKey"));
    world.origin.put("later", b"arrived");
    world
        .get("/open-too/later")
        .await
        .assert(404, Some("NoSuchKey"));
    assert_eq!(world.get("/restricted/later").await.body, "arrived");

    // A refusal is remembered for the TTL, and lifted after it.
    world.origin.deny("profile:guest", "secret", false);
    world
        .get("/restricted/secret")
        .await
        .assert(403, Some("AccessDenied"));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(world.get("/restricted/secret").await.body, "classified");
    assert_eq!(world.get("/open-too/later").await.body, "arrived");
}

#[tokio::test]
async fn a_gateway_that_cannot_fill_reads_the_origin_directly() {
    let world = World::new("").await;
    let bucket = world.bucket("mirror", "default").await;
    world.fills.available.store(false, Ordering::SeqCst);
    // More than one chunk of a direct read, streamed after the first.
    let large: Vec<u8> = (0..(9u32 << 20)).map(|n| b'a' + (n % 26) as u8).collect();
    world.origin.put("large", &large);
    let (_, gets, _) = world.origin.counts();
    let got = world.get("/mirror/large").await;
    got.assert(200, None);
    assert!(got.body.as_bytes() == large.as_slice(), "the bytes differ");
    assert_eq!(world.origin.counts().1, gets + 2);
    // Nothing is cached: the entry stays evicted, for the primary to fill.
    assert_eq!(
        world.entry_state(&bucket, "large").await,
        Some(EntryState::Evicted)
    );
    assert_eq!(world.fills(), 0);

    world.origin.put("small", b"tiny");
    let ranged = world
        .setup
        .call(Method::GET, "/mirror/small", &[("range", "bytes=1-2")], "")
        .await;
    ranged.assert(206, None);
    assert_eq!(ranged.body, "in");

    // A change between the HEAD and the read is asked about again.
    world.origin.lock().change_after_head = Some("changed".to_owned());
    assert_eq!(world.get("/mirror/small").await.body, "changed");

    // Once this node fills, the copy is cached.
    world.fills.available.store(true, Ordering::SeqCst);
    assert_eq!(world.get("/mirror/small").await.body, "changed");
    assert_eq!(
        world.entry_state(&bucket, "small").await,
        Some(EntryState::Clean)
    );
}

#[tokio::test]
async fn an_origin_that_cannot_answer_fails_reads_with_503() {
    let world = World::new("freshness = \"ttl\"\nfreshness_ttl_seconds = 60\n").await;
    world.bucket("mirror", "default").await;
    world.origin.put("k", b"bytes");
    world.origin.lock().failing = true;
    world
        .get("/mirror/k")
        .await
        .assert(503, Some("ServiceUnavailable"));
    world
        .setup
        .call(Method::GET, "/mirror?list-type=2", &[], "")
        .await
        .assert(503, Some("ServiceUnavailable"));
    world.origin.lock().failing = false;
    assert_eq!(world.get("/mirror/k").await.body, "bytes");
    // Under a TTL, what was learned serves while the origin is down.
    world.origin.lock().failing = true;
    assert_eq!(world.get("/mirror/k").await.body, "bytes");
}

#[tokio::test]
async fn a_bucket_whose_origin_is_not_read_here_answers_503() {
    let world = World::new("").await;
    world
        .setup
        .create("mirror", "read_only", Some(ORIGIN))
        .await
        .assert(200, None);
    // No credentials: the origin is not read by this node yet.
    world
        .get("/mirror/k")
        .await
        .assert(503, Some("ServiceUnavailable"));

    // Without any origin reads at all, neither reads nor listings work.
    let setup = setup_with(config("")).await;
    setup
        .create("mirror", "read_only", Some(ORIGIN))
        .await
        .assert(200, None);
    let got = setup.call(Method::GET, "/mirror/k", &[], "").await;
    got.assert(503, Some("ServiceUnavailable"));
    let listed = setup
        .call(Method::GET, "/mirror?list-type=2", &[], "")
        .await;
    listed.assert(503, Some("ServiceUnavailable"));
}

/// A node's origin reads without a `revalidate` of their own: a HEAD
/// cannot tell a refusal from a failure.
#[derive(Debug)]
struct HeadOnly(Arc<Origin>);

impl RemoteReads for HeadOnly {
    fn import(&self, bucket: &BucketId) -> Option<skys3_index::ImportCheckpoint> {
        self.0.import(bucket)
    }

    fn head<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, Option<RemoteObject>> {
        self.0.head(bucket, key)
    }

    fn get<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        etag: &'a ETag,
        range: Range<u64>,
    ) -> RemoteFuture<'a, Option<Bytes>> {
        self.0.get(bucket, key, etag, range)
    }

    fn list<'a>(
        &'a self,
        bucket: &'a BucketId,
        listing: RemoteListing,
    ) -> RemoteFuture<'a, RemotePage> {
        self.0.list(bucket, listing)
    }

    fn origin_scope(&self, bucket: &BucketId) -> Option<OriginScope> {
        self.0.origin_scope(bucket)
    }
}

#[tokio::test]
async fn revalidation_defaults_to_a_head() {
    let origin = Arc::new(Origin::default());
    let mut config = config("");
    config.remote = Some(Arc::new(HeadOnly(Arc::clone(&origin))) as Arc<dyn RemoteReads>);
    let setup = setup_with(config).await;
    setup
        .create("mirror", "read_only", Some(ORIGIN))
        .await
        .assert(200, None);
    let bucket = setup.register("mirror").await.unwrap();
    origin.reads_with(&bucket, "default");
    origin.put("k", b"via head");
    let got = setup.call(Method::GET, "/mirror/k", &[], "").await;
    // No fills: the bytes come straight from the origin.
    got.assert(200, None);
    assert_eq!(got.body, "via head");
    setup
        .call(Method::GET, "/mirror/gone", &[], "")
        .await
        .assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn listings_are_forwarded_to_the_origin() {
    let world = World::new("").await;
    world.bucket("mirror", "default").await;
    for key in ["a", "b/1", "b/2", "c", "d"] {
        world.origin.put(key, key.as_bytes());
    }
    // A cached key that the origin then deletes does not list.
    assert_eq!(world.get("/mirror/a").await.body, "a");
    world.origin.delete("a");

    let first = world
        .setup
        .call(
            Method::GET,
            "/mirror?list-type=2&delimiter=/&max-keys=2",
            &[],
            "",
        )
        .await;
    first.assert(200, None);
    assert!(first.body.contains("<Prefix>b/</Prefix>"), "{}", first.body);
    assert!(first.body.contains("<Key>c</Key>"), "{}", first.body);
    assert!(!first.body.contains("<Key>a</Key>"), "{}", first.body);
    assert!(first.body.contains("<IsTruncated>true</IsTruncated>"));
    let start = first.body.find("<NextContinuationToken>").unwrap() + 23;
    let end = first.body.find("</NextContinuationToken>").unwrap();
    let token = first.body[start..end].to_owned();
    let encoded: String = url_encode(&token);
    let second = world
        .setup
        .call(
            Method::GET,
            &format!("/mirror?list-type=2&delimiter=/&max-keys=2&continuation-token={encoded}"),
            &[],
            "",
        )
        .await;
    second.assert(200, None);
    assert!(second.body.contains("<Key>d</Key>"), "{}", second.body);
    assert!(!second.body.contains("<Key>c</Key>"), "{}", second.body);
    assert!(second.body.contains("<IsTruncated>false</IsTruncated>"));

    // Resumed after a common prefix, the keys under it are skipped; V1
    // lists the same way.
    let v1 = world
        .setup
        .call(Method::GET, "/mirror?marker=b/&prefix=", &[], "")
        .await;
    v1.assert(200, None);
    assert!(v1.body.contains("<Key>c</Key>") && v1.body.contains("<Key>d</Key>"));
    let v1 = world
        .setup
        .call(Method::GET, "/mirror?delimiter=/&marker=b/", &[], "")
        .await;
    let rest = v1.body.replace("<Marker>b/</Marker>", "");
    assert!(!rest.contains("b/"), "{rest}");
    let none = world
        .setup
        .call(Method::GET, "/mirror?list-type=2&max-keys=0", &[], "")
        .await;
    assert!(none.body.contains("<IsTruncated>false</IsTruncated>"));
}

#[tokio::test]
async fn listings_follow_the_origins_pages_until_the_page_is_full() {
    let world = World::new("").await;
    world.bucket("mirror", "default").await;
    for n in 0..30 {
        world.origin.put(&format!("k{n:02}"), b"x");
    }
    // The origin's pages, of 7 items each, are followed to fill a page.
    world.origin.lock().page_limit = Some(7);
    let listed = world
        .setup
        .call(Method::GET, "/mirror?list-type=2", &[], "")
        .await;
    listed.assert(200, None);
    assert_eq!(listed.body.matches("<Key>").count(), 30);
    assert!(listed.body.contains("<IsTruncated>false</IsTruncated>"));
    assert_eq!(world.origin.counts().2, 5);
    // A page that is full while the origin has more is truncated.
    let page = world
        .setup
        .call(Method::GET, "/mirror?list-type=2&max-keys=10", &[], "")
        .await;
    assert_eq!(page.body.matches("<Key>").count(), 10);
    assert!(page.body.contains("<Key>k09</Key>"), "{}", page.body);
    assert!(page.body.contains("<IsTruncated>true</IsTruncated>"));
    assert_eq!(world.origin.counts().2, 7);
    // An origin that never fills a page is given up on.
    world.origin.lock().page_limit = Some(0);
    let stuck = world
        .setup
        .call(Method::GET, "/mirror?list-type=2", &[], "")
        .await;
    stuck.assert(503, Some("ServiceUnavailable"));
}

fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[test]
fn origin_scopes_name_their_origin_and_credentials() {
    let target = RemoteTarget {
        endpoint: "https://origin.example".to_owned(),
        bucket: "datasets".to_owned(),
        prefix: None,
    };
    let scope = OriginScope::new(&target, "default");
    assert_eq!(
        scope.to_string(),
        "https://origin.example/datasets/ as default"
    );
}
