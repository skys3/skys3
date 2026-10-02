//! The hot cache (design §9.2) through the gateway's pipeline: a gateway
//! on another node than the shards reads each GET's bytes from them as a
//! holder, keeps the whole objects it read, and serves later GETs of the
//! same version from memory, but never a version the read plan does not
//! name.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_over};
use http::Method;
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{HotCache, HotCacheUsage};

/// A gateway with a hot cache of `capacity` bytes, over shards on
/// another node if `remote`, with a `local` bucket `photos`. Bodies above
/// 1 KiB are stored as extents of 1,000 bytes.
async fn gateway(capacity: u64, remote: bool) -> (Setup, HotCache) {
    let mut config = config("");
    config.inline_max_bytes = 1024;
    config.extent_bytes = 1000;
    let cache = HotCache::new(capacity);
    config.hot_cache = cache.clone();
    let mut shards = MemoryShards::new().await;
    if remote {
        shards = shards.seen_from("node-2".parse().unwrap());
    }
    let setup = setup_over(config, shards).await;
    setup.create_local("photos").await;
    (setup, cache)
}

/// `len` bytes of `letter`, but for a marker every 100 bytes.
fn body(len: usize, letter: u8) -> Bytes {
    (0..len)
        .map(|i| if i % 100 == 0 { b'0' + (i / 100 % 10) as u8 } else { letter })
        .collect()
}

async fn put(setup: &Setup, key: &str, data: &Bytes) {
    let uri = format!("/photos/{key}");
    setup
        .send(with_body(Method::PUT, &uri, &[], data.clone()))
        .await
        .assert(200, None);
}

async fn get(setup: &Setup, key: &str, headers: &[(&str, &str)]) -> Answer {
    let uri = format!("/photos/{key}");
    setup.call(Method::GET, &uri, headers, "").await
}

/// Waits until the cache holds `entries` objects: a read fills it once
/// its stream has ended, just after the client has the last byte.
async fn holds(cache: &HotCache, entries: u64) -> HotCacheUsage {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let usage = cache.usage();
            if usage.entries == entries {
                return usage;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the cache fills")
}

#[tokio::test]
async fn reads_from_another_node_fill_the_cache_and_a_new_version_misses() {
    let (setup, cache) = gateway(1 << 20, true).await;
    let first = body(2500, b'a');
    put(&setup, "k", &first).await;

    // The first GET reads from the holder and fills the cache.
    let got = get(&setup, "k", &[]).await;
    got.assert(200, None);
    assert_eq!(got.body.as_bytes(), first);
    let usage = holds(&cache, 1).await;
    assert_eq!((usage.bytes, usage.hits, usage.misses), (2500, 0, 1));

    // The next ones, whole or a range, are served from it.
    assert_eq!(get(&setup, "k", &[]).await.body.as_bytes(), first);
    let range = get(&setup, "k", &[("range", "bytes=1000-1199")]).await;
    range.assert(206, None);
    assert_eq!(range.body.as_bytes(), &first[1000..1200]);
    assert_eq!(cache.usage().hits, 2);

    // An overwrite of the same length: the plan names the new version,
    // which misses, drops the old bytes, and reads the new ones.
    let second = body(2500, b'b');
    put(&setup, "k", &second).await;
    let got = get(&setup, "k", &[]).await;
    assert_eq!(got.body.as_bytes(), second);
    let usage = holds(&cache, 1).await;
    assert_eq!((usage.hits, usage.misses), (2, 2));
    assert_eq!(get(&setup, "k", &[]).await.body.as_bytes(), second);
    assert_eq!(cache.usage().hits, 3);

    // A range that misses reads from the holder and keeps nothing.
    put(&setup, "other", &first).await;
    let range = get(&setup, "other", &[("range", "bytes=0-99")]).await;
    assert_eq!(range.body.as_bytes(), &first[..100]);
    // A deleted key needs no lookup.
    setup
        .call(Method::DELETE, "/photos/k", &[], "")
        .await
        .assert(204, None);
    get(&setup, "k", &[]).await.assert(404, Some("NoSuchKey"));
    // Empty objects need no bytes from anyone.
    put(&setup, "empty", &Bytes::new()).await;
    assert_eq!(get(&setup, "empty", &[]).await.body, "");
    let usage = cache.usage();
    assert_eq!((usage.entries, usage.hits, usage.misses), (1, 3, 3));
}

#[tokio::test]
async fn large_objects_and_local_holders_do_not_fill_the_cache() {
    // An object above an eighth of the cache is not kept.
    let (setup, cache) = gateway(8 * 2000, true).await;
    let large = body(2500, b'a');
    put(&setup, "large", &large).await;
    for _ in 0..2 {
        assert_eq!(get(&setup, "large", &[]).await.body.as_bytes(), large);
    }
    let small = body(1500, b'c');
    put(&setup, "small", &small).await;
    assert_eq!(get(&setup, "small", &[]).await.body.as_bytes(), small);
    assert_eq!(holds(&cache, 1).await.bytes, 1500);

    // The gateway's own node holds the bytes: nothing to keep.
    let (setup, cache) = gateway(1 << 20, false).await;
    put(&setup, "k", &small).await;
    for _ in 0..2 {
        assert_eq!(get(&setup, "k", &[]).await.body.as_bytes(), small);
    }
    let usage = cache.usage();
    assert_eq!((usage.entries, usage.hits, usage.misses), (0, 0, 2));
}
