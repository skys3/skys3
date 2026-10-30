//! Adaptive flush concurrency (design §7.7) against simulated targets.
//!
//! Each seed commits a backlog of small objects to one node's shard and
//! starts its flusher against a simulated store behind a network path
//! (`SimLink`): a round trip, a bandwidth that request bodies queue for,
//! and in the second scenario a request-rate limit. A task samples the
//! target's window four times a round trip.
//!
//! - **The bandwidth-delay product.** The round trip is one of 1, 10, 50,
//!   100, and 150 ms, and the bandwidth is drawn so that the
//!   bandwidth-delay product, in requests, lies between 6 and 60: a
//!   request holds the link for its transfer time, so the window that
//!   keeps it busy is one plus the round trip over the transfer time. On a
//!   quarter of the seeds `flush_max_inflight_bytes_per_target` holds half
//!   of that. Over the second half of the flush the mean window must be
//!   near the product, or near what the byte bound allows, and the store
//!   must never hold more request-body bytes than the bound.
//! - **A rate limit.** The path has a product of 40 requests at a 10 ms
//!   round trip. Once the window has grown to most of it, the store
//!   answers requests beyond 1,000 a second, about ten in flight, with
//!   `503 SlowDown` for 300 ms, and then stops. The window must drop to
//!   near the limit's while it lasts, with most requests admitted, and
//!   grow back to most of the product after it.
//!
//! Both check that every object reached the store once: each key holds
//! exactly one version, with the object's bytes and the write identity of
//! its `PUT`, and the index is clean.
//!
//! **Time scale.** Tokio's timers have a resolution of a millisecond, and
//! a transfer at the bandwidths drawn here takes a fraction of one, which
//! a paused clock would round to whole milliseconds. Every duration of the
//! path, and the flusher's backoffs, are therefore simulated [`SCALE`]
//! times longer, with the bandwidth [`SCALE`] times lower. The controller
//! compares only latencies with each other and counts rounds, so its
//! window does not depend on the scale; the scenarios report times
//! divided by it.

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_flush::test_hooks::{ConcurrencyBug, seed_concurrency_bug};
use skys3_flush::{FlushSettings, Target};
use skys3_sim::s3::{RateLimit, SimLink, SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::Outcome;
use crate::support::{Node, cluster, identity, md5_etag, runtime, writes};

/// How many times slower than the paths they model the scenarios run.
const SCALE: u32 = 100;

/// The size of every object: small enough to be stored inline.
const SIZE: u64 = 1024;

/// The round trips the first scenario cycles through, by seed.
const ROUND_TRIPS_MS: [u64; 5] = [1, 10, 50, 100, 150];

/// The least requests in flight per shard, as configured by default.
const FLOOR: u32 = 4;

/// The most, well above any product the scenarios set.
const CEILING: u32 = 1024;

/// The PUTs committed at once while the backlog is written.
const COMMITS_AT_ONCE: usize = 64;

/// A target on `store` whose window lies between [`FLOOR`] and
/// [`CEILING`], with `max_inflight_bytes` as its byte bound.
fn target(store: &SimS3, max_inflight_bytes: u64) -> Arc<Target<SimS3>> {
    let settings = FlushSettings {
        min_concurrency: FLOOR,
        max_concurrency: CEILING,
        max_inflight_bytes,
        min_backoff: Duration::from_millis(10) * SCALE,
        max_backoff: Duration::from_millis(200) * SCALE,
        streaming: false,
        ..FlushSettings::default()
    };
    Arc::new(Target::new(
        Arc::new(store.clone()),
        "",
        writes(true, true, true),
        cluster(),
        settings,
    ))
}

/// A versioned store, so that a duplicated flush leaves a second version,
/// behind `link` as the scenario runs it, with a jitter of up to a
/// twentieth of the round trip on each leg of a request.
fn store(context: &mut SimContext, link: &SimLink) -> SimS3 {
    let store = context.s3(SimS3Config {
        versioning: true,
        ..SimS3Config::default()
    });
    let link = scaled(link);
    store.set_link(link);
    store.set_faults(SimS3Faults {
        max_delay: link.round_trip / 20,
        ..SimS3Faults::NONE
    });
    store
}

/// `link` as the scenarios run it, [`SCALE`] times slower.
fn scaled(link: &SimLink) -> SimLink {
    SimLink {
        round_trip: link.round_trip * SCALE,
        bandwidth: link
            .bandwidth
            .and_then(|bandwidth| NonZeroU64::new(bandwidth.get() / u64::from(SCALE))),
        rate_limit: link.rate_limit.and_then(|limit| {
            Some(RateLimit {
                requests_per_second: NonZeroU32::new(limit.requests_per_second.get() / SCALE)?,
                burst: limit.burst,
            })
        }),
    }
}

/// The body of object `n`.
fn body(n: u32) -> String {
    let head = format!("object {n} ");
    format!("{head}{}", "x".repeat(SIZE as usize - head.len()))
}

/// The key of object `n`.
fn key(n: u32) -> String {
    format!("k{n:05}")
}

/// The window, in requests of [`SIZE`] bytes, that keeps `link` busy.
fn product(link: &SimLink) -> f64 {
    let transfer = link.transfer_time(SIZE).as_secs_f64();
    1.0 + link.round_trip.as_secs_f64() / transfer
}

/// The window sampled over time, as (time since the flusher started,
/// window), in simulated time.
type Samples = Arc<Mutex<Vec<(Duration, u32)>>>;

/// Samples `target`'s window every `every` until the task is aborted.
fn sample(target: &Arc<Target<SimS3>>, every: Duration) -> (Samples, tokio::task::JoinHandle<()>) {
    let samples = Samples::default();
    let (target, kept) = (Arc::clone(target), Arc::clone(&samples));
    let start = Instant::now();
    let task = tokio::spawn(async move {
        loop {
            let limit = target.concurrency().limit;
            kept.lock().unwrap().push((start.elapsed(), limit));
            tokio::time::sleep(every).await;
        }
    });
    (samples, task)
}

/// The mean window of the samples within `from..to`.
fn mean(samples: &[(Duration, u32)], from: Duration, to: Duration) -> f64 {
    let window: Vec<f64> = samples
        .iter()
        .filter(|(at, _)| (from..to).contains(at))
        .map(|&(_, limit)| f64::from(limit))
        .collect();
    window.iter().sum::<f64>() / window.len().max(1) as f64
}

/// Commits objects `0..count`, [`COMMITS_AT_ONCE`] at a time, and returns
/// the `seq` of each `PUT`.
async fn backlog(node: &Node, count: u32) -> Vec<u64> {
    let mut seqs = vec![0; count as usize];
    let mut commits = JoinSet::new();
    for n in 0..count {
        if commits.len() == COMMITS_AT_ONCE
            && let Some(done) = commits.join_next().await
        {
            let (n, seq): (u32, u64) = done.unwrap();
            seqs[n as usize] = seq;
        }
        let shard = node.shard.clone();
        commits.spawn(async move {
            let committed = shard.commit(crate::support::put(&key(n), &body(n))).await;
            (n, committed.unwrap().position.seq.get())
        });
    }
    while let Some(done) = commits.join_next().await {
        let (n, seq) = done.unwrap();
        seqs[n as usize] = seq;
    }
    seqs
}

/// Waits until `target` has flushed `count` versions, polling every
/// `every`.
async fn drained(target: &Target<SimS3>, count: usize, every: Duration) {
    while target.counters().flushes.get() < count as u64 {
        tokio::time::sleep(every).await;
    }
}

/// Checks that the objects whose `PUT`s committed at `seqs` reached
/// `store` exactly once, with their bytes and the identity of their `PUT`,
/// and that `node`'s index is clean.
async fn reached_once(node: &Node, store: &SimS3, seqs: &[u64]) -> Outcome {
    let mut versions: BTreeMap<String, usize> = BTreeMap::new();
    for (key, _, marker) in store.versions() {
        if marker {
            return Err(format!("{key} has a delete marker").into());
        }
        *versions.entry(key).or_default() += 1;
    }
    if versions.len() != seqs.len() {
        return Err(format!("{} keys at the remote, not {}", versions.len(), seqs.len()).into());
    }
    for (n, &seq) in (0..).zip(seqs) {
        let key = key(n);
        if versions.get(&key) != Some(&1) {
            return Err(format!("{key} has {:?} versions, not one", versions.get(&key)).into());
        }
        let object = store.object(&key).ok_or(format!("{key} is missing"))?;
        let expected = md5_etag(body(n).as_bytes());
        if object.info.etag != expected {
            return Err(format!("{key} has ETag {}, not {expected}", object.info.etag).into());
        }
        let wanted = identity(seq);
        if object.info.metadata.write_identity() != Some(wanted.as_str()) {
            return Err(format!("{key} lacks the identity {wanted}").into());
        }
    }
    let unclean = node.unclean().await;
    if !unclean.is_empty() {
        return Err(format!("{} entries are not clean", unclean.len()).into());
    }
    Ok(())
}

/// The window approaches the bandwidth-delay product, or what the byte
/// bound allows.
pub fn reaches_the_bandwidth_delay_product(context: &mut SimContext) -> Outcome {
    let seed = context.seed();
    let mut rng = SmallRng::seed_from_u64(context.fork_seed());
    let round_trip =
        Duration::from_millis(ROUND_TRIPS_MS[(seed % ROUND_TRIPS_MS.len() as u64) as usize]);
    let wanted: f64 = rng.random_range(6.0..60.0);
    // One plus the round trip over the transfer time is `wanted`.
    let transfer = round_trip.as_secs_f64() / (wanted - 1.0);
    let bandwidth = NonZeroU64::new((SIZE as f64 / transfer) as u64).ok_or("no bandwidth")?;
    let link = SimLink {
        round_trip,
        bandwidth: Some(bandwidth),
        rate_limit: None,
    };
    let product = product(&link);
    // On a quarter of the seeds, the byte bound allows half the product.
    let bytes_bound = rng.random_ratio(1, 4);
    let max_inflight_bytes = if bytes_bound {
        (product / 2.0).floor().max(1.0) as u64 * SIZE
    } else {
        1 << 30
    };
    let allowed = (max_inflight_bytes / SIZE) as f64;
    let store = store(context, &link);
    let count = (20.0 * product) as u32 + 200;
    let target = target(&store, max_inflight_bytes);
    let started = std::time::Instant::now();
    let (samples, elapsed) = runtime().block_on(async {
        let node = Node::open_inline(seed).await;
        let seqs = backlog(&node, count).await;
        let flusher = node.flusher(&target);
        let start = Instant::now();
        let (samples, sampler) = sample(&target, round_trip * SCALE / 4);
        drained(&target, seqs.len(), round_trip * SCALE).await;
        let elapsed = start.elapsed();
        sampler.abort();
        node.settle(&flusher).await;
        reached_once(&node, &store, &seqs).await?;
        let samples = samples.lock().unwrap().clone();
        Ok::<_, Box<dyn std::error::Error>>((samples, elapsed))
    })?;
    let reached = mean(&samples, elapsed / 2, elapsed);
    let expected = if bytes_bound {
        product.min(allowed)
    } else {
        product
    }
    .max(f64::from(FLOOR));
    eprintln!(
        "seed {seed}: round trip {round_trip:?}, bandwidth {:.2} MB/s, product {product:.1}, \
         byte bound {}, mean window {reached:.1} (expected {expected:.1}), {count} objects \
         in {:.2?}, {:.2?} real",
        bandwidth.get() as f64 / 1e6,
        if bytes_bound {
            format!("{allowed:.0} objects")
        } else {
            "none".to_owned()
        },
        elapsed / SCALE,
        started.elapsed()
    );
    let stats = store.stats();
    if stats.max_in_flight_bytes > max_inflight_bytes {
        return Err(format!(
            "the store held {} request bytes at once, over the bound of {max_inflight_bytes}",
            stats.max_in_flight_bytes
        )
        .into());
    }
    if reached < 0.8 * expected - 1.0 || reached > 1.35 * expected + 1.0 {
        return Err(format!(
            "the window reached {reached:.1}, not near {expected:.1} (product {product:.1})"
        )
        .into());
    }
    Ok(())
}

/// The window backs off under a rate limit, and grows back after it.
pub fn backs_off_under_a_rate_limit(context: &mut SimContext) -> Outcome {
    let seed = context.seed();
    let mut rng = SmallRng::seed_from_u64(context.fork_seed());
    let round_trip = Duration::from_millis(10);
    // A product of 24 to 48 requests.
    let wanted: u64 = rng.random_range(24..=48);
    let link = SimLink {
        round_trip,
        bandwidth: NonZeroU64::new(SIZE * (wanted - 1) * 100),
        rate_limit: None,
    };
    let product = product(&link);
    // A rate that keeps 6 to a quarter of the product in flight, in
    // hundreds a second, which the scaled limit keeps whole.
    let latency = (round_trip + link.transfer_time(SIZE)).as_secs_f64();
    let in_flight = rng.random_range(6.0..=product / 4.0);
    let rate = ((in_flight / latency / f64::from(SCALE)).round() as u32).max(1) * SCALE;
    let limited = SimLink {
        rate_limit: Some(RateLimit {
            requests_per_second: NonZeroU32::new(rate).ok_or("no rate")?,
            // At least a window's worth of requests sent together.
            burst: rng.random_range(10..=20),
        }),
        ..link
    };
    // The window that keeps `rate` requests a second in flight.
    let at_rate = f64::from(rate) * latency;
    let store = store(context, &link);
    let target = target(&store, 1 << 30);
    let count = 1600;
    let throttle_for = Duration::from_millis(300) * SCALE;
    let started = std::time::Instant::now();
    runtime().block_on(async {
        let node = Node::open_inline(seed).await;
        let seqs = backlog(&node, count).await;
        let flusher = node.flusher(&target);
        let start = Instant::now();
        let every = round_trip * SCALE;
        let (samples, sampler) = sample(&target, every / 4);
        // Until the window has grown to most of the product.
        while f64::from(target.concurrency().limit) < 0.75 * product {
            tokio::time::sleep(every).await;
        }
        let before = target.concurrency().limit;
        let throttled_at = start.elapsed();
        store.set_link(scaled(&limited));
        // The window comes down in the first half; the second half
        // measures how it holds.
        tokio::time::sleep(throttle_for / 2).await;
        let halfway = store.stats();
        tokio::time::sleep(throttle_for / 2).await;
        store.set_link(scaled(&link));
        let lifted_at = start.elapsed();
        let stats = store.stats();
        let requests = stats.requests - halfway.requests;
        let slow_downs = stats.rate_limited - halfway.rate_limited;
        drained(&target, seqs.len(), every).await;
        let elapsed = start.elapsed();
        sampler.abort();
        node.settle(&flusher).await;
        reached_once(&node, &store, &seqs).await?;
        let samples = samples.lock().unwrap().clone();
        // The second half of the limit, and the window's peak after it.
        let during = mean(&samples, throttled_at + throttle_for / 2, lifted_at);
        let after = samples
            .iter()
            .filter(|(at, _)| *at >= lifted_at)
            .map(|&(_, limit)| limit)
            .max()
            .unwrap_or(0);
        let throttles = target.counters().throttles.get();
        eprintln!(
            "seed {seed}: product {product:.1}, rate limit {at_rate:.1} in flight; window \
             {before} before, mean {during:.1} during, peak {after} after; {slow_downs} of \
             {requests} requests slowed down in the second half, {} in all; {count} objects \
             in {:.2?}, {:.2?} real",
            stats.rate_limited,
            elapsed / SCALE,
            started.elapsed()
        );
        if slow_downs == 0 || throttles != stats.rate_limited {
            return Err(format!(
                "{} slow-downs, {throttles} throttles counted",
                stats.rate_limited
            )
            .into());
        }
        if during > 2.0 * at_rate {
            return Err(format!(
                "the window stayed at {during:.1} under a limit of {at_rate:.1} in flight"
            )
            .into());
        }
        if slow_downs * 4 > requests {
            return Err(format!("{slow_downs} of {requests} requests were slowed down").into());
        }
        if f64::from(after) < 0.75 * product {
            return Err(
                format!("the window grew back to {after} only, not most of {product:.1}").into(),
            );
        }
        Ok(())
    })
}

/// Runs each scenario with a bug seeded into adaptive concurrency, and
/// checks that it fails on one of the first seeds:
///
/// - throttles that do not shrink the window, under the rate limit;
/// - requests that do not wait for the in-flight byte bound, on the
///   seeds of the first scenario whose byte bound is below the product.
pub fn catch_seeded_bugs() {
    type Scenario = fn(&mut SimContext) -> Outcome;
    let cases: [(ConcurrencyBug, Scenario, &str); 2] = [
        (
            ConcurrencyBug::IgnoresThrottles,
            backs_off_under_a_rate_limit,
            "under a limit",
        ),
        (
            ConcurrencyBug::IgnoresInflightBytes,
            reaches_the_bandwidth_delay_product,
            "over the bound",
        ),
    ];
    for (bug, scenario, symptom) in cases {
        seed_concurrency_bug(bug);
        let caught = (0..4).find_map(|seed| {
            let started = std::time::Instant::now();
            let error = scenario(&mut SimContext::new(seed)).err()?;
            eprintln!(
                "{bug:?} caught at seed {seed} in {:.2?}: {error}",
                started.elapsed()
            );
            Some(error.to_string())
        });
        seed_concurrency_bug(ConcurrencyBug::None);
        let caught = caught.unwrap_or_else(|| panic!("{bug:?} was not caught"));
        assert!(caught.contains(symptom), "{bug:?}: {caught}");
    }
}
