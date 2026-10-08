use std::cmp::Reverse;
use std::collections::BinaryHeap;

use skys3_remote::S3ErrorKind;

use super::*;

const MS: Duration = Duration::from_millis(1);

/// Sends `count` requests, `waited` if they found the window full.
fn send(controller: &mut Controller, count: u32, waited: bool) -> Vec<u64> {
    (0..count).map(|_| controller.sent(waited)).collect()
}

/// Answers `requests` after `latency` each.
fn answer(controller: &mut Controller, requests: &[u64], latency: Duration) {
    for &number in requests {
        controller.answered(number, Signal::Answered(latency));
    }
}

/// Drives a controller a round at a time, as a backlog does: each round
/// sends a full window, and is measured by the answers to the rest of the
/// previous window and to the first request of the new one, which ends it.
struct Driver {
    controller: Controller,
    outstanding: Vec<u64>,
}

impl Driver {
    fn new(floor: u32, ceiling: u32) -> Self {
        Self {
            controller: Controller::new(floor, ceiling),
            outstanding: Vec::new(),
        }
    }

    /// Runs a round whose answers take `latency`, and returns the window.
    fn round(&mut self, latency: Duration) -> u32 {
        let limit = self.controller.limit();
        let sent = send(&mut self.controller, limit, true);
        let previous = std::mem::take(&mut self.outstanding);
        answer(&mut self.controller, &previous, latency);
        answer(&mut self.controller, &sent[..1], latency);
        self.outstanding = sent[1..].to_vec();
        self.controller.limit()
    }
}

#[test]
fn slow_start_doubles_each_full_round_up_to_the_ceiling() {
    let mut driver = Driver::new(4, 100);
    assert_eq!(driver.controller.limit(), 4);
    assert_eq!(driver.controller.base(), None);
    let limits: Vec<u32> = (0..6).map(|_| driver.round(10 * MS)).collect();
    assert_eq!(limits, [8, 16, 32, 64, 100, 100]);
    assert_eq!(driver.controller.base(), Some(10 * MS));
    assert_eq!(driver.controller.bounds(), (4, 100));
}

#[test]
fn a_round_in_which_nothing_waited_does_not_grow() {
    let mut controller = Controller::new(4, 100);
    let requests = send(&mut controller, 2, false);
    answer(&mut controller, &requests, 10 * MS);
    assert_eq!(controller.limit(), 4);
    // One request that waited is enough.
    let mut requests = send(&mut controller, 3, false);
    requests.extend(send(&mut controller, 1, true));
    answer(&mut controller, &requests, 10 * MS);
    assert_eq!(controller.limit(), 8);
}

#[test]
fn rising_latency_shrinks_to_below_the_knee_then_grows_by_one() {
    let mut driver = Driver::new(4, 1000);
    for _ in 0..5 {
        driver.round(10 * MS);
    }
    assert_eq!(driver.controller.limit(), 128);
    // A quarter above the base is still near it.
    assert_eq!(driver.round(12 * MS + MS / 2), 256);
    // Twice the base: the knee is at 128, and the window aims below it,
    // with a factor of at least a half.
    assert_eq!(driver.round(20 * MS), 128);
    // The round after only measures.
    assert_eq!(driver.round(20 * MS), 128);
    // Out of the slow start: one request a round.
    assert_eq!(driver.round(10 * MS), 129);
    assert_eq!(driver.round(10 * MS), 130);
    // A third above the base: 130 × 0.9 × 0.75, truncated.
    assert_eq!(driver.round(13 * MS + MS / 3), 87);
    assert_eq!(driver.controller.base(), Some(10 * MS));
}

#[test]
fn a_small_rise_aims_just_below_the_knee() {
    let mut driver = Driver::new(1, 1000);
    for _ in 0..4 {
        driver.round(100 * MS);
    }
    assert_eq!(driver.controller.limit(), 16);
    // Just past the tolerance, the knee is at 16 × 100 / 126 and the
    // window aims at 0.9 of it.
    assert_eq!(driver.round(126 * MS), 11);
}

#[test]
fn rounds_end_when_a_request_sent_after_their_start_is_answered() {
    let mut controller = Controller::new(4, 100);
    let first = send(&mut controller, 4, true);
    // Requests of the same round answered out of order end it at the first
    // answer: the round started before any of them.
    answer(&mut controller, &first[3..], 10 * MS);
    assert_eq!(controller.limit(), 8);
    // The rest belong to the next round, which ends only with a request
    // sent after it started.
    answer(&mut controller, &first[..3], 10 * MS);
    assert_eq!(controller.limit(), 8);
    let second = send(&mut controller, 1, true);
    answer(&mut controller, &second, 10 * MS);
    assert_eq!(controller.limit(), 16);
}

#[test]
fn throttles_shrink_once_per_window() {
    let mut driver = Driver::new(4, 1000);
    for _ in 0..4 {
        driver.round(10 * MS);
    }
    let controller = &mut driver.controller;
    assert_eq!(controller.limit(), 64);
    let requests = send(controller, 64, true);
    controller.answered(requests[0], Signal::Throttled);
    assert_eq!(controller.limit(), 44);
    // Throttles of requests sent before the decrease count once.
    for &number in &requests[1..10] {
        controller.answered(number, Signal::Throttled);
    }
    assert_eq!(controller.limit(), 44);
    // A request sent after it is throttled too: the rate is still too high.
    let later = controller.sent(true);
    controller.answered(later, Signal::Throttled);
    assert_eq!(controller.limit(), 30);
    // Throttles never take the window below its floor.
    for _ in 0..20 {
        let number = controller.sent(true);
        controller.answered(number, Signal::Throttled);
    }
    assert_eq!(controller.limit(), 4);
    // Then it grows by one a round, after a round that only measures.
    assert_eq!(driver.round(10 * MS), 4);
    assert_eq!(driver.round(10 * MS), 5);
    assert_eq!(driver.round(10 * MS), 6);
}

#[test]
fn failures_say_nothing() {
    let mut controller = Controller::new(4, 100);
    let requests = send(&mut controller, 4, true);
    for &number in &requests {
        controller.answered(number, Signal::Failed);
    }
    assert_eq!(controller.limit(), 4);
    assert_eq!(controller.base(), None);
}

#[test]
fn the_base_round_trip_is_a_windowed_minimum() {
    let mut driver = Driver::new(1, 1);
    driver.round(10 * MS);
    driver.round(30 * MS);
    driver.round(20 * MS);
    assert_eq!(driver.controller.base(), Some(10 * MS));
    // The minimum ages out after its rounds, and the next smallest of the
    // rounds kept takes its place.
    for _ in 3..BASE_ROUNDS {
        driver.round(40 * MS);
    }
    assert_eq!(driver.controller.base(), Some(10 * MS));
    driver.round(40 * MS);
    assert_eq!(driver.controller.base(), Some(20 * MS));
    driver.round(40 * MS);
    driver.round(40 * MS);
    assert_eq!(driver.controller.base(), Some(40 * MS));
}

#[test]
fn bounds_clamp_the_window() {
    let mut controller = Controller::new(0, 0);
    assert_eq!((controller.limit(), controller.bounds()), (1, (1, 1)));
    controller.set_bounds(8, 64);
    assert_eq!(controller.limit(), 8);
    controller.set_bounds(2, 4);
    assert_eq!(controller.limit(), 4);
    controller.set_bounds(10, 5);
    assert_eq!(controller.bounds(), (10, 10));
}

#[test]
fn answers_are_samples_throttles_or_nothing() {
    let latency = 5 * MS;
    let error = |kind| Err::<(), _>(S3Error::new(kind, ""));
    assert_eq!(Signal::of(&Ok(()), latency), Signal::Answered(latency));
    for kind in [S3ErrorKind::PreconditionFailed, S3ErrorKind::NoSuchKey] {
        assert_eq!(Signal::of(&error(kind), latency), Signal::Answered(latency));
    }
    for kind in [S3ErrorKind::SlowDown, S3ErrorKind::ServiceUnavailable] {
        assert_eq!(Signal::of(&error(kind), latency), Signal::Throttled);
    }
    let too_many = Err::<(), _>(S3Error::new(S3ErrorKind::Other, "").with_status(429));
    assert_eq!(Signal::of(&too_many, latency), Signal::Throttled);
    for kind in [S3ErrorKind::InternalError, S3ErrorKind::Timeout] {
        assert_eq!(Signal::of(&error(kind), latency), Signal::Failed);
    }
}

/// A model of a link with round trip `round_trip` and `bandwidth` bytes a
/// second, whose request bodies of `size` bytes queue one after another,
/// driven by a controller between `floor` and `ceiling` with an endless
/// backlog. Returns the mean window over the second half of `requests`.
fn steady_window(
    round_trip: Duration,
    bandwidth: u64,
    size: u64,
    (floor, ceiling): (u32, u32),
    requests: u64,
) -> f64 {
    let transfer = Duration::from_nanos(size * 1_000_000_000 / bandwidth);
    let mut controller = Controller::new(floor, ceiling);
    // Answers due, as (time, request, time sent).
    let mut due = BinaryHeap::new();
    let (mut now, mut link_free) = (Duration::ZERO, Duration::ZERO);
    let (mut sent, mut answered, mut in_flight) = (0, 0, 0);
    let (mut sum, mut samples) = (0.0, 0.0);
    while answered < requests {
        while in_flight < controller.limit() {
            let number = controller.sent(true);
            let leaves = link_free.max(now) + transfer;
            link_free = leaves;
            due.push(Reverse((leaves + round_trip, number, now)));
            in_flight += 1;
            sent += 1;
        }
        let Reverse((at, number, sent_at)) = due.pop().unwrap();
        now = at;
        in_flight -= 1;
        answered += 1;
        controller.answered(number, Signal::Answered(at - sent_at));
        if answered > requests / 2 {
            sum += f64::from(controller.limit());
            samples += 1.0;
        }
    }
    assert!(sent >= requests);
    sum / samples
}

/// The bandwidth-delay product in requests: the window that keeps the
/// link busy, one request leaving as the previous one's answer returns.
fn bdp(round_trip: Duration, bandwidth: u64, size: u64) -> f64 {
    1.0 + bandwidth as f64 * round_trip.as_secs_f64() / size as f64
}

#[test]
fn the_window_approaches_the_bandwidth_delay_product() {
    const MIB: u64 = 1 << 20;
    const SIZE: u64 = 64 << 10;
    for round_trip in [1, 10, 50, 100, 150].map(Duration::from_millis) {
        for bandwidth in [16 * MIB, 64 * MIB, 256 * MIB] {
            let product = bdp(round_trip, bandwidth, SIZE);
            // Long enough for the slow start and many periods after it.
            let requests = (400.0 * product) as u64 + 20_000;
            let window = steady_window(round_trip, bandwidth, SIZE, (1, 4096), requests);
            eprintln!(
                "{round_trip:?} at {} MiB/s: window {window:.1}, BDP {product:.1}",
                bandwidth / MIB
            );
            assert!(
                window >= 0.9 * product - 1.0 && window <= 1.2 * product + 1.0,
                "{round_trip:?} at {bandwidth} B/s: window {window:.1}, BDP {product:.1}"
            );
        }
    }
}

#[test]
fn the_ceiling_bounds_a_larger_product() {
    let window = steady_window(Duration::from_millis(150), 1 << 30, 1 << 16, (4, 64), 4000);
    assert!((60.0..=64.0).contains(&window), "{window}");
}

/// A runtime whose clock only moves when every task waits.
fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

#[test]
fn the_window_holds_requests_until_slots_free_in_order() {
    paused().block_on(async {
        let window = Arc::new(Window::new((2, 2)));
        assert_eq!(window.status().limit, 2);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..5u64 {
            let (window, order) = (Arc::clone(&window), Arc::clone(&order));
            tasks.spawn(async move {
                window
                    .send(async {
                        order.lock().unwrap().push(n);
                        tokio::time::sleep(10 * MS).await;
                        Ok::<_, S3Error>(n)
                    })
                    .await
            });
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(MS).await;
        assert_eq!(window.status().in_flight, 2);
        while let Some(done) = tasks.join_next().await {
            done.unwrap().unwrap();
        }
        assert_eq!(*order.lock().unwrap(), [0, 1, 2, 3, 4]);
        let status = window.status();
        assert_eq!((status.in_flight, status.limit), (0, 2));
        assert_eq!(status.base_round_trip, Some(10 * MS));
    });
}

#[test]
fn shards_widen_the_bounds_and_a_narrower_window_waits_for_requests_to_end() {
    paused().block_on(async {
        let window = Arc::new(Window::new((4, 4)));
        assert_eq!(window.status().limit, 4);
        let first = window.join();
        let second = window.join();
        let status = window.status();
        assert_eq!((status.floor, status.ceiling, status.limit), (8, 8, 8));
        // Ten requests, each held until it is released.
        let release = Arc::new(Semaphore::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let (window, release) = (Arc::clone(&window), Arc::clone(&release));
            tasks.spawn(async move {
                window
                    .send(async {
                        release.acquire().await.unwrap().forget();
                        Ok::<_, S3Error>(())
                    })
                    .await
            });
        }
        let in_flight = |released: usize| {
            let (window, release) = (Arc::clone(&window), Arc::clone(&release));
            async move {
                release.add_permits(released);
                tokio::time::sleep(MS).await;
                window.status().in_flight
            }
        };
        assert_eq!(in_flight(0).await, 8);
        // One shard leaves: the window is four, below the eight in flight.
        drop(second);
        let status = window.status();
        assert_eq!((status.floor, status.ceiling, status.limit), (4, 4, 4));
        // The first four to end free nothing; the next frees a slot.
        assert_eq!(in_flight(2).await, 6);
        assert_eq!(in_flight(2).await, 4);
        // Then each that ends lets one of the two waiting go.
        assert_eq!(in_flight(1).await, 4);
        assert_eq!(in_flight(1).await, 4);
        assert_eq!(in_flight(1).await, 3);
        assert_eq!(in_flight(10).await, 0);
        while let Some(done) = tasks.join_next().await {
            done.unwrap().unwrap();
        }
        drop(first);
        // Without members, the window is sized for one shard.
        assert_eq!(window.status().floor, 4);
        assert!(format!("{window:?}").contains("Window"));
    });
}

#[test]
fn throttles_are_counted() {
    paused().block_on(async {
        let window = Window::new((1, 8));
        let counter = Counter::default();
        window.count_throttles(counter.clone());
        let slow_down = window
            .send(async { Err::<(), _>(S3Error::new(S3ErrorKind::SlowDown, "")) })
            .await;
        assert_eq!(slow_down.unwrap_err().kind(), S3ErrorKind::SlowDown);
        assert_eq!(counter.get(), 1);
        assert_eq!(window.status().in_flight, 0);
    });
}

#[test]
fn an_abandoned_request_frees_its_slot() {
    paused().block_on(async {
        let window = Window::new((1, 1));
        let abandoned =
            tokio::time::timeout(MS, window.send(std::future::pending::<S3Result<()>>())).await;
        assert!(abandoned.is_err());
        assert_eq!(window.status().in_flight, 0);
        window.send(async { Ok(()) }).await.unwrap();
        // No sample from the abandoned request: the base is the answer's.
        assert_eq!(window.status().base_round_trip, Some(Duration::ZERO));
    });
}
