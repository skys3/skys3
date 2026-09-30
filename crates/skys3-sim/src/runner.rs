//! The seeded simulation runner.
//!
//! A scenario is a function of a [`SimContext`], which carries one seed and
//! derives every random choice from it: `turmoil`'s network and scheduling,
//! each simulated disk's faults, each node's clock drift, and the scenario's
//! own decisions. [`Runner::run`] runs a scenario once per seed of a fixed
//! set. When a seed fails, it panics with the seed and the command that
//! replays exactly that seed.
//!
//! Environment variables:
//!
//! - `SKYS3_SIM_SEED=<seed>` runs only that seed, to replay a failure.
//! - `SKYS3_SIM_SEEDS=<count>` runs seeds `0..count` instead of the
//!   scenario's default count. CI's simulation job uses it for a larger fixed
//!   set.

use std::any::Any;
use std::env;
use std::fmt;
use std::ops::Range;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::thread;

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use skys3_io::{Drift, MonoTime, MonotonicClock, SimDisk, SimDiskFaults};

use crate::s3::{SimS3, SimS3Config};

/// The variable that selects a single seed to replay.
pub const SEED_ENV: &str = "SKYS3_SIM_SEED";

/// The variable that sets how many seeds to run.
pub const SEEDS_ENV: &str = "SKYS3_SIM_SEEDS";

/// The number of seeds a scenario runs by default.
pub const DEFAULT_SEED_COUNT: u64 = 8;

/// The seeds a runner runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeedSet {
    /// One seed, to replay a failure.
    One(u64),
    /// A fixed range of seeds.
    Range(Range<u64>),
}

impl SeedSet {
    /// Reads the seed set from [`SEED_ENV`] and [`SEEDS_ENV`], running seeds
    /// `0..default_count` if neither is set.
    ///
    /// # Errors
    ///
    /// Returns an error if a variable is set but is not a number.
    pub fn from_env(default_count: u64) -> Result<SeedSet, SeedSetError> {
        let seed = env::var(SEED_ENV).ok();
        let count = env::var(SEEDS_ENV).ok();
        Self::parse(seed.as_deref(), count.as_deref(), default_count)
    }

    /// Builds the seed set from the values of [`SEED_ENV`] and
    /// [`SEEDS_ENV`]. A single seed takes precedence over a count.
    ///
    /// # Errors
    ///
    /// Returns an error if a value is present but is not a number.
    pub fn parse(
        seed: Option<&str>,
        count: Option<&str>,
        default_count: u64,
    ) -> Result<SeedSet, SeedSetError> {
        let parse = |variable: &'static str, value: &str| {
            value.trim().parse::<u64>().map_err(|_| SeedSetError {
                variable,
                value: value.to_owned(),
            })
        };
        if let Some(seed) = seed {
            return Ok(SeedSet::One(parse(SEED_ENV, seed)?));
        }
        let count = match count {
            Some(count) => parse(SEEDS_ENV, count)?,
            None => default_count,
        };
        Ok(SeedSet::Range(0..count))
    }

    /// Returns the seeds in order.
    pub fn seeds(&self) -> Range<u64> {
        match *self {
            SeedSet::One(seed) => seed..seed + 1,
            SeedSet::Range(ref range) => range.clone(),
        }
    }
}

/// A seed variable that is not a number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedSetError {
    variable: &'static str,
    value: String,
}

impl fmt::Display for SeedSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} must be an unsigned integer, not {:?}",
            self.variable, self.value
        )
    }
}

impl std::error::Error for SeedSetError {}

/// Everything one run of a scenario derives from its seed.
///
/// Draw from the context in the same order on every run: each value comes
/// from one seeded generator, so the order of draws is part of the replay.
#[derive(Debug)]
pub struct SimContext {
    seed: u64,
    rng: SmallRng,
}

impl SimContext {
    /// Returns a context for `seed`.
    pub fn new(seed: u64) -> Self {
        SimContext {
            seed,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Returns the seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the scenario's random generator.
    pub fn rng(&mut self) -> &mut SmallRng {
        &mut self.rng
    }

    /// Returns a new seed for a component, such as a node's own generator.
    pub fn fork_seed(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Returns a `turmoil` builder whose network and scheduling randomness is
    /// seeded from this context.
    pub fn builder(&mut self) -> turmoil::Builder {
        let mut builder = turmoil::Builder::new();
        builder.rng_seed(self.fork_seed());
        builder
    }

    /// Returns a simulated disk without faults, seeded from this context.
    pub fn disk(&mut self) -> SimDisk {
        self.disk_with_faults(SimDiskFaults::default())
    }

    /// Returns a simulated disk that injects `faults`, seeded from this
    /// context.
    pub fn disk_with_faults(&mut self, faults: SimDiskFaults) -> SimDisk {
        SimDisk::with_faults(self.fork_seed(), faults)
    }

    /// Returns an empty simulated S3 bucket whose faults are seeded from
    /// this context.
    pub fn s3(&mut self, config: SimS3Config) -> SimS3 {
        SimS3::new(self.fork_seed(), config)
    }

    /// Returns the clock of a new node: a drift drawn uniformly within
    /// `±bound` (the design's `ρ`), and a random origin so that readings
    /// differ between nodes.
    pub fn node_clock(&mut self, bound: Drift) -> NodeClock {
        NodeClock {
            drift: Drift::random_within(&mut self.rng, bound),
            // Up to about 11.6 days, far from both ends of the range.
            start: MonoTime::from_nanos(self.rng.random_range(0..1 << 50)),
        }
    }
}

/// The parameters of one node's clock in a simulation.
///
/// Create the clock with [`NodeClock::start`] inside the node's host
/// software, so it follows that host's simulated time. A restarted node
/// starts a new clock, as a restarted process would.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeClock {
    /// The clock's rate drift.
    pub drift: Drift,
    /// The clock's reading when it starts.
    pub start: MonoTime,
}

impl NodeClock {
    /// Starts the clock on the current runtime.
    pub fn start(&self) -> MonotonicClock {
        MonotonicClock::drifting(self.drift, self.start)
    }
}

/// Runs a scenario once per seed and reports the first failing seed with
/// its replay command.
#[derive(Clone, Debug)]
pub struct Runner {
    seeds: SeedSet,
}

impl Runner {
    /// Returns a runner for the seeds from the environment, or seeds
    /// `0..DEFAULT_SEED_COUNT`.
    ///
    /// # Panics
    ///
    /// Panics if a seed variable is set but is not a number.
    pub fn new() -> Self {
        Self::with_default_seed_count(DEFAULT_SEED_COUNT)
    }

    /// Returns a runner for the seeds from the environment, or seeds
    /// `0..count`, for scenarios that are slower or faster than most.
    ///
    /// # Panics
    ///
    /// Panics if a seed variable is set but is not a number.
    pub fn with_default_seed_count(count: u64) -> Self {
        match SeedSet::from_env(count) {
            Ok(seeds) => Runner { seeds },
            Err(error) => panic!("{error}"),
        }
    }

    /// Returns a runner for exactly `seeds`, ignoring the environment.
    pub fn with_seeds(seeds: SeedSet) -> Self {
        Runner { seeds }
    }

    /// Returns the seeds this runner runs.
    pub fn seeds(&self) -> &SeedSet {
        &self.seeds
    }

    /// Runs `scenario` once per seed, in order, stopping at the first seed
    /// that returns an error or panics.
    ///
    /// # Panics
    ///
    /// Panics if a seed fails, with the seed, the failure, and the command
    /// that replays that seed.
    pub fn run<F>(&self, mut scenario: F)
    where
        F: FnMut(&mut SimContext) -> turmoil::Result,
    {
        for seed in self.seeds.seeds() {
            let mut context = SimContext::new(seed);
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| scenario(&mut context)));
            let failure = match outcome {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => error.to_string(),
                Err(payload) => format!("panicked: {}", panic_message(&*payload)),
            };
            panic!(
                "simulation failed with seed {seed}: {failure}\nreplay with: {}",
                replay_command(seed)
            );
        }
    }
}

impl Default for Runner {
    fn default() -> Self {
        Self::new()
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "non-string panic payload"
    }
}

/// Returns the shell command that reruns the current test with only `seed`.
///
/// Cargo sets `CARGO_PKG_NAME` for the test process, the test binary's name
/// gives the target, and the test harness names each test's thread after
/// the test. Parts that cannot be found are left out, which widens the
/// command but still replays the seed.
pub fn replay_command(seed: u64) -> String {
    let package = env::var("CARGO_PKG_NAME").ok();
    let exe = env::current_exe().ok();
    let target = exe
        .as_deref()
        .and_then(|exe| test_target(exe, package.as_deref()));
    let test = thread::current()
        .name()
        .filter(|name| *name != "main")
        .map(str::to_owned);

    let mut command = format!("{SEED_ENV}={seed} cargo test");
    if let Some(package) = &package {
        command.push_str(&format!(" -p {package}"));
    }
    if let Some(target) = &target {
        command.push(' ');
        command.push_str(target);
    }
    if let Some(test) = &test {
        command.push_str(&format!(" -- {test} --exact"));
    }
    command
}

/// Returns the Cargo arguments that select the test target built as `exe`:
/// `--lib` for the package's unit tests, `--test <name>` for an integration
/// test.
fn test_target(exe: &Path, package: Option<&str>) -> Option<String> {
    let stem = exe.file_stem()?.to_str()?;
    // Cargo names test binaries `<target>-<16 hex digits>`.
    let (name, hash) = stem.rsplit_once('-')?;
    if hash.len() != 16 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let is_lib = package.is_some_and(|package| package.replace('-', "_") == name);
    Some(if is_lib {
        "--lib".to_owned()
    } else {
        format!("--test {name}")
    })
}

#[cfg(test)]
mod tests {
    use skys3_io::Clock;
    use std::cell::RefCell;
    use std::time::Duration;

    use super::*;

    #[test]
    fn seed_set_from_variables() {
        assert_eq!(SeedSet::parse(None, None, 8), Ok(SeedSet::Range(0..8)));
        assert_eq!(
            SeedSet::parse(None, Some("256"), 8),
            Ok(SeedSet::Range(0..256))
        );
        assert_eq!(
            SeedSet::parse(Some(" 42 "), Some("256"), 8),
            Ok(SeedSet::One(42))
        );
        assert_eq!(SeedSet::One(42).seeds(), 42..43);
        assert_eq!(SeedSet::Range(3..5).seeds(), 3..5);

        let error = SeedSet::parse(Some("forty-two"), None, 8).unwrap_err();
        assert_eq!(
            error.to_string(),
            "SKYS3_SIM_SEED must be an unsigned integer, not \"forty-two\""
        );
        let error = SeedSet::parse(None, Some("-1"), 8).unwrap_err();
        assert_eq!(
            error.to_string(),
            "SKYS3_SIM_SEEDS must be an unsigned integer, not \"-1\""
        );
    }

    #[test]
    fn runner_reads_the_environment() {
        // The tests run without the seed variables unless a developer sets
        // them, in which case the runner must honor them.
        let expected = SeedSet::from_env(3).unwrap();
        assert_eq!(Runner::with_default_seed_count(3).seeds(), &expected);
        let expected = SeedSet::from_env(DEFAULT_SEED_COUNT).unwrap();
        assert_eq!(Runner::default().seeds(), &expected);
    }

    #[test]
    fn runs_every_seed_in_order() {
        let seen = RefCell::new(Vec::new());
        Runner::with_seeds(SeedSet::Range(0..5)).run(|context| {
            seen.borrow_mut().push(context.seed());
            Ok(())
        });
        assert_eq!(seen.into_inner(), [0, 1, 2, 3, 4]);
    }

    #[test]
    #[should_panic(expected = "simulation failed with seed 3: boom")]
    fn reports_the_first_failing_seed() {
        Runner::with_seeds(SeedSet::Range(0..10)).run(|context| {
            if context.seed() == 3 {
                Err("boom".into())
            } else {
                Ok(())
            }
        });
    }

    #[test]
    fn failure_report_carries_the_replay_command() {
        let payload = panic::catch_unwind(|| {
            Runner::with_seeds(SeedSet::One(17)).run(|_| panic!("invariant violated"));
        })
        .unwrap_err();
        let message = panic_message(&*payload);
        assert!(
            message.starts_with("simulation failed with seed 17: panicked: invariant violated\n"),
            "{message}"
        );
        assert!(
            message.ends_with(
                "replay with: SKYS3_SIM_SEED=17 cargo test -p skys3-sim --lib -- \
                 runner::tests::failure_report_carries_the_replay_command --exact"
            ),
            "{message}"
        );
    }

    #[test]
    fn panic_messages() {
        assert_eq!(panic_message(&"static"), "static");
        assert_eq!(panic_message(&String::from("owned")), "owned");
        assert_eq!(panic_message(&7_u32), "non-string panic payload");
    }

    #[test]
    fn test_targets_from_binary_names() {
        let target = |exe: &str| test_target(Path::new(exe), Some("skys3-sim"));
        assert_eq!(
            target("/t/deps/skys3_sim-0123456789abcdef").as_deref(),
            Some("--lib")
        );
        assert_eq!(
            target("/t/deps/simulation-0123456789abcdef").as_deref(),
            Some("--test simulation")
        );
        assert_eq!(target("/t/deps/simulation"), None);
        assert_eq!(target("/t/deps/simulation-xyz"), None);
        assert_eq!(target("/t/deps/simulation-0123456789abcdeg"), None);
        assert_eq!(
            test_target(Path::new("/t/x-0123456789abcdef"), None).as_deref(),
            Some("--test x")
        );
    }

    #[test]
    fn context_draws_are_replayable() {
        let draw = |seed| {
            let mut context = SimContext::new(seed);
            let bound = Drift::from_ppm(10_000).unwrap();
            let clock = context.node_clock(bound);
            let disk_seed = context.fork_seed();
            let value: u32 = context.rng().random();
            (clock, disk_seed, value)
        };
        assert_eq!(draw(5), draw(5));
        assert_ne!(draw(5), draw(6));
        let (clock, _, _) = draw(5);
        assert!(clock.drift.ppm().abs() <= 10_000);
        assert!(clock.start.as_nanos() < 1 << 50);
    }

    #[test]
    fn context_builds_seeded_components() {
        let mut context = SimContext::new(9);
        assert_eq!(context.seed(), 9);
        let disk = context.disk();
        assert_eq!(disk.faults(), SimDiskFaults::default());
        let faults = SimDiskFaults {
            capacity: Some(1),
            ..SimDiskFaults::default()
        };
        assert_eq!(context.disk_with_faults(faults.clone()).faults(), faults);
        let store = context.s3(SimS3Config::default());
        assert_eq!(store.config(), &SimS3Config::default());

        let mut sim = context.builder().build();
        let node = context.node_clock(Drift::NONE);
        sim.client("client", async move {
            let clock = node.start();
            assert_eq!(clock.now(), node.start);
            clock.sleep(Duration::from_millis(5)).await;
            assert_eq!(clock.now() - node.start, Duration::from_millis(5));
            Ok(())
        });
        sim.run().unwrap();
    }
}
