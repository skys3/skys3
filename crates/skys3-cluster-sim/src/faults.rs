//! Faults and their schedule.
//!
//! A [`FaultPlan`] lists faults by the simulated time at which they start.
//! Most last for a while and then heal on their own; the driver also heals
//! everything still in force once the workload ends, so the final checks
//! run against a healthy cluster. [`FaultPlan::random`] draws a plan from a
//! [`FaultProfile`] and the seed.

use std::time::Duration;

use rand::Rng;
use rand::rngs::SmallRng;

/// One end of a network link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Endpoint {
    /// The node at this position, from 0.
    Node(usize),
    /// The client at this position, from 0.
    Client(usize),
}

/// A fault the harness injects.
#[derive(Clone, Debug, PartialEq)]
pub enum Fault {
    /// The node's process dies, and with `power_loss` its host loses power
    /// too, dropping every unsynced write. The node restarts after
    /// `downtime`.
    Crash {
        /// The node.
        node: usize,
        /// Whether unsynced writes are lost.
        power_loss: bool,
        /// How long the node stays down.
        downtime: Duration,
    },
    /// No message passes between `a` and `b`; in-flight ones are lost.
    Partition {
        /// One end.
        a: Endpoint,
        /// The other end.
        b: Endpoint,
        /// How long the partition lasts.
        duration: Duration,
    },
    /// Messages between `a` and `b` are held, then delivered together
    /// when the fault heals: delay, and reordering against other links.
    Hold {
        /// One end.
        a: Endpoint,
        /// The other end.
        b: Endpoint,
        /// How long messages are held.
        duration: Duration,
    },
    /// Each link fails at random with probability `rate` per step and is
    /// repaired at random, losing the messages in flight on it.
    MessageLoss {
        /// The per-step failure probability of each link.
        rate: f64,
        /// How long the losses go on.
        duration: Duration,
    },
    /// The next sync of the node's disk fails, losing the bytes it
    /// covered, and takes the disk out of service (§10.4). The node is
    /// later restarted with a power loss, which a disk out of service
    /// needs before it is used again.
    FailSync {
        /// The node.
        node: usize,
        /// The disk, by position on the node.
        disk: usize,
    },
    /// The control store answers no request from any node.
    ControlOutage {
        /// How long the outage lasts.
        duration: Duration,
    },
    /// Every control-store request takes between `min` and four times
    /// `min` before it is applied, and as long again before it is
    /// answered.
    ControlLatency {
        /// The shortest delay of each half of a round trip.
        min: Duration,
        /// How long the latency lasts.
        duration: Duration,
    },
    /// Control-store writes, compare-and-swaps included, are applied but
    /// their answers are lost with this probability.
    LostCasResponses {
        /// The probability of losing each answer.
        probability: f64,
        /// How long the losses go on.
        duration: Duration,
    },
    /// Not a fault: a node held back from the start
    /// ([`ClusterConfig::joining`](crate::ClusterConfig::joining)) starts
    /// for the first time, as a newly provisioned node with valid
    /// credentials and empty disks does.
    Join {
        /// The node.
        node: usize,
    },
}

impl Fault {
    /// How long the fault stays in force, if it heals on its own.
    #[must_use]
    pub fn duration(&self) -> Option<Duration> {
        match self {
            Fault::Crash { downtime, .. } => Some(*downtime),
            Fault::Partition { duration, .. }
            | Fault::Hold { duration, .. }
            | Fault::MessageLoss { duration, .. }
            | Fault::ControlOutage { duration }
            | Fault::ControlLatency { duration, .. }
            | Fault::LostCasResponses { duration, .. } => Some(*duration),
            Fault::FailSync { .. } | Fault::Join { .. } => None,
        }
    }
}

/// A fault and when it starts, as simulated time since the simulation
/// began.
#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledFault {
    /// When the fault starts.
    pub at: Duration,
    /// The fault.
    pub fault: Fault,
}

/// The faults of one run, in the order they start.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FaultPlan {
    faults: Vec<ScheduledFault>,
}

/// How many faults of each kind a random plan draws, over how long, and
/// how severe they are. Each count is a maximum; a plan draws between zero
/// and it.
#[derive(Clone, Debug, PartialEq)]
pub struct FaultProfile {
    /// Faults start between this time and `end`, after the cluster is up.
    pub start: Duration,
    /// The latest start of a fault.
    pub end: Duration,
    /// Node crashes.
    pub crashes: usize,
    /// Partitions and holds.
    pub partitions: usize,
    /// Windows of random message loss.
    pub message_loss: usize,
    /// Failed syncs.
    pub sync_failures: usize,
    /// Control-store outages, latency windows, and windows of lost CAS
    /// answers.
    pub control: usize,
    /// The longest duration of a fault.
    pub max_duration: Duration,
}

impl Default for FaultProfile {
    /// A few of every fault within the first ten simulated seconds.
    fn default() -> Self {
        Self {
            start: Duration::from_millis(500),
            end: Duration::from_secs(10),
            crashes: 2,
            partitions: 3,
            message_loss: 1,
            sync_failures: 1,
            control: 3,
            max_duration: Duration::from_secs(2),
        }
    }
}

impl FaultPlan {
    /// A plan without faults.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Adds `fault`, starting at `at`.
    #[must_use]
    pub fn with(mut self, at: Duration, fault: Fault) -> Self {
        self.push(at, fault);
        self
    }

    /// Adds `fault`, starting at `at`.
    pub fn push(&mut self, at: Duration, fault: Fault) {
        let position = self.faults.partition_point(|scheduled| scheduled.at <= at);
        self.faults.insert(position, ScheduledFault { at, fault });
    }

    /// The faults, in the order they start.
    #[must_use]
    pub fn faults(&self) -> &[ScheduledFault] {
        &self.faults
    }

    /// Draws a plan for a cluster of `nodes` nodes, each with `disks`
    /// disks, and `clients` clients.
    #[must_use]
    pub fn random(
        rng: &mut SmallRng,
        profile: &FaultProfile,
        nodes: usize,
        disks: usize,
        clients: usize,
    ) -> Self {
        let mut plan = Self::none();
        let at = |rng: &mut SmallRng| rng.random_range(profile.start..=profile.end);
        let lasting =
            |rng: &mut SmallRng| rng.random_range(Duration::from_millis(50)..=profile.max_duration);
        let endpoint = |rng: &mut SmallRng| {
            if clients > 0 && rng.random_bool(0.3) {
                Endpoint::Client(rng.random_range(0..clients))
            } else {
                Endpoint::Node(rng.random_range(0..nodes))
            }
        };
        for _ in 0..rng.random_range(0..=profile.crashes) {
            let fault = Fault::Crash {
                node: rng.random_range(0..nodes),
                power_loss: rng.random_bool(0.5),
                downtime: lasting(rng),
            };
            plan.push(at(rng), fault);
        }
        for _ in 0..rng.random_range(0..=profile.partitions) {
            let node = rng.random_range(0..nodes);
            let b = match endpoint(rng) {
                // Another node, or a client if there is no other node.
                Endpoint::Node(other) if other == node && nodes > 1 => {
                    Endpoint::Node((node + 1) % nodes)
                }
                Endpoint::Node(other) if other == node && clients > 0 => Endpoint::Client(0),
                Endpoint::Node(other) if other == node => continue,
                b => b,
            };
            let a = Endpoint::Node(node);
            let duration = lasting(rng);
            let fault = if rng.random_bool(0.5) {
                Fault::Partition { a, b, duration }
            } else {
                Fault::Hold { a, b, duration }
            };
            plan.push(at(rng), fault);
        }
        for _ in 0..rng.random_range(0..=profile.message_loss) {
            let rate = rng.random_range(0.001..0.02);
            let fault = Fault::MessageLoss {
                rate,
                duration: lasting(rng),
            };
            plan.push(at(rng), fault);
        }
        for _ in 0..rng.random_range(0..=profile.sync_failures) {
            let fault = Fault::FailSync {
                node: rng.random_range(0..nodes),
                disk: rng.random_range(0..disks.max(1)),
            };
            plan.push(at(rng), fault);
        }
        for _ in 0..rng.random_range(0..=profile.control) {
            let duration = lasting(rng);
            let fault = match rng.random_range(0..3) {
                0 => Fault::ControlOutage { duration },
                1 => Fault::ControlLatency {
                    min: rng.random_range(Duration::from_millis(100)..=Duration::from_millis(400)),
                    duration,
                },
                _ => Fault::LostCasResponses {
                    probability: rng.random_range(0.2..0.8),
                    duration,
                },
            };
            plan.push(at(rng), fault);
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn plans_keep_faults_in_start_order() {
        let crash = |node| Fault::Crash {
            node,
            power_loss: false,
            downtime: Duration::from_secs(1),
        };
        let plan = FaultPlan::none()
            .with(Duration::from_secs(3), crash(0))
            .with(Duration::from_secs(1), crash(1))
            .with(Duration::from_secs(3), crash(2));
        let order: Vec<_> = plan.faults().iter().map(|f| f.fault.clone()).collect();
        assert_eq!(order, [crash(1), crash(0), crash(2)]);
        assert_eq!(crash(0).duration(), Some(Duration::from_secs(1)));
        assert_eq!(Fault::FailSync { node: 0, disk: 0 }.duration(), None);
        assert_eq!(Fault::Join { node: 3 }.duration(), None);
        assert_eq!(FaultPlan::none().faults(), []);
    }

    #[test]
    fn random_plans_stay_within_the_profile() {
        let profile = FaultProfile::default();
        let mut kinds = [0; 7];
        for seed in 0..64 {
            let plan = FaultPlan::random(&mut SmallRng::seed_from_u64(seed), &profile, 3, 2, 4);
            let again = FaultPlan::random(&mut SmallRng::seed_from_u64(seed), &profile, 3, 2, 4);
            assert_eq!(plan, again);
            for scheduled in plan.faults() {
                assert!((profile.start..=profile.end).contains(&scheduled.at));
                if let Some(duration) = scheduled.fault.duration() {
                    assert!(duration <= profile.max_duration);
                }
                let kind = match &scheduled.fault {
                    Fault::Crash { node, .. } => {
                        assert!(*node < 3);
                        0
                    }
                    Fault::Partition { a, b, .. } | Fault::Hold { a, b, .. } => {
                        assert_ne!(a, b);
                        assert!(matches!(a, Endpoint::Node(n) if *n < 3));
                        1
                    }
                    Fault::MessageLoss { rate, .. } => {
                        assert!(*rate < 0.02);
                        2
                    }
                    Fault::FailSync { node, disk } => {
                        assert!(*node < 3 && *disk < 2);
                        3
                    }
                    Fault::ControlOutage { .. } => 4,
                    Fault::ControlLatency { min, .. } => {
                        assert!(*min >= Duration::from_millis(100));
                        5
                    }
                    Fault::LostCasResponses { probability, .. } => {
                        assert!((0.2..0.8).contains(probability));
                        6
                    }
                    Fault::Join { .. } => panic!("a random plan joins no node"),
                };
                kinds[kind] += 1;
            }
        }
        assert!(kinds.iter().all(|&count| count > 0), "{kinds:?}");

        // One node and no clients leave nothing to partition.
        let profile = FaultProfile {
            partitions: 8,
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(&mut SmallRng::seed_from_u64(1), &profile, 1, 1, 0);
        assert!(
            plan.faults()
                .iter()
                .all(|f| !matches!(f.fault, Fault::Partition { .. } | Fault::Hold { .. }))
        );
    }
}
