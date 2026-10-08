//! Faults a [`SimS3`](super::SimS3) injects: random ones drawn from its
//! seeded generator, and scripted ones queued by a test.

use std::collections::VecDeque;
use std::time::Duration;

use rand::Rng;
use rand::rngs::SmallRng;

/// An S3 operation, to direct a scripted fault at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Operation {
    /// `PutObject`.
    PutObject,
    /// `GetObject`.
    GetObject,
    /// `HeadObject`.
    HeadObject,
    /// `DeleteObject`.
    DeleteObject,
    /// `ListObjectsV2`.
    ListObjectsV2,
    /// `CopyObject`.
    CopyObject,
    /// `CreateMultipartUpload`.
    CreateMultipartUpload,
    /// `UploadPart`.
    UploadPart,
    /// `CompleteMultipartUpload`.
    CompleteMultipartUpload,
    /// `AbortMultipartUpload`.
    AbortMultipartUpload,
    /// `ListParts`.
    ListParts,
}

/// One injected fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fault {
    /// The request takes this long to reach the point where the store
    /// applies it. A conditional write is in progress for that time, so
    /// another write to its key can make it fail with `409`.
    Delay(Duration),
    /// `500 InternalError` without applying the request.
    InternalError,
    /// `503 SlowDown` without applying the request.
    SlowDown,
    /// The request is lost on the way: it is not applied, and the caller
    /// gets [`S3ErrorKind::Timeout`](skys3_remote::S3ErrorKind::Timeout).
    LostRequest,
    /// The response is lost: the request is applied, but the caller gets
    /// [`S3ErrorKind::Timeout`](skys3_remote::S3ErrorKind::Timeout) or
    /// `500 InternalError` instead of its result.
    LostResponse,
    /// `409 ConditionalRequestConflict` without applying the request, as if
    /// a concurrent write had raced it.
    Conflict,
    /// A `GetObject` or `HeadObject` of a key's current object answers with
    /// what the key held before its latest write, or `404 NoSuchKey` if it
    /// held nothing, as an eventually consistent store may. A
    /// `ListObjectsV2` lists every key as it was before its latest write:
    /// a new key is missing, a deleted one is still listed, and an
    /// overwritten one has its old ETag. Reads of a specific version, keys
    /// never written, and other operations are unaffected.
    StaleRead,
}

/// Random faults a [`SimS3`](super::SimS3) injects into every request.
///
/// Each request draws one of the error faults with the given probabilities,
/// which must sum to at most 1, and independently two delays within
/// `min_delay..=max_delay`: one before the store applies the request and
/// one before the caller gets the response. A read is also stale with
/// `stale_read_probability`, and a listing with `stale_list_probability`,
/// independently of the rest. The default injects nothing and never
/// sleeps.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimS3Faults {
    /// The shortest delay of each leg of a request.
    pub min_delay: Duration,
    /// The longest delay of each leg of a request.
    pub max_delay: Duration,
    /// The probability of [`Fault::InternalError`].
    pub internal_error_probability: f64,
    /// The probability of [`Fault::SlowDown`].
    pub slow_down_probability: f64,
    /// The probability of [`Fault::LostRequest`].
    pub lost_request_probability: f64,
    /// The probability of [`Fault::LostResponse`].
    pub lost_response_probability: f64,
    /// The probability that a `GetObject` or `HeadObject` is
    /// [`Fault::StaleRead`], drawn apart from the error faults: a store
    /// without read-after-write consistency.
    pub stale_read_probability: f64,
    /// The probability that a `ListObjectsV2` is [`Fault::StaleRead`]: a
    /// store whose listings lag its writes.
    pub stale_list_probability: f64,
}

impl SimS3Faults {
    /// No faults: every request is answered at once.
    pub const NONE: SimS3Faults = SimS3Faults {
        min_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        internal_error_probability: 0.0,
        slow_down_probability: 0.0,
        lost_request_probability: 0.0,
        lost_response_probability: 0.0,
        stale_read_probability: 0.0,
        stale_list_probability: 0.0,
    };

    /// A store that is down: every request fails with `500 InternalError`.
    pub const OUTAGE: SimS3Faults = SimS3Faults {
        internal_error_probability: 1.0,
        ..SimS3Faults::NONE
    };

    /// Checks that the probabilities are valid and the delays ordered.
    ///
    /// # Panics
    ///
    /// Panics if a probability is outside `0..=1`, the error faults'
    /// probabilities sum to more than 1, or `min_delay > max_delay`.
    pub(super) fn validate(&self) {
        let probabilities = [
            self.internal_error_probability,
            self.slow_down_probability,
            self.lost_request_probability,
            self.lost_response_probability,
        ];
        assert!(
            probabilities.iter().all(|p| (0.0..=1.0).contains(p))
                && probabilities.iter().sum::<f64>() <= 1.0 + f64::EPSILON
                && (0.0..=1.0).contains(&self.stale_read_probability)
                && (0.0..=1.0).contains(&self.stale_list_probability),
            "fault probabilities must be within 0..=1 and sum to at most 1: {self:?}"
        );
        assert!(
            self.min_delay <= self.max_delay,
            "min_delay must not exceed max_delay: {self:?}"
        );
    }
}

/// What happens to one request: its delays, and at most one error fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Plan {
    pub(super) request_delay: Duration,
    pub(super) response_delay: Duration,
    /// Not [`Fault::Delay`] or [`Fault::StaleRead`], which set
    /// `request_delay` and `stale` instead.
    pub(super) fault: Option<Fault>,
    /// Whether a read of a current object is answered from before the
    /// key's latest write.
    pub(super) stale: bool,
    /// For [`Fault::LostResponse`]: whether the caller sees a timeout rather
    /// than a server error.
    pub(super) lost_as_timeout: bool,
}

/// The fault state of a store: the random profile, the profiles of single
/// operations, the queue of scripted faults, and the seeded generator.
#[derive(Debug)]
pub(super) struct Injector {
    pub(super) faults: SimS3Faults,
    /// Profiles that replace `faults` for one operation each.
    operations: Vec<(Operation, SimS3Faults)>,
    scripted: VecDeque<(Operation, Fault)>,
    rng: SmallRng,
}

impl Injector {
    pub(super) fn new(rng: SmallRng) -> Self {
        Injector {
            faults: SimS3Faults::NONE,
            operations: Vec::new(),
            scripted: VecDeque::new(),
            rng,
        }
    }

    pub(super) fn script(&mut self, operation: Operation, fault: Fault) {
        self.scripted.push_back((operation, fault));
    }

    /// Draws the random faults of `operation` from `faults`, or from the
    /// store's profile again with `None`.
    pub(super) fn set_operation(&mut self, operation: Operation, faults: Option<SimS3Faults>) {
        self.operations.retain(|(op, _)| *op != operation);
        if let Some(faults) = faults {
            self.operations.push((operation, faults));
        }
    }

    /// Plans one request of `operation`. The first scripted fault for the
    /// operation, if any, replaces the random error fault.
    pub(super) fn plan(&mut self, operation: Operation) -> Plan {
        let faults = self
            .operations
            .iter()
            .find(|(op, _)| *op == operation)
            .map_or(&self.faults, |(_, faults)| faults);
        let delay = |rng: &mut SmallRng| {
            if faults.max_delay.is_zero() {
                Duration::ZERO
            } else {
                rng.random_range(faults.min_delay..=faults.max_delay)
            }
        };
        let mut plan = Plan {
            request_delay: delay(&mut self.rng),
            response_delay: delay(&mut self.rng),
            fault: None,
            stale: false,
            lost_as_timeout: self.rng.random_bool(0.5),
        };
        let draw: f64 = self.rng.random();
        let random = [
            (faults.internal_error_probability, Fault::InternalError),
            (faults.slow_down_probability, Fault::SlowDown),
            (faults.lost_request_probability, Fault::LostRequest),
            (faults.lost_response_probability, Fault::LostResponse),
        ]
        .into_iter()
        .scan(0.0, |cumulative, (probability, fault)| {
            *cumulative += probability;
            Some((*cumulative, fault))
        })
        .find(|&(cumulative, _)| draw < cumulative)
        .map(|(_, fault)| fault);
        plan.fault = random;
        // Drawn only when enabled, so profiles without stale reads keep the
        // generator's sequence, and the seeds recorded for them, unchanged.
        let stale = match operation {
            Operation::GetObject | Operation::HeadObject => faults.stale_read_probability,
            Operation::ListObjectsV2 => faults.stale_list_probability,
            _ => 0.0,
        };
        if stale > 0.0 {
            plan.stale = self.rng.random_bool(stale);
        }

        if let Some(index) = self.scripted.iter().position(|&(op, _)| op == operation) {
            match self.scripted.remove(index).map(|(_, fault)| fault) {
                Some(Fault::Delay(delay)) => {
                    plan.request_delay = delay;
                    plan.fault = None;
                }
                Some(Fault::StaleRead) => {
                    plan.stale = true;
                    plan.fault = None;
                }
                scripted => plan.fault = scripted,
            }
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    fn injector(faults: SimS3Faults) -> Injector {
        let mut injector = Injector::new(SmallRng::seed_from_u64(1));
        injector.faults = faults;
        injector
    }

    #[test]
    fn no_faults_plan_nothing() {
        let mut injector = injector(SimS3Faults::NONE);
        for _ in 0..100 {
            let plan = injector.plan(Operation::PutObject);
            assert_eq!(plan.fault, None);
            assert!(plan.request_delay.is_zero() && plan.response_delay.is_zero());
        }
    }

    #[test]
    fn random_faults_follow_their_probabilities() {
        let mut injector = injector(SimS3Faults {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(3),
            internal_error_probability: 0.1,
            slow_down_probability: 0.2,
            lost_request_probability: 0.3,
            lost_response_probability: 0.4,
            stale_read_probability: 0.0,
            stale_list_probability: 0.0,
        });
        let mut counts = [0_u32; 4];
        for _ in 0..10_000 {
            let plan = injector.plan(Operation::GetObject);
            for delay in [plan.request_delay, plan.response_delay] {
                assert!((Duration::from_millis(1)..=Duration::from_millis(3)).contains(&delay));
            }
            let index = match plan.fault {
                Some(Fault::InternalError) => 0,
                Some(Fault::SlowDown) => 1,
                Some(Fault::LostRequest) => 2,
                Some(Fault::LostResponse) => 3,
                other => panic!("unexpected fault {other:?}"),
            };
            counts[index] += 1;
        }
        for (count, expected) in counts.into_iter().zip([1_000, 2_000, 3_000, 4_000]) {
            assert!(count.abs_diff(expected) < 300, "{counts:?}");
        }
    }

    #[test]
    fn scripted_faults_target_their_operation_in_order() {
        let mut injector = injector(SimS3Faults::NONE);
        injector.script(Operation::PutObject, Fault::SlowDown);
        injector.script(Operation::DeleteObject, Fault::Conflict);
        injector.script(Operation::PutObject, Fault::Delay(Duration::from_secs(1)));
        assert_eq!(injector.plan(Operation::GetObject).fault, None);
        assert_eq!(
            injector.plan(Operation::PutObject).fault,
            Some(Fault::SlowDown)
        );
        let delayed = injector.plan(Operation::PutObject);
        assert_eq!(delayed.fault, None);
        assert_eq!(delayed.request_delay, Duration::from_secs(1));
        assert_eq!(injector.plan(Operation::PutObject).fault, None);
        assert_eq!(
            injector.plan(Operation::DeleteObject).fault,
            Some(Fault::Conflict)
        );
    }

    #[test]
    fn stale_reads_are_drawn_apart_from_errors() {
        let mut random = injector(SimS3Faults {
            lost_response_probability: 1.0,
            stale_read_probability: 0.5,
            ..SimS3Faults::NONE
        });
        let plans: Vec<_> = (0..1_000)
            .map(|_| random.plan(Operation::GetObject))
            .collect();
        assert!(plans.iter().all(|p| p.fault == Some(Fault::LostResponse)));
        let stale = plans.iter().filter(|p| p.stale).count();
        assert!(stale.abs_diff(500) < 100, "{stale}");

        // Listings and other operations draw from their own probability.
        let mut lists = injector(SimS3Faults {
            stale_read_probability: 1.0,
            ..SimS3Faults::NONE
        });
        assert!(!lists.plan(Operation::ListObjectsV2).stale);
        assert!(!lists.plan(Operation::PutObject).stale);
        let mut lists = injector(SimS3Faults {
            stale_list_probability: 1.0,
            ..SimS3Faults::NONE
        });
        assert!(lists.plan(Operation::ListObjectsV2).stale);
        assert!(!lists.plan(Operation::HeadObject).stale);

        let mut scripted = injector(SimS3Faults::NONE);
        scripted.script(Operation::GetObject, Fault::StaleRead);
        let plan = scripted.plan(Operation::GetObject);
        assert!(plan.stale && plan.fault.is_none());
        assert!(!scripted.plan(Operation::GetObject).stale);
    }

    #[test]
    #[should_panic(expected = "fault probabilities")]
    fn stale_read_probability_must_be_a_probability() {
        SimS3Faults {
            stale_read_probability: 1.5,
            ..SimS3Faults::NONE
        }
        .validate();
    }

    #[test]
    fn an_operation_can_have_faults_of_its_own() {
        let mut injector = injector(SimS3Faults::NONE);
        let lossy = SimS3Faults {
            lost_response_probability: 1.0,
            ..SimS3Faults::NONE
        };
        injector.set_operation(Operation::CompleteMultipartUpload, Some(lossy.clone()));
        injector.set_operation(Operation::UploadPart, Some(SimS3Faults::OUTAGE));
        // A second profile for an operation replaces the first.
        injector.set_operation(Operation::UploadPart, Some(lossy));
        for _ in 0..20 {
            let complete = injector.plan(Operation::CompleteMultipartUpload);
            assert_eq!(complete.fault, Some(Fault::LostResponse));
            let part = injector.plan(Operation::UploadPart);
            assert_eq!(part.fault, Some(Fault::LostResponse));
            assert_eq!(injector.plan(Operation::PutObject).fault, None);
        }
        // Scripted faults still come first.
        injector.script(Operation::CompleteMultipartUpload, Fault::SlowDown);
        let scripted = injector.plan(Operation::CompleteMultipartUpload);
        assert_eq!(scripted.fault, Some(Fault::SlowDown));
        injector.set_operation(Operation::CompleteMultipartUpload, None);
        assert_eq!(
            injector.plan(Operation::CompleteMultipartUpload).fault,
            None
        );
    }

    #[test]
    fn outage_fails_everything() {
        let mut injector = injector(SimS3Faults::OUTAGE);
        for _ in 0..100 {
            assert_eq!(
                injector.plan(Operation::ListParts).fault,
                Some(Fault::InternalError)
            );
        }
    }

    #[test]
    #[should_panic(expected = "sum to at most 1")]
    fn probabilities_must_sum_to_at_most_one() {
        SimS3Faults {
            slow_down_probability: 0.6,
            lost_response_probability: 0.6,
            ..SimS3Faults::NONE
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "min_delay must not exceed max_delay")]
    fn delays_must_be_ordered() {
        SimS3Faults {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(1),
            ..SimS3Faults::NONE
        }
        .validate();
    }
}
