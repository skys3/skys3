//! Tokio task IDs, which decide the order a dropped runtime drops its
//! tasks in, and so are part of a seed's replay.
//!
//! `turmoil` runs each host on a current-thread Tokio runtime, and crashes
//! or bounces a host by dropping that runtime. Tokio keeps a runtime's live
//! tasks in a sharded list: a current-thread runtime's is
//! `OwnedTasks::new(1)`, which has `1.next_power_of_two() * 4` = 4 lists,
//! and a task goes in list `id & 3`. Dropping the runtime shuts its tasks
//! down list by list, oldest first within a list. Each dropped task drops
//! its connections and channels, which wakes the tasks of other hosts in
//! that order. Task IDs come from one counter for the whole process, so
//! without care the path a crash takes depends on how many tasks the
//! process spawned before the seed ran: a seed that passes alone can fail
//! after other tests, and the printed replay command does not reproduce it.
//!
//! [`align`] moves the counter so that the next task ID falls in a chosen
//! list. [`Runner::run`](crate::Runner::run) aligns before each seed, and
//! [`SimContext::builder`](crate::SimContext::builder) before each
//! simulation, to list [`task_shift`]`(seed)`: `seed % 4`, so consecutive
//! seeds cover all four orders, or [`TASK_SHIFT_ENV`] if it is set. That
//! holds only while no other thread of the process spawns tasks during the
//! run, so CI runs each simulation test in its own process
//! (`scripts/ci/simulation.sh`); `cargo test` of a whole target still runs
//! its tests side by side, and its runs may differ from the replay.
//! `tests/task_ids.rs` checks the list order this module relies on, and
//! fails if a Tokio upgrade changes it.

use std::env;

use tokio::runtime::{Builder, Handle};
use tokio::task::{Id, JoinHandle};

/// How many orders a dropped current-thread runtime can drop tasks in: its
/// task lists, `OwnedTasks::new(1)` in Tokio's current-thread scheduler,
/// whose sharded list has `num_cores.next_power_of_two() * 4` lists. A
/// task with ID `id` is in list `id % DROP_ORDERS`.
pub const DROP_ORDERS: u64 = 4;

/// The variable that sets the list the next task ID falls in, from 0 to
/// [`DROP_ORDERS`] − 1, instead of the seed's: to try another drop order
/// of one seed while debugging. The replay command repeats it.
pub const TASK_SHIFT_ENV: &str = "SKYS3_SIM_TASK_SHIFT";

/// The most throwaway tasks [`align`] spawns before it gives up: a few
/// rounds of [`DROP_ORDERS`], which leaves room for other threads' spawns.
const MAX_ALIGN_SPAWNS: u64 = 16 * DROP_ORDERS;

/// The list a seed's task IDs start in: [`TASK_SHIFT_ENV`] if it is set,
/// or `seed % DROP_ORDERS`.
///
/// # Panics
///
/// Panics if [`TASK_SHIFT_ENV`] is set but is not a number.
pub fn task_shift(seed: u64) -> u64 {
    match env::var(TASK_SHIFT_ENV) {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(shift) => shift % DROP_ORDERS,
            Err(_) => panic!("{TASK_SHIFT_ENV} must be an unsigned integer, not {value:?}"),
        },
        Err(_) => seed % DROP_ORDERS,
    }
}

/// Spawns throwaway tasks until the next task ID falls in list `shift %
/// DROP_ORDERS`, and returns the last one's ID. On a runtime, it spawns
/// there; elsewhere, on a runtime of its own.
///
/// # Panics
///
/// Panics if the IDs never reach that list, which means Tokio no longer
/// numbers tasks one after the other.
pub fn align(shift: u64) -> u64 {
    let target = shift % DROP_ORDERS;
    let spawn = |spawned: &mut dyn FnMut() -> JoinHandle<()>| {
        for _ in 0..MAX_ALIGN_SPAWNS {
            let id = id_number(spawned().id());
            if (id + 1) % DROP_ORDERS == target {
                return id;
            }
        }
        panic!(
            "Tokio task IDs did not reach list {target} in {MAX_ALIGN_SPAWNS} spawns; \
             skys3_sim::tasks assumes they count up by one"
        )
    };
    match Handle::try_current() {
        Ok(handle) => spawn(&mut || handle.spawn(async {})),
        Err(_) => {
            let runtime = Builder::new_current_thread()
                .build()
                .expect("a current-thread runtime without drivers always builds");
            spawn(&mut || runtime.spawn(async {}))
        }
    }
}

/// The number of a task ID, which Tokio shows only as text.
///
/// # Panics
///
/// Panics if the ID's text is not a number.
pub fn id_number(id: Id) -> u64 {
    let text = id.to_string();
    text.parse()
        .unwrap_or_else(|_| panic!("Tokio task ID {text:?} is not a number"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shifts_follow_the_seed() {
        // The variable is unset in the test environment.
        if env::var(TASK_SHIFT_ENV).is_err() {
            assert_eq!(task_shift(0), 0);
            assert_eq!(task_shift(5), 1);
            assert_eq!(task_shift(11), 3);
        }
    }

    #[tokio::test]
    async fn aligns_on_the_current_runtime_too() {
        let last = align(2);
        assert_eq!((last + 1) % DROP_ORDERS, 2);
    }
}
