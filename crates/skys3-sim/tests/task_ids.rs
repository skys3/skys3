//! What `skys3_sim::tasks` relies on in Tokio: task IDs count up by one,
//! and a dropped current-thread runtime drops its tasks list by list, task
//! `id` in list `id % DROP_ORDERS`, oldest first within a list. If a Tokio
//! upgrade changes either, this test fails, and the alignment that makes a
//! crash replay the same way needs another look.
//!
//! One test in its own target, so no other test spawns tasks meanwhile.

use std::sync::{Arc, Mutex};

use skys3_sim::tasks::{DROP_ORDERS, align, id_number};
use tokio::runtime::{Builder, Runtime};

/// Records its task's ID when the task is dropped.
struct Recorder {
    id: Arc<Mutex<Option<u64>>>,
    dropped: Arc<Mutex<Vec<u64>>>,
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let id = self.id.lock().unwrap().expect("set after the spawn");
        self.dropped.lock().unwrap().push(id);
    }
}

fn runtime() -> Runtime {
    Builder::new_current_thread().build().unwrap()
}

#[test]
fn task_ids_and_the_drop_order_are_as_the_alignment_assumes() {
    // IDs count up by one.
    let ids: Vec<u64> = {
        let runtime = runtime();
        (0..8)
            .map(|_| id_number(runtime.spawn(async {}).id()))
            .collect()
    };
    for pair in ids.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "{ids:?}");
    }

    // A dropped runtime drops its pending tasks list by list.
    let dropped = Arc::new(Mutex::new(Vec::new()));
    let runtime = runtime();
    let mut spawned = Vec::new();
    for _ in 0..3 * DROP_ORDERS {
        let id = Arc::new(Mutex::new(None));
        let recorder = Recorder {
            id: Arc::clone(&id),
            dropped: Arc::clone(&dropped),
        };
        let handle = runtime.spawn(async move {
            let _recorder = recorder;
            std::future::pending::<()>().await;
        });
        let number = id_number(handle.id());
        *id.lock().unwrap() = Some(number);
        spawned.push(number);
    }
    drop(runtime);
    let mut expected = spawned.clone();
    expected.sort_by_key(|id| (id % DROP_ORDERS, *id));
    assert_eq!(*dropped.lock().unwrap(), expected, "spawned {spawned:?}");

    // Alignment puts the next ID in the list asked for.
    for shift in 0..2 * DROP_ORDERS {
        let last = align(shift);
        let next = id_number(runtime_spawn_id());
        assert_eq!(next, last + 1);
        assert_eq!(next % DROP_ORDERS, shift % DROP_ORDERS);
    }
}

/// The ID of a task spawned on a runtime of its own.
fn runtime_spawn_id() -> tokio::task::Id {
    runtime().spawn(async {}).id()
}
