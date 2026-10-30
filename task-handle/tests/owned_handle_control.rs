//! Control for F1/F2: the same public calls with a backend that owns its data
//! (`'static`) and has no padding. This passes under miri on the pinned revision,
//! so the UB reports from the other two tests come from the backend shapes they
//! use, not from the harness.
//!
//! Run: cargo +nightly miri test -p repro-task-handle --test owned_handle_control

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use factories::actor::task::{ActorTaskHandle, ActorTaskHandleVTable, WaitForTerminationFut};
use repro_task_handle as _;

struct Owned(Arc<AtomicBool>);

impl ActorTaskHandleVTable for Owned {
    fn abort(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn wait_for_termination(&self) -> WaitForTerminationFut {
        Box::pin(async {})
    }
}

#[test]
fn owned_handle_control() {
    let flag = Arc::new(AtomicBool::new(false));
    let handle = ActorTaskHandle::from_vtable(Owned(flag.clone()));
    handle.abort();
    drop(handle);
    assert!(flag.load(Ordering::Relaxed));
}
