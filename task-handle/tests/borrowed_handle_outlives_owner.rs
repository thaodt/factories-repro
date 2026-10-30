//! F1: `from_vtable` has no `T: 'static` bound, yet `ActorTaskHandle` has no
//! lifetime parameter. A backend that borrows local data can outlive that data,
//! and the safe `abort()` then dereferences a dangling reference.
//!
//! Run: cargo +nightly miri test -p repro-task-handle --features borrow-probe --test borrowed_handle_outlives_owner
//! Expected on the pinned revision: miri reports undefined behavior (use-after-free).
//! Natively the test is ignored, because executing it is UB. Once F1 is fixed
//! this file must fail to compile (the `borrow-probe` feature keeps that from
//! breaking the rest of the crate).

use std::sync::atomic::{AtomicBool, Ordering};

use factories::actor::task::{ActorTaskHandle, ActorTaskHandleVTable, WaitForTerminationFut};
use repro_task_handle as _;

struct Borrowed<'a>(&'a AtomicBool);

impl ActorTaskHandleVTable for Borrowed<'_> {
    fn abort(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn wait_for_termination(&self) -> WaitForTerminationFut {
        Box::pin(async {})
    }
}

#[test]
#[cfg_attr(
    not(miri),
    ignore = "executes UB on the pinned revision; run under Miri"
)]
fn borrowed_handle_outlives_owner() {
    // No `unsafe` in caller code.
    let handle = {
        let flag = Box::new(AtomicBool::new(false));
        ActorTaskHandle::from_vtable(Borrowed(&flag))
        // `flag` is freed here.
    };
    handle.abort(); // writes through the dangling `&AtomicBool`
}
