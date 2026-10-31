//! F2: when `size_of::<T>()` and `align_of::<T>()` fit a pointer, `from_vtable` writes
//! `T` into a `MaybeUninit<*mut c_void>` and calls `assume_init()` on the pointer.
//! Padding bytes of `T` are uninitialized, so this produces a raw pointer with
//! uninitialized bytes (task.rs:49-53). Pre-zeroing the slot doesnot help:
//! writing a `T` does not preserve the padding bytes.
//!
//! Run: cargo +nightly miri test -p repro-task-handle --test padded_inline_handle
//! Expected on the pinned revision: miri reports undefined behavior at the
//! `assume_init()` in `ActorTaskHandle::from_vtable`, before `abort()` runs.
//! Natively the test is ignored, because executing it is UB.

use factories::actor::task::{ActorTaskHandle, ActorTaskHandleVTable, WaitForTerminationFut};
use repro_task_handle as _;

/// 4 bytes on x86_64: `tag` at 0, one padding byte at 1, `id` at 2..4.
#[repr(C)]
struct Padded {
    tag: u8,
    id: u16,
}

impl ActorTaskHandleVTable for Padded {
    fn abort(&self) {
        assert_eq!((self.tag, self.id), (1, 2));
    }

    fn wait_for_termination(&self) -> WaitForTerminationFut {
        Box::pin(async {})
    }
}

#[test]
#[cfg_attr(
    not(miri),
    ignore = "executes UB on the pinned revision; run under miri"
)]
fn padded_inline_handle() {
    // Takes the inline-storage branch of `from_vtable`.
    assert!(size_of::<Padded>() <= size_of::<*mut ()>());
    assert!(align_of::<Padded>() <= align_of::<*mut ()>());

    let handle = ActorTaskHandle::from_vtable(Padded { tag: 1, id: 2 });
    handle.abort();
    drop(handle);
}
