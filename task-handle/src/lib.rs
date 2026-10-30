//! F1 and F2: `ActorTaskHandle::from_vtable` is a safe function that can cause UB.
//!
//! The runtime repro lives in `tests/` and only runs under miri:
//!
//! - `tests/borrowed_handle_outlives_owner.rs` (F1): use-after-free. Needs
//!   `--features borrow-probe`.
//! - `tests/padded_inline_handle.rs` (F2): uninitialized padding read as a pointer.
//! - `tests/owned_handle_control.rs`: the same calls with a sound backend. Passes
//!   under Miri, showing that the harness itself is clean.
//!
//! The doctest below is the native, stable-toolchain signal for F1. It is the
//! expected contract: a borrow that escapes its scope must be rejected by the
//! compiler. On the pinned revision it compiles, so `cargo test --doc` reports
//! "Test compiled successfully, but it's marked `compile_fail`".
//!
//! Stable rustdoc only checks that the snippet fails to compile, the `E0597`
//! code is verified on nightly alone.
//!
//! ```compile_fail,E0597
//! use std::sync::atomic::{AtomicBool, Ordering};
//! use factories::actor::task::{ActorTaskHandle, ActorTaskHandleVTable, WaitForTerminationFut};
//!
//! struct Borrowed<'a>(&'a AtomicBool);
//!
//! impl ActorTaskHandleVTable for Borrowed<'_> {
//!     fn abort(&self) {
//!         self.0.store(true, Ordering::Relaxed);
//!     }
//!
//!     fn wait_for_termination(&self) -> WaitForTerminationFut {
//!         Box::pin(async {})
//!     }
//! }
//!
//! let handle = {
//!     let flag = AtomicBool::new(false);
//!     ActorTaskHandle::from_vtable(Borrowed(&flag))
//!     // `flag` is dropped here, but `handle` still points at it.
//! };
//! // Never call `abort()` here: compiling is enough to show the bug.
//! std::mem::forget(handle);
//! ```

// Link the critical-section implementation that `factories_sync` expects.
use critical_section as _;
