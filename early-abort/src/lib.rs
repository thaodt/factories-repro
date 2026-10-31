//! F3: aborting an actor's task before its first poll leaves the actor in
//! `Starting` forever. See `tests/abort_before_first_poll.rs`.
//!
//! `standard_run_with` creates its `DeadOnDropGuard` inside the `async fn`
//! body, so the guard only exists once the future has been polled. Aborting
//! the task before that drops an unpolled future: nothing transitions the
//! actor to `Dead`, records `Aborted` or notifies watchers.
