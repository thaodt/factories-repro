//! I3: a watch registered while the watched actor is dying can be lost.
//! See `tests/watch_while_dying.rs`.
//!
//! When an actor terminates, `DeadOnDropGuard::drop` drains the subscriptions
//! a last time (`notify_subscribers_now`), delivers to each and only then
//! calls `transition_dead()`. A `watch()` that lands after that last drain but
//! before `Dead` is pushed into a set that nobody reads again, so it never
//! fires, although the watcher saw the actor `Stopping`, not `Dead`.
