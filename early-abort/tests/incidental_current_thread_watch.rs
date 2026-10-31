//! Incidental finding, not part of F3 (ignored by default).
//!
//! Run: cargo test -p repro-early-abort --test incidental_current_thread_watch -- --ignored
//!
//! A task abort notifies watchers from `DeadOnDropGuard::drop`, synchronously.
//! With the default `WatchDeliveryPolicy::Always` and the default bounded
//! mailbox, that calls `tokio::task::block_in_place` (runtime/tokio/channel.rs:246)
//! even though the watcher's mailbox is empty. On a current-thread runtime, the
//! `#[tokio::test]` default, `block_in_place` panics with "can call blocking only
//! when running on the multi-threaded runtime". The panic escapes the guard after
//! `Aborted` was recorded but before the `Dead` transition.
//!
//! Expected on the pinned revision: fails, the watcher gets nothing and the
//! worker is left `Running` with reason `Aborted`.

use std::time::Duration;

use factories::actor::state::LifecycleState;
use factories::prelude::*;
use tokio::sync::mpsc;

const WATCHDOG: Duration = Duration::from_secs(2);

#[derive(Actor)]
struct Worker;

#[factories::messages]
impl Worker {
    #[handler]
    fn ping(&self) {}
}

#[derive(Actor)]
struct Watcher {
    events: mpsc::UnboundedSender<TerminationKind>,
}

#[factories::messages]
impl Watcher {
    #[handler(message = Terminated)]
    fn terminated(&mut self, #[message] event: Terminated) {
        let _ = self.events.send(event.kind());
    }
}

#[tokio::test]
#[ignore = "incidental finding, separate from F3; see README"]
async fn abort_watched_actor_on_current_thread_runtime() {
    let spawner = TokioTaskSpawner::current();
    let (events, mut observed) = mpsc::unbounded_channel();
    // Default launcher: bounded mailbox of 64, empty when the signal is sent.
    let watcher = ActorLauncher::default()
        .spawn_ready(&spawner, Watcher { events })
        .await
        .unwrap();
    let worker = ActorLauncher::default()
        .spawn_ready(&spawner, Worker)
        .await
        .unwrap();
    watcher.watch(&worker, 0);

    let task = worker.state().task().unwrap();
    task.abort();
    tokio::time::timeout(WATCHDOG, task.wait_for_termination())
        .await
        .expect("the aborted task terminates");

    let event = tokio::time::timeout(WATCHDOG, observed.recv()).await;
    let state = worker.state();
    assert!(
        matches!(event, Ok(Some(TerminationKind::Aborted)))
            && state.lifecycle() == LifecycleState::Dead,
        "watcher event = {event:?}; worker lifecycle = {:?}, reason = {:?}",
        state.lifecycle(),
        state.termination_reason().map(|reason| reason.kind()),
    );
}
