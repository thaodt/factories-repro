//! F3: an actor whose task is aborted before its first poll never reaches `Dead`.
//!
//! Run: cargo test -p repro-early-abort
//!
//! The `abort_before_first_poll_*` tests use a current-thread runtime (the
//! `#[tokio::test]` default) and do not await between `spawn` and `abort`, so
//! the actor task cannot have been polled when it is aborted. The tests first
//! wait for the task to really terminate, then check what the actor state
//! reports. No result depends on timing: every timeout is a watchdog for a
//! wait that can no longer complete.
//!
//! `abort_race_on_multi_thread_runtime` shows the same bug without a staged
//! ordering: on a multi-thread runtime, `spawn` followed by `abort` races the
//! worker thread's first poll and the abort usually wins.
//!
//! Expected on the pinned revision:
//! - The two `abort_after_first_poll_*` controls pass.
//! - `abort_before_first_poll_reaches_dead` fails: lifecycle stays `Starting`,
//!   no termination reason, `wait_for_terminal()` never resolves.
//! - `abort_before_first_poll_notifies_watcher` fails: the watcher never
//!   receives `Terminated`.
//! - `abort_race_on_multi_thread_runtime` fails: most of the 1000 actors stay
//!   `Starting` (970-996 per run here).

use std::time::Duration;

use factories::actor::state::{LifecycleState, SharedActorState};
use factories::prelude::*;
use factories::runtime::tokio::{TokioMpscChannelCapacity, TokioMpscChannelOptions};
use tokio::sync::mpsc;

const WATCHDOG: Duration = Duration::from_secs(2);

#[derive(Actor)]
struct Worker;

#[factories::messages]
impl Worker {
    #[handler]
    fn ping(&self) {}
}

/// Forwards every `Terminated` signal it receives to the test.
#[derive(Actor)]
struct Watcher {
    events: mpsc::UnboundedSender<(u64, TerminationKind)>,
}

#[factories::messages]
impl Watcher {
    #[handler(message = Terminated)]
    fn terminated(&mut self, #[message] event: Terminated) {
        let _ = self.events.send((event.tag(), event.kind()));
    }
}

fn describe(state: &SharedActorState<Worker>) -> String {
    format!(
        "lifecycle = {:?}, termination reason = {:?}",
        state.lifecycle(),
        state.termination_reason().map(|reason| reason.kind()),
    )
}

/// Abort, wait until the task has really terminated, then require the
/// documented outcome: `Dead` with `TerminationReason::Aborted`.
async fn abort_and_expect_dead(state: &SharedActorState<Worker>) {
    let task = state.task().expect("tokio spawner provides a task handle");
    task.abort();
    tokio::time::timeout(WATCHDOG, task.wait_for_termination())
        .await
        .expect("the aborted task terminates");

    // The task is gone, so nothing can change the state any more.
    let terminal = tokio::time::timeout(WATCHDOG, state.wait_for_terminal()).await;
    assert!(
        terminal.is_ok(),
        "task has terminated, but wait_for_terminal() never resolves: {}",
        describe(state),
    );
    assert_eq!(
        state.lifecycle(),
        LifecycleState::Dead,
        "{}",
        describe(state)
    );
    assert_eq!(
        state.termination_reason().map(|reason| reason.kind()),
        Some(TerminationKind::Aborted),
        "{}",
        describe(state),
    );
}

/// Spawn a worker watched by a `Watcher`, abort it and require that the
/// watcher receives `Terminated` with `TerminationKind::Aborted`.
async fn abort_and_expect_terminated(poll_first: bool) {
    let spawner = TokioTaskSpawner::current();
    let (events, mut observed) = mpsc::unbounded_channel();
    // Unbounded on purpose: delivering to a *bounded* mailbox from the drop
    // path panics on a current-thread runtime, which would hide F3 behind a
    // different bug (see tests/incidental_current_thread_watch.rs).
    let watcher = ActorLauncher::<Watcher>::builder()
        .channel_options(TokioMpscChannelOptions {
            capacity: TokioMpscChannelCapacity::Unbounded,
            ..Default::default()
        })
        .build()
        .spawn_ready(&spawner, Watcher { events })
        .await
        .unwrap();

    let worker = ActorLauncher::default().spawn(&spawner, Worker);
    watcher.watch(&worker, 7);
    if poll_first {
        tokio::task::yield_now().await;
    }
    let task = worker.state().task().unwrap();
    task.abort();
    tokio::time::timeout(WATCHDOG, task.wait_for_termination())
        .await
        .expect("the aborted task terminates");

    let event = tokio::time::timeout(WATCHDOG, observed.recv()).await;
    assert!(
        matches!(event, Ok(Some((7, TerminationKind::Aborted)))),
        "watcher never received Terminated for the aborted worker ({event:?}): {}",
        describe(worker.state()),
    );
}

#[tokio::test]
async fn abort_before_first_poll_reaches_dead() {
    let worker = ActorLauncher::default().spawn(&TokioTaskSpawner::current(), Worker);
    // No await since `spawn`: the actor future has not been polled yet.
    abort_and_expect_dead(worker.state()).await;
}

#[tokio::test]
async fn abort_before_first_poll_notifies_watcher() {
    abort_and_expect_terminated(false).await;
}

/// Spawn and abort 1000 actors on a 4-worker runtime, wait until every task
/// has terminated and require that every actor reached `Dead`.
///
/// Spawned tasks go to the worker threads, so whether an actor is polled
/// before `abort()` is a race. On the pinned revision almost every abort wins
/// the race and those actors are left `Starting`. The test can pass on an
/// unfixed build only if all 1000 tasks happen to be polled first, which was
/// never observed; after a fix it passes deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_race_on_multi_thread_runtime() {
    const N: usize = 1000;
    let spawner = TokioTaskSpawner::current();
    let workers: Vec<_> = (0..N)
        .map(|_| {
            let worker = ActorLauncher::default().spawn(&spawner, Worker);
            worker.state().task().unwrap().abort();
            worker
        })
        .collect();
    for worker in &workers {
        tokio::time::timeout(
            WATCHDOG,
            worker.state().task().unwrap().wait_for_termination(),
        )
        .await
        .expect("the aborted task terminates");
    }

    // Every task is gone, so the states are final.
    let starting = workers
        .iter()
        .filter(|worker| worker.state().lifecycle() == LifecycleState::Starting)
        .count();
    let not_dead = workers
        .iter()
        .filter(|worker| worker.state().lifecycle() != LifecycleState::Dead)
        .count();
    assert_eq!(
        not_dead, 0,
        "{not_dead} of {N} actors never reached Dead after their task terminated ({starting} still Starting)",
    );
}

// Controls: the same aborts once the actor has been polled work as documented.

#[tokio::test]
async fn abort_after_first_poll_reaches_dead() {
    let worker = ActorLauncher::default().spawn(&TokioTaskSpawner::current(), Worker);
    // Let the current-thread runtime poll the actor task once.
    tokio::task::yield_now().await;
    assert_ne!(worker.state().lifecycle(), LifecycleState::Dead);
    abort_and_expect_dead(worker.state()).await;
}

#[tokio::test]
async fn abort_after_first_poll_notifies_watcher() {
    abort_and_expect_terminated(true).await;
}
