//! I3: a `watch()` registered while the watched actor is dying never fires.
//!
//! Run: cargo test -p repro-watch-race
//!
//! Termination order on the pinned revision:
//!
//! 1. `run_stop_hook` drains the subscriptions and delivers `Terminated`
//!    (`notify_subscribers`).
//! 2. `DeadOnDropGuard::drop` drains a last time (`notify_subscribers_now`),
//!    which catches watches registered after step 1.
//! 3. It delivers to the subscriptions drained in step 2.
//! 4. `transition_dead()`.
//!
//! A watch registered after step 2 but before step 4 is pushed into a set that
//! nobody reads again. The watcher sees the actor `Stopping`, not `Dead`, yet
//! never receives `Terminated`. `add_subscription` documents only that
//! "registering after the actor is already dead simply never fires".
//!
//! Expected on the pinned revision:
//! - `watch_registered_before_dead_always_fires` fails: a thread keeps calling
//!   `watch()` while the child stops and thousands of those watches, made
//!   while the child was still `Stopping`, are never delivered.
//! - The two controls pass. Watches registered before the stop, or during the
//!   async delivery in step 1, are delivered.
//!
//! No result depends on sleeps. After the child is `Dead`, every `Terminated`
//! that will ever be sent is already in the watcher's mailbox, so an `ask` to
//! the watcher (processed in FIFO order) flushes them before counting.

use std::collections::BTreeSet;
use std::time::Duration;

use factories::actor::state::LifecycleState;
use factories::prelude::*;
use factories::runtime::tokio::{TokioMpscChannelCapacity, TokioMpscChannelOptions};
use tokio::sync::{mpsc, oneshot};

const WATCHDOG: Duration = Duration::from_secs(10);
const ROUNDS: usize = 20;
/// Upper bound on watches per round, in case the child never dies.
const MAX_WATCHES: u64 = 1_000_000;

#[derive(Actor)]
struct Child;

#[factories::messages]
impl Child {
    #[handler]
    fn finish(&mut self, #[context] cx: ActorContext<'_, Self>) {
        cx.stop();
    }
}

/// Forwards the tag of every `Terminated` it receives to the test.
#[derive(Actor)]
struct Watcher {
    events: mpsc::UnboundedSender<u64>,
}

#[factories::messages]
impl Watcher {
    #[handler(message = Terminated)]
    fn terminated(&mut self, #[message] event: Terminated) {
        let _ = self.events.send(event.tag());
    }

    /// Answered after every message queued before it has been handled.
    #[handler]
    fn flush(&self) {}

    #[handler]
    async fn stall(&mut self, started: oneshot::Sender<()>, gate: oneshot::Receiver<()>) {
        started.send(()).unwrap();
        let _ = gate.await;
    }

    #[handler]
    fn filler(&self) {}
}

fn spawn_watcher(
    spawner: &TokioTaskSpawner,
    capacity: TokioMpscChannelCapacity,
) -> (WatcherHandle, mpsc::UnboundedReceiver<u64>) {
    let (events, received) = mpsc::unbounded_channel();
    let watcher = ActorLauncher::<Watcher>::builder()
        .channel_options(TokioMpscChannelOptions {
            capacity,
            ..Default::default()
        })
        .build()
        .spawn(spawner, Watcher { events });
    (watcher, received)
}

/// Wait for the child to die, flush the watcher's mailbox and return the
/// tags of every `Terminated` it received.
async fn delivered_tags(
    child: &ChildHandle,
    watcher: &WatcherHandle,
    received: &mut mpsc::UnboundedReceiver<u64>,
) -> BTreeSet<u64> {
    tokio::time::timeout(WATCHDOG, child.state().wait_for_terminal())
        .await
        .expect("the child reaches Dead");
    tokio::time::timeout(WATCHDOG, watcher.flush())
        .await
        .expect("the watcher answers")
        .unwrap();
    let mut tags = BTreeSet::new();
    while let Ok(tag) = received.try_recv() {
        tags.insert(tag);
    }
    tags
}

/// Every `watch()` that returns while the child is not yet `Dead` must fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_registered_before_dead_always_fires() {
    let spawner = TokioTaskSpawner::current();
    for round in 0..ROUNDS {
        let (watcher, mut received) = spawn_watcher(&spawner, TokioMpscChannelCapacity::Unbounded);
        let child = ActorLauncher::default()
            .spawn_ready(&spawner, Child)
            .await
            .unwrap();

        // A plain thread keeps registering watches with increasing tags and
        // records the child's lifecycle right after each `watch()` returns.
        let (watcher_ref, child_ref) = (watcher.clone(), child.clone());
        let (warmed_up, warm) = std::sync::mpsc::channel();
        let spammer = std::thread::spawn(move || {
            let mut observed = Vec::new();
            let mut dead_seen = 0;
            for tag in 1..=MAX_WATCHES {
                watcher_ref.watch(&child_ref, tag);
                let lifecycle = child_ref.state().lifecycle();
                observed.push((tag, lifecycle));
                if tag == 64 {
                    warmed_up.send(()).unwrap();
                }
                if lifecycle == LifecycleState::Dead {
                    dead_seen += 1;
                    if dead_seen == 64 {
                        break;
                    }
                }
            }
            observed
        });
        warm.recv().unwrap();
        child.finish().tell().await.unwrap();
        let observed = tokio::task::spawn_blocking(move || spammer.join().unwrap())
            .await
            .unwrap();

        let delivered = delivered_tags(&child, &watcher, &mut received).await;
        let lost: Vec<_> = observed
            .iter()
            .filter(|(tag, lifecycle)| {
                *lifecycle != LifecycleState::Dead && !delivered.contains(tag)
            })
            .collect();
        let before_dead = observed
            .iter()
            .filter(|(_, lifecycle)| *lifecycle != LifecycleState::Dead)
            .count();
        assert!(
            lost.is_empty(),
            "round {round}: {} of {before_dead} watches made while the child was not yet Dead never \
             received Terminated ({} delivered); first lost: tag {}, child was {:?} when watch() returned",
            lost.len(),
            delivered.len(),
            lost[0].0,
            lost[0].1,
        );
    }
}

/// Control: watches registered before the child stops are all delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watches_registered_before_stop_are_delivered() {
    const WATCHES: u64 = 1000;
    let spawner = TokioTaskSpawner::current();
    let (watcher, mut received) = spawn_watcher(&spawner, TokioMpscChannelCapacity::Unbounded);
    let child = ActorLauncher::default()
        .spawn_ready(&spawner, Child)
        .await
        .unwrap();
    for tag in 1..=WATCHES {
        watcher.watch(&child, tag);
    }
    child.finish().tell().await.unwrap();

    let delivered = delivered_tags(&child, &watcher, &mut received).await;
    assert_eq!(delivered, (1..=WATCHES).collect());
}

/// Control: a watch registered while the child is stuck in the async delivery
/// of step 1 is caught by the guard's last drain (step 2) and delivered. The
/// window is only between step 2 and `Dead`.
#[tokio::test]
async fn watch_during_async_delivery_is_delivered() {
    let spawner = TokioTaskSpawner::current();
    let (slow, mut slow_received) = spawn_watcher(&spawner, TokioMpscChannelCapacity::Bounded(1));
    let (late, mut late_received) = spawn_watcher(&spawner, TokioMpscChannelCapacity::Unbounded);
    let child = ActorLauncher::default()
        .spawn_ready(&spawner, Child)
        .await
        .unwrap();
    slow.watch(&child, 1);

    // Park the slow watcher's handler and fill its single mailbox slot, so
    // the child's async delivery to it has to wait.
    let (started, observed) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    slow.stall(started, gate).tell().await.unwrap();
    observed.await.unwrap();
    slow.filler()
        .try_tell()
        .expect("fill the only mailbox slot");

    child.finish().tell().await.unwrap();
    // The reason is recorded right before the async drain and delivery.
    while child.state().termination_reason().is_none() {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    assert_eq!(child.state().lifecycle(), LifecycleState::Stopping);

    late.watch(&child, 2);
    release.send(()).unwrap();

    assert_eq!(
        delivered_tags(&child, &slow, &mut slow_received).await,
        BTreeSet::from([1])
    );
    assert_eq!(
        delivered_tags(&child, &late, &mut late_received).await,
        BTreeSet::from([2])
    );
}
