//! The cyclic scheduling policy, driven from outside the crate.
//!
//! The timing tests hold a lock so that they do not compete with each other
//! for the CPU, and each guards against passing without the work happening:
//! it checks that the latency queue ran many times and that the CPU-heavy
//! queues did work, before checking any bound.

use glommio::{
    executor, spawn_local_into, yield_if_needed, Latency, LocalExecutorBuilder, Placement,
    Scheduling, Shares, TaskQueueHandle,
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::Mutex,
    time::{Duration, Instant},
};

static TIMING: Mutex<()> = Mutex::new(());

const MINOR: Duration = Duration::from_millis(1);
/// The work a CPU-heavy task does between two checks for preemption.
const CHUNK: Duration = Duration::from_micros(50);
/// Room for a debug build's reactor passes and the chunk a frame boundary
/// waits for, per frame of a bound.
const SLACK_PER_FRAME: Duration = Duration::from_micros(250);

fn cyclic(major: u32) -> Scheduling {
    Scheduling::Cyclic {
        minor: MINOR,
        major,
    }
}

fn spin(d: Duration) {
    let start = Instant::now();
    while start.elapsed() < d {
        std::hint::spin_loop();
    }
}

/// Spawns a task into `queue` that does `chunk` of work and then yields if
/// preemption is due, until `stop`, counting its chunks.
fn cpu_task(queue: TaskQueueHandle, chunk: Duration, stop: Rc<Cell<bool>>) -> glommio::Task<u64> {
    spawn_local_into(
        async move {
            let mut chunks = 0;
            while !stop.get() {
                spin(chunk);
                chunks += 1;
                yield_if_needed().await;
            }
            chunks
        },
        queue,
    )
    .unwrap()
}

/// What a busy latency queue saw: the longest it waited between two of its
/// turns, and how many turns it had.
struct Turns {
    longest: Duration,
    turns: u64,
}

/// Spawns a latency task into `queue` that is always runnable: it does a
/// chunk, yields if preemption is due, and measures how long the yield took,
/// which is how long it waited for its next turn, for `run`.
fn busy_latency_task(queue: TaskQueueHandle, run: Duration) -> glommio::Task<Turns> {
    spawn_local_into(
        async move {
            let start = Instant::now();
            let mut turns = Turns {
                longest: Duration::ZERO,
                turns: 0,
            };
            while start.elapsed() < run {
                spin(CHUNK);
                let before = Instant::now();
                yield_if_needed().await;
                let waited = before.elapsed();
                if waited > Duration::from_micros(100) {
                    turns.turns += 1;
                }
                if start.elapsed() > Duration::from_millis(20) {
                    turns.longest = turns.longest.max(waited);
                }
            }
            turns
        },
        queue,
    )
    .unwrap()
}

#[test]
fn the_cyclic_policy_is_reachable_from_outside() {
    let (bound, vruntime_bound, overruns) = LocalExecutorBuilder::new(Placement::Unbound)
        .scheduling(cyclic(12))
        .spawn(|| async move {
            let latency = executor().create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(3)),
                "latency",
            );
            let _other =
                executor().create_task_queue(Shares::Static(1000), Latency::NotImportant, "other");
            spawn_local_into(async {}, latency).unwrap().await;
            let overruns = executor()
                .task_queue_stats(latency)
                .unwrap()
                .frame_overruns();
            (
                executor().slot_wait_bound(latency),
                executor().slot_wait_bound(TaskQueueHandle::default()),
                overruns,
            )
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(bound, Some(Duration::from_millis(4)), "three frames apart");
    assert!(vruntime_bound.is_some(), "the default queue owns slots too");
    assert_eq!(overruns, 0);

    let bound = LocalExecutorBuilder::new(Placement::Unbound)
        .scheduling(Scheduling::Vruntime)
        .spawn(|| async move { executor().slot_wait_bound(TaskQueueHandle::default()) })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(bound, None);
}

/// A latency queue that is always runnable, beside eight CPU-heavy queues
/// that yield, gets a turn at least every three frames, as its latency asks:
/// under the vruntime scheduler it would wait for all eight in turn.
#[test]
fn a_busy_latency_queue_waits_within_its_table_bound() {
    let _timing = TIMING.lock().unwrap_or_else(|e| e.into_inner());
    let (turns, bound, chunks) = LocalExecutorBuilder::new(Placement::Unbound)
        .scheduling(cyclic(12))
        .spawn(|| async move {
            let latency = executor().create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(3)),
                "latency",
            );
            let stop = Rc::new(Cell::new(false));
            let cpu: Vec<_> = (0..8)
                .map(|i| {
                    let queue = executor().create_task_queue(
                        Shares::Static(1000),
                        Latency::NotImportant,
                        &format!("cpu{i}"),
                    );
                    cpu_task(queue, CHUNK, stop.clone())
                })
                .collect();
            let turns = busy_latency_task(latency, Duration::from_millis(300)).await;
            stop.set(true);
            let mut chunks = 0;
            for task in cpu {
                chunks += task.await;
            }
            (turns, executor().slot_wait_bound(latency).unwrap(), chunks)
        })
        .unwrap()
        .join()
        .unwrap();
    println!(
        "busy latency: longest wait {:?}, bound {bound:?}, {} turns, {chunks} chunks",
        turns.longest, turns.turns
    );
    assert_eq!(bound, Duration::from_millis(4));
    assert!(
        turns.turns >= 30,
        "the latency queue had {} turns",
        turns.turns
    );
    assert!(chunks >= 1000, "the CPU queues did {chunks} chunks");
    let allowed = bound + SLACK_PER_FRAME * 4;
    assert!(
        turns.longest <= allowed,
        "the latency queue waited {:?}, its bound is {bound:?} (allowed {allowed:?})",
        turns.longest
    );
}

/// A queue whose task spins for four frames without yielding is charged for
/// it: it gets no more of the CPU than the queue beside it with the same
/// shares, its overruns are counted, and a latency queue's wait grows by no
/// more than one overrun.
#[test]
fn an_overrunning_queue_is_charged_and_the_others_keep_their_bound() {
    let _timing = TIMING.lock().unwrap_or_else(|e| e.into_inner());
    const OVERRUN: Duration = Duration::from_millis(4);
    let result = LocalExecutorBuilder::new(Placement::Unbound)
        .scheduling(cyclic(10))
        .spawn(|| async move {
            let latency = executor().create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(3)),
                "latency",
            );
            let hog =
                executor().create_task_queue(Shares::Static(1000), Latency::NotImportant, "hog");
            let polite =
                executor().create_task_queue(Shares::Static(1000), Latency::NotImportant, "polite");
            let stop = Rc::new(Cell::new(false));
            let hog_task = cpu_task(hog, OVERRUN, stop.clone());
            let polite_task = cpu_task(polite, CHUNK, stop.clone());
            let turns = busy_latency_task(latency, Duration::from_millis(600)).await;
            stop.set(true);
            let spins = hog_task.await;
            let chunks = polite_task.await;
            let hog_stats = executor().task_queue_stats(hog).unwrap();
            let polite_stats = executor().task_queue_stats(polite).unwrap();
            (
                turns,
                executor().slot_wait_bound(latency).unwrap(),
                spins,
                chunks,
                hog_stats,
                polite_stats,
            )
        })
        .unwrap()
        .join()
        .unwrap();
    let (turns, bound, spins, chunks, hog, polite) = result;
    println!(
        "overrun: longest wait {:?}, bound {bound:?}, hog {:?} in {spins} spins with {} \
         overruns, polite {:?} with {} overruns",
        turns.longest,
        hog.runtime(),
        hog.frame_overruns(),
        polite.runtime(),
        polite.frame_overruns()
    );
    assert!(
        turns.turns >= 50,
        "the latency queue had {} turns",
        turns.turns
    );
    assert!(spins >= 10, "the hog spun {spins} times");
    assert!(chunks >= 1000, "the polite queue did {chunks} chunks");

    assert!(
        hog.frame_overruns() >= 10,
        "{} overruns counted for {spins} spins",
        hog.frame_overruns()
    );
    let share = hog.runtime().as_secs_f64() / (hog.runtime() + polite.runtime()).as_secs_f64();
    assert!(
        share <= 0.6,
        "the hog had {share:.2} of the CPU the two equal queues used ({:?} against {:?})",
        hog.runtime(),
        polite.runtime()
    );
    assert!(
        polite.frame_overruns() * 10 <= hog.frame_overruns(),
        "the polite queue was counted {} overruns",
        polite.frame_overruns()
    );
    let allowed = bound + OVERRUN + SLACK_PER_FRAME * 4;
    assert!(
        turns.longest <= allowed,
        "the latency queue waited {:?}, its bound is {bound:?} plus one overrun \
         (allowed {allowed:?})",
        turns.longest
    );
}

/// The frames of queues with nothing to run go to a queue that has work: a
/// queue that owns no frame at all, beside idle queues that own them all,
/// still gets the CPU.
#[test]
fn idle_owners_leave_their_frames_to_the_others() {
    let _timing = TIMING.lock().unwrap_or_else(|e| e.into_inner());
    const RUN: Duration = Duration::from_millis(300);
    let (busy, chunks, wall) = LocalExecutorBuilder::new(Placement::Unbound)
        .scheduling(cyclic(10))
        .spawn(|| async move {
            let idle =
                executor().create_task_queue(Shares::Static(1000), Latency::NotImportant, "idle");
            let busy =
                executor().create_task_queue(Shares::Static(1), Latency::NotImportant, "busy");
            assert!(executor().slot_wait_bound(idle).is_some());
            assert_eq!(executor().slot_wait_bound(busy), None, "owns no frame");
            let stop = Rc::new(Cell::new(false));
            let task = cpu_task(busy, CHUNK, stop.clone());
            let start = Instant::now();
            glommio::timer::sleep(RUN).await;
            stop.set(true);
            let chunks = task.await;
            let wall = start.elapsed();
            (executor().task_queue_stats(busy).unwrap(), chunks, wall)
        })
        .unwrap()
        .join()
        .unwrap();
    let share = busy.runtime().as_secs_f64() / wall.as_secs_f64();
    println!(
        "idle owners: busy ran {:?} of {wall:?}, {share:.3}",
        busy.runtime()
    );
    assert!(chunks >= 1000, "the busy queue did {chunks} chunks");
    assert!(
        share >= 0.8,
        "the busy queue ran {:?} of {wall:?}, {share:.2}, owning no frame",
        busy.runtime()
    );
}
