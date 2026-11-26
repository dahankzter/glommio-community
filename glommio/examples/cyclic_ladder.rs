//! How long a `Latency::Matters(1ms)` queue waits beside CPU-heavy queues,
//! under the vruntime scheduler and the cyclic policy, and what the CPU-heavy
//! queues get done meanwhile.
//!
//! One run is one configuration, so a driver can interleave them and pin each
//! with `taskset`:
//!
//! ```sh
//! cargo build --release --example cyclic_ladder
//! taskset -c 20 target/release/examples/cyclic_ladder <policy> <cpu-queues> \
//!     <overrun 0|1> <latency sleep|busy> <seconds> <units-per-chunk>
//! target/release/examples/cyclic_ladder calibrate
//! ```
//!
//! `policy` is `v` (vruntime, default 100 ms preempt timer), `v:<us>`
//! (vruntime with that preempt timer) or `c:<minor us>:<major>` (cyclic).
//!
//! The latency queue runs one task, in one of two shapes:
//!
//! * `sleep`: it sleeps a pseudo-random 0.2 to 2 ms and records how late it
//!   ran after its deadline. An event-driven queue, idle between events.
//! * `busy`: it is always runnable, doing 20 us of work and then yielding if
//!   preemption is due, and records how long each such yield took: the wait
//!   for its next turn. A queue with a backlog.
//!
//! Each CPU-heavy queue runs one task that does a fixed amount of work, a
//! chunk of `units-per-chunk` units (about 50 us, see `calibrate`), and then
//! yields if preemption is due. Throughput is units per second over all of
//! them. With `overrun 1` one more queue spins 5 ms at a time without
//! yielding in between.
//!
//! Prints one line of `key=value` pairs.

use glommio::{
    executor, spawn_local_into, timer::sleep, yield_if_needed, Latency, LocalExecutorBuilder,
    Placement, Scheduling, Shares,
};
use std::{
    cell::{Cell, RefCell},
    hint::black_box,
    rc::Rc,
    time::{Duration, Instant},
};

const WARMUP: Duration = Duration::from_millis(200);
const OVERRUN: Duration = Duration::from_millis(5);
const BUSY_CHUNK: Duration = Duration::from_micros(20);

fn unit(seed: u64) -> u64 {
    let mut x = seed | 1;
    for _ in 0..256 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x = x.wrapping_mul(0x2545_f491_4f6c_dd1d);
    }
    black_box(x)
}

fn spin(d: Duration) {
    let start = Instant::now();
    while start.elapsed() < d {
        std::hint::spin_loop();
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("calibrate") {
        let start = Instant::now();
        let mut acc = 0;
        for i in 0..20_000 {
            acc ^= unit(i);
        }
        black_box(acc);
        let per_unit = start.elapsed() / 20_000;
        let per_chunk = Duration::from_micros(50).as_nanos() / per_unit.as_nanos().max(1);
        println!("unit={per_unit:?} units_per_50us={per_chunk}");
        return;
    }
    let policy = args[0].clone();
    let cpu_queues: usize = args[1].parse().unwrap();
    let overrun = args[2] == "1";
    let busy = args[3] == "busy";
    let run = Duration::from_secs_f64(args[4].parse().unwrap());
    let units_per_chunk: u64 = args[5].parse().unwrap();

    let mut builder = LocalExecutorBuilder::new(Placement::Unbound);
    let parts: Vec<&str> = policy.split(':').collect();
    match parts.as_slice() {
        ["v"] => {}
        ["v", us] => {
            builder = builder.preempt_timer(Duration::from_micros(us.parse().unwrap()));
        }
        ["c", minor, major] => {
            builder = builder.scheduling(Scheduling::Cyclic {
                minor: Duration::from_micros(minor.parse().unwrap()),
                major: major.parse().unwrap(),
            });
        }
        _ => panic!("policy: v, v:<us> or c:<minor us>:<major>"),
    }

    let line = builder
        .spawn(move || async move {
            let stop = Rc::new(Cell::new(false));
            let measuring = Rc::new(Cell::new(false));
            let units = Rc::new(Cell::new(0u64));
            let mut tasks = Vec::new();
            for i in 0..cpu_queues {
                let queue = executor().create_task_queue(
                    Shares::Static(1000),
                    Latency::NotImportant,
                    &format!("cpu{i}"),
                );
                let (stop, measuring, units) = (stop.clone(), measuring.clone(), units.clone());
                tasks.push(
                    spawn_local_into(
                        async move {
                            let mut seed = i as u64;
                            while !stop.get() {
                                for _ in 0..units_per_chunk {
                                    seed = unit(seed);
                                }
                                if measuring.get() {
                                    units.set(units.get() + units_per_chunk);
                                }
                                yield_if_needed().await;
                            }
                        },
                        queue,
                    )
                    .unwrap(),
                );
            }
            let hog = overrun.then(|| {
                let queue = executor().create_task_queue(
                    Shares::Static(1000),
                    Latency::NotImportant,
                    "hog",
                );
                let stop = stop.clone();
                tasks.push(
                    spawn_local_into(
                        async move {
                            while !stop.get() {
                                spin(OVERRUN);
                                yield_if_needed().await;
                            }
                        },
                        queue,
                    )
                    .unwrap(),
                );
                queue
            });

            let latency = executor().create_task_queue(
                Shares::Static(1000),
                Latency::Matters(Duration::from_millis(1)),
                "latency",
            );
            let waits = Rc::new(RefCell::new(Vec::<u64>::with_capacity(1 << 20)));
            let latency_task = {
                let (stop, measuring, waits) = (stop.clone(), measuring.clone(), waits.clone());
                spawn_local_into(
                    async move {
                        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
                        while !stop.get() {
                            if busy {
                                spin(BUSY_CHUNK);
                                if executor().need_preempt() {
                                    let before = Instant::now();
                                    yield_if_needed().await;
                                    if measuring.get() {
                                        waits.borrow_mut().push(before.elapsed().as_nanos() as u64);
                                    }
                                }
                            } else {
                                x ^= x << 13;
                                x ^= x >> 7;
                                x ^= x << 17;
                                let d = Duration::from_micros(200 + x % 1800);
                                let deadline = Instant::now() + d;
                                sleep(d).await;
                                if measuring.get() {
                                    let late = Instant::now().saturating_duration_since(deadline);
                                    waits.borrow_mut().push(late.as_nanos() as u64);
                                }
                            }
                        }
                    },
                    latency,
                )
                .unwrap()
            };

            sleep(WARMUP).await;
            for queue in hog.iter() {
                executor().task_queue_stats(*queue).unwrap();
            }
            let _ = executor().task_queue_stats(latency).unwrap();
            measuring.set(true);
            let start = Instant::now();
            sleep(run).await;
            measuring.set(false);
            let wall = start.elapsed();
            stop.set(true);
            let hog_stats = hog.map(|queue| executor().task_queue_stats(queue).unwrap());
            let latency_stats = executor().task_queue_stats(latency).unwrap();
            latency_task.await;
            for task in tasks {
                task.await;
            }

            let mut waits = waits.take();
            waits.sort_unstable();
            let us = |ns: u64| ns as f64 / 1000.0;
            let bound = executor()
                .slot_wait_bound(latency)
                .map(|d| format!("{:.0}", d.as_secs_f64() * 1e6))
                .unwrap_or_else(|| "-".into());
            let mut line = format!(
                "policy={policy} cpu={cpu_queues} overrun={} latency={} samples={} \
                 p50_us={:.1} p99_us={:.1} max_us={:.1} bound_us={bound} units_per_s={:.0} \
                 latency_overruns={}",
                u8::from(overrun),
                if busy { "busy" } else { "sleep" },
                waits.len(),
                us(percentile(&waits, 0.5)),
                us(percentile(&waits, 0.99)),
                us(waits.last().copied().unwrap_or(0)),
                units.get() as f64 / wall.as_secs_f64(),
                latency_stats.frame_overruns(),
            );
            if let Some(hog) = hog_stats {
                line += &format!(
                    " hog_share={:.3} hog_overruns={}",
                    hog.runtime().as_secs_f64() / wall.as_secs_f64(),
                    hog.frame_overruns()
                );
            }
            line
        })
        .unwrap()
        .join()
        .unwrap();
    println!("{line}");
}
