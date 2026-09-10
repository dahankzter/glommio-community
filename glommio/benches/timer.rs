//! What the timer structure costs, in the shape glommio uses it.
//!
//! Instrumenting a server first said the workload is arming and cancelling
//! with almost no expiry: a read timeout is set per operation and withdrawn
//! when the read completes, so at 4096 connections every timer was cancelled
//! and none fired. These cases are weighted accordingly.
//!
//! Timed through `iter_custom` with the executor driven by hand rather than
//! `to_async`, because the measured window has to exclude building the timers
//! and dropping them, which are the other case in the same benchmark.

mod common;

use common::Glommio;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_lite::future::poll_once;
use glommio::timer::{sleep, Timer};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

const POPULATIONS: &[usize] = &[64, 256, 1_024, 4_096];

/// Far past any population the other cases use.
///
/// The question these answer is not how the structure behaves at the sizes a
/// server reaches today but whether its curve ever turns up. A million timers
/// is eight more levels of a B-tree than four thousand, so if `O(log n)` is
/// going to cost anything it has to cost it here.
const PREMISE_POPULATIONS: &[usize] = &[64, 4_096, 65_536, 262_144, 1_048_576];

/// Far enough out that nothing in these cases reaches it.
const PARKED: Duration = Duration::from_secs(3_600);

/// Registering a timer that will not fire.
fn arm(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/arm");
    group.sample_size(10).warm_up_time(Duration::from_secs(1));

    for &n in PREMISE_POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut timers: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();

                        let started = Instant::now();
                        for timer in timers.iter_mut() {
                            // The first poll is what registers it.
                            black_box(poll_once(timer).await);
                        }
                        total += started.elapsed();
                    }
                    total / n as u32
                })
            })
        });
    }
    group.finish();
}

/// Withdrawing a timer that has not fired, which the measured workload does to
/// every timer it creates.
fn cancel(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/cancel");
    group.sample_size(10).warm_up_time(Duration::from_secs(1));

    for &n in PREMISE_POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut timers: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();
                        for timer in timers.iter_mut() {
                            poll_once(timer).await;
                        }

                        let started = Instant::now();
                        drop(timers);
                        total += started.elapsed();
                    }
                    total / n as u32
                })
            })
        });
    }
    group.finish();
}

/// A short sleep with a population already waiting.
///
/// Two things show up here. A structure that answers "what is due next" by
/// scanning grows with the population. And one that reports the tick a
/// deadline was rounded into rather than the deadline itself puts a floor
/// under every sleep shorter than its resolution.
fn sleep_under_population(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/sleep_100us");
    group.sample_size(10).warm_up_time(Duration::from_secs(1));

    for &n in PREMISE_POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut parked: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();
                    for timer in parked.iter_mut() {
                        poll_once(timer).await;
                    }

                    let started = Instant::now();
                    for _ in 0..iters {
                        sleep(Duration::from_micros(100)).await;
                    }
                    let elapsed = started.elapsed();

                    drop(parked);
                    elapsed
                })
            })
        });
    }
    group.finish();
}

/// A short sleep with a population whose deadlines all fall in one slot.
///
/// [`PARKED`] deadlines are an hour out, which is inside the wheel's reach:
/// they share one level-3 slot rather than reaching the overflow map, and no
/// case here builds a populated slot at a level the reactor reads on a poll. Timers armed for the
/// same near deadline all land together, which is what a burst of connections
/// sharing a read timeout produces. Answering "what is due next" by scanning
/// that slot grows with the population; the wheel is meant not to.
fn sleep_under_clustered_population(c: &mut Criterion) {
    // Inside the finest level, so the whole population shares one slot, and far
    // enough out that nothing fires while a batch is measured.
    const CLUSTER: Duration = Duration::from_millis(200);
    const SLEEPS_PER_BATCH: usize = 400;

    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/sleep_100us_clustered");

    for &n in POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    let mut remaining = iters;
                    while remaining > 0 {
                        let this_batch = remaining.min(SLEEPS_PER_BATCH as u64);
                        // Rebuilt per batch, outside the timed region, because
                        // the cluster would otherwise come due mid-measurement.
                        let mut cluster: Vec<Timer> = (0..n).map(|_| Timer::new(CLUSTER)).collect();
                        for timer in cluster.iter_mut() {
                            black_box(poll_once(timer).await);
                        }

                        let started = Instant::now();
                        for _ in 0..this_batch {
                            sleep(Duration::from_micros(100)).await;
                        }
                        total += started.elapsed();

                        drop(cluster);
                        remaining -= this_batch;
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

/// A short sleep with a population clustered in a *coarse* level.
///
/// One second is past level 0, so the whole population shares one level-1
/// slot. Asking whether anything there has come due is what separates a slot
/// that answers from its cached minimum from one that is walked.
fn sleep_under_coarse_clustered_population(c: &mut Criterion) {
    const CLUSTER: Duration = Duration::from_secs(1);
    const SLEEPS_PER_BATCH: usize = 400;

    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/sleep_100us_coarse");

    for &n in POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    let mut remaining = iters;
                    while remaining > 0 {
                        let this_batch = remaining.min(SLEEPS_PER_BATCH as u64);
                        let mut cluster: Vec<Timer> = (0..n).map(|_| Timer::new(CLUSTER)).collect();
                        for timer in cluster.iter_mut() {
                            black_box(poll_once(timer).await);
                        }

                        let started = Instant::now();
                        for _ in 0..this_batch {
                            sleep(Duration::from_micros(100)).await;
                        }
                        total += started.elapsed();

                        drop(cluster);
                        remaining -= this_batch;
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    arm,
    cancel,
    sleep_under_population,
    sleep_under_clustered_population,
    sleep_under_coarse_clustered_population
);
criterion_main!(benches);
