//! Fixed-iteration workloads for `perf stat`, to price the cyclic policy's
//! tick and check that no per-step cost grows with depth.
//!
//! ```sh
//! cargo build --release --example cyclic_probe
//! SCHED=c:1000:100 taskset -c 20 perf stat -e instructions:u \
//!     target/release/examples/cyclic_probe <mode> <n> [width]
//! ```
//!
//! `SCHED` is `v` (vruntime, the default), `v:<us>` (vruntime with that
//! preempt timer) or `c:<minor us>:<major>` (cyclic). Run each workload at
//! two values of `n` and divide the difference in instructions by the
//! difference in `n`, which cancels setup.
//!
//! * `pingpong n`: n one-byte TCP round trips on this executor.
//! * `yieldn n width`: `width` tasks in one queue, n task runs in all, each
//!   run ending its queue's slice: the queue-picking path, once per run.
//! * `taskyield n width`: `width` tasks in one queue, n task runs in all,
//!   each yielding only the task, so many runs share a slice and the tick
//!   comes once a frame.
//! * `yieldq n width`: `width` queues of one task each, n task runs in all,
//!   each ending its queue's slice.

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    executor,
    net::{TcpListener, TcpStream},
    spawn_local, spawn_local_into, Latency, LocalExecutorBuilder, Placement, Scheduling, Shares,
};
use std::{hint::black_box, time::Duration};

fn builder() -> LocalExecutorBuilder {
    let builder = LocalExecutorBuilder::new(Placement::Unbound);
    let sched = std::env::var("SCHED").unwrap_or_else(|_| "v".into());
    let parts: Vec<&str> = sched.split(':').collect();
    match parts.as_slice() {
        ["v"] => builder,
        ["v", us] => builder.preempt_timer(Duration::from_micros(us.parse().unwrap())),
        ["c", minor, major] => builder.scheduling(Scheduling::Cyclic {
            minor: Duration::from_micros(minor.parse().unwrap()),
            major: major.parse().unwrap(),
        }),
        _ => panic!("SCHED: v, v:<us> or c:<minor us>:<major>"),
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap();
    let n: u64 = args.next().unwrap().parse().unwrap();
    let width: u64 = args.next().map(|a| a.parse().unwrap()).unwrap_or(2);
    builder()
        .spawn(move || async move {
            match mode.as_str() {
                "pingpong" => {
                    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                    let addr = listener.local_addr().unwrap();
                    let echo = spawn_local(async move {
                        let mut stream = listener.accept().await.unwrap();
                        let mut byte = [0u8; 1];
                        loop {
                            match stream.read(&mut byte).await.unwrap() {
                                0 => break,
                                _ => stream.write_all(&byte).await.unwrap(),
                            }
                        }
                    });
                    let mut client = TcpStream::connect(addr).await.unwrap();
                    client.set_nodelay(true).unwrap();
                    let mut byte = [0u8; 1];
                    for _ in 0..n {
                        client.write_all(b"x").await.unwrap();
                        client.read_exact(&mut byte).await.unwrap();
                        black_box(&byte);
                    }
                    client.close().await.unwrap();
                    echo.await;
                }
                "yieldn" | "taskyield" => {
                    let per = n / width;
                    let queue_yield = mode == "yieldn";
                    let handles: Vec<_> = (0..width)
                        .map(|_| {
                            spawn_local(async move {
                                for _ in 0..per {
                                    if queue_yield {
                                        executor().yield_task_queue_now().await;
                                    } else {
                                        executor().yield_now().await;
                                    }
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await;
                    }
                }
                "yieldq" => {
                    let per = n / width;
                    let handles: Vec<_> = (0..width)
                        .map(|i| {
                            let queue = executor().create_task_queue(
                                Shares::Static(1000),
                                Latency::NotImportant,
                                &format!("q{i}"),
                            );
                            spawn_local_into(
                                async move {
                                    for _ in 0..per {
                                        executor().yield_task_queue_now().await;
                                    }
                                },
                                queue,
                            )
                            .unwrap()
                        })
                        .collect();
                    for h in handles {
                        h.await;
                    }
                }
                _ => panic!("mode: pingpong, yieldn, taskyield or yieldq"),
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
