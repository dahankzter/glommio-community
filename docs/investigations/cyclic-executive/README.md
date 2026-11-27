# Cyclic executive prototype: results

2026-11-27. Prototype of the fixed-frame scheduling policy from `2026-11-25-design-cyclic-executive.md`, measured against the vruntime scheduler. An experiment: nothing pushed, nothing meant for upstream as is.

Branch `experiment/cyclic-executive`, on upstream main `79004f21`. Commits:

| commit | what |
|---|---|
| `87f34a48` | feat: add an opt-in fixed-frame scheduling policy (API, table, pick, charging, indexed heap, reactor counters, tests) |
| `17580f63` | fix: charge a cyclic slice only for an overrun, not for its last stretch |
| `b75b4970` | fix: let latency queues own at most half of a cyclic table |
| `79c17535` | test: measure the cyclic policy against the vruntime scheduler (the two examples) |
| `bf54b7ac` | perf: keep the standard heap for vruntime, read the time only per frame |

All GPG-signed and signed off, each with `Assisted-by: claude-opus-5.5`. `17580f63` was reworded in place before anything was pushed: its first message claimed the tolerance fixed the 1.98 ms case at 100 us frames, which the next measurement showed it did not.

## Summary

- **The bound holds and does not grow with load,** for queues that yield well within a frame. A latency queue with a backlog, beside 16 CPU-heavy queues, waits at most 1.10 ms between turns with 1 ms frames (p99 1.03 ms), against 16.4 ms under the vruntime scheduler with the same 1 ms tick. That is the case the policy exists for, and the only one where it clearly wins.
- **For an event-driven queue (idle, woken), the vruntime scheduler with a short tick is already better.** Since #241, a queue woken during a slice runs at the next boundary. Under the cyclic policy it waits for its own slot. Both stay within the cyclic bound, but vruntime's p99 is 104 us at a 100 us tick where cyclic's is 1.00 ms, beside 16 CPU-heavy queues. The real fault for this workload is elsewhere: the vruntime scheduler at its **default 100 ms timer** makes such a queue wait **100 ms**, because a sleeping latency queue does not shorten the timer, so its timer is noticed only when a CPU-heavy queue's 100 ms slice ends (the family of SCH-7 and issue #231, not the same case).
- **Work conservation is free.** CPU-heavy throughput matches the vruntime scheduler at the same tick to within 0.1%, and a queue that owns no frame at all ran 99.2% of the time beside idle owners.
- **Overrun charging works where frames are long compared with the yield granularity.** An overrunning queue went from 0.67 of the CPU without charging to 0.47 with it (fair: 0.5). The 1/8-frame tolerance fails at 100 us frames for a task yielding every 20 us: it is charged as an overrunner and misses slots (1.97 ms against a 1.10 ms bound).
- **The derived table needs a budget.** A `Latency::Matters(d)` queue with `d` no longer than the frame claimed every frame and starved everything, the main task included, until latency queues were capped at half the table.
- **The tick costs nothing per task run** and about 145 to 190 instructions per queue pick (11 ns, 1.1 to 1.2% of a ping-pong round trip) at either frame length. No per-step cost grows with run-queue depth. Across queues, the pick is O(log q) like the vruntime pick.
- **The default is unchanged within 1 to 2%** per queue pick, after a first version that cost it up to 22%.

## Design as built

### API (additive)

- `glommio::Scheduling { Vruntime (default), Cyclic { minor: Duration, major: u32 } }`, `#[non_exhaustive]`.
- `LocalExecutorBuilder::scheduling(Scheduling)`. Under `Cyclic`, the builder's `preempt_timer` is not used: the tick is `minor`.
- `ExecutorProxy::slot_wait_bound(handle) -> Option<Duration>`: the table bound, (longest start-to-start gap between the queue's slots + 1) x minor. `None` under vruntime or for a queue that owns no frame.
- `TaskQueueStats::frame_overruns()`: slices that ran past their frame by more than minor/8.
- The pool builder is not wired up (it always passes `Vruntime`).

### Structures

- `executor/cyclic.rs`: `Scheduling`, the table builder `build_table`, and `Cyclic`, the per-executor state: the table (owner per frame, gap per queue), a stale flag, the preempt-timer counts (fires, arms) as of the current frame, and when the frame began.
- `executor/queue_heap.rs`: `RunHeap`, an enum of `BinaryHeap` (vruntime, unchanged) and `QueueHeap`, an indexed binary heap (cyclic). Under the indexed heap each `TaskQueue` records its position in a `Cell<usize>`, so the frame's owner can be taken out wherever it sits in O(log q). Same `Ord`, which is total (entry breaks ties).
- `TaskQueue` gains `heap_slot`, `cyclic_shares` (shares the table was built with), `frame_debt`, `paid_frame`.
- `sys/uring.rs` `Reactor` counts preempt timers fired (`preempt_fires`, counted where a fired timer that was armed as a preempt timer is given up) and armed (`preempt_arms`). `Reactor::preempt_epoch()` exposes both.

### Where it hooks in

Three items the window model fingerprints changed, and their fingerprints were updated:

| item | change |
|---|---|
| `struct ExecutorQueues` | `active_executors: RunHeap`, new field `cyclic: Option<Box<Cyclic>>` |
| `fn run_one_task_queue` | `tq.active_executors.pop()` became `tq.pick_queue(&self.context.reactor)` |
| `fn finish_task_queue_run` | one call, `tq.account_frame(&queue, &self.context.reactor)` |

Under `Vruntime`, `pick_queue` is the plain pop and `account_frame` returns at once, so the modelled behaviour is unchanged; the model mirrors only that branch. Outside fingerprinted code: `reevaluate_preempt_timer` returns early under `Cyclic` (the tick is fixed), `reset_vruntimes` rebuilds whichever heap is in use, and `create_task_queue`/`remove_task_queue` mark the table stale.

### The rules

- **Frame clock.** A frame is one preempt-timer tick. The table position is `fires % major`; an arm without a fire (after a park) restarts the frame in place. The frame starts when the first pick after the tick sees the new counts, the only time a pick reads the clock.
- **Table.** Rebuilt lazily at the next pick after a queue is created or removed or a queue's dynamic shares change. Members sorted by index (determinism). Latency queues first, tightest first: a slot every `max(1, d / minor)` frames from the first free frame, a collision moving to the next free frame (a next-free array with path halving). The rest is split by shares with cumulative rounding (sums exactly), and each queue's slots go to evenly spaced ideal positions with staggered phases, queues with most slots placed first, collisions moving to the next free frame. O(q log q + frames).
- **Pick (per slice).** The counts (two `Cell` loads), `table[fires % len]`, one hash lookup. If the owner is in the heap and not paying debt: removed by position, run. Otherwise the heap's least queue, unless it is paying debt and another queue is runnable, in which case the next one (one extra pop and push, never more). O(log q).
- **Overrun.** At slice end, only if `need_preempt()` (the frame's timer may have fired), lateness = now minus (frame start + minor). Over minor/8 it is added to the queue's debt (capped at one major frame) and counted as an overrun; within it, nothing. A queue owing a frame or more gives up the next frame it would have run in, owned or slack, once per frame, and pays one frame of debt.
- **Within a slot** nothing changed: the queue's runnables in wake order, #241's window and hold intact.
- **Open decision 5** (mid-slice completion-queue peek) not built; a doc note on `Cyclic::enter` says where it would go.

### Decisions changed while building, and why

1. **Frame clock: preempt-timer fires, not wall time.** Wall-clock frames need the timer aligned to frame boundaries. The kept-armed timer drifts by a reactor pass per period (rearmed after each fire), so a frame now and then would get no boundary at all and its owner would lose its slot. Counting fires makes each boundary one tick by construction and keeps the kept-armed timer. It also makes charging exact: a 5 ms overrun is one long frame, not five lost frames belonging to other queues.
2. **The run heap had to support removal.** The owner is taken out of the heap wherever it sits. `BinaryHeap` cannot do that without a scan, so the cyclic policy uses an indexed heap. Used for both policies, it cost the vruntime pick 2% with one queue and 22% with 32 queues (perf stat against main). So vruntime keeps `BinaryHeap`, behind an enum.
3. **Debt is paid in slack frames too.** Charged only against owned slots, an overrunner still took slack frames by vruntime and overran in them again. Paying one frame per frame owed wherever it would run (owner claim or least-queue pick, skipping only the heap top, never scanning) cut the overrunner from 200 ms to 184 ms of CPU in the integration test. The effect is small.
4. **Overrun tolerance.** Charging all lateness charged polite queues for their yield granularity. Lateness within minor/8 is now neither charged nor counted. Where it does not help, see open decision 4.
5. **Latency queues own at most half the table.** Decision 2 asked for "a slot at least every d". With d no longer than the frame, a busy latency queue owned every frame. A CPU queue beside it did 0 chunks in 200 ms, and the ladder executor hung because its main task, in the default queue, never ran. Spacings are now widened alike until latency queues fit in half the table. `Matters(1ms)` at 1 ms frames gets every other frame (bound 3 ms, not 2 ms).
6. **The clock is read only when the frame changes.** Reading `Instant::now()` on every pick cost about 390 instructions per pick with the rest of it. Lateness is possible only if the frame's timer has fired, which `need_preempt()` shows until the next reactor pass, so the end of a slice reads the clock only then.
7. **Table build is O(q log q + frames),** not O(q + frames): members are sorted by queue index so the table does not depend on hash-map iteration order. Rebuild only.
8. **Shares are clamped to 1000** by `Shares::reciprocal_shares`, so a queue cannot ask for more than the default queue's share, which always owns its proportion of the table. Idle, its frames are slack. Busy (the main task doing work), it takes them.

## Tests

`glommio/tests/cyclic_scheduling.rs` (integration, drives the builder from outside the crate; the timing tests hold a lock and check the work happened before checking any bound), and unit tests in `executor/cyclic.rs` (table, charge, debt) and `executor/queue_heap.rs` (removal order).

| test | what it shows | final |
|---|---|---|
| `the_cyclic_policy_is_reachable_from_outside` | builder, `slot_wait_bound` (4 ms for `Matters(3ms)` at 1 ms frames), `frame_overruns`, `None` under vruntime | pass |
| `a_busy_latency_queue_waits_within_its_table_bound` | busy `Matters(3ms)` queue beside 8 CPU-heavy queues waits at most bound + 1 ms | pass, longest 2.0 to 2.1 ms against 4 ms |
| `an_overrunning_queue_is_charged_and_the_others_keep_their_bound` | a 4 ms spinner gets at most 0.6 of two equal queues' CPU, its overruns counted, latency wait within bound + one overrun | pass: hog 184 ms against 207 ms, 46 overruns in 46 spins, polite 0, longest 5.0 to 7.1 ms against 9 ms allowed |
| `idle_owners_leave_their_frames_to_the_others` | a queue owning no frame gets the CPU when the owners are idle | pass, 99.2% of wall |
| `a_busy_latency_queue_tighter_than_a_frame_does_not_starve_the_others` | decision change 5 | pass, 1980 chunks in 200 ms |

Ten consecutive runs of the integration file passed before the later changes, and it passed on every run since.

### Failing first

| mutation or earlier code | test that fails | how |
|---|---|---|
| pick is a plain pop (the policy accepted but vruntime underneath) | busy latency bound | waited 8.10 ms against 4 ms |
| no charging | overrun charged | hog had 0.67 of the CPU (304 ms against 150 ms) |
| strict: an idle owner's frame not given away | all four | "parking with N runnable task queues" debug assert: strict needs an idle wait the run loop does not have |
| debt paid only in owned slots (first design) | none | passes; hog 200 ms against 184 ms with slack payment, latency waits alike |
| no latency cap (before `b75b4970`) | tight latency | the CPU queue did 0 chunks |
| charge all lateness (before the tolerance) | `an_overrun_is_charged_and_capped` unit | 50 us of debt for a slice 50 us late in a 1 ms frame |
| idle-owner test as first written, Shares 8000 | itself | shares clamp to 1000: rewritten to use a queue with no frame |

## Measurements

Box: AMD Threadripper PRO 9975WX, kernel 7.3.3, release builds, one executor pinned with `taskset -c 20`, nothing else heavy running. Programs: `glommio/examples/cyclic_ladder.rs` and `glommio/examples/cyclic_probe.rs`. They are examples rather than criterion benches because the deliverables are a wait distribution and instruction counts, not a time per iteration.

### Latency and throughput

Each configuration ran 3 times for 2 s after 200 ms of warm-up, interleaved: the three repetitions are the outer loop, and within each the five policies alternate for every shape. p50 and p99 are the median over the runs, max the maximum, throughput the median. The latency queue is `Latency::Matters(1ms)`, shares 1000. Each CPU-heavy queue does about 50 us of fixed work (118 units) and then calls `yield_if_needed`. With overrun, one more queue spins 5 ms at a time without yielding. "sleep" is a queue that sleeps 0.2 to 2 ms and records how late it ran. "busy" is a queue that always has work and records how long each preempted yield took.

Policies: vruntime at its default 100 ms timer; vruntime with the preempt timer at 1 ms and at 100 us (the fair comparisons, same tick); cyclic at 1 ms x 100 frames and 100 us x 1000 frames. The bound column is `slot_wait_bound`: 3 ms at 1 ms frames (every other frame, after the cap) and 1.1 ms at 100 us (every 10th frame).

#### latency queue: sleep, overrun 0

| policy | CPU queues | p50 | p99 | max | bound | samples | CPU units/s |
|---|---|---|---|---|---|---|---|
| vruntime, 100 ms timer | 1 | 718 us | 99.93 ms | 99.93 ms | - | 43 | 2.314 M |
| vruntime, 1 ms timer | 1 | 467 us | 1.00 ms | 1.02 ms | - | 1274 | 2.309 M |
| cyclic 1 ms x 100 | 1 | 648 us | 1.92 ms | 2.03 ms | 3.00 ms | 1086 | 2.309 M |
| vruntime, 100 us timer | 1 | 54 us | 104 us | 108 us | - | 1728 | 2.272 M |
| cyclic 100 us x 1000 | 1 | 97 us | 207 us | 213 us | 1.10 ms | 1665 | 2.270 M |
| vruntime, 100 ms timer | 4 | 721 us | 99.95 ms | 99.97 ms | - | 43 | 2.313 M |
| vruntime, 1 ms timer | 4 | 468 us | 1.00 ms | 1.33 ms | - | 1274 | 2.309 M |
| cyclic 1 ms x 100 | 4 | 775 us | 1.85 ms | 2.05 ms | 3.00 ms | 1025 | 2.308 M |
| vruntime, 100 us timer | 4 | 54 us | 104 us | 199 us | - | 1728 | 2.272 M |
| cyclic 100 us x 1000 | 4 | 221 us | 599 us | 724 us | 1.10 ms | 1495 | 2.270 M |
| vruntime, 100 ms timer | 16 | 754 us | 99.95 ms | 99.95 ms | - | 43 | 2.313 M |
| vruntime, 1 ms timer | 16 | 471 us | 1.01 ms | 1.61 ms | - | 1273 | 2.309 M |
| cyclic 1 ms x 100 | 16 | 921 us | 1.83 ms | 2.03 ms | 3.00 ms | 991 | 2.308 M |
| vruntime, 100 us timer | 16 | 54 us | 104 us | 109 us | - | 1728 | 2.270 M |
| cyclic 100 us x 1000 | 16 | 407 us | 1.00 ms | 1.04 ms | 1.10 ms | 1309 | 2.270 M |

#### latency queue: sleep, overrun 1

| policy | CPU queues | p50 | p99 | max | bound | samples | CPU units/s | hog CPU share |
|---|---|---|---|---|---|---|---|---|
| vruntime, 100 ms timer | 1 | 3.98 ms | 99.80 ms | 99.95 ms | - | 192 | 1.158 M | 0.499 |
| vruntime, 1 ms timer | 1 | 604 us | 4.93 ms | 4.99 ms | - | 813 | 1.154 M | 0.500 |
| cyclic 1 ms x 100 | 1 | 790 us | 5.79 ms | 6.02 ms | 3.00 ms | 686 | 1.161 M | 0.497 |
| vruntime, 100 us timer | 1 | 66 us | 4.95 ms | 5.00 ms | - | 1005 | 1.141 M | 0.497 |
| cyclic 100 us x 1000 | 1 | 89 us | 5.11 ms | 5.21 ms | 1.10 ms | 1093 | 1.345 M | 0.407 |
| vruntime, 100 ms timer | 4 | 3.85 ms | 99.87 ms | 99.97 ms | - | 81 | 1.743 M | 0.247 |
| vruntime, 1 ms timer | 4 | 498 us | 4.70 ms | 4.99 ms | - | 1092 | 1.847 M | 0.200 |
| cyclic 1 ms x 100 | 4 | 800 us | 5.63 ms | 6.01 ms | 3.00 ms | 880 | 1.847 M | 0.200 |
| vruntime, 100 us timer | 4 | 58 us | 4.74 ms | 5.00 ms | - | 1441 | 1.818 M | 0.200 |
| cyclic 100 us x 1000 | 4 | 192 us | 5.19 ms | 5.52 ms | 1.10 ms | 1273 | 1.821 M | 0.197 |
| vruntime, 100 ms timer | 16 | 1.07 ms | 100.64 ms | 100.66 ms | - | 43 | 2.201 M | 0.049 |
| vruntime, 1 ms timer | 16 | 476 us | 4.29 ms | 4.96 ms | - | 1220 | 2.176 M | 0.057 |
| cyclic 1 ms x 100 | 16 | 876 us | 4.73 ms | 5.97 ms | 3.00 ms | 965 | 2.176 M | 0.057 |
| vruntime, 100 us timer | 16 | 55 us | 4.11 ms | 4.99 ms | - | 1645 | 2.141 M | 0.057 |
| cyclic 100 us x 1000 | 16 | 431 us | 5.12 ms | 5.91 ms | 1.10 ms | 1242 | 2.140 M | 0.057 |

#### latency queue: busy, overrun 0

| policy | CPU queues | p50 | p99 | max | bound | samples | CPU units/s |
|---|---|---|---|---|---|---|---|
| vruntime, 100 ms timer | 1 | 1.02 ms | 1.03 ms | 2.05 ms | - | 997 | 1.155 M |
| vruntime, 1 ms timer | 1 | 1.02 ms | 1.03 ms | 1.16 ms | - | 997 | 1.154 M |
| cyclic 1 ms x 100 | 1 | 1.02 ms | 1.03 ms | 1.05 ms | 3.00 ms | 997 | 1.154 M |
| vruntime, 100 us timer | 1 | 105 us | 110 us | 282 us | - | 9831 | 1.136 M |
| cyclic 100 us x 1000 | 1 | 105 us | 314 us | 521 us | 1.10 ms | 9829 | 1.135 M |
| vruntime, 100 ms timer | 4 | 4.09 ms | 4.10 ms | 4.17 ms | - | 414 | 1.831 M |
| vruntime, 1 ms timer | 4 | 4.09 ms | 4.10 ms | 4.18 ms | - | 398 | 1.848 M |
| cyclic 1 ms x 100 | 4 | 1.02 ms | 1.03 ms | 1.04 ms | 3.00 ms | 990 | 1.165 M |
| vruntime, 100 us timer | 4 | 416 us | 423 us | 623 us | - | 3932 | 1.818 M |
| cyclic 100 us x 1000 | 4 | 416 us | 938 us | 1.08 ms | 1.10 ms | 3932 | 1.817 M |
| vruntime, 100 ms timer | 16 | 15.33 ms | 16.38 ms | 16.39 ms | - | 122 | 2.168 M |
| vruntime, 1 ms timer | 16 | 16.36 ms | 16.38 ms | 16.39 ms | - | 117 | 2.173 M |
| cyclic 1 ms x 100 | 16 | 1.02 ms | 1.03 ms | 1.10 ms | 3.00 ms | 1000 | 1.165 M |
| vruntime, 100 us timer | 16 | 1.66 ms | 1.67 ms | 2.18 ms | - | 1157 | 2.138 M |
| cyclic 100 us x 1000 | 16 | 937 us | 944 us | 1.97 ms | 1.10 ms | 1927 | 2.047 M |

#### latency queue: busy, overrun 1

| policy | CPU queues | p50 | p99 | max | bound | samples | CPU units/s | hog CPU share |
|---|---|---|---|---|---|---|---|---|
| vruntime, 100 ms timer | 1 | 1.02 ms | 6.03 ms | 6.04 ms | - | 680 | 0.730 M | 0.342 |
| vruntime, 1 ms timer | 1 | 1.02 ms | 6.03 ms | 6.05 ms | - | 665 | 0.770 M | 0.332 |
| cyclic 1 ms x 100 | 1 | 1.02 ms | 5.00 ms | 5.01 ms | 3.00 ms | 780 | 0.794 M | 0.265 |
| vruntime, 100 us timer | 1 | 105 us | 5.11 ms | 5.12 ms | - | 6585 | 0.761 M | 0.330 |
| cyclic 100 us x 1000 | 1 | 105 us | 5.21 ms | 5.31 ms | 1.10 ms | 6585 | 0.760 M | 0.330 |
| vruntime, 100 ms timer | 4 | 4.09 ms | 9.10 ms | 9.12 ms | - | 345 | 1.505 M | 0.175 |
| vruntime, 1 ms timer | 4 | 4.09 ms | 9.10 ms | 9.11 ms | - | 332 | 1.537 M | 0.167 |
| cyclic 1 ms x 100 | 4 | 1.02 ms | 5.00 ms | 5.01 ms | 3.00 ms | 905 | 1.015 M | 0.107 |
| vruntime, 100 us timer | 4 | 416 us | 5.42 ms | 5.52 ms | - | 3283 | 1.517 M | 0.165 |
| cyclic 100 us x 1000 | 4 | 318 us | 5.73 ms | 5.84 ms | 1.10 ms | 3281 | 1.516 M | 0.165 |
| vruntime, 100 ms timer | 16 | 15.34 ms | 21.36 ms | 21.38 ms | - | 116 | 2.041 M | 0.057 |
| vruntime, 1 ms timer | 16 | 16.36 ms | 21.38 ms | 21.39 ms | - | 111 | 2.053 M | 0.055 |
| cyclic 1 ms x 100 | 16 | 1.02 ms | 5.00 ms | 5.00 ms | 3.00 ms | 966 | 1.123 M | 0.030 |
| vruntime, 100 us timer | 16 | 1.66 ms | 6.66 ms | 6.67 ms | - | 1093 | 2.021 M | 0.055 |
| cyclic 100 us x 1000 | 16 | 937 us | 5.83 ms | 5.84 ms | 1.10 ms | 1827 | 1.935 M | 0.055 |

Reading the tables:

- **Busy latency queue (the policy's case).** Under vruntime, its wait between turns is one slice per competing queue: 4.1 ms beside 4, 16.4 ms beside 16 at a 1 ms tick, 1.67 ms beside 16 at 100 us. Under cyclic it is flat: p99 1.03 ms, max 1.04 to 1.10 ms at 1 ms frames whatever the count, and 0.94 ms p99 at 100 us beside 16. The CPU-heavy queues lose what the latency queue is guaranteed: at 1 ms frames it owns half the table, so their throughput falls from 1.85M to 1.17M units/s. That is the reservation working, not overhead.
- **Busy latency queue, max beside 16 at 100 us: 1.97 ms against a 1.10 ms bound.** The latency task yields every 20 us, more than an eighth of a 100 us frame, so 5 to 22 of its slices a run count as overruns (none at 1 ms frames), are charged, and an occasional forfeited slot doubles a gap. See open decision 4.
- **Sleeping latency queue.** Vruntime at the same tick is better: p99 1.00 ms against cyclic's 1.83 to 1.92 ms at 1 ms, and 104 us against up to 1.01 ms at 100 us. The window from #241 runs a queue woken during a slice at the next boundary. Cyclic makes it wait for its next slot unless the owner of the current one is idle. Cyclic stays within its bound. Vruntime at the default 100 ms timer waits 100 ms, whichever queue count.
- **Overrun.** Every policy with a short tick is held to about 5 to 6 ms by the 5 ms spinner: the cooperative limit. A sleeping queue under cyclic waits a little longer (max 6.0 against 5.0 ms at 1 ms), the spinner plus the wait for its slot. Busy latency beside the spinner: vruntime adds the spinner to one slice per competitor (9.1 ms beside 4, 21.4 ms beside 16 at 1 ms), cyclic stays at 5.0 ms. The spinner's share under cyclic is at or below vruntime's.
- **Throughput.** With the latency queue sleeping, the CPU-heavy queues do the same work under both policies at the same tick, to within 0.1%. Work conservation costs nothing measurable. The 100 us tick costs about 1.6% against 1 ms under either policy.

### Tick cost and depth

`perf stat -e instructions:u,task-clock`, each workload at two sizes, the difference divided by the difference in iterations to cancel setup. Median of 3, range in brackets where it is not tight. `main` is the same probe built on upstream main `79004f21` in a throwaway worktree (no `Scheduling` arm). `yieldn` and `yieldq` end their queue's slice on every task run, so every run is a queue pick. `taskyield` yields only the task, so many runs share a slice and the tick comes once a frame.

instructions per iteration:

| workload | main (vruntime) | vruntime | vruntime 1 ms | cyclic 1 ms | vruntime 100 us | cyclic 100 us |
|---|---|---|---|---|---|---|
| ping-pong round trip | 19932 | 19952 | 19961 | 20189 | 20052 (20038-20057) | 20277 (20268-20296) |
| queue pick, 2 tasks queued | 924 | 934 | 934 | 1077 | 939 | 1082 |
| queue pick, 100 tasks | 924 | 934 | 934 | 1077 | 939 | 1082 |
| queue pick, 10k tasks | 924 | 934 | 934 | 1077 | 939 | 1082 |
| task run in a slice, 2 tasks | 382 | 378 | 378 | 378 | 379 | 379 |
| task run in a slice, 100 tasks | 382 | 378 | 378 | 378 | 379 | 379 |
| task run in a slice, 10k tasks | 382 | 378 | 378 | 378 | 379 | 379 |
| queue pick, 2 queues | 966 | 985 | 986 | 1136 | 990 | 1141 |
| queue pick, 8 queues | 1063 | 1070 | 1071 | 1241 (1231-1242) | 1075 | 1264 (1263-1268) |
| queue pick, 32 queues | 1128 | 1127 | 1127 | 1149 (1094-1205) | 1132 | 1195 (1191-1206) |

ns per iteration (task-clock):

| workload | main (vruntime) | vruntime | vruntime 1 ms | cyclic 1 ms | vruntime 100 us | cyclic 100 us |
|---|---|---|---|---|---|---|
| ping-pong round trip | 5095.2 | 5071.8 | 5095.8 | 5157.8 | 5174.4 | 5210.8 |
| queue pick, 2 tasks queued | 64.8 | 66.5 | 66.7 | 77.3 | 67.6 | 78.8 |
| queue pick, 100 tasks | 64.4 | 67.0 | 66.6 | 77.2 | 67.6 | 78.7 |
| queue pick, 10k tasks | 64.6 | 66.5 | 66.8 | 77.7 | 67.6 | 79.0 |
| task run in a slice, 2 tasks | 16.3 | 15.9 | 16.0 | 16.1 | 16.3 | 16.3 |
| task run in a slice, 100 tasks | 16.7 | 16.1 | 16.4 | 16.0 | 16.4 | 16.4 |
| task run in a slice, 10k tasks | 16.6 | 16.2 | 16.3 | 16.3 | 16.5 | 16.6 |
| queue pick, 2 queues | 66.7 | 67.4 | 67.3 | 78.9 | 68.9 | 80.1 |
| queue pick, 8 queues | 73.9 | 75.5 | 75.8 | 84.3 | 77.1 | 86.6 |
| queue pick, 32 queues | 80.8 | 81.8 | 81.8 | 80.1 | 83.5 | 83.1 |

Reading it:

- **The default is unchanged within 2%.** Vruntime on the branch matches main per pick with 32 queues, is 10 instructions (1.1%) over with one queue and 19 (2%) with two, and 0.1% per ping-pong round trip. Those are the policy branches in pick and finish. A task run within a slice is 4 instructions cheaper than on main, a layout effect.
- **Per task run within a slice, the tick costs nothing:** `taskyield` is the same under both policies, at both frame lengths and at 2, 100 and 10,000 queued tasks.
- **Per queue pick, cyclic costs 143 instructions (11 ns) more than a plain pop with one queue, 151 with 2 queues, 171 with 8,** and 22 with 32 (noisy there: 1094 to 1205). The cost is the owner lookup in a hash map, two `RefCell` borrows, removal by position and the frame check. It is identical at 2, 100 and 10,000 queued tasks.
- **Across queues (2, 8, 32), the pick grows as the heap's O(log q),** under both policies: that is the existing heap, not a scan.
- **Ping-pong pays 1.1 to 1.2% per round trip under cyclic** (237 instructions at 1 ms, 225 at 100 us, against vruntime at the same tick), a pick per reactor pass. Frame length barely matters here, since a round trip is far shorter than either frame. Before the clock reads were made lazy, it was 4.5%.

## The open decisions, from the numbers

1. **Idle slots: work-conserving.** Confirmed. It costs nothing measurable in throughput, and a strict variant would need an idle-wait the run loop does not have (the mutation tripped the park assertion). Keep work-conserving as the only mode until a user asks for strict.
2. **Table source.** The derived table works for shares. For latency it is unsafe without a budget: "a slot at least every d" is a reservation claimed whenever the queue is runnable, and with d at or under the frame it took everything. The half-table cap is a stopgap. Recommendation: give latency queues an explicit budget, slots per major frame or (budget, period) in the manner of SCHED_DEADLINE, and derive only the spacing from `d`. That is the explicit table, or close to it.
3. **Overrun policy: charge, with the tolerance tied to yield granularity.** Charging is necessary (0.67 to 0.47 of the CPU) and cheap. Paying debt in slack frames as well as owned ones helps a little. Demotion is not needed for fairness, since charging already holds the overrunner to its share. It does not help latency: every overrun delays everyone by its length (about 5 ms in every row), and only splitting the work or moving it off the executor fixes that. The counter is what points at the code.
4. **Minor frame length.** The tick itself is cheap at both: 0 per task run, about 140 instructions per pick, about 1.6% of throughput at 100 us. What limits the frame is the yield granularity: a frame must be at least 8 times the longest stretch between yields of any queue that relies on its bound, or that queue is charged as an overrunner and misses slots. 1 ms frames held every bound measured. 100 us frames broke the busy latency queue's (20 us chunks). Prefer 1 ms, or make the tolerance absolute.
5. **Events between boundaries.** Not built. The numbers argue for it more than anything else here: the sleeping queue's wait under cyclic is the wait for its next slot, where vruntime's window gets it the next boundary. A mid-frame check that ends the slice of a non-owner when an owner or a latency queue has been woken would let event-driven queues claim without waiting for their slot.

## Recommendation

Do not take the policy upstream as it stands. The one workload where it clearly wins, a latency queue with a backlog beside many CPU-heavy queues, is real but narrow. For event-driven latency queues, the vruntime scheduler with a short tick already does better.

What to do next, in order:

1. **Fix the vruntime scheduler's real gap first:** a sleeping `Latency::Matters` queue does not shorten the preempt timer, so at the default 100 ms it waits 100 ms. Arming the timer for the tightest latency of all queues, not only runnable ones (or arming a user timer on the latency ring while busy), would bring the event-driven case to the 1 ms row with no new policy. It belongs with SCH-7 and issue #231, the mid-pass case. It is the cheapest and largest win in these tables.
2. **If the cyclic policy is pursued,** give latency queues an explicit budget (open decision 2), tie the overrun tolerance to an absolute stretch, not a fraction of the frame (decision 4), and build the mid-frame claim for woken owners (decision 5). Then rerun `cyclic_ladder`: the sleeping rows should fall to the vruntime rows, the busy rows should hold.
3. **Store the owner's `Rc` in the table** instead of a queue index, to drop the hash lookup from the pick. Removal must then drop the table's references after releasing the queues' borrow, as `remove_task_queue` already does for the queue itself.
4. The `#[yield_points]` macro from the note is what makes the bound usable at all, and decision 4 says so: the bound holds only when every stretch between yields is short compared with the frame.
