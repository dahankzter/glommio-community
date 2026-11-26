//! A fixed-frame ("cyclic executive") scheduling policy, opt-in beside the
//! vruntime scheduler.
//!
//! Time is cut into minor frames, one preempt-timer tick each, and a table
//! names the task queue that owns each frame of the major frame, the cycle the
//! table repeats over. The owner of the current frame has first claim on the
//! CPU; when it has nothing runnable the frame goes to the other queues by the
//! vruntime scheduler's order. A queue that keeps the CPU past the end of a
//! frame is charged the overrun against its own next slots.
//!
//! The table is built from the queues' shares and latency classes whenever
//! the set of queues or their shares change, in O(queues + frames). Choosing
//! the queue for a slice is an index into it: no step of the scheduler scans
//! queues or tasks.

use super::TaskQueue;
use crate::Latency;
use ahash::AHashMap;
use std::{cell::RefCell, rc::Rc, time::Duration, time::Instant};

/// How an executor chooses which task queue runs next.
///
/// Set with [`LocalExecutorBuilder::scheduling`]. Experimental.
///
/// [`LocalExecutorBuilder::scheduling`]: crate::LocalExecutorBuilder::scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Scheduling {
    /// Queues run by virtual runtime weighted by their shares, and a slice
    /// ends when the preempt timer fires. The default.
    #[default]
    Vruntime,
    /// A fixed-frame schedule, a cyclic executive.
    ///
    /// The preempt timer ticks every `minor`, and each tick is a frame. A
    /// table of `major` frames names the queue that owns each one: slots in
    /// proportion to the queues' [`Shares`], spread evenly, and for a
    /// [`Latency::Matters`] queue one at least every so often as its latency
    /// asks, rounded to frames. The owner of the current frame runs first; a
    /// frame whose owner has nothing runnable goes to the other queues by
    /// virtual runtime, so no CPU is idled. The preempt timer set on the
    /// builder is not used.
    ///
    /// A queue whose tasks yield within a frame waits at most the longest
    /// gap between two of its slots plus one frame, whatever the load:
    /// [`ExecutorProxy::slot_wait_bound`] reports it. The guarantee is
    /// conditional on cooperation. A task that computes without awaiting
    /// keeps the CPU past the end of its frame and delays every queue; the
    /// scheduler cannot stop it, only measure it. Each such overrun is
    /// charged against the next slots of the queue that overran, so it gains
    /// nothing by it, and counted in [`TaskQueueStats::frame_overruns`].
    ///
    /// [`Shares`]: crate::Shares
    /// [`ExecutorProxy::slot_wait_bound`]: crate::ExecutorProxy::slot_wait_bound
    /// [`TaskQueueStats::frame_overruns`]: crate::TaskQueueStats::frame_overruns
    Cyclic {
        /// The length of a frame, one preempt-timer tick.
        minor: Duration,
        /// The number of frames in the table.
        major: u32,
    },
}

/// The table entry of a frame nobody owns.
pub(super) const NO_OWNER: usize = usize::MAX;

/// A queue as the table builder sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Member {
    /// The queue's index in the executor's queue map.
    pub(super) index: usize,
    /// Its current shares.
    pub(super) shares: u64,
    /// For a [`Latency::Matters`] queue, the most frames apart its slots
    /// may start.
    pub(super) spacing: Option<u32>,
}

/// A schedule table: the owner of each frame, and for each queue the longest
/// start-to-start gap between two of its slots, in frames.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Table {
    pub(super) frames: Vec<usize>,
    pub(super) gaps: AHashMap<usize, u32>,
}

/// Finds free frames: `next[i]` leads to the first free frame at or after
/// `i`, with path halving, so placing every slot costs near-constant time
/// each. Frame `len` is a sentinel that is always free.
struct NextFree {
    next: Vec<u32>,
}

impl NextFree {
    fn new(len: usize) -> Self {
        Self {
            next: (0..=len as u32).collect(),
        }
    }

    fn find(&mut self, mut at: usize) -> usize {
        while self.next[at] as usize != at {
            let up = self.next[at] as usize;
            self.next[at] = self.next[up];
            at = up;
        }
        at
    }

    fn take(&mut self, at: usize) {
        self.next[at] = at as u32 + 1;
    }
}

/// The most of a table latency queues may own together, as a divisor: half.
///
/// A latency queue's spacing is a reservation the queue claims whether or
/// not it is busy, so one that asks for a slot every frame and always has
/// work would own every frame and starve the rest, the executor's own main
/// task included: measured, a CPU-heavy queue beside it did nothing at all.
/// When the latency queues ask for more than this, every spacing is widened
/// by the same factor until they fit, and the bound reported widens with
/// it. `Latency::Matters` carries no budget, so the table has to impose one.
const LATENCY_SHARE_DIVISOR: usize = 2;

/// Builds the table for `members` over `major` frames.
///
/// Latency queues are placed first, tightest first, each at its spacing from
/// the first free frame, a slot that lands on a taken frame moving to the next
/// free one, their spacings widened alike if together they would own more
/// than half the table. The frames left are divided among the other queues in proportion
/// to their shares, by cumulative rounding so they add up exactly. Each
/// queue's slots then go at evenly spaced ideal positions among those frames,
/// the queues' phases staggered so that queues of one rate interleave, the
/// queues with the most slots placed first since a displaced slot costs them
/// the most, and a slot whose ideal frame is taken moving to the next free
/// one. A queue whose shares round to no slot, or a latency queue that finds
/// no free frame, gets frames only when their owners leave them idle.
///
/// O(members + frames), with no sort that is not a counting sort.
pub(super) fn build_table(members: &[Member], major: u32) -> Table {
    let len = major.max(1) as usize;
    let mut frames = vec![NO_OWNER; len];
    let mut free = NextFree::new(len);

    let demand: usize = members
        .iter()
        .filter_map(|m| m.spacing)
        .map(|spacing| len.div_ceil((spacing as usize).clamp(1, len)))
        .sum();
    let widen = demand.div_ceil((len / LATENCY_SHARE_DIVISOR).max(1)).max(1);
    let mut by_spacing: Vec<Vec<usize>> = vec![Vec::new(); len + 1];
    for (at, member) in members.iter().enumerate() {
        if let Some(spacing) = member.spacing {
            by_spacing[(spacing as usize).clamp(1, len)].push(at);
        }
    }
    let mut placed = 0;
    for at in by_spacing.into_iter().flatten() {
        let member = &members[at];
        let spacing = ((member.spacing.unwrap() as usize).clamp(1, len) * widen).min(len);
        let first = free.find(0);
        if first == len {
            break;
        }
        let mut ideal = first;
        while ideal < len {
            let frame = free.find(ideal);
            if frame == len {
                break;
            }
            frames[frame] = member.index;
            free.take(frame);
            placed += 1;
            ideal += spacing;
        }
    }

    let free_frames: Vec<usize> = (0..len).filter(|&f| frames[f] == NO_OWNER).collect();
    let spare = free_frames.len();
    debug_assert_eq!(spare, len - placed);
    let others: Vec<&Member> = members.iter().filter(|m| m.spacing.is_none()).collect();
    let total: u128 = others.iter().map(|m| u128::from(m.shares.max(1))).sum();
    if spare > 0 && total > 0 {
        let mut slots = Vec::with_capacity(others.len());
        let mut cumulative = 0u128;
        let mut given = 0usize;
        for member in &others {
            cumulative += u128::from(member.shares.max(1));
            let upto = ((cumulative * spare as u128 + total / 2) / total) as usize;
            slots.push(upto - given);
            given = upto;
        }
        debug_assert_eq!(given, spare);

        let q = others.len();
        let mut by_slots: Vec<Vec<usize>> = vec![Vec::new(); spare + 1];
        for (k, &n) in slots.iter().enumerate() {
            if n > 0 {
                by_slots[n].push(k);
            }
        }
        let mut order = vec![NO_OWNER; spare];
        let mut open = NextFree::new(spare);
        for k in by_slots.into_iter().rev().flatten() {
            let n = slots[k];
            for j in 0..n {
                let ideal = ((2 * (j * q + k) + 1) * spare) / (2 * q * n);
                let mut at = open.find(ideal);
                if at == spare {
                    at = open.find(0);
                }
                order[at] = others[k].index;
                open.take(at);
            }
        }
        for (frame, owner) in free_frames.into_iter().zip(order) {
            frames[frame] = owner;
        }
    }

    Table {
        gaps: gaps(&frames),
        frames,
    }
}

/// The longest start-to-start gap, in frames and around the cycle, between
/// two slots of each queue that owns any. A queue with one slot has the whole
/// cycle as its gap.
fn gaps(frames: &[usize]) -> AHashMap<usize, u32> {
    let len = frames.len();
    let mut first: AHashMap<usize, usize> = AHashMap::new();
    let mut last: AHashMap<usize, usize> = AHashMap::new();
    let mut gaps: AHashMap<usize, u32> = AHashMap::new();
    for (frame, &owner) in frames.iter().enumerate() {
        if owner == NO_OWNER {
            continue;
        }
        if let Some(previous) = last.insert(owner, frame) {
            let gap = gaps.entry(owner).or_default();
            *gap = (*gap).max((frame - previous) as u32);
        } else {
            first.insert(owner, frame);
        }
    }
    for (owner, frame) in last {
        let around = (first[&owner] + len - frame) as u32;
        let gap = gaps.entry(owner).or_default();
        *gap = (*gap).max(around);
    }
    gaps
}

/// The table frames `latency` asks a queue's slots to start at most apart,
/// given frames of `minor`.
pub(super) fn spacing(latency: Latency, minor: Duration) -> Option<u32> {
    match latency {
        Latency::NotImportant => None,
        Latency::Matters(d) => {
            let frames = d.as_nanos() / minor.as_nanos().max(1);
            Some(frames.clamp(1, u128::from(u32::MAX)) as u32)
        }
    }
}

/// An executor's cyclic schedule and where it is in it.
#[derive(Debug)]
pub(super) struct Cyclic {
    pub(super) minor: Duration,
    major: u32,
    table: Table,
    /// The table is stale: queues were created or removed, or their shares
    /// changed. It is rebuilt before the next pick.
    pub(super) stale: bool,
    /// How many times the preempt timer has fired, as of the current frame:
    /// the frame's place in the table.
    fires: u64,
    /// How many times the preempt timer has been armed, as of the current
    /// frame. An arm without a fire restarts the frame without moving on.
    arms: u64,
    /// When the current frame began, so when its preempt timer fires.
    frame_start: Instant,
}

/// How far past the end of its frame a slice may end and still count as
/// having yielded in time: a slice that ends on a task's yield always ends a
/// little after the timer fired, by up to the task's last stretch between
/// checks. Within this, nothing is charged.
///
/// Charging all lateness was tried first, and charged queues for their
/// normal granularity. This helps only queues that yield well within an
/// eighth of a frame: beside a 100 us frame, a queue that yields every 20 us
/// still overruns by this measure, is charged, and gives up an occasional
/// slot, so a busy latency queue beside 16 CPU-heavy queues waited up to
/// 1.98 ms against its 1.10 ms bound with or without the tolerance. The frame
/// has to be several times longer than the longest stretch between yields.
const OVERRUN_TOLERANCE_DIVISOR: u32 = 8;

impl Cyclic {
    pub(super) fn new(minor: Duration, major: u32) -> Self {
        Self {
            minor,
            major: major.max(1),
            table: Table::default(),
            stale: true,
            fires: u64::MAX,
            arms: u64::MAX,
            frame_start: Instant::now(),
        }
    }

    /// Rebuilds the table from `queues` if it is stale. Each queue's shares
    /// as built are kept with it, so that a change of dynamic shares is
    /// noticed when it runs.
    ///
    /// The members are sorted by index so the table does not depend on the
    /// queue map's iteration order: O(q log q) on a rebuild, never per step.
    #[cold]
    pub(super) fn rebuild<'a>(&mut self, queues: impl Iterator<Item = &'a Rc<RefCell<TaskQueue>>>) {
        let mut members: Vec<Member> = queues
            .map(|queue| {
                let state = queue.borrow();
                state.cyclic_shares.set(state.stats.reciprocal_shares);
                Member {
                    index: state.index(),
                    shares: state.stats.current_shares() as u64,
                    spacing: spacing(state.io_requirements.latency_req, self.minor),
                }
            })
            .collect();
        members.sort_unstable_by_key(|member| member.index);
        self.table = build_table(&members, self.major);
        self.stale = false;
    }

    /// Moves to the frame the preempt timer's counts name, and returns the
    /// queue index that owns it. Reads the time only when the frame changes.
    ///
    /// Each fire of the timer is a frame boundary and moves one place along
    /// the table. An arm without a fire, after a park or a cancelled timer,
    /// restarts the current frame in place.
    ///
    /// Open decision 5, ending a slice early when its owner goes idle and an
    /// event waits for another queue, would peek at the completion queue tail
    /// and the timer wheel's next deadline between tasks, beside
    /// `need_preempt`. Not built: the latency ring's own events already end a
    /// slice early, and the owner then gets its claim back.
    pub(super) fn enter(&mut self, fires: u64, arms: u64) -> usize {
        if arms != self.arms || fires != self.fires {
            self.fires = fires;
            self.arms = arms;
            self.frame_start = Instant::now();
        }
        let frames = &self.table.frames;
        if frames.is_empty() {
            return NO_OWNER;
        }
        frames[(fires % frames.len() as u64) as usize]
    }

    /// Whether a runnable queue that owes `debt` and last paid in frame
    /// `paid` gives up the current frame, which it was about to get as its
    /// owner or as the least queue taking a frame left idle.
    ///
    /// A queue that owes a whole frame or more pays one frame of it by giving
    /// the frame up, and gives up the rest of a frame it has paid with, so it
    /// pays at most once a frame however often the frame is handed out.
    pub(super) fn skips(&self, debt: &mut Duration, paid: &mut u64) -> bool {
        if *paid == self.fires {
            return true;
        }
        if *debt >= self.minor {
            *debt -= self.minor;
            *paid = self.fires;
            return true;
        }
        false
    }

    /// Charges a slice that ended at `end` for what it ran past the end of
    /// its frame, adding it to `debt`, if
    /// that is more than an eighth of a frame: an overrun, which it returns
    /// whether it was. The debt is capped at one major frame, which the
    /// queue has also been charged in vruntime: one long stall does not lock
    /// a queue out of its slots for longer than a cycle.
    pub(super) fn charge(&self, end: Instant, debt: &mut Duration) -> bool {
        let late = end.saturating_duration_since(self.frame_start + self.minor);
        if late <= self.minor / OVERRUN_TOLERANCE_DIVISOR {
            return false;
        }
        let cap = self.minor.saturating_mul(self.major);
        *debt = (*debt + late).min(cap);
        true
    }

    /// The longest a queue that yields in time waits for its next slot: the
    /// longest gap between two of its slots plus one frame. `None` for a
    /// queue with no slot.
    pub(super) fn wait_bound(&self, index: usize) -> Option<Duration> {
        let gap = *self.table.gaps.get(&index)?;
        Some(self.minor.saturating_mul(gap.saturating_add(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(index: usize, shares: u64) -> Member {
        Member {
            index,
            shares,
            spacing: None,
        }
    }

    fn slots(table: &Table, index: usize) -> Vec<usize> {
        (0..table.frames.len())
            .filter(|&f| table.frames[f] == index)
            .collect()
    }

    #[test]
    fn slots_follow_shares() {
        let table = build_table(&[member(0, 1000), member(1, 3000)], 100);
        assert_eq!(slots(&table, 0).len(), 25);
        assert_eq!(slots(&table, 1).len(), 75);
        assert!(table.frames.iter().all(|&o| o != NO_OWNER));
    }

    #[test]
    fn slots_are_interleaved_not_blocked() {
        let table = build_table(&[member(0, 1), member(1, 1)], 10);
        assert_eq!(table.frames, vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 1]);
        assert_eq!(table.gaps[&0], 2);
        assert_eq!(table.gaps[&1], 2);
        let table = build_table(&[member(7, 1), member(8, 2), member(9, 1)], 8);
        assert_eq!(table.frames, vec![7, 8, 9, 8, 7, 8, 9, 8]);
        assert_eq!(table.gaps[&7], 4);
        assert_eq!(table.gaps[&8], 2);
        assert_eq!(table.gaps[&9], 4);
        let table = build_table(&[member(0, 1000), member(1, 8000), member(2, 1000)], 10);
        assert_eq!(slots(&table, 1).len(), 8);
        assert_eq!(table.gaps[&1], 2);
    }

    #[test]
    fn a_latency_queue_gets_a_slot_at_its_spacing() {
        let members = [
            member(0, 1000),
            Member {
                index: 1,
                shares: 1,
                spacing: Some(5),
            },
            member(2, 1000),
        ];
        let table = build_table(&members, 100);
        let latency = slots(&table, 1);
        assert_eq!(latency.len(), 20);
        assert_eq!(table.gaps[&1], 5);
        assert_eq!(slots(&table, 0).len(), 40);
        assert_eq!(slots(&table, 2).len(), 40);
    }

    #[test]
    fn two_latency_queues_at_one_spacing_do_not_collide() {
        let members = [
            Member {
                index: 1,
                shares: 1,
                spacing: Some(4),
            },
            Member {
                index: 2,
                shares: 1,
                spacing: Some(4),
            },
            member(3, 1),
        ];
        let table = build_table(&members, 20);
        assert_eq!(table.gaps[&1], 4);
        assert_eq!(table.gaps[&2], 4);
        assert_eq!(slots(&table, 3).len(), 10);
    }

    #[test]
    fn latency_queues_own_at_most_half_the_table() {
        let latency = |index, spacing| Member {
            index,
            shares: 1,
            spacing: Some(spacing),
        };
        let table = build_table(&[member(0, 1000), latency(1, 1)], 10);
        assert_eq!(slots(&table, 1).len(), 5);
        assert_eq!(table.gaps[&1], 2, "every frame asked, every other given");
        assert_eq!(slots(&table, 0).len(), 5);
        let table = build_table(&[member(0, 1000), latency(1, 2), latency(2, 4)], 20);
        assert_eq!(slots(&table, 1).len(), 5);
        assert_eq!(slots(&table, 2).len(), 3);
        assert_eq!(table.gaps[&1], 4);
        assert_eq!(slots(&table, 0).len(), 12);
        let table = build_table(&[member(0, 1000), latency(1, 4)], 20);
        assert_eq!(table.gaps[&1], 4, "within half, as asked");
    }

    #[test]
    fn a_tiny_share_may_get_no_slot() {
        let table = build_table(&[member(0, 10_000), member(1, 1)], 10);
        assert_eq!(slots(&table, 0).len(), 10);
        assert!(!table.gaps.contains_key(&1));
    }

    #[test]
    fn spacing_rounds_down_to_frames() {
        let minor = Duration::from_micros(100);
        assert_eq!(spacing(Latency::NotImportant, minor), None);
        assert_eq!(
            spacing(Latency::Matters(Duration::from_millis(1)), minor),
            Some(10)
        );
        assert_eq!(
            spacing(Latency::Matters(Duration::from_micros(150)), minor),
            Some(1)
        );
        assert_eq!(
            spacing(Latency::Matters(Duration::from_micros(10)), minor),
            Some(1)
        );
    }

    #[test]
    fn an_overrun_is_charged_and_capped() {
        let minor = Duration::from_millis(1);
        let mut cyclic = Cyclic::new(minor, 4);
        cyclic.enter(0, 0);
        let start = cyclic.frame_start;
        let mut debt = Duration::ZERO;
        assert!(!cyclic.charge(start + Duration::from_micros(900), &mut debt));
        assert_eq!(debt, Duration::ZERO);
        assert!(!cyclic.charge(start + Duration::from_micros(1050), &mut debt));
        assert_eq!(debt, Duration::ZERO, "yielded within an eighth of a frame");
        assert!(cyclic.charge(start + Duration::from_millis(3), &mut debt));
        assert_eq!(debt, Duration::from_millis(2));
        assert!(cyclic.charge(start + Duration::from_millis(100), &mut debt));
        assert_eq!(debt, Duration::from_millis(4));
    }

    #[test]
    fn a_queue_in_debt_gives_up_one_frame_per_frame() {
        let minor = Duration::from_millis(1);
        let mut cyclic = Cyclic::new(minor, 4);
        let mut debt = Duration::from_micros(2500);
        let mut paid = u64::MAX;
        cyclic.enter(0, 0);
        assert!(cyclic.skips(&mut debt, &mut paid));
        assert!(cyclic.skips(&mut debt, &mut paid), "the rest of the frame");
        assert_eq!(debt, Duration::from_micros(1500), "paid once");
        cyclic.enter(0, 1);
        assert!(
            cyclic.skips(&mut debt, &mut paid),
            "a re-arm is the same frame"
        );
        assert_eq!(debt, Duration::from_micros(1500));
        cyclic.enter(1, 2);
        assert!(cyclic.skips(&mut debt, &mut paid));
        assert_eq!(debt, Duration::from_micros(500));
        cyclic.enter(2, 3);
        assert!(
            !cyclic.skips(&mut debt, &mut paid),
            "less than a frame owed"
        );
        assert_eq!(debt, Duration::from_micros(500));
    }
}
