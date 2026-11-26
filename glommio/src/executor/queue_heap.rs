//! The heap of runnable task queues, with removal by position.
//!
//! The vruntime scheduler only ever pops the least queue, which
//! [`std::collections::BinaryHeap`] does. The cyclic policy also takes the
//! queue that owns the current frame out of the heap wherever it sits, which
//! `BinaryHeap` cannot do without a scan. So each queue records its position
//! here, and removal is a swap with the last element and one sift: O(log q)
//! in the number of runnable queues, never a scan.
//!
//! The order is `TaskQueue`'s own `Ord`, whose greatest element is the queue
//! with the least vruntime, ties going to the one that entered first. Since
//! no two queues share an entry, that order is total and every correct heap
//! pops the same sequence. The vruntime scheduler keeps `BinaryHeap`, which
//! is cheaper when nothing is removed: see [`RunHeap`].

use super::TaskQueue;
use std::{cell::RefCell, collections::BinaryHeap, rc::Rc};

/// The run heap an executor uses: the standard heap under the vruntime
/// scheduler, which only ever pops, and the indexed one under the cyclic
/// policy, which also removes the frame's owner.
///
/// The indexed heap was tried for both first. Its position updates cost the
/// vruntime scheduler 2% per queue pick with one queue and 22% with 32, so it
/// is used only where removal is needed.
#[derive(Debug)]
pub(super) enum RunHeap {
    Plain(BinaryHeap<Rc<RefCell<TaskQueue>>>),
    Indexed(QueueHeap),
}

impl RunHeap {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Plain(heap) => heap.len(),
            Self::Indexed(heap) => heap.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Plain(heap) => heap.is_empty(),
            Self::Indexed(heap) => heap.is_empty(),
        }
    }

    pub(super) fn peek(&self) -> Option<&Rc<RefCell<TaskQueue>>> {
        match self {
            Self::Plain(heap) => heap.peek(),
            Self::Indexed(heap) => heap.peek(),
        }
    }

    /// The queues in no particular order.
    pub(super) fn iter(&self) -> std::slice::Iter<'_, Rc<RefCell<TaskQueue>>> {
        match self {
            Self::Plain(heap) => heap.as_slice().iter(),
            Self::Indexed(heap) => heap.iter(),
        }
    }

    #[inline]
    pub(super) fn push(&mut self, queue: Rc<RefCell<TaskQueue>>) {
        match self {
            Self::Plain(heap) => heap.push(queue),
            Self::Indexed(heap) => heap.push(queue),
        }
    }

    #[inline]
    pub(super) fn pop(&mut self) -> Option<Rc<RefCell<TaskQueue>>> {
        match self {
            Self::Plain(heap) => heap.pop(),
            Self::Indexed(heap) => heap.pop(),
        }
    }

    /// Restores the heap order after the keys of queues in it changed.
    pub(super) fn rebuild(&mut self) {
        match self {
            Self::Plain(heap) => {
                let items = std::mem::take(heap).into_vec();
                *heap = BinaryHeap::from(items);
            }
            Self::Indexed(heap) => heap.rebuild(),
        }
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        match self {
            Self::Plain(heap) => heap.clear(),
            Self::Indexed(heap) => heap.clear(),
        }
    }
}

/// The position a queue outside the heap records.
pub(super) const NOT_IN_HEAP: usize = usize::MAX;

/// A max-heap of task queues by `TaskQueue`'s `Ord`, each queue knowing its
/// own position.
#[derive(Debug, Default)]
pub(super) struct QueueHeap {
    items: Vec<Rc<RefCell<TaskQueue>>>,
}

impl QueueHeap {
    pub(super) fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(super) fn peek(&self) -> Option<&Rc<RefCell<TaskQueue>>> {
        self.items.first()
    }

    /// The queues in no particular order.
    pub(super) fn iter(&self) -> std::slice::Iter<'_, Rc<RefCell<TaskQueue>>> {
        self.items.iter()
    }

    pub(super) fn push(&mut self, queue: Rc<RefCell<TaskQueue>>) {
        let at = self.items.len();
        queue.borrow().heap_slot.set(at);
        self.items.push(queue);
        self.sift_up(at);
    }

    pub(super) fn pop(&mut self) -> Option<Rc<RefCell<TaskQueue>>> {
        self.take(0)
    }

    /// Takes `queue` out of the heap if it is in it.
    pub(super) fn remove(&mut self, queue: &Rc<RefCell<TaskQueue>>) -> bool {
        let at = queue.borrow().heap_slot.get();
        if at >= self.items.len() || !Rc::ptr_eq(&self.items[at], queue) {
            return false;
        }
        self.take(at).is_some()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        for queue in self.items.drain(..) {
            queue.borrow().heap_slot.set(NOT_IN_HEAP);
        }
    }

    /// Restores the heap order after the keys of queues in it changed.
    pub(super) fn rebuild(&mut self) {
        for at in (0..self.items.len() / 2).rev() {
            self.sift_down(at);
        }
    }

    fn take(&mut self, at: usize) -> Option<Rc<RefCell<TaskQueue>>> {
        if at >= self.items.len() {
            return None;
        }
        let taken = self.items.swap_remove(at);
        taken.borrow().heap_slot.set(NOT_IN_HEAP);
        if at < self.items.len() {
            self.items[at].borrow().heap_slot.set(at);
            if at > 0 && self.above(at, (at - 1) / 2) {
                self.sift_up(at);
            } else {
                self.sift_down(at);
            }
        }
        Some(taken)
    }

    /// Whether the queue at `a` belongs above the one at `b`.
    fn above(&self, a: usize, b: usize) -> bool {
        *self.items[a].borrow() > *self.items[b].borrow()
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.items.swap(a, b);
        self.items[a].borrow().heap_slot.set(a);
        self.items[b].borrow().heap_slot.set(b);
    }

    fn sift_up(&mut self, mut at: usize) {
        while at > 0 {
            let parent = (at - 1) / 2;
            if !self.above(at, parent) {
                break;
            }
            self.swap(at, parent);
            at = parent;
        }
    }

    fn sift_down(&mut self, mut at: usize) {
        loop {
            let left = 2 * at + 1;
            if left >= self.items.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.items.len() && self.above(right, left) {
                right
            } else {
                left
            };
            if !self.above(child, at) {
                break;
            }
            self.swap(at, child);
            at = child;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{executor::LocalExecutor, IoRequirements, Shares};

    /// Pops and removals at every position agree with sorting by the same
    /// order, and every queue's recorded position stays its own.
    #[test]
    fn removal_anywhere_keeps_the_order() {
        let ex = LocalExecutor::default();
        let queues: Vec<_> = (0..40u64)
            .map(|i| {
                let queue = TaskQueue::new(
                    Default::default(),
                    "q",
                    Shares::Static(1),
                    IoRequirements::default(),
                    0,
                    ex.task_registry(),
                );
                {
                    let mut state = queue.borrow_mut();
                    state.vruntime = (i * 7919) % 13;
                    state.entry = i;
                }
                queue
            })
            .collect();
        for remove_every in [2, 3, 5, 7] {
            let mut heap = QueueHeap::new();
            for queue in &queues {
                heap.push(queue.clone());
            }
            let mut expected: Vec<_> = queues.clone();
            for (at, queue) in queues.iter().enumerate() {
                if at % remove_every == 0 {
                    assert!(heap.remove(queue));
                    assert!(!heap.remove(queue), "removed twice");
                    expected.retain(|q| !Rc::ptr_eq(q, queue));
                }
            }
            for (at, queue) in heap.iter().enumerate() {
                assert_eq!(queue.borrow().heap_slot.get(), at);
            }
            expected.sort_by(|a, b| b.cmp(a));
            let popped: Vec<_> = std::iter::from_fn(|| heap.pop()).collect();
            assert_eq!(popped.len(), expected.len());
            assert!(popped.iter().zip(&expected).all(|(a, b)| Rc::ptr_eq(a, b)));
            assert!(queues
                .iter()
                .all(|q| q.borrow().heap_slot.get() == NOT_IN_HEAP));
        }
    }
}
