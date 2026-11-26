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
//! pops the same sequence, so the vruntime scheduler behaves exactly as it did
//! over `BinaryHeap`.

use super::TaskQueue;
use std::{cell::RefCell, rc::Rc};

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
