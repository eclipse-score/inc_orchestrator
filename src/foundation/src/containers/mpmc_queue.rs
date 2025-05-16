// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::mem::MaybeUninit;

use iceoryx2_bb_elementary::pointer_trait::PointerTrait;

use super::queue::Queue;

type ArcInternal<T> = std::sync::Arc<T>;
type MutexInternal<T> = std::sync::Mutex<T>;
type MutexGuardInternal<'a, T> = std::sync::MutexGuard<'a, T>;

pub struct QueuePtrIterator<'a, T> {
    guard: MutexGuardInternal<'a, Queue<T>>,
}

impl<T> Iterator for QueuePtrIterator<'_, T> {
    type Item = *mut MaybeUninit<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.guard.is_empty() {
            return None;
        }

        let item = unsafe { Some(self.guard.data.as_mut_ptr().add(self.guard.tail & (self.guard.capacity - 1))) };
        self.guard.tail += 1;
        item
    }
}

///
/// A struct wrapping the iceoryx2 Queue
///
/// It supports pushing from and popping to a SpmcStealQueue.
/// Pushing has to go through the LocalProducerConsumer.
///
pub struct MpmcQueue<T> {
    inner: ArcInternal<MutexInternal<Queue<T>>>,
}

impl<T: Send> MpmcQueue<T> {
    ///
    /// Initialize a MpmcQueue
    ///
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: ArcInternal::new(MutexInternal::new(Queue::new(capacity))),
        }
    }

    ///
    /// Pop an item from the queue.
    ///
    pub fn pop(&self) -> Option<T> {
        self.inner.lock().unwrap().pop()
    }

    ///
    /// Push an item into the queue.
    ///
    pub fn push(&self, item: T) -> bool {
        self.inner.lock().unwrap().push(item)
    }

    ///
    /// Take a look, if the queue is empty.
    ///
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    ///
    /// Return how many items are stored in the queue.
    ///
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    ///
    /// Returns a Lock to the inner queue
    pub(crate) fn lock(&self) -> MutexGuardInternal<'_, Queue<T>> {
        self.inner.lock().unwrap()
    }

    ///
    /// Returns an Iterator, that yields pointers to the items in the queue. While the Iterator is
    /// valid you are holding a lock to the Mutex to the inner Queue. You are responsible yourself
    /// for copying out the items behind the pointer while holding the lock. If you do not copy
    /// the items out, you are most likely leaking resources.
    ///
    pub fn iter(&self) -> QueuePtrIterator<T> {
        QueuePtrIterator { guard: self.lock() }
    }
}
