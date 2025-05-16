// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::alloc::{self, Layout}; // TODO: maybe replace this too from iceoryx2
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::prelude::*;
use std::sync::atomic::Ordering;

use crate::containers::mpmc_queue::MpmcQueue;

///
/// Delivers access for push & pop actions for a SpmcStealQueue ensuring there is only one producer.
///
pub struct LocalProducerConsumer<'a, T: Send> {
    queue: &'a SpmcStealQueue<T>,
    _marker: PhantomData<*const ()>, // marker for !Send and !Sync
}

impl<T: Send> LocalProducerConsumer<'_, T> {
    ///
    /// Tries to push a single `value` to the local queue. If that does not work, because the queue
    /// is full, items from the local queue are pushed to the global `mpmc` queue. This frees some
    /// room for new items in the local queue. The `value` is then pushed again to the local queue.
    ///
    ///  # Returns
    ///
    ///
    ///
    pub fn push(&self, value: T, mpmc: &MpmcQueue<T>) -> Result<(), T> {
        self.queue.push(value, mpmc)
    }

    ///
    /// Pops single value out of Queue
    ///
    /// # Returns
    ///
    /// None if queue is empty, otherwise Some(value)
    ///
    pub fn pop(&self) -> Option<T> {
        self.queue.pop_local()
    }
}

impl<T: Send> Drop for LocalProducerConsumer<'_, T> {
    fn drop(&mut self) {
        self.queue.has_producer.store(false, Ordering::SeqCst);
    }
}

///
/// Delivers access for push & pop actions for a SpmcStealQueue ensuring there is only one producer.
///
pub struct BoundProducerConsumer<T: Send> {
    queue: Arc<SpmcStealQueue<T>>,
    _marker: PhantomData<*const ()>, // marker for !Send and !Sync
}

impl<T: Send> BoundProducerConsumer<T> {
    ///
    /// Pushes single value to the Queue
    ///
    ///  # Returns
    ///
    ///
    ///
    pub fn push(&self, value: T, mpmc: &MpmcQueue<T>) -> Result<(), T> {
        self.queue.push(value, mpmc)
    }

    ///
    /// Pops single value out of Queue
    ///
    /// # Returns
    ///
    /// None if queue is empty, otherwise Some(value)
    ///
    pub fn pop(&self) -> Option<T> {
        self.queue.pop_local()
    }

    pub fn count(&self) -> u32 {
        self.queue.count()
    }

    pub fn capacity(&self) -> u32 {
        self.queue.capacity()
    }

    ///
    /// Takes items out of the provided `MpmcQueue` by pointer, and puts them into the spmc. This
    /// does only involve a single copy operation per item. This operation is done holding the
    /// Mutex lock on the mpmc.
    ///
    pub fn fetch_from(&self, mpmc: &MpmcQueue<T>) -> usize {
        self.queue.fetch_from(mpmc)
    }
}

impl<T: Send> Drop for BoundProducerConsumer<T> {
    fn drop(&mut self) {
        self.queue.has_producer.store(false, Ordering::SeqCst);
    }
}

///
/// Lock free queue for Single-Producer-Multiple-Consumer, supporting stealing API and fixed size allocated during construction
///
/// Key safety assumptions:
/// - push can happen only from single thread, pop can happen from same thread as push
/// - stealing can happen from any thread
/// - stealing have same order of extracting values as 'pop'
/// - T cannot have self referential data directly inside itself
///
///
pub struct SpmcStealQueue<T: Send> {
    /// The counter used for insertion operation
    head: FoundationAtomicU32,

    /// This holds tuple (steal_place, real_tail). steal_place is used for tracking stealing, real_tail is used for pop values out of queue         
    tail: FoundationAtomicU64,

    /// Used to turn head/tail into index of `data`
    access_mask: u32,

    /// Ensures that only one producer exists
    has_producer: FoundationAtomicBool,
    capacity: u32,
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// Type is Safe to share across thread and change thread
unsafe impl<T: Send> Send for SpmcStealQueue<T> {}
unsafe impl<T: Send> Sync for SpmcStealQueue<T> {}

impl<T: Send> SpmcStealQueue<T> {
    ///
    /// Construct new queue of size `size`. It will panic if construction can not happen.
    /// # Arguments
    ///
    /// - `size` - must be power of two
    ///
    pub fn new(size: u32) -> Self {
        assert!(size.is_power_of_two());

        let layout = Layout::array::<UnsafeCell<MaybeUninit<T>>>(size as usize).expect("Invalid layout for array provided");

        let ptr = unsafe { alloc::alloc(layout) as *mut UnsafeCell<MaybeUninit<T>> };

        if ptr.is_null() {
            panic!("Memory allocation failed for SpmcStealQueue with size {}", size);
        }

        Self {
            head: FoundationAtomicU32::new(0),
            tail: FoundationAtomicU64::new(0),
            data: unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, size as usize)) },
            access_mask: size - 1,
            has_producer: FoundationAtomicBool::new(false),
            capacity: size,
        }
    }

    ///
    /// Returns number of elements the queue can hold at max
    ///
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    ///
    /// Returns number of elements in the queue
    ///
    pub fn count(&self) -> u32 {
        // *Synchronization* : Any thread can call this fn, we want it to be really accurate.
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);

        Self::count_internal(head, tail)
    }

    ///
    /// Created `LocalProducerConsumer` if there is any existing, otherwise None
    ///
    pub fn get_local(&self) -> Option<LocalProducerConsumer<T>> {
        // *Synchronization* : SeqCst only to provide strongest guarantee while obtaining producer
        if self
            .has_producer
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            .is_ok()
        {
            Some(LocalProducerConsumer {
                queue: self,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    ///
    /// Created `LocalProducerConsumer` if there is any existing, otherwise None
    ///
    pub fn get_boundedl(self: &Arc<Self>) -> Option<BoundProducerConsumer<T>> {
        // *Synchronization* : SeqCst only to provide strongest guarantee while obtaining producer
        if self
            .has_producer
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
            .is_ok()
        {
            Some(BoundProducerConsumer {
                queue: self.clone(),
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    ///
    /// Let caller steal a work from other queue into its queue
    ///
    /// # Arguments
    ///
    /// - `dst` - destination queue
    /// - `divider` - the call will try to steal min(half count of a queue, avail space in dst) by default. When divider is provided, it will calculate steal size as `half_count / divider`
    ///
    pub fn steal_into(&self, dst: &Self, divider: Option<u8>) -> Option<u32> {
        // dst is stealing thread queue and writing to it is done only by the one that calls this function now

        let dst_head = dst.head.load(Ordering::Relaxed); // Only we are changing our head
        let dst_tail = dst.tail.load(Ordering::Acquire);
        let (dst_steal, _) = Self::unpack_tail(dst_tail);

        let dst_avail_size = dst_head.wrapping_sub(dst_steal);

        if dst_avail_size >= dst.data.len() as u32 {
            return None; // No space left
        }

        // How much did we stolen
        let n = self.steal_internal(
            dst,
            dst_head,
            dst.data.len() as u32 - dst_avail_size,
            divider.unwrap_or(0) + 2, // By default, half content steal
        );

        if n == 0 {
            return None; // Nothing ;)
        }

        // *Synchronization*: This makes sure above writes are before we increment head, which basically publish data to others
        dst.head.store(dst_head.wrapping_add(n), Ordering::Release);

        Some(n)
    }

    //
    // Private impl part
    //

    ///
    /// Push to local spmc queue. If that does not work, try to make room in local queue by pushing
    /// items to global mpmc queue and then try again.
    /// An error is returned if global mpmc is also full.
    ///
    fn push(&self, value: T, mpmc: &MpmcQueue<T>) -> Result<(), T> {
        let res = self.push_local_queue(value);
        if let Err(v) = res {
            if self.push_to_mpmc(mpmc, None) == 0 {
                return Err(v);
            }

            return self.push_local_queue(v);
        }

        res
    }

    fn push_local_queue(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed); // Write only from current thread
        let tail = self.tail.load(Ordering::Acquire); // Access by multiple threads, so we want to make sure other threads have made visible all operations prior to Release

        let (steal, _) = Self::unpack_tail(tail); // We can push only based on where steel counter is, as this is real indicator of free space (if someone stealing, tail may have been already advanced)

        if head.wrapping_sub(steal) < self.data.len() as u32 {
            let index = head & self.access_mask;

            // Head index is only mutated when being here by single thread and is borrowed only once mutation finishes, so Cell req are fulfilled
            unsafe {
                let entry = self.data[index as usize].get();
                (*entry).as_mut_ptr().write(value);
            }

            // *Synchronization*: This makes sure above writes are before we increment head
            self.head.store(head.wrapping_add(1), Ordering::Release);
            return Ok(());
        }

        //TODO: consider push_overflow to global wait queue, currently we may end here even if queue is not full as tail might already been taken up.
        Err(value)
    }

    fn fetch_from(&self, mpmc: &MpmcQueue<T>) -> usize {
        let head = self.head.load(Ordering::Relaxed); // Write only from current thread
        let tail = self.tail.load(Ordering::Acquire); // Access by multiple threads, so we want to make sure other threads have made visible all operations prior to Release

        let num_possible = mpmc.len().min((self.capacity - Self::count_internal(head, tail)) as usize);
        let mut num_items = 0;

        // Head index is only mutated when being here by single thread and is borrowed only once mutation finishes, so Cell req are fulfilled
        let mut iter = mpmc.iter();
        while num_items < num_possible {
            // When we are taking a pointer to an item from the iterator by calling next, we are
            // also responsible for taking the item behind the pointer. If we do not do this, the
            // item is lost.
            if let Some(ptr) = iter.next() {
                let index = (head + num_items as u32) & self.access_mask;
                unsafe {
                    let entry = self.data[index as usize].get();
                    std::ptr::copy_nonoverlapping(ptr, entry, 1);
                }

                num_items += 1;
            } else {
                break;
            }
        }

        // *Synchronization*: This makes sure above writes are before we increment head
        self.head.store(head.wrapping_add(num_items as u32), Ordering::Release);
        num_items
    }

    fn pop_local(&self) -> Option<T> {
        let mut tail: u64 = self.tail.load(Ordering::Acquire); // Modified by multiple threads

        let index = loop {
            let head = self.head.load(Ordering::Relaxed); // We are in same thread that could only change it

            let (steal, real_tail) = Self::unpack_tail(tail);

            if head == real_tail {
                // Empty
                return None;
            }

            let index = (real_tail & self.access_mask) as usize;
            let next_real_tail = real_tail.wrapping_add(1);

            let expected_tail = if steal == real_tail {
                // No one stealing, keep both same
                Self::pack_tail(next_real_tail, next_real_tail)
            } else {
                Self::pack_tail(steal, next_real_tail) // someone stealing, keep steal as is and try to move only tail
            };

            let res = self.tail.compare_exchange(
                tail,
                expected_tail,
                Ordering::AcqRel,
                Ordering::Acquire, // TODO: SeqCst ?
            );

            match res {
                Ok(_) => break index,
                Err(actual) => tail = actual, // Start from beginning
            }
        };

        // If you wonder why we do it like that, we already marked index as taken, so write can happen there but this call is a call
        // for very same thread as write can happen, so no one will write us before we really read it
        unsafe {
            let entry: &UnsafeCell<MaybeUninit<T>> = &self.data[index];
            Some((*entry.get()).as_ptr().read())
        }
    }

    pub fn steal_internal(&self, dst: &Self, dst_head: u32, max_steal_size: u32, divider: u8) -> u32 {
        let mut tail = self.tail.load(Ordering::Acquire);

        let (start_idx, count) = loop {
            let (steal, real_tail) = Self::unpack_tail(tail);
            let head = self.head.load(Ordering::Acquire); // As writer thread can modify it any time, we need to fetch it always

            if head == real_tail {
                return 0; // Nothing in queue, empty already
            }

            if steal != real_tail {
                return 0; // Someone else is stealing now from us, lets stop the new thread from doing the same
            }

            // Now we can try to steal something
            let mut expected_steel_count: u32 = head.wrapping_sub(real_tail) / divider as u32;
            expected_steel_count = expected_steel_count.min(max_steal_size);
            expected_steel_count = expected_steel_count.max(1); // no less than one to steal

            let next_real_tail = real_tail.wrapping_add(expected_steel_count);
            let expected_tail = Self::pack_tail(steal, next_real_tail); // For now steel is as it was, this will mark that we are stealing and prevent others to steal from us

            let res = self.tail.compare_exchange(tail, expected_tail, Ordering::AcqRel, Ordering::Acquire);

            match res {
                Ok(_) => break (real_tail & self.access_mask, expected_steel_count),
                Err(actual) => {
                    tail = actual; // Lets try again, as someone else was doing something to queue
                }
            }
        };

        if count == 0 {
            return 0;
        }

        for i in 0..count {
            unsafe {
                let src_index = ((start_idx + i) & self.access_mask) as usize;
                let src_entry: &UnsafeCell<MaybeUninit<T>> = &self.data[src_index];

                let dst_index = ((dst_head + i) & dst.access_mask) as usize;
                let dst_entry: &UnsafeCell<MaybeUninit<T>> = &dst.data[dst_index];

                (*dst_entry.get()).as_mut_ptr().write((*src_entry.get()).as_ptr().read());
            }
        }

        tail = self.tail.load(Ordering::Acquire);
        loop {
            let (_, real_tail) = Self::unpack_tail(tail);

            let expected_tail = Self::pack_tail(real_tail, real_tail); // We are done, try align steal and tail to either our tail value or someone else if higher

            let res = self.tail.compare_exchange(tail, expected_tail, Ordering::AcqRel, Ordering::SeqCst);

            match res {
                Ok(_) => {
                    return count;
                }
                Err(actual) => tail = actual,
            }
        }
    }

    ///
    /// This makes room in our local spmc queue and pushes items to the global mpmc queue. How many items
    /// will be pushed is influenced by the `divider`.
    /// The function returns how many items were finally pushed.
    ///
    /// Note: This function is assumed to be only called from the single producer thread.
    ///
    fn push_to_mpmc(&self, mpmc: &MpmcQueue<T>, divider: Option<u8>) -> u32 {
        let head = self.head.load(Ordering::Relaxed); // Write only from current thread
        let mut mpmc_inner_queue = mpmc.lock();
        let mpmc_free_space = (mpmc_inner_queue.capacity() - mpmc_inner_queue.len()) as u32;
        let mut num_items;
        let mut index;
        let mut tail = self.tail.load(Ordering::Acquire); // Access by multiple threads, so we want to make sure other threads have made visible all operations prior to Release

        // Advance tail before actually copying out the items
        loop {
            let (steal, real_tail) = SpmcStealQueue::<T>::unpack_tail(tail); // We can push only based on where steal counter is, as this is real indicator of free space (if someone stealing, tail may have been already advanced)
            index = real_tail;

            let num_items_from_spmc = Self::count_internal(head, tail) / divider.unwrap_or(2) as u32;
            num_items = mpmc_free_space.min(num_items_from_spmc);
            let next_real_tail = real_tail.wrapping_add(num_items);
            let expected_tail = if steal == real_tail {
                // No one stealing, keep both same
                SpmcStealQueue::<T>::pack_tail(next_real_tail, next_real_tail)
            } else {
                SpmcStealQueue::<T>::pack_tail(steal, next_real_tail) // someone stealing, keep steal as is and try to move only tail
            };

            let res = self.tail.compare_exchange(
                tail,
                expected_tail,
                Ordering::AcqRel,
                Ordering::Acquire, // TODO: SeqCst ?
            );

            // if updating tail worked, we can break out. If it failed, someone stealed inbetween
            // and we retry.
            match res {
                Ok(_) => {
                    break;
                }
                Err(actual) => tail = actual,
            }
        }

        let mut successful_items = 0;
        for _ in 0..num_items {
            index &= self.access_mask;
            let item = self.data[index as usize].get();
            let item = unsafe { (*item).assume_init_read() };
            if !mpmc_inner_queue.push(item) {
                break;
            }

            index = index.wrapping_add(1);
            successful_items += 1;
        }

        successful_items
    }

    fn count_internal(head: u32, tail: u64) -> u32 {
        let (_, real_tail) = Self::unpack_tail(tail);

        head.wrapping_sub(real_tail)
    }

    fn unpack_tail(value: u64) -> (u32, u32) {
        ((value >> 32) as u32, value as u32)
    }

    fn pack_tail(steal: u32, tail: u32) -> u64 {
        ((steal as u64) << 32) | tail as u64
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {

    static DUMMY: u32 = 0xDEADBEEF;

    use super::*;
    use std::vec::Vec;
    use std::{sync::Arc, thread};

    fn push_n_elems_with_increment(start_val: u32, n: u32, queue: &LocalProducerConsumer<u32>) -> std::vec::Vec<u32> {
        let mut vec_pushed = std::vec::Vec::new();
        let mpmc = MpmcQueue::new(1);
        for i in 0..n {
            assert!(queue.push(i + start_val, &mpmc).is_ok());
            vec_pushed.push(i + start_val);
        }

        vec_pushed
    }

    #[test]
    fn test_unpack() {
        let (upper, lower) = SpmcStealQueue::<bool>::unpack_tail(0xdeadbeef12345678);
        assert_eq!(upper, 0xdeadbeef);
        assert_eq!(lower, 0x12345678);
    }

    #[test]
    fn test_push_pop_same_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let pc = queue.get_local().unwrap();

        push_n_elems_with_increment(0, 5, &pc);

        assert_eq!(pc.pop().unwrap_or(DUMMY), 0);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 1);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 2);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 3);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 4);

        assert!(pc.pop().is_none()); // No more data
        assert!(pc.pop().is_none()); // No more data
    }

    #[test]
    fn test_push_pop_interleaved_same_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let pc = queue.get_local().unwrap();

        push_n_elems_with_increment(0, 3, &pc);

        assert_eq!(pc.pop().unwrap_or(DUMMY), 0);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 1);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 2);

        push_n_elems_with_increment(3, 2, &pc);

        assert_eq!(pc.pop().unwrap_or(DUMMY), 3);
        assert_eq!(pc.pop().unwrap_or(DUMMY), 4);

        assert!(pc.pop().is_none()); // No more data
        assert!(pc.pop().is_none()); // No more data
    }

    #[test]
    fn test_push_overflow_same_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(8);
        let pc = queue.get_local().unwrap();

        push_n_elems_with_increment(0, 8, &pc);

        let mpmc = MpmcQueue::new(1);
        // queue is full, push will make room by pushing first item from spmc to the mpmc and then
        // push the actual item to spmc
        let res = pc.push(8, &mpmc);
        assert!(res.is_ok());
        let res = pc.push(9, &mpmc);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), 9); // We get back value as error so we can use it again
        assert_eq!(mpmc.pop(), Some(0)); // First item was pushed to mpmc
        for i in 1..9 {
            let item = pc.pop();
            assert_eq!(item, Some(i));
        }

        assert_eq!(pc.pop(), None);
    }

    #[test]
    fn test_pop_underflow_same_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(8);
        let pc = queue.get_local().unwrap();

        assert!(pc.pop().is_none());

        push_n_elems_with_increment(0, 3, &pc);

        pc.pop();
        pc.pop();
        pc.pop();

        assert!(pc.pop().is_none());

        let mpmc = MpmcQueue::new(1);
        assert!(queue.push(4, &mpmc).is_ok());
        pc.pop();

        assert!(pc.pop().is_none());
    }

    #[test]
    fn test_only_single_producer_can_be_created_same_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(8);
        let pc = queue.get_local().unwrap();

        assert!(queue.get_local().is_none());
        assert!(queue.get_local().is_none());

        drop(pc);

        let new_pc = queue.get_local();

        assert!(new_pc.is_some());
        assert!(queue.get_local().is_none());
    }

    #[test]
    fn test_steal_single_thread() {
        let queue: SpmcStealQueue<u32> = SpmcStealQueue::new(8);
        let pc = queue.get_local().unwrap();

        push_n_elems_with_increment(0, 5, &pc);

        assert_eq!(pc.pop().unwrap_or(DUMMY), 0);

        let steal_into_queue: SpmcStealQueue<u32> = SpmcStealQueue::new(8);
        let steal_pc = steal_into_queue.get_local().unwrap();

        {
            let ret = queue.steal_into(&steal_into_queue, None); // Test if by default we get 2 values (since there are 4)

            assert_eq!(ret.unwrap_or(DUMMY), 2);
            assert_eq!(steal_pc.pop().unwrap_or(DUMMY), 1);
            assert_eq!(steal_pc.pop().unwrap_or(DUMMY), 2);

            assert_eq!(pc.pop().unwrap_or(DUMMY), 3);
            assert_eq!(pc.pop().unwrap_or(DUMMY), 4);
        }

        // test divider
        {
            assert!(pc.pop().is_none());

            push_n_elems_with_increment(0, 8, &pc);

            let ret = queue.steal_into(&steal_into_queue, Some(2)); // Test if by default we get 2 values (since there are 4)

            assert_eq!(ret.unwrap_or(DUMMY), 2);
            assert_eq!(steal_pc.pop().unwrap_or(DUMMY), 0);
            assert_eq!(steal_pc.pop().unwrap_or(DUMMY), 1);
        }
    }

    fn steal_fn(queue: Arc<SpmcStealQueue<u32>>, stop: Arc<FoundationAtomicBool>) -> Vec<u32> {
        let mut vec = Vec::new();
        let steal_queue: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let steal_pc = steal_queue.get_local().unwrap();

        while !stop.load(Ordering::SeqCst) {
            let ret = queue.steal_into(&steal_queue, None);

            if let Some(count) = ret {
                for _ in 0..count {
                    let v = steal_pc.pop();

                    if let Some(val) = v {
                        vec.push(val);
                    }
                }
            }
        }

        vec
    }
    fn steal_for_pop_fn(queue: Arc<SpmcStealQueue<u32>>) -> Vec<u32> {
        let mut vec = Vec::new();
        let steal_queue: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let steal_pc = steal_queue.get_local().unwrap();

        loop {
            if queue.steal_into(&steal_queue, None).is_some() {
                loop {
                    if let Some(val) = steal_pc.pop() {
                        vec.push(val);
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }

        vec
    }

    #[test]
    fn test_mt_one_pop_one_stealer() {
        let mut tasks_done: [Option<u32>; 32768] = [None; 32768];
        let queue: Arc<SpmcStealQueue<u32>> = Arc::new(SpmcStealQueue::new(32768));
        let pc = queue.get_local().unwrap();
        push_n_elems_with_increment(0, 32768, &pc);

        let handle1 = {
            let qc = queue.clone();
            thread::spawn(move || steal_for_pop_fn(qc))
        };

        loop {
            match pc.pop() {
                Some(i) => {
                    assert_eq!(tasks_done[i as usize], None);
                    tasks_done[i as usize] = Some(i);
                }
                None => break,
            }
        }

        let worker1_vec = handle1.join().unwrap();
        for i in worker1_vec {
            assert_eq!(tasks_done[i as usize], None);
            tasks_done[i as usize] = Some(i);
        }

        for i in tasks_done {
            assert_ne!(i, None);
        }
    }

    #[test]
    fn test_one_producer_one_stealer_mt_thread() {
        let stop = Arc::new(FoundationAtomicBool::new(false));
        let queue: Arc<SpmcStealQueue<u32>> = Arc::new(SpmcStealQueue::new(256));
        let pc = queue.get_local().unwrap();

        let mut vec_produced: Vec<u32> = Vec::new();

        vec_produced.extend(push_n_elems_with_increment(0, 10, &pc));

        let handle1 = {
            let sc = stop.clone();
            let qc = queue.clone();
            thread::spawn(move || steal_fn(qc, sc))
        };

        let mut i: u32 = vec_produced.last().unwrap().clone();
        let mpmc = MpmcQueue::new(256);
        while i < 100000 {
            if let Ok(_) = pc.push(i, &mpmc) {
                vec_produced.push(i);
                i += 1;
            }

            if i % 1000 == 0 {
                thread::yield_now();
            }
        }

        // Stop all
        stop.store(true, Ordering::SeqCst);

        let mut worker1_vec = handle1.join().unwrap();

        worker1_vec.sort();

        // Threads may have finished before final end, so something is still there, so we shall pop it here
        let remaining_in_queue = queue.count();
        while let Some(v) = pc.pop() {
            worker1_vec.push(v);
        }

        let is_more_than_20 = remaining_in_queue > (vec_produced.len() as f32 * 0.2) as u32;
        assert!(!is_more_than_20, "Was all {}, remaining {}", vec_produced.len(), remaining_in_queue); // We assume no more than 20% can stay in queue on producer thread

        // We may have some things in global queue now, we need to account them into comparison
        while let Some(v) = mpmc.pop() {
            worker1_vec.push(v);
        }

        worker1_vec.sort();

        assert_eq!(worker1_vec.len() + queue.count() as usize, vec_produced.len());

        assert_eq!(
            vec_produced,
            worker1_vec,
            "producer (len: {}) vs consumer(len: {}) vectors not equal, was remaining in queue {}",
            vec_produced.len(),
            worker1_vec.len(),
            remaining_in_queue
        );
    }

    #[test]
    fn test_one_producer_multi_stealer_mt_thread() {
        let stop = Arc::new(FoundationAtomicBool::new(false));
        let queue: Arc<SpmcStealQueue<u32>> = Arc::new(SpmcStealQueue::new(256));
        let pc = queue.get_local().unwrap();

        let mut vec_produced: Vec<u32> = Vec::new();

        vec_produced.extend(push_n_elems_with_increment(0, 10, &pc));

        let handle1 = {
            let sc = stop.clone();
            let qc = queue.clone();
            thread::spawn(move || steal_fn(qc, sc))
        };

        let handle2 = {
            let sc = stop.clone();
            let qc = queue.clone();
            thread::spawn(move || steal_fn(qc, sc))
        };

        let handle3 = {
            let sc = stop.clone();
            let qc = queue.clone();
            thread::spawn(move || steal_fn(qc, sc))
        };

        let mut i: u32 = vec_produced.last().unwrap().clone();
        let mpmc: MpmcQueue<u32> = MpmcQueue::new(256);
        while i < 1000000 {
            if let Ok(_) = pc.push(i, &mpmc) {
                vec_produced.push(i);
                i += 1;
            }

            if i % 12000 == 0 {
                thread::yield_now();
            }
        }

        // Stop all
        stop.store(true, Ordering::SeqCst);

        let mut worker1_vec = handle1.join().unwrap();
        let worker2_vec = handle2.join().unwrap();
        let worker3_vec = handle3.join().unwrap();

        worker1_vec.extend(worker2_vec);
        worker1_vec.extend(worker3_vec);

        // Threads may have finished before final end, so something is still there, so we shall pop it here
        let remaining_in_queue = queue.count();
        while let Some(v) = pc.pop() {
            worker1_vec.push(v);
        }

        let is_more_than_20 = remaining_in_queue > (worker1_vec.len() as f32 * 0.2) as u32;
        assert!(!is_more_than_20, "Was all {}, remaining {}", vec_produced.len(), remaining_in_queue); // We assume no more than 20% can stay in queue on producer thread

        // We may have some things in global queue now, we need to account them into comparison
        while let Some(v) = mpmc.pop() {
            worker1_vec.push(v);
        }

        worker1_vec.sort();

        assert_eq!(worker1_vec.len() + queue.count() as usize, vec_produced.len());

        assert_eq!(
            vec_produced,
            worker1_vec,
            "producer (len: {}) vs consumer(len: {}) vectors not equal, was remaining in queue {}",
            vec_produced.len(),
            worker1_vec.len(),
            remaining_in_queue
        );
    }

    #[test]
    fn test_1_thread_push_mpmc() {
        let src: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let src_pc = src.get_local().unwrap();

        push_n_elems_with_increment(0, 30, &src_pc);
        let dst: MpmcQueue<u32> = MpmcQueue::new(16);
        assert_eq!(src.push_to_mpmc(&dst, None), 15);

        for i in 0..15 {
            assert_eq!(dst.pop(), Some(i));
        }

        assert_eq!(dst.pop(), None);

        for i in 15..30 {
            assert_eq!(src_pc.pop(), Some(i));
        }

        assert_eq!(src_pc.pop(), None);

        push_n_elems_with_increment(30, 30, &src_pc);
        assert_eq!(src.push_to_mpmc(&dst, None), 15);

        for i in 30..45 {
            assert_eq!(dst.pop(), Some(i));
        }

        assert_eq!(dst.pop(), None);

        for i in 45..60 {
            assert_eq!(src_pc.pop(), Some(i));
        }

        assert_eq!(src_pc.pop(), None);
    }

    ///
    /// This is a multithreaded test that does stealing from a SpmcStealQueue in one thread and
    /// `push_to_mpmc` in another thread.
    ///
    #[test]
    fn test_mt_one_push_mpmc_one_stealer() {
        const QUEUE_LEN: usize = 32768;
        let mut tasks_done: [Option<u32>; QUEUE_LEN] = [None; QUEUE_LEN];
        let queue: Arc<SpmcStealQueue<u32>> = Arc::new(SpmcStealQueue::new(QUEUE_LEN as u32));
        let pc = queue.get_local().unwrap();
        push_n_elems_with_increment(0, QUEUE_LEN as u32, &pc);

        let handle1 = {
            let qc = queue.clone();
            thread::spawn(move || steal_for_pop_fn(qc))
        };

        let dst: MpmcQueue<u32> = MpmcQueue::new(16);
        loop {
            // push to mpmc
            if queue.push_to_mpmc(&dst, None) == 0 {
                break;
            }

            // empty the mpmc
            loop {
                match dst.pop() {
                    Some(i) => {
                        assert_eq!(tasks_done[i as usize], None);
                        tasks_done[i as usize] = Some(i);
                    }
                    None => break,
                }
            }
        }

        let worker1_vec = handle1.join().unwrap();
        // insert tasks from steal thread into our tasks_done array
        for i in worker1_vec {
            assert_eq!(tasks_done[i as usize], None);
            tasks_done[i as usize] = Some(i);
        }

        // Is there one last remaining task in spmc that can't be stolen?
        loop {
            match pc.pop() {
                Some(i) => {
                    assert_eq!(tasks_done[i as usize], None);
                    tasks_done[i as usize] = Some(i);
                }
                None => break,
            }
        }

        // check if every task was exactly one time in a task queue
        for i in tasks_done {
            assert_ne!(i, None);
        }
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by TAKE_GLOBAL_WORK_SIZE
    ///
    #[test]
    fn test_push_mpmc_multi_1() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..16 {
            assert_eq!(src.push(i), true);
        }

        assert_eq!(dst_pc.fetch_from(&src), 16);

        for i in 0..16 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);
        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by space left in spmc
    ///
    #[test]
    fn test_push_mpmc_multi_2() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..27 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..16 {
            assert_eq!(src.push(i), true);
        }

        assert_eq!(dst_pc.fetch_from(&src), 5);

        for i in 0..27 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 0..5 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);

        for i in 5..16 {
            assert_eq!(src.pop(), Some(i));
        }

        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by space left in a wrapped spmc
    ///
    #[test]
    fn test_push_mpmc_multi_3() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..32 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..7 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 0..16 {
            assert_eq!(src.push(i), true);
        }

        assert_eq!(dst_pc.fetch_from(&src), 7);

        for i in 7..32 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 0..7 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);

        for i in 7..16 {
            assert_eq!(src.pop(), Some(i));
        }

        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by items left in mpmc
    ///
    #[test]
    fn test_push_mpmc_multi_4() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..3 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..4 {
            assert_eq!(src.push(i), true);
        }

        assert_eq!(dst_pc.fetch_from(&src), 4);

        for i in 0..3 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 0..4 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);
        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by items left in a wrapped mpmc
    /// with head at index 0
    ///
    #[test]
    fn test_push_mpmc_multi_5() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..3 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..16 {
            assert_eq!(src.push(i), true);
        }

        for i in 0..8 {
            assert_eq!(src.pop(), Some(i));
        }

        assert_eq!(dst_pc.fetch_from(&src), 8);

        for i in 0..3 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 8..16 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);
        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by items left in a wrapped mpmc
    /// with head < than tail
    ///
    #[test]
    fn test_push_mpmc_multi_6() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 100..106 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..16 {
            assert_eq!(src.push(i), true);
        }

        for i in 0..12 {
            assert_eq!(src.pop(), Some(i));
        }

        for i in 16..17 {
            assert_eq!(src.push(i), true);
        }

        assert_eq!(dst_pc.fetch_from(&src), 5);

        for i in 100..106 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 12..17 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);
        assert_eq!(src.pop(), None);
    }

    ///
    /// Number of items transfered from mpmc -> spmc is limited by items left in a wrapped mpmc
    /// with head at the end
    ///
    #[test]
    fn test_push_mpmc_multi_7() {
        let dst: SpmcStealQueue<u32> = SpmcStealQueue::new(32);
        let dst_pc = Arc::new(dst).get_boundedl().unwrap();

        let src: MpmcQueue<u32> = MpmcQueue::new(16);
        for i in 0..3 {
            assert_eq!(dst_pc.push(i, &src), Ok(()));
        }

        for i in 0..14 {
            assert_eq!(src.push(i), true);
        }

        for i in 0..8 {
            assert_eq!(src.pop(), Some(i));
        }

        assert_eq!(dst_pc.fetch_from(&src), 6);

        for i in 0..3 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        for i in 8..14 {
            assert_eq!(dst_pc.pop(), Some(i));
        }

        assert_eq!(dst_pc.pop(), None);
        assert_eq!(src.pop(), None);
    }
}
