// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    alloc::{self, dealloc, Layout},
    future::Future,
    marker::PhantomPinned,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
};

use foundation::prelude::{CommonErrors, FoundationAtomicU8};
use std::sync::Arc;

///
/// This is a pool of futures that holds `dyn Future<Output = OutType>` objects (same as Box) and after future is dropped, it is again available in poll to reuse.
/// This means that after init, there is no more dynamic allocation done by claiming future.
/// Keep in mind you can only put here futures that are compatible with future [`Layout`] that was claimed by [`ReusableBoxFuturePoll::new`]
///
/// # Key consideration
///  - this is round robin poll. This means that even if some Future in middle finishes, next will pickup in round robin fashion and if there is no available future, it witt return error
///
/// # Use cases
///  - The main usage of this is when you recreate same futures in some cyclic manner and you know it shall be recycled after a cycle.
///
///
pub struct ReusableBoxFuturePoll<OutType> {
    boxes: Box<[IndirectStorage<OutType>]>, // Stores (initialization_maker, pointer to allocated storage)
    states: Box<[Arc<BoxState>]>,           // state of boxes, matches via index with above
    position: usize,                        // position in round robin queue
    size: usize,                            // number of futures
    mem_layout: Layout,                     // Layout of first future that was requested to store
}

///
/// Future that can be simply awaited to execute what was stored there
///
pub struct ReusableBoxFuture<OutType> {
    memory: NonNull<dyn Future<Output = OutType> + Send + 'static>,
    state: Arc<BoxState>,
    layout: Layout,
    _pin: PhantomPinned,
}

unsafe impl<OutType> Send for ReusableBoxFuturePoll<OutType> {}

type IndirectStorage<OutType> = NonNull<dyn Future<Output = OutType> + Send + 'static>;

const FUTURE_TAKEN: u8 = 1; // Future is taken by user, not available
const FUTURE_FREE: u8 = 0; // Future is in a poll, can be taken
const FUTURE_POLL_GONE: u8 = 0xFF; // Poll is dropped or dropping and the future has to drop itself instead being dropped by poll

impl<OutType> ReusableBoxFuturePoll<OutType> {
    ///
    /// Creates a pool with `cnt` futures available
    ///
    pub fn new<U>(cnt: usize, _: U) -> Self
    where
        U: Future<Output = OutType> + Send + 'static,
    {
        //TODO: Consider using Vec from iceoryx once they fix miri issues, otherwise it's hard to use other code based on it
        let input_layout = Layout::new::<U>();
        let boxes = Self::create_arr_storage(cnt, |_| unsafe {
            let memory = alloc::alloc(input_layout);
            let typed = memory as *mut U;

            NonNull::new(typed).unwrap() as IndirectStorage<OutType>
        });

        let states = Self::create_arr_storage(cnt, |_| Arc::new(BoxState::new()));

        Self {
            boxes,
            states,
            position: 0,
            size: cnt,
            mem_layout: input_layout,
        }
    }

    ///
    /// Try to obtain next future. This means that if next one in poll is free, it will return it, otherwise Err()
    ///
    /// # Returns
    ///
    /// - `Ok()` - when all fine
    /// - `Err(CommonErrors::NoData)` — if there is no next free future
    /// - `Err(CommonErrors::GenericError)` — if layout of provided future does not match to layout provided to very first call into this function
    ///
    pub fn next<U>(&mut self, future: U) -> Result<ReusableBoxFuture<OutType>, CommonErrors>
    where
        U: Future<Output = OutType> + Send + 'static,
    {
        let input_layout = Layout::new::<U>();

        if input_layout != self.mem_layout {
            return Err(CommonErrors::GenericError);
        }

        let index = self.position % self.size;

        let atomic_ref = &self.states[index].0;

        match atomic_ref.compare_exchange(
            FUTURE_FREE,
            FUTURE_TAKEN,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(_) => return Err(CommonErrors::NoData), // next is not free yet, this is user problem now
        }

        let mut next_box: NonNull<dyn Future<Output = OutType> + Send> = self.boxes[index];

        Self::replace_future(future, &mut next_box);

        self.position += 1; // increase pos, so next one will take correct box

        Ok(ReusableBoxFuture {
            memory: next_box,
            state: self.states[index].clone(),
            layout: input_layout,
            _pin: PhantomPinned,
        })
    }

    ///
    /// Safety: Storage is initialized and the type U has correct layout must be checked by caller
    ///
    fn replace_future<U>(future: U, storage: &mut IndirectStorage<OutType>)
    where
        U: Future<Output = OutType> + Send + 'static,
    {
        unsafe {
            let data = storage.as_ptr() as *mut U;
            data.write(future); // The drop happens before this region is back into a poll so we can just write to it

            // This is really important line. We rewrite ptr with same ptr but in practice we rewrite with U type ptr which will force compiler to update underlying vtable doe dyn type.
            // If this is not done, then it will think that it has old vtable and will use it! (don't replace with ptr.as_ptr(), its same but lacks the type which is crucial here)
            *storage = NonNull::new_unchecked(data);
        }
    }

    fn create_arr_storage<T, U: Fn(usize) -> T>(size: usize, init: U) -> Box<[T]> {
        let layout = Layout::array::<T>(size).unwrap();

        // SAFETY: We are manually allocating memory here
        let ptr = unsafe { alloc::alloc(layout) as *mut T };

        for i in 0..size {
            unsafe {
                ptr.add(i).write(init(i)); // just filling with values as it has to be initialized
            }
        }

        // SAFETY: Create boxed slice from raw parts
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, size)) }
    }
}

impl<OutType> Drop for ReusableBoxFuturePoll<OutType> {
    fn drop(&mut self) {
        for index in 0..self.size {
            let boxed = &self.boxes[index];

            // Here we assume future is in usage, if yes, we are setting flag and dealloc will happen at future side, if not, then wew had FUTURE_FREE and we can dealloc it (drop always done in future)
            match self.states[index].0.compare_exchange(
                FUTURE_TAKEN,
                FUTURE_POLL_GONE,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(actual) => unsafe {
                    assert_eq!(actual, FUTURE_FREE);

                    // Deallocate the memory, passing the raw pointer and layout
                    dealloc(boxed.as_ptr() as *mut u8, self.mem_layout);
                },
            }
        }
    }
}

struct BoxState(FoundationAtomicU8);

impl BoxState {
    fn new() -> Self {
        Self(FoundationAtomicU8::new(FUTURE_FREE))
    }
}

unsafe impl<OutType> Send for ReusableBoxFuture<OutType> {} // Since future is send, we are ok to send it

impl<OutType> ReusableBoxFuture<OutType> {
    pub fn into_pin(self) -> Pin<Self> {
        unsafe { Pin::new_unchecked(self) }
    }
}

impl<OutType> Drop for ReusableBoxFuture<OutType> {
    fn drop(&mut self) {
        struct DropGuard<'a, T> {
            this: &'a ReusableBoxFuture<T>,
        }

        impl<T> Drop for DropGuard<'_, T> {
            fn drop(&mut self) {
                match self.this.state.0.compare_exchange(
                    FUTURE_TAKEN,
                    FUTURE_FREE,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {}

                    // Means that poll is dropped probably and we need to cleanup own memory
                    Err(val) => {
                        assert_eq!(val, FUTURE_POLL_GONE);
                        unsafe {
                            dealloc(self.this.memory.as_ptr() as *mut u8, self.this.layout);
                        }
                    }
                }
            }
        }

        // This is funny, since here replaced future may panic in drop, the code after this line will not executed, it would leave future not replaced and input future consumed.
        // Thats why we put guard above because during panic in drop, stack gets unwind and it will call other drops in this fn, including our guard
        let _guard = DropGuard { this: self }; // make sure that after drop, we fire sync logic

        unsafe {
            std::ptr::drop_in_place(self.memory.as_ptr());
        }
    }
}

impl<OutType> Deref for ReusableBoxFuture<OutType> {
    type Target = dyn Future<Output = OutType> + Send + 'static;

    fn deref(&self) -> &Self::Target {
        unsafe { self.memory.as_ref() } // The self.memory is allocated and never move around so it can be pinned. Also there is no other way to move it out except drop
    }
}

impl<OutType> DerefMut for ReusableBoxFuture<OutType> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.memory.as_mut() } // The self.memory is allocated and never move around so it can be pinned. Also there is no other way to move it out except drop
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use std::panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe};

    use foundation::prelude::FoundationAtomicU16;

    use super::*;

    struct TestFutureMock {
        dropped: FoundationAtomicU16,
    }

    impl TestFutureMock {
        fn was_dropped(&self) -> bool {
            self.dropped.load(std::sync::atomic::Ordering::SeqCst) > 0
        }

        fn dropped(&self, val: u16) {
            self.dropped.fetch_add(val, std::sync::atomic::Ordering::SeqCst);
        }

        fn get_dropped(&self) -> u16 {
            self.dropped.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    struct TestFuture {
        mock: Arc<TestFutureMock>,
    }

    impl Default for TestFuture {
        fn default() -> Self {
            let mock = Arc::new(TestFutureMock {
                dropped: FoundationAtomicU16::new(0),
            });

            Self { mock: mock }
        }
    }

    impl Future for TestFuture {
        type Output = u32;

        fn poll(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            std::task::Poll::Ready(1)
        }
    }

    impl Drop for TestFuture {
        fn drop(&mut self) {
            self.mock.dropped(1);
        }
    }

    struct TestFuture2 {
        mock: Arc<TestFutureMock>,
    }

    impl Future for TestFuture2 {
        type Output = u32;

        fn poll(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            std::task::Poll::Ready(1)
        }
    }

    impl Drop for TestFuture2 {
        fn drop(&mut self) {
            self.mock.dropped(1234);
        }
    }

    struct TestFuturePanic {
        mock: Arc<TestFutureMock>,
    }

    impl Future for TestFuturePanic {
        type Output = u32;

        fn poll(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            std::task::Poll::Ready(1)
        }
    }

    impl Drop for TestFuturePanic {
        fn drop(&mut self) {
            self.mock.dropped(100);
            panic!("I am panicking....");
        }
    }

    fn get_mock() -> (TestFuture, Arc<TestFutureMock>) {
        let mock = Arc::new(TestFutureMock {
            dropped: FoundationAtomicU16::new(0),
        });

        (TestFuture { mock: mock.clone() }, mock)
    }

    fn get_mock_panic() -> (TestFuturePanic, Arc<TestFutureMock>) {
        let mock = Arc::new(TestFutureMock {
            dropped: FoundationAtomicU16::new(0),
        });

        (TestFuturePanic { mock: mock.clone() }, mock)
    }

    #[test]
    fn test_future_is_dropped_when_lifetime_ends() {
        {
            let (fut, mock) = get_mock();

            {
                let mut p = ReusableBoxFuturePoll::<u32>::new(3, TestFuture::default());
                {
                    let r = p.next(fut);
                    assert!(r.is_ok());
                }

                assert!(mock.was_dropped());
            }
        }

        {
            let (fut, mock) = get_mock();

            let (fut1, mock1) = get_mock();

            let (fut2, _) = get_mock();

            {
                let mut p = ReusableBoxFuturePoll::<u32>::new(3, TestFuture::default());
                let mut r = p.next(fut);
                assert!(r.is_ok());

                r = p.next(fut1);
                assert!(r.is_ok());
                assert!(mock.was_dropped()); // previous was dropped

                r = p.next(fut2);
                assert!(r.is_ok());
                assert!(mock1.was_dropped());
            }
        }
    }

    #[test]
    fn test_future_is_never_dropped_once_poll_is_out() {
        let (fut, mock) = get_mock();
        let r;

        {
            let mut p = ReusableBoxFuturePoll::<u32>::new(3, TestFuture::default());
            r = p.next(fut);
            assert!(r.is_ok());
        }

        // Poll is gone, but no drop was executed
        assert!(!mock.was_dropped());

        drop(r);

        assert!(mock.was_dropped());
    }

    #[test]
    fn test_no_more_futures_return_err() {
        {
            let mut p = ReusableBoxFuturePoll::<u32>::new(3, async { 1 });

            let r = p.next(async { 1 });
            assert!(r.is_ok());

            let r1 = p.next(async { 1 });
            assert!(r1.is_ok());

            let r2 = p.next(async { 1 });
            assert!(r2.is_ok());

            let r3 = p.next(async { 1 });
            assert_eq!(r3.err().unwrap(), CommonErrors::NoData);
        }

        {
            let mut p = ReusableBoxFuturePoll::<u32>::new(3, async { 1 });

            let r = p.next(async { 1 });
            assert!(r.is_ok());

            let r1 = p.next(async { 1 });
            assert!(r1.is_ok());

            let r2 = p.next(async { 1 });
            assert!(r2.is_ok());

            drop(r2); // Even if dropped something, next one is first one so it still fails

            let r3 = p.next(async { 1 });
            assert_eq!(r3.err().unwrap(), CommonErrors::NoData);
        }
    }

    #[test]
    fn test_return_future_if_available() {
        {
            let mut p = ReusableBoxFuturePoll::<u32>::new(3, async { 1 });

            let r = p.next(async { 1 });
            assert!(r.is_ok());

            let r1 = p.next(async { 1 });
            assert!(r1.is_ok());

            let r2 = p.next(async { 1 });
            assert!(r2.is_ok());

            let r3 = p.next(async { 1 });
            assert_eq!(r3.err().unwrap(), CommonErrors::NoData);
        }

        {
            let mut p = ReusableBoxFuturePoll::<u32>::new(3, async { 1 });

            let r = p.next(async { 1 });
            assert!(r.is_ok());

            let r1 = p.next(async { 1 });
            assert!(r1.is_ok());

            let r2 = p.next(async { 1 });
            assert!(r2.is_ok());

            drop(r); // Dropped first so it shall work

            let r3 = p.next(async { 1 });
            assert!(r3.is_ok());

            drop(r1); // Dropped next so it shall work

            let r4 = p.next(async { 1 });
            assert!(r4.is_ok());

            drop(r3); // Dropped one after so it shall break

            let r5 = p.next(async { 1 });
            assert!(r5.is_err());
        }
    }

    async fn test1() -> u32 {
        let mut x = 2 * 2;
        println!("test");
        for i in 0..34 {
            x *= i;
        }
        x
    }

    async fn test2() -> u32 {
        let x = 2 * 2;
        let (fut1, _) = get_mock();

        fut1.await;
        x
    }

    #[test]
    fn test_mixing_not_compatible_futures_return_error() {
        let mut p = ReusableBoxFuturePoll::<u32>::new(3, test1());
        let mut r = p.next(test1());
        assert!(r.is_ok());

        r = p.next(test2());
        assert_eq!(r.err().unwrap(), CommonErrors::GenericError);
    }

    #[test]
    fn test_replacing_future_really_replaces_it() {
        let mut p = ReusableBoxFuturePoll::<u32>::new(1, TestFuture::default());

        let (fut, mut mock) = get_mock();
        p.next(fut).unwrap();
        assert!(mock.was_dropped());
        assert_eq!(1, mock.get_dropped());

        mock = Arc::new(TestFutureMock {
            dropped: FoundationAtomicU16::new(0),
        });

        let fut2 = TestFuture2 { mock: mock.clone() };
        p.next(fut2).unwrap();
        assert!(mock.was_dropped());
        assert_eq!(1234, mock.get_dropped());
    }

    #[test]
    fn test_panic_while_drop() {
        struct TestWrapper(ReusableBoxFuturePoll<u32>);
        impl UnwindSafe for TestWrapper {}
        impl RefUnwindSafe for TestWrapper {}

        let mut p = TestWrapper(ReusableBoxFuturePoll::<u32>::new(1, TestFuture::default()));

        let (panic_fur, panic_mock) = get_mock_panic();

        let o = p.0.next(panic_fur);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            drop(o);
        }));

        assert_eq!("I am panicking....".to_owned(), *result.err().unwrap().downcast::<&str>().unwrap());

        assert!(panic_mock.was_dropped());

        let (fut, normal_mock) = get_mock();

        let next = p.0.next(fut);

        assert!(next.is_ok());
        drop(next);

        assert!(normal_mock.was_dropped())
    }
}
