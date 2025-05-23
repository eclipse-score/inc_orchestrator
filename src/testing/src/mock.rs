//
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
//

use std::collections::VecDeque;
use std::sync::{atomic::AtomicUsize, atomic::Ordering::Relaxed};

pub trait CallableTrait<OutType> {
    fn call(&mut self) -> OutType;
}

///
/// Helper mock object
///
#[derive(Default)]
pub struct MockFnBuilder<OutType>(MockFn<OutType>);

#[derive(Default)]
pub struct MockFn<OutType> {
    call_count: AtomicUsize,
    min_count: usize,
    expected_count: usize,
    is_times_set: bool,
    is_will_once_set: bool,
    is_will_repeatedly_set: bool,
    should_ignore_check_at_drop: bool,
    repeated_value: OutType,
    returns: VecDeque<OutType>,
}

impl<OutType: Default> MockFnBuilder<OutType> {
    pub fn new() -> MockFnBuilder<OutType> {
        Self(MockFn {
            call_count: AtomicUsize::new(0),
            min_count: 0,
            expected_count: 0,
            is_times_set: false,
            is_will_once_set: false,
            is_will_repeatedly_set: false,
            should_ignore_check_at_drop: false,
            repeated_value: OutType::default(),
            returns: VecDeque::new(),
        })
    }

    ///
    /// Set how many times exactly the call() must be invoked
    /// If used, times() must be called exactly once
    ///
    pub fn times(mut self, count: usize) -> Self {
        if self.0.is_will_repeatedly_set {
            // no need to check the call count at drop() as we're panicking anyway
            self.0.should_ignore_check_at_drop = true;
            panic!("times() called after will_repeatedly()!")
        }

        self.0.is_times_set = true;
        self.0.expected_count = count;
        self
    }

    ///
    /// Ensure that the call() is invoked at least one more time and the call() returns the ret_val
    ///
    pub fn will_once(mut self, ret_val: OutType) -> Self {
        if self.0.is_will_repeatedly_set {
            // no need to check the call count at drop() as we're panicking anyway
            self.0.should_ignore_check_at_drop = true;
            panic!("will_once() called after will_repeatedly()!")
        }

        self.0.is_will_once_set = true;
        self.0.returns.push_back(ret_val);
        self.0.min_count += 1;
        self
    }

    ///
    /// Allow the call() to be invoked multiple times and the call() returns the ret_val
    /// If used, will_repeatedly() must be called the last
    ///
    pub fn will_repeatedly(mut self, ret_val: OutType) -> Self {
        if self.0.is_will_repeatedly_set {
            // no need to check the call count at drop() as we're panicking anyway
            self.0.should_ignore_check_at_drop = true;
            panic!("will_repeatedly() is called more than once!")
        }

        self.0.is_will_repeatedly_set = true;
        self.0.repeated_value = ret_val;
        self.0.min_count += 1;
        self
    }

    pub fn build(mut self) -> MockFn<OutType> {
        // if only will_once is set, the min_count becomes the expected_count
        if self.0.is_will_once_set && !self.0.is_will_repeatedly_set {
            self.0.expected_count = self.0.min_count;
        }
        self.0
    }
}

impl<OutType: Default + Clone> CallableTrait<OutType> for MockFn<OutType> {
    fn call(&mut self) -> OutType {
        self.call_count.fetch_add(1, Relaxed);

        if !self.returns.is_empty() {
            // return the ret_val in the order it was inserted (FIFO)
            self.returns.pop_front().unwrap()
        } else {
            // return the repeated_value if set or default otherwise
            if self.is_will_repeatedly_set {
                self.repeated_value.clone()
            } else {
                OutType::default()
            }
        }
    }
}

impl<OutType> Drop for MockFn<OutType> {
    fn drop(&mut self) {
        // check the call counts only if we haven't panicked before
        if !self.should_ignore_check_at_drop {
            let call_count = self.call_count.load(Relaxed);
            if self.expected_count > 0 {
                // the times() clause or only will_once() is set, so check for exact call counts
                assert_eq!(
                    call_count, self.expected_count,
                    "MockFn is called {} times, but should be {} times!",
                    call_count, self.expected_count
                );
            } else {
                // check whether the mock fn is called at least min_count due to its configuration
                assert!(
                    self.min_count <= call_count,
                    "MockFn is called {} times, but should be at least {} times!",
                    call_count,
                    self.min_count
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_times_only() {
        let mut mock = MockFnBuilder::<bool>::new().times(3).build();

        for _ in 0..3 {
            assert!(!mock.call());
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_call_count_less_than_specified_times() {
        let mut mock = MockFnBuilder::<bool>::new().times(3).build();

        for _ in 0..2 {
            assert!(!mock.call());
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_call_count_more_than_specified_times() {
        let mut mock = MockFnBuilder::<bool>::new().times(3).build();

        for _ in 0..4 {
            assert!(!mock.call());
        }
    }

    #[test]
    fn test_call_count_equals_will_once_count() {
        let mut mock = MockFnBuilder::<bool>::new().will_once(true).will_once(false).build();

        assert!(mock.call());
        assert!(!mock.call());
    }

    #[test]
    #[should_panic]
    fn test_call_count_more_than_will_once_count() {
        let mut mock = MockFnBuilder::<bool>::new().will_once(true).will_once(false).build();

        assert!(mock.call());
        assert!(!mock.call());
        mock.call();
    }

    #[test]
    #[should_panic]
    fn test_panic_call_count_less_than_will_once_count() {
        let mut mock = MockFnBuilder::<bool>::new().will_once(true).will_once(false).build();

        assert!(mock.call());
    }

    #[test]
    fn test_with_will_repeated_only() {
        let mut mock = MockFnBuilder::<bool>::new().will_repeatedly(true).build();

        for _ in 0..3 {
            assert!(mock.call());
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_call_count_less_than_min_count_with_repeatedly() {
        let mut mock = MockFnBuilder::<bool>::new().will_once(true).will_repeatedly(false).build();

        assert!(mock.call());
    }

    #[test]
    fn test_err_with_multiple_will_repeatedly() {
        let result = std::panic::catch_unwind(|| MockFnBuilder::<bool>::new().will_repeatedly(true).will_repeatedly(false).build());

        assert!(result.is_err());
    }

    #[test]
    fn test_err_with_will_once_after_will_repeatedly() {
        let result = std::panic::catch_unwind(|| MockFnBuilder::<bool>::new().will_repeatedly(false).will_once(true).build());

        assert!(result.is_err());
    }

    #[test]
    fn test_err_with_times_after_will_repeatedly() {
        let result = std::panic::catch_unwind(|| MockFnBuilder::<bool>::new().will_repeatedly(false).times(1).build());

        assert!(result.is_err());
    }
}
