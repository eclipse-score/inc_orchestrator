// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

use foundation::prelude::FoundationAtomicBool;

pub struct RuntimeSequentialAccess<T> {
    data: UnsafeCell<T>,
    is_used: FoundationAtomicBool,
}

// Safety: `RuntimeSequentialAccess` assumes single-threaded execution, so we manually implement `Send` and `Sync`
// Attention: This is unfinished as it shall act more like async mutex which goes into sleep once there is no lock available
unsafe impl<T> Send for RuntimeSequentialAccess<T> {}
unsafe impl<T> Sync for RuntimeSequentialAccess<T> {}

impl<T> RuntimeSequentialAccess<T> {
    pub fn new(value: T) -> Self {
        Self {
            data: UnsafeCell::new(value),
            is_used: FoundationAtomicBool::new(false),
        }
    }

    pub fn is_locked(&self) -> bool {
        self.is_used.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn lock(&self) -> RuntimeSequentialAccessGuard<T> {
        if self
            .is_used
            .compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
            .is_err()
        {
            panic!("Trying to take a fake lock in orchestration from owned object while this object is being executed");
        }

        RuntimeSequentialAccessGuard { fake_mtx: self }
    }
}

// Scoped guard that allows mutable access while it's held
pub struct RuntimeSequentialAccessGuard<'a, T> {
    fake_mtx: &'a RuntimeSequentialAccess<T>,
}

impl<T> Drop for RuntimeSequentialAccessGuard<'_, T> {
    fn drop(&mut self) {
        self.fake_mtx.is_used.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl<T> Deref for RuntimeSequentialAccessGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.fake_mtx.data.get() }
    }
}

impl<T> DerefMut for RuntimeSequentialAccessGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.fake_mtx.data.get() }
    }
}
