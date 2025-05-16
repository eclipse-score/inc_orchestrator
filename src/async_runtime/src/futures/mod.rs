// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

pub mod reusable_box_future;
pub mod yield_now;

///
/// Helper state that can be used to implement Futures.
///
/// # Note
/// Look into [`JoinHandle`] as example.
///
#[derive(Copy, Clone, PartialEq)]
pub enum FutureState {
    New,      // called first time
    Polled,   // polled 1..N times
    Finished, // done
}

impl FutureState {
    ///
    /// Assigns a state from `internal` and translate it into a [`std::task::Poll`] so it compatible with trait Future interface
    ///
    pub fn assign_and_propagate<T>(&mut self, internal: FutureInternalReturn<T>) -> std::task::Poll<T> {
        *self = internal.0;
        internal.into()
    }
}

///
/// In Futures you need to return Pending or Ready with value. This small helper is able to connect type-less FutureState
/// with value of type `T` than can be returned in case of ready. This also provides `into` to match [`Future`] API.
///
pub struct FutureInternalReturn<T>(FutureState, Option<T>);

impl<T> Default for FutureInternalReturn<T> {
    fn default() -> Self {
        Self::polled()
    }
}

impl<T> FutureInternalReturn<T> {
    ///
    /// Use when pool was returning `Pending`
    ///
    pub fn polled() -> Self {
        Self(FutureState::Polled, None)
    }

    ///
    /// Use when Future returned `Ready` with value
    ///
    pub fn ready(value: T) -> Self {
        Self(FutureState::Finished, Some(value))
    }
}

#[allow(clippy::from_over_into)] // Only one direction conversion
impl<T> Into<std::task::Poll<T>> for FutureInternalReturn<T> {
    fn into(self) -> std::task::Poll<T> {
        match self.1 {
            Some(v) => std::task::Poll::Ready(v),
            None => std::task::Poll::Pending,
        }
    }
}
