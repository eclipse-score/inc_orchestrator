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

use std::alloc::{self, dealloc, Layout};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

use iceoryx2_bb_container::vec::Vec;

use crate::prelude::FoundationAtomicU8;
use crate::types::CommonErrors;

pub trait ReusableObjectTrait {
    ///
    /// Reusable objects calls drop only when their pool is gone and they are not used anymore. Otherwise when they return
    /// to pool, we use this API to clear it's state. What it means is up to the user.
    ///
    /// > NOTE: We do this instead drop because imagine you have some container that does dynamic allocation that you want to reuse. If we would
    /// > drop it, it would deallocate and allocate again. With this you have a full control what You do when you stop using your object.
    ///
    fn reusable_clear(&mut self);
}

///
/// Reusable, round-robin object pool that keeps your object **NOT DROPPED** until the pool is gone and object is not used anymore.
/// This pool is `!Sync`, but it allows `Send` it's object across threads once `T` is send. It can still properly manage returning back objects to the pool.
///
///
pub struct ReusableObjects<T: ReusableObjectTrait> {
    storage: Box<[NonNull<T>]>,
    states: Box<[Arc<ObjectState>]>, // state of boxes, matches via index with above
    position: usize,                 // position in round robin queue
    size: usize,                     // number of futures
}

impl<T: ReusableObjectTrait> ReusableObjects<T> {
    ///
    /// Creates a pool with `cnt` objects available using `init` fn
    ///
    pub fn new<U: Fn(usize) -> T>(cnt: usize, init: U) -> Self {
        assert!(cnt != 0, "Pool cannot be 0 sized!");

        let storage = Self::create_arr_storage(cnt, |i| unsafe {
            let input_layout = Layout::new::<T>();
            let memory = alloc::alloc(input_layout);
            let typed = memory as *mut T;
            typed.write(init(i));

            NonNull::new(typed).unwrap()
        });

        let states = Self::create_arr_storage(cnt, |_| Arc::new(ObjectState::new()));

        Self {
            storage,
            states,
            position: 0,
            size: cnt,
        }
    }

    ///
    /// Returns a wrapper to next available object or error if no next object available
    ///
    pub fn next_object(&mut self) -> Result<ReusableObject<T>, CommonErrors> {
        let index = self.position % self.size;

        let atomic_ref = &self.states[index].0;

        //Safety: forms also a barrier for code reorder
        match atomic_ref.compare_exchange(
            OBJECT_FREE,
            OBJECT_TAKEN,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(_) => return Err(CommonErrors::NoData), // next is not free yet, this is user problem now
        }

        self.position += 1; // increase pos, so next one will take correct box

        Ok(ReusableObject {
            storage: self.storage[index],
            state: self.states[index].clone(),
        })
    }

    fn create_arr_storage<Type, U: Fn(usize) -> Type>(size: usize, init: U) -> Box<[Type]> {
        let layout = Layout::array::<Type>(size).unwrap();

        // SAFETY: We are manually allocating memory here
        let ptr = unsafe { alloc::alloc(layout) as *mut Type };

        for i in 0..size {
            unsafe {
                ptr.add(i).write(init(i)); // just filling with values as it has to be initialized
            }
        }

        // SAFETY: Create boxed slice from raw parts
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, size)) }
    }
}

///
/// Helper wrapper for user type that implements `Deref` to `T`.
///
/// > NOTE: This does not implement `DerefMut` because this can change underlying object in unexpected way.
/// > Imagine ie. that you have a Vec<T> that has dynamically allocated size at start, and then you assign other instance to it with different size.
/// > You would have your pool with Vec<T> with different sizes. To prevent that, we don't allow mutable access. We recommend to implement additional
/// > API for certain types using pattern `pub struct  NewType(ReusableObject<Vec<T>>)` or implementing own trait for specific type.
///
pub struct ReusableObject<T: ReusableObjectTrait> {
    storage: NonNull<T>,
    state: Arc<ObjectState>, // state of boxes, matches via index with above
}

unsafe impl<T: ReusableObjectTrait + Send> Send for ReusableObject<T> {}

impl<T: ReusableObjectTrait> ReusableObject<T> {
    ///
    /// Gets mutable access to underlying container.
    ///
    /// # Safety
    ///   - user needs to ensure that inner (`T`) is not replaced by other instance, otherwise it can lead to undefined behavior like losing same properties for all reusable objects
    ///
    /// # Usage
    ///   - it's advised to use this fn only when implementing new type (pub struct  NewType(ReusableObject<Vec<T>>)) to expose new API and not &mut inner  or implementing custom trait
    ///
    pub unsafe fn as_inner_mut(&mut self) -> &mut T {
        self.storage.as_mut()
    }
}

impl<T: ReusableObjectTrait> Drop for ReusableObjects<T> {
    fn drop(&mut self) {
        for index in 0..self.size {
            let object = &self.storage[index];

            // Here we assume object is in usage, if yes, we are setting flag and dealloc will happen at future side, if not, then we had OBJECT_FREE and we can drop & dealloc
            match self.states[index].0.compare_exchange(
                OBJECT_TAKEN,
                OBJECT_POOL_GONE,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(actual) => unsafe {
                    assert_eq!(actual, OBJECT_FREE);
                    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst); // we will drop the value, so we must sync memory first so we call drop() on current object
                    object.drop_in_place();
                    dealloc(object.as_ptr() as *mut u8, Layout::new::<T>());
                },
            }
        }
    }
}

impl<T: ReusableObjectTrait> Drop for ReusableObject<T> {
    fn drop(&mut self) {
        struct DropGuard<'a, T: ReusableObjectTrait> {
            this: &'a mut ReusableObject<T>,
        }

        impl<T: ReusableObjectTrait> DropGuard<'_, T> {
            fn reusable_clear(&mut self) {
                unsafe {
                    self.this.storage.as_mut().reusable_clear();
                }
            }
        }

        impl<T: ReusableObjectTrait> Drop for DropGuard<'_, T> {
            fn drop(&mut self) {
                match self.this.state.0.compare_exchange(
                    OBJECT_TAKEN,
                    OBJECT_FREE,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {}

                    // Means that pool is dropped and we need to cleanup own memory
                    Err(val) => {
                        assert_eq!(val, OBJECT_POOL_GONE);
                        unsafe {
                            self.this.storage.drop_in_place();
                            dealloc(self.this.storage.as_ptr() as *mut u8, Layout::new::<T>());
                        }
                    }
                }
            }
        }

        // Panic protector when it happens in _clear
        let mut guard = DropGuard { this: self }; // make sure that after clear, we fire sync logic
        guard.reusable_clear();
    }
}

impl<T: ReusableObjectTrait> Deref for ReusableObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.storage.as_ref() }
    }
}

const OBJECT_TAKEN: u8 = 1; // Object is taken by user, not available
const OBJECT_FREE: u8 = 0; // Object is in a pool, can be taken
const OBJECT_POOL_GONE: u8 = 0xFF; // Pool is dropped or dropping and the object has to drop itself instead being dropped by pool

struct ObjectState(FoundationAtomicU8);

impl ObjectState {
    fn new() -> Self {
        Self(FoundationAtomicU8::new(OBJECT_FREE))
    }
}

// Macro to implement for basic types
macro_rules! impl_reusable_for_primitive {
    ($($t:ty),*) => {
        $(
            impl ReusableObjectTrait for $t {
                #[inline]
                fn reusable_clear(&mut self) {
                    *self = Default::default();
                }
            }
        )*
    };
}

// Implement for common primitive types
impl_reusable_for_primitive!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, char);

// Implement for iceoryx2 Vec<T>
impl<T> ReusableObjectTrait for Vec<T> {
    fn reusable_clear(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use crate::prelude::FoundationAtomicUsize;

    use super::*;
    use std::sync::Arc;

    pub struct TestObject {
        id: usize,
        value: i32,
        clear_count: Arc<FoundationAtomicUsize>,
    }

    impl ReusableObjectTrait for TestObject {
        fn reusable_clear(&mut self) {
            self.value = 0;
            self.clear_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn test_create_and_access() {
        // Test that we can create a pool and access objects
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let mut pool = ReusableObjects::new(5, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });

        // Get first object
        let obj1 = pool.next_object().expect("Should get first object");
        assert_eq!(obj1.id, 0);
        assert_eq!(obj1.value, 10);

        // Get second object
        let obj2 = pool.next_object().expect("Should get second object");
        assert_eq!(obj2.id, 1);
        assert_eq!(obj2.value, 10);
    }

    #[test]
    fn test_round_robin() {
        // Test round-robin behavior
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let mut pool = ReusableObjects::new(3, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });

        // Take all objects
        let obj1 = pool.next_object().expect("Should get first object");
        let obj2 = pool.next_object().expect("Should get second object");
        let obj3 = pool.next_object().expect("Should get third object");

        assert_eq!(obj1.id, 0);
        assert_eq!(obj2.id, 1);
        assert_eq!(obj3.id, 2);

        // Try to get more - should fail
        assert!(pool.next_object().is_err());

        // Drop one and try again - should get first object again
        drop(obj1);
        let obj4 = pool.next_object().expect("Should get an object after dropping one");
        assert_eq!(obj4.id, 0); // We expect the first ID again due to round-robin
    }

    #[test]
    fn test_reusable_clear() {
        // Test that reusable_clear is called when object is returned to pool
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let mut pool = ReusableObjects::new(1, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });

        {
            let obj = pool.next_object().expect("Should get object");
            assert_eq!(obj.value, 10);
            // obj will be dropped here
        }

        // Check that clear was called once
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);

        // Get object again and verify it was cleared
        let obj = pool.next_object().expect("Should get object again");
        assert_eq!(obj.value, 0);
    }

    #[test]
    fn test_exhaustion_recovery() {
        // Test behavior when pool is exhausted and then objects are returned
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let mut pool = ReusableObjects::new(2, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });

        // Take all objects
        let obj1 = pool.next_object().expect("Should get first object");
        let obj2 = pool.next_object().expect("Should get second object");

        // Try to get more - should fail
        assert!(pool.next_object().is_err());

        // Return all objects
        drop(obj1);
        drop(obj2);

        // Should be able to get objects again
        let obj1 = pool.next_object().expect("Should get object again");
        let obj2 = pool.next_object().expect("Should get object again");
        assert_eq!(obj1.id, 0);
        assert_eq!(obj2.id, 1);
    }

    #[test]
    fn test_drop_while_objects_in_use() {
        // Test that objects are properly cleaned up when pool is dropped while objects are still in use
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let object_holder = {
            let mut pool = ReusableObjects::new(1, |i| TestObject {
                id: i,
                value: 10,
                clear_count: cc.clone(),
            });

            // Get the object
            let obj = pool.next_object().expect("Should get object");

            // Return the object but drop the pool
            obj
            // Pool is dropped here
        };

        // Object should still be usable
        assert_eq!(object_holder.id, 0);
        assert_eq!(object_holder.value, 10);

        // When we drop the object, it should clean itself up instead of returning to the pool
        drop(object_holder);

        // Verify the object was cleared before being dropped
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_multiple_drops_and_acquires() {
        // Test repeated acquisition and dropping of objects
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        let mut pool = ReusableObjects::new(3, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });

        // Cycle 1
        let obj1 = pool.next_object().expect("Should get object");
        let obj2 = pool.next_object().expect("Should get object");
        drop(obj1);
        let obj3 = pool.next_object().expect("Should get object");
        drop(obj2);
        drop(obj3);

        // Cycle 2
        let obj1 = pool.next_object().expect("Should get object");
        let obj2 = pool.next_object().expect("Should get object");
        let obj3 = pool.next_object().expect("Should get object");
        drop(obj2);
        drop(obj1);
        drop(obj3);

        // Check clear count
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 6);
    }

    // Add a test for zero-sized pool
    #[test]
    #[should_panic]
    fn test_zero_sized_pool() {
        let clear_count = Arc::new(FoundationAtomicUsize::new(0));
        let cc = clear_count.clone();

        // Pool with 0 size shall assert
        let _pool = ReusableObjects::new(0, |i| TestObject {
            id: i,
            value: 10,
            clear_count: cc.clone(),
        });
    }
}

#[cfg(test)]
#[cfg(loom)]
mod tests {
    use crate::prelude::FoundationAtomicUsize;

    use super::*;

    use std::sync::atomic::Ordering;

    use loom::model::Builder;

    pub struct TestObject {
        id: usize,
        value: i32,
        clear_count: Arc<FoundationAtomicUsize>,
    }

    impl ReusableObjectTrait for TestObject {
        fn reusable_clear(&mut self) {
            self.value = 0;
            self.clear_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_thread_safety() {
        use loom::model::Builder;

        let builder = Builder::new();

        builder.check(|| {
            let clear_count = Arc::new(FoundationAtomicUsize::new(0));
            let cc = clear_count.clone();

            let mut pool = ReusableObjects::new(1, |i| TestObject {
                id: i,
                value: 10,
                clear_count: cc.clone(),
            });

            let obj = pool.next_object().expect("Should get object");

            // Send object to another thread
            let handle = loom::thread::spawn(move || {
                // Access object in another thread
                assert_eq!(obj.id, 0);
                assert_eq!(obj.value, 10);
                // Object will be dropped and returned to pool when thread exits
            });

            let _ = handle.join().unwrap();

            // Should be able to get the object back now
            let obj = pool.next_object().expect("Should get object back after thread completes");
            assert_eq!(obj.id, 0);
            assert_eq!(obj.value, 0); // Value should be cleared
        });
    }

    #[test]
    fn test_reusable_object() {
        let builder = Builder::new();

        builder.check(|| {
            let mut pool = ReusableObjects::new(2, |_| 0);

            let obj1 = pool.next_object().unwrap();
            let obj2 = pool.next_object().unwrap();

            let handle = loom::thread::spawn(move || {
                let x = obj2.deref() + 1;
                x
            });

            assert!(pool.next_object().is_err()); // obj1 is still used so we cannot take any new one.

            drop(obj1);

            let _ = handle.join();

            {
                let _obj1 = pool.next_object().unwrap();
                let _obj2 = pool.next_object().unwrap();
            }
        });
    }

    #[test]
    fn test_reusable_object_with_values() {
        let builder = Builder::new();

        builder.check(|| {
            let mut pool = ReusableObjects::new(2, |i| i);

            let obj1 = pool.next_object().unwrap();
            let obj2 = pool.next_object().unwrap();

            assert_eq!(*obj1, 0);
            assert_eq!(*obj2, 1);

            let handle = loom::thread::spawn(move || {
                drop(obj1);
            });

            let res = pool.next_object();
            match res {
                Ok(v) => assert_eq!(0, *v),
                Err(_) => {}
            }

            let _ = handle.join();
        });
    }
}
