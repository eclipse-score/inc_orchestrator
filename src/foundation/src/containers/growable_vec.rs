use std::{
    alloc::Layout,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    slice::{Iter, IterMut},
};

use iceoryx2_bb_container::vec::*;

///
/// [`GrowableVec`] is extension to iceoryx2 [`Vec`] with has one time dynamically allocated size. This implementation will grow the size of container when
/// there is no more space while adding data until the call to `lock()` method that does not allow it to grow any-longer. This is useful when You don't know
/// size at compile time, but You are fine with allocations until `init` phase is done. Primary use case is to use in builders when not knowing size in advance.
///
/// The [`GrowableVec`] will grow by `2` each time there is no more space to push value into.
///
pub struct GrowableVec<T> {
    inner: Vec<MaybeUninit<T>>, // MaybeUninit only for regrow, it's zero cast anyway
    is_locked: bool,
}

impl<T> GrowableVec<T> {
    pub fn new(init_size: usize) -> Self {
        assert_eq!(Layout::new::<T>(), Layout::new::<MaybeUninit<T>>());

        Self {
            inner: Vec::new(init_size),
            is_locked: false,
        }
    }

    /// Locks vector, that it will not grow anymore
    pub fn lock(&mut self) {
        self.is_locked = true;
    }

    /// Returns lock status
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }

    /// Returns the capacity of the vector
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns the number of elements stored inside the vector
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the vector is empty, otherwise false
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns true if the vector is full, otherwise false
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns a  slice to the contents of the vector
    pub fn as_slice(&self) -> &[T] {
        let len = self.inner.len();
        unsafe { std::slice::from_raw_parts(self.inner.as_slice().as_ptr().cast(), len) }
    }

    /// Returns a mutable slice to the contents of the vector
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        let len = self.inner.len();
        unsafe { std::slice::from_raw_parts_mut(self.inner.as_mut_slice().as_mut_ptr().cast(), len) }
    }

    /// Remove and return last elem in container
    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop().map(|mu| unsafe { mu.assume_init() })
    }

    // Adds `value` to end of vector. Return true if action succeeded
    pub fn push(&mut self, value: T) -> bool {
        if self.inner.is_full() && !self.is_locked {
            self.reallocate_internal();
        }

        self.inner.push(MaybeUninit::new(value))
    }

    /// Remove all elements in container
    pub fn clear(&mut self) {
        for _ in 0..self.inner.len() {
            unsafe {
                self.inner.pop().unwrap().assume_init_drop();
            }
        }
    }

    ///
    /// Simple and naive impl of move by copy, as this is for init phase, we don't care about performance here.
    ///
    fn reallocate_internal(&mut self) {
        // TODO: This is workaround, proper impl is simple but requires access to MetaVec internals which is not possible currently.
        // We can copy a code from MetaVec and adapt if we need something better.
        let mut new_container = Vec::new(self.inner.capacity() * 2);

        // Fake that we have data inside so internal push position gets correct
        for _ in 0..self.inner.len() {
            new_container.push(MaybeUninit::uninit());
        }

        // Do the copy from source
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.inner.as_slice().as_ptr(),
                new_container.as_mut_slice().as_mut_ptr(),
                self.inner.len(),
            );
        }
        // manually forget all data since since it was copied -> results in move like action
        self.inner.clear(); // Inner clear to drop data as MaybeUnint prevent drop on T

        // move into self
        self.inner = new_container;
    }
}

impl<T> Drop for GrowableVec<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T> Deref for GrowableVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> DerefMut for GrowableVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

///
/// Inefficient implementation with double copy
///
/// We only allow conversion into Vec and not vice versa. Hence 'from' is not supported
///
#[allow(clippy::from_over_into)]
impl<T> Into<Vec<T>> for GrowableVec<T> {
    fn into(mut self) -> Vec<T> {
        let mut first = Vec::new(self.len());

        // Reverse order
        for _ in 0..self.len() {
            first.push(self.pop().unwrap());
        }

        let len = first.len();

        // Correct order...
        for i in 0..(len / 2) {
            first.swap(i, len - i - 1);
        }

        first
    }
}

unsafe impl<T: Send> Send for GrowableVec<T> {} // if type is send, so we are

impl<'a, T> IntoIterator for &'a GrowableVec<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut GrowableVec<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {

    use super::*;

    #[test]
    fn test_for_loop_support() {
        {
            let mut data = GrowableVec::<u8>::new(10);
            data.push(1);
            data.push(2);
            data.push(3);

            let mut i: u8 = 1;
            for e in &data {
                assert_eq!(i, *e);
                i += 1;
            }
        }

        {
            let mut data = GrowableVec::<u8>::new(10);
            data.push(1);
            data.push(2);
            data.push(3);

            let mut i: u8 = 1;
            for e in &mut data {
                assert_eq!(i, *e);

                i += 1;
                *e = *e + 1;
                assert_eq!(i, *e);
            }
        }
    }

    #[test]
    fn test_created_with_correct_size() {
        assert_eq!(100, GrowableVec::<u8>::new(100).capacity());
        assert_eq!(77, GrowableVec::<u8>::new(77).capacity());
        assert_eq!(1234, GrowableVec::<u8>::new(1234).capacity());
    }

    #[test]
    fn test_when_no_more_space_and_not_locked_grows() {
        let mut data = GrowableVec::<u8>::new(3);

        data.push(1);
        data.push(2);
        data.push(3);

        assert!(data.push(1));
        assert_eq!(6, data.capacity()); // double the size
        assert_eq!(4, data.len()); // still 4 items are there
    }

    #[test]
    fn test_when_no_more_space_and_locked_not_grows() {
        let mut data = GrowableVec::<u8>::new(3);

        data.push(1);
        data.push(2);
        data.push(3);

        data.lock();

        assert!(!data.push(1));
        assert_eq!(3, data.capacity()); // no grow
        assert_eq!(3, data.len()); // same items

        assert!(data.is_locked());
    }

    #[test]
    fn test_is_empty() {
        let mut data = GrowableVec::<u8>::new(1);

        assert!(data.is_empty());
        data.push(1);

        assert!(!data.is_empty());

        data.push(1);
        data.push(1);
        data.push(1);

        data.clear();
        assert!(data.is_empty());
    }

    fn check_data(data: &[usize], count: usize, start_val: usize) {
        for i in 0..count {
            assert_eq!(data[i], start_val + i);
        }
    }

    #[test]
    fn test_data_is_preserved() {
        fn add_data(data: &mut GrowableVec<usize>, count: usize, start_val: usize) {
            for i in start_val..count + start_val {
                data.push(i);
            }
        }

        let mut data = GrowableVec::<usize>::new(1);

        assert!(data.is_empty());

        add_data(&mut data, 10, 0);
        assert_eq!(10, data.len());
        check_data(&data, 10, 0);

        add_data(&mut data, 20, 10);
        assert_eq!(30, data.len());
        check_data(&data, 30, 0);

        add_data(&mut data, 3, 30);
        assert_eq!(33, data.len());
        check_data(&data, 33, 0);
    }

    #[test]
    fn test_no_double_drop() {
        let mut is_used = false;
        let validator = move || {
            assert!(!is_used);
            is_used = true;
        };

        struct TestData<T: FnMut()> {
            v: T,
        }

        impl<T: FnMut()> Drop for TestData<T> {
            fn drop(&mut self) {
                (self.v)();
            }
        }

        let mut data = GrowableVec::new(1);
        data.push(TestData { v: validator });
        data.push(TestData { v: validator });
    }

    #[test]
    fn test_into_vec() {
        // even
        {
            let mut data = GrowableVec::<usize>::new(1);
            data.push(1);
            data.push(2);
            data.push(3);
            data.push(4);

            let iv: Vec<usize> = data.into();

            assert_eq!(iv.len(), 4);

            check_data(&iv, 4, 1);
        }

        //uneven
        {
            let mut data = GrowableVec::<usize>::new(1);
            data.push(1);
            data.push(2);
            data.push(3);
            data.push(4);
            data.push(5);

            let iv: Vec<usize> = data.into();

            assert_eq!(iv.len(), 5);

            check_data(&iv, 5, 1);
        }
    }
}
