use iceoryx2_bb_elementary::{owning_pointer::OwningPointer, pointer_trait::PointerTrait};
use std::mem::{replace, MaybeUninit};

///
/// Internal basic implementation of FIFO queue that can be used to implement further advanced queues. The internals are pub(crate) to let build oder containers by this
/// crate, still providing simple `Queue` to other crates.
///
pub struct Queue<T> {
    pub(crate) data: OwningPointer<MaybeUninit<T>>,
    pub(crate) head: usize,
    pub(crate) tail: usize,
    pub(crate) capacity: usize,
}

unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Queue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());

        Self {
            data: OwningPointer::new_with_alloc(capacity),
            head: 0,
            tail: 0,
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.head - self.tail
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    pub fn push(&mut self, item: T) -> bool {
        if self.len() == self.capacity {
            return false;
        }

        unsafe {
            self.data.as_mut_ptr().add(self.head & (self.capacity - 1)).write(MaybeUninit::new(item));
        }
        self.head += 1;
        true
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let item: T = unsafe { replace(&mut *self.data.as_mut_ptr().add(self.tail & (self.capacity - 1)), MaybeUninit::uninit()).assume_init() };

        self.tail += 1;
        Some(item)
    }
}
