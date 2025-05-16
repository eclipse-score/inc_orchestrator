use crate::prelude::*;
use crate::{prelude::FoundationAtomicBool, types::CommonErrors};

use super::queue::Queue;
use std::sync::{Arc, Condvar, Mutex};

///
/// Implements the **threadsafe** FIFO queue that triggers consumer once new data is added. This is MPSC implementation
///
pub struct TriggerQueue<T> {
    mtx: Mutex<Queue<T>>,
    cv: Condvar,
    state: FoundationAtomicBool, // true when sleeping, used to not do notify (syscall) when consumer is anyway not waiting on it
    has_consumer: FoundationAtomicBool,
}

unsafe impl<T: Send> Send for TriggerQueue<T> {}
unsafe impl<T> Sync for TriggerQueue<T> {}

pub struct TriggerQueueConsumer<T> {
    queue: Arc<TriggerQueue<T>>,
}

impl<T> TriggerQueueConsumer<T> {
    ///
    /// Pops single element from queue if present
    ///
    pub fn pop(&self) -> Option<T> {
        self.queue.pop()
    }

    ///
    /// Pops up to `container.size()` elements from the queue using single lock operation. This helps to minimize number of OS calls if you have a local storage
    ///
    pub fn pop_into_vec(&self, container: &mut Vec<T>) {
        self.queue.pop_into_vec(container)
    }

    ///
    /// Pops an item from a queue once it's available, otherwise it does block up to `dur`.
    ///
    pub fn pop_blocking_with_timeout(&self, dur: std::time::Duration) -> Result<T, CommonErrors> {
        self.queue.pop_blocking_with_timeout(dur)
    }

    ///
    /// Returns max number of elements queue can hold
    ///
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }
}

impl<T> Drop for TriggerQueueConsumer<T> {
    fn drop(&mut self) {
        self.queue.has_consumer.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl<T> TriggerQueue<T> {
    ///
    /// Creates queue with `count` size, must be power of two
    ///
    pub fn new(count: usize) -> Self {
        Self {
            mtx: Mutex::new(Queue::new(count)),
            cv: Condvar::new(),
            state: FoundationAtomicBool::new(false),
            has_consumer: FoundationAtomicBool::new(false),
        }
    }

    ///
    /// Returns max number of elements queue can hold
    ///
    pub fn capacity(&self) -> usize {
        self.mtx.lock().unwrap().capacity()
    }

    ///
    /// Returns  consumer if there is non existing already
    ///
    pub fn get_consumer(self: &Arc<Self>) -> Option<TriggerQueueConsumer<T>> {
        match self
            .has_consumer
            .compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
        {
            Ok(_) => Some(TriggerQueueConsumer { queue: self.clone() }),
            Err(_) => None,
        }
    }
    ///
    /// Push data into a queue and notify consumer only if it does already wait on being notified.
    ///
    pub fn push(&self, value: T) -> bool {
        let mut data = self.mtx.lock().unwrap();
        let ret = data.push(value);

        if self.state.load(std::sync::atomic::Ordering::Relaxed) {
            drop(data);
            self.cv.notify_one();
        }

        ret
    }

    fn pop(&self) -> Option<T> {
        self.mtx.lock().unwrap().pop()
    }

    fn pop_into_vec(&self, container: &mut Vec<T>) {
        let mut data = self.mtx.lock().unwrap();

        loop {
            if container.is_full() {
                return;
            }

            let val = data.pop();
            if val.is_none() {
                return;
            }

            container.push(val.unwrap());
        }
    }

    fn pop_blocking_with_timeout(&self, dur: std::time::Duration) -> Result<T, CommonErrors> {
        let data = self.mtx.lock().unwrap();

        self.state.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut res = self.cv.wait_timeout_while(data, dur, |guard| guard.is_empty()).unwrap();
        self.state.store(false, std::sync::atomic::Ordering::Relaxed);

        if res.1.timed_out() {
            Err(CommonErrors::Timeout)
        } else {
            Ok(res.0.pop().unwrap()) // As we did not timed out and we are single consumer, there must be value inside
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use std::{
        sync::{atomic::Ordering, Arc},
        thread,
        time::Duration,
    };

    pub fn collect_from_iterator<I, T>(iter: I, size: usize) -> Vec<T>
    where
        I: IntoIterator<Item = T>,
    {
        let mut result = Vec::new(size);

        for item in iter {
            result.push(item);
        }

        result
    }

    #[test]
    fn test_pop_empty_queue() {
        let queue = TriggerQueue::<i32>::new(128);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_pop_single_element() {
        let queue = TriggerQueue::<i32>::new(128);
        queue.push(42);
        assert_eq!(queue.pop(), Some(42));
        // Queue should be empty after pop
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_push_pop_multiple_elements() {
        let queue = TriggerQueue::<i32>::new(128);

        // Push multiple elements
        queue.push(1);
        queue.push(2);
        queue.push(3);

        // Pop them in FIFO order
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_pop_into_vec_empty_queue() {
        let queue = TriggerQueue::<i32>::new(128);
        let mut vec = Vec::new(128);
        queue.pop_into_vec(&mut vec);
        assert!(vec.is_empty());
    }

    #[test]
    fn test_pop_into_vec_partial_fill() {
        let queue = TriggerQueue::<i32>::new(128);

        // Push elements
        queue.push(1);
        queue.push(2);

        // Create a vector with capacity for more elements
        let mut vec = Vec::new(128);
        queue.pop_into_vec(&mut vec);

        // Should contain exactly the elements pushed
        assert_eq!(vec, collect_from_iterator(1..3, 128));

        // Queue should be empty now
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_pop_into_vec_full_container() {
        let queue = TriggerQueue::<i32>::new(128);

        for i in 1..=10 {
            queue.push(i);
        }

        let mut vec = Vec::new(10);
        queue.pop_into_vec(&mut vec);

        // Should take as many elements as possible
        assert_eq!(vec.len(), 10);
        assert_eq!(vec, collect_from_iterator(1..11, 10));

        // Queue should be empty
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_pop_blocking_with_timeout_empty_queue() {
        let queue = TriggerQueue::<i32>::new(128);

        // Should timeout when queue is empty
        let result = queue.pop_blocking_with_timeout(Duration::from_millis(50));
        assert!(matches!(result, Err(CommonErrors::Timeout)));
    }

    #[test]
    fn test_pop_blocking_with_timeout_immediate_data() {
        let queue = TriggerQueue::<i32>::new(128);

        // Push data before blocking pop
        queue.push(42);

        // Should return immediately with the data
        let result = queue.pop_blocking_with_timeout(Duration::from_secs(1));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_pop_blocking_with_timeout_data_arrives() {
        let queue = Arc::new(TriggerQueue::<i32>::new(128));
        let queue_clone = Arc::clone(&queue);

        // Spawn thread that will push data after a delay
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            queue_clone.push(42)
        });

        // This should wait and then receive the data
        let result = queue.pop_blocking_with_timeout(Duration::from_secs(1));
        assert_eq!(result, Ok(42));

        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_push_pop() {
        let queue = Arc::new(TriggerQueue::<i32>::new(128));
        let queue_clone = Arc::clone(&queue);

        const NUM_ITEMS: i32 = 1000;

        // Spawn producer thread
        let producer = thread::spawn(move || {
            for i in 0..NUM_ITEMS {
                queue_clone.push(i);
                // Small delay to test interleaving
                if i % 100 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        // Consumer thread
        let consumer = thread::spawn(move || {
            let mut received = 0;
            let mut sum = 0;

            while received < NUM_ITEMS {
                match queue.pop_blocking_with_timeout(Duration::from_millis(100)) {
                    Ok(val) => {
                        sum += val;
                        received += 1;
                    }
                    Err(CommonErrors::Timeout) => continue,
                    Err(_) => panic!("Unexpected error"),
                }
            }

            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();

        // Expected sum of 0..NUM_ITEMS
        let expected_sum: i32 = (0..NUM_ITEMS).sum();
        assert_eq!(sum, expected_sum);
    }

    #[test]
    fn test_pop_into_vec_concurrent() {
        let queue = Arc::new(TriggerQueue::<i32>::new(128));
        let queue_clone1 = Arc::clone(&queue);
        let queue_clone2 = Arc::clone(&queue);

        const NUM_PRODUCERS: usize = 5;
        const ITEMS_PER_PRODUCER: i32 = 200;

        // Spawn multiple producer threads
        let mut handles = Vec::new(NUM_PRODUCERS);
        for p in 0..NUM_PRODUCERS {
            let q = Arc::clone(&queue_clone1);
            let handle = thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    loop {
                        let success = q.push(p as i32 * ITEMS_PER_PRODUCER + i);
                        if success {
                            break;
                        }

                        thread::sleep(Duration::from_millis(10));
                    }
                }
            });
            handles.push(handle);
        }

        // Consumer thread using pop_into_vec
        let consumer = thread::spawn(move || {
            let expected_total = NUM_PRODUCERS as i32 * ITEMS_PER_PRODUCER;
            let mut all_received = Vec::new(expected_total as usize);

            while all_received.len() < expected_total as usize {
                let mut batch = Vec::new(50);
                queue_clone2.pop_into_vec(&mut batch);

                if !batch.is_empty() {
                    for e in batch.as_slice() {
                        all_received.push(*e);
                    }
                } else {
                    thread::sleep(Duration::from_millis(10));
                }
            }

            all_received
        });

        while !handles.is_empty() {
            handles.pop().unwrap().join().unwrap();
        }

        // Get consumer result
        let received = consumer.join().unwrap();

        // Check that we received exactly the expected number of items
        assert_eq!(received.len(), (NUM_PRODUCERS as i32 * ITEMS_PER_PRODUCER) as usize);

        // Check that all values are present
        let mut received_sorted = received;
        received_sorted.sort();
        let expected: Vec<i32> = collect_from_iterator(
            0..(NUM_PRODUCERS as i32 * ITEMS_PER_PRODUCER),
            (NUM_PRODUCERS as i32 * ITEMS_PER_PRODUCER) as usize,
        );
        assert_eq!(received_sorted, expected);
    }

    #[test]
    fn test_state_transitions() {
        let queue = TriggerQueue::<i32>::new(128);

        // Initially state should be false (not sleeping)
        assert_eq!(queue.state.load(Ordering::SeqCst), false);

        // Set up a thread that will wait
        let queue_arc = Arc::new(queue);
        let queue_clone = Arc::clone(&queue_arc);

        let handle = thread::spawn(move || {
            // Should set state to true before waiting
            let result = queue_clone.pop_blocking_with_timeout(Duration::from_millis(500));

            // After timeout, state should be false again
            assert_eq!(queue_clone.state.load(Ordering::SeqCst), false);

            result
        });

        // Give the thread time to enter waiting state
        thread::sleep(Duration::from_millis(100));

        // State should be true (sleeping)
        assert_eq!(queue_arc.state.load(Ordering::SeqCst), true);

        // Wait for thread to finish
        let _ = handle.join().unwrap();
    }

    #[test]
    fn test_multiple_waiters() {
        let queue = Arc::new(TriggerQueue::<i32>::new(128));
        let queue_clone1 = Arc::clone(&queue);
        let queue_clone2 = Arc::clone(&queue);

        // Create two waiting threads
        let handle1 = thread::spawn(move || queue_clone1.pop_blocking_with_timeout(Duration::from_millis(500)));

        let handle2 = thread::spawn(move || queue_clone2.pop_blocking_with_timeout(Duration::from_millis(500)));

        // Give threads time to start waiting
        thread::sleep(Duration::from_millis(50));

        // Push two items
        queue.push(1);
        queue.push(2);

        // Both threads should get an item
        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();

        // We can't guarantee which thread gets which value due to scheduling,
        // but both should get a value
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // The values should be 1 and 2
        let mut values = vec![result1.unwrap(), result2.unwrap()];
        values.sort();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn test_zero_timeout() {
        let queue = TriggerQueue::<i32>::new(128);

        // Push an item
        queue.push(42);

        // Zero timeout should work if data is available
        let result = queue.pop_blocking_with_timeout(Duration::from_secs(0));
        assert_eq!(result, Ok(42));

        // Zero timeout should fail immediately if no data
        let result = queue.pop_blocking_with_timeout(Duration::from_secs(0));
        assert!(matches!(result, Err(CommonErrors::Timeout)));
    }

    #[test]
    fn test_consumer_drop_allows_new_consumer() {
        let queue = Arc::new(TriggerQueue::<i32>::new(32));

        // Create a scope to contain our first consumer
        {
            let consumer = queue.get_consumer();
            assert!(consumer.is_some());
            // Consumer will be dropped when it goes out of scope
        }

        // We should be able to get a new consumer
        let new_consumer = queue.get_consumer();
        assert!(new_consumer.is_some());
    }

    #[test]
    fn test_thread_safe_consumer_handoff() {
        let queue = Arc::new(TriggerQueue::<i32>::new(32));

        // Thread 1 gets consumer then releases it
        let queue_clone1 = Arc::clone(&queue);
        let handle1 = thread::spawn(move || {
            let consumer = queue_clone1.get_consumer().unwrap();

            // Push some data
            queue_clone1.push(1);
            queue_clone1.push(2);

            // Use the consumer to read data
            assert_eq!(consumer.queue.pop(), Some(1));

            // Now drop the consumer explicitly
            drop(consumer);
        });

        // Wait for thread 1 to complete
        handle1.join().unwrap();

        // Thread 2 should now be able to get the consumer
        let queue_clone2 = Arc::clone(&queue);
        let handle2 = thread::spawn(move || {
            let consumer = queue_clone2.get_consumer();
            assert!(consumer.is_some());
            let consumer = consumer.unwrap();

            // Should be able to read the remaining data
            assert_eq!(consumer.queue.pop(), Some(2));
        });

        handle2.join().unwrap();
    }
}
