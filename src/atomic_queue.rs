//! Concurrent atomic object queue

use crate::atomic_circular_buffer::AtomicCircularBuffer;
use crate::buffer::RPBuffer;
use crate::queue::RPQueue;
use std::sync::Arc;

use std::time::Duration;

/// A thread safe, lock-free atomic queue. Objects are added to the queue via a closure and are owned by the queue as well
/// This means objects in the queue will only be dropped when the queue's backing buffer is also destroyed.
pub struct AtomicQueue<'a, T> {
    buffer: AtomicCircularBuffer<'a, T>,
}

/// ```
///
/// use rustpool::AtomicQueue;
/// use rustpool::RPQueue;

/// fn main() {
///     // create a blocking queue with 3 elements set to value 0
///     let q = AtomicQueue::<isize>::new(3, || 0);
///
///     println!("simple blocking queue size {}", q.capacity());
///
///     // take the first element
///     let v1 = q.take().unwrap();
///
///     println!("first element is {}", v1);
///
///     // remove the remaining items
///     let v2 = q.take().unwrap();
///     let v3 = q.take().unwrap();
///
///     // how many items available now?
///     println!("items in the queue {}", q.available());
///
///     // no more items to return but the atomic queue will never block
///     let empty = q.take_wait(std::time::Duration::from_secs(1));
///
///     assert_eq!(None, empty);
///
///     // put everything back
///     q.offer(v1);
///     q.offer(v2);
///     q.offer(v3);
///
///     // how many items available now?
///     println!("items in the queue {}", q.available());
///
///     let new_value: isize = 1;
///
///     // we can't add more items than the original capacity
///     q.offer(&new_value);
///
///     // we still have the same number of available items
///     println!("items in the queue {}", q.available());
/// }
/// ```

impl<'a, T> AtomicQueue<'a, T> {
    /// Create a new queue with fixed size `size` using custom create function
    pub fn new(size: usize, f: impl FnMut() -> T) -> Self {
        let buf = AtomicQueue::<T>::allocate(size, f);

        return AtomicQueue { buffer: buf };
    }

    /// Create a new queue with fixed `size` wrapped as an atomic reference counter using a custom create function
    pub fn new_arc(size: usize, f: impl FnMut() -> T) -> Arc<Self> {
        let q = AtomicQueue::new(size, f);
        return Arc::new(q);
    }

    fn allocate(size: usize, mut f: impl FnMut() -> T) -> AtomicCircularBuffer<'a, T> {
        let mut buf: AtomicCircularBuffer<T> = AtomicCircularBuffer::new(size);
        for _ in 0..size {
            buf.add(f());
        }
        return buf;
    }
}

impl<'a, T> RPQueue<'a, T> for AtomicQueue<'a, T> {
    #[inline]
    /// Returns the size of the queue in total number objects originally allocated
    fn capacity(&self) -> usize {
        return self.buffer.capacity();
    }

    #[inline]
    /// Returns the number of available items in the buffer
    fn available(&self) -> usize {
        return self.buffer.available();
    }

    /// Returns an object from the queue or None if the pool is empty
    #[inline]
    fn take(&self) -> Option<&'a mut T> {
        return self.buffer.take();
    }

    /// This method behaves the same as [`AtomicQueue::take`] as the atomic implementation will never block if the pool is empty
    #[inline]
    fn take_wait(&self, _timeout: Duration) -> Option<&'a mut T> {
        return self.take();
    }

    /// Returns an object to the pool
    #[inline]
    fn offer(&self, item: &'a T) -> usize {
        return self.buffer.offer(item);
    }
}

unsafe impl<'a, T> Sync for AtomicQueue<'a, T> {}
unsafe impl<'a, T> Send for AtomicQueue<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_aq_take_offer() {
        let mut count = 0;
        let multiplier = 3;
        let mut queue_items = HashMap::new();

        let create_fn = || {
            count = count + 1;
            queue_items.insert(count, 0);
            return count;
        };

        let queue = AtomicQueue::new_arc(11, create_fn);

        for _ in 0..count * multiplier {
            let item = queue.take().unwrap();

            let current = queue_items[item];
            queue_items.insert(*item, current + 1);
            queue.offer(item);
        }

        for _ in 0..count {
            let item = queue.take().unwrap();

            let current = queue_items.remove(item);

            assert_eq!(multiplier, current.unwrap());
        }

        assert_eq!(0, queue_items.len());
    }
}
