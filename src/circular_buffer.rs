//! Fixed size circular buffer

use crate::buffer::RPBuffer;
use std::marker::PhantomData;
use std::option::Option;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const DEFAULT_ORDERING: Ordering = Ordering::SeqCst;

/// A static FIFO circular buffer. Objects in the buffer are owned by the buffer and their memory will only be release when the buffer is dropped
pub struct CircularBuffer<'a, T> {
    items: Vec<AtomicPtr<T>>,
    item_tracker: Vec<*mut T>,
    capacity: usize,
    // the usage of atomic here is just to simplify the code using immutable methods. This will be replaced with another strategy in the future
    length: AtomicUsize,
    start: AtomicUsize,
    end: AtomicUsize,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> CircularBuffer<'a, T> {
    /// Creates a new buffer with fixed size
    pub fn new(capacity: usize) -> Self {
        return CircularBuffer {
            items: Vec::with_capacity(capacity),
            item_tracker: Vec::with_capacity(capacity),
            capacity: capacity,
            length: AtomicUsize::new(0),
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
            phantom: PhantomData,
        };
    }

    #[inline]
    fn move_end(&self) {
        self.end.fetch_add(1, DEFAULT_ORDERING);
        self.length.fetch_add(1, DEFAULT_ORDERING);
        // if we got to the end of the buffer, wrap around
        if self.end.load(DEFAULT_ORDERING) == self.capacity {
            self.end.store(0, DEFAULT_ORDERING);
        }
    }
}

impl<'a, T> RPBuffer<'a, T> for CircularBuffer<'a, T> {
    /// Adds an item to the buffer returning the total number of items in the buffer
    fn add(&mut self, item: T) -> usize {
        if self.length.load(DEFAULT_ORDERING) < self.capacity {
            let raw_item = Box::into_raw(Box::new(item));
            self.items.push(AtomicPtr::new(raw_item));
            self.item_tracker.push(raw_item);
            self.move_end();
        }
        return self.available();
    }

    /// Returns the maximum allowed number of items in the buffer
    #[inline]
    fn capacity(&self) -> usize {
        return self.capacity;
    }

    /// Returns the number of available items in the buffer
    #[inline]
    fn available(&self) -> usize {
        return self.length.load(DEFAULT_ORDERING);
    }

    /// Returns back an existing item to the buffer, returning the number of items available or None if the buffer is full
    #[inline]
    fn offer(&self, item: &'a T) -> usize {
        // this is to avoid returning more items to the buffer than originally intended
        if self.length.load(DEFAULT_ORDERING) == self.capacity {
            return 0;
        }

        let p: *const T = item as *const T;

        let atomic_item = &self.items[self.end.load(DEFAULT_ORDERING)];
        atomic_item.store(p as *mut T, DEFAULT_ORDERING);

        self.move_end();
        return self.available();
    }

    /// Removes one item from the buffer returning None if the buffer is empty
    #[inline]
    fn take(&self) -> Option<&'a mut T> {
        if self.length.load(DEFAULT_ORDERING) < 1 {
            return None;
        }

        let atomic_item = &self.items[self.start.load(DEFAULT_ORDERING)];
        let item = atomic_item.load(DEFAULT_ORDERING);

        self.start.fetch_add(1, DEFAULT_ORDERING);
        self.length.fetch_sub(1, DEFAULT_ORDERING);

        if self.start.load(DEFAULT_ORDERING) == self.capacity {
            self.start.store(0, DEFAULT_ORDERING);
        }

        unsafe {
            return Some(&mut (*item));
        }
    }
}

impl<'a, T> Drop for CircularBuffer<'a, T> {
    fn drop(&mut self) {
        for i in 0..self.item_tracker.len() {
            let ptr = self.item_tracker[i];
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cb_add_take() {
        let mut cb = CircularBuffer::<u32>::new(2);

        cb.add(1);
        cb.add(2);

        let v1 = cb.take();
        assert_eq!(1, *v1.unwrap());
        assert_eq!(1, cb.available());

        let v2 = cb.take();
        assert_eq!(2, *v2.unwrap());
        assert_eq!(0, cb.available());

        assert_eq!(None, cb.take());
    }

    #[test]
    fn test_cb_add_full() {
        let length = 3;
        let mut cb = CircularBuffer::<u32>::new(length);

        for count in 0..length {
            let l = count + 1;
            assert_eq!(l, cb.add(1));
        }

        assert_eq!(length, cb.add(1));
        assert_eq!(0, cb.end.load(DEFAULT_ORDERING));
        assert_eq!(length, cb.available());
    }

    #[test]
    fn test_cb_take_offer() {
        let mut cb = CircularBuffer::<u32>::new(2);

        // first populate the buffer
        cb.add(1);
        cb.add(2);

        // then consume all items, saving the last one
        cb.take();
        let item = cb.take().unwrap();

        // finally add one back and then take it again
        assert_eq!(1, cb.offer(item));
        assert_eq!(2, *cb.take().unwrap());
    }

    #[test]
    fn test_cb_offer_full() {
        let mut cb = CircularBuffer::<u32>::new(1);
        cb.add(1);
        let item = cb.take().unwrap();
        cb.offer(item);
        assert_eq!(0, cb.offer(item));
    }
}
