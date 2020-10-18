//! Fixed size lock free circular buffer

use crate::buffer::RPBuffer;
use std::marker::PhantomData;
use std::option::Option;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const DEFAULT_ORDERING: Ordering = Ordering::SeqCst;

/// A fixed size thread safe and lock-free circular buffer. Objects in the buffer are owned by the buffer and their memory will only be release when
/// the buffer is dropped
pub struct AtomicCircularBuffer<'a, T> {
    items: Vec<AtomicPtr<T>>,
    item_tracker: Vec<*mut T>,
    capacity: usize,
    length: AtomicUsize,
    start: AtomicUsize,
    end: AtomicUsize,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> AtomicCircularBuffer<'a, T> {
    /// Creates a new buffer with fixed size
    pub fn new(capacity: usize) -> Self {
        return AtomicCircularBuffer {
            items: Vec::with_capacity(capacity),
            item_tracker: Vec::with_capacity(capacity),
            capacity: capacity,
            length: AtomicUsize::new(0),
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
            phantom: PhantomData,
        };
    }
}

unsafe impl<'a, T> Sync for AtomicCircularBuffer<'a, T> {}
unsafe impl<'a, T> Send for AtomicCircularBuffer<'a, T> {}

impl<'a, T> Drop for AtomicCircularBuffer<'a, T> {
    fn drop(&mut self) {
        for i in 0..self.item_tracker.len() {
            let ptr = self.item_tracker[i];
            unsafe {
                Box::from_raw(ptr as *mut T);
            }
        }
    }
}

impl<'a, T> RPBuffer<'a, T> for AtomicCircularBuffer<'a, T> {
    /// Adds an item to the buffer returning the total number of items in the buffer or None if full
    /// This method is not thread safe and should only be used for the initial population of the buffer
    fn add(&mut self, item: T) -> usize {
        let cur_length = self.length.load(DEFAULT_ORDERING);
        if cur_length < self.capacity {
            let raw_item = Box::into_raw(Box::new(item));
            self.items.push(AtomicPtr::new(raw_item));
            self.item_tracker.push(raw_item);

            self.length.fetch_add(1, DEFAULT_ORDERING);
            let last_value = self.end.fetch_add(1, DEFAULT_ORDERING);
            // if we got to the end of the buffer, wrap around
            if last_value == self.capacity - 1 {
                self.end.store(0, DEFAULT_ORDERING);
            }
        }
        return self.available();
    }

    /// Returns the number of items available in the buffer
    fn available(&self) -> usize {
        return self.length.load(DEFAULT_ORDERING);
    }

    /// Removes one item from the buffer returning None if the buffer is empty
    /// This function can perform busy wait while waiting for an object to be available or the buffer to be empty
    #[inline]
    fn take(&self) -> Option<&'a mut T> {
        let mut current_length = self.length.load(DEFAULT_ORDERING);
        while current_length > 0 {
            let result = self.length.compare_exchange(current_length, current_length - 1, DEFAULT_ORDERING, DEFAULT_ORDERING);

            // if we managed to increment length, we secured one of the items
            if result.is_ok() {
                let mut current_start = self.start.load(DEFAULT_ORDERING);

                loop {
                    let result: Result<usize, usize>;

                    // if we reached the end of the buffer, we have to wrap around instead of incrementing
                    if current_start == self.capacity - 1 {
                        result = self.start.compare_exchange(current_start, 0, DEFAULT_ORDERING, DEFAULT_ORDERING);
                    } else {
                        result = self.start.compare_exchange(current_start, current_start + 1, DEFAULT_ORDERING, DEFAULT_ORDERING);
                    }

                    current_start = match result {
                        Ok(x) => x,
                        Err(x) => x,
                    };

                    if result.is_ok() {
                        // if we managed to increment start, continue getting an item from the buffer
                        let atomic_item = &self.items[current_start];
                        let mut current_item = atomic_item.load(DEFAULT_ORDERING);

                        // but we need to take it and replace it with null imediately
                        loop {
                            let result = atomic_item.compare_exchange(current_item, std::ptr::null_mut(), DEFAULT_ORDERING, DEFAULT_ORDERING);
                            let last_item = match result {
                                Ok(x) => x,
                                Err(x) => x,
                            };

                            if result.is_ok() && last_item != std::ptr::null_mut() {
                                unsafe {
                                    return Some(&mut (*last_item));
                                }
                            }

                            current_item = last_item;
                        }
                    }
                }
            }

            current_length = match result {
                Ok(x) => x,
                Err(x) => x,
            };
        }
        return None;
    }

    #[inline]
    /// Puts back an existing item to the buffer, returning the number of items available or None if the buffer is full
    fn offer(&self, item: &'a T) -> usize {
        // this is to avoid returning more items to the buffer than originally intended
        let mut current_length = self.length.load(DEFAULT_ORDERING);
        if current_length == self.capacity {
            return 0;
        }

        let item_ptr: *const T = item as *const T;
        let mut current_end = self.end.load(DEFAULT_ORDERING);
        loop {
            let result: Result<usize, usize>;

            // if we are close to the end, don't try to increment and instead, set end to zero
            if current_end == self.capacity - 1 {
                result = self.end.compare_exchange(current_end, 0, DEFAULT_ORDERING, DEFAULT_ORDERING);
            } else {
                result = self.end.compare_exchange(current_end, current_end + 1, DEFAULT_ORDERING, DEFAULT_ORDERING);
            }

            current_end = match result {
                Ok(x) => x,
                Err(x) => x,
            };

            // if we managed to increment the end counter, we can return the item to the buffer
            if result.is_ok() {
                let atomic_item = &self.items[current_end];
                let item_val = item_ptr as *mut T;

                // replace only if the current value is nil
                loop {
                    let result = atomic_item.compare_exchange(std::ptr::null_mut(), item_val, DEFAULT_ORDERING, DEFAULT_ORDERING);

                    if result.is_ok() {
                        break;
                    }
                }

                current_length = self.length.load(DEFAULT_ORDERING);

                while current_length < self.capacity {
                    let result = self.length.compare_exchange(current_length, current_length + 1, DEFAULT_ORDERING, DEFAULT_ORDERING);
                    if result.is_ok() {
                        return self.available();
                    }

                    current_length = match result {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                }

                panic!("More items have been returned to the buffer than its capacity. This should not happen");
            }
        }
    }

    /// Returns the maximum allowed number of items in the buffer
    #[inline]
    fn capacity(&self) -> usize {
        return self.capacity;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_acb_add() {
        let obj1 = String::from("object 1");
        let obj2 = String::from("object 2");
        let mut buf = AtomicCircularBuffer::<String>::new(2);

        assert_eq!(1, buf.add(obj1));
        assert_eq!(2, buf.add(obj2));

        assert_eq!(2, buf.available());
    }

    #[test]
    fn test_acb_take_serial() {
        let mut buf = AtomicCircularBuffer::<String>::new(2);
        buf.add(String::from("object 1"));
        buf.add(String::from("object 2"));

        let obj1 = buf.take();
        let obj2 = buf.take();

        assert_eq!("object 1", obj1.unwrap());
        assert_eq!("object 2", obj2.unwrap());
        assert_eq!(None, buf.take());
    }

    #[test]
    fn test_acb_take_offer() {
        let mut buf = AtomicCircularBuffer::<String>::new(2);
        buf.add(String::from("o1"));
        buf.add(String::from("o2"));

        let mut obj = buf.take().unwrap();
        buf.offer(obj);

        obj = buf.take().unwrap();
        assert_eq!("o2", obj);
        buf.offer(obj);

        obj = buf.take().unwrap();
        assert_eq!("o1", obj);

        assert_eq!(2, buf.offer(obj));
        buf.take();
        obj = buf.take().unwrap();
        assert_eq!(1, buf.offer(obj));
    }

    #[test]
    fn test_acb_offer_overflow() {
        let mut buf = AtomicCircularBuffer::<String>::new(2);
        buf.add(String::from("o1"));
        buf.add(String::from("o2"));

        let obj = buf.take().unwrap();
        assert_eq!(2, buf.offer(obj));
        assert_eq!(0, buf.offer(obj));
    }

    #[test]
    fn test_acb_take_all_offer_all() {
        let mut buf = AtomicCircularBuffer::<String>::new(3);
        buf.add(String::from("o1"));
        buf.add(String::from("o2"));
        buf.add(String::from("o3"));

        buf.take();
        let mut obj = buf.take().unwrap();
        assert_eq!("o2", obj);
        obj = buf.take().unwrap();
        assert_eq!("o3", obj);

        // return the last object only
        buf.offer(obj);
        obj = buf.take().unwrap();
        assert_eq!("o3", obj);

        assert_eq!(None, buf.take());
    }

    #[test]
    fn test_acb_mt_take_offer() {
        let thread_count = 6;
        let size = thread_count - 1;
        let iterations = 1000;
        let mut buf = AtomicCircularBuffer::<String>::new(size);
        let mut buffer_items = HashMap::new();

        for c in 0..size {
            let item = format!("object {}", c);
            buf.add(item.clone());
            buffer_items.insert(item, true);
        }

        let mut jhv = Vec::new();
        let arc_buffer = Arc::new(buf);
        for _ in 0..thread_count {
            let c_buf = arc_buffer.clone();
            let jh = thread::spawn(move || {
                let mut taken = 0;
                for _ in 0..iterations {
                    let obj = c_buf.take();
                    if obj != None {
                        c_buf.offer(obj.unwrap());
                        taken = taken + 1;
                    }
                }

                return taken;
            });

            jhv.push(jh);
        }

        jhv.into_iter().for_each(|jh| {
            let taken = jh.join();

            assert!(taken.unwrap() > size);
        });

        // now we check that all objects in the buffer have been returned correctly
        assert_eq!(size, arc_buffer.available());

        // now check all items in the buffer are as expected and there are no duplicates
        let mut item = arc_buffer.take();
        while item != None {
            let object = item.unwrap();
            buffer_items.remove(object);
            item = arc_buffer.take();
        }

        assert_eq!(0, buffer_items.len());
    }

    struct TestOwn {
        value: String,
    }

    impl Drop for TestOwn {
        fn drop(&mut self) {
            println!("dropped");
        }
    }

    fn print_test(t: &mut TestOwn) {
        println!("printing - {}", t.value);
    }

    #[test]
    fn test_own_test() {
        {
            let optr = Box::into_raw(Box::new(TestOwn { value: String::from("test name") }));

            unsafe {
                let mut_v = &mut (*optr);
                print_test(mut_v);
                let mut_v2 = &mut (*optr);
                print_test(mut_v2);
            }
        }
        assert_eq!(1, 1);
    }
}
