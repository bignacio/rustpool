//! Concurrent blocking object queue
use crate::buffer::RPBuffer;
use crate::circular_buffer::CircularBuffer;
use crate::queue::RPQueue;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// A thread safe blocking queue. Objects are added to the queue via a closure and are owned by the queue as well
/// This means objects in the queue will only be dropped when the queue's backing buffer is also destroyed.
pub struct BlockingQueue<'a, T> {
    buffer: *mut CircularBuffer<'a, T>,
    buffer_mtx: Mutex<*mut CircularBuffer<'a, T>>,
    buffer_cvar: Condvar,
}

///```

/// use rustpool::BlockingQueue;
/// use rustpool::RPQueue;
///
/// fn main() {
///   let new_value: isize = 1;
///
///    // create a blocking queue with 3 elements set to value 0
///    let q = BlockingQueue::<isize>::new(3, || 0);
///
///    println!("simple blocking queue size {}", q.capacity());
///
///    // take the first element
///    let v1 = q.take().unwrap();
///
///    println!("first element is {}", v1);
///
///    // remove the remaining items
///    let v2 = q.take().unwrap();
///    let v3 = q.take().unwrap();
///
///    // how many items available now?
///    println!("items in the queue {}", q.available());
///
///    // no more items to return, let's wait at most one second
///    let empty = q.take_wait(std::time::Duration::from_secs(1));
///
///    assert_eq!(None, empty);
///
///    // put everything back
///    q.offer(v1);
///    q.offer(v2);
///    q.offer(v3);
///
///    // how many items available now?
///    println!("items in the queue {}", q.available());
///
///    // we can't add more items than the original capacity
///    q.offer(&new_value);
///
///    // we still have the same number of available items
///    println!("items in the queue {}", q.available());
///}
/// ```

impl<'a, T> BlockingQueue<'a, T> {
    /// Create a new queue with fixed size `size` using custom create function
    pub fn new(size: usize, f: impl FnMut() -> T) -> Self {
        let buf = BlockingQueue::<T>::allocate(size, f);
        let buf_ptr = Box::into_raw(Box::new(buf));
        return BlockingQueue {
            buffer: buf_ptr,
            buffer_mtx: Mutex::new(buf_ptr),
            buffer_cvar: Condvar::new(),
        };
    }

    /// Create a new queue with fixed `size` wrapped as an atomic reference counter using a custom create function
    pub fn new_arc(size: usize, f: impl FnMut() -> T) -> Arc<Self> {
        let q = BlockingQueue::new(size, f);
        return Arc::new(q);
    }

    fn allocate(size: usize, mut f: impl FnMut() -> T) -> CircularBuffer<'a, T> {
        let mut buf: CircularBuffer<T> = CircularBuffer::new(size);
        for _ in 0..size {
            buf.add(f());
        }
        return buf;
    }
}

impl<'a, T> RPQueue<'a, T> for BlockingQueue<'a, T> {
    #[inline]
    /// Returns the size of the queue in total number objects originally allocated
    fn capacity(&self) -> usize {
        unsafe {
            return (*self.buffer).capacity();
        }
    }

    #[inline]
    /// Returns the number of available items in the buffer
    fn available(&self) -> usize {
        unsafe {
            return (*self.buffer).available();
        }
    }

    /// Returns an object from the queue or None if the pool is empty. This method allows a race when checking if
    /// the pool is empty in order to quickly exit without causing lock contention.
    /// This method is preferred over [`BlockingQueue::take_wait`] when empty or near empty pools are common.
    #[inline]
    fn take(&self) -> Option<&'a mut T> {
        if self.available() <= 0 {
            return None;
        }

        let guard = self.buffer_mtx.lock().unwrap();
        unsafe {
            return (**guard).take();
        }
    }

    /// Returns an object from the queue or block until one is available
    #[inline]
    fn take_wait(&self, timeout: Duration) -> Option<&'a mut T> {
        let mut guard = self.buffer_mtx.lock().unwrap();

        unsafe {
            let wait_result = self.buffer_cvar.wait_timeout_while(guard, timeout, |current| (**current).available() == 0).unwrap();
            if wait_result.1.timed_out() {
                return None;
            }
            guard = wait_result.0;
        };

        unsafe {
            return (**guard).take();
        }
    }

    /// Returns an object to the pool
    #[inline]
    fn offer(&self, item: &'a T) -> usize {
        let guard = self.buffer_mtx.lock().unwrap();

        unsafe {
            return (**guard).offer(item);
        }
    }
}

impl<'a, T> Drop for BlockingQueue<'a, T> {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.buffer);
        }
    }
}

unsafe impl<'a, T> Sync for BlockingQueue<'a, T> {}
unsafe impl<'a, T> Send for BlockingQueue<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Debug, PartialEq)]
    struct TestObject {
        name: String,
    }

    impl TestObject {
        fn new(name: String) -> Self {
            return TestObject { name: name };
        }
    }

    impl Default for TestObject {
        fn default() -> Self {
            return TestObject::new(String::from("default"));
        }
    }

    fn create_test_queue<'a>(size: usize) -> Arc<BlockingQueue<'a, TestObject>> {
        let mut counter = 0;

        let obj_maker = || {
            counter = counter + 1;
            let name = format!("object {}", counter);
            return TestObject::new(name);
        };

        return BlockingQueue::new_arc(size, obj_maker);
    }

    #[test]
    fn test_bq_take() {
        let size = 3;
        let q = create_test_queue(size);
        let obj1 = q.take();
        let obj2 = q.take();
        let obj3 = q.take();

        assert_eq!("object 1", obj1.unwrap().name);
        assert_eq!("object 2", obj2.unwrap().name);
        assert_eq!("object 3", obj3.unwrap().name);

        assert_eq!(size, q.capacity());
    }

    #[test]
    fn test_bq_take_and_offer() {
        let size = 2;
        let q = create_test_queue(size);

        // ignore the first, object 1 and never put it back
        q.take();

        // take object 2
        let obj2 = q.take().unwrap();
        assert_eq!("object 2", obj2.name);
        // and put it back so we can take it again
        q.offer(obj2);

        let obj2 = q.take().unwrap();
        assert_eq!("object 2", obj2.name);
    }

    #[test]
    fn test_bq_take_threads() {
        let size = 2;
        let q = create_test_queue(size);

        let tq = q.clone();
        let t0 = thread::spawn(move || {
            return tq.take().unwrap().name.as_str();
        });

        let tq = q.clone();
        let t1 = thread::spawn(move || {
            return tq.take().unwrap().name.as_str();
        });

        let name0 = t0.join().unwrap();
        let name1 = t1.join().unwrap();

        let expected = vec!["object 1", "object 2"];
        let mut actual = vec![name0, name1];
        actual.sort();

        assert_eq!(expected, actual);
        assert_eq!(size, q.capacity());
    }

    #[test]
    fn test_bq_over_limit() {
        let size = 2;

        let q = BlockingQueue::<TestObject>::new(size, || TestObject::default());

        for _ in 0..size {
            q.take();
        }

        let obj = q.take();

        assert_eq!(None, obj);
    }

    #[test]
    fn test_bq_over_limit_block() {
        let size = 2;
        let wait_ms = Duration::from_millis(10);

        let q = BlockingQueue::<TestObject>::new(size, || TestObject::default());

        for _ in 0..size {
            q.take();
        }

        let now = Instant::now();

        // it will block here
        let obj = q.take_wait(wait_ms);
        let elapsed = now.elapsed();
        assert_eq!(None, obj);
        assert!(elapsed >= wait_ms, "didn't block for sufficient time. Expected {} actual {}", wait_ms.as_millis(), elapsed.as_millis());
    }

    #[test]
    fn test_bq_mt_take_offer() {
        let thread_count = 7;
        let size = thread_count;
        let iterations = 1000;
        let mut queue_items = HashMap::new();
        let mut counter = 0;

        let item_maker = || {
            counter = counter + 1;
            let item = format!("object {}", counter);
            queue_items.insert(item.clone(), true);
            return item;
        };

        let arc_queue = BlockingQueue::new_arc(size, item_maker);

        let mut jhv = Vec::new();
        for _ in 0..thread_count {
            let c_buf = arc_queue.clone();
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
        assert_eq!(size, arc_queue.available());

        // now check all items in the buffer are as expected and there are no duplicates
        let mut item = arc_queue.take();
        while item != None {
            let object = item.unwrap();
            queue_items.remove(object);
            item = arc_queue.take();
        }

        assert_eq!(0, queue_items.len());
    }
}
