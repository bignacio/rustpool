//! Traits for queues

use std::time::Duration;

/// RPQueue trait for pool queues
pub trait RPQueue<'a, T> {
    /// Returns the size of the queue in total number objects originally allocated
    fn capacity(&self) -> usize;

    /// Returns the number of available items in the buffer
    fn available(&self) -> usize;

    /// Returns an object from the queue or None if the pool is empty available.
    /// This method is preferred over `take_wait` when empty or near empty pools are common.
    fn take(&self) -> Option<&'a mut T>;

    /// Returns an object from the queue or block until one is available
    fn take_wait(&self, timeout: Duration) -> Option<&'a mut T>;

    /// Returns an object to the pool.
    fn offer(&self, item: &'a T) -> usize;
}
