//! Traits for buffers

pub trait RPBuffer<'a, T> {
    /// Adds an item to the buffer returning the total number of items in the buffer
    fn add(&mut self, item: T) -> usize;

    /// Returns the maximum allowed number of items in the buffer
    fn capacity(&self) -> usize;

    /// Returns the number of available items in the buffer
    fn available(&self) -> usize;

    /// Puts back an existing item to the buffer, returning the number of items available
    fn offer(&self, item: &'a T) -> usize;

    /// Removes one item from the buffer returning None if the buffer is empty
    fn take(&self) -> Option<&'a mut T>;
}
