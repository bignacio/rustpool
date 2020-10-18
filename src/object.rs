//! A general memory friendly object pool
use crate::atomic_queue::AtomicQueue;
use crate::blocking_queue::BlockingQueue;
use std::ops::FnMut;
use std::sync::Arc;

/// The Builder for different types of object pools
pub struct ObjectPoolBuilder {
    size: usize,
}

/// The builder for object pools backed by an atomic queue
pub struct AtomicObjectPoolBuilder {
    size: usize,
}

/// The builder for object pools backed by a blocking queue
pub struct BlockingObjectPoolBuilder {
    size: usize,
}

impl ObjectPoolBuilder {
    /// Create a new object pool builder
    pub fn new() -> Self {
        return ObjectPoolBuilder { size: 0 };
    }

    /// Set the target pool size
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        return self;
    }

    /// Tells the builder we want an atomic pool
    pub fn atomic(&self) -> AtomicObjectPoolBuilder {
        return AtomicObjectPoolBuilder { size: self.size };
    }

    /// tells the builder we want a blocking pool
    pub fn blocking(&self) -> BlockingObjectPoolBuilder {
        return BlockingObjectPoolBuilder { size: self.size };
    }
}

impl AtomicObjectPoolBuilder {
    /// Build an atomic object pool
    pub fn build<T>(&self, f: impl FnMut() -> T) -> AtomicQueue<'static, T> {
        return AtomicQueue::new(self.size, f);
    }

    /// Build an atomic object pool wrapped in an Arc
    pub fn build_arc<T>(&self, f: impl FnMut() -> T) -> Arc<AtomicQueue<'static, T>> {
        return AtomicQueue::new_arc(self.size, f);
    }
}

impl BlockingObjectPoolBuilder {
    /// Buil a blocking object pool
    pub fn build<T>(&self, f: impl FnMut() -> T) -> BlockingQueue<'static, T> {
        return BlockingQueue::new(self.size, f);
    }

    /// Buil a blocking object pool wrapped in an Arc
    pub fn build_arc<T>(&self, f: impl FnMut() -> T) -> Arc<BlockingQueue<'static, T>> {
        return BlockingQueue::new_arc(self.size, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::RPQueue;
    use std::ops::Deref;

    struct TestObject {}
    impl Default for TestObject {
        fn default() -> Self {
            return TestObject {};
        }
    }

    #[test]
    fn obj_builder_create_blocking() {
        let size = 2;

        let pool = ObjectPoolBuilder::new().size(size).blocking().build(|| TestObject::default());

        assert_eq!(size, pool.available());
    }

    #[test]
    fn obj_builder_create_blocking_arc() {
        let size = 3;

        let pool = ObjectPoolBuilder::new().size(size).blocking().build_arc(|| TestObject::default());

        assert_eq!(size, pool.deref().available());
    }

    #[test]
    fn obj_builder_create_atomic() {
        let size = 4;

        let pool = ObjectPoolBuilder::new().size(size).atomic().build(|| TestObject::default());

        assert_eq!(size, pool.available());
    }

    #[test]
    fn obj_builder_create_atomic_arc() {
        let size = 5;

        let pool = ObjectPoolBuilder::new().size(size).atomic().build_arc(|| TestObject::default());

        assert_eq!(size, pool.deref().available());
    }
}
