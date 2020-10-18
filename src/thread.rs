//! A thread pool and affinity library
extern crate libc;
use libc::{cpu_set_t, sched_getaffinity, sched_setaffinity, syscall, SYS_getcpu, CPU_ISSET, CPU_SET, CPU_SETSIZE};

use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A CPU Node contains the cpu id and its associated NUMA node
#[derive(Copy, Clone)]
pub struct CPUNode {
    id: usize,
    _numa_node: usize,
    empty: bool,
    all_cpus: bool,
}

/// Simple trait to help with the fact FnOnce can't be implemented
trait Runnable {
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Runnable for F {
    fn run(self: Box<F>) {
        (*self)()
    }
}

/// Type for runnables
type ThreadTask<'a> = Box<dyn Runnable + Send + 'a>;

/// Shared state
struct ThreadState {
    running: AtomicBool,
    task_receiver: Mutex<Receiver<ThreadTask<'static>>>,
}

/// A NUMA aware thread pool
pub struct ThreadPool {
    count: usize,
    join_handles: Vec<JoinHandle<()>>,
    task_sender: Sender<ThreadTask<'static>>,
    state: Arc<ThreadState>,
}

/// ```
///use rustpool::{ThreadPool, ThreadPoolBuilder};

///fn main() {
///    // change USE_ALL_CPUS to target multiple or a single CPU
///    const USE_ALL_CPUS: bool = true;
///
///    let cpu = ThreadPool::get_cpus()[0];
///
///    let builder: ThreadPoolBuilder;
///    if USE_ALL_CPUS {
///        builder = ThreadPoolBuilder::new().for_all_cpus().count(2);
///    } else {
///        builder = ThreadPoolBuilder::new().for_cpu(cpu).count(2);
///    }
///    let res = builder.build();
///
///    if res.is_ok() {
///        println!("Thread pool created");
///        let tp = res.unwrap();
///
///        tp.schedule_task(|| print_task_info(1));
///        tp.schedule_task(|| print_task_info(2));
///
///        // on drop, the pool will wait for all running tasks to end
///        return;
///    }
///
///    // panic if we can't create the thread pool
///    std::panic::panic_any(res.err());
///}
///
///fn print_task_info(task_id: u16) {
///    println!("Task {} executed in thread - {:?}", task_id, std::thread::current().id());
///    let thread_cpus = ThreadPool::get_cpus();
///
///    println!("CPU count available to task {}: {}", task_id, thread_cpus.len());
///    std::thread::sleep(std::time::Duration::from_millis(1000));
///}

/// ```

/// Builder for thread pools
pub struct ThreadPoolBuilder {
    count: usize,
    target_cpu: CPUNode,
    all_cpus: bool,
}

impl ThreadPoolBuilder {
    /// Creates a new thread pool builder
    pub fn new() -> Self {
        return ThreadPoolBuilder {
            count: 0,
            target_cpu: CPUNode::empty(),
            all_cpus: false,
        };
    }

    /// Sets the number of threads to be created in the pool
    pub fn count(mut self, count: usize) -> Self {
        self.count = count;
        return self;
    }

    /// Creates a pool with threads on all available CPUs. This option cannot be used with `for_cpu`
    pub fn for_all_cpus(mut self) -> Self {
        self.all_cpus = true;
        return self;
    }

    /// Tells the builder to create a thread pool where all threads have affinity with a single CPU core.
    /// This option cannot be used with `for_all_cpus`
    pub fn for_cpu(mut self, cpu: CPUNode) -> Self {
        self.target_cpu = cpu;
        return self;
    }

    /// Creates a thread pool using the given builder parameters
    pub fn build(&self) -> Result<ThreadPool, String> {
        // if the parameters are conflicting, return an error
        if self.all_cpus && !self.target_cpu.is_empty() {
            return Err("Cannot use all cpus and target cpu at the same time".to_string());
        }

        let all_cores_cpu = CPUNode::new_all_cpus();

        let mut tp = ThreadPool::new();
        for _c in 0..self.count {
            let mut target_cpu = self.target_cpu;
            if self.all_cpus {
                target_cpu = all_cores_cpu;
            }
            let res = tp.add_thread(target_cpu);

            if res.is_err() {
                return Err(res.unwrap_err());
            }
        }
        return Ok(tp);
    }
}

impl ThreadPool {
    /// Create a new thread pool with count threads
    fn new() -> Self {
        let (sender, receiver) = channel::<ThreadTask<'static>>();
        let state = ThreadState {
            running: AtomicBool::new(true),
            task_receiver: Mutex::new(receiver),
        };

        return ThreadPool {
            count: 0,
            join_handles: Vec::new(),
            task_sender: sender,
            state: Arc::new(state),
        };
    }

    /// Add a new thread to the pool for a specific CPU
    fn add_thread(&mut self, cpu: CPUNode) -> Result<(), String> {
        if cpu.is_empty() {
            return Err("Cannot add thread to empty CPU".to_string());
        }

        let state_clone = self.state.clone();
        let join_handle = std::thread::spawn(move || ThreadPool::process_tasks(state_clone, cpu));

        self.join_handles.push(join_handle);
        self.count = self.count + 1;

        self.check_can_run_task();

        return Ok(());
    }

    /// Verify that at least one of the threads added can run tasks
    fn check_can_run_task(&mut self) {
        let (sender, receiver) = sync_channel(1);

        self.schedule_task(move || {
            let _ignored = sender.send(1u8).expect("Failure verifying if the task can be executed");
        });

        receiver.recv_timeout(std::time::Duration::from_secs(1)).expect("Timeout while verifying if tasks can be executed");
    }

    /// Processes all queued up tasks
    fn process_tasks(state: Arc<ThreadState>, cpu: CPUNode) {
        // do not set affinity if the target CPU is all
        if !cpu.all_cpus {
            unsafe {
                let cpu_id = cpu.id;
                let mut cpu_set: cpu_set_t = mem::zeroed::<cpu_set_t>();
                let set_size = mem::size_of::<cpu_set_t>();

                CPU_SET(cpu_id, &mut cpu_set);
                if sched_setaffinity(0, set_size, &cpu_set) != 0 {
                    return;
                }
            }
        }

        let mut running = state.running.load(Ordering::SeqCst);
        while running {
            let may_have_task = {
                let task_receiver = state.task_receiver.lock().expect("Failed to read from task list");
                task_receiver.recv()
            };

            let task = match may_have_task {
                Ok(task) => task,
                Err(_) => return, // the channel is closed and we can exit all threads
            };

            task.run();
            running = state.running.load(Ordering::SeqCst);
        }
    }

    /// Wait for all threads to finish and remove them from the pool
    pub fn shutdown(&mut self) {
        self.state.running.store(false, Ordering::SeqCst);

        // schedule one no-op task per thread just we can break out of the execution loop. this needs to be better
        for _c in 0..self.count {
            self.schedule_task(|| {});
        }

        while self.join_handles.len() > 0 {
            let handle = self.join_handles.pop().unwrap();
            let _ignored = handle.join();
        }
    }

    /// Run a function on an available thread
    pub fn schedule_task<F>(&self, task: F)
    where
        F: FnOnce() -> (),
        F: Send + Sync + 'static,
    {
        self.task_sender.send(Box::new(task)).expect("Failed to schedule task for execution");
    }

    /// Returns all CPUs and their associated NUMA nodes
    pub fn get_cpus() -> Vec<CPUNode> {
        let cpu_ids = ThreadPool::get_cpu_ids();
        // do it in a different thread since we need to change affinit all the time
        // and don't want to interfere with other threads
        let t = std::thread::spawn(move || {
            let mut cpu_nodes = Vec::<CPUNode>::new();
            for cpu_id in cpu_ids {
                let node = ThreadPool::get_numa_node_for_cpu(cpu_id);

                if node != None {
                    cpu_nodes.push(CPUNode {
                        id: cpu_id,
                        _numa_node: node.unwrap(),
                        empty: false,
                        all_cpus: false,
                    });
                }
            }
            return cpu_nodes;
        });

        return t.join().unwrap();
    }

    /// Returns a list of CPU ids
    fn get_cpu_ids() -> Vec<usize> {
        let mut cpus = Vec::new();
        let mut cpu_set: cpu_set_t;
        let r = unsafe {
            cpu_set = mem::zeroed::<cpu_set_t>();
            sched_getaffinity(0, std::mem::size_of::<cpu_set_t>(), &mut cpu_set)
        };

        if r == 0 {
            for c in 0..CPU_SETSIZE as usize {
                if unsafe { CPU_ISSET(c, &cpu_set) } {
                    cpus.push(c);
                }
            }
        }
        return cpus;
    }

    /// Return the NUMA node for a given CPU
    fn get_numa_node_for_cpu(cpu_id: usize) -> Option<usize> {
        unsafe {
            let mut cpu_set = mem::zeroed::<cpu_set_t>();
            CPU_SET(cpu_id, &mut cpu_set);
            if sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &cpu_set) != 0 {
                return None;
            }
        }
        let cpu: usize = 0;
        let node: usize = 0;
        let null_arg: *const usize = std::ptr::null();

        let r = unsafe { syscall(SYS_getcpu, &cpu, &node, null_arg) };
        if r == 0 {
            return Some(node);
        }
        return None;
    }
}

unsafe impl Send for ThreadPool {}
unsafe impl Sync for ThreadPool {}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl CPUNode {
    /// Returns an empty and initialized CPU node
    pub fn empty() -> Self {
        return CPUNode {
            id: 0,
            _numa_node: 0,
            empty: true,
            all_cpus: false,
        };
    }

    /// Determines if this CPU node is empty
    pub fn is_empty(&self) -> bool {
        return self.empty;
    }

    /// Create a new CPU node indicating it's for all CPUs
    fn new_all_cpus() -> Self {
        return CPUNode {
            id: 0,
            _numa_node: 0,
            empty: false,
            all_cpus: true,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_create_empty_thread_pool() {
        let tp = ThreadPool::new();

        assert_eq!(0, tp.count);
    }

    #[test]
    fn test_get_cpu_ids() {
        let cpus = ThreadPool::get_cpu_ids();

        assert!(cpus.len() > 0);
    }

    #[test]
    fn test_get_cpu_numa_node() {
        let cpus = ThreadPool::get_cpu_ids();

        for cpuid in cpus {
            let node = ThreadPool::get_numa_node_for_cpu(cpuid);

            assert_ne!(None, node);
        }
    }
    #[test]
    fn test_get_cpus_with_nodes() {
        let cpus = ThreadPool::get_cpus();
        let expected_count = ThreadPool::get_cpu_ids().len();
        assert_eq!(expected_count, cpus.len());

        for cpu in cpus {
            assert_eq!(false, cpu.empty);
        }
    }

    #[test]
    fn test_build_thread_pool_target_cpu() {
        let count = 4;
        let target_cpu = ThreadPool::get_cpus()[4];

        let builder = ThreadPoolBuilder::new().count(count).for_cpu(target_cpu);
        let res = builder.build();
        assert_eq!(true, res.is_ok());
        let tp = res.unwrap();
        assert_eq!(count, tp.count);
    }

    #[test]
    fn test_builf_fail_all_and_target_cpus() {
        let target_cpu = ThreadPool::get_cpus()[0];

        let builder = ThreadPoolBuilder::new().for_all_cpus().count(1).for_cpu(target_cpu);
        let result = builder.build();

        assert_eq!(true, result.is_err());
    }

    #[test]
    fn test_build_with_threads() {
        let thread_count = 3;
        let builder = ThreadPoolBuilder::new().count(thread_count).for_cpu(ThreadPool::get_cpus()[0]);
        let tp = builder.build().unwrap();

        assert_eq!(thread_count, tp.join_handles.len());
    }

    #[test]
    fn test_add_thread_single_cpu() {
        let target_cpu = ThreadPool::get_cpus()[0];
        verify_add_thread_correct_cpu(target_cpu);
    }

    #[test]
    fn test_add_thread_all_valid_cpu() {
        for target_cpu in ThreadPool::get_cpus() {
            verify_add_thread_correct_cpu(target_cpu);
        }
    }

    fn verify_add_thread_correct_cpu(target_cpu: CPUNode) {
        let mut tp = ThreadPool::new();

        let res = tp.add_thread(target_cpu);
        const ATTEMPTS: u8 = 5;
        assert_eq!(true, res.is_ok());
        assert_eq!(1, tp.join_handles.len());

        let thread_cpuids = Arc::new(Mutex::new(Vec::new()));
        let thread_cpuids_clone = thread_cpuids.clone();

        tp.schedule_task(move || {
            let cpu_ids = ThreadPool::get_cpu_ids();
            // go through all returned to make sure only one can do work. Here cpu_ids.len() should always be 1
            for id in cpu_ids {
                let mut guard = thread_cpuids_clone.lock().unwrap();
                guard.push(id);
            }
        });

        for _ in 0..ATTEMPTS {
            std::thread::yield_now();
            {
                let ids_vec = thread_cpuids.lock().unwrap();
                // only one CPU should have scheduled the thread
                if ids_vec.len() == 1 {
                    assert_eq!(target_cpu.id, ids_vec[0]);

                    return;
                }
            }

            // not cool to do this, don't do it at home kids.
            // we need a better way to wait for the test task to finish
            std::thread::sleep(std::time::Duration::from_millis(200));
        }

        panic!("failed to schedule threads on target cpus")
    }

    #[test]
    #[should_panic(expected = "Timeout while verifying if tasks can be executed: Timeout")]
    fn test_fail_add_thread_invalid_cpu() {
        //panic!("Could not set affinity for cpu");
        let mut tp = ThreadPool::new();
        let invalid_cpu = CPUNode {
            id: (CPU_SETSIZE - 1) as usize,
            _numa_node: 0,
            empty: false,
            all_cpus: false,
        };
        let err = tp.add_thread(invalid_cpu);

        // this code should never be reached
        assert_eq!(false, err.is_err());
    }

    #[test]
    fn test_schedule_and_run_single_cpu() {
        let cpu = ThreadPool::get_cpus()[0];
        let builder = ThreadPoolBuilder::new().for_cpu(cpu).count(1);
        let mut tp = builder.build().unwrap();

        let executed = std::sync::Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        tp.schedule_task(move || executed_clone.store(true, Ordering::SeqCst));
        tp.shutdown();
        assert_eq!(true, executed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_schedule_and_run_all_cpus() {
        let builder = ThreadPoolBuilder::new().for_all_cpus().count(1);
        let mut tp = builder.build().unwrap();

        let executed = std::sync::Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        tp.schedule_task(move || executed_clone.store(true, Ordering::SeqCst));
        tp.shutdown();
        assert_eq!(true, executed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_fail_no_cpu() {
        let mut tp = ThreadPool::new();
        let result = tp.add_thread(CPUNode::empty());

        assert_eq!(true, result.is_err());
    }
}
