/// Thread pool example
use rustpool::{ThreadPool, ThreadPoolBuilder};

fn main() {
    // change USE_ALL_CPUS to target multiple or a single CPU
    const USE_ALL_CPUS: bool = false;

    let cpu = ThreadPool::get_cpus()[0];

    let builder: ThreadPoolBuilder;
    if USE_ALL_CPUS {
        builder = ThreadPoolBuilder::new().for_all_cpus().count(2);
    } else {
        builder = ThreadPoolBuilder::new().for_cpu(cpu).count(2);
    }
    let res = builder.build();

    if res.is_ok() {
        println!("Thread pool created");
        let tp = res.unwrap();

        tp.schedule_task(|| print_task_info(1));
        tp.schedule_task(|| print_task_info(2));

        // on drop, the pool will wait for all running tasks to end
        return;
    }

    // panic if we can't create the thread pool
    std::panic::panic_any(res.err());
}

fn print_task_info(task_id: u16) {
    println!("Task {} executed in thread - {:?}", task_id, std::thread::current().id());
    let thread_cpus = ThreadPool::get_cpus();

    println!("CPU count available to task {}: {}", task_id, thread_cpus.len());
    std::thread::sleep(std::time::Duration::from_millis(1000));
}
