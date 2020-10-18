/// Sample atomic queue use
use rustpool::AtomicQueue;
use rustpool::RPQueue;

fn main() {
    // create a blocking queue with 3 elements set to value 0
    let q = AtomicQueue::<isize>::new(3, || 0);

    println!("simple blocking queue size {}", q.capacity());

    // take the first element
    let v1 = q.take().unwrap();

    println!("first element is {}", v1);

    // remove the remaining items
    let v2 = q.take().unwrap();
    let v3 = q.take().unwrap();

    // how many items available now?
    println!("items in the queue {}", q.available());

    // no more items to return but the atomic queue will never block
    let empty = q.take_wait(std::time::Duration::from_secs(1));

    assert_eq!(None, empty);

    // put everything back
    q.offer(v1);
    q.offer(v2);
    q.offer(v3);

    // how many items available now?
    println!("items in the queue {}", q.available());

    let new_value: isize = 1;

    // we can't add more items than the original capacity
    q.offer(&new_value);

    // we still have the same number of available items
    println!("items in the queue {}", q.available());
}
