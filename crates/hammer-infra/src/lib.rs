#![doc = r#"
```compile_fail
use hammer_infra::heap::Heap;
```

```compile_fail
use hammer_infra::vec::RawVec;
```

```compile_fail
use allocator_api2::alloc::Allocator;
use hammer_infra::vec::Vec;
fn leak(v: &Vec<u64>) {
    let _ = v.allocator();
}
```
"#]

pub mod align;
pub(crate) mod aligned_alloc;
pub mod bihash;
pub mod bitmap;
pub mod boxed;
pub mod checksum;
pub mod descriptor;
pub mod fifo;
pub mod fifo_queue;
pub(crate) mod heap;
pub(crate) mod heap_boxed;
pub(crate) mod heap_vec;
pub mod hint;
pub(crate) mod main_alloc;
pub mod map;
pub mod mask_compare;
pub mod mtrie;
pub mod multi_ring_msg_queue;
pub mod physmem;
pub mod pool;
pub mod prefetch;
pub mod rbtree;
pub mod ring;
pub mod segment;
pub mod simd;
pub mod spinlock;
pub mod svm_region;
pub mod timer_wheel;
pub mod vec;

/// Creates a [`Vec`](crate::vec::Vec) containing the arguments.
#[macro_export]
macro_rules! vec {
    () => {
        $crate::vec::Vec::new()
    };
    ($elem:expr; $n:expr) => {{
        let count = $n;
        let mut values = $crate::vec::Vec::with_capacity(count);
        values.resize(count, $elem);
        values
    }};
    ($($x:expr),+ $(,)?) => {{
        let mut values = $crate::vec::Vec::new();
        $(
            values.push($x);
        )+
        values
    }};
}
