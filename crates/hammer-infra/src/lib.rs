#![doc = r#"
```compile_fail
use hammer_infra::heap::Heap;
```
"#]

pub mod align;
pub mod bihash;
pub mod bitmap;
pub mod bytes;
pub mod checksum;
pub mod descriptor;
pub mod fifo;
pub mod fifo_queue;
pub(crate) mod heap;
pub(crate) mod heap_boxed;
pub mod hint;
pub mod linked_list;
pub mod main_heap;
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
pub mod sparse_vec;
pub mod stack;
pub mod svm_region;
pub mod sync;
pub mod thread_owned;
pub mod timer_wheel;

pub use main_heap::PageSize;
pub use svm_region::page_size;
