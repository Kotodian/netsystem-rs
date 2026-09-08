use crate::error::{BufferInvariant, DataPlaneResult};
use crate::graph::NodeErrorIndex;
mod cursor;
mod flags;
pub mod frame_pool;
mod header;
mod main;
mod opaque;
mod operations;
mod pool;

pub use crate::graph::frame::{Frame, FrameBatchWidth};
pub use cursor::BufferPacketCursor;
pub use flags::BufferFlags;
pub use main::{BufferMain, BufferThreadCache};
pub use opaque::{BufferOpaque, BufferOpaqueRegion, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES};
use opaque::{PrimaryOpaque, SecondaryOpaque};

/// Logical vector capacity of a graph Frame.
pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;
pub const DEFAULT_PACKET_HEADROOM: usize = 256;
include!(concat!(env!("OUT_DIR"), "/buffer_config.rs"));
const BUFFER_INVALID_INDEX: u32 = 0;

/// Central free indices are transferred to a Worker cache in batches.
const BUFFER_THREAD_CACHE_BATCH: usize = 32;
/// High-water mark at which the thread cache returns a batch back to the
/// Pool free list, preventing unbounded cache growth and keeping Pool free
/// list non-empty for other consumers.
const BUFFER_THREAD_CACHE_HIGH_WATER: usize = 512;
pub use header::Buffer;
use main::BufferPool;
use std::cell::RefMut;
