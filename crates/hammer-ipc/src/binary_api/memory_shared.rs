//! Binary API message storage in an ordinary SVM region.

use std::alloc::Layout;
use std::fs::OpenOptions;
use std::io;
use std::mem::{MaybeUninit, align_of, offset_of, size_of};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hammer_infra::mem::{HeapUsage, MemHeap};
use hammer_infra::svm::queue::{SvmQueue, SvmQueueConditionalWait, SvmQueueConfig, SvmQueueError};
use hammer_infra::svm::region::{
    RegionLock, SvmRegion, SvmRegionConfig, SvmRegionError, SvmRegionFlags,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_stats::{DirectoryIndex, StatsMain};
use serde::de::Error as _;

use super::{Api, api::ApiMain, codec};

const SHMEM_VERSION: u32 = 2;

#[hammer_component_macros::runtime_error(subsystem = "binary API memory")]
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    #[error("open shared-memory backing `{path}`: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("shared-memory backing did not open within {waited:?}: {source}")]
    OpenTimeout {
        waited: Duration,
        #[source]
        source: io::Error,
    },
    #[error("shared-memory header was not ready within {waited:?}")]
    ReadyTimeout { waited: Duration },
    #[error("shared-memory region: {source}")]
    Region {
        #[source]
        source: SvmRegionError,
    },
    #[error("shared-memory queue: {source}")]
    Queue {
        #[source]
        source: SvmQueueError,
    },
}

#[repr(C)]
pub struct ShmemHeader {
    version: u32,
    server_pid: AtomicI32,
    input_queue: NonNull<SvmQueue>,
    server_rings: Vec<RingAlloc>,
    client_rings: Vec<RingAlloc>,
    application_restarts: AtomicU32,
    restart_reclaims: AtomicU32,
    garbage_collects: AtomicU32,
    socket_file_index: u32,
}

// SAFETY: queue/ring addresses and vector metadata are immutable after
// region publication; the PID and independent counters are atomic.
unsafe impl Send for ShmemHeader {}
unsafe impl Sync for ShmemHeader {}

#[repr(C)]
struct RingAlloc {
    queue: NonNull<SvmQueue>,
    element_size: u16,
    capacity: u16,
    hits: AtomicU32,
    misses: AtomicU32,
}

// SAFETY: published queue address and geometry are immutable; only stats
// change independently, and the queue protects its client ring head.
unsafe impl Send for RingAlloc {}
unsafe impl Sync for RingAlloc {}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const _: () = {
    assert!(align_of::<ShmemHeader>() == 8);
    assert!(offset_of!(ShmemHeader, version) == 0);
    assert!(offset_of!(ShmemHeader, server_pid) == 4);
    assert!(offset_of!(ShmemHeader, input_queue) == 8);
    assert!(offset_of!(ShmemHeader, server_rings) == 16);
    assert!(offset_of!(ShmemHeader, client_rings) == 40);
    assert!(offset_of!(ShmemHeader, application_restarts) == 64);
    assert!(size_of::<RingAlloc>() == 24);
    assert!(offset_of!(RingAlloc, queue) == 0);
    assert!(offset_of!(RingAlloc, element_size) == 8);
    assert!(offset_of!(RingAlloc, capacity) == 10);
    assert!(offset_of!(RingAlloc, hits) == 12);
    assert!(offset_of!(RingAlloc, misses) == 16);
};

pub struct MsgBuf {
    payload: NonNull<u8>,
    payload_len: usize,
    initialized_len: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RingRole {
    Server,
    Client,
}

impl ShmemHeader {
    fn create(
        heap: &MemHeap,
        input_queue_length: u32,
        pid: i32,
    ) -> Result<NonNull<Self>, SvmQueueError> {
        let queue = RingAlloc::new_queue(
            heap,
            if input_queue_length == 0 {
                1024
            } else {
                input_queue_length
            },
            u32::try_from(size_of::<usize>()).expect("pointer size fits queue slot"),
            pid,
        )?;
        let mut server_rings = Vec::with_capacity(3);
        for (size, capacity) in [(64, 1024), (256, 128), (1024, 64)] {
            server_rings.push(RingAlloc::new(
                heap,
                size + size_of::<RingAlloc>(),
                capacity,
            )?);
        }
        let mut client_rings = Vec::with_capacity(3);
        for (size, capacity) in [(1024, 1024), (2048, 128), (4096, 8)] {
            client_rings.push(RingAlloc::new(
                heap,
                size + size_of::<RingAlloc>(),
                capacity,
            )?);
        }
        let header = Box::new(Self {
            version: SHMEM_VERSION,
            server_pid: AtomicI32::new(pid),
            input_queue: queue,
            server_rings,
            client_rings,
            application_restarts: AtomicU32::new(0),
            restart_reclaims: AtomicU32::new(0),
            garbage_collects: AtomicU32::new(0),
            socket_file_index: u32::MAX,
        });
        Ok(NonNull::from(Box::leak(header)))
    }

    // SAFETY: user_ctx must point to a header initialized and published by
    // this same build. Its Vec values must already satisfy Rust's invariants;
    // range checks reject mismatched geometry, not corrupt Vec bits.
    unsafe fn validate(
        region: &SvmRegion,
        address: NonNull<u8>,
    ) -> Result<NonNull<Self>, MapError> {
        if !address.as_ptr().addr().is_multiple_of(align_of::<Self>())
            || !region.contains_range(address, size_of::<Self>())
        {
            return Err(MapError::Queue {
                source: SvmQueueError::InvalidHeader,
            });
        }
        let header = address.cast::<Self>();
        let shared = unsafe { header.as_ref() };
        for rings in [&shared.server_rings, &shared.client_rings] {
            if rings.len() != 3 || rings.capacity() < rings.len() {
                return Err(MapError::Queue {
                    source: SvmQueueError::InvalidHeader,
                });
            }
            let address =
                NonNull::new(rings.as_ptr().cast_mut().cast::<u8>()).ok_or(MapError::Queue {
                    source: SvmQueueError::InvalidHeader,
                })?;
            let bytes =
                rings
                    .capacity()
                    .checked_mul(size_of::<RingAlloc>())
                    .ok_or(MapError::Queue {
                        source: SvmQueueError::LayoutOverflow,
                    })?;
            if !address
                .as_ptr()
                .addr()
                .is_multiple_of(align_of::<RingAlloc>())
                || !region.contains_range(address, bytes)
            {
                return Err(MapError::Queue {
                    source: SvmQueueError::InvalidHeader,
                });
            }
        }
        let queue = shared.input_queue;
        unsafe { Self::validate_queue(region, queue) }?;
        if unsafe { queue.as_ref() }.element_size() != size_of::<usize>() {
            return Err(MapError::Queue {
                source: SvmQueueError::ElementSizeMismatch {
                    requested: size_of::<usize>(),
                    stored: unsafe { queue.as_ref() }.element_size(),
                },
            });
        }
        for ring in shared.server_rings.iter().chain(&shared.client_rings) {
            unsafe { Self::validate_queue(region, ring.queue) }?;
            let queue = unsafe { ring.queue.as_ref() };
            if queue.element_size() != usize::from(ring.element_size)
                || queue.capacity() != usize::from(ring.capacity)
                || queue.element_size() < MsgBuf::layout(0).0.size()
                || !queue
                    .element_size()
                    .is_multiple_of(MsgBuf::layout(0).0.align())
            {
                return Err(MapError::Queue {
                    source: SvmQueueError::InvalidHeader,
                });
            }
        }
        Ok(header)
    }

    unsafe fn validate_queue(region: &SvmRegion, queue: NonNull<SvmQueue>) -> Result<(), MapError> {
        let base = queue.cast::<u8>();
        if !queue.as_ptr().addr().is_multiple_of(64)
            || !region.contains_range(base, size_of::<SvmQueue>())
        {
            return Err(MapError::Queue {
                source: SvmQueueError::InvalidHeader,
            });
        }
        let end = region
            .base()
            .as_ptr()
            .addr()
            .checked_add(region.size())
            .ok_or(MapError::Queue {
                source: SvmQueueError::LayoutOverflow,
            })?;
        unsafe { SvmQueue::attach(base, end - base.as_ptr().addr()) }
            .map_err(|source| MapError::Queue { source })?;
        Ok(())
    }

    #[inline]
    pub(super) fn server_pid(&self) -> i32 {
        self.server_pid.load(Ordering::Relaxed)
    }

    #[inline]
    pub(super) fn application_restarts(&self) -> u32 {
        self.application_restarts.load(Ordering::Relaxed)
    }

    fn set_server_pid(&self, pid: i32) {
        self.server_pid.store(pid, Ordering::Relaxed);
    }

    #[inline]
    fn rings(&self, role: RingRole) -> &[RingAlloc] {
        match role {
            RingRole::Server => &self.server_rings,
            RingRole::Client => &self.client_rings,
        }
    }

    /// The caller keeps the mapping installed and prevents concurrent unmap.
    #[inline]
    pub unsafe fn input_queue(&self) -> &SvmQueue {
        unsafe { self.input_queue.as_ref() }
    }
}

impl RingAlloc {
    fn new(heap: &MemHeap, element_size: usize, capacity: u32) -> Result<Self, SvmQueueError> {
        let element_size =
            u32::try_from(element_size).map_err(|_| SvmQueueError::LayoutOverflow)?;
        let queue = Self::new_queue(heap, capacity, element_size, 0)?;
        for _ in 0..capacity {
            let slot = unsafe { queue.as_ref().ring_head_slot_unlocked() };
            unsafe {
                slot.as_ptr()
                    .cast::<AtomicPtr<SvmQueue>>()
                    .write(AtomicPtr::new(ptr::null_mut()));
                slot.as_ptr()
                    .add(MsgBuf::timestamp_offset())
                    .cast::<AtomicU32>()
                    .write(AtomicU32::new(0));
                queue.as_ref().advance_ring_head_unlocked();
            }
        }
        Ok(Self {
            queue,
            element_size: u16::try_from(element_size).expect("default ring element size fits u16"),
            capacity: u16::try_from(capacity).expect("default ring count fits u16"),
            hits: AtomicU32::new(0),
            misses: AtomicU32::new(0),
        })
    }

    fn new_queue(
        heap: &MemHeap,
        nels: u32,
        elsize: u32,
        consumer_pid: i32,
    ) -> Result<NonNull<SvmQueue>, SvmQueueError> {
        let config = SvmQueueConfig {
            nels,
            elsize,
            consumer_pid,
        };
        let bytes = SvmQueue::size_to_alloc(&config)?;
        let layout =
            Layout::from_size_align(bytes, 64).map_err(|_| SvmQueueError::LayoutOverflow)?;
        let base = heap
            .allocate_zeroed(layout)
            .expect("shared Data Heap queue allocation");
        unsafe { SvmQueue::init(base, &config) }
    }

    #[inline(always)]
    unsafe fn alloc_slot(
        &self,
        allocation_bytes: usize,
        role: RingRole,
        garbage_collects: &AtomicU32,
    ) -> Option<MsgBuf> {
        if allocation_bytes > usize::from(self.element_size) {
            return None;
        }
        let queue = unsafe { self.queue.as_ref() };
        let mut lock = if role == RingRole::Client {
            Some(loop {
                match queue.lock() {
                    Ok(lock) => break lock,
                    Err(SvmQueueError::OwnerDied) => continue,
                    Err(error) => panic!("client ring queue lock cannot recover: {error:?}"),
                }
            })
        } else {
            None
        };
        let slot = if let Some(guard) = lock.as_mut() {
            guard.ring_head_slot()
        } else {
            unsafe { queue.ring_head_slot_unlocked() }
        };
        let marker = unsafe { AtomicPtr::<SvmQueue>::from_ptr(slot.as_ptr().cast()) };
        if !marker.load(Ordering::Acquire).is_null() {
            let timestamp = unsafe {
                &*slot
                    .as_ptr()
                    .add(MsgBuf::timestamp_offset())
                    .cast::<AtomicU32>()
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock provides GC seconds")
                .as_secs() as u32;
            let mark = timestamp.load(Ordering::Relaxed);
            if mark == 0 {
                timestamp.store(now, Ordering::Relaxed);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            if now.wrapping_sub(mark) <= 10 {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            garbage_collects.fetch_add(1, Ordering::Relaxed);
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        marker.store(self.queue.as_ptr(), Ordering::Release);
        unsafe {
            (*slot
                .as_ptr()
                .add(MsgBuf::timestamp_offset())
                .cast::<AtomicU32>())
            .store(0, Ordering::Relaxed)
        };
        if let Some(guard) = lock.as_mut() {
            guard.advance_ring_head();
        } else {
            unsafe { queue.advance_ring_head_unlocked() };
        }
        let payload_offset = MsgBuf::layout(0).1;
        Some(MsgBuf {
            payload: unsafe { NonNull::new_unchecked(slot.as_ptr().add(payload_offset)) },
            payload_len: 0,
            initialized_len: 0,
        })
    }
}

impl MsgBuf {
    #[inline]
    pub fn len(&self) -> usize {
        self.payload_len
    }

    /// The caller owns a message address dequeued from this live mapping.
    /// Range checks cannot prove that an allocation was not reclaimed by a peer.
    pub unsafe fn from_address(region: &SvmRegion, address: usize) -> Self {
        let offset = Self::layout(0).1;
        let prefix_address = address
            .checked_sub(offset)
            .expect("dequeued API message has a complete prefix");
        let prefix = NonNull::new(prefix_address as *mut u8)
            .expect("dequeued API message prefix is nonnull");
        assert!(
            prefix_address.is_multiple_of(Self::layout(0).0.align())
                && region.contains_range(prefix, offset),
            "dequeued API message prefix is aligned and inside its region"
        );
        let payload =
            NonNull::new(address as *mut u8).expect("dequeued API message address is nonnull");
        let length = unsafe {
            prefix
                .as_ptr()
                .add(Self::length_offset())
                .cast::<u32>()
                .read()
        };
        let payload_len = u32::from_be(length) as usize;
        assert!(
            region.contains_range(payload, payload_len),
            "dequeued API message payload is inside its region"
        );
        Self {
            payload,
            payload_len,
            initialized_len: payload_len,
        }
    }

    #[inline]
    pub unsafe fn as_bytes(&self) -> Result<&[u8], codec::Error> {
        if self.initialized_len < self.payload_len {
            return Err(codec::Error::custom(
                "API message payload is not initialized",
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(self.payload.as_ptr(), self.payload_len) })
    }

    pub unsafe fn encode<T: Api>(&mut self, value: &T) -> Result<usize, codec::Error> {
        let output = unsafe {
            std::slice::from_raw_parts_mut(
                self.payload.as_ptr().cast::<MaybeUninit<u8>>(),
                self.payload_len,
            )
        };
        let bytes = codec::serialize_uninit(value, output)?;
        self.initialized_len = self.initialized_len.max(bytes);
        Ok(bytes)
    }

    pub unsafe fn decode<T: Api>(&self) -> Result<T, codec::Error> {
        let mut decoder = codec::Deserializer::new(unsafe { self.as_bytes() }?);
        let message = T::deserialize(&mut decoder)?;
        if decoder.remaining_bytes() != 0 {
            return Err(codec::Error::custom(
                "API message has trailing payload bytes",
            ));
        }
        Ok(message)
    }

    pub(crate) unsafe fn free_nolock(self, lock: &RegionLock<'_>) {
        if unsafe { self.release_ring() } {
            return;
        }
        let heap = lock.data_heap().expect("API region has a Data Heap");
        let active_heap = heap.activate();
        let (layout, offset) = Self::layout(self.payload_len);
        let base = unsafe { NonNull::new_unchecked(self.payload.as_ptr().sub(offset)) };
        unsafe { heap.deallocate(base, layout) };
        drop(active_heap);
    }

    unsafe fn release_ring(&self) -> bool {
        let base = unsafe { self.payload.as_ptr().sub(Self::layout(0).1) };
        let marker = unsafe { AtomicPtr::<SvmQueue>::from_ptr(base.cast()) };
        if marker.load(Ordering::Acquire).is_null() {
            return false;
        }
        unsafe {
            (*base.add(Self::timestamp_offset()).cast::<AtomicU32>()).store(0, Ordering::Relaxed)
        };
        marker.store(ptr::null_mut(), Ordering::Release);
        true
    }

    const fn layout(payload_len: usize) -> (Layout, usize) {
        let (prefix, _) = match Layout::new::<AtomicPtr<SvmQueue>>().extend(Layout::new::<u32>()) {
            Ok(layout) => layout,
            Err(_) => panic!("message length offset fits"),
        };
        let (prefix, _) = match prefix.extend(Layout::new::<AtomicU32>()) {
            Ok(layout) => layout,
            Err(_) => panic!("GC timestamp offset fits"),
        };
        let payload = match Layout::array::<u8>(payload_len) {
            Ok(layout) => layout,
            Err(_) => panic!("message length fits allocation layout"),
        };
        match prefix.extend(payload) {
            Ok(layout) => layout,
            Err(_) => panic!("message prefix and payload fit allocation layout"),
        }
    }

    const fn timestamp_offset() -> usize {
        let (prefix, _) = match Layout::new::<AtomicPtr<SvmQueue>>().extend(Layout::new::<u32>()) {
            Ok(layout) => layout,
            Err(_) => panic!("message length offset fits"),
        };
        match prefix.extend(Layout::new::<AtomicU32>()) {
            Ok((_, offset)) => offset,
            Err(_) => panic!("GC timestamp offset fits"),
        }
    }

    const fn length_offset() -> usize {
        match Layout::new::<AtomicPtr<SvmQueue>>().extend(Layout::new::<u32>()) {
            Ok((_, offset)) => offset,
            Err(_) => panic!("message length offset fits"),
        }
    }
}

impl From<&MsgBuf> for usize {
    #[inline]
    fn from(message: &MsgBuf) -> Self {
        message.payload.as_ptr().addr()
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const _: () = {
    assert!(MsgBuf::layout(0).0.align() == 8);
    assert!(MsgBuf::layout(0).1 == 16);
    assert!(MsgBuf::length_offset() == 8);
    assert!(MsgBuf::timestamp_offset() == 12);
};

impl ApiMain {
    // Message allocation and release use this same owner. ShmemHeader supplies
    // ring storage; it never combines its own rings with a different main's heap.
    pub unsafe fn alloc(&self, payload_len: usize) -> MsgBuf {
        unsafe { self.alloc_internal(payload_len, None, false, false) }
            .expect("non-nullable shared-message allocation cannot exhaust the Data Heap")
    }

    pub unsafe fn alloc_zeroed(&self, payload_len: usize) -> MsgBuf {
        unsafe { self.alloc_internal(payload_len, None, true, false) }
            .expect("non-nullable shared-message allocation cannot exhaust the Data Heap")
    }

    pub unsafe fn alloc_or_null(&self, payload_len: usize) -> Option<MsgBuf> {
        unsafe { self.alloc_internal(payload_len, None, false, true) }
    }

    pub unsafe fn alloc_as_client(&self, payload_len: usize) -> MsgBuf {
        unsafe { self.alloc_internal(payload_len, Some(RingRole::Client), false, false) }
            .expect("non-nullable shared-message allocation cannot exhaust the Data Heap")
    }

    pub unsafe fn alloc_zeroed_as_client(&self, payload_len: usize) -> MsgBuf {
        unsafe { self.alloc_internal(payload_len, Some(RingRole::Client), true, false) }
            .expect("non-nullable shared-message allocation cannot exhaust the Data Heap")
    }

    pub unsafe fn alloc_as_client_or_null(&self, payload_len: usize) -> Option<MsgBuf> {
        unsafe { self.alloc_internal(payload_len, Some(RingRole::Client), false, true) }
    }

    unsafe fn alloc_internal(
        &self,
        payload_len: usize,
        forced_role: Option<RingRole>,
        zeroed: bool,
        nullable: bool,
    ) -> Option<MsgBuf> {
        assert!(
            payload_len <= i32::MAX as usize,
            "API message length fits signed int"
        );
        let (layout, payload_offset) = MsgBuf::layout(payload_len);
        let header = unsafe { self.shmem_header() };
        let role = forced_role.unwrap_or_else(|| {
            if self.process_pid.load(Ordering::Relaxed) == header.server_pid() {
                RingRole::Server
            } else {
                RingRole::Client
            }
        });
        if role == RingRole::Server {
            hammer_runtime::thread_main::ensure_main_thread()
                .expect("server message rings require the main thread");
        }

        let mut message = header.rings(role).iter().find_map(|ring| unsafe {
            ring.alloc_slot(layout.size(), role, &header.garbage_collects)
        });
        if message.is_none() {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            let region = unsafe { *self.rp.get() }.expect("API region mapped before allocation");
            let lock = unsafe { region.as_ref() }
                .lock()
                .expect("Data Heap region lock");
            let heap = lock.data_heap().expect("API region has a Data Heap");
            let active_heap = heap.activate();
            let allocation = if nullable {
                heap.allocate(layout)
            } else {
                Some(
                    heap.allocate(layout)
                        .expect("non-nullable Data Heap allocation"),
                )
            };
            message = allocation.map(|base| {
                unsafe {
                    base.as_ptr()
                        .cast::<AtomicPtr<SvmQueue>>()
                        .write(AtomicPtr::new(ptr::null_mut()));
                    base.as_ptr()
                        .add(MsgBuf::timestamp_offset())
                        .cast::<AtomicU32>()
                        .write(AtomicU32::new(0));
                }
                MsgBuf {
                    payload: unsafe { NonNull::new_unchecked(base.as_ptr().add(payload_offset)) },
                    payload_len,
                    initialized_len: 0,
                }
            });
            drop(active_heap);
            drop(lock);
        }
        let mut message = message?;
        message.payload_len = payload_len;
        let prefix = unsafe { message.payload.as_ptr().sub(payload_offset) };
        unsafe {
            prefix
                .add(MsgBuf::length_offset())
                .cast::<u32>()
                .write((payload_len as u32).to_be())
        };
        if zeroed {
            unsafe { ptr::write_bytes(message.payload.as_ptr(), 0, payload_len) };
            message.initialized_len = payload_len;
        }
        Some(message)
    }

    /// Releases a message owned by the caller, through the main whose region
    /// supplied it. The region must stay mapped and peers must not reclaim it.
    pub unsafe fn free(&self, message: MsgBuf) {
        let region = unsafe { *self.rp.get() }.expect("originating region remains mapped");
        let region = unsafe { region.as_ref() };
        let (layout, offset) = MsgBuf::layout(message.payload_len);
        let prefix = NonNull::new(
            (message
                .payload
                .as_ptr()
                .addr()
                .checked_sub(offset)
                .expect("message has allocation prefix")) as *mut u8,
        )
        .expect("message allocation is nonnull");
        assert!(
            region.contains_range(prefix, layout.size()),
            "message is released through its originating API region"
        );
        if unsafe { message.release_ring() } {
            return;
        }
        let lock = region.lock().expect("Data Heap region lock");
        unsafe { message.free_nolock(&lock) };
    }

    /// The caller must keep the selected mapping installed for the borrow.
    #[inline]
    pub unsafe fn shmem_header(&self) -> &ShmemHeader {
        unsafe {
            (*self.shmem_header.get())
                .expect("API region mapped before header access")
                .as_ref()
        }
    }

    /// The caller has exclusive lifecycle access, a mapped root, and no
    /// outstanding local message/queue borrows when it changes the mapping.
    /// An existing user_ctx must point to a same-build initialized header;
    /// arbitrary shared bytes cannot be parsed as Rust Vec metadata.
    pub unsafe fn map_shared_region(
        &self,
        mut root: SvmRegion,
        path: &Path,
        is_server: bool,
    ) -> Result<(), MapError> {
        let backing: OwnedFd = if is_server {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|source| MapError::Open {
                    path: path.to_path_buf(),
                    source,
                })?
                .into()
        } else {
            let mut opened = None;
            let mut open_error = None;
            for _ in 0..10_000 {
                std::thread::sleep(Duration::from_millis(10));
                match OpenOptions::new().read(true).write(true).open(path) {
                    Ok(file) => {
                        opened = Some(file.into());
                        break;
                    }
                    Err(source) => open_error = Some(source),
                }
            }
            opened.ok_or_else(|| MapError::OpenTimeout {
                waited: Duration::from_secs(100),
                source: open_error.expect("client attempted to open the API backing"),
            })?
        };
        if unsafe {
            libc::fchown(
                backing.as_raw_fd(),
                self.api_uid() as libc::uid_t,
                self.api_gid() as libc::gid_t,
            )
        } != 0
        {
            let source = io::Error::last_os_error();
            tracing::warn!(path = %path.display(), ?source, "API backing ownership unchanged");
        }
        let config = SvmRegionConfig {
            name: self.api_region_name.clone(),
            size: usize::try_from(if self.api_size == 0 {
                16 << 20
            } else {
                self.api_size
            })
            .expect("API data size fits target pointer width"),
            pvt_heap_size: usize::try_from(self.api_pvt_heap_size)
                .expect("API PVT heap fits target pointer width"),
            flags: SvmRegionFlags::DATA_HEAP,
        };
        let mut region = Box::new(
            root.find_or_create_subregion(&config, backing)
                .map_err(|source| MapError::Region { source })?,
        );
        let region_address = NonNull::from(&mut *region);
        let pid = std::process::id() as i32;
        let attachment = (|| {
            let mut header_address = region.user_context();
            let mut initialized_here = false;
            if header_address.is_none() && is_server {
                let mut lock = region
                    .lock()
                    .map_err(|source| MapError::Region { source })?;
                header_address = region.user_context();
                if header_address.is_none() {
                    let heap = lock.data_heap().expect("API region has a Data Heap");
                    let active_heap = heap.activate();
                    let header = ShmemHeader::create(heap, self.input_queue_length, pid)
                        .map_err(|source| MapError::Queue { source })?;
                    drop(active_heap);
                    lock.set_user_context(header.cast());
                    header_address = Some(header.cast());
                    initialized_here = true;
                }
            }
            if header_address.is_none() && !is_server {
                for _ in 0..10_000 {
                    std::thread::sleep(Duration::from_millis(10));
                    header_address = region.user_context();
                    if header_address.is_some() {
                        break;
                    }
                }
            }
            let header_address = header_address.ok_or(MapError::ReadyTimeout {
                waited: Duration::from_secs(100),
            })?;
            let header = unsafe { ShmemHeader::validate(&region, header_address) }?;
            let restart_lock = if is_server && !initialized_here {
                // The boxed descriptor stays at this address when moved into
                // the mapped-region list. No lock survives a failed unmap.
                let mut lock = unsafe { region_address.as_ref() }
                    .lock()
                    .map_err(|source| MapError::Region { source })?;
                lock.remove_exited_clients()
                    .map_err(|source| MapError::Region { source })?;
                Some(lock)
            } else {
                None
            };
            Ok((header, restart_lock))
        })();
        let (header, restart_lock): (NonNull<ShmemHeader>, Option<RegionLock<'_>>) =
            match attachment {
                Ok(attached) => attached,
                Err(error) => {
                    if let Err(source) = (*region).unmap() {
                        tracing::error!(?source, "failed API attachment could not unmap");
                        std::process::abort();
                    }
                    return Err(error);
                }
            };

        if let Some(lock) = restart_lock {
            let shared = unsafe { header.as_ref() };
            shared.application_restarts.fetch_add(1, Ordering::Relaxed);
            shared.set_server_pid(pid);
            let queue = unsafe { shared.input_queue() };
            queue.set_consumer_pid(pid);
            unsafe { queue.reset_mutex_for_restart() };
            unsafe { *self.rp.get() = Some(region_address) };
            loop {
                let mut address = [0_u8; size_of::<usize>()];
                match queue.sub(&mut address, SvmQueueConditionalWait::Nowait) {
                    Ok(()) => {
                        unsafe {
                            MsgBuf::from_address(&region, usize::from_ne_bytes(address))
                                .free_nolock(&lock)
                        };
                        shared.restart_reclaims.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(SvmQueueError::Empty) => break,
                    Err(error) => {
                        tracing::error!(?error, "restart input queue cannot be drained");
                        std::process::abort();
                    }
                }
            }
            drop(lock);
            match root.lock() {
                Ok(mut lock) => {
                    if let Err(source) = lock.remove_exited_clients() {
                        tracing::warn!(?source, "root PID scan after API restart failed");
                    }
                }
                Err(source) => tracing::warn!(?source, "root PID scan after API restart failed"),
            }
        }
        unsafe {
            let regions = &mut *self.mapped_shmem_regions.get();
            regions.push(Box::new(root));
            regions.push(region);
            *self.rp.get() = Some(region_address);
            *self.shmem_header.get() = Some(header);
        }
        self.process_pid.store(pid, Ordering::Relaxed);
        if is_server {
            // vl_mem_api_init: mapping precedes built-in memory message setup.
            unsafe { self.set_primary_region() };
            super::memclnt::setup_message_id_table(self);
        }
        Ok(())
    }

    /// Only ordinary mapped regions are detached; private memfd regions have
    /// a distinct owner and lifetime.
    pub unsafe fn unmap_shared_regions(&self) -> Result<(), SvmRegionError> {
        unsafe {
            *self.shmem_header.get() = None;
            *self.rp.get() = None;
            *self.primary_rp.get() = None;
            let regions = &mut *self.mapped_shmem_regions.get();
            let mut unmap_error = None;
            while let Some(region) = regions.pop() {
                if let Err(source) = (*region).unmap() {
                    if unmap_error.is_none() {
                        unmap_error = Some(source);
                    } else {
                        tracing::error!(?source, "additional API region could not unmap");
                    }
                }
            }
            if let Some(error) = unmap_error {
                return Err(error);
            }
        }
        Ok(())
    }
}

// ---- api segment heap entries: this module owns the region heaps, so the
// ---- three `/mem` entries and their collectors live here.

/// Collects the root region's private heap, `/mem/global_vm pvt`.
fn collect_root_region_pvt_usage(entry_index: DirectoryIndex, row: u32) {
    let api = ApiMain::current();
    if !api.is_mapped() {
        return;
    }
    let region = api
        .root_region()
        .lock()
        .expect("mapped root region takes its lock");
    let usage = region.pvt_heap().usage();
    drop(region);
    write_region_usage(entry_index, row, usage);
}

/// Collects the api region's private heap, `/mem/<region> pvt`.
fn collect_api_region_pvt_usage(entry_index: DirectoryIndex, row: u32) {
    let api = ApiMain::current();
    if !api.is_mapped() {
        return;
    }
    let region = api
        .primary_region()
        .lock()
        .expect("mapped API region takes its lock");
    let usage = region.pvt_heap().usage();
    drop(region);
    write_region_usage(entry_index, row, usage);
}

/// Collects the api region's data heap (queues and the shmem header),
/// `/mem/<region> data`.
fn collect_api_region_data_usage(entry_index: DirectoryIndex, row: u32) {
    let api = ApiMain::current();
    if !api.is_mapped() {
        return;
    }
    let region = api
        .primary_region()
        .lock()
        .expect("mapped API region takes its lock");
    let usage = region
        .data_heap()
        .expect("API region has a Data Heap")
        .usage();
    drop(region);
    write_region_usage(entry_index, row, usage);
}

/// The one lock and family mapping the three collectors above share.
fn write_region_usage(entry_index: DirectoryIndex, row: u32, usage: HeapUsage) {
    let stats_main = StatsMain::global().expect("stats owner is installed before the round");
    let mut segment = stats_main.segment.lock();
    hammer_stats::mem::update_mem_usage(&mut segment, entry_index, row, usage);
}

/// Registers the root region's private heap entry.
///
/// The entry name is derived from the region name and its role: the root
/// region is `/global_vm`; the api region name comes from `ApiSegmentConfig`
/// (default `/vpe-api`, leaf form `vpe-api`).
#[hammer_component_macros::stats_registration]
fn register_root_region_pvt_heap(stats_main: &StatsMain) -> RuntimeResult<()> {
    hammer_stats::mem::register_mem_heap(stats_main, "global_vm pvt", collect_root_region_pvt_usage)
        .map_err(RuntimeError::from)
}

/// Registers the api region's private heap entry.
#[hammer_component_macros::stats_registration]
fn register_api_region_pvt_heap(stats_main: &StatsMain) -> RuntimeResult<()> {
    let api = ApiMain::current();
    let leaf = api.api_region_name.trim_start_matches('/');
    let name = format!("{leaf} pvt");
    hammer_stats::mem::register_mem_heap(stats_main, &name, collect_api_region_pvt_usage)
        .map_err(RuntimeError::from)
}

/// Registers the api region's data heap entry.
#[hammer_component_macros::stats_registration]
fn register_api_region_data_heap(stats_main: &StatsMain) -> RuntimeResult<()> {
    let api = ApiMain::current();
    let leaf = api.api_region_name.trim_start_matches('/');
    let name = format!("{leaf} data");
    hammer_stats::mem::register_mem_heap(stats_main, &name, collect_api_region_data_usage)
        .map_err(RuntimeError::from)
}
