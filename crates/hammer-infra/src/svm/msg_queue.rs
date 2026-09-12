//! VPP-style shared-memory multi-ring message queue.
//!
//! The descriptor queue and ring accounting live in the shared mapping. Ring
//! bytes are supplied by the caller in [`SvmMsgQRingConfig`], matching VPP's
//! `svm_msg_q_ring_cfg_t::data` ownership boundary. The queue never allocates,
//! frees, or binds payload storage after construction.

use std::alloc::{Layout, LayoutError};
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::time::Duration;

use posix_sync::condvar::{
    BorrowedCondvar, CondvarBuilder, CondvarClock, CondvarSharing, CondvarSignalError,
    RawCondvarAlloc, WaitOutcome,
};
use posix_sync::mutex::guards::{MutexGuard, RobustGuardContainer, StandardGuard};
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexSharing, RawMutexAlloc, robustness_markers::Robust,
};

use crate::svm::queue::SvmQueueConditionalWait;
use crate::svm::ssvm::{SsvmError, SsvmPrivate};
use crate::sync::{SpinLock, SpinLockGuard};

const CACHE_LINE_BYTES: usize = 64;
static SVM_MSG_Q_MAPPING_ANCHOR: () = ();

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SvmMsgQDescriptorParts {
    pub ring_index: u32,
    pub elt_index: u32,
}

#[repr(C)]
pub union SvmMsgQDescriptor {
    pub parts: SvmMsgQDescriptorParts,
    pub as_u64: u64,
}

impl SvmMsgQDescriptor {
    pub const INVALID: Self = Self { as_u64: u64::MAX };

    pub const fn new(ring_index: u32, elt_index: u32) -> Self {
        Self {
            parts: SvmMsgQDescriptorParts {
                ring_index,
                elt_index,
            },
        }
    }

    pub fn parts(self) -> SvmMsgQDescriptorParts {
        unsafe { self.parts }
    }

    pub fn is_invalid(self) -> bool {
        unsafe { self.as_u64 == u64::MAX }
    }
}

impl Copy for SvmMsgQDescriptor {}
impl Clone for SvmMsgQDescriptor {
    fn clone(&self) -> Self {
        *self
    }
}

impl std::fmt::Debug for SvmMsgQDescriptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SvmMsgQDescriptor")
            .field("ring_index", &self.parts().ring_index)
            .field("elt_index", &self.parts().elt_index)
            .finish()
    }
}

impl PartialEq for SvmMsgQDescriptor {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.as_u64 == other.as_u64 }
    }
}

impl Eq for SvmMsgQDescriptor {}

/// Caller-owned bytes for one VPP message ring.
pub struct SvmMsgQRingConfig<'data> {
    pub nitems: u32,
    pub elsize: u32,
    pub data: &'data mut [u8],
}

impl<'data> SvmMsgQRingConfig<'data> {
    pub fn new(nitems: u32, elsize: u32, data: &'data mut [u8]) -> Self {
        Self {
            nitems,
            elsize,
            data,
        }
    }
}

pub struct SvmMsgQConfig<'rings, 'data> {
    pub consumer_pid: i32,
    pub q_nitems: u32,
    pub ring_cfgs: &'rings mut [SvmMsgQRingConfig<'data>],
}

#[derive(Debug, thiserror::Error)]
pub enum SvmMsgQError {
    #[error("message queue descriptor count must be positive")]
    ZeroQueueCapacity,
    #[error("message queue must contain at least one ring")]
    NoRings,
    #[error("message ring {ring} item count must be positive")]
    ZeroRingCapacity { ring: u32 },
    #[error("message ring {ring} element size must be positive")]
    ZeroElementSize { ring: u32 },
    #[error("message ring {ring} data length {actual} does not equal {expected}")]
    RingDataLength {
        ring: u32,
        expected: usize,
        actual: usize,
    },
    #[error("message queue layout is too large")]
    LayoutOverflow,
    #[error("message descriptor queue is full")]
    QueueFull,
    #[error("message ring {ring} is full")]
    RingFull { ring: u32 },
    #[error("message ring {ring} does not exist")]
    InvalidRing { ring: u32 },
    #[error("message descriptor is invalid")]
    InvalidDescriptor,
    #[error("message descriptor does not refer to an allocated ring slot")]
    DescriptorNotAllocated,
    #[error("message queue is empty")]
    Empty,
    #[error("message queue lock is busy")]
    LockBusy,
    #[error("message queue wait deadline expired")]
    Timeout,
    #[error("message queue header is invalid")]
    InvalidHeader,
    #[error("message queue segment operation failed: {0}")]
    Segment(#[from] SsvmError),
    #[error("message queue mutex owner died")]
    OwnerDied,
    #[error("message queue mutex is not recoverable: {source}")]
    NotRecoverable {
        #[source]
        source: posix_sync::mutex::MutexLockError,
    },
    #[error("message queue mutex lock failed: {source}")]
    Lock {
        #[source]
        source: posix_sync::mutex::MutexLockError,
    },
    #[error("message queue condition wait failed: {source}")]
    Wait {
        #[source]
        source: posix_sync::condvar::CondvarWaitError,
    },
    #[error("message queue condition signal failed after {operation} committed: {source}")]
    SignalAfterCommit {
        operation: &'static str,
        #[source]
        source: CondvarSignalError,
    },
    #[error("message queue event notification failed after {operation} committed: {source}")]
    EventSignalAfterCommit {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("message queue operation failed: {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("message queue layout construction failed")]
    Layout(#[from] LayoutError),
}

#[repr(C)]
struct SvmMsgQSharedQueue {
    mutex: MaybeUninit<RawMutexAlloc>,
    condvar: MaybeUninit<RawCondvarAlloc>,
    head: u32,
    tail: u32,
    cursize: u32,
    maxsize: u32,
    elsize: u32,
    consumer_pid: i32,
}

#[repr(C)]
struct SvmMsgQRingShared {
    cursize: u32,
    nitems: u32,
    head: u32,
    tail: u32,
    elsize: u32,
}

#[repr(C, align(64))]
struct SvmMsgQShared {
    n_rings: u32,
    reserved: u32,
    q: SvmMsgQSharedQueue,
}

struct SvmMsgQRing {
    shared: NonNull<SvmMsgQRingShared>,
    data: NonNull<u8>,
}

pub struct SvmMsgQ<'data> {
    shared: NonNull<SvmMsgQShared>,
    descriptors: NonNull<SvmMsgQDescriptor>,
    rings: Vec<SvmMsgQRing>,
    mutex: BorrowedMutex<'static, Robust>,
    condvar: BorrowedCondvar<'static>,
    event_fd: Option<OwnedFd>,
    producer_lock: SpinLock<()>,
    _data: std::marker::PhantomData<&'data mut [u8]>,
}

unsafe impl Send for SvmMsgQ<'_> {}
unsafe impl Sync for SvmMsgQ<'_> {}

impl<'data> SvmMsgQ<'data> {
    pub fn layout(config: &SvmMsgQConfig<'_, 'data>) -> Result<Layout, SvmMsgQError> {
        validate_config(config)?;
        let descriptors = (config.q_nitems as usize)
            .checked_mul(size_of::<SvmMsgQDescriptor>())
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        let rings = config
            .ring_cfgs
            .len()
            .checked_mul(size_of::<SvmMsgQRingShared>())
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        let bytes = size_of::<SvmMsgQShared>()
            .checked_add(descriptors)
            .and_then(|bytes| bytes.checked_add(rings))
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        Layout::from_size_align(bytes, CACHE_LINE_BYTES).map_err(SvmMsgQError::Layout)
    }

    /// Initializes only queue metadata in the caller-owned segment allocation.
    /// Ring bytes remain owned by the caller and are referenced by `config`.
    pub unsafe fn init_at(
        segment: &SsvmPrivate,
        offset: u64,
        config: &SvmMsgQConfig<'_, 'data>,
    ) -> Result<Self, SvmMsgQError> {
        let layout = Self::layout(config)?;
        let base = segment.offset_ptr(offset, layout.size(), layout.align())?;
        let shared = NonNull::new(base.cast::<SvmMsgQShared>()).expect("message queue pointer");
        unsafe { std::ptr::write_bytes(shared.as_ptr().cast::<u8>(), 0, layout.size()) };
        unsafe {
            (*shared.as_ptr()).n_rings = config.ring_cfgs.len() as u32;
            (*shared.as_ptr()).q.maxsize = config.q_nitems;
            (*shared.as_ptr()).q.elsize = size_of::<SvmMsgQDescriptor>() as u32;
            (*shared.as_ptr()).q.consumer_pid = config.consumer_pid;
        }
        let (descriptors, rings) = shared_layout(shared, config.ring_cfgs.len(), config.q_nitems);
        for (index, config_ring) in config.ring_cfgs.iter().enumerate() {
            let ring = unsafe { &mut *rings.as_ptr().add(index) };
            ring.nitems = config_ring.nitems;
            ring.elsize = config_ring.elsize;
        }
        let mutex = unsafe {
            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed(
                    (&mut (*shared.as_ptr()).q.mutex as *mut MaybeUninit<RawMutexAlloc>).cast(),
                    &SVM_MSG_Q_MAPPING_ANCHOR,
                )
        };
        let condvar = unsafe {
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed(
                    (&mut (*shared.as_ptr()).q.condvar as *mut MaybeUninit<RawCondvarAlloc>).cast(),
                    &SVM_MSG_Q_MAPPING_ANCHOR,
                )
        };
        Ok(Self {
            shared,
            descriptors,
            rings: ring_handles(rings, config.ring_cfgs),
            mutex,
            condvar,
            event_fd: None,
            producer_lock: SpinLock::new(()),
            _data: std::marker::PhantomData,
        })
    }

    /// Attaches queue metadata and receives the caller-owned ring bytes again.
    pub unsafe fn attach(
        segment: &SsvmPrivate,
        offset: u64,
        ring_cfgs: &'data mut [SvmMsgQRingConfig<'data>],
    ) -> Result<Self, SvmMsgQError> {
        let header = NonNull::new(
            segment
                .offset_ptr(offset, size_of::<SvmMsgQShared>(), CACHE_LINE_BYTES)?
                .cast::<SvmMsgQShared>(),
        )
        .expect("message queue pointer");
        let shared_ref = unsafe { header.as_ref() };
        if shared_ref.n_rings == 0
            || shared_ref.n_rings as usize != ring_cfgs.len()
            || shared_ref.q.maxsize == 0
            || shared_ref.q.elsize as usize != size_of::<SvmMsgQDescriptor>()
            || shared_ref.q.head >= shared_ref.q.maxsize
            || shared_ref.q.tail >= shared_ref.q.maxsize
            || shared_ref.q.cursize > shared_ref.q.maxsize
        {
            return Err(SvmMsgQError::InvalidHeader);
        }
        let (descriptors, rings) = shared_layout(header, ring_cfgs.len(), shared_ref.q.maxsize);
        for (index, config_ring) in ring_cfgs.iter().enumerate() {
            let ring = unsafe { &*rings.as_ptr().add(index) };
            validate_ring_data(index as u32, ring.nitems, ring.elsize, config_ring.data)?;
            if ring.nitems != config_ring.nitems || ring.elsize != config_ring.elsize {
                return Err(SvmMsgQError::InvalidHeader);
            }
        }
        let mutex = unsafe {
            BorrowedMutex::<Robust>::from_raw(
                (&mut (*header.as_ptr()).q.mutex as *mut MaybeUninit<RawMutexAlloc>).cast(),
                &SVM_MSG_Q_MAPPING_ANCHOR,
            )
        };
        let condvar = unsafe {
            BorrowedCondvar::from_raw(
                (&mut (*header.as_ptr()).q.condvar as *mut MaybeUninit<RawCondvarAlloc>).cast(),
                &SVM_MSG_Q_MAPPING_ANCHOR,
                CondvarClock::Monotonic,
            )
        };
        Ok(Self {
            shared: header,
            descriptors,
            rings: ring_handles(rings, ring_cfgs),
            mutex,
            condvar,
            event_fd: None,
            producer_lock: SpinLock::new(()),
            _data: std::marker::PhantomData,
        })
    }

    pub fn size(&self) -> u32 {
        unsafe { (*self.shared.as_ptr()).q.cursize }
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn is_full(&self) -> bool {
        unsafe { self.size() == (*self.shared.as_ptr()).q.maxsize }
    }

    pub fn ring_is_full(&self, ring_index: u32) -> Result<bool, SvmMsgQError> {
        let ring = self.ring(ring_index)?;
        Ok(unsafe { (*ring.shared.as_ptr()).cursize >= (*ring.shared.as_ptr()).nitems })
    }

    pub fn alloc_msg(&self, nbytes: u32) -> SvmMsgQDescriptor {
        for (ring_index, ring) in self.rings.iter().enumerate() {
            let shared = unsafe { &mut *ring.shared.as_ptr() };
            if shared.elsize >= nbytes && shared.cursize < shared.nitems {
                return self.alloc_msg_w_ring_unchecked(ring_index as u32, shared);
            }
        }
        SvmMsgQDescriptor::INVALID
    }

    pub fn alloc_msg_w_ring(&self, ring_index: u32) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        let ring = self.ring(ring_index)?;
        let shared = unsafe { &mut *ring.shared.as_ptr() };
        if shared.cursize >= shared.nitems {
            return Err(SvmMsgQError::RingFull { ring: ring_index });
        }
        Ok(self.alloc_msg_w_ring_unchecked(ring_index, shared))
    }

    pub fn add(&self, msg: SvmMsgQDescriptor, nowait: bool) -> Result<(), SvmMsgQError> {
        let parts = self.validate_descriptor(msg)?;
        if self.event_fd.is_some() {
            let guard = if nowait {
                self.producer_lock
                    .try_lock()
                    .ok_or(SvmMsgQError::LockBusy)?
            } else {
                self.producer_lock.lock()
            };
            self.add_locked(parts, guard, nowait)
        } else {
            let guard = if nowait {
                self.try_lock_shared()?.ok_or(SvmMsgQError::LockBusy)?
            } else {
                self.lock_shared()?
            };
            self.add_locked_shared(parts, guard, nowait)
        }
    }

    pub unsafe fn add_raw(&self, msg: SvmMsgQDescriptor) -> Result<(), SvmMsgQError> {
        let parts = self.validate_descriptor(msg)?;
        unsafe { self.add_raw_parts(parts) }
    }

    pub fn sub(&self, wait: SvmQueueConditionalWait) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        if !self.is_empty() {
            return unsafe { self.sub_raw() };
        }
        match wait {
            SvmQueueConditionalWait::Nowait => Err(SvmMsgQError::Empty),
            SvmQueueConditionalWait::Wait => {
                self.wait(SvmMsgQWaitType::Empty)?;
                unsafe { self.sub_raw() }
            }
            SvmQueueConditionalWait::TimedWait(timeout) => {
                if self.timedwait(SvmMsgQWaitType::Empty, timeout)? == WaitOutcome::TimedOut {
                    Err(SvmMsgQError::Timeout)
                } else {
                    unsafe { self.sub_raw() }
                }
            }
        }
    }

    pub unsafe fn sub_raw(&self) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        if self.is_empty() {
            return Err(SvmMsgQError::Empty);
        }
        let shared = unsafe { &mut (*self.shared.as_ptr()).q };
        let descriptor = unsafe { *self.descriptors.as_ptr().add(shared.head as usize) };
        shared.head = next_index(shared.head, shared.maxsize);
        let was_full = shared.cursize == shared.maxsize;
        shared.cursize -= 1;
        if was_full {
            self.signal("sub")?;
        }
        Ok(descriptor)
    }

    /// Returns the slot pointer for a descriptor.
    ///
    /// # Safety
    /// The caller must hold the descriptor's ownership and must not create
    /// overlapping mutable borrows for the same slot.
    pub unsafe fn msg_data(&self, msg: SvmMsgQDescriptor) -> Result<*mut u8, SvmMsgQError> {
        let parts = self.validate_descriptor(msg)?;
        let ring = self.ring(parts.ring_index)?;
        let shared = unsafe { &*ring.shared.as_ptr() };
        let offset = (parts.elt_index as usize)
            .checked_mul(shared.elsize as usize)
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        Ok(unsafe { ring.data.as_ptr().add(offset) })
    }

    pub fn free_msg(&self, msg: SvmMsgQDescriptor) -> Result<(), SvmMsgQError> {
        let parts = self.validate_descriptor(msg)?;
        let ring = self.ring(parts.ring_index)?;
        let shared = unsafe { &mut *ring.shared.as_ptr() };
        if parts.elt_index != shared.head || shared.cursize == 0 {
            return Err(SvmMsgQError::DescriptorNotAllocated);
        }
        let was_full = shared.cursize == shared.nitems;
        shared.head = next_index(shared.head, shared.nitems);
        shared.cursize -= 1;
        if was_full {
            self.signal("free_msg")?;
        }
        Ok(())
    }

    pub fn wait(&self, wait_type: SvmMsgQWaitType) -> Result<(), SvmMsgQError> {
        if self.event_fd.is_some() {
            while self.wait_predicate(wait_type) {
                self.read_event_fd()?;
            }
            return Ok(());
        }
        let mut guard = self.lock_shared()?;
        while self.wait_predicate(wait_type) {
            match unsafe { self.condvar.wait(&mut guard) } {
                Ok(()) => {}
                Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => {
                    guard
                        .mark_consistent()
                        .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                    return Err(SvmMsgQError::OwnerDied);
                }
                Err(source) => return Err(SvmMsgQError::Wait { source }),
            }
        }
        Ok(())
    }

    pub fn wait_prod(&self) -> Result<(), SvmMsgQError> {
        self.wait(SvmMsgQWaitType::Full)
    }

    pub fn or_ring_wait_prod(&self, ring_index: u32) -> Result<(), SvmMsgQError> {
        self.ring(ring_index)?;
        if self.event_fd.is_some() {
            while self.is_full() || self.ring_is_full(ring_index)? {
                self.read_event_fd()?;
            }
            return Ok(());
        }
        let mut guard = self.lock_shared()?;
        while self.is_full() || self.ring_is_full(ring_index)? {
            match unsafe { self.condvar.wait(&mut guard) } {
                Ok(()) => {}
                Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => {
                    guard
                        .mark_consistent()
                        .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                    return Err(SvmMsgQError::OwnerDied);
                }
                Err(source) => return Err(SvmMsgQError::Wait { source }),
            }
        }
        Ok(())
    }

    pub fn timedwait(
        &self,
        wait_type: SvmMsgQWaitType,
        timeout: Duration,
    ) -> Result<WaitOutcome, SvmMsgQError> {
        if self.event_fd.is_some() {
            if !self.wait_predicate(wait_type) {
                return Ok(WaitOutcome::Notified);
            }
            self.read_event_fd_timeout(timeout)?;
            return Ok(WaitOutcome::Notified);
        }
        let mut guard = self.lock_shared()?;
        if !self.wait_predicate(wait_type) {
            return Ok(WaitOutcome::Notified);
        }
        unsafe { self.condvar.wait_for(&mut guard, timeout) }
            .map_err(|source| SvmMsgQError::Wait { source })
    }

    pub fn set_eventfd(&mut self, event_fd: OwnedFd) {
        self.event_fd = Some(event_fd);
    }

    pub fn alloc_eventfd(&mut self) -> Result<(), SvmMsgQError> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(SvmMsgQError::Io {
                operation: "alloc_eventfd",
                source: io::Error::last_os_error(),
            });
        }
        self.set_eventfd(unsafe { OwnedFd::from_raw_fd(fd) });
        Ok(())
    }

    fn add_locked(
        &self,
        parts: SvmMsgQDescriptorParts,
        _guard: SpinLockGuard<'_, ()>,
        nowait: bool,
    ) -> Result<(), SvmMsgQError> {
        while self.is_full() {
            if nowait {
                return Err(SvmMsgQError::QueueFull);
            }
            self.wait_prod()?;
        }
        unsafe { self.add_raw_parts(parts) }
    }

    fn add_locked_shared(
        &self,
        parts: SvmMsgQDescriptorParts,
        mut guard: StandardGuard<'_>,
        nowait: bool,
    ) -> Result<(), SvmMsgQError> {
        while self.is_full() {
            if nowait {
                return Err(SvmMsgQError::QueueFull);
            }
            match unsafe { self.condvar.wait(&mut guard) } {
                Ok(()) => {}
                Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => {
                    guard
                        .mark_consistent()
                        .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                    return Err(SvmMsgQError::OwnerDied);
                }
                Err(source) => return Err(SvmMsgQError::Wait { source }),
            }
        }
        unsafe { self.add_raw_parts(parts) }
    }

    unsafe fn add_raw_parts(&self, parts: SvmMsgQDescriptorParts) -> Result<(), SvmMsgQError> {
        let queue = unsafe { &mut (*self.shared.as_ptr()).q };
        unsafe {
            *self.descriptors.as_ptr().add(queue.tail as usize) =
                SvmMsgQDescriptor::new(parts.ring_index, parts.elt_index);
        }
        queue.tail = next_index(queue.tail, queue.maxsize);
        let was_empty = queue.cursize == 0;
        queue.cursize += 1;
        if was_empty {
            self.signal("add")?;
        }
        Ok(())
    }

    fn validate_descriptor(
        &self,
        descriptor: SvmMsgQDescriptor,
    ) -> Result<SvmMsgQDescriptorParts, SvmMsgQError> {
        if descriptor.is_invalid() {
            return Err(SvmMsgQError::InvalidDescriptor);
        }
        let parts = descriptor.parts();
        let ring = self.ring(parts.ring_index)?;
        let shared = unsafe { &*ring.shared.as_ptr() };
        if parts.elt_index >= shared.nitems {
            return Err(SvmMsgQError::InvalidDescriptor);
        }
        let distance = (parts.elt_index + shared.nitems - shared.head) % shared.nitems;
        let span = if shared.tail == shared.head {
            if shared.cursize == 0 {
                0
            } else {
                shared.nitems
            }
        } else {
            (shared.tail + shared.nitems - shared.head) % shared.nitems
        };
        if distance >= span {
            return Err(SvmMsgQError::DescriptorNotAllocated);
        }
        Ok(parts)
    }

    fn ring(&self, ring_index: u32) -> Result<&SvmMsgQRing, SvmMsgQError> {
        self.rings
            .get(ring_index as usize)
            .ok_or(SvmMsgQError::InvalidRing { ring: ring_index })
    }

    fn lock_shared(&self) -> Result<StandardGuard<'_>, SvmMsgQError> {
        match unsafe { self.mutex.lock() } {
            Ok(RobustGuardContainer::Standard(guard)) => Ok(guard),
            Ok(RobustGuardContainer::Indeterminate(guard)) => {
                guard
                    .make_consistent()
                    .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                Err(SvmMsgQError::OwnerDied)
            }
            Err(source) => Err(SvmMsgQError::Lock { source }),
        }
    }

    fn try_lock_shared(&self) -> Result<Option<StandardGuard<'_>>, SvmMsgQError> {
        let result =
            unsafe { self.mutex.try_lock() }.map_err(|source| SvmMsgQError::Lock { source })?;
        let Some(container) = result else {
            return Ok(None);
        };
        match container {
            RobustGuardContainer::Standard(guard) => Ok(Some(guard)),
            RobustGuardContainer::Indeterminate(guard) => {
                guard
                    .make_consistent()
                    .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                Err(SvmMsgQError::OwnerDied)
            }
        }
    }

    fn signal(&self, operation: &'static str) -> Result<(), SvmMsgQError> {
        if let Some(event_fd) = &self.event_fd {
            let value = 1_u64.to_ne_bytes();
            let written =
                unsafe { libc::write(event_fd.as_raw_fd(), value.as_ptr().cast(), value.len()) };
            if written != value.len() as isize {
                return Err(SvmMsgQError::EventSignalAfterCommit {
                    operation,
                    source: io::Error::last_os_error(),
                });
            }
        } else {
            unsafe { self.condvar.notify_all() }
                .map_err(|source| SvmMsgQError::SignalAfterCommit { operation, source })?;
        }
        Ok(())
    }

    fn wait_predicate(&self, wait_type: SvmMsgQWaitType) -> bool {
        match wait_type {
            SvmMsgQWaitType::Empty => self.is_empty(),
            SvmMsgQWaitType::Full => self.is_full(),
        }
    }

    fn read_event_fd(&self) -> Result<(), SvmMsgQError> {
        let event_fd = self.event_fd.as_ref().expect("event fd predicate checked");
        let mut value = 0_u64;
        loop {
            let read = unsafe {
                libc::read(
                    event_fd.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    size_of::<u64>(),
                )
            };
            if read == size_of::<u64>() as isize {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                std::thread::yield_now();
                continue;
            }
            return Err(SvmMsgQError::Io {
                operation: "read_eventfd",
                source: error,
            });
        }
    }

    fn read_event_fd_timeout(&self, timeout: Duration) -> Result<(), SvmMsgQError> {
        let event_fd = self.event_fd.as_ref().expect("event fd predicate checked");
        let mut poll_fd = libc::pollfd {
            fd: event_fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let result = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
        if result == 0 {
            return Err(SvmMsgQError::Timeout);
        }
        if result < 0 {
            return Err(SvmMsgQError::Io {
                operation: "poll_eventfd",
                source: io::Error::last_os_error(),
            });
        }
        self.read_event_fd()
    }

    fn alloc_msg_w_ring_unchecked(
        &self,
        ring_index: u32,
        shared: &mut SvmMsgQRingShared,
    ) -> SvmMsgQDescriptor {
        let elt_index = shared.tail;
        shared.tail = next_index(shared.tail, shared.nitems);
        shared.cursize += 1;
        SvmMsgQDescriptor::new(ring_index, elt_index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmMsgQWaitType {
    Empty,
    Full,
}

fn validate_config(config: &SvmMsgQConfig<'_, '_>) -> Result<(), SvmMsgQError> {
    if config.q_nitems == 0 {
        return Err(SvmMsgQError::ZeroQueueCapacity);
    }
    if config.ring_cfgs.is_empty() {
        return Err(SvmMsgQError::NoRings);
    }
    for (index, ring) in config.ring_cfgs.iter().enumerate() {
        if ring.nitems == 0 {
            return Err(SvmMsgQError::ZeroRingCapacity { ring: index as u32 });
        }
        if ring.elsize == 0 {
            return Err(SvmMsgQError::ZeroElementSize { ring: index as u32 });
        }
        validate_ring_data(index as u32, ring.nitems, ring.elsize, ring.data)?;
    }
    Ok(())
}

fn validate_ring_data(
    ring: u32,
    nitems: u32,
    elsize: u32,
    data: &[u8],
) -> Result<(), SvmMsgQError> {
    let expected = (nitems as usize)
        .checked_mul(elsize as usize)
        .ok_or(SvmMsgQError::LayoutOverflow)?;
    if data.len() != expected {
        return Err(SvmMsgQError::RingDataLength {
            ring,
            expected,
            actual: data.len(),
        });
    }
    Ok(())
}

fn shared_layout(
    shared: NonNull<SvmMsgQShared>,
    _ring_count: usize,
    queue_capacity: u32,
) -> (NonNull<SvmMsgQDescriptor>, NonNull<SvmMsgQRingShared>) {
    let descriptor_ptr = unsafe {
        shared
            .as_ptr()
            .cast::<u8>()
            .add(size_of::<SvmMsgQShared>())
            .cast::<SvmMsgQDescriptor>()
    };
    let rings_ptr = unsafe {
        descriptor_ptr
            .cast::<u8>()
            .add(queue_capacity as usize * size_of::<SvmMsgQDescriptor>())
            .cast::<SvmMsgQRingShared>()
    };
    (
        NonNull::new(descriptor_ptr).expect("descriptor pointer is non-null"),
        NonNull::new(rings_ptr).expect("ring pointer is non-null"),
    )
}

fn ring_handles<'data>(
    rings: NonNull<SvmMsgQRingShared>,
    configs: &[SvmMsgQRingConfig<'data>],
) -> Vec<SvmMsgQRing> {
    configs
        .iter()
        .enumerate()
        .map(|(index, config)| SvmMsgQRing {
            shared: unsafe { NonNull::new_unchecked(rings.as_ptr().add(index)) },
            data: NonNull::new(config.data.as_ptr().cast_mut()).expect("ring data is non-null"),
        })
        .collect()
}

fn next_index(index: u32, capacity: u32) -> u32 {
    if index + 1 == capacity { 0 } else { index + 1 }
}
