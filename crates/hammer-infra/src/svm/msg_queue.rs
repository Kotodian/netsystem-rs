//! VPP-style shared-memory multi-ring message queue.
//!
//! The descriptor queue and ring accounting live in the shared mapping. Ring
//! Descriptor storage, ring headers, and ring bytes are laid out contiguously in
//! the caller-provided shared allocation, matching VPP's `svm_msg_q_init`.

use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
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
use crate::sync::{SpinLock, SpinLockGuard};

const CACHE_LINE_BYTES: usize = 64;
static SVM_MSG_Q_MAPPING_ANCHOR: () = ();

#[repr(C)]
pub union SvmMsgQDescriptor {
    words: [u32; 2],
    pub as_u64: u64,
}

impl SvmMsgQDescriptor {
    pub const INVALID: Self = Self { as_u64: u64::MAX };

    pub const fn new(ring_index: u32, elt_index: u32) -> Self {
        Self {
            words: [ring_index, elt_index],
        }
    }

    pub fn ring_index(self) -> u32 {
        unsafe { self.words[0] }
    }

    pub fn elt_index(self) -> u32 {
        unsafe { self.words[1] }
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
            .field("ring_index", &self.ring_index())
            .field("elt_index", &self.elt_index())
            .finish()
    }
}

impl PartialEq for SvmMsgQDescriptor {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.as_u64 == other.as_u64 }
    }
}

impl Eq for SvmMsgQDescriptor {}

/// Configuration for one inline VPP message ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmMsgQRingConfig {
    pub nitems: u32,
    pub elsize: u32,
}

impl SvmMsgQRingConfig {
    pub const fn new(nitems: u32, elsize: u32) -> Self {
        Self { nitems, elsize }
    }
}

pub struct SvmMsgQConfig<'rings> {
    pub consumer_pid: i32,
    pub q_nitems: u32,
    pub rings: &'rings [SvmMsgQRingConfig],
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
    #[error("message queue layout is too large")]
    LayoutOverflow,
    #[error("message descriptor queue is full")]
    QueueFull,
    #[error("message ring {ring} is full")]
    RingFull { ring: u32 },
    #[error("message size {requested} does not fit any ring")]
    MessageTooLarge { requested: usize },
    #[error("message ring {ring} does not exist")]
    InvalidRing { ring: u32 },
    #[error("message descriptor ring {ring} element {element} is invalid")]
    InvalidDescriptor { ring: u32, element: u32 },
    #[error("message descriptor ring {ring} element {element} is not allocated")]
    DescriptorNotAllocated { ring: u32, element: u32 },
    #[error("message element size mismatch: requested {requested} bytes, stored {stored} bytes")]
    ElementSizeMismatch { requested: usize, stored: usize },
    #[error("message queue is empty")]
    Empty,
    #[error("message queue lock is busy")]
    LockBusy,
    #[error("message queue wait deadline expired")]
    Timeout,
    #[error("message queue header is invalid")]
    InvalidHeader,
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
    pad: u32,
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
    nitems: u32,
    elsize: u32,
    shared: NonNull<SvmMsgQRingShared>,
    data: NonNull<u8>,
}

struct SvmMsgQQueue {
    shared: NonNull<SvmMsgQSharedQueue>,
    event_fd: Option<OwnedFd>,
    lock: SpinLock<SvmMsgQProducerState>,
}

struct SvmMsgQProducerState {
    shared: NonNull<SvmMsgQSharedQueue>,
}

pub struct SvmMsgQ {
    descriptors: NonNull<SvmMsgQDescriptor>,
    rings: Vec<SvmMsgQRing>,
    queue: SvmMsgQQueue,
    mutex: BorrowedMutex<'static, Robust>,
    condvar: BorrowedCondvar<'static>,
}

unsafe impl Send for SvmMsgQ {}
unsafe impl Sync for SvmMsgQ {}

enum SvmMsgQProducerLock<'queue> {
    Shared(StandardGuard<'queue>),
    Event(SpinLockGuard<'queue, SvmMsgQProducerState>),
}

pub struct SvmMsgQProducerGuard<'queue> {
    queue: &'queue SvmMsgQ,
    lock: SvmMsgQProducerLock<'queue>,
    wait: SvmQueueConditionalWait,
}

impl SvmMsgQ {
    pub fn size_to_alloc(config: &SvmMsgQConfig<'_>) -> Result<usize, SvmMsgQError> {
        validate_config(config)?;
        let descriptors = (config.q_nitems as usize)
            .checked_mul(size_of::<SvmMsgQDescriptor>())
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        let ring_bytes = config.rings.iter().try_fold(0_usize, |size, ring| {
            let data = (ring.nitems as usize)
                .checked_mul(ring.elsize as usize)
                .ok_or(SvmMsgQError::LayoutOverflow)?;
            size.checked_add(size_of::<SvmMsgQRingShared>())
                .and_then(|size| size.checked_add(data))
                .ok_or(SvmMsgQError::LayoutOverflow)
        })?;
        size_of::<SvmMsgQShared>()
            .checked_add(descriptors)
            .and_then(|size| size.checked_add(ring_bytes))
            .ok_or(SvmMsgQError::LayoutOverflow)
    }

    /// Initializes a queue in a caller-owned, cache-line-aligned shared range.
    ///
    /// The caller must reserve at least [`Self::size_to_alloc`] bytes at `base`.
    pub unsafe fn init(
        base: NonNull<u8>,
        config: &SvmMsgQConfig<'_>,
    ) -> Result<Self, SvmMsgQError> {
        assert!(
            base.as_ptr().addr().is_multiple_of(CACHE_LINE_BYTES),
            "message queue base is cache-line aligned"
        );
        let size = Self::size_to_alloc(config)?;
        let shared = base.cast::<SvmMsgQShared>();
        unsafe { std::ptr::write_bytes(shared.as_ptr().cast::<u8>(), 0, size) };
        unsafe {
            (*shared.as_ptr()).n_rings = config.rings.len() as u32;
            (*shared.as_ptr()).q.maxsize = config.q_nitems;
            (*shared.as_ptr()).q.elsize = size_of::<SvmMsgQDescriptor>() as u32;
            (*shared.as_ptr()).q.pad = 0;
        }
        let (descriptors, rings) = shared_layout(shared, config.q_nitems);
        let mut ring = rings;
        for config_ring in config.rings.iter().copied() {
            let ring_header = unsafe { &mut *ring.as_ptr() };
            ring_header.nitems = config_ring.nitems;
            ring_header.elsize = config_ring.elsize;
            ring = unsafe {
                ring.cast::<u8>()
                    .add(size_of::<SvmMsgQRingShared>())
                    .add(config_ring.nitems as usize * config_ring.elsize as usize)
                    .cast()
            };
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
            descriptors,
            rings: ring_handles(rings, config.rings.len()),
            queue: SvmMsgQQueue {
                shared: unsafe {
                    NonNull::new_unchecked(std::ptr::addr_of_mut!((*shared.as_ptr()).q))
                },
                event_fd: None,
                lock: SpinLock::new(SvmMsgQProducerState {
                    shared: unsafe {
                        NonNull::new_unchecked(std::ptr::addr_of_mut!((*shared.as_ptr()).q))
                    },
                }),
            },
            mutex,
            condvar,
        })
    }

    /// Attaches queue metadata and inline ring storage from its shared base.
    /// The enclosing mapping owner validates the range before calling this.
    pub unsafe fn attach(base: NonNull<u8>) -> Result<Self, SvmMsgQError> {
        let header = base.cast::<SvmMsgQShared>();
        let shared_ref = unsafe { header.as_ref() };
        if shared_ref.n_rings == 0
            || shared_ref.q.maxsize == 0
            || shared_ref.q.elsize as usize != size_of::<SvmMsgQDescriptor>()
            || shared_ref.q.head >= shared_ref.q.maxsize
            || shared_ref.q.tail >= shared_ref.q.maxsize
            || shared_ref.q.cursize > shared_ref.q.maxsize
        {
            return Err(SvmMsgQError::InvalidHeader);
        }
        let (descriptors, rings) = shared_layout(header, shared_ref.q.maxsize);
        let mut ring = rings;
        for _ in 0..shared_ref.n_rings {
            let ring_header = unsafe { &*ring.as_ptr() };
            if ring_header.nitems == 0 || ring_header.elsize == 0 {
                return Err(SvmMsgQError::InvalidHeader);
            }
            ring = unsafe {
                ring.cast::<u8>()
                    .add(size_of::<SvmMsgQRingShared>())
                    .add(ring_header.nitems as usize * ring_header.elsize as usize)
                    .cast()
            };
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
            descriptors,
            rings: ring_handles(rings, shared_ref.n_rings as usize),
            queue: SvmMsgQQueue {
                shared: unsafe {
                    NonNull::new_unchecked(std::ptr::addr_of_mut!((*header.as_ptr()).q))
                },
                event_fd: None,
                lock: SpinLock::new(SvmMsgQProducerState {
                    shared: unsafe {
                        NonNull::new_unchecked(std::ptr::addr_of_mut!((*header.as_ptr()).q))
                    },
                }),
            },
            mutex,
            condvar,
        })
    }

    pub fn size(&self) -> u32 {
        unsafe { (*self.queue.shared.as_ptr()).cursize }
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn is_full(&self) -> bool {
        unsafe { self.size() == (*self.queue.shared.as_ptr()).maxsize }
    }

    pub fn ring_is_full(&self, ring_index: u32) -> Result<bool, SvmMsgQError> {
        let ring = self.ring(ring_index)?;
        Ok(unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems })
    }

    pub fn ring_size(&self, ring_index: u32) -> Result<u32, SvmMsgQError> {
        let ring = self.ring(ring_index)?;
        Ok(unsafe { (*ring.shared.as_ptr()).cursize })
    }

    pub fn producer(
        &self,
        wait: SvmQueueConditionalWait,
    ) -> Result<SvmMsgQProducerGuard<'_>, SvmMsgQError> {
        let lock = if self.queue.event_fd.is_some() {
            let lock = match wait {
                SvmQueueConditionalWait::Nowait => {
                    self.queue.lock.try_lock().ok_or(SvmMsgQError::LockBusy)?
                }
                SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => {
                    self.queue.lock.lock()
                }
            };
            SvmMsgQProducerLock::Event(lock)
        } else {
            SvmMsgQProducerLock::Shared(match wait {
                SvmQueueConditionalWait::Nowait => {
                    self.try_lock_shared()?.ok_or(SvmMsgQError::LockBusy)?
                }
                SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => {
                    self.lock_shared()?
                }
            })
        };
        Ok(SvmMsgQProducerGuard {
            queue: self,
            lock,
            wait,
        })
    }

    pub fn sub(&self, wait: SvmQueueConditionalWait) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        if !self.is_empty() {
            return self.sub_raw();
        }
        match wait {
            SvmQueueConditionalWait::Nowait => Err(SvmMsgQError::Empty),
            SvmQueueConditionalWait::Wait => {
                self.wait(SvmMsgQWaitType::Empty)?;
                self.sub_raw()
            }
            SvmQueueConditionalWait::TimedWait(timeout) => {
                if self.timed_wait(SvmMsgQWaitType::Empty, timeout)? == WaitOutcome::TimedOut {
                    Err(SvmMsgQError::Timeout)
                } else {
                    self.sub_raw()
                }
            }
        }
    }

    fn sub_raw(&self) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        if self.is_empty() {
            return Err(SvmMsgQError::Empty);
        }
        let shared = unsafe { &mut *self.queue.shared.as_ptr() };
        let descriptor = unsafe { *self.descriptors.as_ptr().add(shared.head as usize) };
        shared.head = next_index(shared.head, shared.maxsize);
        let was_full = shared.cursize == shared.maxsize;
        shared.cursize -= 1;
        if was_full {
            self.signal(true, "sub")?;
        }
        Ok(descriptor)
    }

    /// Returns the slot pointer for a descriptor.
    ///
    /// # Safety
    /// The caller must hold the descriptor's ownership and must not create
    /// overlapping mutable borrows for the same slot.
    pub(crate) unsafe fn message_data(
        &self,
        msg: SvmMsgQDescriptor,
    ) -> Result<NonNull<u8>, SvmMsgQError> {
        let (ring_index, elt_index) = self.validate_descriptor(msg)?;
        let ring = self.ring(ring_index)?;
        let offset = (elt_index as usize)
            .checked_mul(ring.elsize as usize)
            .ok_or(SvmMsgQError::LayoutOverflow)?;
        Ok(unsafe { NonNull::new_unchecked(ring.data.as_ptr().add(offset)) })
    }

    pub fn free_msg(&self, msg: SvmMsgQDescriptor) -> Result<(), SvmMsgQError> {
        let (ring_index, elt_index) = self.validate_descriptor(msg)?;
        let ring = self.ring(ring_index)?;
        let shared = unsafe { &mut *ring.shared.as_ptr() };
        if elt_index != shared.head || shared.cursize == 0 {
            return Err(SvmMsgQError::DescriptorNotAllocated {
                ring: ring_index,
                element: elt_index,
            });
        }
        let was_full = shared.cursize == ring.nitems;
        shared.head = next_index(shared.head, ring.nitems);
        shared.cursize -= 1;
        if was_full {
            self.signal(true, "free_msg")?;
        }
        Ok(())
    }

    pub fn wait_empty(&self) -> Result<(), SvmMsgQError> {
        self.wait(SvmMsgQWaitType::Empty)
    }

    pub fn wait_full(&self) -> Result<(), SvmMsgQError> {
        self.wait(SvmMsgQWaitType::Full)
    }

    fn wait(&self, wait_type: SvmMsgQWaitType) -> Result<(), SvmMsgQError> {
        if self.queue.event_fd.is_some() {
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

    pub fn timed_wait_full(&self, timeout: Duration) -> Result<WaitOutcome, SvmMsgQError> {
        self.timed_wait(SvmMsgQWaitType::Full, timeout)
    }

    pub fn timed_wait_empty(&self, timeout: Duration) -> Result<WaitOutcome, SvmMsgQError> {
        self.timed_wait(SvmMsgQWaitType::Empty, timeout)
    }

    fn timed_wait(
        &self,
        wait_type: SvmMsgQWaitType,
        timeout: Duration,
    ) -> Result<WaitOutcome, SvmMsgQError> {
        if self.queue.event_fd.is_some() {
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

    pub fn install_eventfd(&mut self, event_fd: OwnedFd) {
        self.queue.event_fd = Some(event_fd);
    }

    pub fn allocate_eventfd(&mut self) -> Result<BorrowedFd<'_>, SvmMsgQError> {
        let fd = unsafe { libc::eventfd(0, 0) };
        if fd < 0 {
            return Err(SvmMsgQError::Io {
                operation: "alloc_eventfd",
                source: io::Error::last_os_error(),
            });
        }
        self.install_eventfd(unsafe { OwnedFd::from_raw_fd(fd) });
        Ok(self
            .queue
            .event_fd
            .as_ref()
            .expect("eventfd installed")
            .as_fd())
    }

    pub fn cleanup(&mut self) {
        self.queue.event_fd.take();
    }

    unsafe fn add_raw_at_shared(
        &self,
        shared: NonNull<SvmMsgQSharedQueue>,
        ring_index: u32,
        elt_index: u32,
    ) -> Result<(), SvmMsgQError> {
        let queue = unsafe { &mut *shared.as_ptr() };
        unsafe {
            *self.descriptors.as_ptr().add(queue.tail as usize) =
                SvmMsgQDescriptor::new(ring_index, elt_index);
        }
        queue.tail = next_index(queue.tail, queue.maxsize);
        let was_empty = queue.cursize == 0;
        queue.cursize += 1;
        if was_empty {
            self.signal(false, "add")?;
        }
        Ok(())
    }

    fn validate_descriptor(
        &self,
        descriptor: SvmMsgQDescriptor,
    ) -> Result<(u32, u32), SvmMsgQError> {
        if descriptor.is_invalid() {
            return Err(SvmMsgQError::InvalidDescriptor {
                ring: descriptor.ring_index(),
                element: descriptor.elt_index(),
            });
        }
        let ring_index = descriptor.ring_index();
        let elt_index = descriptor.elt_index();
        let ring = self.ring(ring_index)?;
        let shared = unsafe { &*ring.shared.as_ptr() };
        if elt_index >= ring.nitems {
            return Err(SvmMsgQError::InvalidDescriptor {
                ring: ring_index,
                element: elt_index,
            });
        }
        let distance = (elt_index + ring.nitems - shared.head) % ring.nitems;
        let span = if shared.tail == shared.head {
            if shared.cursize == 0 { 0 } else { ring.nitems }
        } else {
            (shared.tail + ring.nitems - shared.head) % ring.nitems
        };
        if distance >= span {
            return Err(SvmMsgQError::DescriptorNotAllocated {
                ring: ring_index,
                element: elt_index,
            });
        }
        Ok((ring_index, elt_index))
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

    fn signal(&self, is_consumer: bool, operation: &'static str) -> Result<(), SvmMsgQError> {
        if let Some(event_fd) = &self.queue.event_fd {
            let value = 1_u64.to_ne_bytes();
            let written =
                unsafe { libc::write(event_fd.as_raw_fd(), value.as_ptr().cast(), value.len()) };
            if written != value.len() as isize {
                return Err(SvmMsgQError::EventSignalAfterCommit {
                    operation,
                    source: io::Error::last_os_error(),
                });
            }
        } else if is_consumer {
            let guard = self.lock_shared()?;
            unsafe { self.condvar.notify_all() }
                .map_err(|source| SvmMsgQError::SignalAfterCommit { operation, source })?;
            drop(guard);
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
        let event_fd = self
            .queue
            .event_fd
            .as_ref()
            .expect("event fd predicate checked");
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
        let event_fd = self
            .queue
            .event_fd
            .as_ref()
            .expect("event fd predicate checked");
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

impl SvmMsgQProducerGuard<'_> {
    pub fn alloc_msg(&mut self, nbytes: usize) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        if nbytes
            > self
                .queue
                .rings
                .iter()
                .map(|ring| ring.elsize as usize)
                .max()
                .unwrap_or(0)
        {
            return Err(SvmMsgQError::MessageTooLarge { requested: nbytes });
        }
        loop {
            if let Some((ring_index, ring)) =
                self.queue.rings.iter().enumerate().find(|(_, ring)| {
                    ring.elsize as usize >= nbytes
                        && unsafe { (*ring.shared.as_ptr()).cursize < ring.nitems }
                })
            {
                let shared = unsafe { &mut *ring.shared.as_ptr() };
                return Ok(self
                    .queue
                    .alloc_msg_w_ring_unchecked(ring_index as u32, shared));
            }
            self.wait_for_space(None)?;
        }
    }

    pub fn alloc_msg_on_ring(
        &mut self,
        ring_index: u32,
    ) -> Result<SvmMsgQDescriptor, SvmMsgQError> {
        let ring = self.queue.ring(ring_index)?;
        loop {
            let queue_full = self.queue.is_full();
            let ring_full = unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems };
            if !queue_full && !ring_full {
                let shared = unsafe { &mut *ring.shared.as_ptr() };
                return Ok(self.queue.alloc_msg_w_ring_unchecked(ring_index, shared));
            }
            self.wait_for_space(Some(ring_index))?;
        }
    }

    pub fn write<T: SvmMsgQElement>(
        &mut self,
        message: SvmMsgQDescriptor,
        value: &T,
    ) -> Result<(), SvmMsgQError> {
        let (ring_index, _) = self.queue.validate_descriptor(message)?;
        let stored = self.queue.ring(ring_index)?.elsize as usize;
        if size_of::<T>() != stored {
            return Err(SvmMsgQError::ElementSizeMismatch {
                requested: size_of::<T>(),
                stored,
            });
        }
        let target = unsafe { self.queue.message_data(message)? };
        unsafe {
            std::ptr::copy_nonoverlapping(value.as_bytes().as_ptr(), target.as_ptr(), stored);
        }
        Ok(())
    }

    pub fn add(&mut self, message: SvmMsgQDescriptor) -> Result<(), SvmMsgQError> {
        let (ring_index, element) = self.queue.validate_descriptor(message)?;
        if self.queue.is_full() {
            return Err(SvmMsgQError::QueueFull);
        }
        let shared = match &self.lock {
            SvmMsgQProducerLock::Shared(_) => self.queue.queue.shared,
            SvmMsgQProducerLock::Event(state) => state.shared,
        };
        unsafe { self.queue.add_raw_at_shared(shared, ring_index, element) }
    }

    fn wait_for_space(&mut self, ring_index: Option<u32>) -> Result<(), SvmMsgQError> {
        match &mut self.lock {
            SvmMsgQProducerLock::Event(state) => {
                let shared = unsafe { state.shared.as_ref() };
                let queue_full = shared.cursize == shared.maxsize;
                let ring_full = match ring_index {
                    Some(ring) => {
                        let ring = self.queue.ring(ring).expect("validated ring index");
                        unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems }
                    }
                    None => self
                        .queue
                        .rings
                        .iter()
                        .all(|ring| unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems }),
                };
                let full = ring_full || (ring_index.is_some() && queue_full);
                if !full {
                    return Ok(());
                }
                match self.wait {
                    SvmQueueConditionalWait::Nowait => Err(match ring_index {
                        Some(ring) => SvmMsgQError::RingFull { ring },
                        None => SvmMsgQError::QueueFull,
                    }),
                    SvmQueueConditionalWait::Wait => loop {
                        let queue_full = unsafe { state.shared.as_ref().cursize }
                            == unsafe { state.shared.as_ref().maxsize };
                        let ring_full = match ring_index {
                            Some(ring) => {
                                let ring = self.queue.ring(ring).expect("validated ring index");
                                unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems }
                            }
                            None => self.queue.rings.iter().all(|ring| unsafe {
                                (*ring.shared.as_ptr()).cursize >= ring.nitems
                            }),
                        };
                        if !ring_full && !(ring_index.is_some() && queue_full) {
                            return Ok(());
                        }
                        std::hint::spin_loop();
                    },
                    SvmQueueConditionalWait::TimedWait(timeout) => {
                        let deadline = std::time::Instant::now() + timeout;
                        loop {
                            let queue_full = unsafe { state.shared.as_ref().cursize }
                                == unsafe { state.shared.as_ref().maxsize };
                            let ring_full = match ring_index {
                                Some(ring) => {
                                    let ring = self.queue.ring(ring).expect("validated ring index");
                                    unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems }
                                }
                                None => self.queue.rings.iter().all(|ring| unsafe {
                                    (*ring.shared.as_ptr()).cursize >= ring.nitems
                                }),
                            };
                            if !ring_full && !(ring_index.is_some() && queue_full) {
                                return Ok(());
                            }
                            if std::time::Instant::now() >= deadline {
                                return Err(SvmMsgQError::Timeout);
                            }
                            std::hint::spin_loop();
                        }
                    }
                }
            }
            SvmMsgQProducerLock::Shared(guard) => loop {
                let ring_full = match ring_index {
                    Some(ring) => self.queue.ring_is_full(ring).unwrap_or(true),
                    None => self
                        .queue
                        .rings
                        .iter()
                        .all(|ring| unsafe { (*ring.shared.as_ptr()).cursize >= ring.nitems }),
                };
                let full = ring_full || (ring_index.is_some() && self.queue.is_full());
                if !full {
                    return Ok(());
                }
                match self.wait {
                    SvmQueueConditionalWait::Nowait => {
                        return Err(match ring_index {
                            Some(ring) => SvmMsgQError::RingFull { ring },
                            None => SvmMsgQError::QueueFull,
                        });
                    }
                    SvmQueueConditionalWait::Wait => {
                        match unsafe { self.queue.condvar.wait(guard) } {
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
                    SvmQueueConditionalWait::TimedWait(timeout) => {
                        match unsafe { self.queue.condvar.wait_for(guard, timeout) } {
                            Ok(WaitOutcome::Notified) => {}
                            Ok(WaitOutcome::TimedOut) => return Err(SvmMsgQError::Timeout),
                            Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => {
                                guard
                                    .mark_consistent()
                                    .map_err(|source| SvmMsgQError::NotRecoverable { source })?;
                                return Err(SvmMsgQError::OwnerDied);
                            }
                            Err(source) => return Err(SvmMsgQError::Wait { source }),
                        }
                    }
                }
            },
        }
    }
}

pub trait SvmMsgQElement:
    zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes
{
}

impl<T> SvmMsgQElement for T where
    T: zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes
{
}

impl SvmMsgQ {
    pub fn read<T: SvmMsgQElement>(&self, message: SvmMsgQDescriptor) -> Result<T, SvmMsgQError> {
        let (ring_index, _) = self.validate_descriptor(message)?;
        let stored = self.ring(ring_index)?.elsize as usize;
        if size_of::<T>() != stored {
            return Err(SvmMsgQError::ElementSizeMismatch {
                requested: size_of::<T>(),
                stored,
            });
        }
        let source = unsafe { self.message_data(message)? };
        let bytes = unsafe { std::slice::from_raw_parts(source.as_ptr(), stored) };
        T::read_from_bytes(bytes).map_err(|_| SvmMsgQError::InvalidHeader)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmMsgQWaitType {
    Empty,
    Full,
}

fn validate_config(config: &SvmMsgQConfig<'_>) -> Result<(), SvmMsgQError> {
    if config.q_nitems == 0 {
        return Err(SvmMsgQError::ZeroQueueCapacity);
    }
    if config.rings.is_empty() {
        return Err(SvmMsgQError::NoRings);
    }
    for (index, ring) in config.rings.iter().enumerate() {
        if ring.nitems == 0 {
            return Err(SvmMsgQError::ZeroRingCapacity { ring: index as u32 });
        }
        if ring.elsize == 0 {
            return Err(SvmMsgQError::ZeroElementSize { ring: index as u32 });
        }
    }
    Ok(())
}

fn shared_layout(
    shared: NonNull<SvmMsgQShared>,
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

fn ring_handles(rings: NonNull<SvmMsgQRingShared>, ring_count: usize) -> Vec<SvmMsgQRing> {
    let mut ring = rings;
    (0..ring_count)
        .map(|_| {
            let shared = ring.as_ptr();
            let header = unsafe { &*shared };
            let handle = SvmMsgQRing {
                nitems: header.nitems,
                elsize: header.elsize,
                shared: ring,
                data: unsafe {
                    NonNull::new_unchecked(shared.cast::<u8>().add(size_of::<SvmMsgQRingShared>()))
                },
            };
            ring = unsafe {
                ring.cast::<u8>()
                    .add(size_of::<SvmMsgQRingShared>())
                    .add(header.nitems as usize * header.elsize as usize)
                    .cast()
            };
            handle
        })
        .collect()
}

fn next_index(index: u32, capacity: u32) -> u32 {
    if index + 1 == capacity { 0 } else { index + 1 }
}
