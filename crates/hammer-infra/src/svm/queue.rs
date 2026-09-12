//! VPP-style fixed-size shared-memory queues.
//!
//! The queue owns only its shared header and synchronization state. Allocation
//! and mapping lifetime belong to the caller that owns the SVM region. The
//! constructors take a mapping only long enough to validate an offset and
//! resolve the queue pointer; the returned handle does not retain that mapping.

use std::alloc::{Layout, LayoutError};
use std::io;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
use std::os::fd::{AsRawFd, OwnedFd};
use std::ptr::NonNull;
use std::time::Duration;

use posix_sync::condvar::{
    BorrowedCondvar, CondvarBuilder, CondvarClock, CondvarSharing, RawCondvarAlloc, WaitOutcome,
};
use posix_sync::mutex::guards::{MutexGuard, RobustGuardContainer, StandardGuard};
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexSharing, RawMutexAlloc, robustness_markers::Robust,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::svm::ssvm::{SsvmError, SsvmPrivate};

const CACHE_LINE_BYTES: usize = 64;

// The mapping is owned by the caller. This anchor only supplies the lifetime
// required by posix-sync's borrowed process-shared primitives.
static SVM_QUEUE_MAPPING_ANCHOR: () = ();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmQueueConfig {
    pub capacity: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmQueueConditionalWait {
    Wait,
    Nowait,
    TimedWait(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmQueueOperation {
    Add,
    AddPair,
    Sub,
}

#[derive(Debug, thiserror::Error)]
pub enum SvmQueueError {
    #[error("queue capacity must be positive")]
    ZeroCapacity,
    #[error("queue element type must have non-zero size")]
    ZeroSizedElement,
    #[error("queue layout is too large")]
    LayoutOverflow,
    #[error("queue capacity is full")]
    QueueFull,
    #[error("queue is empty")]
    Empty,
    #[error("queue lock is busy")]
    LockBusy,
    #[error("queue wait deadline expired")]
    Timeout,
    #[error("queue header is invalid")]
    InvalidHeader,
    #[error("queue element size mismatch: requested {requested} bytes, stored {stored} bytes")]
    ElementSizeMismatch { requested: usize, stored: usize },
    #[error("queue offset {offset} is outside the segment")]
    InvalidOffset { offset: u64 },
    #[error("queue segment operation failed: {0}")]
    Segment(#[from] SsvmError),
    #[error("queue mutex owner died")]
    OwnerDied,
    #[error("queue mutex is not recoverable: {source}")]
    NotRecoverable {
        #[source]
        source: posix_sync::mutex::MutexLockError,
    },
    #[error("queue mutex lock failed: {source}")]
    Lock {
        #[source]
        source: posix_sync::mutex::MutexLockError,
    },
    #[error("queue condition wait failed: {source}")]
    Wait {
        #[source]
        source: posix_sync::condvar::CondvarWaitError,
    },
    #[error("queue condition signal failed after {operation:?} committed: {source}")]
    SignalAfterCommit {
        operation: SvmQueueOperation,
        #[source]
        source: posix_sync::condvar::CondvarSignalError,
    },
    #[error("queue event notification failed after {operation:?} committed: {source}")]
    EventSignalAfterCommit {
        operation: SvmQueueOperation,
        #[source]
        source: io::Error,
    },
    #[error("queue layout construction failed")]
    Layout(#[from] LayoutError),
}

/// Shared queue header. The element bytes begin immediately after this header.
#[repr(C, align(64))]
struct SvmQueueHeader {
    mutex: MaybeUninit<RawMutexAlloc>,
    condvar: MaybeUninit<RawCondvarAlloc>,
    head: u32,
    tail: u32,
    current_size: u32,
    max_size: u32,
    element_size: u32,
    consumer_pid: i32,
    producer_evtfd: i32,
    consumer_evtfd: i32,
}

/// A process-local pointer to a queue in shared memory. The caller that owns
/// the mapping and allocation must keep both alive until every queue handle is
/// dropped and the shared synchronization objects are destroyed.
pub struct SvmQueue<T> {
    header: NonNull<SvmQueueHeader>,
    mutex: BorrowedMutex<'static, Robust>,
    condvar: BorrowedCondvar<'static>,
    producer_evtfd: Option<OwnedFd>,
    consumer_evtfd: Option<OwnedFd>,
    _element: PhantomData<T>,
}

/// A queue mutex guard. Dropping it unlocks the shared mutex.
pub struct SvmQueueLock<'queue, T> {
    queue: &'queue SvmQueue<T>,
    guard: StandardGuard<'queue>,
}

unsafe impl<T: Send> Send for SvmQueue<T> {}
unsafe impl<T: Send + Sync> Sync for SvmQueue<T> {}

impl<T> SvmQueue<T>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn layout(config: &SvmQueueConfig) -> Result<Layout, SvmQueueError> {
        validate_config::<T>(config)?;
        let bytes = size_of::<SvmQueueHeader>()
            .checked_add(
                (config.capacity as usize)
                    .checked_mul(size_of::<T>())
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        Layout::from_size_align(bytes, CACHE_LINE_BYTES).map_err(SvmQueueError::Layout)
    }

    /// Initializes a queue at a caller-owned segment offset.
    ///
    /// # Safety
    /// The caller must own the allocation at `offset`, keep the mapping alive
    /// for every returned queue handle, and ensure no initialized queue already
    /// occupies the range.
    pub unsafe fn init_at(
        segment: &SsvmPrivate,
        offset: u64,
        config: &SvmQueueConfig,
        consumer_pid: i32,
    ) -> Result<Self, SvmQueueError> {
        let layout = Self::layout(config)?;
        let header_ptr = segment.offset_ptr(offset, layout.size(), layout.align())?;
        let header = header_ptr.cast::<SvmQueueHeader>();
        unsafe {
            std::ptr::write(
                header,
                SvmQueueHeader {
                    mutex: MaybeUninit::uninit(),
                    condvar: MaybeUninit::uninit(),
                    head: 0,
                    tail: 0,
                    current_size: 0,
                    max_size: config.capacity,
                    element_size: size_of::<T>() as u32,
                    consumer_pid,
                    producer_evtfd: -1,
                    consumer_evtfd: -1,
                },
            );
        }

        let mutex = unsafe {
            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed(
                    std::ptr::addr_of_mut!((*header).mutex).cast(),
                    &SVM_QUEUE_MAPPING_ANCHOR,
                )
        };
        let condvar = unsafe {
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed(
                    std::ptr::addr_of_mut!((*header).condvar).cast(),
                    &SVM_QUEUE_MAPPING_ANCHOR,
                )
        };
        Ok(Self {
            header: NonNull::new(header).expect("segment offset returned a null queue pointer"),
            mutex,
            condvar,
            producer_evtfd: None,
            consumer_evtfd: None,
            _element: PhantomData,
        })
    }

    /// Attaches to an initialized queue without reconstructing its allocator.
    ///
    /// # Safety
    /// The caller must keep the mapping and allocation alive for the returned
    /// handle and must attach only to a queue initialized by this layout.
    pub unsafe fn attach(segment: &SsvmPrivate, offset: u64) -> Result<Self, SvmQueueError> {
        let header_ptr =
            segment.offset_ptr(offset, size_of::<SvmQueueHeader>(), CACHE_LINE_BYTES)?;
        let header = header_ptr.cast::<SvmQueueHeader>();
        let stored = unsafe { &*header };
        if stored.max_size == 0 || stored.element_size == 0 {
            return Err(SvmQueueError::InvalidHeader);
        }
        if stored.head >= stored.max_size
            || stored.tail >= stored.max_size
            || stored.current_size > stored.max_size
        {
            return Err(SvmQueueError::InvalidHeader);
        }
        if align_of::<T>() > CACHE_LINE_BYTES {
            return Err(SvmQueueError::LayoutOverflow);
        }
        let bytes = size_of::<SvmQueueHeader>()
            .checked_add(
                (stored.max_size as usize)
                    .checked_mul(stored.element_size as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        segment.offset_ptr(offset, bytes, CACHE_LINE_BYTES)?;
        if stored.element_size as usize != size_of::<T>() {
            return Err(SvmQueueError::ElementSizeMismatch {
                requested: size_of::<T>(),
                stored: stored.element_size as usize,
            });
        }

        let mutex = unsafe {
            BorrowedMutex::<Robust>::from_raw(
                std::ptr::addr_of_mut!((*header).mutex).cast(),
                &SVM_QUEUE_MAPPING_ANCHOR,
            )
        };
        let condvar = unsafe {
            BorrowedCondvar::from_raw(
                std::ptr::addr_of_mut!((*header).condvar).cast(),
                &SVM_QUEUE_MAPPING_ANCHOR,
                CondvarClock::Monotonic,
            )
        };
        Ok(Self {
            header: NonNull::new(header).expect("segment offset returned a null queue pointer"),
            mutex,
            condvar,
            producer_evtfd: None,
            consumer_evtfd: None,
            _element: PhantomData,
        })
    }

    pub fn capacity(&self) -> usize {
        self.header().max_size as usize
    }

    pub fn max_size(&self) -> usize {
        self.capacity()
    }

    pub fn element_size(&self) -> usize {
        self.header().element_size as usize
    }

    pub fn consumer_pid(&self) -> i32 {
        self.header().consumer_pid
    }

    pub fn add(&self, value: T, nowait: bool) -> Result<(), SvmQueueError> {
        let mut lock = if nowait {
            self.try_lock()?
        } else {
            self.lock()?
        };
        if nowait && lock.queue.header().current_size == lock.queue.header().max_size {
            return Err(SvmQueueError::QueueFull);
        }
        lock.add_nolock(value)
    }

    pub fn add2(&self, first: T, second: T, nowait: bool) -> Result<(), SvmQueueError> {
        let mut lock = if nowait {
            self.try_lock()?
        } else {
            self.lock()?
        };
        if nowait && lock.queue.header().max_size - lock.queue.header().current_size < 2 {
            return Err(SvmQueueError::QueueFull);
        }
        lock.add_pair_nolock(first, second)
    }

    pub fn sub(&self, conditional_wait: SvmQueueConditionalWait) -> Result<T, SvmQueueError> {
        let mut lock = match conditional_wait {
            SvmQueueConditionalWait::Nowait => self.try_lock()?,
            SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => self.lock()?,
        };
        loop {
            if lock.queue.header().current_size != 0 {
                return lock.sub_one(lock.queue.header().max_size);
            }
            match conditional_wait {
                SvmQueueConditionalWait::Nowait => return Err(SvmQueueError::Empty),
                SvmQueueConditionalWait::Wait => lock.wait()?,
                SvmQueueConditionalWait::TimedWait(timeout) => {
                    if lock.timedwait(timeout)? == WaitOutcome::TimedOut {
                        return Err(SvmQueueError::Timeout);
                    }
                }
            }
        }
    }

    pub fn sub2(&self) -> Result<Option<T>, SvmQueueError> {
        let mut lock = self.lock()?;
        if lock.queue.header().current_size == 0 {
            return Ok(None);
        }
        lock.sub_one(lock.queue.header().max_size / 2).map(Some)
    }

    pub fn len(&self) -> Result<usize, SvmQueueError> {
        let lock = self.lock()?;
        Ok(lock.queue.header().current_size as usize)
    }

    pub fn is_empty(&self) -> Result<bool, SvmQueueError> {
        Ok(self.len()? == 0)
    }

    pub fn is_full(&self) -> Result<bool, SvmQueueError> {
        let lock = self.lock()?;
        Ok(lock.queue.header().current_size == lock.queue.header().max_size)
    }

    pub fn lock(&self) -> Result<SvmQueueLock<'_, T>, SvmQueueError> {
        let guard = self.acquire(false)?;
        Ok(SvmQueueLock { queue: self, guard })
    }

    pub fn try_lock(&self) -> Result<SvmQueueLock<'_, T>, SvmQueueError> {
        let guard = self.acquire(true)?;
        Ok(SvmQueueLock { queue: self, guard })
    }

    /// Destroys the shared synchronization objects. Allocation is released by
    /// the caller's region/heap owner after this method returns.
    ///
    /// # Safety
    /// No queue handle, waiter, or operation in any process may remain.
    pub unsafe fn destroy(self) -> Result<(), SvmQueueError> {
        unsafe {
            self.mutex.destroy();
            self.condvar.destroy();
        }
        Ok(())
    }

    pub fn set_producer_event_fd(&mut self, fd: OwnedFd) {
        self.header_mut().producer_evtfd = fd.as_raw_fd();
        self.producer_evtfd = Some(fd);
    }

    pub fn set_consumer_event_fd(&mut self, fd: OwnedFd) {
        self.header_mut().consumer_evtfd = fd.as_raw_fd();
        self.consumer_evtfd = Some(fd);
    }

    fn header(&self) -> &SvmQueueHeader {
        unsafe { self.header.as_ref() }
    }

    fn header_mut(&self) -> &mut SvmQueueHeader {
        unsafe {
            self.header
                .as_ptr()
                .as_mut()
                .expect("queue pointer is non-null")
        }
    }

    fn elements(&self) -> *mut u8 {
        unsafe {
            self.header
                .as_ptr()
                .cast::<u8>()
                .add(size_of::<SvmQueueHeader>())
        }
    }

    fn acquire(&self, try_only: bool) -> Result<StandardGuard<'_>, SvmQueueError> {
        let result = if try_only {
            unsafe { self.mutex.try_lock() }.map_err(|source| SvmQueueError::Lock { source })?
        } else {
            Some(unsafe { self.mutex.lock() }.map_err(|source| SvmQueueError::Lock { source })?)
        };
        let Some(container) = result else {
            return Err(SvmQueueError::LockBusy);
        };
        let guard = match container {
            RobustGuardContainer::Standard(guard) => guard,
            RobustGuardContainer::Indeterminate(guard) => {
                let guard = guard
                    .make_consistent()
                    .map_err(|source| SvmQueueError::NotRecoverable { source })?;
                drop(guard);
                return Err(SvmQueueError::OwnerDied);
            }
        };
        Ok(guard)
    }

    fn signal(&self, is_producer: bool, operation: SvmQueueOperation) -> Result<(), SvmQueueError> {
        let fd = if is_producer {
            self.producer_evtfd.as_ref()
        } else {
            self.consumer_evtfd.as_ref()
        };
        if let Some(fd) = fd {
            let value = 1_u64.to_ne_bytes();
            let written =
                unsafe { libc::write(fd.as_raw_fd(), value.as_ptr().cast(), value.len()) };
            if written != value.len() as isize {
                return Err(SvmQueueError::EventSignalAfterCommit {
                    operation,
                    source: io::Error::last_os_error(),
                });
            }
        } else {
            unsafe { self.condvar.notify_all() }
                .map_err(|source| SvmQueueError::SignalAfterCommit { operation, source })?;
        }
        Ok(())
    }
}

impl<T> SvmQueueLock<'_, T>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn add_nolock(&mut self, value: T) -> Result<(), SvmQueueError> {
        while self.queue.header().current_size == self.queue.header().max_size {
            self.wait()?;
        }
        let was_empty = self.queue.header().current_size == 0;
        self.write_one(value);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::Add)?;
        }
        Ok(())
    }

    fn add_pair_nolock(&mut self, first: T, second: T) -> Result<(), SvmQueueError> {
        while self.queue.header().max_size - self.queue.header().current_size < 2 {
            self.wait()?;
        }
        let was_empty = self.queue.header().current_size == 0;
        self.write_one(first);
        self.write_one(second);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::AddPair)?;
        }
        Ok(())
    }

    /// Adds one element without checking capacity. The caller must hold this
    /// lock and prove that a slot is available.
    pub unsafe fn add_raw(&mut self, value: T) -> Result<(), SvmQueueError> {
        let was_empty = self.queue.header().current_size == 0;
        self.write_one(value);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::Add)?;
        }
        Ok(())
    }

    /// Removes one element without waiting. The caller must hold this lock and
    /// prove that the queue is non-empty.
    pub unsafe fn sub_raw(&mut self) -> Result<T, SvmQueueError> {
        self.sub_one(self.queue.header().max_size)
    }

    pub fn wait(&mut self) -> Result<(), SvmQueueError> {
        match unsafe { self.queue.condvar.wait(&mut self.guard) } {
            Ok(()) => Ok(()),
            Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => self.owner_died(),
            Err(source) => Err(SvmQueueError::Wait { source }),
        }
    }

    pub fn timedwait(&mut self, timeout: Duration) -> Result<WaitOutcome, SvmQueueError> {
        match unsafe { self.queue.condvar.wait_for(&mut self.guard, timeout) } {
            Ok(outcome) => Ok(outcome),
            Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => {
                self.owner_died().map(|_| WaitOutcome::Notified)
            }
            Err(source) => Err(SvmQueueError::Wait { source }),
        }
    }

    pub fn send_signal(&self, is_producer: bool) -> Result<(), SvmQueueError> {
        self.queue.signal(is_producer, SvmQueueOperation::Add)
    }

    fn write_one(&mut self, value: T) {
        let elements = self.queue.elements();
        let header = self.queue.header_mut();
        let slot = header.tail as usize;
        unsafe {
            std::ptr::write(elements.cast::<T>().add(slot), value);
        }
        header.tail = if header.tail + 1 == header.max_size {
            0
        } else {
            header.tail + 1
        };
        header.current_size += 1;
    }

    fn sub_one(&mut self, signal_at_size: u32) -> Result<T, SvmQueueError> {
        if self.queue.header().current_size == 0 {
            return Err(SvmQueueError::Empty);
        }
        let elements = self.queue.elements();
        let (value, should_signal) = {
            let header = self.queue.header_mut();
            let should_signal = header.current_size == signal_at_size;
            let slot = header.head as usize;
            let value = unsafe { std::ptr::read(elements.cast::<T>().add(slot)) };
            header.head = if header.head + 1 == header.max_size {
                0
            } else {
                header.head + 1
            };
            header.current_size -= 1;
            (value, should_signal)
        };
        if should_signal {
            self.queue.signal(false, SvmQueueOperation::Sub)?;
        }
        Ok(value)
    }

    fn owner_died(&mut self) -> Result<(), SvmQueueError> {
        self.guard
            .mark_consistent()
            .map_err(|source| SvmQueueError::NotRecoverable { source })?;
        Err(SvmQueueError::OwnerDied)
    }
}

fn validate_config<T>(config: &SvmQueueConfig) -> Result<(), SvmQueueError>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    if config.capacity == 0 {
        return Err(SvmQueueError::ZeroCapacity);
    }
    if size_of::<T>() == 0 {
        return Err(SvmQueueError::ZeroSizedElement);
    }
    if size_of::<T>() > u32::MAX as usize
        || align_of::<T>() > u32::MAX as usize
        || align_of::<T>() > CACHE_LINE_BYTES
    {
        return Err(SvmQueueError::LayoutOverflow);
    }
    Ok(())
}
