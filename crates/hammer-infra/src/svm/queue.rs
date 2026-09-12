//! VPP-style fixed-size shared-memory queues.

use std::alloc::{Layout, LayoutError};
use std::io;
use std::mem::{MaybeUninit, size_of};
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

use crate::svm::ssvm::{SsvmError, SsvmPrivate};

const CACHE_LINE_BYTES: usize = 64;

// posix-sync ties a borrowed process-shared primitive to an address-lifetime
// token. The mapping owner keeps the actual mapping alive; this token only
// prevents the queue handle from becoming self-referential.
static SVM_QUEUE_MAPPING_ANCHOR: () = ();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmQueueConfig {
    pub nels: u32,
    pub elsize: u32,
    pub consumer_pid: i32,
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
    #[error("queue element count must be positive")]
    ZeroCapacity,
    #[error("queue element size must be positive")]
    ZeroElementSize,
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
    #[error("queue element buffer has length {actual}, expected {expected}")]
    ElementLengthMismatch { expected: usize, actual: usize },
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

/// VPP `svm_queue_t`. The flexible `data[]` region follows this header in the
/// same caller-owned allocation and is addressed by runtime `elsize`.
#[repr(C, align(64))]
struct SvmQueueHeader {
    mutex: MaybeUninit<RawMutexAlloc>,
    condvar: MaybeUninit<RawCondvarAlloc>,
    head: u32,
    tail: u32,
    cursize: u32,
    maxsize: u32,
    elsize: u32,
    consumer_pid: i32,
    producer_evtfd: i32,
    consumer_evtfd: i32,
}

pub struct SvmQueue {
    header: NonNull<SvmQueueHeader>,
    mutex: BorrowedMutex<'static, Robust>,
    condvar: BorrowedCondvar<'static>,
    producer_event_fd: Option<OwnedFd>,
    consumer_event_fd: Option<OwnedFd>,
}

pub struct SvmQueueLock<'queue> {
    queue: &'queue SvmQueue,
    guard: StandardGuard<'queue>,
    nowait: bool,
}

unsafe impl Send for SvmQueue {}
unsafe impl Sync for SvmQueue {}

impl SvmQueue {
    pub fn layout(config: &SvmQueueConfig) -> Result<Layout, SvmQueueError> {
        validate_config(config)?;
        let bytes = size_of::<SvmQueueHeader>()
            .checked_add(
                (config.nels as usize)
                    .checked_mul(config.elsize as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        Layout::from_size_align(bytes, CACHE_LINE_BYTES).map_err(SvmQueueError::Layout)
    }

    /// Initializes VPP's header and its inline `data[]` region at an existing
    /// caller-owned allocation in the mapped segment.
    pub unsafe fn init_at(
        segment: &SsvmPrivate,
        offset: u64,
        config: &SvmQueueConfig,
    ) -> Result<Self, SvmQueueError> {
        let layout = Self::layout(config)?;
        let header = NonNull::new(
            segment
                .offset_ptr(offset, layout.size(), layout.align())?
                .cast::<SvmQueueHeader>(),
        )
        .expect("queue pointer");
        unsafe {
            std::ptr::write(
                header.as_ptr(),
                SvmQueueHeader {
                    mutex: MaybeUninit::uninit(),
                    condvar: MaybeUninit::uninit(),
                    head: 0,
                    tail: 0,
                    cursize: 0,
                    maxsize: config.nels,
                    elsize: config.elsize,
                    consumer_pid: config.consumer_pid,
                    producer_evtfd: -1,
                    consumer_evtfd: -1,
                },
            );
        }
        let mutex = unsafe {
            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed(
                    std::ptr::addr_of_mut!((*header.as_ptr()).mutex).cast(),
                    &SVM_QUEUE_MAPPING_ANCHOR,
                )
        };
        let condvar = unsafe {
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed(
                    std::ptr::addr_of_mut!((*header.as_ptr()).condvar).cast(),
                    &SVM_QUEUE_MAPPING_ANCHOR,
                )
        };
        Ok(Self {
            header,
            mutex,
            condvar,
            producer_event_fd: None,
            consumer_event_fd: None,
        })
    }

    pub unsafe fn attach(segment: &SsvmPrivate, offset: u64) -> Result<Self, SvmQueueError> {
        let header = NonNull::new(
            segment
                .offset_ptr(offset, size_of::<SvmQueueHeader>(), CACHE_LINE_BYTES)?
                .cast::<SvmQueueHeader>(),
        )
        .expect("queue pointer");
        let stored = unsafe { header.as_ref() };
        if stored.maxsize == 0
            || stored.elsize == 0
            || stored.head >= stored.maxsize
            || stored.tail >= stored.maxsize
            || stored.cursize > stored.maxsize
        {
            return Err(SvmQueueError::InvalidHeader);
        }
        let bytes = size_of::<SvmQueueHeader>()
            .checked_add(
                (stored.maxsize as usize)
                    .checked_mul(stored.elsize as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        segment.offset_ptr(offset, bytes, CACHE_LINE_BYTES)?;
        let mutex = unsafe {
            BorrowedMutex::<Robust>::from_raw(
                std::ptr::addr_of_mut!((*header.as_ptr()).mutex).cast(),
                &SVM_QUEUE_MAPPING_ANCHOR,
            )
        };
        let condvar = unsafe {
            BorrowedCondvar::from_raw(
                std::ptr::addr_of_mut!((*header.as_ptr()).condvar).cast(),
                &SVM_QUEUE_MAPPING_ANCHOR,
                CondvarClock::Monotonic,
            )
        };
        Ok(Self {
            header,
            mutex,
            condvar,
            producer_event_fd: None,
            consumer_event_fd: None,
        })
    }

    pub fn capacity(&self) -> usize {
        self.header().maxsize as usize
    }

    pub fn element_size(&self) -> usize {
        self.header().elsize as usize
    }

    pub fn consumer_pid(&self) -> i32 {
        self.header().consumer_pid
    }

    pub fn add(&self, element: &[u8], nowait: bool) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), element)?;
        let mut lock = if nowait {
            self.try_lock()?
        } else {
            self.lock()?
        };
        lock.add_nolock(element)
    }

    pub fn add2(&self, first: &[u8], second: &[u8], nowait: bool) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), first)?;
        validate_element(self.element_size(), second)?;
        let mut lock = if nowait {
            self.try_lock()?
        } else {
            self.lock()?
        };
        lock.add2_nolock(first, second)
    }

    pub fn sub(
        &self,
        element: &mut [u8],
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), element)?;
        let mut lock = match conditional_wait {
            SvmQueueConditionalWait::Nowait => self.try_lock()?,
            SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => self.lock()?,
        };
        loop {
            if lock.queue.header().cursize != 0 {
                return lock.sub_nolock(element);
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

    pub fn sub2(&self, element: &mut [u8]) -> Result<bool, SvmQueueError> {
        validate_element(self.element_size(), element)?;
        let mut lock = self.lock()?;
        if lock.queue.header().cursize == 0 {
            return Ok(false);
        }
        lock.sub_nolock(element)?;
        Ok(true)
    }

    pub fn len(&self) -> Result<usize, SvmQueueError> {
        let _guard = self.lock()?;
        Ok(self.header().cursize as usize)
    }

    pub fn is_empty(&self) -> Result<bool, SvmQueueError> {
        Ok(self.len()? == 0)
    }

    pub fn is_full(&self) -> Result<bool, SvmQueueError> {
        let _guard = self.lock()?;
        Ok(self.header().cursize == self.header().maxsize)
    }

    pub fn lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError> {
        Ok(SvmQueueLock {
            queue: self,
            guard: self.acquire(false)?,
            nowait: false,
        })
    }

    pub fn try_lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError> {
        Ok(SvmQueueLock {
            queue: self,
            guard: self.acquire(true)?,
            nowait: true,
        })
    }

    pub fn set_producer_event_fd(&mut self, fd: OwnedFd) {
        self.header_mut().producer_evtfd = fd.as_raw_fd();
        self.producer_event_fd = Some(fd);
    }

    pub fn set_consumer_event_fd(&mut self, fd: OwnedFd) {
        self.header_mut().consumer_evtfd = fd.as_raw_fd();
        self.consumer_event_fd = Some(fd);
    }

    /// Destroys the process-shared synchronization objects. The caller frees
    /// the enclosing allocation through its mapping/region owner.
    pub unsafe fn destroy(self) -> Result<(), SvmQueueError> {
        unsafe {
            self.mutex.destroy();
            self.condvar.destroy();
        }
        Ok(())
    }

    fn header(&self) -> &SvmQueueHeader {
        unsafe { self.header.as_ref() }
    }

    fn header_mut(&mut self) -> &mut SvmQueueHeader {
        unsafe { self.header.as_mut() }
    }

    fn data(&self) -> *mut u8 {
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
        match container {
            RobustGuardContainer::Standard(guard) => Ok(guard),
            RobustGuardContainer::Indeterminate(guard) => {
                let guard = guard
                    .make_consistent()
                    .map_err(|source| SvmQueueError::NotRecoverable { source })?;
                drop(guard);
                Err(SvmQueueError::OwnerDied)
            }
        }
    }

    fn signal(&self, is_producer: bool, operation: SvmQueueOperation) -> Result<(), SvmQueueError> {
        let fd = if is_producer {
            self.producer_event_fd.as_ref()
        } else {
            self.consumer_event_fd.as_ref()
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

impl SvmQueueLock<'_> {
    pub fn add_nolock(&mut self, element: &[u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), element)?;
        while self.queue.header().cursize == self.queue.header().maxsize {
            if self.nowait {
                return Err(SvmQueueError::QueueFull);
            }
            self.wait()?;
        }
        let was_empty = self.queue.header().cursize == 0;
        self.write_one(element);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::Add)?;
        }
        Ok(())
    }

    pub fn add2_nolock(&mut self, first: &[u8], second: &[u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), first)?;
        validate_element(self.queue.element_size(), second)?;
        while self.queue.header().maxsize - self.queue.header().cursize < 2 {
            if self.nowait {
                return Err(SvmQueueError::QueueFull);
            }
            self.wait()?;
        }
        let was_empty = self.queue.header().cursize == 0;
        self.write_one(first);
        self.write_one(second);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::AddPair)?;
        }
        Ok(())
    }

    pub unsafe fn add_raw(&mut self, element: &[u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), element)?;
        let was_empty = self.queue.header().cursize == 0;
        self.write_one(element);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::Add)?;
        }
        Ok(())
    }

    pub unsafe fn sub_raw(&mut self, element: &mut [u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), element)?;
        self.sub_nolock(element)
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

    fn write_one(&mut self, element: &[u8]) {
        let header = unsafe { &mut *self.queue.header.as_ptr() };
        let slot = header.tail as usize;
        let element_size = header.elsize as usize;
        let destination = unsafe { self.queue.data().add(slot * element_size) };
        unsafe {
            std::ptr::copy_nonoverlapping(element.as_ptr(), destination, element_size);
        }
        header.tail = if header.tail + 1 == header.maxsize {
            0
        } else {
            header.tail + 1
        };
        header.cursize += 1;
    }

    fn sub_nolock(&mut self, element: &mut [u8]) -> Result<(), SvmQueueError> {
        if self.queue.header().cursize == 0 {
            return Err(SvmQueueError::Empty);
        }
        let header = unsafe { &mut *self.queue.header.as_ptr() };
        let slot = header.head as usize;
        let element_size = header.elsize as usize;
        let source = unsafe { self.queue.data().add(slot * element_size) };
        unsafe {
            std::ptr::copy_nonoverlapping(source, element.as_mut_ptr(), element_size);
        }
        let was_full = header.cursize == header.maxsize;
        header.head = if header.head + 1 == header.maxsize {
            0
        } else {
            header.head + 1
        };
        header.cursize -= 1;
        if was_full {
            self.queue.signal(false, SvmQueueOperation::Sub)?;
        }
        Ok(())
    }

    fn owner_died(&mut self) -> Result<(), SvmQueueError> {
        self.guard
            .mark_consistent()
            .map_err(|source| SvmQueueError::NotRecoverable { source })?;
        Err(SvmQueueError::OwnerDied)
    }
}

fn validate_config(config: &SvmQueueConfig) -> Result<(), SvmQueueError> {
    if config.nels == 0 {
        return Err(SvmQueueError::ZeroCapacity);
    }
    if config.elsize == 0 {
        return Err(SvmQueueError::ZeroElementSize);
    }
    (config.nels as usize)
        .checked_mul(config.elsize as usize)
        .ok_or(SvmQueueError::LayoutOverflow)?;
    Ok(())
}

fn validate_element(expected: usize, element: &[u8]) -> Result<(), SvmQueueError> {
    if element.len() != expected {
        return Err(SvmQueueError::ElementLengthMismatch {
            expected,
            actual: element.len(),
        });
    }
    Ok(())
}
