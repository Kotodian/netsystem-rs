//! VPP-style fixed-size shared-memory queues.

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
    conditional_wait: SvmQueueConditionalWait,
}

unsafe impl Send for SvmQueue {}
unsafe impl Sync for SvmQueue {}

impl SvmQueue {
    pub fn size_to_alloc(config: &SvmQueueConfig) -> Result<usize, SvmQueueError> {
        validate_config(config)?;
        size_of::<SvmQueueHeader>()
            .checked_add(
                (config.nels as usize)
                    .checked_mul(config.elsize as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)
    }

    /// Initializes VPP's header and its inline `data[]` region at an existing
    /// caller-owned allocation in the mapped segment.
    pub(crate) unsafe fn init(
        base: NonNull<u8>,
        config: &SvmQueueConfig,
    ) -> Result<Self, SvmQueueError> {
        assert!(
            base.as_ptr().addr().is_multiple_of(CACHE_LINE_BYTES),
            "queue base is cache-line aligned"
        );
        let _ = Self::size_to_alloc(config)?;
        let header = base.cast::<SvmQueueHeader>();
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

    pub(crate) unsafe fn attach(base: NonNull<u8>) -> Result<Self, SvmQueueError> {
        assert!(
            base.as_ptr().addr().is_multiple_of(CACHE_LINE_BYTES),
            "queue base is cache-line aligned"
        );
        let header = base.cast::<SvmQueueHeader>();
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
        let _ = bytes;
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

    pub fn add(
        &self,
        element: &[u8],
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), element)?;
        let mut lock = match conditional_wait {
            SvmQueueConditionalWait::Nowait => self.try_lock()?,
            SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => self.lock()?,
        };
        lock.conditional_wait = conditional_wait;
        lock.add_nolock(element)
    }

    pub fn add2(
        &self,
        first: &[u8],
        second: &[u8],
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), first)?;
        validate_element(self.element_size(), second)?;
        let mut lock = match conditional_wait {
            SvmQueueConditionalWait::Nowait => self.try_lock()?,
            SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => self.lock()?,
        };
        lock.conditional_wait = conditional_wait;
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
                    if lock.timed_wait(timeout)? == WaitOutcome::TimedOut {
                        return Err(SvmQueueError::Timeout);
                    }
                }
            }
        }
    }

    pub fn sub2(&self, element: &mut [u8]) -> Result<(), SvmQueueError> {
        validate_element(self.element_size(), element)?;
        let mut lock = self.lock()?;
        if lock.queue.header().cursize == 0 {
            return Err(SvmQueueError::Empty);
        }
        lock.sub_nolock(element)?;
        Ok(())
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
            conditional_wait: SvmQueueConditionalWait::Wait,
        })
    }

    pub fn try_lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError> {
        Ok(SvmQueueLock {
            queue: self,
            guard: self.acquire(true)?,
            conditional_wait: SvmQueueConditionalWait::Nowait,
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
    pub(crate) unsafe fn cleanup(&mut self) {
        unsafe {
            self.mutex.destroy();
            self.condvar.destroy();
        }
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
            match self.conditional_wait {
                SvmQueueConditionalWait::Nowait => return Err(SvmQueueError::QueueFull),
                SvmQueueConditionalWait::Wait => self.wait()?,
                SvmQueueConditionalWait::TimedWait(timeout) => {
                    if self.timed_wait(timeout)? == WaitOutcome::TimedOut {
                        return Err(SvmQueueError::Timeout);
                    }
                }
            }
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
            match self.conditional_wait {
                SvmQueueConditionalWait::Nowait => return Err(SvmQueueError::QueueFull),
                SvmQueueConditionalWait::Wait => self.wait()?,
                SvmQueueConditionalWait::TimedWait(timeout) => {
                    if self.timed_wait(timeout)? == WaitOutcome::TimedOut {
                        return Err(SvmQueueError::Timeout);
                    }
                }
            }
        }
        let was_empty = self.queue.header().cursize == 0;
        self.write_one(first);
        self.write_one(second);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::AddPair)?;
        }
        Ok(())
    }

    pub fn sub_raw(&mut self, element: &mut [u8]) -> Result<(), SvmQueueError> {
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

    pub fn timed_wait(&mut self, timeout: Duration) -> Result<WaitOutcome, SvmQueueError> {
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

pub trait SvmQueueElement:
    zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes
{
}

impl<T> SvmQueueElement for T where
    T: zerocopy::KnownLayout + zerocopy::FromBytes + zerocopy::Immutable + zerocopy::IntoBytes
{
}

pub struct SvmQueueElements<T: SvmQueueElement> {
    queue: SvmQueue,
    _element: std::marker::PhantomData<fn() -> T>,
}

impl<T: SvmQueueElement> SvmQueueElements<T> {
    pub(crate) fn from_raw(queue: SvmQueue) -> Self {
        Self {
            queue,
            _element: std::marker::PhantomData,
        }
    }

    pub fn add(
        &self,
        element: &T,
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        self.queue.add(element.as_bytes(), conditional_wait)
    }

    pub fn add2(
        &self,
        first: &T,
        second: &T,
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        self.queue
            .add2(first.as_bytes(), second.as_bytes(), conditional_wait)
    }

    pub fn sub(&self, conditional_wait: SvmQueueConditionalWait) -> Result<T, SvmQueueError> {
        let mut bytes = vec![0_u8; size_of::<T>()];
        self.queue.sub(&mut bytes, conditional_wait)?;
        T::read_from_bytes(&bytes).map_err(|_| SvmQueueError::InvalidHeader)
    }

    pub fn try_sub(&self) -> Result<Option<T>, SvmQueueError> {
        let mut bytes = vec![0_u8; size_of::<T>()];
        match self.queue.sub2(&mut bytes) {
            Ok(()) => T::read_from_bytes(&bytes)
                .map(Some)
                .map_err(|_| SvmQueueError::InvalidHeader),
            Err(SvmQueueError::Empty) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
