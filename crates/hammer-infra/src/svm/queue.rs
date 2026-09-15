//! VPP-style fixed-size shared-memory queues.

use std::cell::UnsafeCell;
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use posix_sync::condvar::{
    BorrowedCondvar, CondvarBuilder, CondvarClock, CondvarSharing, RawCondvarAlloc, WaitOutcome,
};
use posix_sync::mutex::guards::{MutexGuard, RobustGuardContainer, StandardGuard};
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexSharing, RawMutexAlloc, robustness_markers::Robust,
};

const CACHE_LINE_BYTES: usize = 64;

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

/// Shared queue at a 64-byte-aligned, caller-owned allocation. Elements start
/// immediately after this naturally aligned object and use runtime `elsize`.
#[repr(C)]
pub struct SvmQueue {
    mutex: UnsafeCell<MaybeUninit<RawMutexAlloc>>,
    condvar: UnsafeCell<MaybeUninit<RawCondvarAlloc>>,
    head: UnsafeCell<u32>,
    tail: UnsafeCell<u32>,
    cursize: AtomicU32,
    maxsize: u32,
    elsize: u32,
    consumer_pid: AtomicI32,
    producer_evtfd: AtomicI32,
    consumer_evtfd: AtomicI32,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const _: () = {
    use std::mem::{align_of, offset_of};

    assert!(align_of::<SvmQueue>() == 8);
    assert!(size_of::<SvmQueue>() == 120);
    assert!(offset_of!(SvmQueue, mutex) == 0);
    assert!(offset_of!(SvmQueue, condvar) == 40);
    assert!(offset_of!(SvmQueue, head) == 88);
    assert!(offset_of!(SvmQueue, tail) == 92);
    assert!(offset_of!(SvmQueue, cursize) == 96);
    assert!(offset_of!(SvmQueue, maxsize) == 100);
    assert!(offset_of!(SvmQueue, elsize) == 104);
    assert!(offset_of!(SvmQueue, consumer_pid) == 108);
    assert!(offset_of!(SvmQueue, producer_evtfd) == 112);
    assert!(offset_of!(SvmQueue, consumer_evtfd) == 116);
};

pub struct SvmQueueLock<'queue> {
    queue: &'queue SvmQueue,
    guard: Option<StandardGuard<'queue>>,
    conditional_wait: SvmQueueConditionalWait,
}

// SAFETY: geometry is immutable after publication; head/tail update under
// the shared mutex (or exclusive server ring owner) and are accessed atomically
// so a concurrent attach may validate their bounds without a Rust data race.
// POSIX state follows the shared mutex.
// Occupancy, PID and descriptor numbers are independent atomics. The mapping
// owner must keep this allocation live across all queue borrows and guards.
unsafe impl Send for SvmQueue {}
unsafe impl Sync for SvmQueue {}

impl SvmQueue {
    pub fn size_to_alloc(config: &SvmQueueConfig) -> Result<usize, SvmQueueError> {
        validate_config(config)?;
        size_of::<Self>()
            .checked_add(
                (config.nels as usize)
                    .checked_mul(config.elsize as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)
    }

    /// Initializes the queue in an aligned allocation of at least
    /// `size_to_alloc(config)` bytes, kept mapped by the caller.
    pub unsafe fn init(
        base: NonNull<u8>,
        config: &SvmQueueConfig,
    ) -> Result<NonNull<Self>, SvmQueueError> {
        assert!(
            base.as_ptr().addr().is_multiple_of(CACHE_LINE_BYTES),
            "queue base is cache-line aligned"
        );
        Self::size_to_alloc(config)?;
        let queue = base.cast::<Self>();
        unsafe {
            std::ptr::write(
                queue.as_ptr(),
                Self {
                    mutex: UnsafeCell::new(MaybeUninit::uninit()),
                    condvar: UnsafeCell::new(MaybeUninit::uninit()),
                    head: UnsafeCell::new(0),
                    tail: UnsafeCell::new(0),
                    cursize: AtomicU32::new(0),
                    maxsize: config.nels,
                    elsize: config.elsize,
                    consumer_pid: AtomicI32::new(config.consumer_pid),
                    producer_evtfd: AtomicI32::new(-1),
                    consumer_evtfd: AtomicI32::new(-1),
                },
            );
        }
        unsafe {
            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed((*queue.as_ptr()).mutex.get().cast(), queue.as_ref());
        }
        unsafe {
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed((*queue.as_ptr()).condvar.get().cast(), queue.as_ref());
        }
        Ok(queue)
    }

    /// The caller proves that the allocation remains mapped and is not
    /// concurrently initialized or destroyed during validation.
    pub unsafe fn attach(
        base: NonNull<u8>,
        allocation_bytes: usize,
    ) -> Result<NonNull<Self>, SvmQueueError> {
        assert!(
            base.as_ptr().addr().is_multiple_of(CACHE_LINE_BYTES),
            "queue base is cache-line aligned"
        );
        if allocation_bytes < size_of::<Self>() {
            return Err(SvmQueueError::InvalidHeader);
        }
        let queue = base.cast::<Self>();
        let stored = unsafe { queue.as_ref() };
        if stored.maxsize == 0
            || stored.elsize == 0
            || unsafe { AtomicU32::from_ptr(stored.head.get()) }.load(Ordering::Relaxed)
                >= stored.maxsize
            || unsafe { AtomicU32::from_ptr(stored.tail.get()) }.load(Ordering::Relaxed)
                >= stored.maxsize
            || stored.cursize.load(Ordering::Relaxed) > stored.maxsize
        {
            return Err(SvmQueueError::InvalidHeader);
        }
        let bytes = size_of::<Self>()
            .checked_add(
                (stored.maxsize as usize)
                    .checked_mul(stored.elsize as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        if bytes > allocation_bytes {
            return Err(SvmQueueError::InvalidHeader);
        }
        Ok(queue)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.maxsize as usize
    }

    #[inline]
    pub fn element_size(&self) -> usize {
        self.elsize as usize
    }

    #[inline]
    pub fn consumer_pid(&self) -> i32 {
        self.consumer_pid.load(Ordering::Relaxed)
    }

    /// Called only while the mapping's restart owner excludes active users.
    pub fn set_consumer_pid(&self, pid: i32) {
        self.consumer_pid.store(pid, Ordering::Relaxed);
    }

    #[inline]
    pub fn can_send(&self) -> bool {
        self.cursize.load(Ordering::Relaxed) < self.maxsize
    }

    /// Only the sole server main thread may inspect a ring without the mutex.
    #[inline(always)]
    pub unsafe fn ring_head_slot_unlocked(&self) -> NonNull<u8> {
        let head = unsafe { AtomicU32::from_ptr(self.head.get()) }.load(Ordering::Relaxed) as usize;
        let slot = unsafe { self.data().add(head * self.element_size()) };
        unsafe { NonNull::new_unchecked(slot) }
    }

    /// Only the sole server main thread may advance a ring without the mutex.
    #[inline(always)]
    pub unsafe fn advance_ring_head_unlocked(&self) {
        let head = unsafe { AtomicU32::from_ptr(self.head.get()) };
        let next = head.load(Ordering::Relaxed) + 1;
        head.store(
            if next == self.maxsize { 0 } else { next },
            Ordering::Relaxed,
        );
    }

    pub fn add_element<T: SvmQueueElement>(
        &self,
        element: &T,
        wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        self.check_element_size::<T>()?;
        self.add(zerocopy::IntoBytes::as_bytes(element), wait)
    }

    pub fn sub_element<T: SvmQueueElement>(
        &self,
        wait: SvmQueueConditionalWait,
    ) -> Result<T, SvmQueueError> {
        self.check_element_size::<T>()?;
        let mut value = MaybeUninit::<T>::uninit();
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                value.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                size_of::<T>(),
            )
        };
        self.sub_into(bytes, wait)?;
        Ok(unsafe { value.assume_init() })
    }

    pub fn try_sub_element<T: SvmQueueElement>(&self) -> Result<Option<T>, SvmQueueError> {
        self.check_element_size::<T>()?;
        let mut value = MaybeUninit::<T>::uninit();
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                value.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                size_of::<T>(),
            )
        };
        match self.sub2_into(bytes) {
            Ok(()) => Ok(Some(unsafe { value.assume_init() })),
            Err(SvmQueueError::Empty) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn check_element_size<T: SvmQueueElement>(&self) -> Result<(), SvmQueueError> {
        if size_of::<T>() != self.element_size() {
            return Err(SvmQueueError::ElementSizeMismatch {
                requested: size_of::<T>(),
                stored: self.element_size(),
            });
        }
        Ok(())
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
        let output =
            unsafe { std::slice::from_raw_parts_mut(element.as_mut_ptr().cast(), element.len()) };
        self.sub_into(output, conditional_wait)
    }

    fn sub_into(
        &self,
        element: &mut [MaybeUninit<u8>],
        conditional_wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError> {
        validate_element_length(self.element_size(), element.len())?;
        let mut lock = match conditional_wait {
            SvmQueueConditionalWait::Nowait => self.try_lock()?,
            SvmQueueConditionalWait::Wait | SvmQueueConditionalWait::TimedWait(_) => self.lock()?,
        };
        loop {
            if lock.queue.cursize.load(Ordering::Relaxed) != 0 {
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
        let output =
            unsafe { std::slice::from_raw_parts_mut(element.as_mut_ptr().cast(), element.len()) };
        self.sub2_into(output)
    }

    fn sub2_into(&self, element: &mut [MaybeUninit<u8>]) -> Result<(), SvmQueueError> {
        validate_element_length(self.element_size(), element.len())?;
        let mut lock = self.lock()?;
        if lock.queue.cursize.load(Ordering::Relaxed) == 0 {
            return Err(SvmQueueError::Empty);
        }
        lock.sub_nolock(element)?;
        Ok(())
    }

    pub fn len(&self) -> Result<usize, SvmQueueError> {
        let guard = self.lock()?;
        Ok(guard.queue.cursize.load(Ordering::Relaxed) as usize)
    }

    pub fn is_empty(&self) -> Result<bool, SvmQueueError> {
        Ok(self.len()? == 0)
    }

    pub fn is_full(&self) -> Result<bool, SvmQueueError> {
        let guard = self.lock()?;
        Ok(guard.queue.cursize.load(Ordering::Relaxed) == self.maxsize)
    }

    pub fn lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError> {
        Ok(SvmQueueLock {
            queue: self,
            guard: Some(self.acquire(false)?),
            conditional_wait: SvmQueueConditionalWait::Wait,
        })
    }

    pub fn try_lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError> {
        Ok(SvmQueueLock {
            queue: self,
            guard: Some(self.acquire(true)?),
            conditional_wait: SvmQueueConditionalWait::Nowait,
        })
    }

    /// All processes using this shared queue must have corresponding local
    /// descriptors before event mode is enabled, and retain them while used.
    /// A shared numeric fd never transfers descriptor ownership.
    pub unsafe fn set_producer_event_fd(&self, fd: BorrowedFd<'_>) {
        self.producer_evtfd.store(fd.as_raw_fd(), Ordering::Relaxed);
    }

    /// All processes using this shared queue must have corresponding local
    /// descriptors before event mode is enabled, and retain them while used.
    /// A shared numeric fd never transfers descriptor ownership.
    pub unsafe fn set_consumer_event_fd(&self, fd: BorrowedFd<'_>) {
        self.consumer_evtfd.store(fd.as_raw_fd(), Ordering::Relaxed);
    }

    /// Destroys POSIX state only; the mapping owner separately frees storage.
    pub unsafe fn cleanup(&self) {
        unsafe {
            self.borrowed_mutex().destroy();
            self.borrowed_condvar().destroy();
        }
    }

    /// On restart only, under the region lock, with no local queue guard.
    /// Resetting raw POSIX state after ten failed attempts mirrors the
    /// implementation-dependent recovery branch in the shared-memory API.
    pub unsafe fn reset_mutex_for_restart(&self) {
        let mutex = self.mutex.get().cast::<libc::pthread_mutex_t>();
        for _ in 0..10 {
            if unsafe { libc::pthread_mutex_trylock(mutex) } == 0 {
                let status = unsafe { libc::pthread_mutex_unlock(mutex) };
                assert_eq!(status, 0, "restart queue mutex unlock failed");
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        unsafe { std::ptr::write_bytes(self.mutex.get(), 0, 1) };
    }

    fn borrowed_mutex(&self) -> BorrowedMutex<'_, Robust> {
        unsafe { BorrowedMutex::from_raw(self.mutex.get().cast(), self) }
    }

    fn borrowed_condvar(&self) -> BorrowedCondvar<'_> {
        unsafe {
            BorrowedCondvar::from_raw(self.condvar.get().cast(), self, CondvarClock::Monotonic)
        }
    }

    fn data(&self) -> *mut u8 {
        unsafe {
            (self as *const Self)
                .cast::<u8>()
                .add(size_of::<Self>())
                .cast_mut()
        }
    }

    fn acquire(&self, try_only: bool) -> Result<StandardGuard<'_>, SvmQueueError> {
        let mutex = self.borrowed_mutex();
        let result = if try_only {
            unsafe { mutex.try_lock() }.map_err(|source| SvmQueueError::Lock { source })?
        } else {
            Some(unsafe { mutex.lock() }.map_err(|source| SvmQueueError::Lock { source })?)
        };
        let Some(container) = result else {
            return Err(SvmQueueError::LockBusy);
        };
        match container {
            RobustGuardContainer::Standard(guard) => {
                // The borrowed mutex contains no allocation: this queue owns
                // the mapped POSIX storage for the full returned guard borrow.
                Ok(unsafe { std::mem::transmute::<StandardGuard<'_>, StandardGuard<'_>>(guard) })
            }
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
            self.producer_evtfd.load(Ordering::Relaxed)
        } else {
            self.consumer_evtfd.load(Ordering::Relaxed)
        };
        if fd >= 0 {
            let value = 1_u64.to_ne_bytes();
            let written = unsafe { libc::write(fd, value.as_ptr().cast(), value.len()) };
            if written != value.len() as isize {
                return Err(SvmQueueError::EventSignalAfterCommit {
                    operation,
                    source: io::Error::last_os_error(),
                });
            }
        } else {
            unsafe { self.borrowed_condvar().notify_all() }
                .map_err(|source| SvmQueueError::SignalAfterCommit { operation, source })?;
        }
        Ok(())
    }
}

impl SvmQueueLock<'_> {
    #[inline]
    pub fn consumer_head(&self) -> u32 {
        unsafe { AtomicU32::from_ptr(self.queue.head.get()) }.load(Ordering::Relaxed)
    }
    pub fn add_nolock(&mut self, element: &[u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), element)?;
        while self.queue.cursize.load(Ordering::Relaxed) == self.queue.maxsize {
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
        let was_empty = self.queue.cursize.load(Ordering::Relaxed) == 0;
        self.write_one(element);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::Add)?;
        }
        Ok(())
    }

    pub fn add2_nolock(&mut self, first: &[u8], second: &[u8]) -> Result<(), SvmQueueError> {
        validate_element(self.queue.element_size(), first)?;
        validate_element(self.queue.element_size(), second)?;
        if self.queue.maxsize < 2 {
            return Err(SvmQueueError::QueueFull);
        }
        while self.queue.maxsize - self.queue.cursize.load(Ordering::Relaxed) < 2 {
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
        let was_empty = self.queue.cursize.load(Ordering::Relaxed) == 0;
        self.write_one(first);
        self.write_one(second);
        if was_empty {
            self.queue.signal(true, SvmQueueOperation::AddPair)?;
        }
        Ok(())
    }

    pub fn sub_raw(&mut self, element: &mut [u8]) -> Result<(), SvmQueueError> {
        let output =
            unsafe { std::slice::from_raw_parts_mut(element.as_mut_ptr().cast(), element.len()) };
        validate_element_length(self.queue.element_size(), output.len())?;
        self.sub_nolock(output)
    }

    pub fn wait(&mut self) -> Result<(), SvmQueueError> {
        if self.queue.producer_evtfd.load(Ordering::Relaxed) >= 0 {
            self.wait_on_event(None)?;
            return Ok(());
        }
        let guard = self.guard.as_mut().expect("queue lock held during wait");
        match unsafe { self.queue.borrowed_condvar().wait(guard) } {
            Ok(()) => Ok(()),
            Err(posix_sync::condvar::CondvarWaitError::OwnerDead) => self.owner_died(),
            Err(source) => Err(SvmQueueError::Wait { source }),
        }
    }

    pub fn timed_wait(&mut self, timeout: Duration) -> Result<WaitOutcome, SvmQueueError> {
        if self.queue.producer_evtfd.load(Ordering::Relaxed) >= 0 {
            return self.wait_on_event(Some(Instant::now() + timeout));
        }
        let guard = self
            .guard
            .as_mut()
            .expect("queue lock held during timed wait");
        match unsafe { self.queue.borrowed_condvar().wait_for(guard, timeout) } {
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

    fn wait_on_event(&mut self, deadline: Option<Instant>) -> Result<WaitOutcome, SvmQueueError> {
        let occupancy = self.queue.cursize.load(Ordering::Relaxed);
        drop(self.guard.take());
        while self.queue.cursize.load(Ordering::Acquire) == occupancy {
            if deadline.is_some_and(|until| Instant::now() >= until) {
                self.guard = Some(self.queue.acquire(false)?);
                return Ok(WaitOutcome::TimedOut);
            }
            std::hint::spin_loop();
        }
        self.guard = Some(self.queue.acquire(false)?);
        Ok(WaitOutcome::Notified)
    }

    #[inline(always)]
    pub fn ring_head_slot(&mut self) -> NonNull<u8> {
        let head =
            unsafe { AtomicU32::from_ptr(self.queue.head.get()) }.load(Ordering::Relaxed) as usize;
        let slot = unsafe { self.queue.data().add(head * self.queue.element_size()) };
        unsafe { NonNull::new_unchecked(slot) }
    }

    #[inline(always)]
    pub fn advance_ring_head(&mut self) {
        let head = unsafe { AtomicU32::from_ptr(self.queue.head.get()) };
        let next = head.load(Ordering::Relaxed) + 1;
        head.store(
            if next == self.queue.maxsize { 0 } else { next },
            Ordering::Relaxed,
        );
    }

    fn write_one(&mut self, element: &[u8]) {
        let tail = unsafe { AtomicU32::from_ptr(self.queue.tail.get()) };
        let slot = tail.load(Ordering::Relaxed) as usize;
        let element_size = self.queue.element_size();
        let destination = unsafe { self.queue.data().add(slot * element_size) };
        unsafe {
            std::ptr::copy_nonoverlapping(element.as_ptr(), destination, element_size);
        }
        let next = slot as u32 + 1;
        tail.store(
            if next == self.queue.maxsize { 0 } else { next },
            Ordering::Relaxed,
        );
        self.queue.cursize.fetch_add(1, Ordering::Release);
    }

    fn sub_nolock(&mut self, element: &mut [MaybeUninit<u8>]) -> Result<(), SvmQueueError> {
        if self.queue.cursize.load(Ordering::Relaxed) == 0 {
            return Err(SvmQueueError::Empty);
        }
        let head = unsafe { AtomicU32::from_ptr(self.queue.head.get()) };
        let slot = head.load(Ordering::Relaxed) as usize;
        let element_size = self.queue.element_size();
        let source = unsafe { self.queue.data().add(slot * element_size) };
        unsafe {
            std::ptr::copy_nonoverlapping(source, element.as_mut_ptr().cast(), element_size);
        }
        let was_full = self.queue.cursize.load(Ordering::Relaxed) == self.queue.maxsize;
        let next = slot as u32 + 1;
        head.store(
            if next == self.queue.maxsize { 0 } else { next },
            Ordering::Relaxed,
        );
        self.queue.cursize.fetch_sub(1, Ordering::Release);
        if was_full {
            self.queue.signal(false, SvmQueueOperation::Sub)?;
        }
        Ok(())
    }

    fn owner_died(&mut self) -> Result<(), SvmQueueError> {
        self.guard
            .as_mut()
            .expect("queue lock held after owner death")
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
    validate_element_length(expected, element.len())
}

fn validate_element_length(expected: usize, actual: usize) -> Result<(), SvmQueueError> {
    if actual != expected {
        return Err(SvmQueueError::ElementLengthMismatch { expected, actual });
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
