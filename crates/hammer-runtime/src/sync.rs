//! VPP-style synchronization primitives for short, non-blocking runtime work.
//!
//! These locks busy-wait and do not poison. They are not substitutes for the
//! worker barrier, worker-owned state, channels, or sleeping locks.

use core::cell::UnsafeCell;
use core::hint::spin_loop;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicI32, Ordering, compiler_fence, fence};

pub use hammer_infra::sync::{SpinLock, SpinLockGuard};

#[repr(align(64))]
struct RwState(AtomicI32);

/// Prevents the compiler from moving memory accesses across this point.
///
/// This does not synchronize threads or make non-atomic concurrent access
/// valid. It corresponds to VPP's `CLIB_COMPILER_BARRIER`.
#[inline(always)]
pub fn compiler_barrier() {
    compiler_fence(Ordering::SeqCst);
}

/// Establishes a sequentially consistent full memory fence.
///
/// This orders atomic synchronization protocols; it does not make a data race
/// on ordinary memory valid. It corresponds to VPP's `CLIB_MEMORY_BARRIER`.
#[inline(always)]
pub fn memory_barrier() {
    fence(Ordering::SeqCst);
}

/// Orders preceding reads and writes before a following atomic publication.
///
/// The observing side still needs a matching acquire operation. This
/// corresponds to VPP's `clib_atomic_fence_rel`.
#[inline(always)]
pub fn release_fence() {
    fence(Ordering::Release);
}

/// Completes earlier stores before later stores become observable.
///
/// On x86 this includes non-temporal stores through `sfence`, matching VPP's
/// `CLIB_MEMORY_STORE_BARRIER`. Other architectures use a full fence, as VPP
/// does when it has no specialized store-fence implementation.
#[inline(always)]
pub fn store_barrier() {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `_mm_sfence` has no memory operands or caller preconditions.
        unsafe { core::arch::x86_64::_mm_sfence() };
    }

    #[cfg(all(target_arch = "x86", target_feature = "sse2"))]
    {
        // SAFETY: this branch is compiled only when SSE2 is available.
        unsafe { core::arch::x86::_mm_sfence() };
    }

    #[cfg(not(any(
        target_arch = "x86_64",
        all(target_arch = "x86", target_feature = "sse2")
    )))]
    memory_barrier();
}

/// A cache-line-isolated, non-poisoning reader-writer spin lock.
///
/// Readers may proceed concurrently and writers are exclusive. Like VPP's
/// `clib_rwlock_t`, this lock is reader-preferring and does not guarantee that
/// a waiting writer will eventually run while readers keep arriving.
#[repr(C)]
pub struct RwLock<T: ?Sized> {
    state: RwState,
    value: UnsafeCell<T>,
}

impl<T> RwLock<T> {
    /// Creates an unlocked reader-writer lock containing `value`.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            state: RwState(AtomicI32::new(0)),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Spins until shared read access is acquired.
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            let mut readers = self.state.0.load(Ordering::Relaxed);
            while readers < 0 {
                spin_loop();
                readers = self.state.0.load(Ordering::Relaxed);
            }
            assert_ne!(readers, i32::MAX, "rw lock reader count overflow");
            if self
                .state
                .0
                .compare_exchange_weak(readers, readers + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return RwLockReadGuard {
                    lock: self,
                    value: PhantomData,
                };
            }
        }
    }

    /// Spins until exclusive write access is acquired.
    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        while self
            .state
            .0
            .compare_exchange_weak(0, -1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            while self.state.0.load(Ordering::Relaxed) != 0 {
                spin_loop();
            }
        }
        RwLockWriteGuard {
            lock: self,
            value: PhantomData,
        }
    }
}

// SAFETY: moving the lock transfers ownership of `T`.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
// SAFETY: the atomic state permits either multiple readers or one writer.
// Shared readers require Sync and transferring write ownership requires Send.
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

/// Shared access acquired from [`RwLock::read`].
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    value: PhantomData<&'a T>,
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: the lock state allows shared reads and excludes writers.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let readers = self.lock.state.0.fetch_sub(1, Ordering::Release);
        debug_assert!(readers > 0, "rw lock reader count underflow");
    }
}

/// Exclusive access acquired from [`RwLock::write`].
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    value: PhantomData<&'a mut T>,
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: this guard exists only while the writer state is exclusive.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the writer guard is exclusive and `&mut self` prevents
        // aliasing through this guard.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        debug_assert_eq!(self.lock.state.0.load(Ordering::Relaxed), -1);
        self.lock.state.0.store(0, Ordering::Release);
    }
}
