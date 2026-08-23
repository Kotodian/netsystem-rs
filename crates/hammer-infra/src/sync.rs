//! VPP-style synchronization primitives for short, non-blocking work.
//!
//! These locks busy-wait and do not poison. They are not substitutes for the
//! worker barrier, worker-owned state, channels, or sleeping locks.

use core::cell::UnsafeCell;
use core::hint::spin_loop;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicBool, Ordering};

#[repr(align(64))]
struct SpinState(AtomicBool);

/// A cache-line-isolated, non-poisoning mutual-exclusion spin lock.
///
/// Use this only for bounded critical sections that cannot sleep, block, or
/// await. Acquisition is not fair.
#[repr(C)]
pub struct SpinLock<T: ?Sized> {
    state: SpinState,
    value: UnsafeCell<T>,
}

impl<T> SpinLock<T> {
    /// Creates an unlocked spin lock containing `value`.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            state: SpinState(AtomicBool::new(false)),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> SpinLock<T> {
    /// Spins until exclusive access is acquired.
    #[inline]
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        while self
            .state
            .0
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            while self.state.0.load(Ordering::Relaxed) {
                spin_loop();
            }
        }
        SpinLockGuard {
            lock: self,
            value: PhantomData,
        }
    }

    /// Attempts to acquire exclusive access without waiting.
    #[inline]
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        self.state
            .0
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
            .then(|| SpinLockGuard {
                lock: self,
                value: PhantomData,
            })
    }
}

// SAFETY: moving the lock transfers ownership of `T`.
unsafe impl<T: ?Sized + Send> Send for SpinLock<T> {}
// SAFETY: the atomic lock grants at most one guard, so shared lock references
// cannot access `T` concurrently. Moving `T` between guard owners requires Send.
unsafe impl<T: ?Sized + Send> Sync for SpinLock<T> {}

/// Exclusive access acquired from [`SpinLock::lock`] or [`SpinLock::try_lock`].
pub struct SpinLockGuard<'a, T: ?Sized> {
    lock: &'a SpinLock<T>,
    value: PhantomData<&'a mut T>,
}

impl<T: ?Sized> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: this guard exists only after exclusive lock acquisition.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for SpinLockGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the guard is exclusive and `&mut self` prevents aliasing
        // through this guard.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for SpinLockGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.state.0.store(false, Ordering::Release);
    }
}
