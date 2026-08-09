use std::cell::UnsafeCell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, ThreadId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadOwnedError {
    NotInstalled,
    WrongThread,
    AlreadyBorrowed,
}

impl fmt::Display for ThreadOwnedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => formatter.write_str("value is not installed"),
            Self::WrongThread => formatter.write_str("value belongs to another thread"),
            Self::AlreadyBorrowed => formatter.write_str("value is already borrowed"),
        }
    }
}

impl std::error::Error for ThreadOwnedError {}

/// A value installed once and mutably accessed only by its installing thread.
///
/// This provides indexed per-thread storage without thread-local keys. The
/// installing thread is checked before the internal mutable reference is
/// created, and nested mutable access is rejected.
pub struct ThreadOwned<T> {
    state: AtomicU8,
    owner: UnsafeCell<Option<ThreadId>>,
    value: UnsafeCell<Option<T>>,
    borrowed: UnsafeCell<bool>,
}

const UNINSTALLED: u8 = 0;
const INITIALIZING: u8 = 1;
const INSTALLED: u8 = 2;

impl<T> ThreadOwned<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(UNINSTALLED),
            owner: UnsafeCell::new(None),
            value: UnsafeCell::new(None),
            borrowed: UnsafeCell::new(false),
        }
    }

    /// Installs `value` and binds the slot to the current thread.
    ///
    /// Returns the value unchanged when the slot is already installed or was
    /// bound by another thread.
    pub fn install(&self, value: T) -> Result<(), T> {
        let current = thread::current().id();
        if self
            .state
            .compare_exchange(
                UNINSTALLED,
                INITIALIZING,
                Ordering::Acquire,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(value);
        }

        // SAFETY: the initializing state makes this the only successful
        // installation, while readers refuse to touch the cells until the
        // release publication below.
        let owner = unsafe { &mut *self.owner.get() };
        *owner = Some(current);
        let slot = unsafe { &mut *self.value.get() };
        debug_assert!(slot.is_none(), "claimed ThreadOwned slot is empty");
        *slot = Some(value);
        self.state.store(INSTALLED, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn with_mut<R>(&self, operation: impl FnOnce(&mut T) -> R) -> Result<R, ThreadOwnedError> {
        if self.state.load(Ordering::Acquire) != INSTALLED {
            return Err(ThreadOwnedError::NotInstalled);
        }
        let current = thread::current().id();
        let owner = unsafe { &*self.owner.get() };
        match owner {
            None => return Err(ThreadOwnedError::NotInstalled),
            Some(owner) if *owner != current => return Err(ThreadOwnedError::WrongThread),
            Some(_) => {}
        }

        // SAFETY: `owner` matches the current thread, so no other live thread
        // can access these cells. The flag rejects reentrant mutable access on
        // that owner thread.
        let borrowed = unsafe { &mut *self.borrowed.get() };
        if *borrowed {
            return Err(ThreadOwnedError::AlreadyBorrowed);
        }
        *borrowed = true;
        let guard = ThreadOwnedBorrow { slot: self };

        // SAFETY: the owner and reentrancy checks above establish exclusive
        // access until `guard` resets the borrow flag, including during unwind.
        let value = unsafe { &mut *self.value.get() }
            .as_mut()
            .ok_or(ThreadOwnedError::NotInstalled)?;
        let result = operation(value);
        drop(guard);
        Ok(result)
    }

    /// Temporarily binds an installed value to the current thread, restores
    /// the previous owner on normal return and unwind, and grants one `&mut T`.
    ///
    /// # Safety
    /// The caller must prove for the complete scope that:
    /// - the installed value and its storage remain alive;
    /// - the previous owner is quiescent and cannot call `install`, `with_mut`,
    ///   `migrate`, or otherwise access `T`;
    /// - no mutable or shared borrow into `T` is alive when migration begins;
    /// - no other thread can start an access until the previous owner is restored;
    /// - transferring temporary exclusive access to the current thread is valid
    ///   for `T: Send`.
    pub unsafe fn migrate<R>(
        &self,
        operation: impl FnOnce(&mut T) -> R,
    ) -> Result<R, ThreadOwnedError>
    where
        T: Send,
    {
        if self.state.load(Ordering::Acquire) != INSTALLED {
            return Err(ThreadOwnedError::NotInstalled);
        }

        // SAFETY: the caller has proven that no source-thread access is alive
        // for the complete migration scope.
        let borrowed = unsafe { &mut *self.borrowed.get() };
        if *borrowed {
            return Err(ThreadOwnedError::AlreadyBorrowed);
        }
        let previous = unsafe { &mut *self.owner.get() }
            .take()
            .ok_or(ThreadOwnedError::NotInstalled)?;

        // SAFETY: source access is quiescent and the value remains installed.
        // The borrow flag prevents nested migration from the current thread.
        *borrowed = true;
        unsafe {
            *self.owner.get() = Some(thread::current().id());
        }

        let result = catch_unwind(AssertUnwindSafe(|| -> Result<R, ThreadOwnedError> {
            // SAFETY: the owner now matches the current thread and the borrow
            // flag is set until the caller restores it below.
            let value = unsafe { &mut *self.value.get() }
                .as_mut()
                .ok_or(ThreadOwnedError::NotInstalled)?;
            Ok(operation(value))
        }));

        // SAFETY: this migration scope is the only active writer. Restoring
        // the previous owner releases the temporary current-thread binding.
        unsafe {
            *self.owner.get() = Some(previous);
            *self.borrowed.get() = false;
        }
        match result {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

impl<T> Default for ThreadOwned<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for ThreadOwned<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadOwned")
            .field("installed", &(self.state.load(Ordering::Relaxed) == INSTALLED))
            .finish_non_exhaustive()
    }
}

struct ThreadOwnedBorrow<'a, T> {
    slot: &'a ThreadOwned<T>,
}

impl<T> Drop for ThreadOwnedBorrow<'_, T> {
    fn drop(&mut self) {
        // SAFETY: a guard exists only on the bound owner thread while this
        // borrow is active. Dropping the guard ends that exclusive borrow.
        unsafe {
            *self.slot.borrowed.get() = false;
        }
    }
}

// SAFETY: `T` enters the slot on its owner thread and can be dropped after the
// container moves only when `T: Send`.
unsafe impl<T: Send> Send for ThreadOwned<T> {}

// SAFETY: shared cross-thread access checks the installing ThreadId before
// touching either UnsafeCell. Only the owner can create a mutable reference,
// and the borrow flag rejects reentrant owner access.
unsafe impl<T: Send> Sync for ThreadOwned<T> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn installing_thread_mutates_value() {
        let slot = ThreadOwned::new();
        slot.install(41usize).expect("install value");
        let value = slot
            .with_mut(|value| {
                *value += 1;
                *value
            })
            .expect("access installed value");
        assert_eq!(value, 42);
    }

    #[test]
    fn nested_mutable_access_is_rejected() {
        let slot = ThreadOwned::new();
        slot.install(1usize).expect("install value");
        slot.with_mut(|_| {
            assert_eq!(
                slot.with_mut(|_| ()),
                Err(ThreadOwnedError::AlreadyBorrowed)
            );
        })
        .expect("outer access");
    }

    #[test]
    fn another_thread_cannot_access_installed_value() {
        let slot = Arc::new(ThreadOwned::new());
        slot.install(1usize).expect("install value");
        let other = Arc::clone(&slot);
        let error = std::thread::spawn(move || other.with_mut(|_| ()))
            .join()
            .expect("join thread")
            .expect_err("reject non-owner access");
        assert_eq!(error, ThreadOwnedError::WrongThread);
    }

    #[test]
    fn migration_restores_owner_after_unwind() {
        let slot = Arc::new(ThreadOwned::new());
        slot.install(1usize).expect("install value");
        let other = Arc::clone(&slot);
        let joined = std::thread::spawn(move || {
            // SAFETY: the installing thread is joined before the owner is
            // accessed again, so the value is quiescent for this scope.
            unsafe { other.migrate(|_| panic!("migration unwind")) }
        })
        .join();
        assert!(joined.is_err());
        assert_eq!(slot.with_mut(|value| *value += 1), Ok(()));
    }
}
