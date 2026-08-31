use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
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
    pub fn borrow_mut(&self) -> Result<ThreadOwnedBorrow<'_, T>, ThreadOwnedError> {
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
        Ok(ThreadOwnedBorrow { slot: self })
    }

    /// Clears an installed value on its owner thread before the slot is dropped.
    pub fn clear(&self) -> Result<T, ThreadOwnedError> {
        if self.state.load(Ordering::Acquire) != INSTALLED {
            return Err(ThreadOwnedError::NotInstalled);
        }
        if thread::current().id()
            != unsafe { (*self.owner.get()).ok_or(ThreadOwnedError::NotInstalled)? }
        {
            return Err(ThreadOwnedError::WrongThread);
        }
        if unsafe { *self.borrowed.get() } {
            return Err(ThreadOwnedError::AlreadyBorrowed);
        }
        let value = unsafe { &mut *self.value.get() }
            .take()
            .ok_or(ThreadOwnedError::NotInstalled)?;
        unsafe {
            *self.owner.get() = None;
        }
        self.state.store(UNINSTALLED, Ordering::Release);
        Ok(value)
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
            .field(
                "installed",
                &(self.state.load(Ordering::Relaxed) == INSTALLED),
            )
            .finish_non_exhaustive()
    }
}

pub struct ThreadOwnedBorrow<'a, T> {
    slot: &'a ThreadOwned<T>,
}

impl<T> Deref for ThreadOwnedBorrow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            (*self.slot.value.get())
                .as_ref()
                .expect("installed ThreadOwned value is missing")
        }
    }
}

impl<T> DerefMut for ThreadOwnedBorrow<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            (*self.slot.value.get())
                .as_mut()
                .expect("installed ThreadOwned value is missing")
        }
    }
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

// SAFETY: shared cross-thread access checks the installing ThreadId before
// touching either UnsafeCell. Only the owner can create a mutable reference,
// and the borrow flag rejects reentrant owner access.
unsafe impl<T: Send> Sync for ThreadOwned<T> {}
