use std::cell::{RefCell, RefMut, UnsafeCell};
use std::fmt;
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
    value: RefCell<Option<T>>,
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
            value: RefCell::new(None),
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
        let slot = unsafe { &mut *self.value.as_ptr() };
        debug_assert!(slot.is_none(), "claimed ThreadOwned slot is empty");
        *slot = Some(value);
        self.state.store(INSTALLED, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn borrow_mut(&self) -> Result<RefMut<'_, T>, ThreadOwnedError> {
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

        RefMut::filter_map(
            self.value
                .try_borrow_mut()
                .map_err(|_| ThreadOwnedError::AlreadyBorrowed)?,
            |value| value.as_mut(),
        )
        .map_err(|_| ThreadOwnedError::NotInstalled)
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
        let value = self
            .value
            .try_borrow_mut()
            .map_err(|_| ThreadOwnedError::AlreadyBorrowed)?
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

// SAFETY: shared cross-thread access checks the installing ThreadId before
// touching the owner-only value. `RefCell` rejects reentrant owner access.
unsafe impl<T: Send> Sync for ThreadOwned<T> {}

#[cfg(test)]
mod tests {
    use super::{ThreadOwned, ThreadOwnedError};

    #[test]
    fn owner_can_borrow_and_clear() {
        let slot = ThreadOwned::new();
        slot.install(7_u32).expect("first install succeeds");
        *slot.borrow_mut().expect("owner borrow succeeds") = 9;
        assert_eq!(*slot.borrow_mut().expect("second borrow succeeds"), 9);
        assert_eq!(slot.clear().expect("owner clear succeeds"), 9);
        assert!(matches!(
            slot.borrow_mut(),
            Err(ThreadOwnedError::NotInstalled)
        ));
    }

    #[test]
    fn non_owner_cannot_borrow_or_clear() {
        let slot = std::sync::Arc::new(ThreadOwned::new());
        slot.install(7_u32).expect("first install succeeds");
        let worker = std::sync::Arc::clone(&slot);
        let result = std::thread::spawn(move || {
            (
                worker.borrow_mut().unwrap_err(),
                worker.clear().unwrap_err(),
            )
        })
        .join()
        .expect("worker exits");
        assert_eq!(
            result,
            (ThreadOwnedError::WrongThread, ThreadOwnedError::WrongThread)
        );
    }
}
