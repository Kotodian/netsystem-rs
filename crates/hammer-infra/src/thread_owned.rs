use std::cell::UnsafeCell;
use std::fmt;
use std::sync::OnceLock;
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
    owner: OnceLock<ThreadId>,
    value: UnsafeCell<Option<T>>,
    borrowed: UnsafeCell<bool>,
}

impl<T> ThreadOwned<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            owner: OnceLock::new(),
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
        match self.owner.set(current) {
            Ok(()) => {}
            Err(_) if self.owner.get() == Some(&current) => {}
            Err(_) => return Err(value),
        }

        // SAFETY: only the bound owner thread can reach the value. Installation
        // precedes publication to callers, and an installed value is never
        // removed or replaced.
        let slot = unsafe { &mut *self.value.get() };
        if slot.is_some() {
            return Err(value);
        }
        *slot = Some(value);
        Ok(())
    }

    #[inline]
    pub fn with_mut<R>(&self, operation: impl FnOnce(&mut T) -> R) -> Result<R, ThreadOwnedError> {
        let current = thread::current().id();
        match self.owner.get() {
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
            .field("installed", &self.owner.get().is_some())
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
}
