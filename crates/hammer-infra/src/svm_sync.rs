//! Process-shared robust synchronization used by SVM owners.

use std::fmt;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedSyncError {
    Unsupported,
    InvalidAttribute { operation: &'static str, code: i32 },
    Lock { code: i32 },
    OwnerDied,
    NotRecoverable,
    Wait { code: i32 },
}

impl fmt::Display for SharedSyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => {
                formatter.write_str("process-shared synchronization is unsupported")
            }
            Self::InvalidAttribute { operation, code } => {
                write!(formatter, "{operation} failed with code {code}")
            }
            Self::Lock { code } => write!(formatter, "shared mutex lock failed with code {code}"),
            Self::OwnerDied => formatter.write_str("shared mutex owner died"),
            Self::NotRecoverable => formatter.write_str("shared mutex is not recoverable"),
            Self::Wait { code } => {
                write!(formatter, "shared condition wait failed with code {code}")
            }
        }
    }
}

impl std::error::Error for SharedSyncError {}

/// Owner of a process-shared robust mutex placed in an SVM mapping.
pub struct SharedMutex {
    pointer: NonNull<libc::pthread_mutex_t>,
}

unsafe impl Send for SharedMutex {}
unsafe impl Sync for SharedMutex {}

impl SharedMutex {
    /// Initializes a mutex in caller-owned shared storage.
    pub unsafe fn init_at(pointer: *mut libc::pthread_mutex_t) -> Result<Self, SharedSyncError> {
        let pointer = NonNull::new(pointer).ok_or(SharedSyncError::Unsupported)?;
        #[cfg(target_os = "linux")]
        {
            let attributes = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();
            let mut attributes = unsafe { attributes.assume_init() };
            let code = unsafe { libc::pthread_mutexattr_init(&mut attributes) };
            if code != 0 {
                return Err(SharedSyncError::InvalidAttribute {
                    operation: "pthread_mutexattr_init",
                    code,
                });
            }
            let code = unsafe {
                libc::pthread_mutexattr_setpshared(&mut attributes, libc::PTHREAD_PROCESS_SHARED)
            };
            if code == 0 {
                let code = unsafe {
                    libc::pthread_mutexattr_setrobust(&mut attributes, libc::PTHREAD_MUTEX_ROBUST)
                };
                if code != 0 {
                    unsafe {
                        libc::pthread_mutexattr_destroy(&mut attributes);
                    }
                    return Err(SharedSyncError::InvalidAttribute {
                        operation: "pthread_mutexattr_setrobust",
                        code,
                    });
                }
                let code = unsafe { libc::pthread_mutex_init(pointer.as_ptr(), &attributes) };
                unsafe {
                    libc::pthread_mutexattr_destroy(&mut attributes);
                }
                if code != 0 {
                    return Err(SharedSyncError::InvalidAttribute {
                        operation: "pthread_mutex_init",
                        code,
                    });
                }
                return Ok(Self { pointer });
            }
            unsafe {
                libc::pthread_mutexattr_destroy(&mut attributes);
            }
            Err(SharedSyncError::InvalidAttribute {
                operation: "pthread_mutexattr_setpshared",
                code,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pointer;
            Err(SharedSyncError::Unsupported)
        }
    }

    /// Borrows an already initialized mutex in an attached mapping.
    pub unsafe fn attach(pointer: *mut libc::pthread_mutex_t) -> Result<Self, SharedSyncError> {
        NonNull::new(pointer)
            .map(|pointer| Self { pointer })
            .ok_or(SharedSyncError::Unsupported)
    }

    pub fn lock(&self) -> Result<SharedMutexGuard<'_>, SharedSyncError> {
        let code = unsafe { libc::pthread_mutex_lock(self.pointer.as_ptr()) };
        match code {
            0 => Ok(SharedMutexGuard {
                mutex: self,
                locked: true,
            }),
            code if code == libc::EOWNERDEAD => {
                // The caller owns an inconsistent mutex after EOWNERDEAD.
                // Unlocking without pthread_mutex_consistent permanently
                // revokes the old queue identity instead of exposing data.
                let unlock_code = unsafe { libc::pthread_mutex_unlock(self.pointer.as_ptr()) };
                assert_eq!(
                    unlock_code, 0,
                    "owner-death mutex unlock failed with code {unlock_code}"
                );
                Err(SharedSyncError::OwnerDied)
            }
            code if code == libc::ENOTRECOVERABLE => Err(SharedSyncError::NotRecoverable),
            code => Err(SharedSyncError::Lock { code }),
        }
    }
}

pub struct SharedMutexGuard<'a> {
    mutex: &'a SharedMutex,
    locked: bool,
}

impl SharedMutexGuard<'_> {
    pub fn mutex(&self) -> &SharedMutex {
        self.mutex
    }
}

impl Drop for SharedMutexGuard<'_> {
    fn drop(&mut self) {
        if self.locked {
            let code = unsafe { libc::pthread_mutex_unlock(self.mutex.pointer.as_ptr()) };
            assert_eq!(code, 0, "shared mutex unlock failed with code {code}");
            self.locked = false;
        }
    }
}

/// Process-shared monotonic condition variable paired with [`SharedMutex`].
pub struct SharedCondvar {
    pointer: NonNull<libc::pthread_cond_t>,
}

unsafe impl Send for SharedCondvar {}
unsafe impl Sync for SharedCondvar {}

impl SharedCondvar {
    pub unsafe fn init_at(pointer: *mut libc::pthread_cond_t) -> Result<Self, SharedSyncError> {
        let pointer = NonNull::new(pointer).ok_or(SharedSyncError::Unsupported)?;
        #[cfg(target_os = "linux")]
        {
            let attributes = MaybeUninit::<libc::pthread_condattr_t>::uninit();
            let mut attributes = unsafe { attributes.assume_init() };
            let code = unsafe { libc::pthread_condattr_init(&mut attributes) };
            if code != 0 {
                return Err(SharedSyncError::InvalidAttribute {
                    operation: "pthread_condattr_init",
                    code,
                });
            }
            let code = unsafe {
                libc::pthread_condattr_setpshared(&mut attributes, libc::PTHREAD_PROCESS_SHARED)
            };
            if code == 0 {
                let code = unsafe {
                    libc::pthread_condattr_setclock(&mut attributes, libc::CLOCK_MONOTONIC)
                };
                if code != 0 {
                    unsafe {
                        libc::pthread_condattr_destroy(&mut attributes);
                    }
                    return Err(SharedSyncError::InvalidAttribute {
                        operation: "pthread_condattr_setclock",
                        code,
                    });
                }
                let code = unsafe { libc::pthread_cond_init(pointer.as_ptr(), &attributes) };
                unsafe {
                    libc::pthread_condattr_destroy(&mut attributes);
                }
                if code != 0 {
                    return Err(SharedSyncError::InvalidAttribute {
                        operation: "pthread_cond_init",
                        code,
                    });
                }
                return Ok(Self { pointer });
            }
            unsafe {
                libc::pthread_condattr_destroy(&mut attributes);
            }
            Err(SharedSyncError::InvalidAttribute {
                operation: "pthread_condattr_setpshared",
                code,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pointer;
            Err(SharedSyncError::Unsupported)
        }
    }

    pub unsafe fn attach(pointer: *mut libc::pthread_cond_t) -> Result<Self, SharedSyncError> {
        NonNull::new(pointer)
            .map(|pointer| Self { pointer })
            .ok_or(SharedSyncError::Unsupported)
    }

    pub fn wait(&self, guard: &mut SharedMutexGuard<'_>) -> Result<(), SharedSyncError> {
        let code =
            unsafe { libc::pthread_cond_wait(self.pointer.as_ptr(), guard.mutex.pointer.as_ptr()) };
        match code {
            0 => Ok(()),
            code if code == libc::EOWNERDEAD => {
                guard.locked = false;
                let unlock_code =
                    unsafe { libc::pthread_mutex_unlock(guard.mutex.pointer.as_ptr()) };
                assert_eq!(
                    unlock_code, 0,
                    "owner-death mutex unlock failed with code {unlock_code}"
                );
                Err(SharedSyncError::OwnerDied)
            }
            code if code == libc::ENOTRECOVERABLE => Err(SharedSyncError::NotRecoverable),
            code => Err(SharedSyncError::Wait { code }),
        }
    }

    pub fn notify_one(&self) -> Result<(), SharedSyncError> {
        let code = unsafe { libc::pthread_cond_signal(self.pointer.as_ptr()) };
        if code == 0 {
            Ok(())
        } else {
            Err(SharedSyncError::Wait { code })
        }
    }

    pub fn notify_all(&self) -> Result<(), SharedSyncError> {
        let code = unsafe { libc::pthread_cond_broadcast(self.pointer.as_ptr()) };
        if code == 0 {
            Ok(())
        } else {
            Err(SharedSyncError::Wait { code })
        }
    }
}
