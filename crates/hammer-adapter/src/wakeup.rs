use std::io;

/// Cross-platform worker wakeup mechanism. Mirrors VPP's eventfd/kqueue
/// pattern for waking a worker thread blocked in epoll/kqueue.
pub trait WakeupFd {
    /// Wake the worker. Posts a wakeup event (thread-safe).
    fn wake(&self);
    /// Consume the wakeup event (called by the worker after waking).
    fn consume(&self);
    /// Raw file descriptor for integration with epoll/kqueue.
    fn raw_fd(&self) -> i32;
}

// ---------------------------------------------------------------------------
// Linux: eventfd
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
pub struct LinuxEventfdWakeup {
    fd: i32,
}

#[cfg(target_os = "linux")]
impl LinuxEventfdWakeup {
    pub fn new() -> io::Result<Self> {
        // SAFETY: eventfd creates a new eventfd object; EFD_NONBLOCK and
        // EFD_CLOEXEC are safe flag values.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd })
    }
}

#[cfg(target_os = "linux")]
impl WakeupFd for LinuxEventfdWakeup {
    fn wake(&self) {
        let val: u64 = 1;
        // SAFETY: write is safe on a valid eventfd fd; EAGAIN when the
        // counter would overflow is harmless (already woken).
        let ret = unsafe { libc::write(self.fd, &val as *const u64 as *const libc::c_void, 8) };
        if ret == -1 {
            let _ = io::Error::last_os_error();
        }
    }

    fn consume(&self) {
        let mut val: u64 = 0;
        // SAFETY: read is safe on a valid eventfd fd; EAGAIN when no
        // wakeup is pending is harmless.
        let ret = unsafe { libc::read(self.fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
        if ret == -1 {
            let _ = io::Error::last_os_error();
        }
    }

    fn raw_fd(&self) -> i32 {
        self.fd
    }
}

#[cfg(target_os = "linux")]
impl Drop for LinuxEventfdWakeup {
    fn drop(&mut self) {
        // SAFETY: close is safe on a valid fd; self.fd is valid because
        // it came from eventfd and has not been closed yet.
        unsafe {
            libc::close(self.fd);
        }
    }
}

// ---------------------------------------------------------------------------
// macOS: kqueue + EVFILT_USER
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
pub struct MacosKqueueWakeup {
    kq: i32,
    ident: usize,
}

#[cfg(target_os = "macos")]
impl MacosKqueueWakeup {
    pub fn new() -> io::Result<Self> {
        // SAFETY: kqueue() allocates a new kqueue fd.
        let kq = unsafe { libc::kqueue() };
        if kq == -1 {
            return Err(io::Error::last_os_error());
        }

        // Register an EVFILT_USER event that can be triggered from
        // another thread to wake the worker.
        let mut change = libc::kevent {
            ident: 1,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR | libc::EV_RECEIPT,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };

        // SAFETY: kevent modifies the kevent struct we supply as
        // eventlist; EV_RECEIPT fills in the result in-place.
        let ret = unsafe { libc::kevent(kq, &change, 1, &mut change, 1, std::ptr::null()) };
        if ret == -1 {
            let err = io::Error::last_os_error();
            // SAFETY: kq is a valid open fd we just created.
            unsafe {
                libc::close(kq);
            }
            return Err(err);
        }
        // EV_RECEIPT reports errors in the event itself.
        if (change.flags & libc::EV_ERROR) != 0 && change.data != 0 {
            // SAFETY: kq is a valid open fd we just created.
            unsafe {
                libc::close(kq);
            }
            return Err(io::Error::from_raw_os_error(change.data as i32));
        }

        Ok(Self { kq, ident: 1 })
    }
}

#[cfg(target_os = "macos")]
impl WakeupFd for MacosKqueueWakeup {
    fn wake(&self) {
        let ev = libc::kevent {
            ident: self.ident,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
            fflags: libc::NOTE_TRIGGER | libc::NOTE_FFNOP,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: kevent is safe on a valid kq; NOTE_TRIGGER fires the
        // user event, waking the worker blocked in kevent().
        let ret =
            unsafe { libc::kevent(self.kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
        if ret == -1 {
            let _ = io::Error::last_os_error();
        }
    }

    fn consume(&self) {
        let ev = libc::kevent {
            ident: self.ident,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: kevent with EV_CLEAR clears the triggered state of
        // the EVFILT_USER event so it is ready for the next wake.
        let ret =
            unsafe { libc::kevent(self.kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
        if ret == -1 {
            let _ = io::Error::last_os_error();
        }
    }

    fn raw_fd(&self) -> i32 {
        self.kq
    }
}

#[cfg(target_os = "macos")]
impl Drop for MacosKqueueWakeup {
    fn drop(&mut self) {
        // SAFETY: close is safe on a valid kq fd.
        unsafe {
            libc::close(self.kq);
        }
    }
}

// ---------------------------------------------------------------------------
// Platform-generic factory
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
pub fn new_wakeup_fd() -> io::Result<impl WakeupFd> {
    LinuxEventfdWakeup::new()
}

#[cfg(target_os = "macos")]
pub fn new_wakeup_fd() -> io::Result<impl WakeupFd> {
    MacosKqueueWakeup::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wakeup_self_roundtrip() {
        let w = new_wakeup_fd().unwrap();
        w.wake();
        w.consume();
        // Second consume should be a no-op.
        w.consume();
    }
}
