use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use crate::error::{RuntimeError, RuntimeResult};

use super::{POLL_BATCH_SIZE, PollEvent, PollSpec, Readiness, decode_index, encode_index};

pub(super) struct Poller {
    kqueue: OwnedFd,
}

impl Poller {
    pub(super) fn new() -> RuntimeResult<Self> {
        // SAFETY: `kqueue` takes no pointers and returns a fresh descriptor or
        // -1 with errno set.
        let fd = unsafe { libc::kqueue() };
        if fd < 0 {
            return Err(os_error("create kqueue"));
        }
        // SAFETY: ownership of the fresh kqueue descriptor is transferred once.
        let kqueue = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self { kqueue })
    }

    pub(super) fn add(&self, spec: PollSpec) -> RuntimeResult<()> {
        self.update(None, Some(spec))
    }

    pub(super) fn modify(&self, before: PollSpec, after: PollSpec) -> RuntimeResult<()> {
        self.update(Some(before), Some(after))
    }

    pub(super) fn delete(&self, spec: PollSpec) -> RuntimeResult<()> {
        self.update(Some(spec), None)
    }

    pub(super) fn poll(&self, ready: &mut [PollEvent; POLL_BATCH_SIZE]) -> RuntimeResult<usize> {
        let mut events = [empty_event(); POLL_BATCH_SIZE];
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: the kqueue descriptor is live, `events` is writable for the
        // requested count, and `timeout` remains valid for this call.
        let count = unsafe {
            libc::kevent(
                self.kqueue.as_raw_fd(),
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                POLL_BATCH_SIZE as i32,
                &timeout,
            )
        };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(0);
            }
            return Err(RuntimeError::invariant(format!("poll kqueue: {error}")));
        }

        let count = count as usize;
        for (event, ready) in events[..count].iter().zip(&mut ready[..count]) {
            let mut readiness = Readiness::default();
            if event.filter == libc::EVFILT_READ {
                readiness.insert(Readiness::READ);
            }
            if event.filter == libc::EVFILT_WRITE {
                readiness.insert(Readiness::WRITE);
            }
            if event.flags & (libc::EV_ERROR | libc::EV_EOF) != 0 {
                readiness.insert(Readiness::ERROR);
            }
            *ready = PollEvent {
                index: decode_index(event.udata as usize as u64),
                readiness,
                rearm: false,
            };
        }
        Ok(count)
    }

    fn update(&self, before: Option<PollSpec>, after: Option<PollSpec>) -> RuntimeResult<()> {
        let spec = after.or(before).ok_or_else(|| {
            RuntimeError::invariant("kqueue update requires an old or new File poll spec")
        })?;
        let mut changes = [empty_event(); 2];
        let mut count = 0;

        let before_read = before.is_some_and(|spec| spec.read);
        let after_read = after.is_some_and(|spec| spec.read);
        if before_read != after_read {
            changes[count] = change_event(spec, libc::EVFILT_READ, after_read);
            count += 1;
        }

        let before_write = before.is_some_and(|spec| spec.write);
        let after_write = after.is_some_and(|spec| spec.write);
        if before_write != after_write {
            changes[count] = change_event(spec, libc::EVFILT_WRITE, after_write);
            count += 1;
        }

        if count == 0 {
            return Ok(());
        }
        // SAFETY: `changes[..count]` contains initialized kevent records;
        // kqueue only reads that array and no output buffer is supplied.
        let result = unsafe {
            libc::kevent(
                self.kqueue.as_raw_fd(),
                changes.as_ptr(),
                count as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            return Err(os_error("update kqueue File interest"));
        }
        Ok(())
    }
}

fn change_event(spec: PollSpec, filter: i16, enable: bool) -> libc::kevent {
    libc::kevent {
        ident: spec.fd as libc::uintptr_t,
        filter,
        flags: if enable {
            libc::EV_ADD | libc::EV_ENABLE
        } else {
            libc::EV_DELETE
        },
        fflags: 0,
        data: 0,
        udata: encode_index(spec.index) as usize as *mut libc::c_void,
    }
}

const fn empty_event() -> libc::kevent {
    libc::kevent {
        ident: 0,
        filter: 0,
        flags: 0,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn os_error(operation: &str) -> RuntimeError {
    RuntimeError::invariant(format!("{operation}: {}", io::Error::last_os_error()))
}
