use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;

use crate::error::{RuntimeError, RuntimeResult};

use super::{
    FILE_POOL_CAPACITY, POLL_BATCH_SIZE, PollEvent, PollSpec, PollTarget, Readiness, decode_index,
    encode_index,
};

const DEADLINE_TOKEN_BIT: u64 = 1 << 63;

pub(super) struct Poller {
    kqueue: OwnedFd,
    deadline_durations: [Option<Duration>; FILE_POOL_CAPACITY],
    deadline_armed: [bool; FILE_POOL_CAPACITY],
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
        Ok(Self {
            kqueue,
            deadline_durations: [None; FILE_POOL_CAPACITY],
            deadline_armed: [false; FILE_POOL_CAPACITY],
        })
    }

    /// The kqueue descriptor itself is readable while events are pending; the
    /// idle loop sleeps in the tokio reactor yet wakes on File readiness,
    /// matching VPP sleeping inside `epoll_wait` (`vlib_file_poll`).
    pub(super) fn try_clone_wake(&self) -> io::Result<OwnedFd> {
        self.kqueue.try_clone()
    }

    pub(super) fn clear_wake(&self) {
        // The next kevent poll consumes pending events; nothing to drain here.
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

    pub(super) fn add_deadline(&mut self, index: super::Index) -> RuntimeResult<()> {
        self.deadline_durations[index.slot() as usize] = None;
        self.deadline_armed[index.slot() as usize] = false;
        Ok(())
    }

    pub(super) fn set_deadline(
        &mut self,
        index: super::Index,
        duration: Option<Duration>,
    ) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        if self.deadline_durations.get(slot).is_none() {
            return Err(crate::error::RuntimeError::DeadlineIndexInvalid { index });
        }
        if self.deadline_armed[slot] {
            self.delete_timer(index)?;
            self.deadline_armed[slot] = false;
        }
        if let Some(duration) = duration {
            self.add_timer(index, duration)?;
            self.deadline_armed[slot] = true;
        }
        self.deadline_durations[slot] = duration;
        Ok(())
    }

    pub(super) fn delete_deadline(&mut self, index: super::Index) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        if self.deadline_durations.get(slot).is_none() {
            return Err(crate::error::RuntimeError::DeadlineIndexInvalid { index });
        }
        if self.deadline_armed[slot] {
            self.delete_timer(index)?;
        }
        self.deadline_durations[slot] = None;
        self.deadline_armed[slot] = false;
        Ok(())
    }

    pub(super) fn consume_deadline(&mut self, index: super::Index) -> RuntimeResult<()> {
        if self.deadline_durations.get(index.slot() as usize).is_none() {
            return Err(crate::error::RuntimeError::DeadlineIndexInvalid { index });
        }
        self.deadline_armed[index.slot() as usize] = false;
        Ok(())
    }

    pub(super) fn rearm_deadline(&mut self, index: super::Index) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        let Some(duration) = self.deadline_durations[slot] else {
            return Ok(());
        };
        self.add_timer(index, duration)?;
        self.deadline_armed[slot] = true;
        Ok(())
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
            return Err(RuntimeError::FilePollerIo {
                operation: "poll kqueue",
                source: error,
            });
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
            let token = event.udata as usize as u64;
            let target = if token & DEADLINE_TOKEN_BIT != 0 {
                decode_index(token & !DEADLINE_TOKEN_BIT).map(PollTarget::Deadline)
            } else {
                decode_index(token).map(PollTarget::File)
            };
            *ready = PollEvent {
                target,
                readiness,
                rearm: event.filter == libc::EVFILT_TIMER,
            };
        }
        Ok(count)
    }

    fn update(&self, before: Option<PollSpec>, after: Option<PollSpec>) -> RuntimeResult<()> {
        let Some(spec) = after.or(before) else {
            unreachable!("kqueue update requires an old or new File poll spec");
        };
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

    fn add_timer(&self, index: super::Index, duration: Duration) -> RuntimeResult<()> {
        let event = timer_event(index, duration)?;
        // SAFETY: `event` is initialized and kqueue only reads the change
        // record during this synchronous call.
        let result = unsafe {
            libc::kevent(
                self.kqueue.as_raw_fd(),
                std::ptr::from_ref(&event),
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            return Err(os_error("arm kqueue File deadline"));
        }
        Ok(())
    }

    fn delete_timer(&self, index: super::Index) -> RuntimeResult<()> {
        let event = timer_delete_event(index);
        // SAFETY: `event` is initialized and kqueue only reads the change
        // record during this synchronous call.
        let result = unsafe {
            libc::kevent(
                self.kqueue.as_raw_fd(),
                std::ptr::from_ref(&event),
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(RuntimeError::FilePollerIo {
                operation: "disarm kqueue File deadline",
                source: error,
            });
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

fn timer_event(index: super::Index, duration: Duration) -> RuntimeResult<libc::kevent> {
    let nanoseconds = duration.as_nanos().max(1);
    let nanoseconds = isize::try_from(nanoseconds).map_err(|_| RuntimeError::FilePollerIo {
        operation: "arm kqueue File deadline",
        source: io::Error::from_raw_os_error(libc::EOVERFLOW),
    })?;
    Ok(libc::kevent {
        ident: deadline_ident(index),
        filter: libc::EVFILT_TIMER,
        flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
        fflags: libc::NOTE_NSECONDS,
        data: nanoseconds,
        udata: (DEADLINE_TOKEN_BIT | encode_index(index)) as usize as *mut libc::c_void,
    })
}

fn timer_delete_event(index: super::Index) -> libc::kevent {
    libc::kevent {
        ident: deadline_ident(index),
        filter: libc::EVFILT_TIMER,
        flags: libc::EV_DELETE,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn deadline_ident(index: super::Index) -> libc::uintptr_t {
    usize::MAX - index.slot() as usize
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

fn os_error(operation: &'static str) -> RuntimeError {
    RuntimeError::FilePollerIo {
        operation,
        source: io::Error::last_os_error(),
    }
}
