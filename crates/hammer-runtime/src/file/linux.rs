use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_infra::pool::Index;
use hammer_infra::ring::LocalRing;
use io_uring::{IoUring, Probe, cqueue, opcode, squeue, types};

use super::{FILE_POOL_CAPACITY, POLL_BATCH_SIZE, PollEvent, PollSpec, PollTarget, Readiness};

const CONTROL_TOKEN: u64 = 0;
const PROBE_TOKEN: u64 = u64::MAX;
const SLOT_BITS: u32 = FILE_POOL_CAPACITY.trailing_zeros();
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
const INDEX_GENERATION_BITS: u32 = 32;
const REQUEST_SEQUENCE_SHIFT: u32 = SLOT_BITS + INDEX_GENERATION_BITS;
const REQUEST_SEQUENCE_MASK: u32 = (1 << (63 - REQUEST_SEQUENCE_SHIFT)) - 1;
const DEADLINE_TOKEN_BIT: u64 = 1 << 63;

#[derive(Clone, Copy)]
struct Completion {
    user_data: u64,
    result: i32,
    flags: u32,
}

pub(super) struct Poller {
    ring: IoUring,
    pending: LocalRing<Completion>,
    current_tokens: [u64; FILE_POOL_CAPACITY],
    deadline_tokens: [u64; FILE_POOL_CAPACITY],
    deadline_fds: [Option<OwnedFd>; FILE_POOL_CAPACITY],
    deadline_durations: [Option<Duration>; FILE_POOL_CAPACITY],
    request_sequence: u32,
    multishot: bool,
    wake: OwnedFd,
}

impl Poller {
    pub(super) fn new() -> RuntimeResult<Self> {
        let mut builder = IoUring::builder();
        builder.dontfork();
        let mut ring = builder
            .build(FILE_POOL_CAPACITY as u32)
            .map_err(|error| io_error("create worker io_uring", error))?;

        let mut probe = Probe::new();
        ring.submitter()
            .register_probe(&mut probe)
            .map_err(|error| io_error("probe worker io_uring operations", error))?;
        for (name, code) in [
            ("poll-add", opcode::PollAdd::CODE),
            ("poll-remove", opcode::PollRemove::CODE),
        ] {
            if !probe.is_supported(code) {
                return Err(
                    RuntimeError::FilePollerOperationUnsupported { operation: name }.into(),
                );
            }
        }

        let multishot = probe_multishot(&mut ring)?;

        // SAFETY: eventfd returns a fresh descriptor or -1 with errno set.
        let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake < 0 {
            return Err(io_error(
                "create worker io_uring wake eventfd",
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: ownership of the fresh eventfd descriptor is transferred once.
        let wake = unsafe { OwnedFd::from_raw_fd(wake) };
        ring.submitter()
            .register_eventfd(wake.as_raw_fd())
            .map_err(|error| io_error("register worker io_uring wake eventfd", error))?;

        let pending_capacity = ring.completion().capacity().saturating_mul(2);
        Ok(Self {
            ring,
            pending: LocalRing::with_capacity(pending_capacity),
            current_tokens: [0; FILE_POOL_CAPACITY],
            deadline_tokens: [0; FILE_POOL_CAPACITY],
            deadline_fds: std::array::from_fn(|_| None),
            deadline_durations: [None; FILE_POOL_CAPACITY],
            request_sequence: 0,
            multishot,
            wake,
        })
    }

    /// Becomes readable whenever the ring posts a completion; lets the idle
    /// loop sleep in the tokio reactor yet wake on File readiness, matching
    /// VPP sleeping inside `epoll_wait` (`vlib_file_poll`).
    pub(super) fn try_clone_wake(&self) -> io::Result<OwnedFd> {
        self.wake.try_clone()
    }

    pub(super) fn clear_wake(&self) {
        let mut count = [0u8; 8];
        // SAFETY: the eventfd is live and the buffer holds the 8-byte counter;
        // EAGAIN when already clear is expected and ignored.
        let _ = unsafe { libc::read(self.wake.as_raw_fd(), count.as_mut_ptr().cast(), 8) };
    }

    pub(super) fn add(&mut self, spec: PollSpec) -> RuntimeResult<()> {
        if !spec.read && !spec.write {
            self.current_tokens[spec.index.slot() as usize] = CONTROL_TOKEN;
            return Ok(());
        }

        self.add_poll(spec.index, spec.fd, poll_flags(spec), false)
    }

    pub(super) fn modify(&mut self, before: PollSpec, after: PollSpec) -> RuntimeResult<()> {
        self.cancel(before.index)?;
        self.add(after)
    }

    pub(super) fn delete(&mut self, spec: PollSpec) -> RuntimeResult<()> {
        self.cancel(spec.index)
    }

    pub(super) fn add_deadline(&mut self, index: Index) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        // SAFETY: timerfd_create returns a fresh descriptor or -1 with errno.
        let fd = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_CLOEXEC | libc::TFD_NONBLOCK,
            )
        };
        if fd < 0 {
            return Err(io_error(
                "create File deadline timerfd",
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: ownership of the fresh timerfd descriptor is transferred once.
        self.deadline_fds[slot] = Some(unsafe { OwnedFd::from_raw_fd(fd) });
        self.deadline_tokens[slot] = CONTROL_TOKEN;
        self.deadline_durations[slot] = None;
        Ok(())
    }

    pub(super) fn set_deadline(
        &mut self,
        index: Index,
        duration: Option<Duration>,
    ) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        let deadline_fd = self
            .deadline_fds
            .get(slot)
            .and_then(Option::as_ref)
            .map(|fd| fd.as_raw_fd())
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })?;
        match duration {
            Some(duration) => {
                set_timerfd(deadline_fd, Some(duration))?;
                if self.deadline_tokens[slot] == CONTROL_TOKEN {
                    if let Err(error) = self.add_deadline_poll(index, deadline_fd) {
                        if let Err(cleanup_error) = set_timerfd(deadline_fd, None) {
                            tracing::error!(
                                %cleanup_error,
                                "failed to disarm File deadline after poll registration failed"
                            );
                        }
                        return Err(error);
                    }
                }
                self.deadline_durations[slot] = Some(duration);
            }
            None => {
                self.cancel_deadline(index)?;
                set_timerfd(deadline_fd, None)?;
                self.deadline_durations[slot] = None;
            }
        }
        Ok(())
    }

    pub(super) fn delete_deadline(&mut self, index: Index) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        if self
            .deadline_fds
            .get(slot)
            .and_then(Option::as_ref)
            .is_none()
        {
            return Err(RuntimeError::DeadlineIndexInvalid { index }.into());
        }
        self.cancel_deadline(index)?;
        self.deadline_fds[slot] = None;
        self.deadline_durations[slot] = None;
        Ok(())
    }

    pub(super) fn consume_deadline(&mut self, index: Index) -> RuntimeResult<()> {
        let fd = self
            .deadline_fds
            .get(index.slot() as usize)
            .and_then(Option::as_ref)
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })?;
        let mut expirations = 0_u64;
        loop {
            // SAFETY: `expirations` is writable for one timerfd counter and the
            // deadline fd is owned by this worker's FileMain.
            let result = unsafe {
                libc::read(
                    fd.as_raw_fd(),
                    std::ptr::from_mut(&mut expirations).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if result == std::mem::size_of::<u64>() as isize {
                return Ok(());
            }
            if result < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if source.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                return Err(RuntimeError::FileRead { source }.into());
            }
            return Err(io_error(
                "consume File deadline timerfd",
                io::Error::from_raw_os_error(libc::EIO),
            ));
        }
    }

    pub(super) fn rearm_deadline(&mut self, index: Index) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        let Some(duration) = self.deadline_durations[slot] else {
            return Ok(());
        };
        let deadline_fd = self
            .deadline_fds
            .get(slot)
            .and_then(Option::as_ref)
            .map(|fd| fd.as_raw_fd())
            .ok_or(RuntimeError::DeadlineIndexInvalid { index })?;
        self.deadline_tokens[slot] = CONTROL_TOKEN;
        set_timerfd(deadline_fd, Some(duration))?;
        if let Err(error) = self.add_deadline_poll(index, deadline_fd) {
            if let Err(cleanup_error) = set_timerfd(deadline_fd, None) {
                tracing::error!(
                    %cleanup_error,
                    "failed to disarm File deadline after rearm registration failed"
                );
            }
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn poll(
        &mut self,
        ready: &mut [PollEvent; POLL_BATCH_SIZE],
    ) -> RuntimeResult<usize> {
        let mut count = 0;
        while count < ready.len() {
            let Some(completion) = self.pending.pop() else {
                break;
            };
            if let Some(event) = completion_event(
                completion,
                &self.current_tokens,
                &self.deadline_tokens,
                self.multishot,
            )? {
                ready[count] = event;
                count += 1;
            }
        }

        let multishot = self.multishot;
        let current_tokens = &self.current_tokens;
        let deadline_tokens = &self.deadline_tokens;
        let (ring, pending) = (&mut self.ring, &mut self.pending);
        let mut completions = ring.completion();
        for completion in &mut completions {
            let completion = Completion {
                user_data: completion.user_data(),
                result: completion.result(),
                flags: completion.flags(),
            };
            if count < ready.len() {
                if let Some(event) =
                    completion_event(completion, current_tokens, deadline_tokens, multishot)?
                {
                    ready[count] = event;
                    count += 1;
                }
            } else if pending.try_push(completion).is_err() {
                return Err(RuntimeError::FileCompletionQueueFull {
                    operation: "collecting readiness",
                }
                .into());
            }
        }
        Ok(count)
    }

    fn cancel(&mut self, index: Index) -> RuntimeResult<()> {
        self.cancel_token(index, false)
    }

    fn cancel_deadline(&mut self, index: Index) -> RuntimeResult<()> {
        self.cancel_token(index, true)
    }

    fn cancel_token(&mut self, index: Index, deadline: bool) -> RuntimeResult<()> {
        let slot = index.slot() as usize;
        let token = if deadline {
            self.deadline_tokens[slot]
        } else {
            self.current_tokens[slot]
        };
        if token == CONTROL_TOKEN {
            return Ok(());
        }

        let entry = opcode::PollRemove::new(token)
            .build()
            .user_data(CONTROL_TOKEN);
        self.submit(entry)?;

        loop {
            submit_and_wait(&self.ring)?;
            let mut result = None;
            let (ring, pending) = (&mut self.ring, &mut self.pending);
            let mut completions = ring.completion();
            for completion in &mut completions {
                let completion = Completion {
                    user_data: completion.user_data(),
                    result: completion.result(),
                    flags: completion.flags(),
                };
                if completion.user_data == CONTROL_TOKEN {
                    result = Some(completion.result);
                } else if completion.user_data != token && pending.try_push(completion).is_err() {
                    return Err(RuntimeError::FileCompletionQueueFull {
                        operation: "canceling readiness",
                    }
                    .into());
                }
            }
            drop(completions);

            if let Some(result) = result {
                if result != 0 && result != -libc::ENOENT {
                    return Err(completion_error(
                        if deadline {
                            "cancel File deadline readiness"
                        } else {
                            "cancel File readiness"
                        },
                        result,
                    ));
                }
                if deadline {
                    self.deadline_tokens[slot] = CONTROL_TOKEN;
                } else {
                    self.current_tokens[slot] = CONTROL_TOKEN;
                }
                return Ok(());
            }
        }
    }

    fn add_deadline_poll(&mut self, index: Index, fd: i32) -> RuntimeResult<()> {
        self.add_poll(index, fd, libc::POLLIN as u32, true)
    }

    fn add_poll(&mut self, index: Index, fd: i32, flags: u32, deadline: bool) -> RuntimeResult<()> {
        let token = self.next_token(index, deadline);
        let entry = opcode::PollAdd::new(types::Fd(fd), flags)
            .multi(!deadline && self.multishot)
            .build()
            .user_data(token);
        self.submit(entry)?;
        if deadline {
            self.deadline_tokens[index.slot() as usize] = token;
        } else {
            self.current_tokens[index.slot() as usize] = token;
        }
        Ok(())
    }

    fn next_token(&mut self, index: Index, deadline: bool) -> u64 {
        self.request_sequence = self.request_sequence.wrapping_add(1) & REQUEST_SEQUENCE_MASK;
        (if deadline { DEADLINE_TOKEN_BIT } else { 0 })
            | (u64::from(self.request_sequence) << REQUEST_SEQUENCE_SHIFT)
            | (u64::from(index.generation()) << SLOT_BITS)
            | u64::from(index.slot())
    }

    fn submit(&mut self, entry: squeue::Entry) -> RuntimeResult<()> {
        loop {
            let pushed = {
                let mut submissions = self.ring.submission();
                // SAFETY: PollAdd and PollRemove entries contain only copied fd,
                // flags, and integer tokens; no borrowed buffer outlives this call.
                unsafe { submissions.push(&entry) }.is_ok()
            };
            if pushed {
                break;
            }
            submit(&self.ring)?;
        }
        submit(&self.ring).map(|_| ())
    }
}

fn probe_multishot(ring: &mut IoUring) -> RuntimeResult<bool> {
    // SAFETY: eventfd returns a fresh descriptor or -1 with errno set.
    let fd = unsafe { libc::eventfd(1, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io_error(
            "create eventfd for io_uring multishot probe",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: ownership of the fresh eventfd descriptor is transferred once.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let entry = opcode::PollAdd::new(types::Fd(fd.as_raw_fd()), libc::POLLIN as u32)
        .multi(true)
        .build()
        .user_data(PROBE_TOKEN);
    push(ring, entry)?;
    submit_and_wait(ring)?;

    let completion = ring
        .completion()
        .find(|completion| completion.user_data() == PROBE_TOKEN)
        .ok_or(RuntimeError::FilePollerProbeCompletionMissing)?;
    if completion.result() == -libc::EINVAL {
        return Ok(false);
    }
    if completion.result() < 0 {
        return Err(completion_error(
            "probe io_uring multishot poll",
            completion.result(),
        ));
    }
    if !cqueue::more(completion.flags()) {
        return Ok(false);
    }

    let remove = opcode::PollRemove::new(PROBE_TOKEN)
        .build()
        .user_data(CONTROL_TOKEN);
    push(ring, remove)?;
    loop {
        submit_and_wait(ring)?;
        let mut result = None;
        for completion in ring.completion() {
            if completion.user_data() == CONTROL_TOKEN {
                result = Some(completion.result());
            }
        }
        if let Some(result) = result {
            if result != 0 && result != -libc::ENOENT {
                return Err(completion_error("remove io_uring multishot probe", result));
            }
            return Ok(true);
        }
    }
}

fn completion_event(
    completion: Completion,
    current_tokens: &[u64; FILE_POOL_CAPACITY],
    deadline_tokens: &[u64; FILE_POOL_CAPACITY],
    multishot: bool,
) -> RuntimeResult<Option<PollEvent>> {
    if completion.user_data == CONTROL_TOKEN {
        return Ok(None);
    }
    let Some(index) = decode_poll_token(completion.user_data) else {
        return Ok(None);
    };
    let is_deadline = completion.user_data & DEADLINE_TOKEN_BIT != 0;
    let tokens = if is_deadline {
        deadline_tokens
    } else {
        current_tokens
    };
    if tokens[index.slot() as usize] != completion.user_data {
        return Ok(None);
    }
    if completion.result == -libc::ECANCELED || completion.result == -libc::ENOENT {
        return Ok(None);
    }
    if completion.result < 0 {
        return Err(completion_error(
            "complete File readiness",
            completion.result,
        ));
    }

    let result = completion.result;
    let mut readiness = Readiness::default();
    if result & i32::from(libc::POLLIN | libc::POLLPRI) != 0 {
        readiness.insert(Readiness::READ);
    }
    if result & i32::from(libc::POLLOUT) != 0 {
        readiness.insert(Readiness::WRITE);
    }
    if result & i32::from(libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
        readiness.insert(Readiness::ERROR);
    }
    let target = if is_deadline {
        PollTarget::Deadline(index)
    } else {
        PollTarget::File(index)
    };
    Ok(Some(PollEvent {
        target: Some(target),
        readiness,
        rearm: is_deadline || !multishot || !cqueue::more(completion.flags),
    }))
}

fn poll_flags(spec: PollSpec) -> u32 {
    let mut flags = 0;
    if spec.read {
        flags |= libc::POLLIN | libc::POLLPRI;
    }
    if spec.write {
        flags |= libc::POLLOUT;
    }
    flags as u32
}

fn decode_poll_token(token: u64) -> Option<Index> {
    let slot = (token & SLOT_MASK) as u32;
    let generation = ((token >> SLOT_BITS) & u64::from(u32::MAX)) as u32;
    (generation != 0 && slot < FILE_POOL_CAPACITY as u32).then(|| Index::new(slot, generation))
}

fn set_timerfd(fd: i32, duration: Option<Duration>) -> RuntimeResult<()> {
    let (seconds, nanoseconds) = duration
        .map(|duration| {
            let duration = duration.max(Duration::from_nanos(1));
            (duration.as_secs(), i64::from(duration.subsec_nanos()))
        })
        .unwrap_or((0, 0));
    let seconds = libc::time_t::try_from(seconds).map_err(|_| {
        io_error(
            "arm File deadline timerfd",
            io::Error::from_raw_os_error(libc::EOVERFLOW),
        )
    })?;
    let spec = libc::itimerspec {
        it_interval: libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
        it_value: libc::timespec {
            tv_sec: seconds,
            tv_nsec: nanoseconds,
        },
    };
    // SAFETY: `spec` is initialized and the timerfd is owned by this worker.
    let result = unsafe { libc::timerfd_settime(fd, 0, &spec, std::ptr::null_mut()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io_error(
            "arm File deadline timerfd",
            io::Error::last_os_error(),
        ))
    }
}

fn push(ring: &mut IoUring, entry: squeue::Entry) -> RuntimeResult<()> {
    let mut submissions = ring.submission();
    // SAFETY: probe entries contain no borrowed userspace buffer.
    unsafe { submissions.push(&entry) }.map_err(|_| RuntimeError::FileSubmissionQueueFull.into())
}

fn submit(ring: &IoUring) -> RuntimeResult<usize> {
    loop {
        match ring.submit() {
            Ok(submitted) => return Ok(submitted),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_error("submit worker io_uring operations", error)),
        }
    }
}

fn submit_and_wait(ring: &IoUring) -> RuntimeResult<()> {
    loop {
        match ring.submit_and_wait(1) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(io_error("wait for worker io_uring completion", error));
            }
        }
    }
}

fn completion_error(operation: &'static str, result: i32) -> RuntimeError {
    io_error(operation, io::Error::from_raw_os_error(-result))
}

fn io_error(operation: &'static str, source: io::Error) -> RuntimeError {
    RuntimeError::FilePollerIo { operation, source }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn poller_io_error_preserves_os_source() {
        let error = io_error(
            "test File poller operation",
            io::Error::from_raw_os_error(libc::EBADF),
        );

        let RuntimeError::FilePollerIo { operation, .. } = &error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(*operation, "test File poller operation");
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("File poller error source");
        assert_eq!(source.raw_os_error(), Some(libc::EBADF),);
    }
}
