use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_infra::ring::LocalRing;
use io_uring::{IoUring, Probe, cqueue, opcode, squeue, types};

use super::{
    FILE_POOL_CAPACITY, FileReadinessMode, POLL_BATCH_SIZE, PollEvent, PollSpec, PollTarget,
    Readiness,
};

const CONTROL_TOKEN: u64 = u64::MAX - 1;
const DEADLINE_TOKEN_BIT: u64 = 1 << 63;
const TOKEN_INDEX_BITS: u32 = FILE_POOL_CAPACITY.trailing_zeros();
const TOKEN_INDEX_MASK: u64 = (FILE_POOL_CAPACITY - 1) as u64;
const MAX_TOKEN_GENERATION: u64 = (1_u64 << (63 - TOKEN_INDEX_BITS)) - 1;

const _: () = assert!(FILE_POOL_CAPACITY.is_power_of_two());

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
    current_multishot: [bool; FILE_POOL_CAPACITY],
    deadline_tokens: [u64; FILE_POOL_CAPACITY],
    deadline_fds: [Option<OwnedFd>; FILE_POOL_CAPACITY],
    deadline_durations: [Option<Duration>; FILE_POOL_CAPACITY],
    next_generation: u64,
    multishot_available: bool,
    cq_overflow: u32,
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
            current_tokens: [CONTROL_TOKEN; FILE_POOL_CAPACITY],
            current_multishot: [false; FILE_POOL_CAPACITY],
            deadline_tokens: [CONTROL_TOKEN; FILE_POOL_CAPACITY],
            deadline_fds: std::array::from_fn(|_| None),
            deadline_durations: [None; FILE_POOL_CAPACITY],
            next_generation: 1,
            multishot_available: true,
            cq_overflow: 0,
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
            self.current_tokens[spec.index as usize] = CONTROL_TOKEN;
            self.current_multishot[spec.index as usize] = false;
            return Ok(());
        }

        self.add_poll(
            spec.index,
            spec.fd,
            poll_flags(spec),
            false,
            spec.readiness_mode == FileReadinessMode::Drain && !spec.write,
        )?;
        self.flush()
    }

    pub(super) fn rearm(&mut self, spec: PollSpec) -> RuntimeResult<()> {
        if !spec.read && !spec.write {
            self.current_tokens[spec.index as usize] = CONTROL_TOKEN;
            self.current_multishot[spec.index as usize] = false;
            return Ok(());
        }
        self.add_poll(
            spec.index,
            spec.fd,
            poll_flags(spec),
            false,
            spec.readiness_mode == FileReadinessMode::Drain && !spec.write,
        )
    }

    pub(super) fn flush(&mut self) -> RuntimeResult<()> {
        if self.ring.submission().is_empty() {
            return Ok(());
        }
        submit(&self.ring).map(|_| ())
    }

    pub(super) fn is_current(&self, event: &PollEvent) -> bool {
        match event.target {
            Some(PollTarget::File(index)) => self.current_tokens[index as usize] == event.token,
            Some(PollTarget::Deadline(index)) => {
                self.deadline_tokens[index as usize] == event.token
            }
            None => false,
        }
    }

    pub(super) fn has_pending(&mut self) -> bool {
        !self.pending.is_empty() || !self.ring.completion().is_empty()
    }

    pub(super) fn modify(&mut self, before: PollSpec, after: PollSpec) -> RuntimeResult<()> {
        self.cancel(before.index)?;
        self.add(after)
    }

    pub(super) fn delete(&mut self, spec: PollSpec) -> RuntimeResult<()> {
        self.cancel(spec.index)
    }

    pub(super) fn add_deadline(&mut self, index: u32) -> RuntimeResult<()> {
        let slot = index as usize;
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
        index: u32,
        duration: Option<Duration>,
    ) -> RuntimeResult<()> {
        let slot = index as usize;
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
                    if let Err(error) = self
                        .add_deadline_poll(index, deadline_fd)
                        .and_then(|()| self.flush())
                    {
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

    pub(super) fn delete_deadline(&mut self, index: u32) -> RuntimeResult<()> {
        let slot = index as usize;
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

    pub(super) fn consume_deadline(&mut self, index: u32) -> RuntimeResult<()> {
        let fd = self
            .deadline_fds
            .get(index as usize)
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

    pub(super) fn rearm_deadline(&mut self, index: u32) -> RuntimeResult<()> {
        let slot = index as usize;
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
        self.clear_wake();
        let mut count = 0;
        let mut multishot_unsupported = false;
        while count < ready.len() {
            let Some(completion) = self.pending.pop() else {
                break;
            };
            if let Some(event) = completion_event(
                completion,
                &self.current_tokens,
                &self.current_multishot,
                &self.deadline_tokens,
                &mut multishot_unsupported,
            )? {
                ready[count] = event;
                count += 1;
            }
        }

        let current_tokens = &self.current_tokens;
        let current_multishot = &self.current_multishot;
        let deadline_tokens = &self.deadline_tokens;
        let ring = &mut self.ring;
        let mut completions = ring.completion();
        while count < ready.len() {
            let Some(completion) = completions.next() else {
                break;
            };
            let completion = Completion {
                user_data: completion.user_data(),
                result: completion.result(),
                flags: completion.flags(),
            };
            if let Some(event) = completion_event(
                completion,
                current_tokens,
                current_multishot,
                deadline_tokens,
                &mut multishot_unsupported,
            )? {
                ready[count] = event;
                count += 1;
            }
        }
        let overflow = completions.overflow();
        drop(completions);
        if multishot_unsupported {
            self.multishot_available = false;
        }
        if overflow != self.cq_overflow {
            self.cq_overflow = overflow;
            if !self.ring.params().is_feature_nodrop() {
                return Err(RuntimeError::FileCompletionQueueFull {
                    operation: "receiving readiness",
                }
                .into());
            }
            submit(&self.ring)?;
        }
        Ok(count)
    }

    /// The io_uring CQ eventfd is a readiness hint, not the completion
    /// consumer. The next `poll` drains the CQ and validates its tokens.
    pub(super) fn wait(&self, timeout: Duration) -> RuntimeResult<()> {
        let mut descriptor = libc::pollfd {
            fd: self.wake.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let milliseconds = timeout.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: descriptor is writable and the eventfd remains owned by this Poller.
        let result = unsafe { libc::poll(&mut descriptor, 1, milliseconds) };
        if result >= 0 {
            return Ok(());
        }
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::Interrupted {
            return Ok(());
        }
        Err(io_error("wait for worker File readiness", source))
    }

    fn cancel(&mut self, index: u32) -> RuntimeResult<()> {
        self.cancel_token(index, false)
    }

    fn cancel_deadline(&mut self, index: u32) -> RuntimeResult<()> {
        self.cancel_token(index, true)
    }

    fn cancel_token(&mut self, index: u32, deadline: bool) -> RuntimeResult<()> {
        let slot = index as usize;
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
                    self.current_multishot[slot] = false;
                }
                return Ok(());
            }
        }
    }

    fn add_deadline_poll(&mut self, index: u32, fd: i32) -> RuntimeResult<()> {
        self.add_poll(index, fd, libc::POLLIN as u32, true, false)
    }

    fn add_poll(
        &mut self,
        index: u32,
        fd: i32,
        flags: u32,
        deadline: bool,
        multi: bool,
    ) -> RuntimeResult<()> {
        let token = self.next_token(index, deadline);
        let multi = multi && self.multishot_available;
        // VPP vlib/file.c uses level-triggered epoll unless EDGE_TRIGGERED is set.
        // A one-shot poll rearmed after dispatch observes unread data again.
        let entry = opcode::PollAdd::new(types::Fd(fd), flags)
            .multi(multi)
            .build()
            .user_data(token);
        self.enqueue(entry)?;
        if deadline {
            self.deadline_tokens[index as usize] = token;
        } else {
            self.current_tokens[index as usize] = token;
            self.current_multishot[index as usize] = multi;
        }
        Ok(())
    }

    fn next_token(&mut self, index: u32, deadline: bool) -> u64 {
        assert!(
            self.next_generation < MAX_TOKEN_GENERATION,
            "File token generations exhausted"
        );
        let generation = self.next_generation;
        self.next_generation += 1;
        (if deadline { DEADLINE_TOKEN_BIT } else { 0 })
            | (generation << TOKEN_INDEX_BITS)
            | u64::from(index)
    }

    fn submit(&mut self, entry: squeue::Entry) -> RuntimeResult<()> {
        self.enqueue(entry)?;
        self.flush()
    }

    fn enqueue(&mut self, entry: squeue::Entry) -> RuntimeResult<()> {
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
        Ok(())
    }
}

fn completion_event(
    completion: Completion,
    current_tokens: &[u64; FILE_POOL_CAPACITY],
    current_multishot: &[bool; FILE_POOL_CAPACITY],
    deadline_tokens: &[u64; FILE_POOL_CAPACITY],
    multishot_unsupported: &mut bool,
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
    if tokens[index as usize] != completion.user_data {
        return Ok(None);
    }
    if completion.result == -libc::ECANCELED || completion.result == -libc::ENOENT {
        return Ok(None);
    }
    if completion.result == -libc::EINVAL && !is_deadline && current_multishot[index as usize] {
        *multishot_unsupported = true;
        return Ok(Some(PollEvent {
            target: Some(PollTarget::File(index)),
            readiness: Readiness::default(),
            rearm: true,
            token: completion.user_data,
        }));
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
        rearm: !cqueue::more(completion.flags),
        token: completion.user_data,
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

fn decode_poll_token(token: u64) -> Option<u32> {
    let index = u32::try_from(token & TOKEN_INDEX_MASK).ok()?;
    (index < FILE_POOL_CAPACITY as u32).then_some(index)
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
    use super::*;
    use std::time::Instant;

    #[test]
    fn unread_file_remains_ready_after_rearm() {
        let mut poller = Poller::new().expect("io_uring poller is available");
        // SAFETY: eventfd returns a new descriptor, owned by this test.
        let fd = unsafe { libc::eventfd(1, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(fd >= 0);
        // SAFETY: the new descriptor has not been transferred elsewhere.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let spec = PollSpec {
            index: 0,
            fd: fd.as_raw_fd(),
            read: true,
            write: false,
            readiness_mode: FileReadinessMode::Level,
        };
        poller.add(spec).expect("register readable descriptor");

        for _ in 0..2 {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                let mut ready = std::array::from_fn(|_| PollEvent::default());
                if poller.poll(&mut ready).expect("poll readiness") != 0 {
                    assert!(matches!(ready[0].target, Some(PollTarget::File(0))));
                    assert!(ready[0].readiness.contains(Readiness::READ));
                    assert!(ready[0].rearm);
                    poller.add(spec).expect("rearm readable descriptor");
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "unread descriptor lost readiness"
                );
                std::thread::yield_now();
            }
        }
    }
}
