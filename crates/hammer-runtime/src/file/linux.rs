use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::pool::Index;
use hammer_infra::ring::LocalRing;
use io_uring::{IoUring, Probe, cqueue, opcode, squeue, types};

use super::{FILE_POOL_CAPACITY, POLL_BATCH_SIZE, PollEvent, PollSpec, Readiness};

const CONTROL_TOKEN: u64 = 0;
const PROBE_TOKEN: u64 = u64::MAX;
const SLOT_BITS: u32 = FILE_POOL_CAPACITY.trailing_zeros();
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
const INDEX_GENERATION_BITS: u32 = 32;
const REQUEST_SEQUENCE_SHIFT: u32 = SLOT_BITS + INDEX_GENERATION_BITS;
const REQUEST_SEQUENCE_MASK: u32 = (1 << (64 - REQUEST_SEQUENCE_SHIFT)) - 1;

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
    request_sequence: u32,
    multishot: bool,
}

impl Poller {
    pub(super) fn new() -> HammerResult<Self> {
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
                return Err(HammerError::internal(format!(
                    "worker io_uring does not support required {name} operation"
                )));
            }
        }

        let multishot = probe_multishot(&mut ring)?;
        let pending_capacity = ring.completion().capacity().saturating_mul(2);
        Ok(Self {
            ring,
            pending: LocalRing::with_capacity(pending_capacity),
            current_tokens: [0; FILE_POOL_CAPACITY],
            request_sequence: 0,
            multishot,
        })
    }

    pub(super) fn add(&mut self, spec: PollSpec) -> HammerResult<()> {
        if !spec.read && !spec.write {
            self.current_tokens[spec.index.slot() as usize] = CONTROL_TOKEN;
            return Ok(());
        }

        let token = self.next_token(spec.index);
        let entry = opcode::PollAdd::new(types::Fd(spec.fd), poll_flags(spec))
            .multi(self.multishot)
            .build()
            .user_data(token);
        self.submit(entry)?;
        self.current_tokens[spec.index.slot() as usize] = token;
        Ok(())
    }

    pub(super) fn modify(&mut self, before: PollSpec, after: PollSpec) -> HammerResult<()> {
        self.cancel(before.index)?;
        self.add(after)
    }

    pub(super) fn delete(&mut self, spec: PollSpec) -> HammerResult<()> {
        self.cancel(spec.index)
    }

    pub(super) fn poll(&mut self, ready: &mut [PollEvent; POLL_BATCH_SIZE]) -> HammerResult<usize> {
        let mut count = 0;
        while count < ready.len() {
            let Some(completion) = self.pending.pop() else {
                break;
            };
            if let Some(event) = completion_event(completion, &self.current_tokens, self.multishot)?
            {
                ready[count] = event;
                count += 1;
            }
        }

        let multishot = self.multishot;
        let current_tokens = &self.current_tokens;
        let (ring, pending) = (&mut self.ring, &mut self.pending);
        let mut completions = ring.completion();
        for completion in &mut completions {
            let completion = Completion {
                user_data: completion.user_data(),
                result: completion.result(),
                flags: completion.flags(),
            };
            if count < ready.len() {
                if let Some(event) = completion_event(completion, current_tokens, multishot)? {
                    ready[count] = event;
                    count += 1;
                }
            } else if pending.try_push(completion).is_err() {
                return Err(HammerError::internal(
                    "worker io_uring pending completion ring is full",
                ));
            }
        }
        Ok(count)
    }

    fn cancel(&mut self, index: Index) -> HammerResult<()> {
        let slot = index.slot() as usize;
        let token = self.current_tokens[slot];
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
                } else if completion.user_data != token
                    && pending.try_push(completion).is_err()
                {
                    return Err(HammerError::internal(
                        "worker io_uring pending completion ring is full while cancelling",
                    ));
                }
            }
            drop(completions);

            if let Some(result) = result {
                if result != 0 && result != -libc::ENOENT {
                    return Err(completion_error("cancel File readiness", result));
                }
                self.current_tokens[slot] = CONTROL_TOKEN;
                return Ok(());
            }
        }
    }

    fn next_token(&mut self, index: Index) -> u64 {
        self.request_sequence = self.request_sequence.wrapping_add(1) & REQUEST_SEQUENCE_MASK;
        (u64::from(self.request_sequence) << REQUEST_SEQUENCE_SHIFT)
            | (u64::from(index.generation()) << SLOT_BITS)
            | u64::from(index.slot())
    }

    fn submit(&mut self, entry: squeue::Entry) -> HammerResult<()> {
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

fn probe_multishot(ring: &mut IoUring) -> HammerResult<bool> {
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
        .ok_or_else(|| HammerError::internal("io_uring multishot probe produced no completion"))?;
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
    multishot: bool,
) -> HammerResult<Option<PollEvent>> {
    if completion.user_data == CONTROL_TOKEN {
        return Ok(None);
    }
    let Some(index) = decode_poll_token(completion.user_data) else {
        return Ok(None);
    };
    if current_tokens[index.slot() as usize] != completion.user_data {
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
    Ok(Some(PollEvent {
        index: Some(index),
        readiness,
        rearm: !multishot || !cqueue::more(completion.flags),
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

fn push(ring: &mut IoUring, entry: squeue::Entry) -> HammerResult<()> {
    let mut submissions = ring.submission();
    // SAFETY: probe entries contain no borrowed userspace buffer.
    unsafe { submissions.push(&entry) }
        .map_err(|_| HammerError::internal("worker io_uring submission queue is full"))
}

fn submit(ring: &IoUring) -> HammerResult<usize> {
    loop {
        match ring.submit() {
            Ok(submitted) => return Ok(submitted),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_error("submit worker io_uring operations", error)),
        }
    }
}

fn submit_and_wait(ring: &IoUring) -> HammerResult<()> {
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

fn completion_error(operation: &str, result: i32) -> HammerError {
    io_error(operation, io::Error::from_raw_os_error(-result))
}

fn io_error(operation: &str, error: io::Error) -> HammerError {
    HammerError::internal(format!("{operation}: {error}"))
}
