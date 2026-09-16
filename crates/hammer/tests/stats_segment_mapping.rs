#![cfg(target_os = "linux")]

//! Stats segment values through the contract a client relies on: connect to
//! the statseg socket, receive the segment descriptor, map it read-only, walk
//! the directory, and decode values.
//!
//! The decoder in this file is the read-only mapping fixture the ADR requires
//! from this repository; the external client implements the same contract.
//! Run with `cargo test -p hammer --test stats_segment_mapping`.

use std::fs::{self, File};
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::ptr;
use std::slice;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DAEMON_BINARY: &str = env!("CARGO_BIN_EXE_hammer");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(300);
/// One Data Worker: more than one hits an unrelated session worker-init race
/// in the daemon, which these stats checks do not need.
const WORKER_COUNT: usize = 1;

/// The segment publishes version 2 (`STAT_SEGMENT_VERSION`).
const SEGMENT_VERSION: u64 = 2;
const HEADER_BYTES: usize = 40;
const ENTRY_BYTES: usize = 144;
const MAX_NAME_BYTES: usize = 128;
const VECTOR_HEADER_BYTES: usize = 8;
const VECTOR_MIN_ALIGN: usize = 8;

const TYPE_SCALAR: u32 = 1;
const TYPE_SIMPLE: u32 = 2;
const TYPE_SYMLINK: u32 = 6;
const TYPE_GAUGE: u32 = 9;

const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    assert!(size_of::<Entry>() == ENTRY_BYTES);
    assert!(align_of::<Entry>() == 8);
    assert!(offset_of!(Entry, directory_type) == 0);
    assert!(offset_of!(Entry, data) == 8);
    assert!(offset_of!(Entry, name) == 16);
};

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Scalar(u64),
    Gauge(u64),
    Simple(Vec<Vec<u64>>),
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Entry {
    directory_type: u32,
    data: u64,
    name: [u8; MAX_NAME_BYTES],
}

impl Entry {
    fn name(&self) -> &str {
        let Some(end) = self.name.iter().position(|byte| *byte == 0) else {
            panic!("directory entry name is not terminated");
        };
        assert!(
            end <= MAX_NAME_BYTES - 2,
            "directory entry name exceeds the usable length"
        );
        assert!(
            self.name[end + 1..].iter().all(|byte| *byte == 0),
            "directory entry name padding is not zero at byte {}",
            end + 1
                + self.name[end + 1..]
                    .iter()
                    .position(|byte| *byte != 0)
                    .unwrap_or(0)
        );
        std::str::from_utf8(&self.name[..end]).expect("directory entry name is UTF-8")
    }
}

/// One published vector inside the fixture's mapping.
#[derive(Clone, Copy)]
struct Vector {
    data_offset: usize,
    length: usize,
    element_size: usize,
}

impl Vector {
    fn resolve(mapping: &[u8], base: usize, published: usize, element_size: usize) -> Option<Self> {
        let data_offset = published.checked_sub(base)?;
        let header_offset = data_offset.checked_sub(VECTOR_HEADER_BYTES)?;
        let header = mapping.get(header_offset..data_offset)?;
        let length = u32::from_ne_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let alignment = VECTOR_MIN_ALIGN << (header[5] & 0x7f);
        assert!(
            alignment >= VECTOR_MIN_ALIGN && alignment.is_power_of_two(),
            "vector header declares a valid alignment"
        );
        Some(Self {
            data_offset,
            length,
            element_size,
        })
    }

    fn element<'mapping>(&self, mapping: &'mapping [u8], index: usize) -> Option<&'mapping [u8]> {
        if index >= self.length {
            return None;
        }
        let offset = self.data_offset + index * self.element_size;
        mapping.get(offset..offset + self.element_size)
    }

    fn u64(&self, mapping: &[u8], index: usize) -> Option<u64> {
        let element = self.element(mapping, index)?;
        Some(u64::from_ne_bytes(
            element[..8].try_into().expect("8 bytes"),
        ))
    }

    fn pointer(&self, mapping: &[u8], index: usize) -> Option<usize> {
        let element = self.element(mapping, index)?;
        Some(usize::from_ne_bytes(
            element[..8].try_into().expect("8 bytes"),
        ))
    }
}

/// The read-only mapping fixture.
struct Fixture<'mapping> {
    mapping: &'mapping [u8],
}

impl<'mapping> Fixture<'mapping> {
    fn new(mapping: &'mapping [u8]) -> Self {
        Self { mapping }
    }

    /// The published `(base, directory)` pair if the header is stable.
    fn published(&self) -> Option<(usize, Vector)> {
        let (epoch, in_progress, base, directory) = header(self.mapping)?;
        if in_progress != 0 {
            return None;
        }
        let directory = Vector::resolve(self.mapping, base, directory, ENTRY_BYTES)?;
        let (end_epoch, end_in_progress, ..) = header(self.mapping)?;
        if end_in_progress == 0 && end_epoch == epoch {
            Some((base, directory))
        } else {
            None
        }
    }

    fn names(&self) -> Option<Vec<String>> {
        for _ in 0..16 {
            let Some((_, directory)) = self.published() else {
                continue;
            };
            let mut names = Vec::with_capacity(directory.length);
            for index in 0..directory.length {
                names.push(self.entry(&directory, index).name().to_owned());
            }
            return Some(names);
        }
        None
    }

    fn read(&self, name: &str) -> Option<Value> {
        for _ in 0..16 {
            let Some((base, directory)) = self.published() else {
                continue;
            };
            let Some(index) = self.find(&directory, name) else {
                return None;
            };
            return self.value(&base, &directory, index, name, 0);
        }
        None
    }

    fn find(&self, directory: &Vector, name: &str) -> Option<usize> {
        (0..directory.length).find(|index| self.entry(directory, *index).name() == name)
    }

    fn entry(&self, directory: &Vector, index: usize) -> Entry {
        let element = directory
            .element(self.mapping, index)
            .expect("entry in range");
        let mut bytes = [0_u8; ENTRY_BYTES];
        bytes.copy_from_slice(element);
        // SAFETY: the entry is plain data; the fixture only reads the copy.
        unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<Entry>()) }
    }

    fn value(
        &self,
        base: &usize,
        directory: &Vector,
        index: usize,
        name: &str,
        depth: usize,
    ) -> Option<Value> {
        assert!(depth < 16, "symlink `{name}` does not resolve");
        let entry = self.entry(directory, index);
        match entry.directory_type {
            TYPE_SCALAR => Some(Value::Scalar(entry.data)),
            TYPE_GAUGE => Some(Value::Gauge(entry.data)),
            TYPE_SIMPLE => {
                let outer = Vector::resolve(self.mapping, *base, entry.data as usize, 8)?;
                let mut rows = Vec::with_capacity(outer.length);
                for row in 0..outer.length {
                    let Some(inner) =
                        Vector::resolve(self.mapping, *base, outer.pointer(self.mapping, row)?, 8)
                    else {
                        rows.push(Vec::new());
                        continue;
                    };
                    let mut values = Vec::with_capacity(inner.length);
                    for column in 0..inner.length {
                        values.push(inner.u64(self.mapping, column)?);
                    }
                    rows.push(values);
                }
                Some(Value::Simple(rows))
            }
            TYPE_SYMLINK => {
                let target = usize::try_from(entry.data & u64::from(u32::MAX)).expect("low half");
                let column = usize::try_from(entry.data >> 32).expect("high half");
                let value = self.value(base, directory, target, name, depth + 1)?;
                let cropped = match value {
                    Value::Simple(rows) => Value::Simple(
                        rows.into_iter()
                            .map(|row| vec![row[column]])
                            .collect::<Vec<_>>(),
                    ),
                    other => other,
                };
                Some(cropped)
            }
            directory_type => panic!("entry `{name}` has type {directory_type}"),
        }
    }
}

/// Reads version, epoch, in-progress flag, base, and directory of a mapping.
fn header(mapping: &[u8]) -> Option<(u64, u64, usize, usize)> {
    let bytes = mapping.get(..HEADER_BYTES)?;
    let version = u64::from_ne_bytes(bytes[0..8].try_into().expect("8 bytes"));
    assert_eq!(version, SEGMENT_VERSION, "segment version");
    let base = usize::from_ne_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let epoch = u64::from_ne_bytes(bytes[16..24].try_into().expect("8 bytes"));
    let in_progress = u64::from_ne_bytes(bytes[24..32].try_into().expect("8 bytes"));
    let directory = usize::from_ne_bytes(bytes[32..40].try_into().expect("8 bytes"));
    assert!(base != 0, "the segment publishes a base");
    assert!(directory >= base, "the directory lives inside the segment");
    Some((epoch, in_progress, base, directory))
}

/// One owned read-only mapping of the stats segment.
struct SegmentMapping {
    pointer: *mut u8,
    length: usize,
}

unsafe impl Send for SegmentMapping {}
unsafe impl Sync for SegmentMapping {}

impl SegmentMapping {
    fn new(fd: &OwnedFd) -> Self {
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `metadata` is valid writable storage for `fstat`.
        if unsafe { libc::fstat(fd.as_raw_fd(), metadata.as_mut_ptr()) } < 0 {
            panic!("stats segment fstat failed: {}", io::Error::last_os_error());
        }
        // SAFETY: `fstat` initialized every field on success.
        let size = unsafe { metadata.assume_init() }.st_size;
        let length = usize::try_from(size).expect("a positive stats segment size");
        // SAFETY: the descriptor is live and the mapping is read-only.
        let pointer = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            panic!("stats segment mmap failed: {}", io::Error::last_os_error());
        }
        Self {
            pointer: pointer.cast(),
            length,
        }
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: the mapping owns `pointer..pointer + length` for its life.
        unsafe { slice::from_raw_parts(self.pointer, self.length) }
    }
}

impl Drop for SegmentMapping {
    fn drop(&mut self) {
        // SAFETY: the mapping is owned here and released exactly once.
        if unsafe { libc::munmap(self.pointer.cast(), self.length) } != 0 {
            std::process::abort();
        }
    }
}

/// How long one listener probe waits for the descriptor handoff.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(2);

/// Connects to the statseg listener and receives the segment descriptor.
fn connect_and_map(socket_path: &Path) -> SegmentMapping {
    // SAFETY: the descriptor is created here and owned by `socket`.
    let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0) };
    assert!(raw >= 0, "stats socket: {}", io::Error::last_os_error());
    // SAFETY: `raw` is a fresh descriptor.
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let bytes = socket_path.as_os_str().as_encoded_bytes();
    assert!(
        bytes.len() < address.sun_path.len(),
        "stats socket path fits `sockaddr_un`"
    );
    for (index, byte) in bytes.iter().enumerate() {
        address.sun_path[index] = *byte as libc::c_char;
    }
    let length = size_of::<libc::sa_family_t>() + bytes.len() + 1;
    // SAFETY: `address` is a live `sockaddr_un` of the stated length.
    let connected = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            ptr::addr_of!(address).cast::<libc::sockaddr>(),
            length as libc::socklen_t,
        )
    };
    assert_eq!(
        connected,
        0,
        "stats socket connect: {}",
        io::Error::last_os_error()
    );

    // A listener that never hands over the segment must fail the probe
    // instead of blocking it forever.
    let timeout = libc::timeval {
        tv_sec: HANDOFF_TIMEOUT.as_secs() as libc::time_t,
        tv_usec: 0,
    };
    // SAFETY: the descriptor is live and `timeout` outlives the call.
    let applied = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            ptr::addr_of!(timeout).cast(),
            size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    assert_eq!(
        applied,
        0,
        "stats socket receive timeout: {}",
        io::Error::last_os_error()
    );

    let control_bytes = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize };
    let mut control = vec![0_u8; control_bytes];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len() as _;
    // SAFETY: `message` addresses the live control buffer.
    let received = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, 0) };
    assert!(
        received >= 0,
        "stats recvmsg: {}",
        io::Error::last_os_error()
    );
    assert_eq!(received, 0, "the descriptor handoff carries no payload");
    assert_eq!(
        message.msg_flags & libc::MSG_CTRUNC,
        0,
        "control is not truncated"
    );
    // SAFETY: `message` was filled by `recvmsg`.
    let segment_fd = unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        assert!(!header.is_null(), "the handoff carries one control message");
        assert_eq!((*header).cmsg_level, libc::SOL_SOCKET);
        assert_eq!((*header).cmsg_type, libc::SCM_RIGHTS);
        let raw_fd = ptr::read_unaligned(libc::CMSG_DATA(header).cast::<RawFd>());
        assert!(raw_fd >= 0, "the handoff carries one descriptor");
        OwnedFd::from_raw_fd(raw_fd)
    };
    let mapping = SegmentMapping::new(&segment_fd);
    drop(segment_fd);
    mapping
}

struct HammerDaemon {
    child: Child,
    temp_dir: PathBuf,
    root_segment: PathBuf,
    api_segment: PathBuf,
    stats_socket: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stopped: bool,
}

impl HammerDaemon {
    fn start() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos();
        let prefix = format!("hammer-stats-fixture-{}-{unique}", std::process::id());
        let temp_dir = std::env::temp_dir().join(&prefix);
        fs::create_dir_all(&temp_dir).expect("temporary directory is created");

        let config_path = temp_dir.join("startup.toml");
        let stdout_path = temp_dir.join("daemon.stdout");
        let stderr_path = temp_dir.join("daemon.stderr");
        let root_segment = PathBuf::from("/dev/shm").join(format!("{prefix}-global_vm"));
        let api_segment = PathBuf::from("/dev/shm").join(format!("{prefix}-vpe-api"));
        let stats_socket = temp_dir.join("stats.sock");
        fs::write(
            &config_path,
            format!(
                "plugins = []\n\n[memory]\nmain_heap_size = \"256 MiB\"\n\n[worker]\ncount = {WORKER_COUNT}\n\n[statseg]\nsocket_name = \"{}\"\nupdate_interval = \"50ms\"\n\n[api-segment]\nprefix = \"{prefix}\"\n",
                stats_socket.display()
            ),
        )
        .expect("daemon configuration is written");

        let stdout = File::create(&stdout_path).expect("daemon stdout log is created");
        let stderr = File::create(&stderr_path).expect("daemon stderr log is created");
        let child = Command::new(DAEMON_BINARY)
            .arg(&config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("configured Hammer daemon starts");

        Self {
            child,
            temp_dir,
            root_segment,
            api_segment,
            stats_socket,
            stdout_path,
            stderr_path,
            stopped: false,
        }
    }

    fn diagnostics(&mut self, error: impl std::fmt::Display) -> String {
        let status = self.child.try_wait();
        let stdout = fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        format!(
            "{error}\ndaemon status: {status:?}\ndaemon stdout:\n{stdout}\ndaemon stderr:\n{stderr}"
        )
    }

    /// Waits for the listener and returns the segment mapping.
    fn mapping(&mut self) -> SegmentMapping {
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        loop {
            if self.stats_socket.exists() && self.child.try_wait().ok().flatten().is_none() {
                let probe = std::panic::catch_unwind(|| connect_and_map(&self.stats_socket));
                if let Ok(mapping) = probe {
                    return mapping;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{}",
                self.diagnostics("stats listener did not accept a reader")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn shutdown(&mut self) -> ExitStatus {
        if self.stopped {
            return self
                .child
                .try_wait()
                .expect("daemon status is readable")
                .expect("stopped daemon has exited");
        }
        let signal = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("kill command starts");
        assert!(signal.success(), "SIGTERM delivery succeeds: {signal:?}");

        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("daemon status is readable") {
                break status;
            }
            if Instant::now() >= deadline {
                self.child
                    .kill()
                    .expect("daemon is killed after shutdown timeout");
                break self.child.wait().expect("killed daemon is reaped");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        self.stopped = true;
        status
    }
}

impl Drop for HammerDaemon {
    fn drop(&mut self) {
        if !self.stopped && self.child.try_wait().is_ok_and(|status| status.is_none()) {
            if let Err(error) = self.child.kill() {
                eprintln!("failed to kill Hammer daemon during cleanup: {error}");
            }
            if let Err(error) = self.child.wait() {
                eprintln!("failed to reap Hammer daemon during cleanup: {error}");
            }
        }
        for path in [&self.api_segment, &self.root_segment] {
            if let Err(error) = fs::remove_file(path)
                && error.kind() != io::ErrorKind::NotFound
            {
                eprintln!("failed to remove segment {}: {error}", path.display());
            }
        }
        if let Err(error) = fs::remove_dir_all(&self.temp_dir)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!("failed to remove temporary directory: {error}");
        }
    }
}

/// One heap reading, in the `/mem` column order.
fn heap_columns(fixture: &Fixture<'_>, name: &str) -> Vec<u64> {
    match fixture.read(name) {
        Some(Value::Simple(rows)) => {
            assert_eq!(rows.len(), 1, "`{name}` has one row");
            assert_eq!(rows[0].len(), 7, "`{name}` has seven columns");
            rows[0].clone()
        }
        other => panic!("`{name}` is a one-row counter vector, got {other:?}"),
    }
}

/// The single row of a per-worker vector.
fn worker_columns(fixture: &Fixture<'_>, name: &str) -> Vec<u64> {
    match fixture.read(name) {
        Some(Value::Simple(rows)) => {
            assert_eq!(rows.len(), 1, "`{name}` has one row");
            rows[0].clone()
        }
        other => panic!("`{name}` is a one-row counter vector, got {other:?}"),
    }
}

fn heartbeat(fixture: &Fixture<'_>) -> u64 {
    match fixture.read("/sys/heartbeat") {
        Some(Value::Scalar(value)) => value,
        other => panic!("`/sys/heartbeat` is a scalar, got {other:?}"),
    }
}

#[test]
fn stats_segment_publishes_mem_and_system_values() {
    let mut daemon = HammerDaemon::start();
    let mapping = daemon.mapping();
    let fixture = Fixture::new(mapping.bytes());
    assert!(
        mapping.length >= HEADER_BYTES,
        "the mapping holds the shared header"
    );

    let names = fixture
        .names()
        .unwrap_or_else(|| panic!("{}", daemon.diagnostics("no stable directory")));
    for expected in [
        "/mem/main heap",
        "/mem/main heap/total",
        "/mem/stat segment",
        "/mem/global_vm pvt",
        "/mem/vpe-api pvt",
        "/mem/vpe-api data",
        "/sys/num_worker_threads",
        "/sys/main_loop_count_per_worker",
        "/sys/loops_per_worker",
        "/sys/heartbeat",
        "/sys/boottime",
        "/sys/last_stats_clear",
    ] {
        assert!(
            names.iter().any(|name| name == expected),
            "`{expected}` is published: {names:?}"
        );
    }

    for heap in [
        "main heap",
        "stat segment",
        "global_vm pvt",
        "vpe-api pvt",
        "vpe-api data",
    ] {
        let columns = heap_columns(&fixture, &format!("/mem/{heap}"));
        let [
            total,
            used,
            free,
            used_mmap,
            max_allocated,
            free_chunks,
            releasable,
        ] = columns[..]
        else {
            unreachable!("seven columns were checked");
        };
        assert!(total > 0, "`{heap}` has a size");
        assert!(used > 0, "`{heap}` has allocations");
        assert!(free > 0, "`{heap}` has free space");
        assert_eq!(
            used + free,
            total + used_mmap,
            "`{heap}`: used + free = total + mmapped, the dlmalloc mallinfo identity"
        );
        assert!(
            max_allocated >= used,
            "`{heap}`: the maximum footprint covers the current one"
        );
        assert!(free_chunks > 0, "`{heap}` reports free chunks");
        assert!(releasable <= free, "`{heap}`: releasable space is free");
        for (column, alias) in [(0_usize, "total"), (1, "used"), (2, "free")] {
            let expected = columns[column];
            match fixture.read(&format!("/mem/{heap}/{alias}")) {
                Some(Value::Simple(rows)) => {
                    assert_eq!(
                        rows,
                        vec![vec![expected]],
                        "`/mem/{heap}/{alias}` selects column {column}"
                    );
                }
                other => panic!("`/mem/{heap}/{alias}` is a cropped counter vector, got {other:?}"),
            }
        }
    }
    let main_heap = heap_columns(&fixture, "/mem/main heap");
    assert!(
        main_heap[0] <= 256 << 20,
        "the main heap stays inside its configured 256 MiB"
    );

    match fixture.read("/sys/num_worker_threads") {
        Some(Value::Gauge(workers)) => assert_eq!(workers, WORKER_COUNT as u64),
        other => panic!("`/sys/num_worker_threads` is a gauge, got {other:?}"),
    }

    // The counters and rates need at least one collect round and one window.
    std::thread::sleep(Duration::from_secs(1));
    let first = (
        worker_columns(&fixture, "/sys/main_loop_count_per_worker"),
        worker_columns(&fixture, "/sys/loops_per_worker"),
        heartbeat(&fixture),
    );
    std::thread::sleep(SAMPLE_INTERVAL);
    let second = (
        worker_columns(&fixture, "/sys/main_loop_count_per_worker"),
        worker_columns(&fixture, "/sys/loops_per_worker"),
        heartbeat(&fixture),
    );

    assert_eq!(first.0.len(), WORKER_COUNT);
    assert_eq!(first.1.len(), WORKER_COUNT);
    assert!(
        second.2 > first.2,
        "the collect round advances the heartbeat: {} -> {}",
        first.2,
        second.2
    );
    for (index, (earlier, later)) in first.0.iter().zip(&second.0).enumerate() {
        assert!(
            later >= earlier && *later > 0,
            "worker {index} count is cumulative and moved: {earlier} -> {later}"
        );
    }
    for (index, (earlier, later)) in first.1.iter().zip(&second.1).enumerate() {
        if *earlier == 0 || *later == 0 {
            continue;
        }
        assert!(
            *later < earlier.saturating_mul(10),
            "worker {index} rate does not accumulate: {earlier} -> {later}"
        );
    }
    let elapsed = SAMPLE_INTERVAL.as_secs_f64();
    let derived: Vec<f64> = second
        .0
        .iter()
        .zip(&first.0)
        .map(|(current, previous)| current.saturating_sub(*previous) as f64 / elapsed)
        .collect();
    assert!(
        derived.iter().any(|rate| *rate > 0.0),
        "at least one worker ran loops: {derived:?}"
    );
    for (index, (published, measured)) in second.1.iter().zip(&derived).enumerate() {
        if *measured == 0.0 {
            continue;
        }
        let published = *published as f64;
        assert!(
            published > measured / 10.0 && published < measured * 10.0,
            "worker {index} published {published} loops/s, measured {measured} loops/s"
        );
    }

    // A malformed mapping is rejected before any dereference.
    let truncated_fixture = Fixture::new(&mapping.bytes()[..HEADER_BYTES]);
    assert!(
        truncated_fixture.names().is_none(),
        "a mapping without a directory is rejected"
    );
    assert!(
        truncated_fixture.read("/mem/main heap").is_none(),
        "a truncated mapping yields no value"
    );

    drop(fixture);
    drop(mapping);
    let status = daemon.shutdown();
    assert!(
        status.success(),
        "{}",
        daemon.diagnostics(format!("daemon exits unsuccessfully: {status:?}"))
    );
}
