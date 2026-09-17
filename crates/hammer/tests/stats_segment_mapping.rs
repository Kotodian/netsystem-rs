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

/// Buffer Pool slots per NUMA node for this fixture (`worker.buffer.slots_per_numa`).
/// Each Pool publishes its three gauges over this many buffers, rounded up to
/// whole pages when the Pool mapping is established.
const POOL_SLOTS: u64 = 4_096;

/// The segment publishes version 2 (`STAT_SEGMENT_VERSION`).
const SEGMENT_VERSION: u64 = 2;
const HEADER_BYTES: usize = 40;
const ENTRY_BYTES: usize = 144;
const MAX_NAME_BYTES: usize = 128;
const VECTOR_HEADER_BYTES: usize = 8;
const VECTOR_MIN_ALIGN: usize = 8;

const TYPE_SCALAR: u32 = 1;
const TYPE_SIMPLE: u32 = 2;
const TYPE_NAME_VECTOR: u32 = 4;
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
    Names(Vec<String>),
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

    /// The `(target entry, column)` pair of a published symlink.
    fn symlink(&self, name: &str) -> Option<(String, usize)> {
        for _ in 0..16 {
            let Some((_, directory)) = self.published() else {
                continue;
            };
            let index = self.find(&directory, name)?;
            let entry = self.entry(&directory, index);
            assert_eq!(entry.directory_type, TYPE_SYMLINK, "`{name}` is a symlink");
            let target =
                usize::try_from(entry.data & u64::from(u32::MAX)).expect("low half of the target");
            let column = usize::try_from(entry.data >> 32).expect("high half of the target");
            let target = self.entry(&directory, target).name().to_owned();
            return Some((target, column));
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
            TYPE_NAME_VECTOR => {
                let vector = Vector::resolve(self.mapping, *base, entry.data as usize, 8)?;
                let mut names = Vec::with_capacity(vector.length);
                for index in 0..vector.length {
                    match vector.pointer(self.mapping, index) {
                        None | Some(0) => names.push(String::new()),
                        Some(pointer) => {
                            let offset = pointer.checked_sub(*base)?;
                            let bytes = self.mapping.get(offset..)?;
                            let end = bytes.iter().position(|byte| *byte == 0)?;
                            names.push(String::from_utf8(bytes[..end].to_vec()).ok()?);
                        }
                    }
                }
                Some(Value::Names(names))
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
        Self::start_with_worker_config(&Self::worker_config())
    }

    /// Starts a daemon whose `[statseg]` section also carries `statseg_extra`,
    /// for example `per_node_counters = true`.
    fn start_with_node_counters() -> Self {
        Self::start_with_config(&Self::worker_config(), "per_node_counters = true\n")
    }

    fn worker_config() -> String {
        format!(
            "[worker]\ncount = {WORKER_COUNT}\n\n[worker.buffer]\nslots_per_numa = {POOL_SLOTS}\n"
        )
    }

    /// Starts a daemon whose `[worker]` section is exactly `worker_config`.
    fn start_with_worker_config(worker_config: &str) -> Self {
        Self::start_with_config(worker_config, "")
    }

    fn start_with_config(worker_config: &str, statseg_extra: &str) -> Self {
        Self::start_with_plugins(worker_config, statseg_extra, "[]")
    }

    /// Starts a daemon that loads `plugins` as its configured roots.
    ///
    /// `PluginMain::directory` resolves roots next to the daemon executable,
    /// which is where `cargo build --workspace` leaves the plugin cdylibs.
    fn start_with_plugins(worker_config: &str, statseg_extra: &str, plugins: &str) -> Self {
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
                "plugins = {plugins}\n\n[memory]\nmain_heap_size = \"256 MiB\"\n\n{worker_config}\n[statseg]\nsocket_name = \"{}\"\nupdate_interval = \"50ms\"\n{statseg_extra}\n[api-segment]\nprefix = \"{prefix}\"\n",
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

/// The three gauges of one Buffer Pool, in VPP's registration order
/// (`cached`, `used`, `available`; `third_party/vpp/src/vlib/buffer.c:943-955`).
fn pool_columns(fixture: &Fixture<'_>, pool: &str) -> [u64; 3] {
    ["cached", "used", "available"].map(|gauge| {
        match fixture.read(&format!("/buffer-pools/{pool}/{gauge}")) {
            Some(Value::Gauge(value)) => value,
            other => panic!("`/buffer-pools/{pool}/{gauge}` is a gauge, got {other:?}"),
        }
    })
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

    // `/buffer-pools/<pool>/{cached,used,available}`: VPP registers three gauges
    // per Buffer Pool (`third_party/vpp/src/vlib/buffer.c:937-956`); one Pool is
    // established per NUMA node the Data Workers run on.
    let mut pools: Vec<&str> = names
        .iter()
        .filter_map(|name| {
            let (pool, gauge) = name.strip_prefix("/buffer-pools/")?.split_once('/')?;
            matches!(gauge, "cached" | "used" | "available").then_some(pool)
        })
        .collect();
    pools.sort_unstable();
    pools.dedup();
    assert_eq!(pools.len(), 1, "one Pool per Worker NUMA node: {names:?}");
    let pool = pools[0];
    assert!(
        pool.starts_with("default-numa-"),
        "a Pool is named after its NUMA node: {pool}"
    );
    let pool_buffers = pool_columns(&fixture, pool);
    assert!(
        pool_buffers[2] > 0,
        "`{pool}` keeps buffers on its free list: {pool_buffers:?}"
    );
    let pool_total: u64 = pool_buffers.iter().sum();
    assert!(
        (POOL_SLOTS / 2..=POOL_SLOTS * 2).contains(&pool_total),
        "`{pool}` holds the configured slots, rounded up to whole pages: {pool_total} vs {POOL_SLOTS}"
    );

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
        pool_columns(&fixture, pool),
    );
    std::thread::sleep(SAMPLE_INTERVAL);
    let second = (
        worker_columns(&fixture, "/sys/main_loop_count_per_worker"),
        worker_columns(&fixture, "/sys/loops_per_worker"),
        heartbeat(&fixture),
        pool_columns(&fixture, pool),
    );

    assert_eq!(first.0.len(), WORKER_COUNT);
    assert_eq!(first.1.len(), WORKER_COUNT);
    assert_eq!(
        second.3.iter().sum::<u64>(),
        pool_total,
        "`{pool}` keeps its buffer count across collect rounds: {:?} then {:?}",
        first.3,
        second.3
    );
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

/// Waits for the node-name vector to be published completely, then returns the
/// node names by slot.
///
/// The shape is published before the names, and both before the first collect
/// round; a reader that connects early therefore sees a partially filled
/// vector, which is why this waits for the published column count to match.
fn published_node_names(fixture: &Fixture<'_>, daemon: &mut HammerDaemon) -> Vec<String> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    loop {
        if let Some(Value::Names(names)) = fixture.read("/sys/node/names")
            && !names.is_empty()
            && names.iter().all(|name| !name.is_empty())
        {
            let columns = match fixture.read("/sys/node/calls") {
                Some(Value::Simple(rows)) => rows.first().map_or(0, Vec::len),
                _ => 0,
            };
            if columns == names.len() {
                return names;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{}",
            daemon.diagnostics("`/sys/node/names` is published with one name per node")
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `/sys/node/*` and `/nodes/<name>/*`: the five fixed entries of VPP's node
/// collector plus the four aliases of every node (`collector.c:18-27,79-89`).
#[test]
fn stats_segment_publishes_node_counters() {
    let mut daemon = HammerDaemon::start_with_node_counters();
    let mapping = daemon.mapping();
    let fixture = Fixture::new(mapping.bytes());

    let names = fixture
        .names()
        .unwrap_or_else(|| panic!("{}", daemon.diagnostics("no stable directory")));
    for expected in [
        "/sys/node/names",
        "/sys/node/clocks",
        "/sys/node/vectors",
        "/sys/node/calls",
        "/sys/node/suspends",
    ] {
        assert!(
            names.iter().any(|name| name == expected),
            "`{expected}` is published: {names:?}"
        );
    }

    let node_names = published_node_names(&fixture, &mut daemon);
    let mut unique = node_names.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        node_names.len(),
        "node names are unique: {node_names:?}"
    );

    // Rows are thread zero plus the Data Workers; columns are node slots.
    let row_count = WORKER_COUNT + 1;
    for counter in ["clocks", "vectors", "calls", "suspends"] {
        let rows = counter_rows(&fixture, &format!("/sys/node/{counter}"));
        assert_eq!(
            rows.len(),
            row_count,
            "`/sys/node/{counter}` has one row per thread"
        );
        for row in &rows {
            assert_eq!(
                row.len(),
                node_names.len(),
                "`/sys/node/{counter}` has one column per node"
            );
        }
    }

    // `/nodes/<name>/<counter>` is a symlink onto that node's column, so it
    // reads the same values as the column of the vector it aliases.
    for (slot, name) in node_names.iter().enumerate() {
        for counter in ["clocks", "vectors", "calls", "suspends"] {
            // VPP names the alias after the canonical entry leaf plus the node
            // name (`collector.c:79-89`), so `calls` stays `calls`.
            let path = format!("/sys/node/{counter}");
            let column: Vec<u64> = counter_rows(&fixture, &path)
                .iter()
                .map(|row| row[slot])
                .collect();
            let alias = counter_rows(&fixture, &format!("/nodes/{name}/{counter}"));
            let selected: Vec<u64> = alias.iter().map(|row| row[0]).collect();
            assert_eq!(
                selected, column,
                "`/nodes/{name}/{counter}` selects column {slot}"
            );
        }
    }

    drop(fixture);
    drop(mapping);
    let status = daemon.shutdown();
    assert!(
        status.success(),
        "{}",
        daemon.diagnostics(format!("daemon exits unsuccessfully: {status:?}"))
    );
}

/// One simple counter vector as `row → column → value`.
fn counter_rows(fixture: &Fixture<'_>, name: &str) -> Vec<Vec<u64>> {
    match fixture.read(name) {
        Some(Value::Simple(rows)) => rows,
        other => panic!("`{name}` is a counter vector, got {other:?}"),
    }
}

/// The plugin roots the registered-error half loads; the ip plugin owns the
/// ip4-local/ip4-receive/icmp-error nodes and the icmp plugin the echo and
/// input nodes, which are the only in-repo nodes that declare errors.
const ERROR_PLUGIN_ROOTS: &str = r#"["ip", "icmp"]"#;

/// Whether the plugin cdylibs the registered-error half loads were built next
/// to the daemon binary.
///
/// The CI build job's `cargo build --workspace --all-targets` and a plain
/// `cargo build --workspace` produce them; a bare `cargo test -p hammer` does
/// not, and a fixture must not silently claim to cover a case whose artifact is
/// absent.
fn plugin_cdylibs_present() -> bool {
    let directory = Path::new(DAEMON_BINARY)
        .parent()
        .expect("the daemon binary has a parent directory");
    ["ip", "icmp"].iter().all(|name| {
        directory
            .join(format!("libhammer_plugin_{name}.so"))
            .exists()
    })
}

/// `/node/errors` and `/err/<node>/<error>`: the error family VPP's
/// `vlib_register_errors` publishes (`error.c:113-200`).
///
/// The counter vector exists only once a node declares errors
/// (`error.c:135,158-159`), so a daemon with no plugins publishes neither the
/// vector nor a single alias; once the ip and icmp plugins are loaded, the
/// vector carries the reserved no-error column 0, one column per registered
/// error, one row per runtime thread, and one alias per error onto exactly its
/// own column.
#[test]
fn stats_segment_publishes_node_error_columns() {
    let mut daemon = HammerDaemon::start();
    let mapping = daemon.mapping();
    let fixture = Fixture::new(mapping.bytes());
    let names = fixture
        .names()
        .unwrap_or_else(|| panic!("{}", daemon.diagnostics("no stable directory")));
    assert!(
        !names.iter().any(|name| name == "/node/errors"),
        "a daemon whose nodes declare no errors publishes no error vector: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name.starts_with("/err/")),
        "a daemon whose nodes declare no errors publishes no error alias: {names:?}"
    );
    drop(fixture);
    drop(mapping);
    let status = daemon.shutdown();
    assert!(
        status.success(),
        "{}",
        daemon.diagnostics(format!("daemon exits unsuccessfully: {status:?}"))
    );

    if !plugin_cdylibs_present() {
        eprintln!(
            "skipping the registered-error half: plugin cdylibs are not next to {DAEMON_BINARY}"
        );
        return;
    }

    let mut daemon =
        HammerDaemon::start_with_plugins(&HammerDaemon::worker_config(), "", ERROR_PLUGIN_ROOTS);
    let mapping = daemon.mapping();
    let fixture = Fixture::new(mapping.bytes());
    let names = fixture
        .names()
        .unwrap_or_else(|| panic!("{}", daemon.diagnostics("no stable directory")));
    assert!(
        names.iter().any(|name| name == "/node/errors"),
        "the loaded plugins declare errors, so the vector exists: {names:?}"
    );

    let aliases: Vec<String> = names
        .iter()
        .filter(|name| name.starts_with("/err/"))
        .cloned()
        .collect();
    assert!(
        !aliases.is_empty(),
        "the loaded plugins' nodes registered error aliases: {names:?}"
    );

    // Rows are thread zero plus the Data Workers; columns are the reserved
    // no-error column 0 plus one column per registered error.
    let rows = counter_rows(&fixture, "/node/errors");
    assert_eq!(
        rows.len(),
        WORKER_COUNT + 1,
        "`/node/errors` has one row per runtime thread"
    );
    let columns = rows[0].len();
    assert_eq!(
        columns,
        aliases.len() + 1,
        "every published column but the reserved one is aliased by one error"
    );
    for row in &rows {
        assert_eq!(row.len(), columns, "`/node/errors` is rectangular");
    }

    // Every alias is a symlink onto `/node/errors` and selects its own column;
    // the ranges are contiguous and non-overlapping, so the aliased columns are
    // exactly 1..columns.
    let mut selected = Vec::with_capacity(aliases.len());
    for name in &aliases {
        let (target, column) = fixture
            .symlink(name)
            .unwrap_or_else(|| panic!("{}", daemon.diagnostics(format!("`{name}` resolves"))));
        assert_eq!(target, "/node/errors", "`{name}` aliases the error vector");
        assert!(column > 0, "`{name}` skips the reserved no-error column");
        assert_eq!(
            counter_rows(&fixture, name).len(),
            WORKER_COUNT + 1,
            "`{name}` has one row per runtime thread"
        );
        let (node, error) = name
            .trim_start_matches("/err/")
            .split_once('/')
            .expect("every alias is `/err/<node>/<error>`");
        assert!(
            !node.is_empty() && !error.is_empty(),
            "`{name}` names a node and an error"
        );
        selected.push(column);
    }
    selected.sort_unstable();
    assert_eq!(
        selected,
        (1..columns).collect::<Vec<_>>(),
        "each registered error owns exactly one column"
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

/// One `/memfd:buffers` mapping of a running daemon, as the kernel reports it.
#[derive(Debug)]
struct BufferMapping {
    huge_pages: bool,
    pages_per_node: Vec<(u32, u64)>,
}

/// The daemon's buffer mappings from `/proc/<pid>/numa_maps`.
fn buffer_mappings(pid: u32) -> Vec<BufferMapping> {
    let maps = fs::read_to_string(format!("/proc/{pid}/numa_maps"))
        .expect("the daemon publishes its NUMA maps");
    maps.lines()
        .filter(|line| line.contains("memfd:buffers"))
        .map(|line| {
            let mut mapping = BufferMapping {
                huge_pages: false,
                pages_per_node: Vec::new(),
            };
            for token in line.split_whitespace() {
                if token == "huge" {
                    mapping.huge_pages = true;
                } else if let Some(rest) = token.strip_prefix('N')
                    && let Some((node, pages)) = rest.split_once('=')
                    && let (Ok(node), Ok(pages)) = (node.parse(), pages.parse())
                {
                    mapping.pages_per_node.push((node, pages));
                }
            }
            mapping
        })
        .collect()
}

/// The CPUs this process may run on.
fn allowed_cpus() -> Vec<usize> {
    // SAFETY: `sched_getaffinity` writes into the live cpu set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is a live cpu set of the size the syscall expects.
    let result = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
    assert_eq!(
        result,
        0,
        "sched_getaffinity: {}",
        io::Error::last_os_error()
    );
    (0..libc::CPU_SETSIZE as usize)
        // SAFETY: the syscall filled the whole set.
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .collect()
}

/// The CPUs of one NUMA node, from its `cpulist` (`0-9,20-29`).
fn node_cpus(node: u32) -> Vec<usize> {
    let path = format!("/sys/devices/system/node/node{node}/cpulist");
    let text = fs::read_to_string(path).unwrap_or_default();
    text.trim()
        .split(',')
        .filter(|range| !range.is_empty())
        .flat_map(|range| match range.split_once('-') {
            Some((first, last)) => {
                let first: usize = first.parse().expect("cpulist range start");
                let last: usize = last.parse().expect("cpulist range end");
                (first..=last).collect::<Vec<_>>()
            }
            None => vec![range.parse().expect("cpulist entry")],
        })
        .collect()
}

/// The NUMA node one CPU belongs to, when this host reports node topology.
fn numa_node_of_cpu(cpu: usize) -> Option<u32> {
    let mut nodes: Vec<u32> = fs::read_dir("/sys/devices/system/node")
        .ok()?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_prefix("node")?.parse().ok()
        })
        .collect();
    nodes.sort_unstable();
    nodes
        .into_iter()
        .find(|node| node_cpus(*node).contains(&cpu))
}

/// One allowed CPU on each of the two lowest NUMA nodes that have one.
fn two_numa_nodes_with_allowed_cpus() -> Option<[(u32, usize); 2]> {
    let mut per_node: Vec<(u32, usize)> = Vec::new();
    for cpu in allowed_cpus() {
        let Some(node) = numa_node_of_cpu(cpu) else {
            continue;
        };
        if per_node.iter().any(|(known, _)| *known == node) {
            continue;
        }
        per_node.push((node, cpu));
        if per_node.len() == 2 {
            break;
        }
    }
    match per_node[..] {
        [first, second] => Some([first, second]),
        _ => None,
    }
}

/// Free HugeTLB pages of one size, or zero when the host has no such pool.
fn free_huge_pages(page_bytes: usize) -> u64 {
    let path = format!(
        "/sys/kernel/mm/hugepages/hugepages-{}kB/free_hugepages",
        page_bytes >> 10
    );
    fs::read_to_string(path)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// Buffer Pools follow the NUMA nodes their Data Workers run on, are backed by
/// the configured HugeTLB pages, and publish three consistent gauges each.
///
/// The host must expose two NUMA nodes with allowed CPUs and free 2 MiB HugeTLB
/// pages; run with
/// `cargo test -p hammer --test stats_segment_mapping -- --ignored`.
#[test]
#[ignore = "requires two NUMA nodes with allowed CPUs and free 2 MiB HugeTLB pages"]
fn buffer_pools_follow_worker_numa_nodes_on_huge_pages() {
    const HUGE_PAGE_BYTES: usize = 2 << 20;
    let Some([(first_node, first_cpu), (second_node, second_cpu)]) =
        two_numa_nodes_with_allowed_cpus()
    else {
        panic!("this host exposes no two NUMA nodes with allowed CPUs");
    };
    let free_pages = free_huge_pages(HUGE_PAGE_BYTES);
    assert!(
        free_pages > 0,
        "the host has no free {HUGE_PAGE_BYTES}-byte HugeTLB pages"
    );

    let spare: Vec<usize> = allowed_cpus()
        .into_iter()
        .filter(|cpu| *cpu != first_cpu && *cpu != second_cpu)
        .take(2)
        .collect();
    let mut worker_config = String::from("[worker]\ncount = 2\n\n[worker.cpu]\n");
    if let [main_cpu, app_cpu] = spare[..] {
        worker_config.push_str(&format!("main_core = {main_cpu}\napp_core = {app_cpu}\n"));
    }
    worker_config.push_str(&format!(
        "worker_cores = [{first_cpu}, {second_cpu}]\n\n[worker.numa]\nenabled = true\n\n[worker.buffer]\nslots_per_numa = {POOL_SLOTS}\npage_size = \"default-hugepage\"\n"
    ));

    let mut daemon = HammerDaemon::start_with_worker_config(&worker_config);
    let mapping = daemon.mapping();
    let fixture = Fixture::new(mapping.bytes());
    let names = fixture
        .names()
        .unwrap_or_else(|| panic!("{}", daemon.diagnostics("no stable directory")));

    let mut pools: Vec<String> = names
        .iter()
        .filter_map(|name| {
            let (pool, gauge) = name.strip_prefix("/buffer-pools/")?.split_once('/')?;
            matches!(gauge, "cached" | "used" | "available").then(|| pool.to_owned())
        })
        .collect();
    pools.sort_unstable();
    pools.dedup();
    assert_eq!(
        pools,
        [
            format!("default-numa-{first_node}"),
            format!("default-numa-{second_node}")
        ],
        "one Pool per Data Worker NUMA node: {names:?}"
    );

    let mut totals = Vec::new();
    for node in [first_node, second_node] {
        let pool = format!("default-numa-{node}");
        let buffers = pool_columns(&fixture, &pool);
        assert!(
            buffers[2] > 0,
            "`{pool}` keeps buffers on its free list: {buffers:?}"
        );
        let total: u64 = buffers.iter().sum();
        assert!(
            (POOL_SLOTS / 2..=POOL_SLOTS * 2).contains(&total),
            "`{pool}` holds its configured slots, rounded up to whole pages: {total}"
        );
        totals.push((node, total));
    }

    // The kernel's view of the same Pools: one HugeTLB-backed buffer mapping per
    // Node, each placed on that Node. A Pool that fell back to ordinary pages,
    // or was allocated on the wrong Node, cannot satisfy this.
    std::thread::sleep(SAMPLE_INTERVAL);
    let mappings = buffer_mappings(daemon.child.id());
    assert_eq!(
        mappings.len(),
        2,
        "one buffer mapping per Pool: {mappings:?}"
    );
    for node in [first_node, second_node] {
        assert!(
            mappings.iter().any(|mapping| {
                mapping.huge_pages
                    && mapping
                        .pages_per_node
                        .iter()
                        .any(|(mapped_node, pages)| *mapped_node == node && *pages > 0)
            }),
            "a HugeTLB buffer mapping is placed on NUMA node {node}: {mappings:?}"
        );
    }
    for (node, total) in &totals {
        let pool = format!("default-numa-{node}");
        assert_eq!(
            pool_columns(&fixture, &pool).iter().sum::<u64>(),
            *total,
            "`{pool}` keeps its buffer count across collect rounds"
        );
    }

    drop(fixture);
    drop(mapping);
    let status = daemon.shutdown();
    assert!(
        status.success(),
        "{}",
        daemon.diagnostics(format!("daemon exits unsuccessfully: {status:?}"))
    );
}
