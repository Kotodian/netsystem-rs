//! `[statseg]` configuration and lifecycle for the shared stats segment.
//!
//! Mirrors VPP's `statseg_config` early config function plus the stat segment
//! initialization, listener socket, collector process and main-loop-exit
//! unlink.

use std::io::{self, IoSlice};
use std::mem::size_of;
use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use byte_unit::Byte;
use hammer_component_macros::Stats;
use hammer_infra::mem::{MemHeap, MemMain, PageSize};
use hammer_stats::{
    Collector, DirectoryEntry, DirectoryIndex, SimpleCounter, StatsMain, Timestamp,
};
use socket2::{Domain, MsgHdr, SockAddr, SockRef, Socket, Type};

use crate::error::RuntimeResult;
use crate::file::FILE_MAIN;
use crate::{DataPlaneMain, File, RuntimeError, ThreadMain};

pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_STATS_SEGMENT_SIZE: usize = 32 << 20;

fn default_size() -> Byte {
    Byte::from_u64(DEFAULT_STATS_SEGMENT_SIZE as u64)
}

fn default_page_size() -> PageSize {
    PageSize::Default
}

fn default_update_interval() -> Duration {
    DEFAULT_UPDATE_INTERVAL
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatsConfig {
    pub socket_name: PathBuf,
    #[serde(default = "default_size")]
    pub size: Byte,
    #[serde(default = "default_page_size")]
    pub page_size: PageSize,
    #[serde(default)]
    pub per_node_counters: bool,
    #[serde(default = "default_update_interval", with = "humantime_serde")]
    pub update_interval: Duration,
}

impl StatsConfig {
    fn validate(&self) -> RuntimeResult<()> {
        if self.socket_name.as_os_str().is_empty() {
            return Err(RuntimeError::ConfigValidation {
                message: "statseg.socket_name is required".to_owned(),
            });
        }
        if self.memory_size().is_none() {
            return Err(RuntimeError::ConfigValidation {
                message: "statseg.size must be a non-zero byte size".to_owned(),
            });
        }
        Ok(())
    }

    fn memory_size(&self) -> Option<usize> {
        usize::try_from(self.size.as_u64())
            .ok()
            .filter(|size| *size != 0)
    }
}

static STATS_CONFIG: OnceLock<StatsConfig> = OnceLock::new();

/// The fixed system metrics plus the main loop's own two per-worker vectors.
///
/// Each `bootstrap` field names the fixed VPP directory index of its slot:
/// the declaration creates those slots once, in `Sys::bootstrap`, and
/// `Sys::install` binds them afterwards. `/sys/num_worker_threads` is not
/// here: the worker count belongs to the worker-thread domain, which creates
/// that gauge in its own registration.
#[derive(Stats)]
pub(crate) struct Sys {
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_HEARTBEAT)]
    // Advanced through the segment's own collector work, like the VPP
    // `STAT_COUNTER_HEARTBEAT` update in `do_stat_segment_updates`.
    #[allow(dead_code)]
    heartbeat: Timestamp,
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_LAST_STATS_CLEAR)]
    // Consumed by the owner clear baseline, which does not exist yet.
    #[allow(dead_code)]
    last_stats_clear: Timestamp,
    #[stats(bootstrap = hammer_stats::STAT_COUNTER_BOOTTIME)]
    boottime: Timestamp,
    /// Cumulative main-loop count of each Data Worker, one column per worker.
    #[stats(columns = worker_column_count())]
    main_loop_count_per_worker: SimpleCounter,
    /// Damped loops per second of each Data Worker, one column per worker.
    #[stats(columns = worker_column_count())]
    loops_per_worker: SimpleCounter,
}

/// Published width of one per-worker `/sys` vector: one column per Data Worker.
fn worker_column_count() -> u32 {
    ThreadMain::global().worker_count()
}

/// `/mem/main heap`: the process Main Heap's seven columns and three aliases.
#[derive(Stats)]
pub(crate) struct MainHeapUsage {
    #[stats(
        path = "/mem/main heap",
        columns = hammer_stats::mem::STAT_MEM_COLUMNS,
        symlinks = [
            ("total", hammer_stats::mem::STAT_MEM_TOTAL),
            ("used", hammer_stats::mem::STAT_MEM_USED),
            ("free", hammer_stats::mem::STAT_MEM_FREE),
        ],
    )]
    usage: SimpleCounter,
}

/// `/mem/stat segment`: the stats segment's own heap.
#[derive(Stats)]
pub(crate) struct StatSegmentUsage {
    #[stats(
        path = "/mem/stat segment",
        columns = hammer_stats::mem::STAT_MEM_COLUMNS,
        symlinks = [
            ("total", hammer_stats::mem::STAT_MEM_TOTAL),
            ("used", hammer_stats::mem::STAT_MEM_USED),
            ("free", hammer_stats::mem::STAT_MEM_FREE),
        ],
    )]
    usage: SimpleCounter,
}

#[hammer_component_macros::process_node(name = "statseg-collector-process")]
fn stat_segment_collector_process(
    _: &mut DataPlaneMain,
) -> impl std::future::Future<Output = RuntimeResult<()>> + Send + 'static {
    async move {
        let sys = Sys::global();
        let stats_main = StatsMain::global()?;
        let boottime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| RuntimeError::SystemClockBeforeUnixEpoch { source })?
            .as_secs();
        stats_main
            .segment
            .set_timestamp(sys.boottime.index, boottime);

        loop {
            stats_main.collect();
            let update_interval = stats_main.segment.update_interval();
            tokio::time::sleep(update_interval).await;
        }
    }
}

pub(crate) fn stats_config() -> &'static StatsConfig {
    STATS_CONFIG
        .get()
        .expect("stats configuration is installed before stats initialization")
}

#[hammer_component_macros::config_function(
    name = "runtime_stats_config",
    section = "statseg",
    early = true,
    required = true
)]
fn configure_stats(config: StatsConfig) -> RuntimeResult<()> {
    config.validate()?;
    assert!(
        STATS_CONFIG.set(config).is_ok(),
        "stats configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(name = "stats_main_init")]
fn init_stats_main(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let config = stats_config();
    // Establishment order follows `vlib_stats_init`: create the segment and its
    // owner, fill the fixed slots, run the registration image (entry
    // declarations and collector registrations), publish the owner, then hand
    // the segment descriptor to readers.
    let mut stats_main = StatsMain::create(
        "stat segment",
        config
            .memory_size()
            .expect("stats segment size is validated by the config function"),
        config.page_size,
        config.update_interval,
        config.per_node_counters,
    )?;
    Sys::bootstrap(&stats_main.segment)?;
    crate::init::run_stats_registrations(&mut stats_main)?;
    stats_main.publish()?;
    let listener =
        bind_listener(&config.socket_name).map_err(|source| RuntimeError::StatsListenerBind {
            path: config.socket_name.clone(),
            source,
        })?;
    FILE_MAIN
        .get()
        .expect("FileMain is initialized before stats startup")
        .add(File::new(
            OwnedFd::from(listener),
            format!("stats segment listener {}", config.socket_name.display()),
            0,
            stats_segment_listener::file_functions::<crate::NodeMain, crate::RuntimeError>(),
        ))?;
    Ok(())
}

// ---- Collectors: one type per "way of reading a value", registered by this
// ---- module's collect registrations. A collector writes only its own entry.

/// Reads one heap into its `/mem` entry, VPP's `stat_provider_mem_usage_update_fn`.
///
/// One type serves every heap; the instance's own field is the heap it reads,
/// the Rust form of VPP's `private_data` cell (`provider_mem.c:69`).
struct HeapCollector {
    entry_index: DirectoryIndex,
    heap: &'static MemHeap,
}

impl Collector for HeapCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        hammer_stats::mem::update_mem_usage(entry, self.heap.usage());
    }
}

/// Which per-worker published value one `/sys` vector reproduces.
enum WorkerCounter {
    /// Cumulative main-loop count.
    MainLoopCount,
    /// Damped loops per second.
    LoopsPerSecond,
}

/// Copies one worker-published per-worker vector into its `/sys` entry.
///
/// VPP's `vector_rate_collector_fn` has the same shape: the round only copies
/// what each worker published, so it neither allocates nor returns an error.
struct WorkerCounterCollector {
    entry_index: DirectoryIndex,
    counter: WorkerCounter,
}

impl Collector for WorkerCounterCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        let threads = ThreadMain::global();
        for slot in 0..threads.worker_count() {
            let thread_index = slot + 1;
            let value = match self.counter {
                WorkerCounter::MainLoopCount => threads
                    .worker_main_loop_count(thread_index)
                    .load(Ordering::Relaxed),
                WorkerCounter::LoopsPerSecond => threads
                    .worker_loops_per_second(thread_index)
                    .load(Ordering::Relaxed),
            };
            entry.set_simple_counter_cell(0, slot, value);
        }
    }
}

// ---- Collect registrations: what each owner updates each round, one per entry.

/// Registers the process main heap collector.
#[hammer_component_macros::stats_collect_registration]
fn register_main_heap(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    stats_main.register_collector(HeapCollector {
        entry_index: MainHeapUsage::global().usage.index,
        heap: MemMain::main_heap(),
    });
    Ok(())
}

/// Registers the stats segment's own heap collector.
#[hammer_component_macros::stats_collect_registration]
fn register_stat_segment_heap(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    stats_main.register_collector(HeapCollector {
        entry_index: StatSegmentUsage::global().usage.index,
        heap: stats_main.segment.heap(),
    });
    Ok(())
}

/// Registers the two per-worker main-loop vectors, one collector each.
#[hammer_component_macros::stats_collect_registration]
fn register_worker_main_loop(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    let sys = Sys::global();
    for (entry_index, counter) in [
        (
            sys.main_loop_count_per_worker.index,
            WorkerCounter::MainLoopCount,
        ),
        (sys.loops_per_worker.index, WorkerCounter::LoopsPerSecond),
    ] {
        stats_main.register_collector(WorkerCounterCollector {
            entry_index,
            counter,
        });
    }
    Ok(())
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_stats_main(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    // `stats_segment_socket_exit` unlinks the listener path and does not fail
    // shutdown: a path that survives is reclaimed by the next startup bind.
    if let Err(source) = std::fs::remove_file(&stats_config().socket_name)
        && source.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(%source, "failed to unlink the stats segment listener path");
    }
    Ok(())
}

/// File callbacks of the registered stats segment listener.
///
/// The listener only serves the descriptor handoff, so it has no write
/// callback, like VPP's `stats_socket_accept_ready`.
#[hammer_component_macros::file]
mod stats_segment_listener {
    fn read<Context, Error>(
        _: &mut Context,
        file: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<crate::RuntimeError>,
    {
        super::handoff_segment(file.fd()).map_err(Into::into)
    }

    fn error<Context, Error>(
        _: &mut Context,
        _: &mut hammer_core::file::File<Context, Error>,
    ) -> Result<(), Error>
    where
        Error: From<crate::RuntimeError>,
    {
        Err(crate::RuntimeError::FileAccept {
            source: std::io::Error::from(std::io::ErrorKind::ConnectionAborted),
        }
        .into())
    }
}

/// Listener backlog depth, matching `clib_socket_init`'s `listen(fd, 5)`.
const SOCKET_BACKLOG: i32 = 5;

/// Binds the stats segment listener, mirroring the Unix server path of
/// `clib_socket_init`: Linux listeners keep the stats seqpacket message
/// boundary, a listener path whose owning process is gone is reclaimed, while a
/// live listener or an existing non-socket path is an error.
fn bind_listener(socket_name: &Path) -> io::Result<Socket> {
    #[cfg(target_os = "linux")]
    let socket_type = Type::SEQPACKET;
    #[cfg(not(target_os = "linux"))]
    let socket_type = Type::STREAM;
    let listener = Socket::new(Domain::UNIX, socket_type, None)?;
    // File readiness drives the accept, so the listener must never block the
    // main-thread poller.
    listener.set_nonblocking(true)?;
    let address = SockAddr::unix(socket_name)?;
    match bind_group_writable(&listener, &address) {
        Ok(()) => {}
        Err(bind_error) if bind_error.kind() == io::ErrorKind::AddrInUse => {
            match UnixStream::connect(socket_name) {
                Ok(_) => return Err(bind_error),
                Err(source) if source.kind() == io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(socket_name)?;
                    bind_group_writable(&listener, &address)?;
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    bind_group_writable(&listener, &address)?;
                }
                Err(_) => return Err(bind_error),
            }
        }
        Err(source) => return Err(source),
    }
    listener.listen(SOCKET_BACKLOG)?;
    Ok(listener)
}

/// Binds while keeping the group-write permission of the stats listener, like
/// the `CLIB_SOCKET_F_ALLOW_GROUP_WRITE` bind in `clib_socket_init`.
fn bind_group_writable(listener: &Socket, address: &SockAddr) -> io::Result<()> {
    // The umask window only covers this bind and is restored unconditionally.
    let previous_umask = unsafe { libc::umask(libc::S_IWOTH) };
    let result = listener.bind(address);
    // SAFETY: restore the exact value returned by the preceding umask call.
    unsafe { libc::umask(previous_umask) };
    result
}

/// Accepts one pending stats reader and hands it the segment descriptor, like
/// VPP's `stats_socket_accept_ready`.
fn handoff_segment(listener_fd: RawFd) -> RuntimeResult<()> {
    // SAFETY: the callback passes the descriptor of the registered listener;
    // the reference below only borrows it.
    let listener_fd = unsafe { BorrowedFd::borrow_raw(listener_fd) };
    let listener = SockRef::from(&listener_fd);
    let (peer, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(source) => return Err(RuntimeError::FileAccept { source }),
        }
    };
    // `clib_socket_accept` makes the accepted client non-blocking.
    peer.set_nonblocking(true)
        .map_err(|source| RuntimeError::FileAccept { source })?;
    // Hammer does not ignore SIGPIPE process-wide like `unix_main_signal_init`,
    // so a macOS reader that vanished before the handoff must not kill the
    // daemon.
    #[cfg(target_os = "macos")]
    if let Err(source) = peer.set_nosigpipe(true) {
        if source.kind() == io::ErrorKind::InvalidInput {
            // macOS rejects the option once the reader closed before the
            // accept; the connection cannot be served.
            return Ok(());
        }
        return Err(RuntimeError::FileAccept { source });
    }
    let segment_fd = StatsMain::global()?.segment.segment_fd();
    // `stats_socket_accept_ready` reports a failed handoff and closes the
    // connection; the listener stays registered.
    if let Err(source) = send_segment_fd(&peer, segment_fd) {
        tracing::warn!(%source, "failed to hand the stats segment descriptor to a reader");
    }
    Ok(())
}

/// Sends the shared segment descriptor to one stats reader, like
/// `clib_socket_sendmsg` with a zero-length message.
fn send_segment_fd(peer: &Socket, segment_fd: RawFd) -> io::Result<()> {
    let control_bytes = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize };
    let mut control = vec![0_u8; control_bytes];
    // SAFETY: `header` only addresses the control buffer of this call.
    unsafe {
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len() as _;
        let header = libc::CMSG_FIRSTHDR(&message);
        debug_assert!(!header.is_null(), "control buffer fits one descriptor");
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
        ptr::write_unaligned(libc::CMSG_DATA(header).cast::<RawFd>(), segment_fd);
    }
    let payload = [IoSlice::new(&[])];
    let message = MsgHdr::new().with_buffers(&payload).with_control(&control);
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_NOSIGNAL;
    #[cfg(not(target_os = "linux"))]
    let flags = 0;
    let sent = peer.sendmsg(&message, flags)?;
    debug_assert_eq!(sent, 0, "descriptor handoff carries no payload");
    Ok(())
}
