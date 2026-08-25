use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use hammer_infra::segment::SegmentAllocationError;
use socket2::{Domain, SockAddr, Socket, Type};

mod metric;
mod protocol;
mod segment;
#[path = "stats_segment_socket.rs"]
mod stats_segment_socket_source;

pub use stats_segment_socket_source::stats_segment_socket;

pub use metric::{
    CombinedCounter, Gauge, Histogram, MetricValue, NameVector, Ring, RingSchema, SimpleCounter,
    Timestamp,
};
pub use protocol::{
    Counter, DirectoryDataPointer, DirectoryEntry, DirectoryType, Gauge as GaugeValue,
    RingBufferHeader, RingConfig, ScalarBits, SharedHeader, StringVectorPointer, ring_layout,
    vec_len, vector_element_offset,
};
use protocol::{DirectoryIndex, NameBytes};
use segment::StatsSegment;

pub struct StatsMain {
    segment: StatsSegment,
    socket_path: PathBuf,
}

static STATS_MAIN: OnceLock<StatsMain> = OnceLock::new();
const STATS_SOCKET_BACKLOG: i32 = 5;

// SAFETY: StatsSegmentState contains mapped raw pointers, but every access to
// the directory and payload ownership is serialized by its SpinLock. StatsMain
// never exposes mapped pointers.
unsafe impl Send for StatsMain {}
unsafe impl Sync for StatsMain {}

impl StatsMain {
    pub fn init(name: &str, size: usize, socket_path: &Path) -> StatsResult<OwnedFd> {
        if STATS_MAIN.get().is_some() {
            return Err(StatsError::AlreadyInitialized);
        }
        if socket_path.as_os_str().is_empty() {
            return Err(StatsError::InvalidSocketPath);
        }
        let socket_path = socket_path.to_owned();
        let segment = StatsSegment::create(name, size)?;
        if let Err(source) = std::fs::remove_file(&socket_path)
            && source.kind() != io::ErrorKind::NotFound
        {
            return Err(StatsError::Io(source));
        }
        #[cfg(target_os = "linux")]
        let socket = Socket::new(Domain::UNIX, Type::SEQPACKET, None);
        #[cfg(not(target_os = "linux"))]
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None);
        let socket = socket.map_err(StatsError::Io)?;
        socket.set_nonblocking(true).map_err(StatsError::Io)?;
        socket.set_cloexec(true).map_err(StatsError::Io)?;
        let address = SockAddr::unix(&socket_path).map_err(StatsError::Io)?;
        #[cfg(target_os = "linux")]
        {
            let enabled: libc::c_int = 1;
            // SAFETY: `socket` is a live Unix socket and `enabled` points to a
            // valid integer for the duration of this setsockopt call.
            let result = unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_PASSCRED,
                    std::ptr::from_ref(&enabled).cast(),
                    std::mem::size_of_val(&enabled) as libc::socklen_t,
                )
            };
            if result < 0 {
                return Err(StatsError::Io(io::Error::last_os_error()));
            }
        }
        // Match VPP's allow-group-write bind: preserve the process umask while
        // preventing other-write during creation so the group-write bit remains.
        #[cfg(unix)]
        let previous_umask = unsafe { libc::umask(libc::S_IWOTH) };
        let bind_result = socket.bind(&address).map_err(StatsError::Io);
        #[cfg(unix)]
        {
            // SAFETY: restore the exact value returned by the preceding umask.
            unsafe { libc::umask(previous_umask) };
        }
        bind_result?;
        if let Err(error) = socket.listen(STATS_SOCKET_BACKLOG).map_err(StatsError::Io) {
            drop(socket);
            if let Err(source) = std::fs::remove_file(&socket_path)
                && source.kind() != io::ErrorKind::NotFound
            {
                return Err(StatsError::Io(source));
            }
            return Err(error);
        }
        let raw_listener = socket.into_raw_fd();
        // SAFETY: `raw_listener` is transferred from `socket` and is not used
        // again after ownership moves into `OwnedFd`.
        let listener = unsafe { OwnedFd::from_raw_fd(raw_listener) };
        let cleanup_path = socket_path.clone();
        if STATS_MAIN
            .set(Self {
                segment,
                socket_path,
            })
            .is_err()
        {
            drop(listener);
            if let Err(source) = std::fs::remove_file(cleanup_path)
                && source.kind() != io::ErrorKind::NotFound
            {
                return Err(StatsError::Io(source));
            }
            return Err(StatsError::AlreadyInitialized);
        }
        Ok(listener)
    }

    pub fn global() -> StatsResult<&'static Self> {
        STATS_MAIN.get().ok_or(StatsError::NotInitialized)
    }

    pub fn unlink_socket_path(&self) -> StatsResult<()> {
        match std::fs::remove_file(&self.socket_path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(StatsError::Io(source)),
        }
    }

    pub fn accept(&self, listener_fd: RawFd) -> StatsResult<()> {
        let accepted = loop {
            #[cfg(target_os = "linux")]
            let raw_fd = unsafe {
                libc::accept4(
                    listener_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            #[cfg(not(target_os = "linux"))]
            let raw_fd =
                unsafe { libc::accept(listener_fd, std::ptr::null_mut(), std::ptr::null_mut()) };
            if raw_fd >= 0 {
                #[cfg(not(target_os = "linux"))]
                {
                    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
                    if flags < 0
                        || unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) }
                            < 0
                        || unsafe { libc::fcntl(raw_fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0
                    {
                        // SAFETY: `raw_fd` is the descriptor returned by accept.
                        unsafe { libc::close(raw_fd) };
                        return Err(StatsError::Io(io::Error::last_os_error()));
                    }
                }
                // SAFETY: `raw_fd` is a newly accepted descriptor.
                break unsafe { OwnedFd::from_raw_fd(raw_fd) };
            }
            let source = io::Error::last_os_error();
            match source.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(StatsError::Io(source)),
            }
        };
        #[cfg(target_os = "macos")]
        {
            let one: libc::c_int = 1;
            // SAFETY: `accepted` is a live Unix socket owned for this call.
            let result = unsafe {
                libc::setsockopt(
                    accepted.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    std::ptr::from_ref(&one).cast(),
                    std::mem::size_of_val(&one) as libc::socklen_t,
                )
            };
            if result < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::InvalidInput {
                    return Ok(());
                }
                return Err(StatsError::Io(source));
            }
        }
        match self.segment.send_to(accepted.as_fd()) {
            Err(StatsError::Io(source))
                if matches!(
                    source.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::NotConnected
                ) =>
            {
                Ok(())
            }
            result => result,
        }
    }

    #[cfg(test)]
    pub(crate) fn create(name: &str, size: usize) -> StatsResult<Self> {
        Ok(Self {
            segment: StatsSegment::create(name, size)?,
            socket_path: PathBuf::from("/dev/null"),
        })
    }

    pub(crate) fn bind_index(
        &self,
        path: &str,
        expected: DirectoryType,
    ) -> StatsResult<DirectoryIndex> {
        let name = NameBytes::try_from(path)?;
        self.segment.find(name, path, expected)
    }

    pub(crate) fn store_timestamp(&self, index: DirectoryIndex, value: u64) -> StatsResult<()> {
        self.segment.store_timestamp(index, value)
    }

    pub(crate) fn increment_timestamp(&self, index: DirectoryIndex) -> StatsResult<()> {
        self.segment.increment_timestamp(index)
    }

    pub(crate) fn store_gauge(&self, index: DirectoryIndex, value: f64) -> StatsResult<()> {
        self.segment.store_gauge(index, value)
    }

    pub(crate) fn validate_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
    ) -> StatsResult<()> {
        self.segment.validate(index, row, column)
    }

    pub(crate) fn write_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: u64,
    ) -> StatsResult<()> {
        self.segment.add_simple_counter(index, row, column, value)
    }

    pub(crate) fn write_combined_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: Counter,
    ) -> StatsResult<()> {
        self.segment.add_combined_counter(index, row, column, value)
    }

    pub(crate) fn write_histogram(
        &self,
        index: DirectoryIndex,
        row: u32,
        bucket: u32,
        value: u64,
    ) -> StatsResult<()> {
        self.segment.add_histogram(index, row, bucket, value)
    }

    pub fn add_gauge(&self, descriptor: Gauge) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::Gauge>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_timestamp(&self, descriptor: Timestamp) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::ScalarBits>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_simple_counter(&self, descriptor: SimpleCounter) -> StatsResult<()> {
        let layout = metric::layout::Simple::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_combined_counter(&self, descriptor: CombinedCounter) -> StatsResult<()> {
        let layout = metric::layout::Combined::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_name_vector(&self, descriptor: NameVector) -> StatsResult<()> {
        let layout = metric::layout::NameVector::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_histogram(&self, descriptor: Histogram) -> StatsResult<()> {
        let layout = metric::layout::Histogram::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_ring<T>(&self, descriptor: Ring<T>) -> StatsResult<()>
    where
        T: RingSchema,
    {
        let layout = metric::layout::Ring::<T>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }
}

pub type StatsResult<T> = Result<T, StatsError>;

#[derive(Debug)]
pub enum StatsError {
    Protocol,
    Io(io::Error),
    Allocation(SegmentAllocationError),
    CapacityTooSmall {
        requested: usize,
        minimum: usize,
    },
    InvalidLayout,
    CollectionCapacity,
    DuplicateName,
    MetricNotFound {
        name: String,
    },
    MetricTypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    MetricUnbound,
    DirectoryIndexOutOfBounds {
        index: u32,
        length: usize,
    },
    DirectoryEntryUnavailable {
        index: u32,
    },
    Teardown,
    WorkerNotQuiescent,
    InvalidShape,
    InvalidRingSchema {
        expected: usize,
        actual: usize,
    },
    PublicationFailed,
    InvalidSocketPath,
    AlreadyInitialized,
    NotInitialized,
    ClientConnect {
        path: PathBuf,
        source: io::Error,
    },
    ClientReceive {
        source: io::Error,
    },
    ClientAncillaryData {
        received_fds: usize,
        malformed: bool,
    },
    ClientFstat {
        source: io::Error,
    },
    ClientMapping {
        source: io::Error,
    },
    ClientRetryExhausted {
        operation: &'static str,
    },
}

impl fmt::Display for StatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol => formatter.write_str("stats protocol error"),
            Self::Io(error) => write!(formatter, "stats mapping I/O error: {error}"),
            Self::Allocation(error) => write!(formatter, "stats allocation error: {error}"),
            Self::CapacityTooSmall { requested, minimum } => write!(
                formatter,
                "stats mapping capacity {requested} is below minimum {minimum}"
            ),
            Self::InvalidLayout => formatter.write_str("invalid stats allocation layout"),
            Self::CollectionCapacity => {
                formatter.write_str("stats owner collection capacity failed")
            }
            Self::DuplicateName => formatter.write_str("duplicate stats directory name"),
            Self::MetricNotFound { name } => {
                write!(formatter, "stats metric `{name}` is not registered")
            }
            Self::MetricTypeMismatch { expected, actual } => write!(
                formatter,
                "stats metric has type `{actual}`, expected `{expected}`"
            ),
            Self::MetricUnbound => {
                formatter.write_str("stats metric is not bound to a directory entry")
            }
            Self::DirectoryIndexOutOfBounds { index, length } => write!(
                formatter,
                "stats directory index {index} is outside length {length}"
            ),
            Self::DirectoryEntryUnavailable { index } => {
                write!(formatter, "stats directory entry {index} is unavailable")
            }
            Self::Teardown => formatter.write_str("stats segment is tearing down"),
            Self::WorkerNotQuiescent => formatter.write_str("stats workers are not quiescent"),
            Self::InvalidShape => formatter.write_str("invalid stats shape"),
            Self::InvalidRingSchema { expected, actual } => write!(
                formatter,
                "invalid stats ring schema: expected {expected} bytes, got {actual} bytes"
            ),
            Self::PublicationFailed => formatter.write_str("stats publication failed"),
            Self::InvalidSocketPath => formatter.write_str("stats socket path is required"),
            Self::AlreadyInitialized => formatter.write_str("stats main is already initialized"),
            Self::NotInitialized => formatter.write_str("stats main is not initialized"),
            Self::ClientConnect { path, source } => {
                write!(
                    formatter,
                    "connect to stats socket `{}`: {source}",
                    path.display()
                )
            }
            Self::ClientReceive { source } => {
                write!(formatter, "receive stats segment fd: {source}")
            }
            Self::ClientAncillaryData {
                received_fds,
                malformed,
            } => write!(
                formatter,
                "invalid stats ancillary data: received {received_fds} fd(s), malformed={malformed}"
            ),
            Self::ClientFstat { source } => write!(formatter, "stat stats segment fd: {source}"),
            Self::ClientMapping { source } => {
                write!(formatter, "map stats segment read-only: {source}")
            }
            Self::ClientRetryExhausted { operation } => {
                write!(
                    formatter,
                    "stats client retry limit exhausted during {operation}"
                )
            }
        }
    }
}

impl std::error::Error for StatsError {}

impl From<protocol::Error> for StatsError {
    fn from(_error: protocol::Error) -> Self {
        Self::Protocol
    }
}

impl From<io::Error> for StatsError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentAllocationError> for StatsError {
    fn from(error: SegmentAllocationError) -> Self {
        Self::Allocation(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRingEntry;

    impl metric::RingSchema for TestRingEntry {
        const ENTRY_SIZE: u32 = 4;
        const SCHEMA_VERSION: u32 = 1;

        fn schema() -> &'static [u8] {
            &[1, 2]
        }

        fn encode(&self, destination: &mut [u8]) -> StatsResult<()> {
            if destination.len() != Self::ENTRY_SIZE as usize {
                return Err(StatsError::InvalidShape);
            }
            destination.fill(0);
            Ok(())
        }

        fn decode(source: &[u8]) -> StatsResult<Self> {
            if source.len() != Self::ENTRY_SIZE as usize {
                return Err(StatsError::InvalidShape);
            }
            Ok(Self)
        }
    }

    #[test]
    fn add_boundaries_convert_and_delegate_all_metric_families() -> StatsResult<()> {
        let stats = StatsMain::create("st-facade", 2 * 1024 * 1024)?;
        stats.add_gauge(metric::Gauge::new("/facade/gauge"))?;
        stats.add_timestamp(metric::Timestamp::new("/facade/timestamp"))?;
        stats.add_simple_counter(metric::SimpleCounter::new("/facade/simple"))?;
        stats.add_combined_counter(metric::CombinedCounter::new("/facade/combined"))?;
        stats.add_name_vector(metric::NameVector {
            name: "/facade/names".to_owned(),
            length: 2,
        })?;
        stats.add_histogram(metric::Histogram::new("/facade/histogram"))?;
        stats.add_ring(metric::Ring::<TestRingEntry>::new(
            "/facade/ring".to_owned(),
            protocol::RingConfig::new(
                <TestRingEntry as metric::RingSchema>::ENTRY_SIZE,
                2,
                1,
                <TestRingEntry as metric::RingSchema>::schema().len() as u32,
                <TestRingEntry as metric::RingSchema>::SCHEMA_VERSION,
            ),
            <TestRingEntry as metric::RingSchema>::schema().into(),
        ))?;

        assert_eq!(stats.segment.directory_vector_len(), 7);
        assert!(matches!(
            stats.add_gauge(metric::Gauge::new("/facade/gauge")),
            Err(StatsError::DuplicateName)
        ));
        Ok(())
    }
}
