//! Real-queue harness for the hammer-app VCL MQ protocol tests.
//!
//! No closure replaces the dispatcher and no reply is fabricated through an
//! injected handler: the daemon side owns the real reply `SessionProducer`
//! over a real shared control segment, consumes the client's real requests,
//! and delivers established Sessions in the production attach descriptor
//! format (metadata words + SCM_RIGHTS). Blocking client flows wait on the
//! real queue signal pair.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use hammer_app::attach::AppClient;
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSessionConfig, SessionControlPayload, SessionHandle, SessionMsgQueue, SessionOffsets,
    SessionProducer, SingleProducer,
};
use hammer_runtime::attach::{
    ATTACH_METADATA_BYTES, ATTACH_METADATA_WORDS, ATTACH_PROTOCOL_VERSION,
};

/// One real Application Session control request/reply queue pair over a
/// shared segment, plus the daemon-side descriptor stream.
///
/// The client owns the request producer and the reply consumer (the
/// production AppClient roles); the daemon owns the request consumer and the
/// reply producer. Replies enqueued through `enqueue` signal the client
/// through the real queue signal pair.
pub struct TestControlPair {
    pub application: u32,
    /// Daemon-side consumer of the client's Session control requests.
    pub requests: SessionMsgQueue<SingleProducer>,
    /// Daemon-side producer of Session control replies.
    pub replies: SessionProducer,
    /// Daemon side of the descriptor stream: established-Session metadata
    /// words and SCM_RIGHTS descriptors are written here.
    pub stream: UnixStream,
}

impl TestControlPair {
    /// Consumes the next client request, decoded as `M`.
    pub fn dequeue<M: SessionControlPayload>(&mut self) -> M {
        let item = self
            .requests
            .dequeue_control()
            .expect("dequeue control request")
            .expect("client request present");
        item.decode::<M>()
            .expect("decode request payload")
            .expect("decode request")
    }

    /// Enqueues one reply on the real reply queue; the signal wakes a
    /// blocking client wait.
    pub fn enqueue<M: SessionControlPayload>(&mut self, message: &M) {
        self.replies
            .enqueue_control(message)
            .expect("enqueue control reply");
    }
}

/// One control queue initialized at `offset` of `seg` with the production
/// shape (fixed control slots, signal pair).
fn init_control_queue(seg: &Segment, offset: u64) -> SessionMsgQueue<SingleProducer> {
    let (q_nitems, ring_nitems) = (32, 32);
    unsafe {
        SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(
            seg.clone(),
            offset,
            q_nitems,
            ring_nitems,
        )
    }
    .expect("control queue")
}

/// Builds one client/daemon control pair over a real shared control segment
/// with the production queue shape (fixed control slots, signal pair).
pub fn control_pair() -> (AppClient, TestControlPair) {
    let application = 7;
    let control = Segment::shared_default();
    let layout = SessionMsgQueue::<SingleProducer>::layout_bytes_with_control(32, 32)
        .expect("control queue layout");
    let requests_off = control.alloc(layout, 64).expect("requests queue offset");
    let replies_off = control.alloc(layout, 64).expect("replies queue offset");
    let requests_queue = init_control_queue(&control, requests_off);
    let replies_queue = init_control_queue(&control, replies_off);
    let requests_producer = requests_queue
        .claim_producer()
        .expect("claim requests producer");
    let replies_producer = replies_queue
        .claim_producer()
        .expect("claim replies producer");
    // One Application Rx MQ for Data Worker 0; established Sessions are
    // published on worker 0 so the client resolves them here.
    let rx_mq = Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("Application Rx MQ"));
    let (client_stream, daemon_stream) = UnixStream::pair().expect("descriptor stream pair");
    let client = AppClient::with_queues(
        client_stream,
        application,
        requests_producer,
        replies_queue,
        Box::new([rx_mq]),
    );
    let pair = TestControlPair {
        application,
        requests: requests_queue,
        replies: replies_producer,
        stream: daemon_stream,
    };
    (client, pair)
}

/// One published established Session: the shared segment and layout the
/// daemon initialized and delivered, kept for data-path writes.
pub struct PublishedSession {
    pub segment: Segment,
    pub offsets: SessionOffsets,
}

/// Initializes one Session's FIFOs and event queue in a fresh shared
/// segment and delivers its attach descriptors (metadata words + SCM_RIGHTS)
/// to the client on `stream`, in the production daemon format.
pub fn publish_session(
    stream: &UnixStream,
    handle: SessionHandle,
    config: AppSessionConfig,
) -> PublishedSession {
    let segment = Segment::shared_default();
    let offsets = SessionOffsets::allocate(&segment, config.fifo_capacity, config.evt_q_capacity)
        .expect("session layout");
    let ring_nitems = config.evt_q_capacity.max(1) as u32;
    let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
    unsafe {
        Fifo::init_at(segment.clone(), offsets.rx_fifo_off, config.fifo_capacity)
            .expect("init rx fifo");
        Fifo::init_at(segment.clone(), offsets.tx_fifo_off, config.fifo_capacity)
            .expect("init tx fifo");
    }
    let evt_q = unsafe {
        SessionMsgQueue::init_at_with_signal(
            segment.clone(),
            offsets.evt_q_off,
            q_nitems,
            ring_nitems,
        )
    }
    .expect("init evt_q");
    let evt_q_fd = evt_q.read_fd().expect("evt_q signal read fd");
    send_session_descriptors(stream, handle, &segment, &offsets, evt_q_fd)
        .expect("deliver session descriptors");
    PublishedSession { segment, offsets }
}

/// Sends the production attach descriptor frame: `ATTACH_METADATA_BYTES` of
/// metadata words followed by a single SCM_RIGHTS cmsghdr carrying the
/// session segment descriptor and the event queue signal read descriptor.
fn send_session_descriptors(
    stream: &UnixStream,
    handle: SessionHandle,
    segment: &Segment,
    offsets: &SessionOffsets,
    evt_q_fd: RawFd,
) -> io::Result<()> {
    let mut payload = [0_u8; ATTACH_METADATA_BYTES];
    debug_assert_eq!(ATTACH_METADATA_WORDS * size_of::<u64>(), payload.len());
    let words = [
        ATTACH_PROTOCOL_VERSION,
        handle.session_index as u64,
        handle.thread_index as u64,
        segment.size() as u64,
        offsets.rx_fifo_off,
        offsets.tx_fifo_off,
        offsets.evt_q_off,
    ];
    for (word, chunk) in words
        .into_iter()
        .zip(payload.chunks_exact_mut(size_of::<u64>()))
    {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    let segment_fd = segment
        .shared_fd()
        .ok_or_else(|| io::Error::other("published session segment is not shareable"))?;
    let descriptors = [segment_fd, evt_q_fd];
    // SAFETY: libc CMSG_SPACE only computes a control-buffer size.
    let control_bytes =
        unsafe { libc::CMSG_SPACE(std::mem::size_of_val(&descriptors) as u32) } as usize;
    let control_elements = control_bytes.div_ceil(size_of::<libc::cmsghdr>());
    let mut control = Vec::<libc::cmsghdr>::with_capacity(control_elements);
    control.resize_with(control_elements, || unsafe { std::mem::zeroed() });
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: payload.len(),
    };
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = std::ptr::from_mut(&mut iov);
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = control_bytes as _;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        if header.is_null() {
            return Err(io::Error::other("descriptor header is missing"));
        }
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(&descriptors) as u32) as _;
        std::ptr::copy_nonoverlapping(
            descriptors.as_ptr(),
            libc::CMSG_DATA(header).cast::<RawFd>(),
            descriptors.len(),
        );
    }
    let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &message, 0) };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    if sent as usize != payload.len() {
        return Err(io::Error::other("partial descriptor send"));
    }
    Ok(())
}
