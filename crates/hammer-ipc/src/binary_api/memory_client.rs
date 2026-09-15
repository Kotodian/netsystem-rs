//! Caller-owned VAPI SHM client. ApiMain supplies only mapping and allocation.
use std::alloc::Layout;
use std::collections::{HashMap, VecDeque};
use std::mem::{align_of, size_of};
use std::num::{NonZeroU32, NonZeroUsize};
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use hammer_infra::svm::queue::{
    SvmQueue, SvmQueueConditionalWait, SvmQueueConfig, SvmQueueError, SvmQueueOperation,
};
use hammer_infra::svm::region::SvmRegionError;

use super::api::ApiMain;
use super::control::{ControlPing, ControlPingReply};
use super::memclnt::{
    MemclntCreateV2, MemclntCreateV2Reply, MemclntDelete, MemclntDeleteReply, MemclntKeepalive,
    MemclntKeepaliveReply,
};
use super::memory_shared::MsgBuf;
use super::{Api, codec, table};

const RECEIVE_INTERVAL: Duration = Duration::from_micros(400);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("memory client already connected")]
    AlreadyConnected,
    #[error("memory API region is not mapped")]
    NotMapped,
    #[error("memory client is not connected")]
    NotConnected,
    #[error("create-v2 reply did not arrive for context {context}")]
    ConnectTimeout { context: u32 },
    #[error("delete reply did not arrive for client index {client_index}")]
    DisconnectTimeout { client_index: u32 },
    #[error("create-v2 rejected with response {response}")]
    CreateRejected { response: i32 },
    #[error("required API message `{name_crc}` is unavailable")]
    IncompatibleMessage { name_crc: &'static str },
    #[error("API response did not arrive for context {context}")]
    ResponseTimeout { context: u32 },
    #[error("API request lost its response for context {context}")]
    NoResponse { context: u32 },
    #[error("client request capacity {capacity} is exhausted")]
    RequestsFull { capacity: usize },
    #[error("API message {id:?} is unavailable")]
    MessageUnavailable { id: MessageId },
    #[error("unknown server message ID {id}")]
    UnknownMessageId { id: u16 },
    #[error("context {context} expected {expected:?}, received {received:?}")]
    UnexpectedResponse {
        context: u32,
        expected: MessageId,
        received: MessageId,
    },
    #[error("memory client queue operation: {source}")]
    Queue {
        #[source]
        source: SvmQueueError,
    },
    #[error("memory client codec: {source}")]
    Codec {
        #[source]
        source: codec::Error,
    },
    #[error("memory client region: {source}")]
    Region {
        #[source]
        source: SvmRegionError,
    },
}

/// One caller-owned SHM connection, corresponding to vapi_ctx_s. VecDeque owns
/// the request ring's start/count/capacity. This SHM-only async client needs no
/// socket selector, blocking mode, RX thread, or requests_mutex.
/// Lifecycle messages are local to their handshake futures, never client fields.
pub struct Client<C> {
    api: &'static ApiMain,
    connected: bool,
    input_queue: Option<NonNull<SvmQueue>>,
    client_index: Option<u32>,
    create_pending: bool,
    delete_pending: bool,
    message_table: HashMap<String, u16>,
    server_message_ids: Vec<Option<u16>>,
    local_message_ids: Vec<Option<MessageId>>,
    handle_keepalives: bool,
    context_counter: u32,
    max_outstanding_requests: usize,
    requests: VecDeque<Request<C>>,
    generic_callback: Option<fn(&mut C, Message)>,
    event_callbacks: Vec<Option<fn(&mut C, Message)>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RequestMode {
    Regular,
    Dump,
    Stream,
}

struct Request<C> {
    context: u32,
    response_id: MessageId,
    mode: RequestMode,
    callback: fn(&mut C, u32, bool, Result<Option<Message>, Error>),
}

hammer_component_macros::api_client_messages! {
    [MemclntCreateV2, MemclntCreateV2Reply, MemclntDelete, MemclntDeleteReply,
     MemclntKeepalive, MemclntKeepaliveReply, ControlPing, ControlPingReply]
    fn control_ping() -> ControlPing {
        ControlPing { id: 0, client_index: 0, context: 0 }
    }
}

impl<C> Default for Client<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> Client<C> {
    /// Captures the selected mapping owner once. Later TLS selection changes do
    /// not redirect this connection's allocations, receive queue or releases.
    pub fn new() -> Self {
        Self {
            api: ApiMain::current(),
            connected: false,
            input_queue: None,
            client_index: None,
            create_pending: false,
            delete_pending: false,
            message_table: HashMap::new(),
            server_message_ids: vec![None; MessageId::ALL.len()],
            local_message_ids: Vec::new(),
            handle_keepalives: false,
            context_counter: 0,
            max_outstanding_requests: 0,
            requests: VecDeque::new(),
            generic_callback: None,
            event_callbacks: vec![None; MessageId::ALL.len()],
        }
    }
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.connected
    }
    #[inline]
    pub fn client_index(&self) -> Option<u32> {
        if self.connected {
            self.client_index
        } else {
            None
        }
    }
    #[inline]
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }
    #[inline]
    pub fn message_id(&self, id: MessageId) -> Option<u16> {
        self.server_message_ids[id as usize]
    }
    #[inline]
    pub fn set_event_callback(&mut self, id: MessageId, callback: Option<fn(&mut C, Message)>) {
        self.event_callbacks[id as usize] = callback;
    }
    #[inline]
    pub fn set_generic_callback(&mut self, callback: Option<fn(&mut C, Message)>) {
        self.generic_callback = callback;
    }
}

fn committed(source: &SvmQueueError, operation: SvmQueueOperation) -> bool {
    matches!(source,
        SvmQueueError::SignalAfterCommit { operation: reported, .. }
        | SvmQueueError::EventSignalAfterCommit { operation: reported, .. }
        if *reported == operation)
}

impl<C> Client<C> {
    fn allocate<T: Api>(&self, request: &T) -> Result<MsgBuf, Error> {
        if !self.api.is_mapped() {
            return Err(Error::NotMapped);
        }
        let payload_len =
            codec::serialized_len(request).map_err(|source| Error::Codec { source })?;
        let mut allocation = unsafe { self.api.alloc_as_client(payload_len) };
        match unsafe { allocation.encode(request) } {
            Ok(written) => assert_eq!(written, payload_len, "API serializer length must be stable"),
            Err(source) => {
                unsafe { self.api.free(allocation) };
                return Err(Error::Codec { source });
            }
        }
        Ok(allocation)
    }

    fn try_send<T: Api>(&self, request: &T) -> Result<(), Error> {
        if self.input_queue.is_none() {
            return Err(Error::NotConnected);
        }
        let allocation = self.allocate(request)?;
        let address = usize::from(&allocation).to_ne_bytes();
        let result = unsafe { self.api.shmem_header().input_queue() }
            .add(&address, SvmQueueConditionalWait::Nowait);
        if result
            .as_ref()
            .err()
            .is_some_and(|source| !committed(source, SvmQueueOperation::Add))
        {
            unsafe { self.api.free(allocation) };
        }
        result.map_err(|source| Error::Queue { source })
    }

    fn submit(
        &mut self,
        mut message: Message,
        callback: fn(&mut C, u32, bool, Result<Option<Message>, Error>),
        details_callback: Option<fn(&mut C, u32, bool, Result<Option<Message>, Error>)>,
    ) -> Result<u32, Error> {
        if !self.connected {
            return Err(Error::NotConnected);
        }
        let service = message
            .service()
            .expect("generated request has an API service");
        let response_id = MessageId::ALL
            .iter()
            .copied()
            .find(|id| Some(id.name()) == service.reply)
            .expect("client inventory includes service reply");
        let details_id = service.stream_message.map(|name| {
            MessageId::ALL
                .iter()
                .copied()
                .find(|id| id.name() == name)
                .expect("client inventory includes service details")
        });
        assert_eq!(
            details_id.is_some(),
            details_callback.is_some(),
            "stream request declares its details callback"
        );
        let slots = if details_id.is_some() { 2 } else { 1 };
        if self.max_outstanding_requests - self.requests.len() < slots {
            return Err(Error::RequestsFull {
                capacity: self.max_outstanding_requests,
            });
        }
        let dump = service.stream && details_id.is_none();
        let required = [
            Some(message.id()),
            Some(response_id),
            details_id,
            dump.then_some(MessageId::ControlPing),
            dump.then_some(MessageId::ControlPingReply),
        ];
        for id in required.into_iter().flatten() {
            self.message_id(id)
                .ok_or(Error::MessageUnavailable { id })?;
        }
        let context = loop {
            self.context_counter = (self.context_counter.wrapping_add(1) & 0x7fff_ffff).max(1);
            let context = self.context_counter | 0x8000_0000;
            if !self
                .requests
                .iter()
                .any(|request| request.context == context)
            {
                break context;
            }
        };
        let client_index = self.client_index.expect("connected client has index");
        message.set_request_header(
            self.message_id(message.id()).unwrap(),
            client_index,
            context,
        );
        let allocation = message.allocate(self)?;
        let ping = if dump {
            match self.allocate(&ControlPing {
                id: self.message_id(MessageId::ControlPing).unwrap(),
                client_index,
                context,
            }) {
                Ok(ping) => Some(ping),
                Err(error) => {
                    unsafe { self.api.free(allocation) };
                    return Err(error);
                }
            }
        } else {
            None
        };
        let address = usize::from(&allocation).to_ne_bytes();
        let queue = unsafe { self.api.shmem_header().input_queue() };
        let (sent, operation) = if let Some(ping) = &ping {
            (
                queue.add2(
                    &address,
                    &usize::from(ping).to_ne_bytes(),
                    SvmQueueConditionalWait::Nowait,
                ),
                SvmQueueOperation::AddPair,
            )
        } else {
            (
                queue.add(&address, SvmQueueConditionalWait::Nowait),
                SvmQueueOperation::Add,
            )
        };
        if let Err(source) = sent {
            if !committed(&source, operation) {
                unsafe { self.api.free(allocation) };
                if let Some(ping) = ping {
                    unsafe { self.api.free(ping) };
                }
                return Err(Error::Queue { source });
            }
            // The peer owns the messages. Reporting a retryable send error here
            // would duplicate the operation; retain the request and report the
            // notification fault separately, as for receive-side notifications.
            tracing::warn!(
                ?source,
                context,
                "request committed despite notification error"
            );
        }
        if let Some(response_id) = details_id {
            self.requests.push_back(Request {
                context,
                response_id,
                mode: RequestMode::Stream,
                callback: details_callback.expect("stream callback declared"),
            });
        }
        self.requests.push_back(Request {
            context,
            response_id,
            mode: if dump {
                RequestMode::Dump
            } else {
                RequestMode::Regular
            },
            callback,
        });
        Ok(context)
    }

    /// Handshakes on an already mapped ApiMain. Repeating after a cancelled or
    /// timed-out create resumes the published handshake with its original
    /// parameters; it never submits a second create. Keep the mapping alive.
    pub async fn connect(
        &mut self,
        name: &str,
        response_queue_size: NonZeroU32,
        max_outstanding_requests: NonZeroUsize,
        handle_keepalives: bool,
    ) -> Result<(), Error> {
        let api = self.api;
        if !api.is_mapped() {
            return Err(Error::NotMapped);
        }
        if self.connected
            || !self.requests.is_empty()
            || self.client_index.is_some()
            || (self.input_queue.is_some() && !self.create_pending)
        {
            return Err(Error::AlreadyConnected);
        }
        if self.input_queue.is_none() {
            let region = unsafe { (*api.rp.get()).expect("API mapping remains installed") };
            let lock = unsafe { region.as_ref() }
                .lock()
                .map_err(|source| Error::Region { source })?;
            let heap = lock.data_heap().expect("API region has a Data Heap");
            let config = SvmQueueConfig {
                nels: response_queue_size.get(),
                elsize: u32::try_from(size_of::<usize>()).expect("pointer-sized queue element"),
                consumer_pid: std::process::id() as i32,
            };
            let bytes =
                SvmQueue::size_to_alloc(&config).map_err(|source| Error::Queue { source })?;
            let layout = Layout::from_size_align(bytes, 64).expect("validated client queue layout");
            let active_heap = heap.activate();
            let queue_address = heap
                .allocate_zeroed(layout)
                .expect("non-nullable shared client queue allocation");
            let queue = match unsafe { SvmQueue::init(queue_address, &config) } {
                Ok(queue) => queue,
                Err(source) => {
                    unsafe { heap.deallocate(queue_address, layout) };
                    return Err(Error::Queue { source });
                }
            };
            drop(active_heap);
            drop(lock);
            self.input_queue = Some(queue);
            self.requests.reserve(max_outstanding_requests.get());
            self.max_outstanding_requests = max_outstanding_requests.get();
            self.handle_keepalives = handle_keepalives;
        }
        if !self.create_pending {
            let mut client_name = [0_u8; 64];
            let name_bytes = name.as_bytes();
            let length = name_bytes.len().min(63);
            client_name[..length].copy_from_slice(&name_bytes[..length]);
            let request = MemclntCreateV2 {
                id: 25,
                context: 0,
                ctx_quota: 0,
                input_queue: self
                    .input_queue
                    .expect("client queue exists")
                    .as_ptr()
                    .addr() as u64,
                name: client_name,
                api_versions: [0; 8],
                keepalive: true,
            };
            // Publish once, before the first await. A full queue returns to the
            // caller with no create committed; a cancelled wait never resends it.
            match self.try_send(&request) {
                Ok(()) => (),
                Err(Error::Queue { source }) if committed(&source, SvmQueueOperation::Add) => {
                    tracing::warn!(?source, "create-v2 committed despite notification error");
                }
                Err(error) => {
                    if let Err(cleanup_error) = self.release_input_queue() {
                        tracing::error!(
                            ?cleanup_error,
                            "client queue cleanup before create publication"
                        );
                    }
                    return Err(error);
                }
            }
            self.create_pending = true;
        }
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        let reply: MemclntCreateV2Reply = loop {
            let Some(message) = self.recv(deadline).await? else {
                return Err(Error::ConnectTimeout { context: 0 });
            };
            let payload = unsafe { message.as_bytes() }.expect("received message initialized");
            if payload.get(..2) != Some(26_u16.to_be_bytes().as_slice()) {
                unsafe { api.free(message) };
                continue;
            }
            let decoded = unsafe { message.decode::<MemclntCreateV2Reply>() };
            unsafe { api.free(message) };
            let reply = decoded.map_err(|source| Error::Codec { source })?;
            if reply.context == 0 {
                break reply;
            }
        };
        self.create_pending = false;
        if reply.response < 0 {
            if let Err(cleanup_error) = self.release_input_queue() {
                tracing::error!(?cleanup_error, "client queue cleanup after rejected create");
            }
            return Err(Error::CreateRejected {
                response: reply.response,
            });
        }
        self.client_index = Some(reply.index);
        // vapi_memclnt_create_v2_reply_t_handler imports into this client only.
        let region = unsafe { (*api.rp.get()).expect("client API mapping remains installed") };
        let region = unsafe { region.as_ref() };
        let table_address =
            usize::try_from(reply.message_table).expect("shared table address fits pointer width");
        let table = NonNull::new(table_address as *mut Vec<u8>)
            .expect("shared message table address is nonnull");
        assert!(
            table_address.is_multiple_of(align_of::<Vec<u8>>())
                && region.contains_range(table.cast(), size_of::<Vec<u8>>()),
            "shared message table object is inside API region"
        );
        let bytes = unsafe { table.as_ref() };
        let data = NonNull::new(bytes.as_ptr().cast_mut())
            .expect("published shared table has nonnull bytes");
        assert!(
            bytes.capacity() >= bytes.len() && region.contains_range(data, bytes.len()),
            "shared message table bytes are inside API region"
        );
        let entries = match table::deserialize_message_table(bytes) {
            Ok(entries) => entries,
            Err(source) => {
                if let Err(cleanup_error) = self.send_disconnect(true) {
                    tracing::error!(?cleanup_error, "disconnect after rejected message table");
                }
                return Err(Error::Codec { source });
            }
        };
        let names: HashMap<_, _> = entries.into_iter().collect();
        for required in [ControlPing::NAME_CRC, ControlPingReply::NAME_CRC] {
            if !names.contains_key(required) {
                if let Err(cleanup_error) = self.send_disconnect(true) {
                    tracing::error!(
                        ?cleanup_error,
                        "disconnect after incompatible message table"
                    );
                }
                return Err(Error::IncompatibleMessage { name_crc: required });
            }
        }
        for id in MessageId::ALL.iter().copied() {
            if let Some(server_id) = names.get(id.name_crc()).copied() {
                self.server_message_ids[id as usize] = Some(server_id);
                self.local_message_ids.resize(
                    self.local_message_ids.len().max(usize::from(server_id) + 1),
                    None,
                );
                self.local_message_ids[usize::from(server_id)] = Some(id);
            }
        }
        if self.handle_keepalives {
            for id in [
                MessageId::MemclntKeepalive,
                MessageId::MemclntKeepaliveReply,
            ] {
                if self.message_id(id).is_none() {
                    if let Err(cleanup_error) = self.send_disconnect(true) {
                        tracing::error!(
                            ?cleanup_error,
                            "disconnect after unavailable keepalive API"
                        );
                    }
                    return Err(Error::MessageUnavailable { id });
                }
            }
        }
        self.message_table = names;
        self.connected = true;
        Ok(())
    }
    // VAPI recv consumes keepalive internally. Reply retry state lives in
    // this future, and no synchronous queue guard crosses await.
    async fn recv(&mut self, deadline: Instant) -> Result<Option<MsgBuf>, Error> {
        let api = self.api;
        if !api.is_mapped() {
            return Err(Error::NotMapped);
        }
        let queue = self.input_queue.ok_or(Error::NotConnected)?;
        loop {
            let mut address = [0_u8; size_of::<usize>()];
            let status =
                unsafe { queue.as_ref() }.sub(&mut address, SvmQueueConditionalWait::Nowait);
            match status {
                Ok(()) => (),
                Err(SvmQueueError::Empty | SvmQueueError::LockBusy) => {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                    tokio::time::sleep(
                        RECEIVE_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
                    )
                    .await;
                    continue;
                }
                Err(source) if committed(&source, SvmQueueOperation::Sub) => {
                    // Polling does not depend on a successful space notification.
                    // This message was dequeued and must still be consumed.
                    tracing::warn!(
                        ?source,
                        "client received message despite dequeue notification error"
                    );
                }
                Err(source) => return Err(Error::Queue { source }),
            }
            let region = unsafe { (*api.rp.get()).expect("client region remains mapped") };
            let message =
                unsafe { MsgBuf::from_address(region.as_ref(), usize::from_ne_bytes(address)) };
            let payload = unsafe { message.as_bytes() }.expect("received message initialized");
            let id = payload
                .get(..2)
                .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]));
            if id == self.message_id(MessageId::MemclntKeepalive)
                && id.is_some()
                && self.handle_keepalives
                && self.client_index.is_some()
            {
                let decoded = unsafe { message.decode::<MemclntKeepalive>() };
                unsafe { api.free(message) };
                decoded.map_err(|source| Error::Codec { source })?;
                let reply = MemclntKeepaliveReply {
                    id: self
                        .message_id(MessageId::MemclntKeepaliveReply)
                        .expect("keepalive reply available"),
                    context: self.client_index.expect("connected client index"),
                    retval: 0,
                };
                loop {
                    match self.try_send(&reply) {
                        Ok(()) => break,
                        Err(Error::Queue { source })
                            if committed(&source, SvmQueueOperation::Add) =>
                        {
                            tracing::warn!(
                                ?source,
                                "keepalive reply committed despite notification error"
                            );
                            break;
                        }
                        Err(Error::Queue {
                            source: SvmQueueError::QueueFull | SvmQueueError::LockBusy,
                        }) => {
                            if Instant::now() >= deadline {
                                return Ok(None);
                            }
                            tokio::time::sleep(
                                RECEIVE_INTERVAL
                                    .min(deadline.saturating_duration_since(Instant::now())),
                            )
                            .await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                tokio::task::yield_now().await;
                continue;
            }
            return Ok(Some(message));
        }
    }

    pub async fn dispatch_one(
        &mut self,
        callbacks: &mut C,
        timeout: Duration,
    ) -> Result<bool, Error> {
        if !self.connected {
            return Err(Error::NotConnected);
        }
        let Some(allocation) = self.recv(Instant::now() + timeout).await? else {
            return Ok(false);
        };
        let decoded = match unsafe { allocation.as_bytes() } {
            Ok(payload) => {
                if let Some(bytes) = payload.get(..2) {
                    let id = u16::from_be_bytes([bytes[0], bytes[1]]);
                    match self
                        .local_message_ids
                        .get(usize::from(id))
                        .copied()
                        .flatten()
                    {
                        Some(id) => id.decode(payload).map_err(|source| Error::Codec { source }),
                        None => Err(Error::UnknownMessageId { id }),
                    }
                } else {
                    Err(Error::Codec {
                        source: <codec::Error as serde::de::Error>::custom(
                            "truncated API message id",
                        ),
                    })
                }
            }
            Err(source) => Err(Error::Codec { source }),
        };
        unsafe { self.api.free(allocation) };
        let message = decoded?;
        if message
            .context()
            .is_some_and(|context| context & 0x8000_0000 != 0)
        {
            self.dispatch_response(callbacks, message)?;
        } else if let Some(callback) =
            self.event_callbacks[message.id() as usize].or(self.generic_callback)
        {
            callback(callbacks, message);
        }
        Ok(true)
    }

    pub async fn dispatch(&mut self, callbacks: &mut C, timeout: Duration) -> Result<(), Error> {
        let deadline = Instant::now() + timeout;
        while let Some(request) = self.requests.front() {
            if Instant::now() >= deadline {
                return Err(Error::ResponseTimeout {
                    context: request.context,
                });
            }
            self.dispatch_one(
                callbacks,
                deadline.saturating_duration_since(Instant::now()),
            )
            .await?;
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    fn dispatch_response(&mut self, callbacks: &mut C, message: Message) -> Result<(), Error> {
        let context = message.context().expect("response has context");
        let Some(position) = self
            .requests
            .iter()
            .position(|request| request.context == context)
        else {
            // Late or foreign request context. It is not an event and does not
            // prove loss of any request currently in the ordered ring.
            return Ok(());
        };
        let id = message.id();
        let request = &self.requests[position];
        let terminal = request.mode == RequestMode::Stream && id != request.response_id;
        let expected = if terminal {
            let reply = self
                .requests
                .get(position + 1)
                .expect("stream retains terminal slot");
            assert_eq!(reply.context, context, "stream slots share context");
            reply.response_id
        } else {
            request.response_id
        };
        let dump_end = request.mode == RequestMode::Dump && id == MessageId::ControlPingReply;
        if id != expected && !dump_end {
            return Err(Error::UnexpectedResponse {
                context,
                expected,
                received: id,
            });
        }
        for _ in 0..position {
            let request = self.requests.pop_front().unwrap();
            (request.callback)(
                callbacks,
                request.context,
                true,
                Err(Error::NoResponse {
                    context: request.context,
                }),
            );
        }
        if terminal {
            self.requests.pop_front().expect("stream details slot");
        }
        let request = self.requests.front().expect("matched response slot");
        let is_last = request.mode == RequestMode::Regular || dump_end;
        let callback = request.callback;
        if is_last {
            self.requests.pop_front().expect("completed response slot");
        }
        callback(
            callbacks,
            context,
            is_last,
            Ok(if dump_end { None } else { Some(message) }),
        );
        Ok(())
    }

    /// Publishes delete once, or transfers queue cleanup to the server when
    /// do_cleanup is true. Call disconnect afterwards to retire pending callbacks.
    /// A repeated call never changes a delete already committed to the queue.
    pub fn send_disconnect(&mut self, do_cleanup: bool) -> Result<(), Error> {
        let index = self.client_index.ok_or(Error::NotConnected)?;
        if self.delete_pending {
            return Ok(());
        }
        let request = MemclntDelete {
            id: 3,
            index,
            handle: 0,
            do_cleanup,
        };
        let sent = self.try_send(&request);
        if sent.is_ok()
            || matches!(&sent, Err(Error::Queue { source }) if committed(source, SvmQueueOperation::Add))
        {
            self.connected = false;
            self.delete_pending = true;
            if do_cleanup {
                self.input_queue = None;
                self.client_index = None;
                self.message_table.clear();
                self.server_message_ids.fill(None);
                self.local_message_ids.clear();
                self.delete_pending = false;
            }
        }
        match sent {
            Err(Error::Queue { source }) if committed(&source, SvmQueueOperation::Add) => {
                tracing::warn!(?source, "delete committed despite notification error");
                Ok(())
            }
            result => result,
        }
    }

    pub async fn disconnect(&mut self, callbacks: &mut C) -> Result<(), Error> {
        let api = self.api;
        if self.create_pending {
            // A cancelled create has no index yet. Resume its reply before
            // deleting; connect reuses the queue and original create parameters.
            self.connect(
                "",
                NonZeroU32::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                self.handle_keepalives,
            )
            .await?;
        }
        if let Some(client_index) = self.client_index {
            let deadline = Instant::now() + DISCONNECT_TIMEOUT;
            loop {
                match self.send_disconnect(false) {
                    Ok(()) => break,
                    Err(Error::Queue { source }) if committed(&source, SvmQueueOperation::Add) => {
                        tracing::warn!(?source, "delete committed despite notification error");
                        break;
                    }
                    Err(Error::Queue {
                        source: SvmQueueError::QueueFull | SvmQueueError::LockBusy,
                    }) => {
                        if Instant::now() >= deadline {
                            return Err(Error::DisconnectTimeout { client_index });
                        }
                        tokio::time::sleep(RECEIVE_INTERVAL).await;
                    }
                    Err(error) => return Err(error),
                }
            }
            loop {
                let Some(message) = self.recv(deadline).await? else {
                    return Err(Error::DisconnectTimeout { client_index });
                };
                let payload = unsafe { message.as_bytes() }.expect("received message initialized");
                if payload.get(..2) != Some(4_u16.to_be_bytes().as_slice()) {
                    unsafe { api.free(message) };
                    continue;
                }
                let reply = unsafe { message.decode::<MemclntDeleteReply>() };
                unsafe { api.free(message) };
                reply.map_err(|source| Error::Codec { source })?;
                break;
            }
            self.client_index = None;
            self.delete_pending = false;
        }
        // Once delete is acknowledged, retrying cleanup cannot resend it.
        self.release_input_queue()?;
        self.connected = false;
        self.message_table.clear();
        self.server_message_ids.fill(None);
        self.local_message_ids.clear();
        while let Some(request) = self.requests.pop_front() {
            (request.callback)(
                callbacks,
                request.context,
                true,
                Err(Error::NoResponse {
                    context: request.context,
                }),
            );
        }
        Ok(())
    }
    fn release_input_queue(&mut self) -> Result<(), Error> {
        let Some(queue) = self.input_queue else {
            return Ok(());
        };
        let api = self.api;
        let region = unsafe { (*api.rp.get()).expect("mapping remains installed during cleanup") };
        let region = unsafe { region.as_ref() };
        loop {
            let mut address = [0_u8; size_of::<usize>()];
            let status = unsafe { queue.as_ref() }.sub2(&mut address);
            if matches!(status, Err(SvmQueueError::Empty)) {
                break;
            }
            if status.is_ok()
                || status
                    .as_ref()
                    .err()
                    .is_some_and(|source| committed(source, SvmQueueOperation::Sub))
            {
                unsafe { api.free(MsgBuf::from_address(region, usize::from_ne_bytes(address))) };
            }
            status.map_err(|source| Error::Queue { source })?;
        }
        let lock = region.lock().map_err(|source| Error::Region { source })?;
        let heap = lock.data_heap().expect("API region has a Data Heap");
        let client_queue = unsafe { queue.as_ref() };
        let config = SvmQueueConfig {
            nels: u32::try_from(client_queue.capacity()).expect("queue capacity fits u32"),
            elsize: u32::try_from(client_queue.element_size())
                .expect("queue element size fits u32"),
            consumer_pid: client_queue.consumer_pid(),
        };
        let bytes =
            SvmQueue::size_to_alloc(&config).expect("attached queue geometry remains valid");
        let layout = Layout::from_size_align(bytes, 64).expect("validated queue layout");
        let active_heap = heap.activate();
        unsafe {
            client_queue.cleanup();
            heap.deallocate(queue.cast(), layout)
        };
        drop(active_heap);
        drop(lock);
        self.input_queue = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
