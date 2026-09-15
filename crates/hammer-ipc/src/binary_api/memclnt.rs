//! Shared-memory Binary API messages and their server-owned protocol operations.
use std::alloc::Layout;
use std::fmt;
use std::mem::size_of;
use std::ptr::NonNull;
use std::time::Instant;

use hammer_infra::svm::queue::{
    SvmQueue, SvmQueueConditionalWait, SvmQueueConfig, SvmQueueError, SvmQueueOperation,
};
use hammer_infra::svm::region::SvmRegion;
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

use super::api::ApiMain;
use super::memory_shared::ShmemHeader;
use super::{Api, Array};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "memclnt_delete", returns = MemclntDeleteReply,
    handler = memclnt_delete_handler)]
pub struct MemclntDelete {
    pub id: u16,
    pub index: u32,
    pub handle: u64,
    pub do_cleanup: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "memclnt_delete_reply")]
pub struct MemclntDeleteReply {
    pub id: u16,
    pub response: i32,
    pub handle: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "memclnt_keepalive", autoreply = MemclntKeepaliveReply,
    reply_handler = memclnt_keepalive_reply_handler)]
pub struct MemclntKeepalive {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_create_v2", returns = MemclntCreateV2Reply,
    handler = memclnt_create_v2_handler)]
pub struct MemclntCreateV2 {
    pub id: u16,
    pub context: u32,
    pub ctx_quota: i32,
    pub input_queue: u64,
    #[api(string)]
    pub name: [u8; 64],
    pub api_versions: [u32; 8],
    pub keepalive: bool,
}

impl Serialize for MemclntCreateV2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_struct("memclnt_create_v2", 7)?;
        fields.serialize_field("id", &self.id)?;
        fields.serialize_field("context", &self.context)?;
        fields.serialize_field("ctx_quota", &self.ctx_quota)?;
        fields.serialize_field("input_queue", &self.input_queue)?;
        fields.serialize_field("name", self.name.as_slice())?;
        fields.serialize_field("api_versions", &self.api_versions)?;
        fields.serialize_field("keepalive", &self.keepalive)?;
        fields.end()
    }
}

impl<'de> Deserialize<'de> for MemclntCreateV2 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CreateV2;
        impl<'de> Visitor<'de> for CreateV2 {
            type Value = MemclntCreateV2;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("memory client create-v2 message")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
                Ok(MemclntCreateV2 {
                    id: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("id"))?,
                    context: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("context"))?,
                    ctx_quota: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("ctx_quota"))?,
                    input_queue: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("input_queue"))?,
                    name: fields
                        .next_element_seed(Array::<[u8; 64]>::new())?
                        .ok_or_else(|| A::Error::missing_field("name"))?,
                    api_versions: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("api_versions"))?,
                    keepalive: fields
                        .next_element()?
                        .ok_or_else(|| A::Error::missing_field("keepalive"))?,
                })
            }
        }
        deserializer.deserialize_struct(
            "memclnt_create_v2",
            &[
                "id",
                "context",
                "ctx_quota",
                "input_queue",
                "name",
                "api_versions",
                "keepalive",
            ],
            CreateV2,
        )
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "memclnt_create_v2_reply")]
pub struct MemclntCreateV2Reply {
    pub id: u16,
    pub context: u32,
    pub response: i32,
    pub handle: u64,
    pub index: u32,
    pub message_table: u64,
}

pub(super) struct ApiRegistration {
    pub pool_index: u32,
    pub name: [u8; 64],
    pub input_queue: NonNull<SvmQueue>,
    pub region: NonNull<SvmRegion>,
    pub shmem_header: NonNull<ShmemHeader>,
    pub last_heard: Instant,
    pub last_queue_head: u32,
    pub unanswered_keepalives: u32,
    pub keepalive_enabled: bool,
    pub is_being_removed: bool,
    pub cleanup_queue: bool,
}

fn memclnt_create_v2_handler(request: MemclntCreateV2) {
    let api = ApiMain::current();
    hammer_runtime::thread_main::ensure_main_thread()
        .expect("memory API server executes on runtime main thread");
    let region = unsafe { (*api.rp.get()).expect("API region mapped before create") };
    let region = unsafe { region.as_ref() };
    let queue_address = usize::try_from(request.input_queue)
        .expect("client queue address fits same-build pointer width");
    let Some(queue_base) = NonNull::new(queue_address as *mut u8) else {
        tracing::warn!("create-v2 rejected a null client queue");
        return;
    };
    let Some(region_end) = region.base().as_ptr().addr().checked_add(region.size()) else {
        tracing::error!("API region extent overflow");
        return;
    };
    if !queue_address.is_multiple_of(64)
        || !region.contains_range(queue_base, size_of::<SvmQueue>())
    {
        tracing::warn!(queue_address, "create-v2 rejected an out-of-region queue");
        return;
    }
    let queue = match unsafe { SvmQueue::attach(queue_base, region_end - queue_address) } {
        Ok(queue) if unsafe { queue.as_ref() }.element_size() == size_of::<usize>() => queue,
        Ok(_) => {
            tracing::warn!(
                queue_address,
                "create-v2 queue element size is not an address"
            );
            return;
        }
        Err(source) => {
            tracing::warn!(?source, queue_address, "create-v2 queue rejected");
            return;
        }
    };
    if let Err(source) = unsafe { api.serialize_shared_message_table() } {
        tracing::error!(?source, "create-v2 shared message table unavailable");
        return;
    }
    let table = unsafe { (*api.serialized_message_table.get()).unwrap() };
    let registration = {
        let lock = match region.lock() {
            Ok(lock) => lock,
            Err(source) => {
                tracing::error!(?source, "create-v2 region unavailable");
                return;
            }
        };
        let heap = lock.data_heap().expect("API region has a Data Heap");
        let active_heap = heap.activate();
        let allocation = heap
            .allocate(Layout::new::<ApiRegistration>())
            .expect("non-nullable shared registration allocation");
        drop(active_heap);
        allocation.cast::<ApiRegistration>()
    };
    let clients = unsafe { &mut *api.clients.get() };
    let slot = clients.insert(registration);
    assert!(
        slot < 0x00ff_ffff,
        "registration index fits 24-bit client index"
    );
    unsafe {
        registration.as_ptr().write(ApiRegistration {
            pool_index: slot,
            name: request.name,
            input_queue: queue,
            region: NonNull::from(region),
            shmem_header: NonNull::from(api.shmem_header()),
            last_heard: Instant::now(),
            last_queue_head: 0,
            unanswered_keepalives: 0,
            keepalive_enabled: request.keepalive,
            is_being_removed: false,
            cleanup_queue: false,
        })
    };
    let index = slot << 8 | unsafe { api.shmem_header() }.application_restarts() & 0xff;
    let reply = MemclntCreateV2Reply {
        id: 26,
        context: request.context,
        response: 0,
        handle: registration.as_ptr().addr() as u64,
        index,
        message_table: table.as_ptr().addr() as u64,
    };
    let mut message = unsafe { api.alloc(30) };
    let written = unsafe { message.encode(&reply) }
        .expect("owned protocol message encodes at its declared length");
    assert_eq!(written, 30, "message length agrees with API declaration");
    let address = usize::from(&message).to_ne_bytes();
    let sent = (unsafe { queue.as_ref() }).add(&address, SvmQueueConditionalWait::Nowait);
    if sent
        .as_ref()
        .err()
        .is_some_and(|source| !is_committed(source, SvmQueueOperation::Add))
    {
        unsafe { api.free(message) };
    }
    match sent {
        Ok(()) => (),
        Err(source) => {
            tracing::error!(?source, slot, "create-v2 reply not delivered");
            if !is_committed(&source, SvmQueueOperation::Add) {
                if let Err(cleanup_source) = api.remove_registration(slot, false) {
                    tracing::error!(
                        ?cleanup_source,
                        slot,
                        "create-v2 registration rollback failed"
                    );
                }
            }
        }
    }
}

fn memclnt_keepalive_reply_handler(reply: MemclntKeepaliveReply) {
    let api = ApiMain::current();
    let Some(mut registration) = api.registration(reply.context) else {
        return;
    };
    let value = unsafe { registration.as_mut() };
    value.unanswered_keepalives = 0;
    value.last_heard = Instant::now();
}

fn memclnt_delete_handler(request: MemclntDelete) {
    let api = ApiMain::current();
    let Some(mut registration) = api.registration(request.index) else {
        return;
    };
    let slot = unsafe { registration.as_ref().pool_index };
    if !request.do_cleanup {
        let queue = unsafe { registration.as_ref().input_queue.as_ref() };
        let reply = MemclntDeleteReply {
            id: 4,
            response: 1,
            handle: request.handle,
        };
        let mut message = unsafe { api.alloc(14) };
        let written = unsafe { message.encode(&reply) }
            .expect("owned protocol message encodes at its declared length");
        assert_eq!(written, 14, "message length agrees with API declaration");
        let address = usize::from(&message).to_ne_bytes();
        let sent = queue.add(&address, SvmQueueConditionalWait::Nowait);
        if sent
            .as_ref()
            .err()
            .is_some_and(|source| !is_committed(source, SvmQueueOperation::Add))
        {
            unsafe { api.free(message) };
        }
        if let Err(source) = sent {
            tracing::error!(?source, slot, "delete reply queue");
            if !is_committed(&source, SvmQueueOperation::Add) {
                return;
            }
        }
    }
    let value = unsafe { registration.as_mut() };
    value.is_being_removed = true;
    value.cleanup_queue = request.do_cleanup;
    if let Err(source) = api.remove_registration(slot, request.do_cleanup) {
        tracing::error!(?source, slot, "registration removal deferred");
    }
}

pub fn is_committed(source: &SvmQueueError, operation: SvmQueueOperation) -> bool {
    matches!(source,
        SvmQueueError::SignalAfterCommit { operation: reported, .. }
        | SvmQueueError::EventSignalAfterCommit { operation: reported, .. }
        if *reported == operation)
}

pub fn receive() -> Result<bool, SvmQueueError> {
    let api = ApiMain::current();
    hammer_runtime::thread_main::ensure_main_thread()
        .expect("memory API server executes on runtime main thread");
    let header = unsafe { api.shmem_header() };
    let queue = unsafe { header.input_queue() };
    let mut address = [0_u8; size_of::<usize>()];
    let status = queue.sub2(&mut address);
    if matches!(status, Err(SvmQueueError::Empty)) {
        return Ok(false);
    }
    if status.is_ok()
        || status
            .as_ref()
            .err()
            .is_some_and(|source| is_committed(source, SvmQueueOperation::Sub))
    {
        let region = unsafe { (*api.rp.get()).expect("memory queue region remains mapped") };
        let message = unsafe {
            super::memory_shared::MsgBuf::from_address(
                region.as_ref(),
                usize::from_ne_bytes(address),
            )
        };
        let payload = unsafe { message.as_bytes() }
            .expect("dequeued payload was initialized before publication");
        let dispatched = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if payload.len() < size_of::<u16>() {
                tracing::warn!("truncated memory API message id");
                return;
            }
            let id = u16::from_be_bytes([payload[0], payload[1]]);
            if let Some(data) = api.get_msg_data(id) {
                if let Some(dispatch) = data.dispatch {
                    if let Err(source) = dispatch(payload, !data.is_mp_safe) {
                        tracing::warn!(?source, id, "memory API message rejected");
                    }
                }
            }
        }));
        unsafe { api.free(message) };
        if let Err(payload) = dispatched {
            std::panic::resume_unwind(payload);
        }
    }
    status.map(|()| true)
}

// Fixed bootstrap IDs from memclnt.api; unsupported messages retain their gaps.
// The derived Api::HANDLER selects executable entries; replies without a server
// handler still appear in name/CRC discovery.
hammer_component_macros::api_message_table! {
    pub fn setup_message_id_table;
    MemclntDelete = 3 { is_mp_safe: false, traced: false, replay: false };
    MemclntDeleteReply = 4 { is_mp_safe: false, traced: false, replay: false };
    MemclntKeepalive = 21 { is_mp_safe: true, traced: false, replay: false };
    MemclntKeepaliveReply = 22 { is_mp_safe: true, traced: false, replay: false };
    MemclntCreateV2 = 25 { is_mp_safe: false, traced: false, replay: false };
    MemclntCreateV2Reply = 26 { is_mp_safe: false, traced: false, replay: false };
}

impl ApiMain {
    pub(super) fn registration(&self, client_index: u32) -> Option<NonNull<ApiRegistration>> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("memory API server executes on runtime main thread");
        let slot = client_index >> 8;
        let clients = unsafe { &*self.clients.get() };
        let registration = *clients.get(slot)?;
        let value = unsafe { registration.as_ref() };
        let header = unsafe { value.shmem_header.as_ref() };
        (!value.is_being_removed
            && value.pool_index == slot
            && client_index & 0xff == header.application_restarts() & 0xff)
            .then_some(registration)
    }

    pub fn registration_queue(
        &self,
        client_index: u32,
    ) -> Option<&'static hammer_infra::svm::queue::SvmQueue> {
        let registration = self.registration(client_index)?;
        let queue = unsafe { registration.as_ref().input_queue.as_ref() };
        Some(unsafe { &*(queue as *const _) })
    }
    fn remove_registration(
        &self,
        slot: u32,
        do_cleanup: bool,
    ) -> Result<(), hammer_infra::svm::region::SvmRegionError> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("memory API server executes on runtime main thread");
        let clients = unsafe { &mut *self.clients.get() };
        let Some(registration) = clients.get(slot).copied() else {
            return Ok(());
        };
        let value = unsafe { registration.as_ref() };
        if do_cleanup {
            let queue = unsafe { value.input_queue.as_ref() };
            loop {
                let mut address = [0_u8; size_of::<usize>()];
                let status = queue.sub2(&mut address);
                if matches!(status, Err(SvmQueueError::Empty)) {
                    break;
                }
                if status.is_ok()
                    || status
                        .as_ref()
                        .err()
                        .is_some_and(|source| is_committed(source, SvmQueueOperation::Sub))
                {
                    let region = unsafe { value.region.as_ref() };
                    unsafe {
                        self.free(super::memory_shared::MsgBuf::from_address(
                            region,
                            usize::from_ne_bytes(address),
                        ))
                    };
                }
                if let Err(source) = status {
                    tracing::error!(?source, slot, "queue drain deferred registration removal");
                    return Ok(());
                }
            }
        }
        let region = unsafe { value.region.as_ref() };
        let lock = region.lock()?;
        let heap = lock.data_heap().expect("API region has a Data Heap");
        let active_heap = heap.activate();
        if do_cleanup {
            let queue = unsafe { value.input_queue.as_ref() };
            unsafe { queue.cleanup() };
            let config = SvmQueueConfig {
                nels: u32::try_from(queue.capacity()).expect("queue capacity fits u32"),
                elsize: u32::try_from(queue.element_size()).expect("queue element size fits u32"),
                consumer_pid: queue.consumer_pid(),
            };
            let bytes =
                SvmQueue::size_to_alloc(&config).expect("attached queue geometry remains valid");
            let layout = Layout::from_size_align(bytes, 64).expect("validated queue layout");
            unsafe { heap.deallocate(value.input_queue.cast::<u8>(), layout) };
        }
        unsafe { heap.deallocate(registration.cast::<u8>(), Layout::new::<ApiRegistration>()) };
        drop(active_heap);
        drop(lock);
        clients
            .remove(slot)
            .expect("registration slot remains present");
        Ok(())
    }
    /// Maintains this main's registrations at the Process liveness deadline.
    /// Corresponds to memory_api.c::vl_mem_api_dead_client_scan.
    pub fn dead_client_scan(&self, now: Instant) {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("memory API server executes on runtime main thread");
        let slots: Vec<u32> = unsafe { &*self.clients.get() }
            .iter()
            .map(|(slot, _)| slot)
            .collect();
        for slot in slots {
            let registration = unsafe { &mut *self.clients.get() }.get(slot).copied();
            let Some(mut registration) = registration else {
                continue;
            };
            let value = unsafe { registration.as_mut() };
            if value.is_being_removed {
                if let Err(source) = self.remove_registration(slot, value.cleanup_queue) {
                    tracing::error!(?source, slot, "deferred registration cleanup");
                }
                continue;
            }
            if !value.keepalive_enabled {
                continue;
            }
            let queue = unsafe { value.input_queue.as_ref() };
            let head = match queue.lock() {
                Ok(guard) => guard.consumer_head(),
                Err(source) => {
                    tracing::error!(?source, slot, "keepalive queue head unavailable");
                    continue;
                }
            };
            if head != value.last_queue_head {
                value.last_queue_head = head;
                value.last_heard = now;
                value.unanswered_keepalives = 0;
                continue;
            }
            if now.duration_since(value.last_heard) < std::time::Duration::from_secs(10) {
                continue;
            }
            if value.unanswered_keepalives >= 2 {
                let pid = queue.consumer_pid();
                if unsafe { libc::kill(pid, 0) } != 0
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    value.is_being_removed = true;
                    if let Err(source) = self.remove_registration(slot, false) {
                        tracing::error!(?source, slot, "dead registration cleanup deferred");
                    }
                } else {
                    value.unanswered_keepalives = 0;
                    value.last_heard = now;
                }
                continue;
            }
            value.unanswered_keepalives += 1;
            value.last_heard = now;
            let index = slot << 8 | unsafe { self.shmem_header() }.application_restarts() & 0xff;
            let request = MemclntKeepalive {
                id: 21,
                client_index: index,
                context: index,
            };
            // VPP's keepalive probe is the exception to ordinary server replies:
            // send_memclnt_keepalive uses alloc_as_if_client_w_reg.
            assert_eq!(
                Some(value.region),
                unsafe { *self.rp.get() },
                "ordinary registration belongs to this API region"
            );
            let mut message = unsafe { self.alloc_as_client(10) };
            let written = unsafe { message.encode(&request) }
                .expect("owned protocol message encodes at its declared length");
            assert_eq!(written, 10, "message length agrees with API declaration");
            let address = usize::from(&message).to_ne_bytes();
            let sent = queue.add(&address, SvmQueueConditionalWait::Nowait);
            if sent
                .as_ref()
                .err()
                .is_some_and(|source| !is_committed(source, SvmQueueOperation::Add))
            {
                unsafe { self.free(message) };
            }
            if let Err(source) = sent {
                if !matches!(source, SvmQueueError::QueueFull | SvmQueueError::LockBusy) {
                    tracing::error!(?source, slot, "keepalive probe queue unavailable");
                }
            }
        }
    }
}
