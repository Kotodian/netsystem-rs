//! Message identity and installation. VPP's range failure, replacement and
//! duplicate-name behaviors are intentionally distinct operations.
use super::{Api, codec};
use std::cell::{Cell, Ref, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU32};

use hammer_infra::pool::Pool;
use hammer_infra::svm::region::{SvmRegion, SvmRegionConfig, SvmRegionFlags};

use super::memclnt::ApiRegistration;
use super::memory_shared::ShmemHeader;

#[allow(non_upper_case_globals)]
static api_global_main: OnceLock<ApiMain> = OnceLock::new();

thread_local! {
    // A selector, not a per-thread server registry. The daemon leaves every
    // selection on the default; client workers may select their own Main.
    #[allow(non_upper_case_globals)]
    static my_api_main: Cell<Option<&'static ApiMain>> = const { Cell::new(None) };
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("API message range `{name}` is already installed")]
    MessageRangeExists { name: std::string::String },
    #[error("invalid API message range count {count}")]
    MessageCountInvalid { count: u16 },
    #[error("API message `{name_crc}` is not installed")]
    MessageNameCrcMissing { name_crc: std::string::String },
}

pub struct ApiMsgConfig {
    pub id: u16,
    pub name: &'static str,
    pub crc: u32,
    pub dispatch: Option<fn(&[u8], bool) -> Result<(), codec::Error>>,
    pub is_mp_safe: bool,
    pub traced: bool,
    pub replay: bool,
}
impl ApiMsgConfig {
    pub fn new<T: Api>(id: u16) -> Self {
        Self {
            id,
            name: T::NAME,
            crc: T::CRC,
            dispatch: None,
            is_mp_safe: false,
            traced: !T::FLAGS.contains(&"dont_trace"),
            replay: true,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ApiMsgData {
    pub name: Option<&'static str>,
    pub dispatch: Option<fn(&[u8], bool) -> Result<(), codec::Error>>,
    pub is_mp_safe: bool,
    pub trace_enable: bool,
    pub replay_allowed: bool,
}

pub struct ApiMsgRange {
    pub name: std::string::String,
    pub first_msg_id: u16,
    pub last_msg_id: u16,
}

pub struct ApiVersion {
    pub name: &'static str,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

pub struct ApiMain {
    msg_data: RefCell<Vec<ApiMsgData>>,
    msg_id_by_name: RefCell<HashMap<&'static str, u16>>,
    msg_index_by_name_and_crc: RefCell<HashMap<std::string::String, u16>>,
    first_available_msg_id: Cell<u16>,
    msg_ranges: RefCell<Vec<ApiMsgRange>>,
    msg_range_by_name: RefCell<HashMap<std::string::String, usize>>,
    api_version_list: RefCell<Vec<ApiVersion>>,
    pub(super) clients: UnsafeCell<Pool<NonNull<ApiRegistration>>>,
    pub(super) serialized_message_table: UnsafeCell<Option<NonNull<u8>>>,
    pub(super) rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) primary_rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) private_rps: UnsafeCell<Vec<NonNull<SvmRegion>>>,
    pub(super) mapped_shmem_regions: UnsafeCell<Vec<Box<SvmRegion>>>,
    pub(super) shmem_header: UnsafeCell<Option<NonNull<ShmemHeader>>>,
    pub(super) process_pid: AtomicI32,
    pub(super) ring_misses: AtomicU32,
    pub(super) input_queue_length: u32,
    api_uid: u32,
    api_gid: u32,
    global_base_va: u64,
    global_size: u64,
    pub(super) api_size: u64,
    global_pvt_heap_size: u64,
    pub(super) api_pvt_heap_size: u64,
    pub(super) api_region_name: String,
}

// SAFETY: configuration requires &mut access before installation. Every
// registry Cell/RefCell access checks the runtime main thread first, including
// the ranges and versions installed by later API-init hooks. Region pointers
// change only through the unsafe exclusive
// lifecycle operations, with no allocator, queue or async borrower remaining.
unsafe impl Send for ApiMain {}
unsafe impl Sync for ApiMain {}

impl ApiMain {
    pub fn new(first_available_msg_id: u16) -> Self {
        Self {
            msg_data: RefCell::new(Vec::new()),
            msg_id_by_name: RefCell::new(HashMap::new()),
            msg_index_by_name_and_crc: RefCell::new(HashMap::new()),
            first_available_msg_id: Cell::new(first_available_msg_id),
            msg_ranges: RefCell::new(Vec::new()),
            msg_range_by_name: RefCell::new(HashMap::new()),
            api_version_list: RefCell::new(Vec::new()),
            clients: UnsafeCell::new(Pool::new()),
            serialized_message_table: UnsafeCell::new(None),
            rp: UnsafeCell::new(None),
            primary_rp: UnsafeCell::new(None),
            private_rps: UnsafeCell::new(Vec::new()),
            mapped_shmem_regions: UnsafeCell::new(Vec::new()),
            shmem_header: UnsafeCell::new(None),
            process_pid: AtomicI32::new(0),
            ring_misses: AtomicU32::new(0),
            input_queue_length: 0,
            api_uid: unsafe { libc::getuid() },
            api_gid: unsafe { libc::getgid() },
            global_base_va: 0,
            global_size: 0,
            api_size: 0,
            global_pvt_heap_size: 0,
            api_pvt_heap_size: 0,
            api_region_name: "/vpe-api".to_owned(),
        }
    }

    #[inline]
    pub fn is_mapped(&self) -> bool {
        unsafe { (*self.shmem_header.get()).is_some() }
    }

    pub fn install(self) {
        assert!(
            api_global_main.set(self).is_ok(),
            "Binary API main installs once"
        );
    }

    /// The calling thread's selected Main, defaulting to the installed Main.
    /// This selection is independent of the caller-owned VAPI Client value.
    #[inline(always)]
    pub fn current() -> &'static Self {
        my_api_main.get().unwrap_or_else(|| {
            api_global_main
                .get()
                .expect("Binary API main installed before use")
        })
    }

    /// Selects an existing Main for synchronous implicit-owner API entry points.
    ///
    /// # Safety
    /// The caller must preserve the selected Main's thread-access contract.
    /// In particular, daemon handlers must run with the daemon Main selected;
    /// sharing a reference does not permit concurrent region mutation. An async
    /// operation must retain its actual Main rather than reselect after await.
    /// Region switching/unmapping requires all users of that region to finish.
    #[inline(always)]
    pub unsafe fn set_main(main: &'static Self) {
        my_api_main.set(Some(main));
    }

    pub fn set_input_queue_length(&mut self, length: u32) {
        self.input_queue_length = length;
    }

    pub fn set_api_uid(&mut self, uid: u32) {
        self.api_uid = uid;
    }

    pub fn set_api_gid(&mut self, gid: u32) {
        self.api_gid = gid;
    }

    pub fn set_global_base_va(&mut self, base_va: u64) {
        self.global_base_va = base_va;
    }

    pub fn set_global_size(&mut self, bytes: u64) {
        self.global_size = bytes;
    }

    pub fn set_api_size(&mut self, bytes: u64) {
        self.api_size = bytes;
    }

    pub fn set_global_pvt_heap_size(&mut self, bytes: u64) {
        self.global_pvt_heap_size = bytes;
    }

    pub fn set_api_pvt_heap_size(&mut self, bytes: u64) {
        self.api_pvt_heap_size = bytes;
    }

    pub fn set_api_region_name(&mut self, name: String) {
        self.api_region_name = name;
    }

    pub fn api_uid(&self) -> u32 {
        self.api_uid
    }

    pub fn api_gid(&self) -> u32 {
        self.api_gid
    }

    pub fn global_base_va(&self) -> NonZeroUsize {
        let base_va = if self.global_base_va == 0 {
            0x130000000
        } else {
            self.global_base_va
        };
        NonZeroUsize::new(usize::try_from(base_va).expect("root base fits target pointer width"))
            .expect("root base must be nonzero")
    }

    pub fn root_region_config(&self) -> SvmRegionConfig {
        SvmRegionConfig {
            name: "/global_vm".to_owned(),
            size: usize::try_from(if self.global_size == 0 {
                64 << 20
            } else {
                self.global_size
            })
            .expect("root size fits target pointer width"),
            pvt_heap_size: usize::try_from(self.global_pvt_heap_size)
                .expect("root PVT heap fits target pointer width"),
            flags: SvmRegionFlags::NODATA,
        }
    }

    /// Requires a live selected ordinary region and exclusive lifecycle access.
    pub unsafe fn set_primary_region(&self) {
        let region = unsafe { *self.rp.get() }.expect("primary region must be mapped");
        unsafe { *self.primary_rp.get() = Some(region) };
    }

    pub fn get_msg_ids(&self, name: &str, count: u16) -> Result<u16, Error> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message ranges are accessed by runtime main thread");
        let mut names = self.msg_range_by_name.borrow_mut();
        if names.contains_key(name) {
            return Err(Error::MessageRangeExists {
                name: name.to_owned(),
            });
        }
        if count > 1024 {
            return Err(Error::MessageCountInvalid { count });
        }
        assert!(
            unsafe { (*self.serialized_message_table.get()).is_none() },
            "API ranges are installed before clients receive the message table"
        );
        // VPP stores the counter in u16 and permits zero count. Use explicit
        // wrapping, rather than adding a protocol error absent from that API.
        let base = self.first_available_msg_id.get();
        let end = base.wrapping_add(count);
        let mut ranges = self.msg_ranges.borrow_mut();
        ranges.push(ApiMsgRange {
            name: name.to_owned(),
            first_msg_id: base,
            last_msg_id: end.wrapping_sub(1),
        });
        names.insert(name.to_owned(), ranges.len() - 1);
        self.first_available_msg_id.set(end);
        Ok(base)
    }

    pub fn msg_config(&self, config: ApiMsgConfig) {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        if config.id == 0 {
            tracing::warn!(
                name = config.name,
                "API message id is zero; skipping registration"
            );
            return;
        }
        assert!(
            unsafe { (*self.serialized_message_table.get()).is_none() },
            "API message configuration precedes client message-table publication"
        );
        let index = usize::from(config.id);
        let mut msg_data = self.msg_data.borrow_mut();
        let capacity = msg_data.len().max(index + 1);
        msg_data.resize(capacity, ApiMsgData::default());
        if let Some(dispatch) = msg_data[index].dispatch
            && config
                .dispatch
                .is_none_or(|next| !std::ptr::fn_addr_eq(dispatch, next))
        {
            tracing::warn!(name = config.name, "replacing an API message handler");
        }
        msg_data[index] = ApiMsgData {
            name: Some(config.name),
            dispatch: config.dispatch,
            is_mp_safe: config.is_mp_safe,
            trace_enable: config.traced,
            replay_allowed: config.replay,
        };
        drop(msg_data);
        self.msg_id_by_name
            .borrow_mut()
            .insert(config.name, config.id);
    }

    pub fn add_msg_name_crc(&self, name_crc: &str, id: u16) {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        let mut names = self.msg_index_by_name_and_crc.borrow_mut();
        if names.contains_key(name_crc) {
            tracing::warn!(name_crc, "duplicate API identity ignored");
            return;
        }
        assert!(
            unsafe { (*self.serialized_message_table.get()).is_none() },
            "API message identities are installed before clients receive the table"
        );
        names.insert(name_crc.to_owned(), id);
    }
    pub fn get_msg_index(&self, name_crc: &str) -> Result<u16, Error> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        self.msg_index_by_name_and_crc
            .borrow()
            .get(name_crc)
            .copied()
            .ok_or_else(|| Error::MessageNameCrcMissing {
                name_crc: name_crc.to_owned(),
            })
    }

    pub fn msg_id_by_name(&self, name: &str) -> Option<u16> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        self.msg_id_by_name.borrow().get(name).copied()
    }
    #[inline]
    pub fn get_msg_data(&self, id: u16) -> Option<ApiMsgData> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        self.msg_data.borrow().get(usize::from(id)).copied()
    }
    pub fn message_range(&self, name: &str) -> Option<Ref<'_, ApiMsgRange>> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message ranges are accessed by runtime main thread");
        let index = *self.msg_range_by_name.borrow().get(name)?;
        Some(Ref::map(self.msg_ranges.borrow(), |ranges| &ranges[index]))
    }
    pub fn add_version(&self, version: ApiVersion) {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API versions are accessed by runtime main thread");
        self.api_version_list.borrow_mut().push(version);
    }
    pub fn versions(&self) -> Ref<'_, [ApiVersion]> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API versions are accessed by runtime main thread");
        Ref::map(self.api_version_list.borrow(), Vec::as_slice)
    }
    #[inline]
    pub fn message_table(&self) -> Ref<'_, HashMap<std::string::String, u16>> {
        hammer_runtime::thread_main::ensure_main_thread()
            .expect("API message registry is accessed by runtime main thread");
        self.msg_index_by_name_and_crc.borrow()
    }
}

#[inline(always)]
fn handler<T: Api>(message: T, function: fn(T)) {
    function(message);
}

#[doc(hidden)]
pub fn dispatch_message<T: Api>(
    payload: &[u8],
    barrier_required: bool,
    function: fn(T),
) -> Result<(), codec::Error> {
    let mut decoder = codec::Deserializer::new(payload);
    let message = serde::Deserialize::deserialize(&mut decoder)?;
    if decoder.remaining_bytes() != 0 {
        return Err(<codec::Error as serde::de::Error>::custom(
            "API message has trailing payload bytes",
        ));
    }
    if barrier_required {
        hammer_runtime::worker_thread_barrier_sync!({
            handler::<T>(message, function);
        });
    } else {
        handler::<T>(message, function);
    }
    Ok(())
}
