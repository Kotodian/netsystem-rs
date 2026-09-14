//! Message identity and installation. VPP's range failure, replacement and
//! duplicate-name behaviors are intentionally distinct operations.
use super::{Api, codec};
use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU32};

use hammer_infra::svm::region::{SvmRegion, SvmRegionConfig, SvmRegionFlags};

use super::memory_shared::ShmemHeader;

#[allow(non_upper_case_globals)]
static api_global_main: OnceLock<ApiMain> = OnceLock::new();

thread_local! {
    #[allow(non_upper_case_globals)]
    static my_api_main: Cell<&'static ApiMain> = Cell::new(
        api_global_main.get().expect("Binary API main installed before thread selection")
    );
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
    pub handler: Option<fn(&mut ApiMain, &[u8], &mut [u8]) -> Result<usize, codec::Error>>,
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
            handler: None,
            is_mp_safe: false,
            traced: !T::FLAGS.contains(&"dont_trace"),
            replay: true,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ApiMsgData {
    pub name: Option<&'static str>,
    pub handler: Option<fn(&mut ApiMain, &[u8], &mut [u8]) -> Result<usize, codec::Error>>,
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
    msg_data: Vec<ApiMsgData>,
    msg_id_by_name: HashMap<&'static str, u16>,
    msg_index_by_name_and_crc: HashMap<std::string::String, u16>,
    first_available_msg_id: u16,
    msg_ranges: Vec<ApiMsgRange>,
    msg_range_by_name: HashMap<std::string::String, usize>,
    api_version_list: Vec<ApiVersion>,
    pub(super) rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) primary_rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) private_rps: UnsafeCell<Vec<NonNull<SvmRegion>>>,
    pub(super) mapped_shmem_regions: UnsafeCell<Vec<Box<SvmRegion>>>,
    pub(super) shmem_header: UnsafeCell<Option<NonNull<ShmemHeader>>>,
    pub(super) process_pid: AtomicI32,
    pub(super) ring_misses: AtomicU32,
    pub(super) input_queue_length: u32,
    api_uid: i32,
    api_gid: i32,
    global_base_va: u64,
    global_size: u64,
    pub(super) api_size: u64,
    global_pvt_heap_size: u64,
    pub(super) api_pvt_heap_size: u64,
    pub(super) api_region_name: String,
}

// SAFETY: registration and configuration stop before installation. Only the
// exclusive lifecycle owner mutates region pointers, and it must ensure no
// allocator or queue borrower remains before changing/unmapping them.
unsafe impl Send for ApiMain {}
unsafe impl Sync for ApiMain {}

impl ApiMain {
    pub fn new(first_available_msg_id: u16) -> Self {
        Self {
            msg_data: Vec::new(),
            msg_id_by_name: HashMap::new(),
            msg_index_by_name_and_crc: HashMap::new(),
            first_available_msg_id,
            msg_ranges: Vec::new(),
            msg_range_by_name: HashMap::new(),
            api_version_list: Vec::new(),
            rp: UnsafeCell::new(None),
            primary_rp: UnsafeCell::new(None),
            private_rps: UnsafeCell::new(Vec::new()),
            mapped_shmem_regions: UnsafeCell::new(Vec::new()),
            shmem_header: UnsafeCell::new(None),
            process_pid: AtomicI32::new(0),
            ring_misses: AtomicU32::new(0),
            input_queue_length: 0,
            api_uid: -1,
            api_gid: -1,
            global_base_va: 0,
            global_size: 0,
            api_size: 0,
            global_pvt_heap_size: 0,
            api_pvt_heap_size: 0,
            api_region_name: "/vpe-api".to_owned(),
        }
    }

    pub fn install(self) {
        assert!(
            api_global_main.set(self).is_ok(),
            "Binary API main installs once"
        );
    }

    #[inline(always)]
    pub fn global() -> &'static Self {
        my_api_main.get()
    }

    /// Selects the main for this thread; the previous selection remains live
    /// and can be restored by passing it to this method again.
    pub fn set_main(main: &'static Self) {
        my_api_main.set(main);
    }

    pub fn set_input_queue_length(&mut self, length: u32) {
        self.input_queue_length = length;
    }

    pub fn set_api_uid(&mut self, uid: i32) {
        self.api_uid = uid;
    }

    pub fn set_api_gid(&mut self, gid: i32) {
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

    pub fn api_uid(&self) -> i32 {
        self.api_uid
    }

    pub fn api_gid(&self) -> i32 {
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

    pub fn get_msg_ids(&mut self, name: &str, count: u16) -> Result<u16, Error> {
        if self.msg_range_by_name.contains_key(name) {
            return Err(Error::MessageRangeExists {
                name: name.to_owned(),
            });
        }
        if count > 1024 {
            return Err(Error::MessageCountInvalid { count });
        }
        // VPP stores the counter in u16 and permits zero count. Use explicit
        // wrapping, rather than adding a protocol error absent from that API.
        let base = self.first_available_msg_id;
        let end = base.wrapping_add(count);
        self.msg_ranges.push(ApiMsgRange {
            name: name.to_owned(),
            first_msg_id: base,
            last_msg_id: end.wrapping_sub(1),
        });
        self.msg_range_by_name
            .insert(name.to_owned(), self.msg_ranges.len() - 1);
        self.first_available_msg_id = end;
        Ok(base)
    }

    pub fn msg_config(&mut self, config: ApiMsgConfig) {
        if config.id == 0 {
            tracing::warn!(
                name = config.name,
                "API message id is zero; skipping registration"
            );
            return;
        }
        let index = usize::from(config.id);
        self.msg_data
            .resize(self.msg_data.len().max(index + 1), ApiMsgData::default());
        if let Some(handler) = self.msg_data[index].handler
            && config
                .handler
                .is_none_or(|next| !std::ptr::fn_addr_eq(handler, next))
        {
            tracing::warn!(name = config.name, "replacing an API message handler");
        }
        self.msg_data[index] = ApiMsgData {
            name: Some(config.name),
            handler: config.handler,
            is_mp_safe: config.is_mp_safe,
            trace_enable: config.traced,
            replay_allowed: config.replay,
        };
        self.msg_id_by_name.insert(config.name, config.id);
    }

    pub fn add_msg_name_crc(&mut self, name_crc: &str, id: u16) {
        if self.msg_index_by_name_and_crc.contains_key(name_crc) {
            tracing::warn!(name_crc, "duplicate API identity ignored");
            return;
        }
        self.msg_index_by_name_and_crc
            .insert(name_crc.to_owned(), id);
    }
    pub fn get_msg_index(&self, name_crc: &str) -> Result<u16, Error> {
        self.msg_index_by_name_and_crc
            .get(name_crc)
            .copied()
            .ok_or_else(|| Error::MessageNameCrcMissing {
                name_crc: name_crc.to_owned(),
            })
    }
    pub fn msg_id_by_name(&self, name: &str) -> Option<u16> {
        self.msg_id_by_name.get(name).copied()
    }
    pub fn get_msg_data(&self, id: u16) -> Option<&ApiMsgData> {
        self.msg_data.get(usize::from(id))
    }
    pub fn message_range(&self, name: &str) -> Option<&ApiMsgRange> {
        self.msg_range_by_name
            .get(name)
            .map(|index| &self.msg_ranges[*index])
    }
    pub fn add_version(&mut self, version: ApiVersion) {
        self.api_version_list.push(version);
    }
    pub fn versions(&self) -> &[ApiVersion] {
        &self.api_version_list
    }
    pub fn message_table(&self) -> impl Iterator<Item = (&str, u16)> + '_ {
        self.msg_index_by_name_and_crc
            .iter()
            .map(|(name, id)| (name.as_str(), *id))
    }
}
