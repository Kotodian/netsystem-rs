//! Offset-based shared region owner.
//!
//! Rust counterpart of VPP's `svm_region_t` / `svm_main_region_t` (ADR-0011
//! section 12). A region is the metadata and allocation authority living inside
//! one [`SvmSegment`] payload: a fixed header, one or two [`SvmRegionHeap`]
//! instances, the member table, and - for a subdivided root - the
//! [`SvmRegionMain`] subregion name registry.
//!
//! Every shared location is a payload-relative offset, so two processes that map
//! the same segment at different addresses observe the same objects. The header
//! is published by storing `version` last; attach reads it first and then
//! validates magic, layout, and section offsets before touching shared state.
//!
//! The region mutex is the only serialization point. It is process-shared and
//! robust, so an owner that dies while holding it is reported instead of
//! deadlocking: the region latches `Failed` and every later access fails with
//! `RegionFailed`. VPP re-initializes the mutex in that situation
//! (`svm.c:713-732`); Hammer refuses to guess whether the shared state the dead
//! owner touched is consistent.

use std::alloc::Layout;
use std::fmt;
use std::io;
use std::mem::{MaybeUninit, align_of, size_of};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};

use posix_sync::condvar::{CondvarBuilder, CondvarClock, CondvarSharing, RawCondvarAlloc};
use posix_sync::mutex::guards::{RobustGuardContainer, StandardGuard};
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexSharing, RawMutexAlloc, robustness_markers::Robust,
};

use crate::svm::hash_map::{SvmHashMap, SvmKeys};
use crate::svm::region_heap::SvmRegionHeap;
use crate::svm::segment::{SVM_SEGMENT_PAYLOAD_OFFSET, SvmSegment, SvmSegmentError};

/// Region version; a nonzero `version` is the single ready authority.
///
/// The value mirrors VPP's `SVM_VERSION` (`svm_common.h:19`) so a region header
/// can be compared against the VPP constant while debugging.
pub const SVM_REGION_VERSION: u64 = (1 << 16) | 1;

/// The region owns a heap for its data section (VPP `SVM_FLAGS_MHEAP`).
pub const REGION_FLAG_DATA_HEAP: u64 = 1 << 0;
/// The region is a root whose data section carries the subregion registry
/// (VPP `SVM_FLAGS_NODATA`).
pub const REGION_FLAG_SUBDIVIDED: u64 = 1 << 2;

/// Every flag bit this version understands.
const REGION_FLAGS: u64 = REGION_FLAG_DATA_HEAP | REGION_FLAG_SUBDIVIDED;

/// Metadata heap reserved in front of a regular region's data section.
///
/// VPP keeps a 128K private mheap for region metadata
/// (`SVM_PVT_MHEAP_SIZE`, `svm_common.h:23`); the same reservation keeps the
/// name registry and member table of a root region out of the data section.
pub const SVM_REGION_METADATA_HEAP_SIZE: u64 = 128 << 10;

/// Longest accepted subregion name.
pub const SVM_REGION_NAME_MAX_LENGTH: usize = 256;

/// Smallest member table; grows by doubling from here.
const MINIMUM_MEMBER_CAPACITY: u64 = 8;

/// Smallest heap range a region creates: one block, rounded to the alignment
/// every region offset uses. The heap itself rejects anything below its own
/// block minimum.
const MINIMUM_HEAP_BYTES: u64 = 64;

/// Shared-state alignment for the header and every region section.
const REGION_ALIGN: usize = 64;

/// Identifies the region header inside a segment payload.
const REGION_MAGIC: u64 = 0x4841_4d4d_4552_5247;

#[inline]
fn align_u64(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

/// End of the fixed header, where the metadata heap starts.
fn header_end() -> u64 {
    align_u64(size_of::<SvmRegionHeader>() as u64, REGION_ALIGN as u64)
}

/// Payload-relative start of the data section for `flags`.
///
/// A subdivided root keeps the whole payload after the header as its metadata
/// heap and points `data_base_offset` at the registry it allocates there. A
/// regular region reserves the metadata heap in front of the data section.
fn data_section_start(flags: u64) -> u64 {
    if flags & REGION_FLAG_SUBDIVIDED != 0 {
        header_end()
    } else {
        header_end() + SVM_REGION_METADATA_HEAP_SIZE
    }
}

/// Shared region header.
///
/// The field order follows VPP's `svm_region_t` where the field survives; the
/// trailing fields are Hammer additions described in ADR-0011 section 12.2.
/// `version` is the ready flag: creation stores it last and attach reads it
/// first. `state` only ever holds `Uninitialized` or `Failed`; `Ready` is
/// derived from `version`, so readiness has one authority.
#[repr(C, align(64))]
pub struct SvmRegionHeader {
    /// Ready flag, written last by the creator and read first by attach.
    pub version: AtomicU64,
    /// Process-shared mutex; the region's only serialization point.
    pub mutex: MaybeUninit<RawMutexAlloc>,
    /// Process-shared condvar. VPP creates one per region and never waits on it;
    /// the field is kept so the shared layout matches that expectation.
    pub condvar: MaybeUninit<RawCondvarAlloc>,
    /// PID of the process currently holding `mutex`, 0 when free.
    pub mutex_owner_pid: AtomicI32,
    /// [`RegionLockTag`] value describing why `mutex` is held, 0 when free.
    pub mutex_owner_tag: AtomicI32,
    /// `REGION_FLAG_*` bits.
    pub flags: AtomicU64,
    /// Payload bytes this header describes.
    pub virtual_size: u64,
    /// Payload-relative start of the data section.
    pub data_base_offset: u64,
    /// Data-section heap; valid only with [`REGION_FLAG_DATA_HEAP`].
    pub data_heap: SvmRegionHeap,
    /// Region metadata heap: names, member table, and the subdivided registry.
    pub metadata_heap: SvmRegionHeap,
    /// Payload-relative offset of the user context, 0 when unset.
    pub user_ctx_offset: u64,
    /// Payload-relative offset of the member pid array, 0 when empty.
    pub client_pids_offset: AtomicU64,
    /// Number of stored member pids.
    pub client_count: AtomicU64,
    /// Capacity of the member pid array.
    pub client_capacity: AtomicU64,
    /// Header identity, checked before any other field is trusted.
    pub magic: u64,
    /// Failure latch; see the type documentation.
    pub state: AtomicU32,
    /// Payload-relative offset of the root object, 0 when unset.
    pub root_offset: AtomicU64,
}

/// Lifecycle of a shared region as observed from any mapping.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmRegionState {
    /// `version` has not been published yet.
    Uninitialized = 0,
    /// `version` is published and the region is usable.
    Ready = 1,
    /// A mutex owner died or the header was corrupted; the region is unusable.
    Failed = 2,
}

/// Why the region mutex is held; stored in `mutex_owner_tag` for debugging.
///
/// VPP passes the same kind of tag to its file-local `region_lock`
/// (`svm.c:89-105`, called with 1/2/4/5/7 at `svm.c:471`, `:733`, `:896`,
/// `:1034`, `:1167`). The discriminants here follow those values where a VPP
/// tag exists; `Membership` and `Allocate` are Hammer additions with no VPP
/// counterpart.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLockTag {
    /// Initializing a region header.
    Init = 1,
    /// Mapping an initialized region.
    Attach = 2,
    /// Reading or writing the subregion registry.
    Subregion = 4,
    /// Releasing a mapping.
    Unmap = 5,
    /// Scanning members or borrowed state.
    Scan = 7,
    /// Joining or leaving the member table.
    Membership = 8,
    /// Allocating or releasing region storage.
    Allocate = 9,
}

/// Operation a caller asked for that the region cannot provide.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionOperation {
    /// Creating a region on a segment this process attached instead of created.
    CreateOnAttachedSegment = 0,
    /// Asking a regular region for the subregion registry.
    SubregionRegistry = 1,
    /// Combining the subdivided layout with a region-owned data heap.
    SubdividedDataHeap = 2,
}

/// Failures a region reports to its caller.
///
/// Heap exhaustion and block corruption are not here: [`SvmRegionHeap`]
/// terminates the process instead, because a shared allocator that is already
/// inconsistent cannot be unwound (ADR-0011 section 12.4).
#[derive(Debug, thiserror::Error)]
pub enum SvmRegionError {
    #[error("region header magic {found:#x} is invalid")]
    InvalidMagic { found: u64 },
    #[error("region version {found} is unsupported, expected {expected}")]
    UnsupportedVersion { found: u64, expected: u64 },
    #[error("region is not ready: {state:?}")]
    NotReady { state: SvmRegionState },
    #[error("region failed while pid {mutex_owner_pid} held its mutex")]
    RegionFailed { mutex_owner_pid: i32 },
    #[error("region mutex owner pid {pid} died")]
    OwnerDied { pid: i32 },
    #[error("region range offset {offset} length {length} is outside {size} bytes")]
    InvalidBounds { offset: u64, length: u64, size: u64 },
    #[error("region offset {offset} is not aligned to {alignment}")]
    Misaligned { offset: u64, alignment: usize },
    #[error("region layout is {declared:#x}, expected {expected:#x}")]
    LayoutMismatch { declared: u64, expected: u64 },
    #[error("region name of {length} bytes is invalid")]
    InvalidRegionName { length: u64 },
    #[error("region root offset {offset} is invalid")]
    InvalidRoot { offset: u64 },
    #[error("region member probe failed for pid {pid}: {source}")]
    MemberProbeUnavailable {
        pid: i32,
        #[source]
        source: io::Error,
    },
    #[error("region operation {operation:?} is unsupported")]
    UnsupportedOperation { operation: RegionOperation },
    #[error("region mutex lock failed: {source}")]
    Lock {
        #[source]
        source: posix_sync::mutex::MutexLockError,
    },
    #[error("region segment operation failed: {0}")]
    Segment(#[from] SvmSegmentError),
}

/// Configuration of a region created inside one segment payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmRegionConfig {
    /// Minimum payload bytes the region needs; `size` counts the region header,
    /// its heaps, and the data section together.
    pub size: u64,
    /// `REGION_FLAG_*` bits.
    pub flags: u64,
}

/// Owner handle for one region inside an [`SvmSegment`].
///
/// The handle is process-local; the state it reaches is shared. Every method
/// that touches shared state takes the region mutex first, so a caller does not
/// have to know the section layout or remember which fields need serialization.
pub struct SvmRegion {
    segment: Arc<SvmSegment>,
    header_offset: u64,
    mutex: BorrowedMutex<'static, Robust>,
}

/// Lifetime witness for mutexes that live inside a region mapping.
///
/// [`SvmRegion`] owns the [`SvmSegment`] that keeps the payload mapped, so the
/// mutex bytes outlive every borrow of the region; this value supplies that
/// lifetime to the borrowed-mutex constructors.
struct RegionMappingAnchor;

static REGION_MAPPING_ANCHOR: RegionMappingAnchor = RegionMappingAnchor;

/// Region mutex guard; releases the mutex and clears the owner fields on drop.
pub struct RegionLock<'region> {
    header: *mut SvmRegionHeader,
    /// Held so dropping the lock releases the region mutex; the fields above
    /// are cleared first, in this type's `Drop` implementation.
    #[expect(dead_code, reason = "the guard releases the mutex on drop")]
    guard: StandardGuard<'region>,
}

impl Drop for RegionLock<'_> {
    fn drop(&mut self) {
        // The owner fields are cleared before `guard` drops and unlocks, so a
        // later lock never observes this pid as the holder.
        unsafe {
            (*self.header).mutex_owner_pid.store(0, Ordering::Release);
            (*self.header).mutex_owner_tag.store(0, Ordering::Release);
        }
    }
}

/// Membership of one process in a region.
///
/// Dropping the value removes this process from the member table once. The
/// region keeps no other record of the process, exactly like VPP's
/// `client_pids` vector.
pub struct RegionMembership<'region> {
    region: &'region SvmRegion,
    pid: i32,
}

impl Drop for RegionMembership<'_> {
    fn drop(&mut self) {
        match self.region.lock(RegionLockTag::Membership) {
            Ok(lock) => {
                if !self
                    .region
                    .member_remove(self.region.header_ptr(), self.pid)
                {
                    eprintln!(
                        "svm region membership for pid {} is not in the table",
                        self.pid
                    );
                }
                drop(lock);
            }
            Err(error) => {
                eprintln!(
                    "svm region membership for pid {} not removed: {error}",
                    self.pid
                );
            }
        }
    }
}

/// Root region registry: subregion names to subregion ids.
///
/// It replaces VPP's `svm_main_region_t` (`svm_common.h:108-115`) without its
/// subregion pool: [`SvmHashMap`] owns the name bytes, stores the id, and
/// enumerates the names, which is everything the pool was used for.
#[repr(C, align(64))]
pub struct SvmRegionMain {
    subregions: SvmHashMap<u64>,
    next_subregion_id: AtomicU64,
}

impl SvmRegionMain {
    /// Creates an empty registry; 0 stays reserved as "no subregion".
    pub const fn new() -> Self {
        Self {
            subregions: SvmHashMap::new(),
            next_subregion_id: AtomicU64::new(1),
        }
    }

    /// Returns the id registered for `name`, creating one when absent.
    ///
    /// The returned flag reports whether this call created the entry. Ids are
    /// monotonic and never reused, so a stale id can never silently name
    /// another subregion.
    pub fn find_or_create(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &mut [u8],
        name: &str,
    ) -> Result<(u64, bool), SvmRegionError> {
        validate_region_name(name)?;
        if let Some(id) = self.subregions.get(heap, arena, name) {
            return Ok((*id, false));
        }
        let id = self.next_subregion_id.fetch_add(1, Ordering::AcqRel);
        self.subregions.insert(heap, arena, name, id);
        Ok((id, true))
    }

    /// Returns the id registered for `name`.
    pub fn subregion_id(&self, heap: &SvmRegionHeap, arena: &[u8], name: &str) -> Option<u64> {
        self.subregions.get(heap, arena, name).copied()
    }

    /// Removes `name` from the registry, returning the id it held.
    ///
    /// The name and its key bytes disappear from the table; the id is not
    /// reused. The caller that owns the subregion segment must have made it
    /// invisible to clients before this call.
    pub fn remove(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &mut [u8],
        name: &str,
    ) -> Option<u64> {
        self.subregions.remove(heap, arena, name)
    }

    /// Number of registered subregions.
    pub fn subregion_count(&self) -> u64 {
        self.subregions.len() as u64
    }

    /// Iterates over every registered name.
    pub fn subregion_names<'a>(
        &'a self,
        heap: &'a SvmRegionHeap,
        arena: &'a [u8],
    ) -> SvmKeys<'a, u64> {
        self.subregions.keys(heap, arena)
    }

    /// Next id that will be handed out.
    pub fn next_subregion_id(&self) -> u64 {
        self.next_subregion_id.load(Ordering::Acquire)
    }
}

impl Default for SvmRegionMain {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SvmRegionMain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SvmRegionMain")
            .field("subregions", &self.subregions.len())
            .field("next_subregion_id", &self.next_subregion_id())
            .finish()
    }
}

impl fmt::Debug for SvmRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SvmRegion")
            .field("header_offset", &self.header_offset)
            .field("payload_len", &self.payload_len())
            .finish_non_exhaustive()
    }
}

impl SvmRegion {
    /// Mapping layout to create for `config`.
    ///
    /// `config.size` counts payload bytes, so the returned mapping adds the
    /// segment header in front of it and rounds the structural minimum up.
    /// The returned size is still a minimum: the segment rounds the mapping up
    /// to a page size. Pass it to the segment constructor, then call
    /// [`Self::create`] with the same config.
    pub fn layout(config: &SvmRegionConfig) -> Result<Layout, SvmRegionError> {
        validate_region_flags(config.flags)?;
        let payload = config
            .size
            .max(data_section_start(config.flags).saturating_add(MINIMUM_HEAP_BYTES));
        let requested = SVM_SEGMENT_PAYLOAD_OFFSET.saturating_add(payload);
        let size = usize::try_from(requested).map_err(|_| SvmRegionError::InvalidBounds {
            offset: 0,
            length: requested,
            size: u64::MAX,
        })?;
        Layout::from_size_align(size, REGION_ALIGN).map_err(|_| SvmRegionError::InvalidBounds {
            offset: 0,
            length: requested,
            size: u64::MAX,
        })
    }

    /// Initializes a region header in a segment this process created.
    ///
    /// The segment stays unpublished until the header is complete, so no other
    /// process can attach to a half-initialized region.
    pub fn create(
        segment: Arc<SvmSegment>,
        config: &SvmRegionConfig,
    ) -> Result<Self, SvmRegionError> {
        validate_region_flags(config.flags)?;
        if !segment.is_creator() {
            return Err(SvmRegionError::UnsupportedOperation {
                operation: RegionOperation::CreateOnAttachedSegment,
            });
        }
        let header_offset = segment.payload_offset();
        let payload_len = segment.payload_len();
        let data_start = data_section_start(config.flags);
        if payload_len < data_start + MINIMUM_HEAP_BYTES || payload_len < config.size {
            return Err(SvmRegionError::InvalidBounds {
                offset: header_offset,
                length: data_start + MINIMUM_HEAP_BYTES,
                size: payload_len,
            });
        }
        let header_pointer =
            segment.offset_ptr(header_offset, size_of::<SvmRegionHeader>(), REGION_ALIGN)?;
        let header = header_pointer.cast::<SvmRegionHeader>();
        // SAFETY: the header owns this range of the payload exclusively; the
        // bytes were just mapped and no other process has a published version
        // to observe them through.
        unsafe {
            std::ptr::write(
                header,
                SvmRegionHeader {
                    version: AtomicU64::new(0),
                    mutex: MaybeUninit::uninit(),
                    condvar: MaybeUninit::uninit(),
                    mutex_owner_pid: AtomicI32::new(0),
                    mutex_owner_tag: AtomicI32::new(0),
                    flags: AtomicU64::new(config.flags),
                    virtual_size: payload_len,
                    data_base_offset: data_start,
                    data_heap: SvmRegionHeap::new(),
                    metadata_heap: SvmRegionHeap::new(),
                    user_ctx_offset: 0,
                    client_pids_offset: AtomicU64::new(0),
                    client_count: AtomicU64::new(0),
                    client_capacity: AtomicU64::new(0),
                    magic: REGION_MAGIC,
                    state: AtomicU32::new(SvmRegionState::Uninitialized as u32),
                    root_offset: AtomicU64::new(0),
                },
            );
        }
        // SAFETY: the raw mutex and condvar storage is inside the header just
        // written, and the builders initialize it in place.
        let mutex = unsafe {
            MutexBuilder::<Robust>::new()
                .with_sharing(MutexSharing::Shared)
                .build_borrowed((&raw mut (*header).mutex).cast(), &REGION_MAPPING_ANCHOR)
        };
        unsafe {
            CondvarBuilder::new()
                .with_sharing(CondvarSharing::Shared)
                .with_clock(CondvarClock::Monotonic)
                .build_borrowed((&raw mut (*header).condvar).cast(), &REGION_MAPPING_ANCHOR);
        }
        let region = Self {
            segment,
            header_offset,
            mutex,
        };
        let subdivided = config.flags & REGION_FLAG_SUBDIVIDED != 0;
        let lock = region.lock(RegionLockTag::Init)?;
        let metadata_start = header_end();
        // Heap offsets stay payload relative: every descriptor manages a range
        // of the same arena, so no caller has to add a section start back.
        let metadata_end = if subdivided {
            payload_len
        } else {
            header_end() + SVM_REGION_METADATA_HEAP_SIZE
        };
        // SAFETY: the create path holds the region lock. Each descriptor is a
        // header field, and the arena is the whole payload, so a descriptor
        // never overlaps the range it manages.
        unsafe { metadata_heap(header) }.initialize(
            unsafe { arena_mut(region.arena_range()) },
            metadata_start,
            metadata_end,
        );
        if config.flags & REGION_FLAG_DATA_HEAP != 0 {
            // SAFETY: as above; the data range starts after the metadata range,
            // so the two descriptors never describe the same bytes.
            unsafe {
                (*std::ptr::addr_of_mut!((*header).data_heap)).initialize(
                    arena_mut(region.arena_range()),
                    data_start,
                    payload_len,
                );
            }
        }
        if subdivided {
            let registry_layout = Layout::new::<SvmRegionMain>();
            let offset = unsafe { metadata_heap(header) }
                .allocate(unsafe { arena_mut(region.arena_range()) }, registry_layout);
            // SAFETY: the heap just returned this block, and the registry is
            // written once before `version` publishes it.
            unsafe {
                std::ptr::write(
                    region.payload_address(offset).cast::<SvmRegionMain>(),
                    SvmRegionMain::new(),
                );
                (*header).data_base_offset = offset;
            }
        }
        // Publish: every earlier write is visible to a process that observes
        // this version.
        unsafe {
            (*header)
                .version
                .store(SVM_REGION_VERSION, Ordering::Release);
        }
        drop(lock);
        region.segment.publish_ready();
        Ok(region)
    }

    /// Attaches to a region that another mapping already initialized.
    ///
    /// Validation happens before any shared state is used, and attach never
    /// mutates the member table: membership is an explicit [`Self::join`].
    pub fn attach(segment: Arc<SvmSegment>) -> Result<Self, SvmRegionError> {
        let header_offset = segment.payload_offset();
        let payload_len = segment.payload_len();
        let header_pointer =
            segment.offset_ptr(header_offset, size_of::<SvmRegionHeader>(), REGION_ALIGN)?;
        let header = header_pointer.cast::<SvmRegionHeader>();
        let version = unsafe { (*header).version.load(Ordering::Acquire) };
        if version == 0 {
            let state = region_state(header);
            if state == SvmRegionState::Failed {
                return Err(SvmRegionError::RegionFailed {
                    mutex_owner_pid: unsafe { (*header).mutex_owner_pid.load(Ordering::Acquire) },
                });
            }
            return Err(SvmRegionError::NotReady { state });
        }
        if version != SVM_REGION_VERSION {
            return Err(SvmRegionError::UnsupportedVersion {
                found: version,
                expected: SVM_REGION_VERSION,
            });
        }
        if failure_latched(header) {
            return Err(SvmRegionError::RegionFailed {
                mutex_owner_pid: unsafe { (*header).mutex_owner_pid.load(Ordering::Acquire) },
            });
        }
        let magic = unsafe { (*header).magic };
        if magic != REGION_MAGIC {
            return Err(SvmRegionError::InvalidMagic { found: magic });
        }
        let declared_size = unsafe { (*header).virtual_size };
        if declared_size != payload_len {
            return Err(SvmRegionError::LayoutMismatch {
                declared: declared_size,
                expected: payload_len,
            });
        }
        let flags = unsafe { (*header).flags.load(Ordering::Acquire) };
        validate_region_flags(flags)?;
        let subdivided = flags & REGION_FLAG_SUBDIVIDED != 0;
        let data_start = unsafe { (*header).data_base_offset };
        if data_start < header_end() || data_start >= payload_len {
            return Err(SvmRegionError::InvalidBounds {
                offset: data_start,
                length: size_of::<SvmRegionHeader>() as u64,
                size: payload_len,
            });
        }
        if subdivided {
            let registry_bytes = size_of::<SvmRegionMain>() as u64;
            if data_start + registry_bytes > payload_len {
                return Err(SvmRegionError::InvalidBounds {
                    offset: data_start,
                    length: registry_bytes,
                    size: payload_len,
                });
            }
        } else if data_start != header_end() + SVM_REGION_METADATA_HEAP_SIZE {
            return Err(SvmRegionError::LayoutMismatch {
                declared: data_start,
                expected: header_end() + SVM_REGION_METADATA_HEAP_SIZE,
            });
        }
        let expected_metadata = if subdivided {
            payload_len
        } else {
            header_end() + SVM_REGION_METADATA_HEAP_SIZE
        };
        let metadata = unsafe { &(*header).metadata_heap };
        if metadata.heap_start() != header_end()
            || metadata.heap_end() != expected_metadata
            || metadata.free_bytes() + metadata.used_bytes() != expected_metadata - header_end()
        {
            return Err(SvmRegionError::LayoutMismatch {
                declared: metadata.heap_end(),
                expected: expected_metadata,
            });
        }
        if flags & REGION_FLAG_DATA_HEAP != 0 {
            let data = unsafe { &(*header).data_heap };
            if data.heap_start() != data_start
                || data.heap_end() != payload_len
                || data.free_bytes() + data.used_bytes() != payload_len - data_start
            {
                return Err(SvmRegionError::LayoutMismatch {
                    declared: data.heap_end(),
                    expected: payload_len,
                });
            }
        }
        // SAFETY: the header was validated above, and the creator initialized
        // this process-shared mutex before publishing `version`.
        let mutex = unsafe {
            BorrowedMutex::<Robust>::from_raw(
                (&raw mut (*header).mutex).cast(),
                &REGION_MAPPING_ANCHOR,
            )
        };
        Ok(Self {
            segment,
            header_offset,
            mutex,
        })
    }

    /// Segment this region lives in.
    pub fn segment(&self) -> &Arc<SvmSegment> {
        &self.segment
    }

    /// Payload-relative offset of the region header.
    pub fn header_offset(&self) -> u64 {
        self.header_offset
    }

    /// Payload bytes the region spans.
    pub fn virtual_size(&self) -> Result<u64, SvmRegionError> {
        self.require_usable()?;
        Ok(unsafe { (*self.header_ptr()).virtual_size })
    }

    /// `REGION_FLAG_*` bits of this region.
    pub fn flags(&self) -> Result<u64, SvmRegionError> {
        self.require_usable()?;
        Ok(unsafe { (*self.header_ptr()).flags.load(Ordering::Acquire) })
    }

    /// Lifecycle state of this region.
    pub fn state(&self) -> Result<SvmRegionState, SvmRegionError> {
        Ok(region_state(self.header_ptr()))
    }

    /// Payload-relative offset of the user context, 0 when unset.
    pub fn user_ctx_offset(&self) -> Result<u64, SvmRegionError> {
        self.require_usable()?;
        Ok(unsafe { (*self.header_ptr()).user_ctx_offset })
    }

    /// Stores the user context offset; 0 clears it.
    ///
    /// A nonzero offset must address a live object behind the header, because
    /// the header itself is not a user context.
    pub fn publish_user_ctx(&self, offset: u64) -> Result<(), SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let pointer = self.header_ptr();
        if offset != 0 && (offset < header_end() || offset >= self.payload_len()) {
            return Err(SvmRegionError::InvalidBounds {
                offset,
                length: 1,
                size: self.payload_len(),
            });
        }
        unsafe {
            (*pointer).user_ctx_offset = offset;
        }
        drop(lock);
        Ok(())
    }

    /// Allocates `layout` from the region's data heap, or from the metadata
    /// heap when the region has no data heap.
    ///
    /// The returned offset is payload relative and stays valid for every process
    /// mapping this region.
    pub fn allocate(&self, layout: Layout) -> Result<u64, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Allocate)?;
        let result = self.allocate_locked(layout);
        drop(lock);
        result
    }

    /// Resizes the live allocation at `offset`; the offset changes only when the
    /// block must grow.
    pub fn reallocate(
        &self,
        offset: u64,
        layout: Layout,
        new_size: usize,
    ) -> Result<u64, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Allocate)?;
        let result = self.reallocate_locked(offset, layout, new_size);
        drop(lock);
        result
    }

    /// Releases the live allocation at `offset`.
    pub fn deallocate(&self, offset: u64, layout: Layout) -> Result<(), SvmRegionError> {
        let lock = self.lock(RegionLockTag::Allocate)?;
        let result = self.deallocate_locked(offset, layout);
        drop(lock);
        result
    }

    /// Free bytes in the heaps this region owns.
    pub fn free_bytes(&self) -> Result<u64, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let total = self.free_bytes_locked(self.header_ptr());
        drop(lock);
        Ok(total)
    }

    /// Bytes currently held by live blocks in the heaps this region owns.
    pub fn used_bytes(&self) -> Result<u64, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let total = self.used_bytes_locked(self.header_ptr());
        drop(lock);
        Ok(total)
    }

    /// Publishes the root object offset; 0 clears it.
    ///
    /// A nonzero offset must address a live object behind the header; the
    /// header and the region heaps' own descriptors live in front of it.
    pub fn publish_root(&self, offset: u64) -> Result<(), SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let pointer = self.header_ptr();
        if offset != 0 && (offset < header_end() || offset >= self.payload_len()) {
            return Err(SvmRegionError::InvalidRoot { offset });
        }
        unsafe {
            (*pointer).root_offset.store(offset, Ordering::Release);
        }
        drop(lock);
        Ok(())
    }

    /// Offset of the published root object.
    pub fn root(&self) -> Result<Option<u64>, SvmRegionError> {
        self.require_usable()?;
        let offset = unsafe { (*self.header_ptr()).root_offset.load(Ordering::Acquire) };
        Ok((offset != 0).then_some(offset))
    }

    /// Subregion registry of a subdivided root region.
    pub fn main(&self) -> Result<&SvmRegionMain, SvmRegionError> {
        self.require_usable()?;
        self.require_subdivided()?;
        let pointer = self.header_ptr();
        let offset = unsafe { (*pointer).data_base_offset };
        Ok(unsafe { &*self.payload_address(offset).cast::<SvmRegionMain>() })
    }

    /// Registers `name`, returning its id and whether this call created it.
    pub fn find_or_create_subregion(&self, name: &str) -> Result<(u64, bool), SvmRegionError> {
        let lock = self.lock(RegionLockTag::Subregion)?;
        self.require_subdivided()?;
        let pointer = self.header_ptr();
        let registry = unsafe {
            &mut *self
                .payload_address((*pointer).data_base_offset)
                .cast::<SvmRegionMain>()
        };
        let result = registry.find_or_create(
            unsafe { metadata_heap(pointer) },
            unsafe { arena_mut(self.arena_range()) },
            name,
        );
        drop(lock);
        result
    }

    /// Id registered for `name`.
    pub fn subregion_id(&self, name: &str) -> Result<Option<u64>, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Subregion)?;
        self.require_subdivided()?;
        let pointer = self.header_ptr();
        let registry = unsafe {
            &*self
                .payload_address((*pointer).data_base_offset)
                .cast::<SvmRegionMain>()
        };
        let result = registry.subregion_id(unsafe { metadata_heap(pointer) }, self.arena(), name);
        drop(lock);
        Ok(result)
    }

    /// Removes `name`, returning the id it held.
    pub fn remove_subregion(&self, name: &str) -> Result<Option<u64>, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Subregion)?;
        self.require_subdivided()?;
        let pointer = self.header_ptr();
        let registry = unsafe {
            &mut *self
                .payload_address((*pointer).data_base_offset)
                .cast::<SvmRegionMain>()
        };
        let result = registry.remove(
            unsafe { metadata_heap(pointer) },
            unsafe { arena_mut(self.arena_range()) },
            name,
        );
        drop(lock);
        Ok(result)
    }

    /// Number of registered subregions.
    pub fn subregion_count(&self) -> Result<u64, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Subregion)?;
        self.require_subdivided()?;
        let count = self.main()?.subregion_count();
        drop(lock);
        Ok(count)
    }

    /// Iterates over the registered subregion names.
    pub fn subregion_names<'region>(
        &'region self,
    ) -> Result<SvmKeys<'region, u64>, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Subregion)?;
        let result = (|| {
            self.require_subdivided()?;
            let pointer = self.header_ptr();
            let registry = unsafe {
                &*self
                    .payload_address((*pointer).data_base_offset)
                    .cast::<SvmRegionMain>()
            };
            let heap: &'region SvmRegionHeap = unsafe { metadata_heap(pointer) };
            Ok(registry.subregion_names(heap, self.arena()))
        })();
        drop(lock);
        result
    }

    /// Joins this process to the region.
    pub fn join(&self) -> Result<RegionMembership<'_>, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Membership)?;
        let pointer = self.header_ptr();
        let pid = std::process::id() as i32;
        self.member_push(pointer, pid);
        drop(lock);
        Ok(RegionMembership { region: self, pid })
    }

    /// Number of member pids.
    pub fn member_count(&self) -> Result<u64, SvmRegionError> {
        self.require_usable()?;
        Ok(unsafe { (*self.header_ptr()).client_count.load(Ordering::Acquire) })
    }

    /// Member pids currently recorded.
    pub fn client_pids(&self) -> Result<&[i32], SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let members = self.client_pids_locked(self.header_ptr());
        drop(lock);
        Ok(members)
    }

    /// Removes members whose process no longer exists, returning how many were
    /// removed.
    ///
    /// Every pid is probed before the table is changed, so a probe that cannot
    /// answer (rather than reporting the process as gone) leaves the table
    /// untouched.
    pub fn remove_exited_members(&self) -> Result<usize, SvmRegionError> {
        let lock = self.lock(RegionLockTag::Scan)?;
        let result = self.remove_exited_members_locked(self.header_ptr());
        drop(lock);
        result
    }

    /// Takes the region mutex.
    ///
    /// A robust mutex reports a dead owner instead of blocking forever; the
    /// region then latches `Failed` and this call returns `OwnerDied`. Later
    /// calls return `RegionFailed`, because the state the dead owner touched
    /// cannot be assumed consistent.
    pub fn lock(&self, tag: RegionLockTag) -> Result<RegionLock<'_>, SvmRegionError> {
        let header = self.header_ptr();
        if failure_latched(header) {
            return Err(SvmRegionError::RegionFailed {
                mutex_owner_pid: unsafe { (*header).mutex_owner_pid.load(Ordering::Acquire) },
            });
        }
        match unsafe { self.mutex.lock() } {
            Ok(RobustGuardContainer::Standard(guard)) => {
                unsafe {
                    (*header)
                        .mutex_owner_pid
                        .store(std::process::id() as i32, Ordering::Release);
                    (*header)
                        .mutex_owner_tag
                        .store(tag as i32, Ordering::Release);
                }
                Ok(RegionLock { header, guard })
            }
            Ok(RobustGuardContainer::Indeterminate(_)) => {
                let pid = unsafe { (*header).mutex_owner_pid.load(Ordering::Acquire) };
                unsafe {
                    (*header)
                        .state
                        .store(SvmRegionState::Failed as u32, Ordering::Release);
                }
                Err(SvmRegionError::OwnerDied { pid })
            }
            Err(source) => Err(SvmRegionError::Lock { source }),
        }
    }

    fn allocate_locked(&self, layout: Layout) -> Result<u64, SvmRegionError> {
        let pointer = self.header_ptr();
        let heap = self.allocation_target(pointer);
        let offset = unsafe {
            let arena = arena_mut(self.arena_range());
            (*heap).allocate(arena, layout)
        };
        Ok(offset)
    }

    fn reallocate_locked(
        &self,
        offset: u64,
        layout: Layout,
        new_size: usize,
    ) -> Result<u64, SvmRegionError> {
        let heap = self.heap_for_offset(offset, layout)?;
        let moved = unsafe {
            let arena = arena_mut(self.arena_range());
            (*heap).reallocate(arena, offset, layout, new_size)
        };
        Ok(moved)
    }

    fn deallocate_locked(&self, offset: u64, layout: Layout) -> Result<(), SvmRegionError> {
        let heap = self.heap_for_offset(offset, layout)?;
        unsafe {
            let arena = arena_mut(self.arena_range());
            (*heap).deallocate(arena, offset, layout);
        }
        Ok(())
    }

    fn remove_exited_members_locked(
        &self,
        pointer: *mut SvmRegionHeader,
    ) -> Result<usize, SvmRegionError> {
        let members = self.client_pids_locked(pointer);
        if members.is_empty() {
            return Ok(0);
        }
        let mut exited = Vec::new();
        for member in members {
            match probe_member(*member) {
                MemberProbe::Alive => {}
                MemberProbe::Exited => exited.push(*member),
                MemberProbe::Unavailable(source) => {
                    return Err(SvmRegionError::MemberProbeUnavailable {
                        pid: *member,
                        source,
                    });
                }
            }
        }
        for pid in &exited {
            self.member_remove(pointer, *pid);
        }
        Ok(exited.len())
    }

    fn member_push(&self, pointer: *mut SvmRegionHeader, pid: i32) {
        let mut offset = unsafe { (*pointer).client_pids_offset.load(Ordering::Acquire) };
        let count = unsafe { (*pointer).client_count.load(Ordering::Acquire) };
        let capacity = unsafe { (*pointer).client_capacity.load(Ordering::Acquire) };
        if count == capacity {
            let grown = (capacity * 2).max(MINIMUM_MEMBER_CAPACITY);
            let arena = unsafe { arena_mut(self.arena_range()) };
            let heap = unsafe { metadata_heap(pointer) };
            let replacement = heap.allocate(arena, pids_layout(grown));
            if offset != 0 {
                let length = (count as usize) * size_of::<i32>();
                arena.copy_within(
                    offset as usize..offset as usize + length,
                    replacement as usize,
                );
                heap.deallocate(arena, offset, pids_layout(capacity));
            }
            unsafe {
                (*pointer)
                    .client_pids_offset
                    .store(replacement, Ordering::Release);
                (*pointer).client_capacity.store(grown, Ordering::Release);
            }
            offset = replacement;
        }
        let slot = self.payload_address(offset).cast::<i32>();
        unsafe {
            slot.add(count as usize).write(pid);
            (*pointer).client_count.store(count + 1, Ordering::Release);
        }
    }

    fn member_remove(&self, pointer: *mut SvmRegionHeader, pid: i32) -> bool {
        let count = unsafe { (*pointer).client_count.load(Ordering::Acquire) } as usize;
        if count == 0 {
            return false;
        }
        let offset = unsafe { (*pointer).client_pids_offset.load(Ordering::Acquire) };
        let slot = self.payload_address(offset).cast::<i32>();
        // SAFETY: the member array is a live metadata-heap block of `count` pids
        // and the region mutex excludes every other process.
        let members = unsafe { std::slice::from_raw_parts_mut(slot, count) };
        let Some(index) = members.iter().position(|member| *member == pid) else {
            return false;
        };
        members.copy_within(index + 1.., index);
        unsafe {
            (*pointer)
                .client_count
                .store((count - 1) as u64, Ordering::Release);
        }
        true
    }

    fn client_pids_locked(&self, pointer: *mut SvmRegionHeader) -> &[i32] {
        let offset = unsafe { (*pointer).client_pids_offset.load(Ordering::Acquire) };
        let count = unsafe { (*pointer).client_count.load(Ordering::Acquire) } as usize;
        if offset == 0 || count == 0 {
            return &[];
        }
        let slot = self.payload_address(offset).cast::<i32>();
        // SAFETY: the member array is a live metadata-heap block of `count` pids,
        // published by a release store of `client_count`.
        unsafe { std::slice::from_raw_parts(slot, count) }
    }

    fn free_bytes_locked(&self, pointer: *mut SvmRegionHeader) -> u64 {
        let flags = unsafe { (*pointer).flags.load(Ordering::Acquire) };
        let metadata = unsafe { (*std::ptr::addr_of!((*pointer).metadata_heap)).free_bytes() };
        if flags & REGION_FLAG_DATA_HEAP == 0 {
            return metadata;
        }
        metadata + unsafe { (*std::ptr::addr_of!((*pointer).data_heap)).free_bytes() }
    }

    fn used_bytes_locked(&self, pointer: *mut SvmRegionHeader) -> u64 {
        let flags = unsafe { (*pointer).flags.load(Ordering::Acquire) };
        let metadata = unsafe { (*std::ptr::addr_of!((*pointer).metadata_heap)).used_bytes() };
        if flags & REGION_FLAG_DATA_HEAP == 0 {
            return metadata;
        }
        metadata + unsafe { (*std::ptr::addr_of!((*pointer).data_heap)).used_bytes() }
    }

    /// Heap that [`Self::allocate`] uses for this region.
    fn allocation_target(&self, pointer: *mut SvmRegionHeader) -> *mut SvmRegionHeap {
        self.data_heap(pointer)
            .unwrap_or(unsafe { std::ptr::addr_of_mut!((*pointer).metadata_heap) })
    }

    /// Heap of the region's data section, when it has one.
    fn data_heap(&self, pointer: *mut SvmRegionHeader) -> Option<*mut SvmRegionHeap> {
        let flags = unsafe { (*pointer).flags.load(Ordering::Acquire) };
        (flags & REGION_FLAG_DATA_HEAP != 0)
            .then(|| unsafe { std::ptr::addr_of_mut!((*pointer).data_heap) })
    }

    /// Heap that owns the payload-relative `offset`.
    ///
    /// Overlapping heap ranges are impossible by construction, so an offset
    /// inside the region belongs to exactly one heap.
    fn heap_for_offset(
        &self,
        offset: u64,
        layout: Layout,
    ) -> Result<*mut SvmRegionHeap, SvmRegionError> {
        let pointer = self.header_ptr();
        let payload_len = self.payload_len();
        let alignment = (layout.align() as u64).max(1);
        if !offset.is_multiple_of(alignment) {
            return Err(SvmRegionError::Misaligned {
                offset,
                alignment: layout.align(),
            });
        }
        let metadata_start = header_end();
        let data_start = unsafe { (*pointer).data_base_offset };
        let data_heap = self.data_heap(pointer);
        let metadata_end = data_heap.map_or(payload_len, |_| data_start);
        if offset >= metadata_start && offset < metadata_end {
            return Ok(unsafe { std::ptr::addr_of_mut!((*pointer).metadata_heap) });
        }
        if let Some(heap) = data_heap
            && offset >= data_start
            && offset < payload_len
        {
            return Ok(heap);
        }
        Err(SvmRegionError::InvalidBounds {
            offset,
            length: layout.size() as u64,
            size: payload_len,
        })
    }

    fn require_usable(&self) -> Result<(), SvmRegionError> {
        let pointer = self.header_ptr();
        if failure_latched(pointer) {
            return Err(SvmRegionError::RegionFailed {
                mutex_owner_pid: unsafe { (*pointer).mutex_owner_pid.load(Ordering::Acquire) },
            });
        }
        Ok(())
    }

    fn require_subdivided(&self) -> Result<(), SvmRegionError> {
        let flags = unsafe { (*self.header_ptr()).flags.load(Ordering::Acquire) };
        if flags & REGION_FLAG_SUBDIVIDED == 0 {
            return Err(SvmRegionError::UnsupportedOperation {
                operation: RegionOperation::SubregionRegistry,
            });
        }
        Ok(())
    }

    fn payload_len(&self) -> u64 {
        self.segment.payload_len()
    }

    fn header_ptr(&self) -> *mut SvmRegionHeader {
        unsafe {
            self.segment
                .base()
                .add(self.header_offset as usize)
                .cast::<SvmRegionHeader>()
        }
    }

    fn payload_address(&self, offset: u64) -> *mut u8 {
        unsafe {
            self.segment
                .base()
                .add((self.header_offset + offset) as usize)
        }
    }

    /// Shared view of the whole region payload.
    ///
    /// Every stored offset is payload relative, so one view serves every heap,
    /// the member table, and the name registry.
    fn arena(&self) -> &[u8] {
        let length = self.payload_len() as usize;
        // SAFETY: the payload is the bytes from offset 0 to the end of this
        // mapping, and the mapping outlives `&self`.
        unsafe { std::slice::from_raw_parts(self.payload_address(0), length) }
    }

    /// Raw mutable view of the whole region payload.
    ///
    /// The region mutex is the exclusivity witness for this borrow: a caller
    /// may only turn the range into an [`arena_mut`] borrow while it holds the
    /// region lock, and must not keep that borrow past its guard.
    fn arena_range(&self) -> *mut [u8] {
        std::ptr::slice_from_raw_parts_mut(self.payload_address(0), self.payload_len() as usize)
    }
}

/// Mutable view of one region payload arena.
///
/// # Safety
///
/// The caller must hold the region mutex, which makes this borrow exclusive,
/// and must not keep the borrow past that guard.
unsafe fn arena_mut<'arena>(range: *mut [u8]) -> &'arena mut [u8] {
    // SAFETY: the caller upholds the exclusivity contract above.
    unsafe { &mut *range }
}

/// Mutable metadata heap descriptor of `pointer`.
///
/// # Safety
///
/// The caller must hold the region mutex. The descriptor is a header field, so
/// it never overlaps an arena this region hands to the metadata heap.
unsafe fn metadata_heap<'arena>(pointer: *mut SvmRegionHeader) -> &'arena mut SvmRegionHeap {
    // SAFETY: the caller upholds the contract above; the projection itself only
    // forms a raw pointer.
    unsafe { &mut *std::ptr::addr_of_mut!((*pointer).metadata_heap) }
}

/// Whether a header records a failure and must not be operated on.
fn failure_latched(header: *mut SvmRegionHeader) -> bool {
    let stored = unsafe { (*header).state.load(Ordering::Acquire) };
    stored != SvmRegionState::Uninitialized as u32
}

/// Lifecycle state derived from the failure latch and the ready flag.
fn region_state(header: *mut SvmRegionHeader) -> SvmRegionState {
    if failure_latched(header) {
        return SvmRegionState::Failed;
    }
    let version = unsafe { (*header).version.load(Ordering::Acquire) };
    if version == SVM_REGION_VERSION {
        SvmRegionState::Ready
    } else {
        SvmRegionState::Uninitialized
    }
}

fn validate_region_flags(flags: u64) -> Result<(), SvmRegionError> {
    if flags & !REGION_FLAGS != 0 {
        return Err(SvmRegionError::LayoutMismatch {
            declared: flags,
            expected: REGION_FLAGS,
        });
    }
    if flags & REGION_FLAG_SUBDIVIDED != 0 && flags & REGION_FLAG_DATA_HEAP != 0 {
        return Err(SvmRegionError::UnsupportedOperation {
            operation: RegionOperation::SubdividedDataHeap,
        });
    }
    Ok(())
}

fn validate_region_name(name: &str) -> Result<(), SvmRegionError> {
    if name.is_empty() || name.len() > SVM_REGION_NAME_MAX_LENGTH {
        return Err(SvmRegionError::InvalidRegionName {
            length: name.len() as u64,
        });
    }
    Ok(())
}

fn pids_layout(capacity: u64) -> Layout {
    Layout::from_size_align((capacity as usize) * size_of::<i32>(), align_of::<i32>())
        .expect("member pid array layout is valid")
}

enum MemberProbe {
    Alive,
    Exited,
    Unavailable(io::Error),
}

/// Probes whether `pid` still exists without sending it a signal.
fn probe_member(pid: i32) -> MemberProbe {
    // SAFETY: signal 0 runs the existence and permission checks and delivers
    // nothing.
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return MemberProbe::Alive;
    }
    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::ESRCH) => MemberProbe::Exited,
        // A live process owned by another user refuses signal 0 with EPERM.
        Some(libc::EPERM) => MemberProbe::Alive,
        _ => MemberProbe::Unavailable(source),
    }
}
