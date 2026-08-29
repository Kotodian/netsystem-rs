use core::{mem, ptr, slice};

use super::*;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(super) struct BufferHeaderCacheline0 {
    pub(super) cacheline0: hammer_infra::align::CacheLineAlignMark,
    pub(super) current_data: i16,
    pub(super) current_length: u16,
    pub(super) flags: BufferFlags,
    pub(super) flow_id: u32,
    pub(super) ref_count: u8,
    pub(super) buffer_pool_index: u8,
    pub(super) error: Option<NodeErrorIndex>,
    pub(super) next_buffer: u32,
    pub(super) current_config_or_punt: u32,
    pub(super) opaque: PrimaryOpaque,
}

const _: () =
    assert!(core::mem::size_of::<Option<NodeErrorIndex>>() == core::mem::size_of::<u16>());
const _: () = assert!(core::mem::size_of::<BufferHeaderCacheline0>() == 64);
const _: () = assert!(core::mem::align_of::<BufferHeaderCacheline0>() == 64);

impl Default for BufferHeaderCacheline0 {
    fn default() -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            current_data: 0,
            current_length: 0,
            flags: BufferFlags::empty(),
            flow_id: 0,
            ref_count: 0,
            buffer_pool_index: 0,
            error: None,
            next_buffer: BUFFER_INVALID_INDEX,
            current_config_or_punt: 0,
            opaque: PrimaryOpaque::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(super) struct BufferHeaderCacheline1 {
    pub(super) cacheline1: hammer_infra::align::CacheLineAlignMark,
    pub(super) trace_handle: u32,
    pub(super) total_length_not_including_first: u32,
    pub(super) opaque2: SecondaryOpaque,
}

const _: () = assert!(core::mem::size_of::<BufferHeaderCacheline1>() == 64);
const _: () = assert!(core::mem::align_of::<BufferHeaderCacheline1>() == 64);

impl Default for BufferHeaderCacheline1 {
    fn default() -> Self {
        Self {
            cacheline1: hammer_infra::align::CacheLineAlignMark,
            trace_handle: 0,
            total_length_not_including_first: 0,
            opaque2: SecondaryOpaque::default(),
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Buffer {
    pub(super) cacheline0: BufferHeaderCacheline0,
    pub(super) cacheline1: BufferHeaderCacheline1,
}

const _: () = assert!(mem::align_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE);
const _: () = assert!(mem::size_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE * 2);

#[inline]
pub(crate) const fn buffer_data_offset() -> usize {
    mem::size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE
}

impl Buffer {
    #[inline]
    pub(crate) fn reset(&mut self, data_size: usize, bytes: &[u8]) -> DataPlaneResult<()> {
        if bytes.len() > data_size {
            return Err(BufferInvariant::BytesExceedCapacity {
                length: bytes.len(),
                capacity: data_size,
            }
            .into());
        }
        let current_len =
            u16::try_from(bytes.len()).map_err(|_| BufferInvariant::CurrentLengthOutOfRange)?;
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.cacheline0.current_data = 0;
        self.cacheline0.current_length = current_len;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
        self.data_region_mut(data_size)[..bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    pub(crate) fn reset_for_free(&mut self, data_size: usize) {
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
    }

    #[inline]
    pub(crate) fn reset_empty(&mut self, data_size: usize, headroom: usize) -> DataPlaneResult<()> {
        if headroom > data_size {
            return Err(BufferInvariant::HeadroomExceedsCapacity.into());
        }
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_current_data_offset(isize::try_from(headroom).expect("headroom fits isize"))?;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        self.cacheline1 = BufferHeaderCacheline1::default();
        Ok(())
    }

    /// Clear only cacheline0 and mark the slot clean, leaving cacheline1 alone.
    /// Caller must have verified `flags.contains(SLOT_CLEAN)` so cacheline1 is
    /// already zeroed from the previous free. Returns the headroom/length pair
    /// the alloc fast path needs.
    #[inline]
    pub(crate) fn reset_empty_fast(
        &mut self,
        data_size: usize,
        headroom: usize,
    ) -> DataPlaneResult<()> {
        if headroom > data_size {
            return Err(BufferInvariant::HeadroomExceedsCapacity.into());
        }
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_current_data_offset(isize::try_from(headroom).expect("headroom fits isize"))?;
        self.cacheline0.ref_count = 1;
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
        Ok(())
    }

    /// Free fast path: only cacheline0 is rewritten (clean-default with
    /// SLOT_CLEAN set); cacheline1 is left untouched because it is already
    /// zeroed when SLOT_CLEAN was set on the slot.
    #[inline]
    pub(crate) fn reset_for_free_fast(&mut self, data_size: usize) {
        self.cacheline0 = BufferHeaderCacheline0::default();
        self.set_data_capacity(data_size);
        self.cacheline0.flags.insert(BufferFlags::SLOT_CLEAN);
    }

    #[inline]
    pub fn opaque(&self) -> &PrimaryOpaque {
        &self.cacheline0.opaque
    }

    #[inline]
    pub fn opaque_mut(&mut self) -> &mut PrimaryOpaque {
        &mut self.cacheline0.opaque
    }

    #[inline]
    pub fn opaque2(&self) -> &SecondaryOpaque {
        &self.cacheline1.opaque2
    }

    #[inline]
    pub fn opaque2_mut(&mut self) -> &mut SecondaryOpaque {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        &mut self.cacheline1.opaque2
    }

    #[inline]
    pub fn current_config(&self) -> NodeId {
        NodeId::new(self.cacheline0.current_config_or_punt)
    }

    #[inline]
    pub fn set_current_config(&mut self, next: NodeId) {
        self.cacheline0.current_config_or_punt = next.slot();
    }

    #[inline]
    pub fn node_error_index(&self) -> Option<NodeErrorIndex> {
        self.cacheline0.error
    }

    #[inline]
    pub fn trace_handle(&self) -> Option<u32> {
        (self.cacheline1.trace_handle != 0).then_some(self.cacheline1.trace_handle)
    }

    #[inline]
    pub fn set_trace_handle(&mut self, handle: u32) {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        self.cacheline1.trace_handle = handle;
    }

    #[inline]
    pub fn take_trace_handle(&mut self) -> Option<u32> {
        let handle = self.trace_handle();
        self.cacheline1.trace_handle = 0;
        if handle.is_none() {
            // trace_handle was already 0; if the rest of cacheline1 is also
            // zeroed we keep CLEAN, otherwise it was already cleared.
        } else {
            // We cannot prove cacheline1 is fully zeroed anymore without a
            // scan; conservatively drop the clean invariant. A subsequent
            // free will rebuild it via the slow path.
            self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        }
        handle
    }

    #[inline]
    pub fn set_node_error_index(&mut self, error: NodeErrorIndex) {
        self.cacheline0.error = Some(error);
    }

    #[inline]
    pub fn clear_node_error(&mut self) {
        self.cacheline0.error = None;
    }

    #[inline]
    pub fn flags(&self) -> BufferFlags {
        BufferFlags::from_bits(self.cacheline0.flags.bits())
    }

    #[inline]
    pub fn current_data_offset(&self) -> i16 {
        self.cacheline0.current_data
    }

    #[inline]
    pub fn current_data(&self) -> usize {
        usize::try_from(self.current_data_offset()).unwrap_or(0)
    }

    #[inline]
    pub fn ref_count(&self) -> u8 {
        self.cacheline0.ref_count
    }

    #[inline]
    pub fn current_len(&self) -> usize {
        usize::from(self.cacheline0.current_length)
    }

    #[inline]
    pub fn next_buffer_slot(&self) -> Option<u32> {
        self.flags()
            .contains(BufferFlags::NEXT_PRESENT)
            .then_some(self.cacheline0.next_buffer)
    }

    #[inline]
    pub fn total_len_not_including_first(&self) -> usize {
        self.cacheline1.total_length_not_including_first as usize
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        let len = self.current_len();
        // SAFETY: `current_ptr` is computed from the inline slot backing owned
        // by the arena, and `current_len` is maintained within slot bounds by
        // the pool mutation paths.
        unsafe { slice::from_raw_parts(self.current_ptr(), len) }
    }

    #[inline]
    pub fn current_ptr(&self) -> *const u8 {
        // SAFETY: the slot layout is `[Buffer][pre_data][data]`; the current
        // window is always kept within that inline backing.
        unsafe {
            self.as_bytes_ptr()
                .add(self.current_start_offset_from_header())
        }
    }

    #[inline]
    pub fn current_mut(&mut self) -> &mut [u8] {
        let len = self.current_len();
        // SAFETY: see `current`; the mutable borrow of `self` guarantees unique
        // access to the current window.
        unsafe {
            slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr()
                    .add(self.current_start_offset_from_header()),
                len,
            )
        }
    }

    #[inline]
    pub fn writable_tail_mut(&mut self) -> &mut [u8] {
        let data_size = self.data_capacity();
        let start = self.current_end_offset_from_header();
        let len = self.available_tail_with_data_size(data_size);
        let writable_start = mem::size_of::<Buffer>();
        let offset = start - writable_start;
        &mut self.slot_writable_region_mut(data_size)[offset..offset + len]
    }

    #[inline]
    pub fn commit_writable_tail(&mut self, len: usize) -> DataPlaneResult<()> {
        if len > self.available_tail_with_data_size(self.data_capacity()) {
            return Err(BufferInvariant::CommitExceedsWritableTail.into());
        }
        self.set_current_len(self.current_len() + len)?;
        Ok(())
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) -> DataPlaneResult<()> {
        if len > self.current_len() {
            return Err(BufferInvariant::TruncateExtendsCurrentLength.into());
        }
        self.set_current_len(len)
    }

    #[inline]
    pub fn advance(&mut self, displacement: isize) -> DataPlaneResult<()> {
        if displacement == 0 {
            return Ok(());
        }
        if displacement < 0 {
            let rewind = displacement.unsigned_abs();
            if rewind > self.available_headroom() {
                return Err(BufferInvariant::RewindExceedsHeadroom.into());
            }
            let new_offset = isize::from(self.current_data_offset())
                - isize::try_from(rewind).expect("rewind fits isize");
            self.set_current_data_offset(new_offset)?;
            self.set_current_len(self.current_len() + rewind)?;
            return Ok(());
        }

        let len = usize::try_from(displacement)
            .map_err(|_| BufferInvariant::AdvanceDisplacementOutOfRange)?;
        if len > self.current_len() {
            return Err(BufferInvariant::AdvanceExceedsCurrentLength.into());
        }
        let new_offset =
            isize::from(self.current_data_offset()) + isize::try_from(len).expect("len fits isize");
        self.set_current_data_offset(new_offset)?;
        self.set_current_len(self.current_len() - len)
    }

    #[inline]
    pub fn current_mut_ptr(&mut self) -> *mut u8 {
        // SAFETY: the slot layout is `[Buffer][pre_data][data]`; the current
        // window is always kept within that inline backing.
        unsafe {
            self.as_mut_bytes_ptr()
                .add(self.current_start_offset_from_header())
        }
    }

    #[inline]
    pub fn prepend(&mut self, bytes: &[u8]) -> DataPlaneResult<()> {
        self.prepend_mut(bytes.len())?.copy_from_slice(bytes);
        Ok(())
    }

    #[inline]
    pub fn prepend_mut(&mut self, len: usize) -> DataPlaneResult<&mut [u8]> {
        if len > self.available_headroom() {
            return Err(BufferInvariant::PrependExceedsHeadroom.into());
        }
        let current_start = self.current_start_offset_from_header();
        let start = current_start - len;
        let offset_from_data = isize::try_from(start).expect("slot offset fits isize")
            - isize::try_from(buffer_data_offset()).expect("buffer data offset fits isize");
        self.set_current_data_offset(offset_from_data)?;
        self.set_current_len(self.current_len() + len)?;
        // SAFETY: `start..current_start` lies within the inline pre_data/data
        // range and the mutable borrow of `self` guarantees uniqueness.
        unsafe {
            Ok(slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr().add(start),
                len,
            ))
        }
    }

    #[inline]
    pub(crate) fn available_tail_with_data_size(&self, data_size: usize) -> usize {
        self.data_end_offset_from_header(data_size)
            .saturating_sub(self.current_end_offset_from_header())
    }

    #[inline]
    pub(crate) fn append_in_place(&mut self, data_size: usize, bytes: &[u8]) -> usize {
        let take = bytes
            .len()
            .min(self.available_tail_with_data_size(data_size));
        if take == 0 {
            return 0;
        }
        let start = self.current_end_offset_from_header();
        let end = start + take;
        let writable_start = mem::size_of::<Buffer>();
        self.slot_writable_region_mut(data_size)[start - writable_start..end - writable_start]
            .copy_from_slice(&bytes[..take]);
        self.set_current_len(self.current_len() + take)
            .expect("buffer append keeps current length within u16");
        take
    }

    #[inline]
    pub(crate) fn set_current_data_offset(&mut self, offset: isize) -> DataPlaneResult<()> {
        let lower_bound =
            -isize::try_from(DEFAULT_PRE_DATA_SIZE).expect("default pre-data size fits isize");
        if offset < lower_bound {
            return Err(BufferInvariant::CurrentDataExceedsPreData.into());
        }
        self.cacheline0.current_data =
            i16::try_from(offset).map_err(|_| BufferInvariant::CurrentDataOutOfRange)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn set_current_len(&mut self, len: usize) -> DataPlaneResult<()> {
        self.cacheline0.current_length =
            u16::try_from(len).map_err(|_| BufferInvariant::CurrentLengthOutOfRange)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn set_data_capacity(&mut self, data_size: usize) {
        self.cacheline0.flags = self.cacheline0.flags.with_private_data_capacity(data_size);
    }

    #[inline]
    pub(crate) fn data_capacity(&self) -> usize {
        self.cacheline0.flags.private_data_capacity()
    }

    #[inline]
    pub(crate) fn set_next_buffer(&mut self, next: Option<Index>) {
        self.cacheline0.next_buffer = next.map_or(BUFFER_INVALID_INDEX, Index::slot);
        if next.is_some() {
            self.cacheline0.flags.insert(BufferFlags::NEXT_PRESENT);
        } else {
            self.cacheline0.flags.remove(BufferFlags::NEXT_PRESENT);
        }
    }

    #[inline]
    pub(crate) fn set_total_len_not_including_first(&mut self, len: usize) -> DataPlaneResult<()> {
        self.cacheline0.flags.remove(BufferFlags::SLOT_CLEAN);
        self.cacheline1.total_length_not_including_first =
            u32::try_from(len).map_err(|_| BufferInvariant::ChainTailLengthOutOfRange)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn as_bytes_ptr(&self) -> *const u8 {
        ptr::from_ref(self).cast::<u8>()
    }

    #[inline]
    pub(crate) fn as_mut_bytes_ptr(&mut self) -> *mut u8 {
        ptr::from_mut(self).cast::<u8>()
    }

    #[inline]
    pub(crate) fn current_start_offset_from_header(&self) -> usize {
        let offset = isize::try_from(buffer_data_offset()).expect("buffer data offset fits isize")
            + isize::from(self.current_data_offset());
        usize::try_from(offset).expect("buffer current start underflowed header")
    }

    #[inline]
    pub(crate) fn current_end_offset_from_header(&self) -> usize {
        self.current_start_offset_from_header() + self.current_len()
    }

    #[inline]
    pub(crate) fn data_end_offset_from_header(&self, data_size: usize) -> usize {
        buffer_data_offset() + data_size
    }

    #[inline]
    pub(crate) fn slot_writable_end_offset_from_header(&self, data_size: usize) -> usize {
        mem::size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE + data_size
    }

    #[inline]
    pub(crate) fn available_headroom(&self) -> usize {
        self.current_start_offset_from_header()
            .saturating_sub(mem::size_of::<Buffer>())
    }

    #[inline]
    pub(crate) fn data_region_mut(&mut self, data_size: usize) -> &mut [u8] {
        // SAFETY: the inline data region begins at `buffer_data_offset()` and
        // spans exactly `data_size` bytes in the owning arena slot.
        unsafe {
            slice::from_raw_parts_mut(self.as_mut_bytes_ptr().add(buffer_data_offset()), data_size)
        }
    }

    #[inline]
    pub(crate) fn slot_writable_region_mut(&mut self, data_size: usize) -> &mut [u8] {
        // SAFETY: the writable inline slot backing begins immediately after the
        // header and spans the full `[pre_data][data]` capacity for the slot.
        unsafe {
            slice::from_raw_parts_mut(
                self.as_mut_bytes_ptr().add(mem::size_of::<Buffer>()),
                self.slot_writable_end_offset_from_header(data_size) - mem::size_of::<Buffer>(),
            )
        }
    }
}
