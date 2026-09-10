use core::sync::atomic::{AtomicU8, Ordering};
use core::{mem, ptr, slice};

use super::*;

#[derive(Debug)]
#[repr(C)]
pub(super) struct BufferTemplate {
    pub(super) cacheline0: hammer_infra::align::CacheLineAlignMark,
    pub(super) current_data: i16,
    pub(super) current_length: u16,
    pub(super) flags: BufferFlags,
    pub(super) flow_id: u32,
    pub(super) ref_count: AtomicU8,
    pub(super) buffer_pool_index: u8,
    pub(super) error: Option<NodeErrorIndex>,
    pub(super) next_buffer: u32,
    pub(super) current_config_or_punt: u32,
    pub(super) opaque: PrimaryOpaque,
}

const _: () =
    assert!(core::mem::size_of::<Option<NodeErrorIndex>>() == core::mem::size_of::<u16>());
const _: () = assert!(core::mem::size_of::<BufferTemplate>() == 64);
const _: () = assert!(core::mem::align_of::<BufferTemplate>() == 64);
const _: () = assert!(mem::offset_of!(BufferTemplate, current_data) == 0);
const _: () = assert!(mem::offset_of!(BufferTemplate, current_length) == 2);
const _: () = assert!(mem::offset_of!(BufferTemplate, flags) == 4);
const _: () = assert!(mem::offset_of!(BufferTemplate, flow_id) == 8);
const _: () = assert!(mem::offset_of!(BufferTemplate, ref_count) == 12);
const _: () = assert!(mem::offset_of!(BufferTemplate, buffer_pool_index) == 13);
const _: () = assert!(mem::offset_of!(BufferTemplate, error) == 14);
const _: () = assert!(mem::offset_of!(BufferTemplate, next_buffer) == 16);
const _: () = assert!(mem::offset_of!(BufferTemplate, current_config_or_punt) == 20);
const _: () = assert!(mem::offset_of!(BufferTemplate, opaque) == 24);

impl Clone for BufferTemplate {
    fn clone(&self) -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            current_data: self.current_data,
            current_length: self.current_length,
            flags: self.flags,
            flow_id: self.flow_id,
            ref_count: AtomicU8::new(self.ref_count.load(Ordering::Relaxed)),
            buffer_pool_index: self.buffer_pool_index,
            error: self.error,
            next_buffer: self.next_buffer,
            current_config_or_punt: self.current_config_or_punt,
            opaque: self.opaque,
        }
    }
}

impl Default for BufferTemplate {
    fn default() -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            current_data: 0,
            current_length: 0,
            flags: BufferFlags::empty(),
            flow_id: 0,
            ref_count: AtomicU8::new(1),
            buffer_pool_index: 0,
            error: None,
            next_buffer: BUFFER_INVALID_INDEX,
            current_config_or_punt: 0,
            opaque: PrimaryOpaque::default(),
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Buffer {
    pub(super) cacheline0: BufferTemplate,
    pub(super) second_half: hammer_infra::align::CacheLineAlignMark,
    trace_handle: u32,
    pub(super) total_length_not_including_first: u32,
    pub(super) opaque2: SecondaryOpaque,
    #[cfg(hammer_buffer_trace_trajectory)]
    trajectory: hammer_infra::align::CacheLineAlignMark,
    #[cfg(hammer_buffer_trace_trajectory)]
    trajectory_nb: u16,
    #[cfg(hammer_buffer_trace_trajectory)]
    trajectory_trace: [u16; 31],
    headroom: hammer_infra::align::CacheLineAlignMark,
    pre_data: [u8; BUFFER_PRE_DATA_SIZE],
    data: [u8; 0],
}

const _: () = assert!(mem::align_of::<Buffer>() == BUFFER_CACHE_LINE_SIZE);
const _: () = assert!(mem::size_of::<Buffer>() == BUFFER_HEADER_SIZE + BUFFER_PRE_DATA_SIZE);
const _: () = assert!(mem::offset_of!(Buffer, headroom) == BUFFER_HEADER_SIZE);
const _: () = assert!(mem::offset_of!(Buffer, pre_data) == BUFFER_HEADER_SIZE);
const _: () = assert!(mem::offset_of!(Buffer, second_half) == 64);
const _: () = assert!(mem::offset_of!(Buffer, trace_handle) == 64);
const _: () = assert!(mem::offset_of!(Buffer, total_length_not_including_first) == 68);
const _: () = assert!(mem::offset_of!(Buffer, opaque2) == 72);
const _: () = assert!(mem::offset_of!(Buffer, data) == BUFFER_HEADER_SIZE + BUFFER_PRE_DATA_SIZE);
#[cfg(hammer_buffer_trace_trajectory)]
const _: () = assert!(mem::offset_of!(Buffer, trajectory_nb) == 128);
#[cfg(hammer_buffer_trace_trajectory)]
const _: () = assert!(mem::offset_of!(Buffer, trajectory_trace) == 130);

impl Buffer {
    #[inline]
    pub fn current_config_index(&self) -> u32 {
        self.cacheline0.current_config_or_punt
    }

    #[inline]
    pub fn set_current_config_index(&mut self, index: u32) {
        self.cacheline0.current_config_or_punt = index;
    }

    #[inline]
    pub fn node_error_index(&self) -> Option<NodeErrorIndex> {
        self.cacheline0.error
    }

    #[inline]
    pub fn trace_handle(&self) -> Option<u32> {
        self.cacheline0
            .flags
            .contains(BufferFlags::TRACED)
            .then_some(self.trace_handle)
    }

    #[inline]
    pub fn set_trace_handle(&mut self, handle: u32) {
        self.trace_handle = handle;
        self.cacheline0.flags.insert(BufferFlags::TRACED);
    }

    #[inline]
    pub fn take_trace_handle(&mut self) -> Option<u32> {
        let handle = self.trace_handle();
        self.cacheline0.flags.remove(BufferFlags::TRACED);
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
    pub fn ref_count(&self) -> u8 {
        self.cacheline0.ref_count.load(Ordering::Acquire)
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
        if self
            .cacheline0
            .flags
            .contains(BufferFlags::TOTAL_LENGTH_VALID)
        {
            self.total_length_not_including_first as usize
        } else {
            0
        }
    }

    #[inline]
    pub fn current(&self) -> &[u8] {
        let len = self.current_len();
        // SAFETY: `current_ptr` is computed from the inline slot backing owned
        // by its Physmem-backed Pool, and `current_len` is maintained within
        // slot bounds by the data-window operations.
        unsafe { slice::from_raw_parts(self.current_ptr(), len) }
    }

    #[inline]
    pub(crate) fn current_ptr(&self) -> *const u8 {
        // SAFETY: the slot layout is `[header][pre_data][data]`; the current
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
    pub fn truncate(&mut self, len: usize) -> DataPlaneResult<()> {
        if len > self.current_len() {
            return Err(BufferInvariant::TruncateExtendsCurrentLength.into());
        }
        self.set_current_window(isize::from(self.current_data_offset()), len);
        Ok(())
    }

    #[inline]
    pub fn advance(&mut self, displacement: isize) {
        let offset = isize::from(self.current_data_offset())
            .checked_add(displacement)
            .expect("Buffer advance offset fits isize");
        let length = (self.current_len() as isize)
            .checked_sub(displacement)
            .and_then(|length| usize::try_from(length).ok())
            .expect("Buffer advance stays within the current length");
        self.set_current_window(offset, length);
    }

    #[inline]
    pub fn reset(&mut self) {
        // vlib_buffer_reset retains current_length for a negative offset and
        // restores previously consumed bytes only for a positive offset.
        let consumed = self.current_data_offset().max(0) as usize;
        self.set_current_window(0, self.current_len() + consumed);
    }

    #[inline]
    pub fn space_left_at_end(&self) -> usize {
        self.data_end_offset_from_header(self.data_capacity())
            .checked_sub(self.current_end_offset_from_header())
            .expect("Buffer current window ends within its Pool data capacity")
    }

    /// Exposes retained storage without clearing it and publishes the new length.
    #[inline]
    pub fn put_uninit(&mut self, len: u16) -> &mut [u8] {
        let len = usize::from(len);
        assert!(
            len <= self.space_left_at_end(),
            "Buffer append fits tail capacity"
        );
        let start = self.current_end_offset_from_header();
        self.set_current_window(
            isize::from(self.current_data_offset()),
            self.current_len() + len,
        );
        // SAFETY: Physmem storage is initialized on mapping and retained on
        // recycle. The validated extension stays in this exclusively borrowed slot.
        unsafe { slice::from_raw_parts_mut(self.as_mut_bytes_ptr().add(start), len) }
    }

    #[inline]
    pub fn push_uninit(&mut self, len: u8) -> &mut [u8] {
        self.advance(-isize::from(len));
        &mut self.current_mut()[..usize::from(len)]
    }

    #[inline]
    pub fn make_headroom(&mut self, len: u8) -> &mut [u8] {
        self.set_current_window(
            isize::from(self.current_data_offset()) + isize::from(len),
            self.current_len(),
        );
        let available = self.space_left_at_end();
        // SAFETY: vlib_buffer_make_headroom returns the new current pointer.
        // ADR-0007 bounds the safe slice by the remaining end capacity. The
        // complete range was validated before changing current_data.
        unsafe { slice::from_raw_parts_mut(self.current_mut_ptr(), available) }
    }

    #[inline]
    pub fn pull(&mut self, len: u8) -> Option<&[u8]> {
        if usize::from(len) > self.current_len() {
            return None;
        }
        let start = self.current_start_offset_from_header();
        self.advance(isize::from(len));
        // SAFETY: this prefix belonged to the previous valid current window;
        // its storage remains borrowed through self after advancing the header.
        Some(unsafe { slice::from_raw_parts(self.as_bytes_ptr().add(start), usize::from(len)) })
    }

    pub(super) fn set_current_window(&mut self, offset: isize, length: usize) {
        let current_data = i16::try_from(offset).expect("Buffer current_data fits i16");
        let current_length = u16::try_from(length).expect("Buffer current_length fits u16");
        assert!(
            offset >= -(BUFFER_PRE_DATA_SIZE as isize),
            "Buffer current_data stays in pre-data"
        );
        let end = offset
            .checked_add(length as isize)
            .expect("Buffer current end fits isize");
        assert!(
            end <= self.data_capacity() as isize,
            "Buffer current window fits Pool data capacity"
        );
        self.cacheline0.current_data = current_data;
        self.cacheline0.current_length = current_length;
    }

    #[inline]
    pub(crate) fn current_mut_ptr(&mut self) -> *mut u8 {
        // SAFETY: the slot layout is `[header][pre_data][data]`; the current
        // window is always kept within that inline backing.
        unsafe {
            self.as_mut_bytes_ptr()
                .add(self.current_start_offset_from_header())
        }
    }

    #[inline]
    pub(crate) fn data_capacity(&self) -> usize {
        BufferMain::global().pools[usize::from(self.cacheline0.buffer_pool_index)].data_size
    }

    #[inline]
    pub fn set_next_buffer(&mut self, next: Option<u32>) {
        self.cacheline0.next_buffer = next.unwrap_or(BUFFER_INVALID_INDEX);
        if next.is_some() {
            self.cacheline0.flags.insert(BufferFlags::NEXT_PRESENT);
        } else {
            self.cacheline0.flags.remove(BufferFlags::NEXT_PRESENT);
        }
    }

    #[inline]
    pub fn set_total_len_not_including_first(&mut self, len: usize) -> DataPlaneResult<()> {
        let len = u32::try_from(len).map_err(|_| BufferInvariant::ChainTailLengthOutOfRange)?;
        self.total_length_not_including_first = len;
        self.cacheline0
            .flags
            .insert(BufferFlags::TOTAL_LENGTH_VALID);
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
        let offset = isize::try_from(mem::offset_of!(Buffer, data))
            .expect("buffer data offset fits isize")
            + isize::from(self.current_data_offset());
        usize::try_from(offset).expect("buffer current start underflowed header")
    }

    #[inline]
    pub(crate) fn current_end_offset_from_header(&self) -> usize {
        self.current_start_offset_from_header() + self.current_len()
    }

    #[inline]
    pub(crate) fn data_end_offset_from_header(&self, data_size: usize) -> usize {
        mem::offset_of!(Buffer, data) + data_size
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::{BUFFER_PRE_DATA_SIZE, BufferMain};
    use crate::error::DataPlaneResult;
    use hammer_infra::PageSize;

    // Issue #293, task 3 only.
    // Upstream test: third_party/vpp/src/plugins/unittest/vlib_test.c,
    // test_vlib_command_fn's "Cover simple functions in buffer.h / buffer_funcs.h".
    // Nonzero cases below exercise the named vendored helper's semantics required
    // by ADR-0007; they are not claimed to be separate upstream test cases.
    #[test]
    fn single_segment_operations_preserve_the_packet_window() -> DataPlaneResult<()> {
        hammer_infra::main_heap::init_default().unwrap();
        BufferMain::new(2048, 16, &[0, 1], 1, PageSize::Default)?;
        let buffers = BufferMain::global();
        // SAFETY: this test is the only executor for runtime thread index 1.
        let mut caches = unsafe { buffers.borrow_worker_caches(1) };
        let mut index = u32::MAX;
        assert_eq!(
            buffers.add_data(&mut caches, 0, &mut index, &[1, 2, 3, 4]),
            4
        );
        let packet = buffers.buffer(&caches, index);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // SAFETY: this test is the only executor for runtime thread index 1.
                unsafe { buffers.borrow_worker_caches(1) };
            }))
            .is_err()
        );
        assert_eq!(packet.current(), &[1, 2, 3, 4]);

        {
            // Ownership: this test owns the allocated segment until its explicit free.
            let buffer = buffers.buffer_mut(&mut caches, index);
            let data_start = buffer.current().as_ptr();

            // vlib_test.c: reset and the four zero-length operations.
            buffer.reset();
            assert_eq!(buffer.current_data_offset(), 0);
            assert_eq!(buffer.current(), &[1, 2, 3, 4]);
            assert!(buffer.put_uninit(0).is_empty());
            assert!(buffer.push_uninit(0).is_empty());
            assert_eq!(buffer.make_headroom(0).as_ptr(), data_start);
            assert_eq!(buffer.pull(0), Some([].as_slice()));
            assert_eq!(buffer.current_data_offset(), 0);
            assert_eq!(buffer.current(), &[1, 2, 3, 4]);

            // buffer.h: vlib_buffer_put_uninit returns the old tail, then
            // increases current_length. It does not move the current pointer.
            let tail = buffer.put_uninit(4);
            assert_eq!(tail.as_ptr(), data_start.wrapping_add(4));
            tail.copy_from_slice(&[5, 6, 7, 8]);
            assert_eq!(buffer.current_data_offset(), 0);
            assert_eq!(buffer.current(), &[1, 2, 3, 4, 5, 6, 7, 8]);
            assert_eq!(buffer.space_left_at_end(), 2040);

            // buffer.h: vlib_buffer_advance changes offset and length inversely;
            // vlib_buffer_reset adds max(current_data, 0) back to current_length.
            buffer.advance(4);
            assert_eq!(buffer.current_data_offset(), 4);
            assert_eq!(buffer.current(), &[5, 6, 7, 8]);
            assert_eq!(buffer.space_left_at_end(), 2040);
            buffer.advance(-4);
            assert_eq!(buffer.current_data_offset(), 0);
            assert_eq!(buffer.current(), &[1, 2, 3, 4, 5, 6, 7, 8]);
            buffer.advance(4);
            buffer.reset();
            assert_eq!(buffer.current_data_offset(), 0);
            assert_eq!(buffer.current(), &[1, 2, 3, 4, 5, 6, 7, 8]);

            if BUFFER_PRE_DATA_SIZE >= 4 {
                // buffer.h: vlib_buffer_push_uninit exposes inline pre_data.
                // vlib_buffer_reset does not subtract a negative current_data.
                let prefix = buffer.push_uninit(4);
                assert_eq!(prefix.as_ptr(), data_start.wrapping_sub(4));
                prefix.copy_from_slice(&[9, 10, 11, 12]);
                assert_eq!(buffer.current_data_offset(), -4);
                assert_eq!(buffer.current_len(), 12);
                assert_eq!(&buffer.current()[..4], &[9, 10, 11, 12]);
                assert_eq!(&buffer.current()[4..], &[1, 2, 3, 4, 5, 6, 7, 8]);
                assert_eq!(buffer.space_left_at_end(), 2040);
                buffer.reset();
                assert_eq!(buffer.current_data_offset(), 0);
                assert_eq!(buffer.current_len(), 12);
                assert_eq!(buffer.current().as_ptr(), data_start);
                assert_eq!(&buffer.current()[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
            }

            // buffer.h: vlib_buffer_make_headroom advances current_data without
            // changing current_length, returning the new current pointer.
            let length = buffer.current_len();
            let writable = buffer.make_headroom(8);
            assert_eq!(writable.as_ptr(), data_start.wrapping_add(8));
            // ADR-0007 defines the Rust slice extent for the upstream pointer.
            assert_eq!(writable.len(), 2048 - 8 - length);
            writable[..4].copy_from_slice(&[13, 14, 15, 16]);
            assert_eq!(buffer.current_data_offset(), 8);
            assert_eq!(buffer.current_len(), length);

            // buffer.h: vlib_buffer_pull returns the previous current pointer
            // and advances the window for a prefix within current_length.
            let prefix = buffer.pull(2).unwrap();
            assert_eq!(prefix.as_ptr(), data_start.wrapping_add(8));
            assert_eq!(prefix, &[13, 14]);
            assert_eq!(buffer.current_data_offset(), 10);
            assert_eq!(buffer.current_len(), length - 2);
            assert_eq!(&buffer.current()[..2], &[15, 16]);
        }
        buffers.free_buffers(&mut caches, &[index], true, |_| {});
        drop(caches);
        crate::buffer::opaque::tests::metadata_copy_and_pool_recycle_preserve_secondary_storage(
            buffers,
        )?;
        crate::buffer::operations::tests::allocation_chains_and_reference_release();
        Ok(())
    }
}
