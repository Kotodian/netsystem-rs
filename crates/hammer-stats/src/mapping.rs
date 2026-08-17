//! The single unsafe boundary between the stats segment and its users.
//!
//! Every raw pointer access into the shared mapping happens here, behind
//! validated bounds and alignment conversions. Callers never touch
//! `Segment::base` directly. Entry-write destinations are prepared with
//! checked arithmetic during a publication's preparation phase and consumed
//! immediately by the infallible publication tail; no raw pointer is stored
//! or reused across structural operations.

use std::sync::atomic::AtomicU64;

use hammer_infra::segment::Segment;

use crate::descriptor::{
    DESCRIPTOR_HEADER_SIZE, MAX_BLOCK_BYTES, MIN_BLOCK_BYTES, MetricDescriptorHeader,
};
use crate::directory::{DirectorySlot, SLOT_SIZE};
use crate::error::StatsError;
use crate::header::StatsHeader;
use crate::metric_value::{MetricValue, VALUE_RECORD_BYTES};
use crate::offset::Offset;

/// Bounds- and alignment-checked view over one shared segment mapping.
///
/// All accessors validate offsets against the mapping size before
/// dereferencing, mirroring the checked conversions VPP applies on entry
/// access (`vlib_stats_get_entry` asserts the index against the directory
/// vector length).
pub(crate) struct Mapping {
    base: *mut u8,
    size: usize,
}

impl Mapping {
    pub(crate) fn new(segment: &Segment) -> Mapping {
        Mapping {
            base: segment.base(),
            size: segment.size(),
        }
    }

    /// Shared access to the header in the reserved first page.
    ///
    /// Construction guarantees a mapping of at least one page; the header
    /// record is 128 bytes, 64-byte-aligned, at the mapping base.
    pub(crate) fn header(&self) -> &StatsHeader {
        // SAFETY: `base` is a non-null, page-aligned mapping of at least one
        // page (Segment invariant), and the header record is 128 bytes laid
        // out at offset zero. All header fields are plain data or atomics,
        // so shared access is sound.
        unsafe { &*self.base.cast::<StatsHeader>() }
    }

    /// Installs a fully formed header at the mapping base.
    ///
    /// Only called once, by the sole constructor, before any reader exists.
    pub(crate) fn write_header(&self, header: StatsHeader) {
        // SAFETY: same bounds and alignment justification as `header()`; the
        // header value is complete (every atomic constructed) before the
        // write, and no reader can observe it before the mapping is shared.
        unsafe {
            self.base.cast::<StatsHeader>().write(header);
        }
    }

    /// Bounds-validated directory entry read, copied by value.
    ///
    /// Mirrors `vlib_stats_get_entry`, which asserts the index against the
    /// directory vector length before dereferencing; here the check is a
    /// typed error instead of an assertion.
    pub(crate) fn entry(
        &self,
        directory_offset: Offset,
        index: u32,
    ) -> Result<DirectorySlot, StatsError> {
        checked_aligned(directory_offset)?;
        let offset = slot_offset(directory_offset, index).ok_or(StatsError::OutOfBounds)?;
        let end = offset
            .checked_add(SLOT_SIZE as u64)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: the span `offset..end` was validated against the mapping
        // size and `SLOT_SIZE` alignment (the directory offset above is
        // 64-aligned and `SLOT_SIZE` is a multiple of 64); the entry is
        // plain data copied by value. The state byte is validated below.
        let entry = unsafe {
            self.base
                .add(offset.get() as usize)
                .cast::<DirectorySlot>()
                .read()
        };
        entry.state()?;
        Ok(entry)
    }

    /// Prepares the destination of an entry write for the slot at `index`:
    /// computes the byte offset with checked arithmetic and validates the
    /// slot span against the mapping bounds.
    ///
    /// The returned pointer is ephemeral: it must be passed to
    /// [`write_entry`](Self::write_entry) within the same preparation and
    /// publication cycle and must never be stored.
    pub(crate) fn entry_write_target(
        &self,
        directory_offset: Offset,
        index: u32,
    ) -> Result<*mut DirectorySlot, StatsError> {
        checked_aligned(directory_offset)?;
        let offset = slot_offset(directory_offset, index).ok_or(StatsError::OutOfBounds)?;
        let end = offset
            .checked_add(SLOT_SIZE as u64)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: the span `offset..end` was validated against the mapping
        // size, and the slot is `SLOT_SIZE`-aligned (the directory offset
        // above is 64-byte aligned and `SLOT_SIZE` is a multiple of 64).
        // The pointer is only valid while the mapping lives and only
        // within this phase.
        Ok(unsafe { self.base.add(offset.get() as usize).cast::<DirectorySlot>() })
    }

    /// Infallible entry write through a target prepared by
    /// [`entry_write_target`](Self::entry_write_target).
    ///
    /// The publication tail performs no arithmetic: the destination was
    /// fully validated during the preparation phase of the same publication.
    ///
    /// # Safety
    ///
    /// `target` must be the return value of `entry_write_target` for the
    /// slot being written, computed after the caller's last structural read
    /// of that slot and consumed only inside the enclosing publication tail.
    pub(crate) unsafe fn write_entry(&self, target: *mut DirectorySlot, entry: DirectorySlot) {
        // SAFETY: `target` was bounds- and alignment-validated when prepared.
        unsafe { target.write(entry) };
    }

    /// Writes a span of directory entries at `block_offset`, checked against
    /// the mapping bounds. Used to populate a freshly grown directory block.
    pub(crate) fn write_directory_entries(
        &self,
        block_offset: Offset,
        entries: &[DirectorySlot],
    ) -> Result<(), StatsError> {
        if entries.is_empty() {
            return Ok(());
        }
        let span = (entries.len() as u64)
            .checked_mul(SLOT_SIZE as u64)
            .ok_or(StatsError::OutOfBounds)?;
        let end = block_offset
            .checked_add(span)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: bounds validated; the source entries are plain data and
        // the destination span is `SLOT_SIZE`-aligned (the block is
        // 64-byte aligned and `SLOT_SIZE` is a multiple of 64).
        unsafe {
            std::ptr::copy_nonoverlapping(
                entries.as_ptr(),
                self.base
                    .add(block_offset.get() as usize)
                    .cast::<DirectorySlot>(),
                entries.len(),
            );
        }
        Ok(())
    }

    /// Bounds- and alignment-checked shared access to a metric block's
    /// descriptor header, used to reconstruct the block's exact layout at
    /// reclamation time.
    pub(crate) fn descriptor(
        &self,
        descriptor_offset: Offset,
    ) -> Result<&MetricDescriptorHeader, StatsError> {
        checked_aligned(descriptor_offset)?;
        let end = descriptor_offset
            .checked_add(DESCRIPTOR_HEADER_SIZE)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: bounds and alignment validated; the header is plain data.
        Ok(unsafe {
            &*self
                .base
                .add(descriptor_offset.get() as usize)
                .cast::<MetricDescriptorHeader>()
        })
    }

    /// Bounds- and alignment-checked shared access to a metric block's full
    /// descriptor bytes, including the trailing value record.
    ///
    /// The returned slice's lifetime is tied to this `Mapping` and must not
    /// outlive the enclosing read call; callers decode it into owned strings
    /// before returning.
    pub(crate) fn descriptor_block(&self, descriptor_offset: Offset) -> Result<&[u8], StatsError> {
        let descriptor = self.descriptor(descriptor_offset)?;
        let total = descriptor.total_size();
        if total < MIN_BLOCK_BYTES || total > MAX_BLOCK_BYTES as u64 {
            return Err(StatsError::InvalidDescriptor(
                "corrupt metric block size".to_owned(),
            ));
        }
        let end = descriptor_offset
            .checked_add(total)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: the span `descriptor_offset..end` was validated against
        // the mapping size; the block is 64-byte aligned (allocation
        // invariant), and the slice is consumed only within the enclosing
        // read call.
        Ok(unsafe {
            std::slice::from_raw_parts(
                self.base.add(descriptor_offset.get() as usize),
                total as usize,
            )
        })
    }

    /// Bounds- and alignment-checked shared access to a value record.
    pub(crate) fn metric_value(&self, value_offset: Offset) -> Result<&MetricValue, StatsError> {
        checked_aligned(value_offset)?;
        let end = value_offset
            .checked_add(VALUE_RECORD_BYTES)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: bounds and 64-byte alignment validated; all fields are
        // atomics, so shared access is sound.
        Ok(unsafe {
            &*self
                .base
                .add(value_offset.get() as usize)
                .cast::<MetricValue>()
        })
    }

    /// Bounds- and alignment-checked access to one atomic vector cell.
    /// Returns a checked byte-copy destination for a mapped payload span.
    ///
    /// The pointer is ephemeral: it is consumed immediately by the caller's
    /// publication/write operation and is never stored across structural work.
    pub(crate) fn byte_write_target(
        &self,
        offset: Offset,
        length: usize,
    ) -> Result<*mut u8, StatsError> {
        if offset.is_null() {
            return Err(StatsError::OutOfBounds);
        }
        let end = offset
            .checked_add(u64::try_from(length).map_err(|_| StatsError::OutOfBounds)?)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: the complete byte span was checked against the mapping.
        Ok(unsafe { self.base.add(offset.get() as usize) })
    }

    /// Reads an exact byte span from the mapping into owned storage.
    pub(crate) fn read_bytes(&self, offset: Offset, length: usize) -> Result<Vec<u8>, StatsError> {
        if offset.is_null() {
            return Err(StatsError::OutOfBounds);
        }
        let end = offset
            .checked_add(u64::try_from(length).map_err(|_| StatsError::OutOfBounds)?)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        let mut bytes = vec![0u8; length];
        // SAFETY: both spans are valid for `length` bytes; the destination is
        // owned and the source is a raw mapped byte region.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.base.add(offset.get() as usize),
                bytes.as_mut_ptr(),
                length,
            );
        }
        Ok(bytes)
    }

    /// Writes bytes through a destination prepared by [`byte_write_target`].
    ///
    /// # Safety
    ///
    /// `target` must be returned by `byte_write_target` for a span at least as
    /// large as `bytes`, and must be consumed while the mapping is alive.
    pub(crate) unsafe fn write_bytes(target: *mut u8, bytes: &[u8]) {
        // SAFETY: guaranteed by the caller's `byte_write_target` check.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), target, bytes.len()) };
    }

    pub(crate) fn atomic_u64(&self, offset: Offset) -> Result<&AtomicU64, StatsError> {
        if offset.is_null() || offset.get() % std::mem::align_of::<AtomicU64>() as u64 != 0 {
            return Err(StatsError::Misaligned);
        }
        let end = offset
            .checked_add(std::mem::size_of::<AtomicU64>() as u64)
            .ok_or(StatsError::OutOfBounds)?;
        if end.get() > self.size as u64 {
            return Err(StatsError::OutOfBounds);
        }
        // SAFETY: the offset and one-cell span were checked against the
        // mapping; vector cells are initialized as AtomicU64 records.
        Ok(unsafe { &*self.base.add(offset.get() as usize).cast::<AtomicU64>() })
    }
}

/// Rejects null or non-64-byte-aligned record offsets before any pointer
/// access into the mapping: every block (directory, descriptor, value) is
/// allocated 64-byte aligned, so an unaligned offset means corruption.
fn checked_aligned(offset: Offset) -> Result<(), StatsError> {
    if offset.is_null() || offset.get() % 64 != 0 {
        return Err(StatsError::Misaligned);
    }
    Ok(())
}

/// Byte offset of slot `index` in the directory block at `directory_offset`.
fn slot_offset(directory_offset: Offset, index: u32) -> Option<Offset> {
    directory_offset.checked_add(u64::from(index).checked_mul(SLOT_SIZE as u64)?)
}
