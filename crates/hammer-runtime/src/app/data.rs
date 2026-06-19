use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::align::CACHE_LINE;
use hammer_infra::boxed::Slice;
use hammer_infra::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppDataAreaConfig {
    pub chunk_size: usize,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppDataAddr {
    chunk: u32,
    generation: u32,
    offset: u32,
    len: u32,
    capacity: u32,
}

impl AppDataAddr {
    #[inline]
    pub const fn new(chunk: u32, generation: u32, offset: u32, len: u32, capacity: u32) -> Self {
        Self {
            chunk,
            generation,
            offset,
            len,
            capacity,
        }
    }

    #[inline]
    pub const fn chunk(self) -> u32 {
        self.chunk
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub const fn offset(self) -> usize {
        self.offset as usize
    }

    #[inline]
    pub const fn len(self) -> usize {
        self.len as usize
    }

    #[inline]
    pub const fn capacity(self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub const fn with_len(self, len: usize) -> Self {
        Self {
            len: len as u32,
            ..self
        }
    }
}

struct AppDataChunk {
    generation: AtomicU32,
    len: AtomicU32,
    in_use: AtomicBool,
}

pub struct AppDataArea {
    chunk_size: usize,
    storage: Slice<u8>,
    chunks: Vec<AppDataChunk>,
}

impl std::fmt::Debug for AppDataArea {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppDataArea")
            .field("chunk_size", &self.chunk_size)
            .field("chunk_count", &self.chunk_count())
            .finish()
    }
}

unsafe impl Send for AppDataArea {}
unsafe impl Sync for AppDataArea {}

impl AppDataArea {
    pub fn new(config: AppDataAreaConfig) -> HammerResult<Self> {
        if config.chunk_size == 0 {
            return Err(HammerError::internal(
                "app data chunk size must be non-zero",
            ));
        }
        if config.chunk_size % CACHE_LINE != 0 {
            return Err(HammerError::internal(
                "app data chunk size must be cacheline aligned",
            ));
        }
        if config.chunk_count == 0 {
            return Err(HammerError::internal(
                "app data chunk count must be non-zero",
            ));
        }
        let total = config
            .chunk_size
            .checked_mul(config.chunk_count)
            .ok_or_else(|| HammerError::internal("app data area size overflow"))?;
        if total > u32::MAX as usize {
            return Err(HammerError::internal(
                "app data area exceeds u32 address space",
            ));
        }
        let storage = Slice::from_elem(total, 0);
        let mut chunks = Vec::with_capacity(config.chunk_count);
        for _ in 0..config.chunk_count {
            chunks.push(AppDataChunk {
                generation: AtomicU32::new(1),
                len: AtomicU32::new(0),
                in_use: AtomicBool::new(false),
            });
        }
        Ok(Self {
            chunk_size: config.chunk_size,
            storage,
            chunks,
        })
    }

    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn alloc(&self) -> Option<AppDataAddr> {
        let index = self.chunks.iter().position(|chunk| {
            chunk
                .in_use
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })?;
        let chunk = &self.chunks[index];
        chunk.len.store(0, Ordering::Release);
        Some(AppDataAddr::new(
            index as u32,
            chunk.generation.load(Ordering::Acquire),
            (index * self.chunk_size) as u32,
            0,
            self.chunk_size as u32,
        ))
    }

    pub fn alloc_chunk(&self, index: u32) -> HammerResult<AppDataAddr> {
        let Some(chunk) = self.chunks.get(index as usize) else {
            return Err(HammerError::internal("app data chunk index out of range"));
        };
        chunk
            .in_use
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| HammerError::internal("app data chunk is already allocated"))?;
        chunk.len.store(0, Ordering::Release);
        Ok(AppDataAddr::new(
            index,
            chunk.generation.load(Ordering::Acquire),
            (index as usize * self.chunk_size) as u32,
            0,
            self.chunk_size as u32,
        ))
    }

    pub fn write(&self, addr: AppDataAddr, bytes: &[u8]) -> HammerResult<AppDataAddr> {
        self.validate(addr)?;
        if bytes.len() > self.chunk_size {
            return Err(HammerError::internal(format!(
                "app data write length {} exceeds chunk size {}",
                bytes.len(),
                self.chunk_size
            )));
        }
        let start = addr.offset();
        // SAFETY: `validate` proves `addr` names a live chunk owned by the
        // caller, and the length check keeps the write inside that chunk.
        // `bytes` is an external source slice and cannot overlap this storage.
        unsafe {
            ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.storage.as_ptr().add(start).cast_mut(),
                bytes.len(),
            );
        }
        self.chunks[addr.chunk() as usize]
            .len
            .store(bytes.len() as u32, Ordering::Release);
        Ok(addr.with_len(bytes.len()))
    }

    pub fn copy_from_buffer(
        &self,
        addr: AppDataAddr,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> HammerResult<AppDataAddr> {
        self.validate(addr)?;
        let len = buffers
            .current_len(index)?
            .checked_add(buffers.total_len_not_including_first(index)?)
            .ok_or_else(|| HammerError::internal("app data buffer chain length overflow"))?;
        if len > self.chunk_size {
            return Err(HammerError::internal(format!(
                "app data write length {len} exceeds chunk size {}",
                self.chunk_size
            )));
        }
        let start = addr.offset();
        buffers.with_current_chain_io_segments(index, |segments, _| {
            let mut write_offset = start;
            for segment in segments {
                // SAFETY: `validate` proves `addr` names a live app-data
                // chunk and `len` bounds the sum of all segment lengths by
                // that chunk. Segment slices are borrowed from packet buffers
                // and cannot overlap app-data storage.
                unsafe {
                    ptr::copy_nonoverlapping(
                        segment.as_ptr(),
                        self.storage.as_ptr().add(write_offset).cast_mut(),
                        segment.len(),
                    );
                }
                write_offset += segment.len();
            }
            Ok(())
        })?;
        self.chunks[addr.chunk() as usize]
            .len
            .store(len as u32, Ordering::Release);
        Ok(addr.with_len(len))
    }

    pub fn copy_from_area(
        &self,
        addr: AppDataAddr,
        source: &AppDataArea,
        source_addr: AppDataAddr,
    ) -> HammerResult<AppDataAddr> {
        self.validate(addr)?;
        source.validate(source_addr)?;
        let source_chunk = &source.chunks[source_addr.chunk() as usize];
        let len = source_addr
            .len()
            .min(source_chunk.len.load(Ordering::Acquire) as usize);
        if len > self.chunk_size {
            return Err(HammerError::internal(format!(
                "app data copy length {len} exceeds chunk size {}",
                self.chunk_size
            )));
        }
        let dst = addr.offset();
        let src = source_addr.offset();
        if ptr::eq(self, source) && ranges_overlap(dst, len, src, len) {
            return Err(HammerError::internal("app data copy ranges overlap"));
        }
        // SAFETY: both addresses were validated as live chunks. The length is
        // bounded by the destination chunk and published source length, and
        // overlap is rejected above for same-area copies.
        unsafe {
            ptr::copy_nonoverlapping(
                source.storage.as_ptr().add(src),
                self.storage.as_ptr().add(dst).cast_mut(),
                len,
            );
        }
        self.chunks[addr.chunk() as usize]
            .len
            .store(len as u32, Ordering::Release);
        Ok(addr.with_len(len))
    }

    pub fn read(&self, addr: AppDataAddr) -> HammerResult<Vec<u8>> {
        let len = self.validate_read(addr)?;
        let start = addr.offset();
        let mut out = Vec::from_elem_copy(len, 0_u8);
        // SAFETY: `validate_read` proves `addr` names a live chunk.
        // `len` is bounded by the published chunk length and destination
        // vector length.
        unsafe {
            ptr::copy_nonoverlapping(self.storage.as_ptr().add(start), out.as_mut_ptr(), len);
        }
        Ok(out)
    }

    pub fn copy_to(&self, addr: AppDataAddr, offset: usize, output: &mut [u8]) -> HammerResult<usize> {
        self.validate(addr)?;
        let end = offset
            .checked_add(output.len())
            .ok_or_else(|| HammerError::internal("app data copy offset overflow"))?;
        if end > addr.len() {
            return Err(HammerError::internal("app data copy exceeds length"));
        }
        let available = self.validate_read(addr)?;
        if end > available {
            return Err(HammerError::internal("app data copy exceeds published length"));
        }
        let start = addr
            .offset()
            .checked_add(offset)
            .ok_or_else(|| HammerError::internal("app data copy start overflow"))?;
        unsafe {
            ptr::copy_nonoverlapping(self.storage.as_ptr().add(start), output.as_mut_ptr(), output.len());
        }
        Ok(output.len())
    }

    pub fn release(&self, addr: AppDataAddr) -> HammerResult<()> {
        self.validate(addr)?;
        let chunk = &self.chunks[addr.chunk() as usize];
        chunk.len.store(0, Ordering::Release);
        let next = chunk
            .generation
            .load(Ordering::Acquire)
            .wrapping_add(1)
            .max(1);
        chunk.generation.store(next, Ordering::Release);
        chunk.in_use.store(false, Ordering::Release);
        Ok(())
    }

    fn validate(&self, addr: AppDataAddr) -> HammerResult<()> {
        let Some(chunk) = self.chunks.get(addr.chunk() as usize) else {
            return Err(HammerError::internal("app data chunk index out of range"));
        };
        if !chunk.in_use.load(Ordering::Acquire) {
            return Err(HammerError::internal("app data chunk is not allocated"));
        }
        if chunk.generation.load(Ordering::Acquire) != addr.generation() {
            return Err(HammerError::internal("app data chunk generation is stale"));
        }
        if addr.capacity() != self.chunk_size {
            return Err(HammerError::internal("app data chunk capacity mismatch"));
        }
        let expected_offset = addr.chunk() as usize * self.chunk_size;
        if addr.offset() != expected_offset {
            return Err(HammerError::internal("app data chunk offset mismatch"));
        }
        if addr.offset().saturating_add(addr.capacity()) > self.storage.len() {
            return Err(HammerError::internal("app data chunk range out of bounds"));
        }
        Ok(())
    }

    fn validate_read(&self, addr: AppDataAddr) -> HammerResult<usize> {
        let Some(chunk) = self.chunks.get(addr.chunk() as usize) else {
            return Err(HammerError::internal("app data chunk index out of range"));
        };
        if !chunk.in_use.load(Ordering::Acquire) {
            return Err(HammerError::internal("app data chunk is not allocated"));
        }
        if chunk.generation.load(Ordering::Acquire) != addr.generation() {
            return Err(HammerError::internal("app data chunk generation is stale"));
        }
        if addr.capacity() != self.chunk_size {
            return Err(HammerError::internal("app data chunk capacity mismatch"));
        }
        let chunk_start = addr.chunk() as usize * self.chunk_size;
        if addr.offset() != chunk_start {
            return Err(HammerError::internal("app data chunk offset mismatch"));
        }
        if chunk_start.saturating_add(addr.capacity()) > self.storage.len() {
            return Err(HammerError::internal("app data chunk range out of bounds"));
        }
        Ok(addr
            .len()
            .min(chunk.len.load(Ordering::Acquire) as usize))
    }
}

#[inline]
fn ranges_overlap(
    first_start: usize,
    first_len: usize,
    second_start: usize,
    second_len: usize,
) -> bool {
    let first_end = first_start.saturating_add(first_len);
    let second_end = second_start.saturating_add(second_len);
    first_start < second_end && second_start < first_end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_data_addr_with_len_keeps_chunk_identity() {
        let addr = AppDataAddr::new(2, 3, 128, 16, 64);

        let selected = addr.with_len(4);

        assert_eq!(selected.chunk(), 2);
        assert_eq!(selected.generation(), 3);
        assert_eq!(selected.offset(), 128);
        assert_eq!(selected.len(), 4);
        assert_eq!(selected.capacity(), 64);
    }
}
