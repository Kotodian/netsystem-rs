//! FIFO storage owner and fixed worker slices.

use std::sync::Arc;

use crate::svm::fifo::{Fifo, FifoError};
use crate::svm::ssvm::SsvmPrivate;

#[derive(Debug, Clone, Copy)]
pub struct SvmFifoSegmentConfig {
    pub slices: u32,
}

pub struct SvmFifoSegment {
    segment: Arc<SsvmPrivate>,
    slices: Vec<Vec<Fifo>>,
    capacity_bytes: usize,
}

pub struct SvmFifoSegmentMain {
    segments: Vec<SvmFifoSegment>,
}

impl SvmFifoSegment {
    pub fn new(segment: Arc<SsvmPrivate>, config: SvmFifoSegmentConfig) -> Result<Self, FifoError> {
        if config.slices == 0 {
            return Err(FifoError::InvalidCapacity);
        }
        let mut slices = Vec::with_capacity(config.slices as usize);
        slices.resize_with(config.slices as usize, Vec::new);
        Ok(Self {
            segment,
            slices,
            capacity_bytes: 0,
        })
    }

    /// Allocates a FIFO in one pre-created worker slice.
    pub fn allocate_fifo(&mut self, slice: u32, capacity: usize) -> Result<u32, FifoError> {
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        let fifo = Fifo::with_capacity(capacity)?;
        self.capacity_bytes = self.capacity_bytes.saturating_add(capacity);
        target.push(fifo);
        Ok((target.len() - 1) as u32)
    }

    pub fn fifo(&self, slice: u32, index: u32) -> Option<&Fifo> {
        self.slices.get(slice as usize)?.get(index as usize)
    }

    pub fn fifo_mut(&mut self, slice: u32, index: u32) -> Option<&mut Fifo> {
        self.slices.get_mut(slice as usize)?.get_mut(index as usize)
    }

    pub fn free_fifo(&mut self, slice: u32, index: u32) -> Result<(), FifoError> {
        let target = self
            .slices
            .get_mut(slice as usize)
            .ok_or(FifoError::InvalidCapacity)?;
        if index as usize >= target.len() {
            return Err(FifoError::InvalidCapacity);
        }
        target.swap_remove(index as usize);
        Ok(())
    }

    pub fn preallocate_chunks(&mut self, _: u32, _: u32, _: u32) -> Result<(), FifoError> {
        Ok(())
    }

    pub fn available_bytes(&self) -> usize {
        self.segment.ssvm_size().saturating_sub(self.capacity_bytes)
    }

    pub fn cached_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub fn ssvm(&self) -> &Arc<SsvmPrivate> {
        &self.segment
    }
}

impl SvmFifoSegmentMain {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn add(&mut self, segment: SvmFifoSegment) -> u32 {
        self.segments.push(segment);
        (self.segments.len() - 1) as u32
    }

    pub fn segment(&self, index: u32) -> Option<&SvmFifoSegment> {
        self.segments.get(index as usize)
    }

    pub fn segment_mut(&mut self, index: u32) -> Option<&mut SvmFifoSegment> {
        self.segments.get_mut(index as usize)
    }
}

impl Default for SvmFifoSegmentMain {
    fn default() -> Self {
        Self::new()
    }
}

pub use crate::svm::fifo::Fifo as SvmFifo;
